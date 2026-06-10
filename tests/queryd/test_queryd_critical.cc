// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Critical fixes tests for Queryd error propagation, circuit breaker,
// query timeout enforcement, and storage client initialization.

#include <gtest/gtest.h>

#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/query_storage_client.h"
#include "cedar/queryd/meta_client.h"

#include <grpcpp/server_context.h>

using namespace cedar;
using namespace cedar::queryd;
using namespace cedar::cypher;

// ============================================================================
// Mock MetaClient helpers
// ============================================================================

class MockMetaClientNoState : public QueryMetaClient {
 public:
  MockMetaClientNoState() : QueryMetaClient(Options{}) {}
  const ClusterState* GetCachedClusterState() const { return nullptr; }
};

class MockMetaClientWithState : public QueryMetaClient {
 public:
  explicit MockMetaClientWithState(const ClusterState& state)
      : QueryMetaClient(Options{}), state_(state) {}
  const ClusterState* GetCachedClusterState() const { return &state_; }
 private:
  ClusterState state_;
};

// ============================================================================
// 1. SplitQuery / Execute silent failures
// ============================================================================

TEST(ErrorPropagation, SplitQueryReturnsErrorWhenClusterStateMissing) {
  QueryStorageClient storage_client;
  MockMetaClientNoState meta_client;
  DistributedExecutor executor(&storage_client, &meta_client, 1);

  DistributedExecutionContext ctx;
  ctx.timeout_ms = 300000;
  cypher::ResultSet result;

  auto s = executor.Execute("MATCH (n) RETURN n", {}, &ctx, &result);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST(ErrorPropagation, SinglePartitionErrorIsPropagated) {
  QueryStorageClient storage_client;
  // No nodes registered -> GetNodeClient returns nullptr
  MockMetaClientNoState meta_client;
  DistributedExecutor executor(&storage_client, &meta_client, 1);

  DistributedExecutionContext ctx;
  ctx.timeout_ms = 300000;
  cypher::ResultSet result;

  // Query with an explicit entity id that looks like single-partition
  auto s = executor.Execute("MATCH (n {id: 42}) RETURN n", {}, &ctx, &result);
  // Should fail because router/storage node is missing, not silently fall through
  EXPECT_FALSE(s.ok());
}

// ============================================================================
// 2. Circuit breaker tracking and fast-fail
// ============================================================================

TEST(CircuitBreaker, OpensAfterFailuresAndReturnsUnavailable) {
  QueryStorageClient storage_client;
  storage_client.RegisterNode(0, "127.0.0.1:9779");

  // Trip the breaker (default failure_threshold is 5)
  for (size_t i = 0; i < 5; ++i) {
    storage_client.ReportNodeResult("127.0.0.1:9779", false);
  }

  auto client = storage_client.GetNodeClient(0);
  ASSERT_NE(client, nullptr);

  cypher::ResultSet result;
  auto s = client->ExecuteSubQuery("MATCH (n) RETURN n", {}, &result);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsUnavailable()) << s.ToString();
}

TEST(CircuitBreaker, SuccessResetsFailureCount) {
  QueryStorageClient storage_client;
  storage_client.RegisterNode(1, "127.0.0.1:9780");

  // Report just under the threshold
  for (size_t i = 0; i < 4; ++i) {
    storage_client.ReportNodeResult("127.0.0.1:9780", false);
  }
  // One success should reset the counter
  storage_client.ReportNodeResult("127.0.0.1:9780", true);

  // Now trip again would need 5 more failures
  auto client = storage_client.GetNodeClient(1);
  ASSERT_NE(client, nullptr);
  cypher::ResultSet result;
  auto s = client->ExecuteSubQuery("MATCH (n) RETURN n", {}, &result);
  // Because there is no real server, the remote RPC will fail, but the
  // circuit breaker should still be closed at this point (inner client exists).
  // The failure will be tracked, but breaker is not open yet.
  EXPECT_FALSE(s.IsUnavailable()) << s.ToString();
}

// ============================================================================
// 3. Query timeout / cancellation enforcement
// ============================================================================

TEST(QueryTimeout, ZeroTimeoutFailsImmediately) {
  QueryStorageClient storage_client;
  QueryMetaClient::Options meta_options;
  QueryMetaClient meta_client(meta_options);
  DistributedExecutor executor(&storage_client, &meta_client, 1);

  DistributedExecutionContext ctx;
  ctx.timeout_ms = 0;
  cypher::ResultSet result;

  auto s = executor.Execute("MATCH (n) RETURN n", {}, &ctx, &result);
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("timeout"), std::string::npos);
}

TEST(QueryTimeout, CancellationIsEnforcedDuringExecution) {
  QueryStorageClient storage_client;
  QueryMetaClient::Options meta_options;
  QueryMetaClient meta_client(meta_options);
  DistributedExecutor executor(&storage_client, &meta_client, 1);

  DistributedExecutionContext ctx;
  ctx.timeout_ms = 300000;
  ctx.is_cancelled = []() { return true; };
  cypher::ResultSet result;

  auto s = executor.Execute("MATCH (n) RETURN n", {}, &ctx, &result);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsCancelled()) << s.ToString();
}


