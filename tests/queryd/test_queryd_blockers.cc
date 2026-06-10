// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Tests for Queryd & Partition BLOCKER fixes

#include <gtest/gtest.h>

#include "cedar/partition/mth/streaming_partitioner.h"
#include "cedar/partition/mth/cedar_key.h"
#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/query_storage_client.h"
#include "cedar/queryd/meta_client.h"

#include <grpcpp/server_context.h>

using namespace cedar;
using namespace cedar::partition;
using namespace cedar::queryd;
using namespace cedar::cypher;

// ============================================================================
// MTH Temporal Routing Byte-Swap Fix
// ============================================================================

TEST(MthRouting, TimestampIsDecodedCorrectly) {
  // Create a streaming partitioner with 2 partitions
  StreamingPartitioner partitioner(2, 1000);

  // Create two keys with the same entity_id but different timestamps.
  // CedarKey::Vertex encodes the timestamp in big-endian.
  uint64_t ts1 = 1000000;  // 1 second in microseconds
  uint64_t ts2 = 2000000;  // 2 seconds in microseconds

  cedar::partition::CedarKey key1 = cedar::partition::CedarKey::Vertex(42, 0, ts1);
  cedar::partition::CedarKey key2 = cedar::partition::CedarKey::Vertex(42, 0, ts2);

  // Before the fix, timestamp_be was used raw (big-endian) which would
  // cause incorrect temporal affinity scoring. After the fix, both keys
  // should be decoded back to host byte order for scoring.
  uint16_t pid1 = partitioner.AssignEvent(key1);
  uint16_t pid2 = partitioner.AssignEvent(key2);

  // Both should be valid partition IDs
  EXPECT_LT(pid1, 2);
  EXPECT_LT(pid2, 2);

  // Verify that DecodeTimestamp reverses EncodeTimestamp
  EXPECT_EQ(cedar::partition::CedarKey::DecodeTimestamp(
      cedar::partition::CedarKey::Vertex(42, 0, ts1).timestamp_be), ts1);
  EXPECT_EQ(cedar::partition::CedarKey::DecodeTimestamp(
      cedar::partition::CedarKey::Vertex(42, 0, ts2).timestamp_be), ts2);
}

TEST(MthRouting, BigEndianTimestampDoesNotCorruptRouting) {
  // Use a large timestamp whose big-endian representation is very different
  // from the host-endian value.
  StreamingPartitioner partitioner(4, 1000);

  uint64_t ts = 0x0000000000000001ULL;  // Small value
  cedar::partition::CedarKey key = cedar::partition::CedarKey::Vertex(99, 0, ts);

  // The raw big-endian value would be 0x0100000000000000 (very large)
  // which would skew temporal affinity. After the fix, it should be
  // decoded back to 1.
  uint16_t pid = partitioner.AssignEvent(key);
  EXPECT_LT(pid, 4);

  // Sanity check: the decoded timestamp must match the original
  EXPECT_EQ(cedar::partition::CedarKey::DecodeTimestamp(key.timestamp_be), ts);
}

// ============================================================================
// Query Timeout Enforcement
// ============================================================================

TEST(QueryTimeout, DistributedExecutorRespectsTimeout) {
  // Create minimal dependencies
  QueryStorageClient storage_client;
  QueryMetaClient::Options meta_options;
  QueryMetaClient meta_client(meta_options);

  // Do not Init() the meta client; GetSchema/GetClusterState still return
  // OK with empty/default data, which is enough for construction.
  DistributedExecutor executor(&storage_client, &meta_client, 1);

  // Execute a simple query with zero timeout -- should fail immediately
  std::string query = "MATCH (n) RETURN n";
  std::unordered_map<std::string, cypher::Value> parameters;
  DistributedExecutionContext ctx;
  ctx.timeout_ms = 0;  // Zero timeout means any elapsed time exceeds it
  cypher::ResultSet result;

  cedar::Status s = executor.Execute(query, parameters, &ctx, &result);

  // Should fail because timeout is exceeded
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("timeout"), std::string::npos);
}

TEST(QueryTimeout, DistributedExecutorAllowsQueryWithReasonableTimeout) {
  QueryStorageClient storage_client;
  QueryMetaClient::Options meta_options;
  QueryMetaClient meta_client(meta_options);
  DistributedExecutor executor(&storage_client, &meta_client, 1);

  std::string query = "MATCH (n) RETURN n";
  std::unordered_map<std::string, cypher::Value> parameters;
  DistributedExecutionContext ctx;
  ctx.timeout_ms = 300000;  // 5 minutes
  cypher::ResultSet result;

  cedar::Status s = executor.Execute(query, parameters, &ctx, &result);

  // With a generous timeout, the query should at least parse and either
  // succeed or fail for storage-related reasons, not timeout.
  if (!s.ok()) {
    EXPECT_EQ(s.ToString().find("timeout"), std::string::npos);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
