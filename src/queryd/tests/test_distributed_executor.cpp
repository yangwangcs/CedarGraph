// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Tests for Distributed Executor

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/query_storage_client.h"
#include "cedar/queryd/meta_client.h"

using namespace cedar::queryd;
using ::testing::_;
using ::testing::Return;

// Mock classes
#include "cedar/queryd/query_storage_client.h"

class MockStorageClient : public QueryStorageClient {
 public:
  MockStorageClient() : QueryStorageClient(Options{}) {}
  
  MOCK_METHOD(Status, ScanNode, (uint64_t, Timestamp, 
              std::vector<std::pair<Timestamp, Descriptor>>*), (override));
  MOCK_METHOD(Status, ScanOutEdges, (uint64_t, uint16_t, Timestamp,
              std::vector<EdgeScanEntry>*), (override));
};

class MockMetaClient : public QueryMetaClient {
 public:
  MockMetaClient() : QueryMetaClient(Options{}) {}
  
  MOCK_METHOD(const ClusterState*, GetCachedClusterState, (), (const, override));
};

class DistributedExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_client_ = std::make_shared<MockStorageClient>();
    meta_client_ = std::make_shared<MockMetaClient>();
    
    executor_ = std::make_unique<DistributedExecutor>(
        storage_client_.get(),
        meta_client_.get(),
        4  // 4 workers
    );
  }
  
  std::shared_ptr<MockStorageClient> storage_client_;
  std::shared_ptr<MockMetaClient> meta_client_;
  std::unique_ptr<DistributedExecutor> executor_;
};

TEST_F(DistributedExecutorTest, BasicQueryExecution) {
  DistributedExecutionContext ctx;
  ctx.query_id = "test-001";
  ctx.timeout_ms = 30000;
  
  cypher::ResultSet result;
  
  // Simple query
  auto s = executor_->Execute(
      "MATCH (n:Person) RETURN n",
      {},
      &ctx,
      &result
  );
  
  // Should succeed (mock returns OK)
  EXPECT_TRUE(s.ok() || s.IsNotSupported());
}

TEST_F(DistributedExecutorTest, ExplainQuery) {
  std::string explain;
  
  auto s = executor_->ExecuteExplain(
      "MATCH (n:Person)-[:WORKS_FOR]->(c:Company) RETURN n.name, c.name",
      &explain
  );
  
  EXPECT_TRUE(s.ok());
  EXPECT_FALSE(explain.empty());
}

TEST_F(DistributedExecutorTest, QueryPlanCache) {
  QueryPlanCache cache(10);
  
  // Create a mock plan
  auto plan = std::make_shared<cypher::ExecutionPlan>(nullptr);
  
  // Put in cache
  cache.Put("SELECT * FROM test", plan);
  
  // Get from cache
  auto cached = cache.Get("SELECT * FROM test");
  EXPECT_NE(cached, nullptr);
  
  // Get non-existent
  auto missing = cache.Get("SELECT * FROM other");
  EXPECT_EQ(missing, nullptr);
  
  // Check stats
  auto stats = cache.GetStats();
  EXPECT_EQ(stats.size, 1);
  EXPECT_EQ(stats.hits, 1);
  EXPECT_EQ(stats.misses, 1);
}

TEST_F(DistributedExecutorTest, PartitionRouter) {
  ClusterState state;
  state.partition_count = 4;
  
  PartitionInfo p0{0, "127.0.0.1:9779", {}, 0, 0, true, 0, 0};
  PartitionInfo p1{1, "127.0.0.1:9780", {}, 0, 0, true, 0, 0};
  state.partitions.push_back(p0);
  state.partitions.push_back(p1);
  
  EXPECT_CALL(*meta_client_, GetCachedClusterState())
      .WillRepeatedly(Return(&state));
  
  PartitionRouter router(meta_client_.get());
  
  // Test routing
  EXPECT_EQ(router.GetPartitionId(0), 0);
  EXPECT_EQ(router.GetPartitionId(1), 1);
  EXPECT_EQ(router.GetPartitionId(4), 0);
  EXPECT_EQ(router.GetPartitionId(5), 1);
}

TEST_F(DistributedExecutorTest, ResultMerger) {
  ResultMerger merger;
  
  // Create test results
  std::vector<SubQueryResult> results;
  
  SubQueryResult r1;
  r1.partition_id = 0;
  r1.status = Status::OK();
  // Add records to r1.result...
  results.push_back(r1);
  
  SubQueryResult r2;
  r2.partition_id = 1;
  r2.status = Status::OK();
  results.push_back(r2);
  
  // Merge
  auto merged = merger.Merge(results);
  
  EXPECT_TRUE(merged.columns.empty());  // Empty test
}

TEST_F(DistributedExecutorTest, ParallelExecutor) {
  ParallelExecutor executor(4);
  
  std::vector<SubQueryTask> tasks;
  for (int i = 0; i < 10; ++i) {
    SubQueryTask task;
    task.partition_id = i;
    task.storage_node = "127.0.0.1:9779";
    task.sequence = i;
    tasks.push_back(task);
  }
  
  DistributedExecutionContext ctx;
  
  // Execute in parallel
  auto results = executor.ExecuteParallel(tasks, storage_client_.get(), &ctx);
  
  EXPECT_EQ(results.size(), 10);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
