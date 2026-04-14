// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "cedar/raft/partition_router.h"
#include "cedar/storage/storage_health_monitor.h"

using namespace cedar;
using namespace cedar::raft;

class PartitionRouterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto health_monitor = std::make_shared<storage::StorageHealthMonitor>();
    
    PartitionRouterConfig config;
    config.default_replica_count = 3;
    config.enable_read_from_follower = true;
    
    router_ = std::make_unique<PartitionRouter>();
    Status s = router_->Initialize(config, health_monitor);
    ASSERT_TRUE(s.ok());
    
    // Register test nodes
    router_->RegisterNode("node-1", "127.0.0.1", 9779, "dc1");
    router_->RegisterNode("node-2", "127.0.0.1", 9780, "dc1");
    router_->RegisterNode("node-3", "127.0.0.1", 9781, "dc2");
  }
  
  std::unique_ptr<PartitionRouter> router_;
};

TEST_F(PartitionRouterTest, Initialize) {
  EXPECT_NE(router_, nullptr);
}

TEST_F(PartitionRouterTest, RegisterNode) {
  Status s = router_->RegisterNode("node-4", "127.0.0.1", 9782, "dc1");
  EXPECT_TRUE(s.ok());
}

TEST_F(PartitionRouterTest, DeregisterNode) {
  Status s = router_->DeregisterNode("node-1");
  EXPECT_TRUE(s.ok());
}

TEST_F(PartitionRouterTest, RouteWriteByEntityId) {
  // Create a partition first
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  Status s = router_->CreatePartition(100, replicas);
  ASSERT_TRUE(s.ok());
  
  // Route write for entity
  auto result = router_->RouteWrite(12345);
  ASSERT_TRUE(result.ok());
  
  RoutingTarget target = result.ValueOrDie();
  EXPECT_FALSE(target.node_id.empty());
  EXPECT_TRUE(target.is_leader);
  EXPECT_EQ(target.partition_id, 12345 & 0xFFFF);
}

TEST_F(PartitionRouterTest, RouteWriteByKey) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  Status s = router_->CreatePartition(200, replicas);
  ASSERT_TRUE(s.ok());
  
  auto result = router_->RouteWrite("user:12345");
  ASSERT_TRUE(result.ok());
  
  RoutingTarget target = result.ValueOrDie();
  EXPECT_FALSE(target.node_id.empty());
  EXPECT_TRUE(target.is_leader);
}

TEST_F(PartitionRouterTest, RouteReadRequireLeader) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  Status s = router_->CreatePartition(300, replicas);
  ASSERT_TRUE(s.ok());
  
  auto result = router_->RouteRead(300, true);
  ASSERT_TRUE(result.ok());
  
  RoutingTarget target = result.ValueOrDie();
  EXPECT_TRUE(target.is_leader);
}

TEST_F(PartitionRouterTest, RouteReadAllowFollower) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  Status s = router_->CreatePartition(400, replicas);
  ASSERT_TRUE(s.ok());
  
  auto result = router_->RouteRead(400, false);
  ASSERT_TRUE(result.ok());
  
  RoutingTarget target = result.ValueOrDie();
  EXPECT_FALSE(target.node_id.empty());
}

TEST_F(PartitionRouterTest, RouteBatchWrite) {
  std::vector<std::string> keys;
  for (int i = 0; i < 50; i++) {
    keys.push_back("key:" + std::to_string(i));
  }
  
  auto results = router_->RouteBatchWrite(keys);
  
  // Should have at least one batch
  EXPECT_GT(results.size(), 0);
  
  // Total keys should match
  size_t total_keys = 0;
  for (const auto& result : results) {
    total_keys += result.keys.size();
    EXPECT_TRUE(result.target.is_leader);
  }
  EXPECT_EQ(total_keys, keys.size());
}

TEST_F(PartitionRouterTest, RouteBatchRead) {
  std::vector<std::string> keys;
  for (int i = 0; i < 50; i++) {
    keys.push_back("key:" + std::to_string(i));
  }
  
  // First do batch write to auto-create partitions
  auto write_results = router_->RouteBatchWrite(keys);
  EXPECT_GT(write_results.size(), 0);
  
  // Now batch read should work on existing partitions
  auto read_results = router_->RouteBatchRead(keys, false);
  
  EXPECT_GT(read_results.size(), 0);
  
  size_t total_keys = 0;
  for (const auto& result : read_results) {
    total_keys += result.keys.size();
  }
  EXPECT_EQ(total_keys, keys.size());
}

TEST_F(PartitionRouterTest, CreateAndRemovePartition) {
  std::vector<std::string> replicas = {"node-1", "node-2"};
  
  Status s = router_->CreatePartition(500, replicas);
  ASSERT_TRUE(s.ok());
  
  auto info = router_->GetPartitionInfo(500);
  ASSERT_TRUE(info.ok());
  EXPECT_EQ(info.ValueOrDie().part_id, 500);
  
  s = router_->RemovePartition(500);
  EXPECT_TRUE(s.ok());
}

TEST_F(PartitionRouterTest, GetAllPartitions) {
  std::vector<std::string> replicas = {"node-1", "node-2"};
  
  router_->CreatePartition(600, replicas);
  router_->CreatePartition(601, replicas);
  router_->CreatePartition(602, replicas);
  
  auto partitions = router_->GetAllPartitions();
  EXPECT_GE(partitions.size(), 3);
}

TEST_F(PartitionRouterTest, GetStats) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  router_->CreatePartition(700, replicas);
  
  auto stats = router_->GetStats();
  EXPECT_GE(stats.total_partitions, 1);
  EXPECT_GE(stats.total_nodes, 3);
}

TEST_F(PartitionRouterTest, GetLeaderBackwardCompatibility) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  router_->CreatePartition(0, replicas);  // Default partition
  
  auto result = router_->GetLeader();
  ASSERT_TRUE(result.ok());
  
  RoutingTarget target = result.ValueOrDie();
  EXPECT_FALSE(target.node_id.empty());
}

TEST_F(PartitionRouterTest, GetNodeForReadWrite) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  router_->CreatePartition(800, replicas);
  
  auto read_result = router_->GetNodeForRead();
  EXPECT_TRUE(read_result.ok());
  
  auto write_result = router_->GetNodeForWrite();
  EXPECT_TRUE(write_result.ok());
}

TEST_F(PartitionRouterTest, RouteChangeCallback) {
  bool callback_called = false;
  router_->SetRouteChangeCallback(
    [&callback_called](PartitionID part_id, const std::string& old_node,
                       const std::string& new_node) {
      callback_called = true;
    });
  
  std::vector<std::string> replicas = {"node-1", "node-2"};
  router_->CreatePartition(900, replicas);
  
  // Callback should be triggered during partition operations
  EXPECT_FALSE(callback_called);  // Not triggered on creation
}

TEST_F(PartitionRouterTest, TargetHasAddressInfo) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  router_->CreatePartition(1000, replicas);
  
  auto result = router_->RouteWrite(1000);
  ASSERT_TRUE(result.ok());
  
  RoutingTarget target = result.ValueOrDie();
  EXPECT_FALSE(target.node_id.empty());
  EXPECT_FALSE(target.address.empty());
  EXPECT_NE(target.port, 0);
  EXPECT_FALSE(target.dc_id.empty());
}
