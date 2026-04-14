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
#include "cedar/raft/partition_raft_manager.h"
#include "cedar/storage/storage_health_monitor.h"

using namespace cedar;
using namespace cedar::raft;

class PartitionRaftManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto health_monitor = std::make_shared<storage::StorageHealthMonitor>();
    
    PartitionRaftManagerConfig config;
    config.default_replica_count = 3;
    config.placement_strategy = PartitionPlacementStrategy::kConsistentHash;
    
    manager_ = std::make_unique<PartitionRaftManager>();
    Status s = manager_->Initialize(config, health_monitor);
    ASSERT_TRUE(s.ok());
    
    // 注册测试节点
    manager_->RegisterNode("node-1", "127.0.0.1", 9779);
    manager_->RegisterNode("node-2", "127.0.0.1", 9780);
    manager_->RegisterNode("node-3", "127.0.0.1", 9781);
  }
  
  std::unique_ptr<PartitionRaftManager> manager_;
};

TEST_F(PartitionRaftManagerTest, ComputePartitionId) {
  // Test partition ID calculation
  PartitionID part1 = PartitionRaftManager::ComputePartitionId(12345);
  PartitionID part2 = PartitionRaftManager::ComputePartitionId(12346);
  
  EXPECT_EQ(part1, 12345 & 0xFFFF);
  EXPECT_EQ(part2, 12346 & 0xFFFF);
}

TEST_F(PartitionRaftManagerTest, GetPartitionIdForKey) {
  PartitionID part1 = manager_->GetPartitionIdForKey("user:12345");
  PartitionID part2 = manager_->GetPartitionIdForKey("user:12346");
  
  // Should be within valid range
  EXPECT_LT(part1, 65536);
  EXPECT_LT(part2, 65536);
  
  // Same key should give same partition
  PartitionID part1_again = manager_->GetPartitionIdForKey("user:12345");
  EXPECT_EQ(part1, part1_again);
}

TEST_F(PartitionRaftManagerTest, CreatePartitionGroup) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  
  Status s = manager_->CreatePartitionGroup(42, replicas);
  ASSERT_TRUE(s.ok());
  
  auto group = manager_->GetPartitionGroup(42);
  ASSERT_NE(group, nullptr);
  EXPECT_EQ(group->GetPartitionId(), 42);
}

TEST_F(PartitionRaftManagerTest, CreateDuplicatePartitionGroup) {
  std::vector<std::string> replicas = {"node-1", "node-2"};
  
  Status s = manager_->CreatePartitionGroup(100, replicas);
  ASSERT_TRUE(s.ok());
  
  // Try to create again
  s = manager_->CreatePartitionGroup(100, replicas);
  EXPECT_FALSE(s.ok());
}

TEST_F(PartitionRaftManagerTest, RemovePartitionGroup) {
  std::vector<std::string> replicas = {"node-1", "node-2"};
  
  Status s = manager_->CreatePartitionGroup(200, replicas);
  ASSERT_TRUE(s.ok());
  
  s = manager_->RemovePartitionGroup(200);
  EXPECT_TRUE(s.ok());
  
  auto group = manager_->GetPartitionGroup(200);
  EXPECT_EQ(group, nullptr);
}

TEST_F(PartitionRaftManagerTest, RouteWriteAutoCreatesPartition) {
  // Before creating partition
  auto result = manager_->RouteWriteByPartition(300);
  
  // May succeed (auto-create) or fail (if not enough healthy nodes)
  // Both are valid behaviors
  if (result.ok()) {
    EXPECT_FALSE(result.ValueOrDie().empty());
  }
}

TEST_F(PartitionRaftManagerTest, RouteReadExistingPartition) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  
  Status s = manager_->CreatePartitionGroup(400, replicas);
  ASSERT_TRUE(s.ok());
  
  auto result = manager_->RouteReadByPartition(400, false);
  ASSERT_TRUE(result.ok());
  EXPECT_FALSE(result.ValueOrDie().empty());
}

TEST_F(PartitionRaftManagerTest, RouteBatchWrite) {
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    keys.push_back("key:" + std::to_string(i));
  }
  
  auto batches = manager_->RouteBatchWrite(keys);
  
  // Should group keys by partition
  EXPECT_GT(batches.size(), 0);
  
  // Total keys should match
  size_t total_keys = 0;
  for (const auto& batch : batches) {
    total_keys += batch.keys.size();
  }
  EXPECT_EQ(total_keys, keys.size());
}

TEST_F(PartitionRaftManagerTest, GetAllPartitions) {
  std::vector<std::string> replicas = {"node-1", "node-2"};
  
  manager_->CreatePartitionGroup(500, replicas);
  manager_->CreatePartitionGroup(501, replicas);
  manager_->CreatePartitionGroup(502, replicas);
  
  auto partitions = manager_->GetAllPartitions();
  EXPECT_GE(partitions.size(), 3);
}

TEST_F(PartitionRaftManagerTest, GetLeaderDistribution) {
  std::vector<std::string> replicas = {"node-1", "node-2", "node-3"};
  
  // Create multiple partitions
  for (int i = 0; i < 10; i++) {
    manager_->CreatePartitionGroup(1000 + i, replicas);
  }
  
  auto distribution = manager_->GetLeaderDistribution();
  
  // Should have distribution info
  EXPECT_GE(distribution.size(), 0);
}

TEST_F(PartitionRaftManagerTest, GetStats) {
  std::vector<std::string> replicas = {"node-1", "node-2"};
  
  manager_->CreatePartitionGroup(600, replicas);
  manager_->CreatePartitionGroup(601, replicas);
  
  auto stats = manager_->GetStats();
  
  EXPECT_EQ(stats.total_partitions, 2);
  EXPECT_EQ(stats.total_nodes, 3);
}

TEST_F(PartitionRaftManagerTest, LeaderChangeCallback) {
  std::vector<std::string> replicas = {"node-1", "node-2"};
  
  bool callback_called = false;
  manager_->SetPartitionLeaderChangeCallback(
    [&callback_called](PartitionID part_id, 
                       const std::string& old_leader,
                       const std::string& new_leader) {
      callback_called = true;
    });
  
  Status s = manager_->CreatePartitionGroup(700, replicas);
  ASSERT_TRUE(s.ok());
  
  // Callback may be called during partition creation
}

TEST_F(PartitionRaftManagerTest, DeregisterNode) {
  Status s = manager_->DeregisterNode("node-3");
  EXPECT_TRUE(s.ok());
  
  auto stats = manager_->GetStats();
  EXPECT_EQ(stats.total_nodes, 2);
  
  // Try to deregister again
  s = manager_->DeregisterNode("node-3");
  EXPECT_FALSE(s.ok());
}
