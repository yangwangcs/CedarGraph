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
#include "cedar/raft/partition_metadata_service.h"

using namespace cedar;
using namespace cedar::raft;

class PartitionMetadataServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    PartitionTopologyConfig config;
    config.default_partition_count = 16;
    config.default_replica_count = 3;
    config.heartbeat_timeout_sec = 1;  // 缩短超时时间以加快测试
    config.heartbeat_check_interval_sec = 1;
    
    service_ = std::make_unique<PartitionMetadataService>();
    Status s = service_->Initialize(config);
    ASSERT_TRUE(s.ok());
  }
  
  void TearDown() override {
    service_->Shutdown();
  }
  
  std::unique_ptr<PartitionMetadataService> service_;
};

TEST_F(PartitionMetadataServiceTest, Initialize) {
  EXPECT_NE(service_, nullptr);
}

TEST_F(PartitionMetadataServiceTest, RegisterNode) {
  StorageNodeMetadata node;
  node.node_id = "node-1";
  node.address = "127.0.0.1";
  node.port = 9779;
  node.dc_id = "dc1";
  
  Status s = service_->RegisterNode(node);
  EXPECT_TRUE(s.ok());
  
  // 重复注册应该失败
  s = service_->RegisterNode(node);
  EXPECT_FALSE(s.ok());
}

TEST_F(PartitionMetadataServiceTest, GetNode) {
  StorageNodeMetadata node;
  node.node_id = "node-1";
  node.address = "127.0.0.1";
  node.port = 9779;
  
  Status s = service_->RegisterNode(node);
  ASSERT_TRUE(s.ok());
  
  auto result = service_->GetNode("node-1");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().node_id, "node-1");
  EXPECT_EQ(result.ValueOrDie().address, "127.0.0.1");
  
  // 获取不存在的节点
  result = service_->GetNode("non-existent");
  EXPECT_FALSE(result.ok());
}

TEST_F(PartitionMetadataServiceTest, Heartbeat) {
  StorageNodeMetadata node;
  node.node_id = "node-1";
  node.address = "127.0.0.1";
  node.port = 9779;
  
  Status s = service_->RegisterNode(node);
  ASSERT_TRUE(s.ok());
  
  s = service_->Heartbeat("node-1");
  EXPECT_TRUE(s.ok());
  
  // 不存在的节点
  s = service_->Heartbeat("non-existent");
  EXPECT_FALSE(s.ok());
}

TEST_F(PartitionMetadataServiceTest, GetOnlineNodes) {
  // 注册 3 个节点
  for (int i = 0; i < 3; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  auto nodes = service_->GetOnlineNodes();
  EXPECT_EQ(nodes.size(), 3);
}

TEST_F(PartitionMetadataServiceTest, CreateSpace) {
  // 先注册节点
  for (int i = 0; i < 3; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  // 创建图空间
  Status s = service_->CreateSpace("test_space", 8, 3);
  EXPECT_TRUE(s.ok()) << s.ToString();
  
  // 重复创建应该失败
  s = service_->CreateSpace("test_space", 8, 3);
  EXPECT_FALSE(s.ok());
  
  // 检查分区
  auto partitions = service_->GetSpacePartitions("test_space");
  EXPECT_EQ(partitions.size(), 8);
  
  for (const auto& part : partitions) {
    EXPECT_EQ(part.space_name, "test_space");
    EXPECT_EQ(part.replica_nodes.size(), 3);
    EXPECT_FALSE(part.leader_node.empty());
    EXPECT_EQ(part.state, PartitionMetadata::State::kNormal);
  }
}

TEST_F(PartitionMetadataServiceTest, CreateSpaceNotEnoughNodes) {
  // 只注册 2 个节点，但要求 3 个副本
  for (int i = 0; i < 2; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  Status s = service_->CreateSpace("test_space", 4, 3);
  EXPECT_FALSE(s.ok());  // 应该失败，因为节点不足
}

TEST_F(PartitionMetadataServiceTest, GetPartitionMetadata) {
  // 注册节点
  for (int i = 0; i < 3; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  Status s = service_->CreateSpace("test_space", 4, 3);
  ASSERT_TRUE(s.ok());
  
  // 获取特定分区
  auto result = service_->GetPartitionMetadata("test_space", 0);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().part_id, 0);
  EXPECT_EQ(result.ValueOrDie().space_name, "test_space");
  
  // 获取不存在的分区
  result = service_->GetPartitionMetadata("test_space", 100);
  EXPECT_FALSE(result.ok());
  
  // 获取不存在的图空间
  result = service_->GetPartitionMetadata("non_existent", 0);
  EXPECT_FALSE(result.ok());
}

TEST_F(PartitionMetadataServiceTest, UpdatePartitionLeader) {
  // 注册节点
  for (int i = 0; i < 3; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  Status s = service_->CreateSpace("test_space", 4, 3);
  ASSERT_TRUE(s.ok());
  
  // 获取原始 Leader
  auto result = service_->GetPartitionMetadata("test_space", 0);
  ASSERT_TRUE(result.ok());
  std::string old_leader = result.ValueOrDie().leader_node;
  
  // 更新 Leader
  std::string new_leader = (old_leader == "node-0") ? "node-1" : "node-0";
  s = service_->UpdatePartitionLeader("test_space", 0, new_leader);
  EXPECT_TRUE(s.ok());
  
  // 验证更新
  result = service_->GetPartitionMetadata("test_space", 0);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().leader_node, new_leader);
  EXPECT_EQ(result.ValueOrDie().version, 2);  // 版本应该增加
}

TEST_F(PartitionMetadataServiceTest, MigratePartition) {
  // 注册节点
  for (int i = 0; i < 4; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  Status s = service_->CreateSpace("test_space", 4, 3);
  ASSERT_TRUE(s.ok());
  
  // 获取分区信息
  auto result = service_->GetPartitionMetadata("test_space", 0);
  ASSERT_TRUE(result.ok());
  std::string from_node = result.ValueOrDie().replica_nodes[0];
  std::string to_node = "node-3";  // 新节点
  
  // 执行迁移
  s = service_->MigratePartition("test_space", 0, from_node, to_node);
  EXPECT_TRUE(s.ok());
  
  // 验证迁移
  result = service_->GetPartitionMetadata("test_space", 0);
  ASSERT_TRUE(result.ok());
  
  // 检查新节点是否在副本列表中
  bool found = false;
  for (const auto& node : result.ValueOrDie().replica_nodes) {
    if (node == to_node) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(PartitionMetadataServiceTest, AddRemovePartitionReplica) {
  // 注册更多节点以确保有足够的选择
  for (int i = 0; i < 6; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  Status s = service_->CreateSpace("test_space", 8, 3);
  ASSERT_TRUE(s.ok());
  
  // 找到分区 0 的副本中没有使用的节点
  auto result = service_->GetPartitionMetadata("test_space", 0);
  ASSERT_TRUE(result.ok());
  auto existing_replicas = result.ValueOrDie().replica_nodes;
  
  std::string new_node;
  for (int i = 0; i < 6; i++) {
    std::string candidate = "node-" + std::to_string(i);
    bool found = false;
    for (const auto& replica : existing_replicas) {
      if (replica == candidate) {
        found = true;
        break;
      }
    }
    if (!found) {
      new_node = candidate;
      break;
    }
  }
  
  ASSERT_FALSE(new_node.empty()) << "Need at least one node not in replica list";
  
  // 添加副本
  s = service_->AddPartitionReplica("test_space", 0, new_node);
  EXPECT_TRUE(s.ok()) << s.ToString();
  
  // 验证添加
  result = service_->GetPartitionMetadata("test_space", 0);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().replica_nodes.size(), 4);
  
  // 重复添加应该失败
  s = service_->AddPartitionReplica("test_space", 0, new_node);
  EXPECT_FALSE(s.ok());
  
  // 移除副本（不是 Leader）
  s = service_->RemovePartitionReplica("test_space", 0, new_node);
  EXPECT_TRUE(s.ok());
  
  // 验证移除
  result = service_->GetPartitionMetadata("test_space", 0);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().replica_nodes.size(), 3);
}

TEST_F(PartitionMetadataServiceTest, GetNodePartitions) {
  // 注册节点
  for (int i = 0; i < 3; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  Status s = service_->CreateSpace("test_space", 4, 3);
  ASSERT_TRUE(s.ok());
  
  // 获取节点上的分区
  auto partitions = service_->GetNodePartitions("node-0");
  EXPECT_GT(partitions.size(), 0);
  
  for (const auto& part : partitions) {
    bool found = false;
    for (const auto& node : part.replica_nodes) {
      if (node == "node-0") {
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found);
  }
}

TEST_F(PartitionMetadataServiceTest, DropSpace) {
  // 注册节点
  for (int i = 0; i < 3; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  Status s = service_->CreateSpace("test_space", 4, 3);
  ASSERT_TRUE(s.ok());
  
  // 删除图空间
  s = service_->DropSpace("test_space");
  EXPECT_TRUE(s.ok());
  
  // 验证删除
  auto partitions = service_->GetSpacePartitions("test_space");
  EXPECT_EQ(partitions.size(), 0);
  
  // 重复删除应该失败
  s = service_->DropSpace("test_space");
  EXPECT_FALSE(s.ok());
}

TEST_F(PartitionMetadataServiceTest, GetStats) {
  // 注册节点
  for (int i = 0; i < 3; i++) {
    StorageNodeMetadata node;
    node.node_id = "node-" + std::to_string(i);
    node.address = "127.0.0.1";
    node.port = 9779 + i;
    
    Status s = service_->RegisterNode(node);
    ASSERT_TRUE(s.ok());
  }
  
  // 创建图空间
  Status s = service_->CreateSpace("space1", 4, 3);
  ASSERT_TRUE(s.ok());
  
  s = service_->CreateSpace("space2", 8, 3);
  ASSERT_TRUE(s.ok());
  
  // 获取统计
  auto stats = service_->GetStats();
  EXPECT_EQ(stats.total_spaces, 2);
  EXPECT_EQ(stats.total_partitions, 12);  // 4 + 8
  EXPECT_EQ(stats.total_nodes, 3);
  EXPECT_EQ(stats.online_nodes, 3);
}

TEST_F(PartitionMetadataServiceTest, WatchTopologyChanges) {
  bool callback_called = false;
  TopologyChange captured_change;
  
  service_->WatchTopologyChanges(
    [&callback_called, &captured_change](const TopologyChange& change) {
      callback_called = true;
      captured_change = change;
    });
  
  // 注册节点（应该触发回调）
  StorageNodeMetadata node;
  node.node_id = "node-1";
  node.address = "127.0.0.1";
  node.port = 9779;
  
  Status s = service_->RegisterNode(node);
  ASSERT_TRUE(s.ok());
  
  // 验证回调被调用
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(captured_change.type, TopologyChangeType::kNodeJoined);
  EXPECT_EQ(captured_change.old_node, "node-1");
}

TEST_F(PartitionMetadataServiceTest, PartitionMetadataSerialize) {
  PartitionMetadata metadata;
  metadata.part_id = 42;
  metadata.leader_node = "node-1";
  metadata.replica_nodes = {"node-1", "node-2", "node-3"};
  metadata.space_name = "test_space";
  metadata.state = PartitionMetadata::State::kNormal;
  metadata.key_count = 1000;
  metadata.data_size = 1024000;
  metadata.version = 5;
  metadata.created_at = std::chrono::system_clock::now();
  metadata.updated_at = std::chrono::system_clock::now();
  
  // 序列化
  std::string serialized = metadata.Serialize();
  EXPECT_FALSE(serialized.empty());
  
  // 反序列化
  auto result = PartitionMetadata::Deserialize(serialized);
  ASSERT_TRUE(result.ok());
  
  auto deserialized = result.ValueOrDie();
  EXPECT_EQ(deserialized.part_id, 42);
  EXPECT_EQ(deserialized.leader_node, "node-1");
  EXPECT_EQ(deserialized.replica_nodes.size(), 3);
  EXPECT_EQ(deserialized.space_name, "test_space");
  EXPECT_EQ(deserialized.key_count, 1000);
  EXPECT_EQ(deserialized.data_size, 1024000);
  EXPECT_EQ(deserialized.version, 5);
}

TEST_F(PartitionMetadataServiceTest, StorageNodeMetadataSerialize) {
  StorageNodeMetadata node;
  node.node_id = "node-1";
  node.address = "127.0.0.1";
  node.port = 9779;
  node.dc_id = "dc1";
  node.state = StorageNodeMetadata::State::kOnline;
  node.num_partitions = 10;
  node.total_disk_bytes = 1000000000;
  node.used_disk_bytes = 500000000;
  node.cpu_usage_percent = 45.5;
  node.memory_usage_percent = 60.0;
  node.registered_at = std::chrono::system_clock::now();
  node.last_heartbeat = std::chrono::system_clock::now();
  
  // 序列化
  std::string serialized = node.Serialize();
  EXPECT_FALSE(serialized.empty());
  
  // 反序列化
  auto result = StorageNodeMetadata::Deserialize(serialized);
  ASSERT_TRUE(result.ok());
  
  auto deserialized = result.ValueOrDie();
  EXPECT_EQ(deserialized.node_id, "node-1");
  EXPECT_EQ(deserialized.address, "127.0.0.1");
  EXPECT_EQ(deserialized.port, 9779);
  EXPECT_EQ(deserialized.dc_id, "dc1");
  EXPECT_EQ(deserialized.num_partitions, 10);
  EXPECT_EQ(deserialized.total_disk_bytes, 1000000000);
  EXPECT_DOUBLE_EQ(deserialized.cpu_usage_percent, 45.5);
}
