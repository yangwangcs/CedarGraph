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

#include "cedar/dtx/partition.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;
using namespace cedar::dtx;

// =============================================================================
// PartitionMeta 测试
// =============================================================================

TEST(PartitionMetaTest, DefaultConstruction) {
  PartitionMeta meta;
  EXPECT_EQ(meta.partition_id, kInvalidPartitionID);
  EXPECT_EQ(meta.primary_node, kInvalidNodeID);
  EXPECT_EQ(meta.vertex_count.load(), 0);
  EXPECT_EQ(meta.edge_count.load(), 0);
}

TEST(PartitionMetaTest, ParamConstruction) {
  PartitionMeta meta(42);
  EXPECT_EQ(meta.partition_id, 42);
  EXPECT_EQ(meta.primary_node, kInvalidNodeID);
}

TEST(PartitionMetaTest, AddSubgraph) {
  PartitionMeta meta(1);
  meta.AddSubgraph(100);
  meta.AddSubgraph(200);
  
  EXPECT_TRUE(meta.ContainsSubgraph(100));
  EXPECT_TRUE(meta.ContainsSubgraph(200));
  EXPECT_FALSE(meta.ContainsSubgraph(300));
}

TEST(PartitionMetaTest, UpdateLoadStats) {
  PartitionMeta meta(1);
  meta.UpdateLoadStats(1000, 50);
  
  EXPECT_EQ(meta.txn_rate.load(), 1000);
  EXPECT_EQ(meta.conflict_rate.load(), 50);
}

TEST(PartitionMetaTest, SerializeDeserialize) {
  PartitionMeta original(42);
  original.primary_node = 100;
  original.vertex_count.store(1000);
  original.edge_count.store(5000);
  original.txn_rate.store(100);
  original.conflict_rate.store(5);
  original.AddSubgraph(10);
  original.AddSubgraph(20);
  
  std::string serialized = original.Serialize();
  auto restored = PartitionMeta::Deserialize(serialized);
  
  EXPECT_EQ(restored.partition_id, original.partition_id);
  EXPECT_EQ(restored.primary_node, original.primary_node);
  EXPECT_EQ(restored.vertex_count.load(), original.vertex_count.load());
  EXPECT_EQ(restored.edge_count.load(), original.edge_count.load());
  EXPECT_EQ(restored.txn_rate.load(), original.txn_rate.load());
  EXPECT_EQ(restored.conflict_rate.load(), original.conflict_rate.load());
  EXPECT_TRUE(restored.ContainsSubgraph(10));
  EXPECT_TRUE(restored.ContainsSubgraph(20));
}

// =============================================================================
// PartitionLoadStats 测试
// =============================================================================

TEST(PartitionLoadStatsTest, LoadScoreCalculation) {
  PartitionLoadStats stats;
  stats.partition_id = 1;
  stats.data_size_bytes = 500 * 1024 * 1024;  // 500MB
  stats.txn_count_1min = 50000;
  stats.conflict_count_1min = 2500;  // 5%冲突率
  stats.p99_latency_ms = 50;
  
  double score = stats.ComputeLoadScore();
  
  // 分数应该在0-1之间
  EXPECT_GT(score, 0.0);
  EXPECT_LE(score, 1.0);
  
  // 超低负载应该分数更低（主要是数据大小贡献）
  PartitionLoadStats low_load;
  low_load.data_size_bytes = 1 * 1024 * 1024;  // 1MB
  low_load.txn_count_1min = 100;
  low_load.conflict_count_1min = 0;
  low_load.p99_latency_ms = 1;
  double low_score = low_load.ComputeLoadScore();
  
  EXPECT_GT(score, low_score);
}

// =============================================================================
// SubgraphBoundary 测试
// =============================================================================

TEST(SubgraphBoundaryTest, ContainsVertex) {
  SubgraphBoundary boundary;
  boundary.subgraph_id = 1;
  boundary.internal_vertex_ids.insert(100);
  boundary.internal_vertex_ids.insert(200);
  boundary.boundary_vertex_ids.insert(300);
  
  // 创建测试Key
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  CedarKey key2 = CedarKey::Vertex(300, 0, Timestamp::Now(), 0, 1);
  CedarKey key3 = CedarKey::Vertex(999, 0, Timestamp::Now(), 0, 1);
  
  EXPECT_TRUE(boundary.Contains(key1));   // 内部顶点
  EXPECT_TRUE(boundary.Contains(key2));   // 边界顶点
  EXPECT_FALSE(boundary.Contains(key3));  // 外部顶点
}

TEST(SubgraphBoundaryTest, IsLocalTransaction) {
  SubgraphBoundary boundary;
  boundary.subgraph_id = 1;
  
  // 所有Key在同一分区
  std::vector<CedarKey> local_keys = {
    CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1),
    CedarKey::Vertex(101, 0, Timestamp::Now(), 0, 1),
    CedarKey::Vertex(102, 0, Timestamp::Now(), 0, 1)
  };
  
  // Key在不同分区
  std::vector<CedarKey> cross_keys = {
    CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1),
    CedarKey::Vertex(200, 0, Timestamp::Now(), 0, 2),  // 不同分区
    CedarKey::Vertex(102, 0, Timestamp::Now(), 0, 1)
  };
  
  EXPECT_TRUE(boundary.IsLocalTransaction(local_keys));
  EXPECT_FALSE(boundary.IsLocalTransaction(cross_keys));
}

TEST(SubgraphBoundaryTest, GetInvolvedPartitions) {
  SubgraphBoundary boundary;
  
  std::vector<CedarKey> keys = {
    CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1),
    CedarKey::Vertex(200, 0, Timestamp::Now(), 0, 2),
    CedarKey::Vertex(300, 0, Timestamp::Now(), 0, 1),  // 重复分区1
    CedarKey::Vertex(400, 0, Timestamp::Now(), 0, 3)
  };
  
  auto partitions = boundary.GetInvolvedPartitions(keys);
  
  EXPECT_EQ(partitions.size(), 3);
  // 应该包含分区1, 2, 3
  EXPECT_TRUE(std::find(partitions.begin(), partitions.end(), 1) != partitions.end());
  EXPECT_TRUE(std::find(partitions.begin(), partitions.end(), 2) != partitions.end());
  EXPECT_TRUE(std::find(partitions.begin(), partitions.end(), 3) != partitions.end());
}

// =============================================================================
// PartitionStrategy 测试
// =============================================================================

TEST(HashPartitionStrategyTest, BasicHash) {
  HashPartitionStrategy strategy;
  
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now());
  CedarKey key2 = CedarKey::Vertex(101, 0, Timestamp::Now());
  
  PartitionID p1 = strategy.ComputePartition(key1, 256);
  PartitionID p2 = strategy.ComputePartition(key2, 256);
  
  // 结果应该在有效范围内
  EXPECT_LT(p1, 256);
  EXPECT_LT(p2, 256);
  
  // 相同Key应该得到相同分区
  PartitionID p1_again = strategy.ComputePartition(key1, 256);
  EXPECT_EQ(p1, p1_again);
}

TEST(RangePartitionStrategyTest, RangeDistribution) {
  RangePartitionStrategy strategy;
  
  // 创建不同范围的Key
  CedarKey key1 = CedarKey::Vertex(0, 0, Timestamp::Now());
  CedarKey key2 = CedarKey::Vertex(std::numeric_limits<uint64_t>::max() / 2, 0, Timestamp::Now());
  CedarKey key3 = CedarKey::Vertex(std::numeric_limits<uint64_t>::max(), 0, Timestamp::Now());
  
  PartitionID p1 = strategy.ComputePartition(key1, 4);
  PartitionID p2 = strategy.ComputePartition(key2, 4);
  PartitionID p3 = strategy.ComputePartition(key3, 4);
  
  // 所有分区都在有效范围内
  EXPECT_LT(p1, 4);
  EXPECT_LT(p2, 4);
  EXPECT_LT(p3, 4);
  
  // 第一个分区应该是0
  EXPECT_EQ(p1, 0);
  // 最后一个分区应该是最大分区索引（可能是3或4，取决于整数除法）
  EXPECT_GE(p3, 2);
  
  // 分区应该是单调递增的
  EXPECT_LE(p1, p2);
  EXPECT_LE(p2, p3);
}

// =============================================================================
// PartitionManager 测试
// =============================================================================

TEST(PartitionManagerTest, Initialize) {
  DTxConfig config;
  PartitionManager manager(config);
  
  auto status = manager.Initialize(256, std::make_unique<HashPartitionStrategy>());
  EXPECT_TRUE(status.ok());
  
  auto partitions = manager.GetAllPartitions();
  EXPECT_EQ(partitions.size(), 256);
}

TEST(PartitionManagerTest, InvalidPartitionCount) {
  DTxConfig config;
  PartitionManager manager(config);
  
  // 0个分区应该失败
  auto status = manager.Initialize(0, std::make_unique<HashPartitionStrategy>());
  EXPECT_FALSE(status.ok());
}

TEST(PartitionManagerTest, GetPartitionMeta) {
  DTxConfig config;
  PartitionManager manager(config);
  manager.Initialize(10, std::make_unique<HashPartitionStrategy>());
  
  auto meta = manager.GetPartitionMeta(5);
  EXPECT_NE(meta, nullptr);
  EXPECT_EQ(meta->partition_id, 5);
  
  // 无效分区
  auto invalid = manager.GetPartitionMeta(100);
  EXPECT_EQ(invalid, nullptr);
}

TEST(PartitionManagerTest, SetAndGetPartitionLeader) {
  DTxConfig config;
  PartitionManager manager(config);
  manager.Initialize(10, std::make_unique<HashPartitionStrategy>());
  
  NodeID leader = 42;
  auto status = manager.SetPartitionLeader(5, leader);
  EXPECT_TRUE(status.ok());
  
  NodeID retrieved = manager.GetPartitionLeader(5);
  EXPECT_EQ(retrieved, leader);
}

TEST(PartitionManagerTest, GetPartitionsForKeys) {
  DTxConfig config;
  PartitionManager manager(config);
  manager.Initialize(256, std::make_unique<HashPartitionStrategy>());
  
  std::vector<CedarKey> keys = {
    CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1),
    CedarKey::Vertex(200, 0, Timestamp::Now(), 0, 2),
    CedarKey::Vertex(300, 0, Timestamp::Now(), 0, 3)
  };
  
  auto partitions = manager.GetPartitionsForKeys(keys);
  EXPECT_EQ(partitions.size(), 3);
  
  // 应该能识别出需要协调
  EXPECT_TRUE(manager.NeedsCoordination(keys));
}

TEST(PartitionManagerTest, SinglePartitionNoCoordination) {
  DTxConfig config;
  PartitionManager manager(config);
  manager.Initialize(256, std::make_unique<HashPartitionStrategy>());
  
  // 所有Key在同一分区
  std::vector<CedarKey> keys = {
    CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 5),
    CedarKey::Vertex(101, 0, Timestamp::Now(), 0, 5),
    CedarKey::Vertex(102, 0, Timestamp::Now(), 0, 5)
  };
  
  EXPECT_FALSE(manager.NeedsCoordination(keys));
}

// =============================================================================
// CedarKeyPartitionHelper 测试
// =============================================================================

TEST(CedarKeyPartitionHelperTest, GetPartitionID) {
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 42);
  
  PartitionID pid = CedarKeyPartitionHelper::GetPartitionID(key);
  EXPECT_EQ(pid, 42);
}

TEST(CedarKeyPartitionHelperTest, SetPartitionID) {
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  EXPECT_EQ(key.part_id(), 0);  // 默认分区0
  
  CedarKey modified = CedarKeyPartitionHelper::SetPartitionID(key, 100);
  EXPECT_EQ(modified.part_id(), 100);
}

TEST(CedarKeyPartitionHelperTest, BelongsToPartition) {
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 42);
  
  EXPECT_TRUE(CedarKeyPartitionHelper::BelongsToPartition(key, 42));
  EXPECT_FALSE(CedarKeyPartitionHelper::BelongsToPartition(key, 100));
}

TEST(CedarKeyPartitionHelperTest, SetPartitionIDs) {
  std::vector<CedarKey> keys = {
    CedarKey::Vertex(100, 0, Timestamp::Now()),
    CedarKey::Vertex(101, 0, Timestamp::Now()),
    CedarKey::Vertex(102, 0, Timestamp::Now())
  };
  
  auto modified = CedarKeyPartitionHelper::SetPartitionIDs(keys, 99);
  
  EXPECT_EQ(modified.size(), 3);
  for (const auto& key : modified) {
    EXPECT_EQ(key.part_id(), 99);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
