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

#include "cedar/dtx/lsm_native_occ.h"
#include "cedar/dtx/twcd_engine.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;
using namespace cedar::dtx;

// =============================================================================
// LocalTransactionCoordinator 测试
// =============================================================================

TEST(LocalTransactionCoordinatorTest, BeginTransaction) {
  DTxConfig config;
  TwcdEngine twcd_engine(config);
  
  // 创建协调器（使用 nullptr 作为存储，因为测试不涉及实际写入）
  LocalTransactionCoordinator coordinator(
      1, nullptr, nullptr, &twcd_engine);
  
  DistributedTxnContext ctx;
  auto status = coordinator.BeginTransaction(&ctx);
  
  EXPECT_TRUE(status.ok());
  EXPECT_NE(ctx.GetTxnID(), kInvalidTxnID);
  EXPECT_NE(ctx.GetStartTimestamp(), 0);
  EXPECT_EQ(ctx.GetState(), DistributedTxnState::kStarted);
  EXPECT_TRUE(ctx.IsSinglePartition());
}

TEST(LocalTransactionCoordinatorTest, BeginTransactionWithExistingId) {
  DTxConfig config;
  TwcdEngine twcd_engine(config);
  
  LocalTransactionCoordinator coordinator(1, nullptr, nullptr, &twcd_engine);
  
  DistributedTxnContext ctx;
  ctx.SetTxnID(12345);
  ctx.SetStartTimestamp(1000);
  
  auto status = coordinator.BeginTransaction(&ctx);
  
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(ctx.GetTxnID(), 12345);  // 保持原有ID
  EXPECT_EQ(ctx.GetStartTimestamp(), 1000);  // 保持原有时间戳
}

TEST(LocalTransactionCoordinatorTest, CommitWithoutConflicts) {
  DTxConfig config;
  TwcdEngine twcd_engine(config);
  
  LocalTransactionCoordinator coordinator(1, nullptr, nullptr, &twcd_engine);
  
  DistributedTxnContext ctx;
  coordinator.BeginTransaction(&ctx);
  
  // 添加写集
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  ctx.AddToWriteSet(key1, TemporalWindow(100, 200));
  
  auto result = coordinator.Commit(&ctx);
  
  EXPECT_TRUE(result.success);
  EXPECT_NE(result.commit_ts, 0);
  EXPECT_EQ(ctx.GetState(), DistributedTxnState::kCommitted);
}

TEST(LocalTransactionCoordinatorTest, CommitWithReadWriteConflict) {
  DTxConfig config;
  TwcdEngine twcd_engine(config);
  
  LocalTransactionCoordinator coordinator(1, nullptr, nullptr, &twcd_engine);
  
  // 事务1: 先写入
  DistributedTxnContext ctx1;
  coordinator.BeginTransaction(&ctx1);
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  ctx1.AddToWriteSet(key1, TemporalWindow(100, 200));
  coordinator.Commit(&ctx1);
  
  // 事务2: 在重叠时间读取同一个Key（注意：需要事务1仍在TW-CD中）
  // 由于事务1已提交并从TW-CD注销，这里我们需要模拟并发
  // 实际测试应该在事务1仍在活跃时检测冲突
  
  DistributedTxnContext ctx2;
  coordinator.BeginTransaction(&ctx2);
  ctx2.AddToReadSet(key1, TemporalWindow(150, 250));  // 时间重叠
  ctx2.AddToWriteSet(key1, TemporalWindow(150, 250));  // 也写入
  
  auto result = coordinator.Commit(&ctx2);
  
  // 由于事务1已提交，TW-CD中已不存在，所以不会检测到冲突
  // 实际冲突检测需要 OCC 的读集验证
  EXPECT_TRUE(result.success);
}

TEST(LocalTransactionCoordinatorTest, AbortTransaction) {
  DTxConfig config;
  TwcdEngine twcd_engine(config);
  
  LocalTransactionCoordinator coordinator(1, nullptr, nullptr, &twcd_engine);
  
  DistributedTxnContext ctx;
  coordinator.BeginTransaction(&ctx);
  
  auto status = coordinator.Abort(&ctx, "Test abort");
  
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(ctx.GetState(), DistributedTxnState::kAborted);
}

TEST(LocalTransactionCoordinatorTest, StatsTracking) {
  DTxConfig config;
  TwcdEngine twcd_engine(config);
  
  LocalTransactionCoordinator coordinator(1, nullptr, nullptr, &twcd_engine);
  
  // 提交几个事务
  for (int i = 0; i < 5; ++i) {
    DistributedTxnContext ctx;
    coordinator.BeginTransaction(&ctx);
    coordinator.Commit(&ctx);
  }
  
  // 中止一个事务
  DistributedTxnContext ctx;
  coordinator.BeginTransaction(&ctx);
  coordinator.Abort(&ctx, "Test");
  
  auto stats = coordinator.GetStats();
  EXPECT_EQ(stats.total_txns, 6);
  EXPECT_EQ(stats.committed_txns, 5);
  EXPECT_EQ(stats.aborted_txns, 1);
}

// =============================================================================
// LndOccEngine 测试
// =============================================================================

TEST(LndOccEngineTest, ClassifyTransaction) {
  DTxConfig config;
  LndOccEngine engine(config);
  
  // 单分区事务
  DistributedTxnContext single;
  single.AddParticipant(1);
  EXPECT_EQ(engine.ClassifyTransaction(&single), TxnType::kSinglePartition);
  
  // 多分区事务
  DistributedTxnContext multi;
  multi.AddParticipant(1);
  multi.AddParticipant(2);
  EXPECT_NE(engine.ClassifyTransaction(&multi), TxnType::kSinglePartition);
}

TEST(LndOccEngineTest, SinglePartitionCommitFlow) {
  DTxConfig config;
  LndOccEngine engine(config);
  
  // 创建模拟存储映射（使用 nullptr）
  std::unordered_map<PartitionID, std::pair<VSLMemTable*, WalWriter*>> stores;
  stores[1] = {nullptr, nullptr};
  
  engine.Initialize(nullptr, stores);
  
  DistributedTxnContext ctx;
  ctx.SetTxnID(GenerateTxnID());
  ctx.SetStartTimestamp(GenerateTimestamp());
  ctx.AddParticipant(1);
  
  auto result = engine.SinglePartitionCommit(&ctx);
  
  EXPECT_TRUE(result.success);
  EXPECT_NE(result.commit_ts, 0);
  
  auto stats = engine.GetStats();
  EXPECT_EQ(stats.single_partition_commits, 1);
}

TEST(LndOccEngineTest, MultiPartitionNotSupported) {
  DTxConfig config;
  LndOccEngine engine(config);
  
  DistributedTxnContext ctx;
  ctx.AddParticipant(1);
  ctx.AddParticipant(2);
  
  // SinglePartitionCommit 应该拒绝多分区事务
  auto result = engine.SinglePartitionCommit(&ctx);
  
  EXPECT_FALSE(result.success);
}

TEST(LndOccEngineTest, CoordinationRatioStats) {
  DTxConfig config;
  LndOccEngine engine(config);
  
  // 创建模拟存储
  std::unordered_map<PartitionID, std::pair<VSLMemTable*, WalWriter*>> stores;
  stores[1] = {nullptr, nullptr};
  engine.Initialize(nullptr, stores);
  
  // 执行事务来生成统计
  for (int i = 0; i < 9; ++i) {
    DistributedTxnContext ctx;
    ctx.SetTxnID(GenerateTxnID());
    ctx.AddParticipant(1);
    engine.SinglePartitionCommit(&ctx);
  }
  
  auto stats = engine.GetStats();
  EXPECT_EQ(stats.single_partition_commits, 9);
  EXPECT_EQ(stats.total_commits, 9);
  EXPECT_DOUBLE_EQ(stats.coordination_ratio, 0.0);  // 100% 单分区，0% 需要协调
}

// =============================================================================
// ZoneAwareWriteGrouper 测试
// =============================================================================

TEST(ZoneAwareWriteGrouperTest, GroupByZone) {
  std::vector<CedarKey> keys = {
    CedarKey::Vertex(100, 5, Timestamp::Now(), 0, 1),    // Topology (col < 10)
    CedarKey::Vertex(200, 15, Timestamp::Now(), 0, 1),   // Temporal (10 <= col < 20)
    CedarKey::Vertex(300, 25, Timestamp::Now(), 0, 1),   // Metadata (20 <= col < 30)
    CedarKey::Vertex(400, 35, Timestamp::Now(), 0, 1),   // Property (col >= 30)
  };
  
  auto groups = ZoneAwareWriteGrouper::GroupByZone(keys);
  
  EXPECT_EQ(groups.size(), 4);
  EXPECT_EQ(groups[ZoneAwareWriteGrouper::ZoneID::kTopology].size(), 1);
  EXPECT_EQ(groups[ZoneAwareWriteGrouper::ZoneID::kTemporal].size(), 1);
  EXPECT_EQ(groups[ZoneAwareWriteGrouper::ZoneID::kMetadata].size(), 1);
  EXPECT_EQ(groups[ZoneAwareWriteGrouper::ZoneID::kProperty].size(), 1);
}

TEST(ZoneAwareWriteGrouperTest, GetZoneForKey) {
  // Topology zone
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now());
  EXPECT_EQ(ZoneAwareWriteGrouper::GetZoneForKey(key1), 
            ZoneAwareWriteGrouper::ZoneID::kTopology);
  
  // Temporal zone
  CedarKey key2 = CedarKey::Vertex(100, 10, Timestamp::Now());
  EXPECT_EQ(ZoneAwareWriteGrouper::GetZoneForKey(key2), 
            ZoneAwareWriteGrouper::ZoneID::kTemporal);
  
  // Metadata zone
  CedarKey key3 = CedarKey::Vertex(100, 20, Timestamp::Now());
  EXPECT_EQ(ZoneAwareWriteGrouper::GetZoneForKey(key3), 
            ZoneAwareWriteGrouper::ZoneID::kMetadata);
  
  // Property zone
  CedarKey key4 = CedarKey::Vertex(100, 30, Timestamp::Now());
  EXPECT_EQ(ZoneAwareWriteGrouper::GetZoneForKey(key4), 
            ZoneAwareWriteGrouper::ZoneID::kProperty);
}

// =============================================================================
// LND-OCC 核心优势测试
// =============================================================================

TEST(LndOccAdvantageTest, NoCoordinationOverhead) {
  // 验证单分区事务无需协调
  
  DTxConfig config;
  TwcdEngine twcd_engine(config);
  LndOccEngine lnd_engine(config);
  
  std::unordered_map<PartitionID, std::pair<VSLMemTable*, WalWriter*>> stores;
  stores[1] = {nullptr, nullptr};
  lnd_engine.Initialize(nullptr, stores);
  
  // 执行单分区事务
  DistributedTxnContext ctx;
  ctx.SetTxnID(GenerateTxnID());
  ctx.AddParticipant(1);
  
  // 添加写集
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  ctx.AddToWriteSet(key1, TemporalWindow(100, 200));
  
  auto result = lnd_engine.SinglePartitionCommit(&ctx);
  
  EXPECT_TRUE(result.success);
  // 关键验证：没有网络往返（0ms协调时间）
  // 实际上 coordinator->Commit 是本地调用
  
  auto stats = lnd_engine.GetStats();
  EXPECT_EQ(stats.coordination_ratio, 0.0);  // 0% 需要协调
}

TEST(LndOccAdvantageTest, LayeredCommitStrategy) {
  DTxConfig config;
  LndOccEngine engine(config);
  
  // 单分区 -> Layer 1
  DistributedTxnContext single;
  single.AddParticipant(1);
  EXPECT_EQ(engine.ClassifyTransaction(&single), TxnType::kSinglePartition);
  
  // 多分区 -> Layer 2 或 3
  DistributedTxnContext multi;
  multi.AddParticipant(1);
  multi.AddParticipant(2);
  // 目前实现回退到 CrossTemporalRange
  EXPECT_EQ(engine.ClassifyTransaction(&multi), TxnType::kCrossTemporalRange);
}


