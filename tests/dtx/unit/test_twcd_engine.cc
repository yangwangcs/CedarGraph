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

#include "cedar/dtx/twcd_engine.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;
using namespace cedar::dtx;

// =============================================================================
// TemporalWindowIntervalTree 测试
// =============================================================================

TEST(IntervalTreeTest, BasicInsertAndQuery) {
  TemporalWindowIntervalTree tree;
  
  // 插入几个窗口
  tree.Insert(TemporalWindow(100, 200), 1);
  tree.Insert(TemporalWindow(150, 250), 2);
  tree.Insert(TemporalWindow(300, 400), 3);
  
  // 查询与 [120, 180] 重叠的事务
  auto result = tree.QueryOverlapping(TemporalWindow(120, 180));
  EXPECT_EQ(result.size(), 2);  // 应该包含事务1和2
  EXPECT_TRUE(result.count(1));
  EXPECT_TRUE(result.count(2));
  EXPECT_FALSE(result.count(3));
}

TEST(IntervalTreeTest, NoOverlapQuery) {
  TemporalWindowIntervalTree tree;
  
  tree.Insert(TemporalWindow(100, 200), 1);
  tree.Insert(TemporalWindow(300, 400), 2);
  
  // 查询不重叠的窗口
  auto result = tree.QueryOverlapping(TemporalWindow(500, 600));
  EXPECT_TRUE(result.empty());
}

TEST(IntervalTreeTest, RemoveWindow) {
  TemporalWindowIntervalTree tree;
  
  tree.Insert(TemporalWindow(100, 200), 1);
  tree.Insert(TemporalWindow(150, 250), 2);
  
  // 移除事务1
  tree.Remove(TemporalWindow(100, 200), 1);
  
  auto result = tree.QueryOverlapping(TemporalWindow(120, 180));
  EXPECT_EQ(result.size(), 1);
  EXPECT_TRUE(result.count(2));
  EXPECT_FALSE(result.count(1));
}

TEST(IntervalTreeTest, UnboundedWindow) {
  TemporalWindowIntervalTree tree;
  
  // 插入无限窗口
  tree.Insert(TemporalWindow(100, 0), 1);  // [100, inf]
  tree.Insert(TemporalWindow(200, 300), 2);
  
  // 查询应该能找到无限窗口
  auto result = tree.QueryOverlapping(TemporalWindow(500, 600));
  EXPECT_EQ(result.size(), 1);
  EXPECT_TRUE(result.count(1));
}

TEST(IntervalTreeTest, SizeTracking) {
  TemporalWindowIntervalTree tree;
  
  EXPECT_EQ(tree.Size(), 0);
  
  tree.Insert(TemporalWindow(100, 200), 1);
  EXPECT_EQ(tree.Size(), 1);
  
  tree.Insert(TemporalWindow(300, 400), 2);
  EXPECT_EQ(tree.Size(), 2);
  
  tree.Remove(TemporalWindow(100, 200), 1);
  EXPECT_EQ(tree.Size(), 1);
}

TEST(IntervalTreeTest, Clear) {
  TemporalWindowIntervalTree tree;
  
  tree.Insert(TemporalWindow(100, 200), 1);
  tree.Insert(TemporalWindow(300, 400), 2);
  
  tree.Clear();
  
  EXPECT_EQ(tree.Size(), 0);
  auto result = tree.QueryOverlapping(TemporalWindow(0, 1000));
  EXPECT_TRUE(result.empty());
}

// =============================================================================
// TwcdEngine 基础测试
// =============================================================================

TEST(TwcdEngineTest, RegisterAndUnregisterWindow) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  TxnID txn1 = 1;
  TemporalWindow window1(100, 200);
  
  // 注册窗口
  auto status = engine.RegisterWindow(txn1, window1);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(engine.GetActiveWindowCount(), 1);
  
  // 重复注册应该失败
  status = engine.RegisterWindow(txn1, window1);
  EXPECT_FALSE(status.ok());
  
  // 注销窗口
  engine.UnregisterWindow(txn1);
  EXPECT_EQ(engine.GetActiveWindowCount(), 0);
}

TEST(TwcdEngineTest, UpdateWindow) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  TxnID txn1 = 1;
  engine.RegisterWindow(txn1, TemporalWindow(100, 200));
  
  // 更新窗口
  auto status = engine.UpdateWindow(txn1, TemporalWindow(100, 300));
  EXPECT_TRUE(status.ok());
  
  // 检查更新后的窗口
  auto overlapping = engine.GetOverlappingTxns(TemporalWindow(250, 280));
  EXPECT_EQ(overlapping.size(), 1);
  EXPECT_TRUE(overlapping.count(txn1));
}

TEST(TwcdEngineTest, NoConflictDifferentTime) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  // 事务1: [100, 200]
  engine.RegisterWindow(1, TemporalWindow(100, 200));
  
  // 事务2: [300, 400] - 时间不重叠，应该无冲突
  auto result = engine.CheckConflict(2, TemporalWindow(300, 400), {}, {});
  
  EXPECT_TRUE(result.Ok());
  EXPECT_FALSE(result.has_conflict);
}

TEST(TwcdEngineTest, ConflictTimeOverlap) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  // 事务1: [100, 200]
  engine.RegisterWindow(1, TemporalWindow(100, 200));
  
  // 事务2: [150, 250] - 时间重叠
  // 注意：CheckConflict需要自己注册，但不会检查自己冲突
  engine.RegisterWindow(2, TemporalWindow(150, 250));
  
  // 检查事务2是否与事务1冲突
  // 这里我们不写write_set，所以不应该有Key级冲突
  auto result = engine.CheckConflict(2, TemporalWindow(150, 250), {}, {});
  
  // 时间重叠但无Key级冲突，应该返回无冲突
  EXPECT_TRUE(result.Ok());
}

// =============================================================================
// TW-CD 读写冲突检测测试
// =============================================================================

TEST(TwcdEngineTest, ReadWriteConflict) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  // 创建Key
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  
  // 事务1: 写操作 [100, 200]
  engine.RegisterWindow(1, TemporalWindow(100, 200));
  engine.RegisterWriteSet(1, {key1});
  
  // 事务2: 读操作 [150, 250]，读取同一个Key
  engine.RegisterWindow(2, TemporalWindow(150, 250));
  
  auto result = engine.CheckConflict(2, TemporalWindow(150, 250), {key1}, {});
  
  EXPECT_TRUE(result.has_conflict);
  EXPECT_EQ(result.type, ConflictCheckResult::Type::kReadWrite);
  EXPECT_EQ(result.conflict_keys.size(), 1);
  EXPECT_EQ(result.conflict_keys[0].entity_id(), key1.entity_id());
}

TEST(TwcdEngineTest, WriteWriteConflict) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  
  // 事务1: 写操作 [100, 200]
  engine.RegisterWindow(1, TemporalWindow(100, 200));
  engine.RegisterWriteSet(1, {key1});
  
  // 事务2: 也写同一个Key [150, 250]
  engine.RegisterWindow(2, TemporalWindow(150, 250));
  engine.RegisterWriteSet(2, {key1});
  
  auto result = engine.CheckConflict(2, TemporalWindow(150, 250), {}, {key1});
  
  EXPECT_TRUE(result.has_conflict);
  EXPECT_EQ(result.type, ConflictCheckResult::Type::kWriteWrite);
}

TEST(TwcdEngineTest, NoConflictSameKeyDifferentTime) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  
  // 事务1: 写操作 [100, 200]
  engine.RegisterWindow(1, TemporalWindow(100, 200));
  engine.RegisterWriteSet(1, {key1});
  
  // 事务2: 读同一个Key，但时间完全不重叠 [300, 400]
  engine.RegisterWindow(2, TemporalWindow(300, 400));
  
  auto result = engine.CheckConflict(2, TemporalWindow(300, 400), {key1}, {});
  
  // 时间解耦 - 无冲突
  EXPECT_FALSE(result.has_conflict);
}

TEST(TwcdEngineTest, MultipleConflicts) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  CedarKey key2 = CedarKey::Vertex(200, 0, Timestamp::Now(), 0, 1);
  CedarKey key3 = CedarKey::Vertex(300, 0, Timestamp::Now(), 0, 1);
  
  // 事务1: 写key1和key2
  engine.RegisterWindow(1, TemporalWindow(100, 200));
  engine.RegisterWriteSet(1, {key1, key2});
  
  // 事务2: 读key1, key2, key3
  engine.RegisterWindow(2, TemporalWindow(150, 250));
  
  auto result = engine.CheckConflict(2, TemporalWindow(150, 250), {key1, key2, key3}, {});
  
  EXPECT_TRUE(result.has_conflict);
  // 应该检测到key1和key2两个冲突
  EXPECT_EQ(result.conflict_keys.size(), 2);
}

// =============================================================================
// TW-CD 统计测试
// =============================================================================

TEST(TwcdEngineTest, StatsTracking) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  // 重置统计
  engine.ResetStats();
  
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  
  // 事务1: 写操作
  engine.RegisterWindow(1, TemporalWindow(100, 200));
  engine.RegisterWriteSet(1, {key1});
  
  // 多次检查
  for (int i = 0; i < 10; ++i) {
    TxnID txn = 100 + i;
    engine.RegisterWindow(txn, TemporalWindow(150, 250));
    engine.CheckConflict(txn, TemporalWindow(150, 250), {key1}, {});
  }
  
  auto stats = engine.GetStats();
  EXPECT_EQ(stats.total_checks, 10);
  EXPECT_EQ(stats.conflict_detected, 10);  // 都应该检测到冲突
  EXPECT_EQ(stats.active_txns, 11);  // 1 + 10
}

// =============================================================================
// TW-CD 核心优势验证
// =============================================================================

TEST(TwcdEngineAdvantageTest, TemporalDecoupling) {
  // 验证核心优势：时序解耦减少冲突
  
  DTxConfig config;
  TwcdEngine engine(config);
  
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  
  // 场景：传统OCC会冲突，但TW-CD因为时间不重叠不会冲突
  
  // T1: 读取key1在时间100
  engine.RegisterWindow(1, TemporalWindow(100, 100));
  
  // T2: 更新key1在时间200（不重叠）
  engine.RegisterWindow(2, TemporalWindow(200, 200));
  engine.RegisterWriteSet(2, {key1});
  
  // T3: 检查读取key1在时间100，即使T2写了同一个key
  auto result = engine.CheckConflict(3, TemporalWindow(100, 100), {key1}, {});
  
  // 应该无冲突，因为时间窗口不重叠
  EXPECT_FALSE(result.has_conflict);
  
  // 对比：如果T1读取的是时间范围[150, 250]，则会冲突
  auto result2 = engine.CheckConflict(4, TemporalWindow(150, 250), {key1}, {});
  EXPECT_TRUE(result2.has_conflict);
}

TEST(TwcdEngineAdvantageTest, OverlappingVsNonOverlapping) {
  DTxConfig config;
  TwcdEngine engine(config);
  
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now(), 0, 1);
  
  // T1: 长时间事务 [0, 1000]
  engine.RegisterWindow(1, TemporalWindow(0, 1000));
  engine.RegisterWriteSet(1, {key});
  
  // T2: 短时间事务 [100, 200] - 完全包含在T1内，应该冲突
  engine.RegisterWindow(2, TemporalWindow(100, 200));
  auto result1 = engine.CheckConflict(2, TemporalWindow(100, 200), {key}, {});
  EXPECT_TRUE(result1.has_conflict);
  
  // T3: 时间在T1之后 [1001, 2000] - 应该无冲突
  engine.RegisterWindow(3, TemporalWindow(1001, 2000));
  auto result2 = engine.CheckConflict(3, TemporalWindow(1001, 2000), {key}, {});
  EXPECT_FALSE(result2.has_conflict);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
