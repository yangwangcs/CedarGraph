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

#include "cedar/dtx/temporal_window.h"
#include "cedar/dtx/types.h"

using namespace cedar;
using namespace cedar::dtx;

// =============================================================================
// TemporalWindow 基础测试
// =============================================================================

TEST(TemporalWindowTest, DefaultConstruction) {
  TemporalWindow w;
  EXPECT_EQ(w.start.value(), 0);
  EXPECT_EQ(w.end.value(), 0);
  EXPECT_TRUE(w.IsEmpty());
}

TEST(TemporalWindowTest, RangeConstruction) {
  TemporalWindow w(Timestamp(100), Timestamp(200));
  EXPECT_EQ(w.start.value(), 100);
  EXPECT_EQ(w.end.value(), 200);
  EXPECT_FALSE(w.IsEmpty());
  EXPECT_FALSE(w.IsPoint());
}

TEST(TemporalWindowTest, PointConstruction) {
  TemporalWindow w(Timestamp(100));
  EXPECT_EQ(w.start.value(), 100);
  EXPECT_EQ(w.end.value(), 100);
  EXPECT_TRUE(w.IsPoint());
}

TEST(TemporalWindowTest, Uint64Construction) {
  TemporalWindow w(100, 200);
  EXPECT_EQ(w.start.value(), 100);
  EXPECT_EQ(w.end.value(), 200);
}

// =============================================================================
// Overlap 测试
// =============================================================================

TEST(TemporalWindowTest, OverlapsFullyContained) {
  // [100, 200] 包含于 [50, 250]
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(50), Timestamp(250));
  EXPECT_TRUE(w1.Overlaps(w2));
  EXPECT_TRUE(w2.Overlaps(w1));
}

TEST(TemporalWindowTest, OverlapsPartialLeft) {
  // [100, 200] 与 [50, 150] 部分重叠
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(50), Timestamp(150));
  EXPECT_TRUE(w1.Overlaps(w2));
}

TEST(TemporalWindowTest, OverlapsPartialRight) {
  // [100, 200] 与 [150, 250] 部分重叠
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(150), Timestamp(250));
  EXPECT_TRUE(w1.Overlaps(w2));
}

TEST(TemporalWindowTest, NoOverlapLeft) {
  // [100, 200] 与 [0, 50] 不重叠
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(0), Timestamp(50));
  EXPECT_FALSE(w1.Overlaps(w2));
}

TEST(TemporalWindowTest, NoOverlapRight) {
  // [100, 200] 与 [250, 300] 不重叠
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(250), Timestamp(300));
  EXPECT_FALSE(w1.Overlaps(w2));
}

TEST(TemporalWindowTest, OverlapsAdjacent) {
  // [100, 200] 与 [200, 300] 在边界重叠（200是共有的）
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(200), Timestamp(300));
  EXPECT_TRUE(w1.Overlaps(w2));
}

TEST(TemporalWindowTest, OverlapsUnbounded) {
  // [100, 200] 与 [150, inf] 重叠
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(150), Timestamp(0));  // 0表示无限
  EXPECT_TRUE(w1.Overlaps(w2));
}

TEST(TemporalWindowTest, OverlapsBothUnbounded) {
  // [100, inf] 与 [50, inf] 重叠
  TemporalWindow w1(Timestamp(100), Timestamp(0));
  TemporalWindow w2(Timestamp(50), Timestamp(0));
  EXPECT_TRUE(w1.Overlaps(w2));
}

// =============================================================================
// Contains 测试
// =============================================================================

TEST(TemporalWindowTest, ContainsPoint) {
  TemporalWindow w(Timestamp(100), Timestamp(200));
  EXPECT_TRUE(w.Contains(Timestamp(100)));  // 边界
  EXPECT_TRUE(w.Contains(Timestamp(150)));  // 中间
  EXPECT_TRUE(w.Contains(Timestamp(200)));  // 边界
  EXPECT_FALSE(w.Contains(Timestamp(99)));  // 左侧外
  EXPECT_FALSE(w.Contains(Timestamp(201))); // 右侧外
}

TEST(TemporalWindowTest, ContainsUnbounded) {
  TemporalWindow w(Timestamp(100), Timestamp(0));  // [100, inf]
  EXPECT_TRUE(w.Contains(Timestamp(100)));
  EXPECT_TRUE(w.Contains(Timestamp(1000000)));
  EXPECT_FALSE(w.Contains(Timestamp(99)));
}

// =============================================================================
// Merge 测试
// =============================================================================

TEST(TemporalWindowTest, MergeAdjacent) {
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(200), Timestamp(300));
  w1.Merge(w2);
  EXPECT_EQ(w1.start.value(), 100);
  EXPECT_EQ(w1.end.value(), 300);
}

TEST(TemporalWindowTest, MergeOverlap) {
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(150), Timestamp(250));
  w1.Merge(w2);
  EXPECT_EQ(w1.start.value(), 100);
  EXPECT_EQ(w1.end.value(), 250);
}

TEST(TemporalWindowTest, MergeUnbounded) {
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(250), Timestamp(0));  // 无限
  w1.Merge(w2);
  EXPECT_EQ(w1.start.value(), 100);
  EXPECT_EQ(w1.end.value(), 0);  // 结果也是无限
}

// =============================================================================
// Intersect 测试
// =============================================================================

TEST(TemporalWindowTest, IntersectNormal) {
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(150), Timestamp(250));
  auto result = w1.Intersect(w2);
  EXPECT_EQ(result.start.value(), 150);
  EXPECT_EQ(result.end.value(), 200);
}

TEST(TemporalWindowTest, IntersectNoOverlap) {
  TemporalWindow w1(Timestamp(100), Timestamp(200));
  TemporalWindow w2(Timestamp(300), Timestamp(400));
  auto result = w1.Intersect(w2);
  EXPECT_EQ(result.start.value(), 0);
  EXPECT_EQ(result.end.value(), 0);  // 空窗口
  EXPECT_TRUE(result.IsEmpty());
}

// =============================================================================
// Span 测试
// =============================================================================

TEST(TemporalWindowTest, SpanCalculation) {
  TemporalWindow w(Timestamp(100), Timestamp(300));
  EXPECT_EQ(w.Span(), 200);
}

TEST(TemporalWindowTest, SpanPoint) {
  TemporalWindow w(Timestamp(100));
  EXPECT_EQ(w.Span(), 0);
}

TEST(TemporalWindowTest, SpanUnbounded) {
  TemporalWindow w(Timestamp(100), Timestamp(0));
  EXPECT_EQ(w.Span(), 0);  // 无限返回0
}

// =============================================================================
// 序列化测试
// =============================================================================

TEST(TemporalWindowTest, ToStringNormal) {
  TemporalWindow w(Timestamp(100), Timestamp(200));
  EXPECT_EQ(w.ToString(), "100:200");
}

TEST(TemporalWindowTest, ToStringUnbounded) {
  TemporalWindow w(Timestamp(100), Timestamp(0));
  EXPECT_EQ(w.ToString(), "100:inf");
}

TEST(TemporalWindowTest, FromStringNormal) {
  auto w = TemporalWindow::FromString("100:200");
  EXPECT_EQ(w.start.value(), 100);
  EXPECT_EQ(w.end.value(), 200);
}

TEST(TemporalWindowTest, FromStringUnbounded) {
  auto w = TemporalWindow::FromString("100:inf");
  EXPECT_EQ(w.start.value(), 100);
  EXPECT_EQ(w.end.value(), 0);
}

TEST(TemporalWindowTest, SerializeDeserialize) {
  TemporalWindow original(Timestamp(100), Timestamp(200));
  
  uint8_t buffer[TemporalWindow::kSerializedSize];
  original.SerializeTo(buffer);
  
  auto restored = TemporalWindow::DeserializeFrom(buffer);
  EXPECT_EQ(restored.start.value(), original.start.value());
  EXPECT_EQ(restored.end.value(), original.end.value());
}

// =============================================================================
// WindowMergeOptimizer 测试
// =============================================================================

TEST(WindowMergeOptimizerTest, MergeAdjacentWindows) {
  std::vector<TemporalWindow> windows = {
    TemporalWindow(Timestamp(100), Timestamp(200)),
    TemporalWindow(Timestamp(250), Timestamp(300)),  // gap=50
    TemporalWindow(Timestamp(350), Timestamp(400))   // gap=50
  };
  
  auto merged = WindowMergeOptimizer::MergeAdjacentWindows(windows, 100);
  EXPECT_EQ(merged.start.value(), 100);
  EXPECT_EQ(merged.end.value(), 400);
}

TEST(WindowMergeOptimizerTest, MergeWithGap) {
  std::vector<TemporalWindow> windows = {
    TemporalWindow(Timestamp(100), Timestamp(200)),
    TemporalWindow(Timestamp(500), Timestamp(600))  // gap=300
  };
  
  // 阈值小于gap，不应该全部合并
  auto merged = WindowMergeOptimizer::MergeAdjacentWindows(windows, 100);
  EXPECT_EQ(merged.start.value(), 100);
  EXPECT_EQ(merged.end.value(), 200);  // 只包含第一个窗口
}

TEST(WindowMergeOptimizerTest, SplitLargeWindow) {
  TemporalWindow w(Timestamp(0), Timestamp(1000));
  auto split = WindowMergeOptimizer::SplitLargeWindow(w, 300);
  
  // 应该分成 [0,300], [300,600], [600,900], [900,1000]
  EXPECT_EQ(split.size(), 4);
  EXPECT_EQ(split[0].start.value(), 0);
  EXPECT_EQ(split[0].end.value(), 300);
  EXPECT_EQ(split[1].start.value(), 300);
  EXPECT_EQ(split[1].end.value(), 600);
}

TEST(WindowMergeOptimizerTest, SplitNotNeeded) {
  TemporalWindow w(Timestamp(100), Timestamp(200));
  auto split = WindowMergeOptimizer::SplitLargeWindow(w, 300);
  
  EXPECT_EQ(split.size(), 1);
  EXPECT_EQ(split[0].start.value(), 100);
  EXPECT_EQ(split[0].end.value(), 200);
}

// =============================================================================
// TemporalLock 测试
// =============================================================================

TEST(TemporalLockTest, ConflictsWithWriteWrite) {
  TemporalLock lock1;
  lock1.txn_id = 1;
  lock1.window = TemporalWindow(Timestamp(100), Timestamp(200));
  lock1.type = LockType::kWrite;
  
  TemporalLock lock2;
  lock2.txn_id = 2;
  lock2.window = TemporalWindow(Timestamp(150), Timestamp(250));
  lock2.type = LockType::kWrite;
  
  EXPECT_TRUE(lock1.ConflictsWith(lock2));
}

TEST(TemporalLockTest, ConflictsWithWriteRead) {
  TemporalLock write_lock;
  write_lock.txn_id = 1;
  write_lock.window = TemporalWindow(Timestamp(100), Timestamp(200));
  write_lock.type = LockType::kWrite;
  
  TemporalLock read_lock;
  read_lock.txn_id = 2;
  read_lock.window = TemporalWindow(Timestamp(150), Timestamp(250));
  read_lock.type = LockType::kRead;
  
  EXPECT_TRUE(write_lock.ConflictsWith(read_lock));
}

TEST(TemporalLockTest, NoConflictReadRead) {
  TemporalLock lock1;
  lock1.txn_id = 1;
  lock1.window = TemporalWindow(Timestamp(100), Timestamp(200));
  lock1.type = LockType::kRead;
  
  TemporalLock lock2;
  lock2.txn_id = 2;
  lock2.window = TemporalWindow(Timestamp(150), Timestamp(250));
  lock2.type = LockType::kRead;
  
  EXPECT_FALSE(lock1.ConflictsWith(lock2));
}

TEST(TemporalLockTest, NoConflictDifferentTime) {
  TemporalLock lock1;
  lock1.txn_id = 1;
  lock1.window = TemporalWindow(Timestamp(100), Timestamp(200));
  lock1.type = LockType::kWrite;
  
  TemporalLock lock2;
  lock2.txn_id = 2;
  lock2.window = TemporalWindow(Timestamp(300), Timestamp(400));
  lock2.type = LockType::kWrite;
  
  // 时间不重叠，无冲突
  EXPECT_FALSE(lock1.ConflictsWith(lock2));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
