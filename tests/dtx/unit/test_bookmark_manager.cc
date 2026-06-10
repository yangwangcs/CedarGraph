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

#include "cedar/dtx/bookmark_manager.h"
#include "cedar/dtx/txn_context.h"

using namespace cedar;
using namespace cedar::dtx;

// =============================================================================
// BookmarkHlc 测试
// =============================================================================

TEST(BookmarkHlcTest, BasicComparison) {
  BookmarkHlc hlc1(1000, 0);
  BookmarkHlc hlc2(1000, 1);
  BookmarkHlc hlc3(1001, 0);
  
  EXPECT_TRUE(hlc1 < hlc2);
  EXPECT_TRUE(hlc2 < hlc3);
  EXPECT_TRUE(hlc1 < hlc3);
  
  EXPECT_FALSE(hlc2 < hlc1);
  EXPECT_FALSE(hlc3 < hlc2);
}

TEST(BookmarkHlcTest, HappensBefore) {
  BookmarkHlc hlc1(1000, 0);
  BookmarkHlc hlc2(1001, 0);
  
  EXPECT_TRUE(hlc1.HappensBefore(hlc2));
  EXPECT_FALSE(hlc2.HappensBefore(hlc1));
}

TEST(BookmarkHlcTest, Concurrent) {
  // 两个HLC如果无因果关系则是并发的
  // 实际上由于HLC的设计，任意两个HLC都有可比较的顺序
  // 所以这个测试主要验证相等的情况
  BookmarkHlc hlc1(1000, 0);
  BookmarkHlc hlc2(1000, 0);
  
  // 相等的HLC不是并发的（它们是同一个）
  EXPECT_FALSE(hlc1.IsConcurrentWith(hlc2));
  EXPECT_TRUE(hlc1 == hlc2);
  
  // 不同的HLC有明确的顺序，也不是并发的
  BookmarkHlc hlc3(1000, 1);
  EXPECT_FALSE(hlc1.IsConcurrentWith(hlc3));
}

TEST(BookmarkHlcTest, SerializeDeserialize) {
  BookmarkHlc original(123456789, 42);
  std::string str = original.ToString();
  
  auto restored = BookmarkHlc::FromString(str);
  
  EXPECT_EQ(restored.wall_time, original.wall_time);
  EXPECT_EQ(restored.logical, original.logical);
  EXPECT_EQ(restored, original);
}

TEST(BookmarkHlcTest, Now) {
  auto hlc1 = BookmarkHlc::Now();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  auto hlc2 = BookmarkHlc::Now();
  
  EXPECT_LE(hlc1.wall_time, hlc2.wall_time);
}

// =============================================================================
// DistributedBookmark 测试
// =============================================================================

TEST(DistributedBookmarkTest, BasicConstruction) {
  DistributedBookmark bm(1000, 100);
  
  EXPECT_EQ(bm.timestamp, 1000);
  EXPECT_EQ(bm.txn_id, 100);
  EXPECT_FALSE(bm.IsEmpty());
}

TEST(DistributedBookmarkTest, FromSimpleBookmark) {
  driver::Bookmark simple(1000, 100);
  DistributedBookmark bm(simple);
  
  EXPECT_EQ(bm.timestamp, 1000);
  EXPECT_EQ(bm.txn_id, 100);
}

TEST(DistributedBookmarkTest, ShardWatermark) {
  DistributedBookmark bm;
  
  bm.SetShardWatermark(1, 1000);
  bm.SetShardWatermark(2, 2000);
  
  EXPECT_EQ(bm.GetShardWatermark(1), 1000);
  EXPECT_EQ(bm.GetShardWatermark(2), 2000);
  EXPECT_EQ(bm.GetShardWatermark(3), 0);  // 不存在
  
  // 更新
  bm.SetShardWatermark(1, 1500);
  EXPECT_EQ(bm.GetShardWatermark(1), 1500);
}

TEST(DistributedBookmarkTest, HappensBefore) {
  DistributedBookmark bm1;
  bm1.timestamp = 1000;
  bm1.hlc = BookmarkHlc(1000, 0);
  
  DistributedBookmark bm2;
  bm2.timestamp = 2000;
  bm2.hlc = BookmarkHlc(2000, 0);
  
  EXPECT_TRUE(bm1.HappensBefore(bm2));
  EXPECT_FALSE(bm2.HappensBefore(bm1));
}

TEST(DistributedBookmarkTest, Merge) {
  DistributedBookmark bm1;
  bm1.timestamp = 1000;
  bm1.SetShardWatermark(1, 100);
  bm1.SetShardWatermark(2, 200);
  
  DistributedBookmark bm2;
  bm2.timestamp = 2000;  // 更新
  bm2.SetShardWatermark(2, 300);  // 更大
  bm2.SetShardWatermark(3, 400);  // 新分片
  
  auto merged = DistributedBookmark::Merge({bm1, bm2});
  
  EXPECT_EQ(merged.timestamp, 2000);  // 取最大
  EXPECT_EQ(merged.GetShardWatermark(1), 100);  // 保留
  EXPECT_EQ(merged.GetShardWatermark(2), 300);  // 取最大
  EXPECT_EQ(merged.GetShardWatermark(3), 400);  // 新添加
}

TEST(DistributedBookmarkTest, SerializeDeserialize) {
  DistributedBookmark original;
  original.timestamp = 123456789;
  original.txn_id = 987654321;
  original.hlc = BookmarkHlc(123456789, 42);
  original.SetShardWatermark(1, 1000);
  original.SetShardWatermark(2, 2000);
  
  std::string str = original.Serialize();
  auto restored_opt = DistributedBookmark::Deserialize(str);
  
  ASSERT_TRUE(restored_opt.has_value());
  auto restored = *restored_opt;
  
  EXPECT_EQ(restored.timestamp, original.timestamp);
  EXPECT_EQ(restored.txn_id, original.txn_id);
  EXPECT_EQ(restored.hlc, original.hlc);
  EXPECT_EQ(restored.GetShardWatermark(1), 1000);
  EXPECT_EQ(restored.GetShardWatermark(2), 2000);
}

TEST(DistributedBookmarkTest, DeserializeV2Format) {
  // 测试兼容 v2 格式
  driver::Bookmark simple(1000, 100);
  std::string v2_str = simple.ToString();  // "v2:1000:100"
  
  auto restored_opt = DistributedBookmark::Deserialize(v2_str);
  
  ASSERT_TRUE(restored_opt.has_value());
  EXPECT_EQ(restored_opt->timestamp, 1000);
  EXPECT_EQ(restored_opt->txn_id, 100);
}

// =============================================================================
// CausalConsistencyChecker 测试
// =============================================================================

TEST(CausalConsistencyCheckerTest, ReadYourWrites) {
  DistributedBookmark write_bm;
  write_bm.timestamp = 1000;
  write_bm.hlc = BookmarkHlc(1000, 0);
  
  DistributedBookmark read_bm;
  read_bm.timestamp = 2000;
  read_bm.hlc = BookmarkHlc(2000, 0);
  
  EXPECT_TRUE(CausalConsistencyChecker::CheckReadYourWrites(write_bm, read_bm));
  
  // 反向不应该满足
  EXPECT_FALSE(CausalConsistencyChecker::CheckReadYourWrites(read_bm, write_bm));
}

TEST(CausalConsistencyCheckerTest, MonotonicReads) {
  DistributedBookmark read1;
  read1.timestamp = 1000;
  read1.hlc = BookmarkHlc(1000, 0);
  
  DistributedBookmark read2;
  read2.timestamp = 2000;
  read2.hlc = BookmarkHlc(2000, 0);
  
  EXPECT_TRUE(CausalConsistencyChecker::CheckMonotonicReads(read1, read2));
  EXPECT_FALSE(CausalConsistencyChecker::CheckMonotonicReads(read2, read1));
}

TEST(CausalConsistencyCheckerTest, MonotonicWrites) {
  DistributedBookmark write1;
  write1.timestamp = 1000;
  write1.hlc = BookmarkHlc(1000, 0);
  
  DistributedBookmark write2;
  write2.timestamp = 2000;
  write2.hlc = BookmarkHlc(2000, 0);
  
  EXPECT_TRUE(CausalConsistencyChecker::CheckMonotonicWrites(write1, write2));
  EXPECT_FALSE(CausalConsistencyChecker::CheckMonotonicWrites(write2, write1));
}

TEST(CausalConsistencyCheckerTest, WritesFollowReads) {
  DistributedBookmark read_bm;
  read_bm.timestamp = 1000;
  read_bm.hlc = BookmarkHlc(1000, 0);
  
  DistributedBookmark write_bm;
  write_bm.timestamp = 2000;
  write_bm.hlc = BookmarkHlc(2000, 0);
  
  EXPECT_TRUE(CausalConsistencyChecker::CheckWritesFollowReads(read_bm, write_bm));
  EXPECT_FALSE(CausalConsistencyChecker::CheckWritesFollowReads(write_bm, read_bm));
}

// =============================================================================
// BookmarkManager 测试
// =============================================================================

TEST(BookmarkManagerTest, GetCurrentHLC) {
  BookmarkManager manager;
  
  auto hlc1 = manager.GetCurrentHLC();
  auto hlc2 = manager.GetCurrentHLC();
  
  // 逻辑计数应该增加
  EXPECT_EQ(hlc1.wall_time, hlc2.wall_time);
  EXPECT_EQ(hlc2.logical, hlc1.logical + 1);
}

TEST(BookmarkManagerTest, UpdateHLC) {
  BookmarkManager manager;
  
  auto local = manager.GetCurrentHLC();
  
  // 模拟收到远程 HLC
  BookmarkHlc remote(local.wall_time + 1000, 5);
  manager.UpdateHLC(remote);
  
  auto updated = manager.GetCurrentHLC();
  
  // 应该更新为更大的值
  EXPECT_GE(updated.wall_time, remote.wall_time);
}

TEST(BookmarkManagerTest, CreateBookmark) {
  BookmarkManager manager;
  
  DistributedTxnContext ctx;
  ctx.SetTxnID(100);
  ctx.SetCommitTimestamp(1000);
  
  // 设置一些水位
  manager.UpdateLocalWatermark(1, 100);
  manager.UpdateLocalWatermark(2, 200);
  
  auto bm = manager.CreateBookmark(ctx);
  
  EXPECT_EQ(bm.timestamp, 1000);
  EXPECT_EQ(bm.txn_id, 100);
  EXPECT_EQ(bm.GetShardWatermark(1), 100);
  EXPECT_EQ(bm.GetShardWatermark(2), 200);
}

TEST(BookmarkManagerTest, WatermarkManagement) {
  BookmarkManager manager;
  
  manager.UpdateLocalWatermark(1, 100);
  EXPECT_EQ(manager.GetWatermark(1), 100);
  
  // 更新更大的值
  manager.UpdateLocalWatermark(1, 200);
  EXPECT_EQ(manager.GetWatermark(1), 200);
  
  // 尝试更新更小的值（应该不生效）
  manager.UpdateLocalWatermark(1, 150);
  EXPECT_EQ(manager.GetWatermark(1), 200);  // 保持200
}

TEST(BookmarkManagerTest, GlobalMinWatermark) {
  BookmarkManager manager;
  
  manager.UpdateLocalWatermark(1, 100);
  manager.UpdateLocalWatermark(2, 200);
  manager.UpdateLocalWatermark(3, 50);
  
  EXPECT_EQ(manager.GetGlobalMinWatermark(), 50);
}

TEST(BookmarkManagerTest, SessionBookmark) {
  BookmarkManager manager;
  
  uint64_t session_id = 42;
  DistributedBookmark bm;
  bm.timestamp = 1000;
  
  manager.SetSessionBookmark(session_id, bm);
  
  auto retrieved = manager.GetSessionBookmark(session_id);
  EXPECT_EQ(retrieved.timestamp, 1000);
}

TEST(BookmarkManagerTest, UpdateSessionBookmark) {
  BookmarkManager manager;
  
  uint64_t session_id = 42;
  
  DistributedBookmark bm1;
  bm1.timestamp = 1000;
  bm1.SetShardWatermark(1, 100);
  
  DistributedBookmark bm2;
  bm2.timestamp = 2000;
  bm2.SetShardWatermark(1, 200);
  
  manager.SetSessionBookmark(session_id, bm1);
  manager.UpdateSessionBookmark(session_id, bm2);
  
  auto merged = manager.GetSessionBookmark(session_id);
  EXPECT_EQ(merged.timestamp, 2000);  // 取最大
  EXPECT_EQ(merged.GetShardWatermark(1), 200);  // 取最大
}

TEST(BookmarkManagerTest, Stats) {
  BookmarkManager manager;
  
  DistributedTxnContext ctx;
  ctx.SetTxnID(100);
  ctx.SetCommitTimestamp(1000);
  
  manager.CreateBookmark(ctx);
  manager.CreateBookmark(ctx);
  
  auto stats = manager.GetStats();
  EXPECT_EQ(stats.bookmarks_created, 2);
}

// =============================================================================
// BBCC 核心优势测试
// =============================================================================

TEST(BbccAdvantageTest, LightweightBookmarkPropagation) {
  // 验证 Bookmark 只有 32 字节基础 + 分片水位
  
  DistributedBookmark bm;
  bm.timestamp = 123456789;
  bm.txn_id = 987654321;
  bm.SetShardWatermark(1, 1000);
  bm.SetShardWatermark(2, 2000);
  
  std::string serialized = bm.Serialize();
  
  // 序列化后的大小应该很小（<200字节）
  EXPECT_LT(serialized.size(), 200);
  
  // 对比：如果使用完整的向量时钟，会大得多
}

TEST(BbccAdvantageTest, HLCOrdering) {
  // 验证 HLC 提供因果排序
  
  BookmarkManager manager1;  // 模拟节点1
  BookmarkManager manager2;  // 模拟节点2
  
  // 节点1创建 Bookmark
  DistributedTxnContext ctx1;
  ctx1.SetTxnID(100);
  ctx1.SetCommitTimestamp(1000);
  auto bm1 = manager1.CreateBookmark(ctx1);
  
  // 传播到节点2，更新HLC
  manager2.UpdateHLC(bm1.hlc);
  
  // 节点2创建 Bookmark
  DistributedTxnContext ctx2;
  ctx2.SetTxnID(101);
  ctx2.SetCommitTimestamp(2000);
  auto bm2 = manager2.CreateBookmark(ctx2);
  
  // 验证因果序
  EXPECT_TRUE(bm1.HappensBefore(bm2));
  EXPECT_FALSE(bm2.HappensBefore(bm1));
}


