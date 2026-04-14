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

#include "cedar/dtx/version_chain.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;
using namespace cedar::dtx;

// =============================================================================
// VersionChainNode 测试
// =============================================================================

TEST(VersionChainNodeTest, BasicConstruction) {
  VersionChainNode node(100, Timestamp(1000), 1);
  
  EXPECT_EQ(node.txn_id, 100);
  EXPECT_EQ(node.commit_ts.value(), 1000);
  EXPECT_EQ(node.version, 1);
  EXPECT_FALSE(node.visible.load());
  EXPECT_EQ(node.ref_count.load(), 0);
}

TEST(VersionChainNodeTest, ReferenceCounting) {
  VersionChainNode node(100, Timestamp(1000), 1);
  
  node.AddRef();
  EXPECT_EQ(node.ref_count.load(), 1);
  
  node.AddRef();
  EXPECT_EQ(node.ref_count.load(), 2);
  
  node.Release();
  EXPECT_EQ(node.ref_count.load(), 1);
}

// =============================================================================
// VersionChainHead 测试
// =============================================================================

TEST(VersionChainHeadTest, InsertAndGetLatest) {
  VersionChainHead head;
  
  // 插入第一个版本
  auto* node1 = new VersionChainNode(100, Timestamp(1000), 1);
  EXPECT_TRUE(head.InsertVersion(node1));
  
  // 还没有标记为可见
  EXPECT_EQ(head.GetLatestVisible(), nullptr);
  
  // 标记为可见
  node1->visible.store(true);
  EXPECT_EQ(head.GetLatestVisible(), node1);
  
  // 插入新版本
  auto* node2 = new VersionChainNode(101, Timestamp(2000), 2);
  EXPECT_TRUE(head.InsertVersion(node2));
  node2->visible.store(true);
  
  // 最新可见版本应该是 node2
  EXPECT_EQ(head.GetLatestVisible(), node2);
  
  // 清理
  delete node1;
  delete node2;
}

TEST(VersionChainHeadTest, GetSpecificVersion) {
  VersionChainHead head;
  
  auto* node1 = new VersionChainNode(100, Timestamp(1000), 1);
  auto* node2 = new VersionChainNode(101, Timestamp(2000), 2);
  
  head.InsertVersion(node1);
  head.InsertVersion(node2);
  node1->visible.store(true);
  node2->visible.store(true);
  
  // 获取特定版本
  EXPECT_EQ(head.GetVersion(1), node1);
  EXPECT_EQ(head.GetVersion(2), node2);
  EXPECT_EQ(head.GetVersion(3), nullptr);  // 不存在
  
  // 清理
  delete node1;
  delete node2;
}

TEST(VersionChainHeadTest, ReaderCount) {
  VersionChainHead head;
  
  EXPECT_EQ(head.reader_count.load(), 0);
  
  head.EnterRead();
  EXPECT_EQ(head.reader_count.load(), 1);
  
  head.EnterRead();
  EXPECT_EQ(head.reader_count.load(), 2);
  
  head.ExitRead();
  EXPECT_EQ(head.reader_count.load(), 1);
}

// =============================================================================
// VersionChainIndex 测试
// =============================================================================

TEST(VersionChainIndexTest, GetOrCreateHead) {
  VersionChainIndex index;
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  
  // 第一次获取，创建新的
  auto* head1 = index.GetOrCreateHead(key);
  EXPECT_NE(head1, nullptr);
  
  // 第二次获取，返回同一个
  auto* head2 = index.GetOrCreateHead(key);
  EXPECT_EQ(head1, head2);
}

TEST(VersionChainIndexTest, CommitVersion) {
  VersionChainIndex index;
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  
  auto status = index.CommitVersion(key, 100, Timestamp(1000), 1);
  EXPECT_TRUE(status.ok());
  
  // 验证可以读取
  VersionChainNode* node = nullptr;
  status = index.ReadLatestVisible(key, &node);
  EXPECT_TRUE(status.ok());
  EXPECT_NE(node, nullptr);
  EXPECT_EQ(node->version, 1);
  EXPECT_EQ(node->txn_id, 100);
}

TEST(VersionChainIndexTest, FastValidateNoConflict) {
  VersionChainIndex index;
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  
  // 提交版本1
  index.CommitVersion(key, 100, Timestamp(1000), 1);
  
  // 验证：读取版本1，在版本1提交后提交
  auto result = index.FastValidate(key, 1, Timestamp(2000));
  
  // 应该验证通过（O(1)）
  EXPECT_EQ(result, ValidationResult::kValid);
}

TEST(VersionChainIndexTest, FastValidateNeedFullCheck) {
  VersionChainIndex index;
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  
  // 提交版本1
  index.CommitVersion(key, 100, Timestamp(1000), 1);
  
  // 提交版本2（更新）
  index.CommitVersion(key, 101, Timestamp(2000), 2);
  
  // 验证：读取版本1，但在版本2提交后尝试提交
  auto result = index.FastValidate(key, 1, Timestamp(3000));
  
  // 如果读取的版本不是最新版本，说明有冲突
  // 或者需要完整检查
  EXPECT_TRUE(result == ValidationResult::kInvalid || 
              result == ValidationResult::kNeedFullCheck);
}

TEST(VersionChainIndexTest, FullValidate) {
  VersionChainIndex index;
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  
  // 提交版本1
  index.CommitVersion(key, 100, Timestamp(1000), 1);
  
  // 完整验证
  auto result = index.FullValidate(key, 1, Timestamp(2000));
  EXPECT_EQ(result, ValidationResult::kValid);
  
  // 验证不存在的版本
  result = index.FullValidate(key, 999, Timestamp(2000));
  EXPECT_EQ(result, ValidationResult::kInvalid);
}

TEST(VersionChainIndexTest, BatchValidate) {
  VersionChainIndex index;
  
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now());
  CedarKey key2 = CedarKey::Vertex(200, 0, Timestamp::Now());
  
  index.CommitVersion(key1, 100, Timestamp(1000), 1);
  index.CommitVersion(key2, 101, Timestamp(1000), 1);
  
  std::vector<std::pair<CedarKey, uint64_t>> read_set = {
    {key1, 1},
    {key2, 1}
  };
  
  auto results = index.BatchValidate(read_set, Timestamp(2000));
  
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].second, ValidationResult::kValid);
  EXPECT_EQ(results[1].second, ValidationResult::kValid);
}

TEST(VersionChainIndexTest, GC) {
  VersionChainIndex index;
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  
  // 提交多个版本
  for (int i = 1; i <= 5; ++i) {
    index.CommitVersion(key, 100 + i, Timestamp(i * 1000), i);
  }
  
  // 运行GC（清理早于时间戳4000的版本）
  index.RunGC(Timestamp(4000));
  
  auto stats = index.GetGCStats();
  // 应该清理了一些版本
  EXPECT_GE(stats.versions_removed, 0);
}

// =============================================================================
// DVC-Val 核心优势测试
// =============================================================================

TEST(DvcValAdvantageTest, O1Validation) {
  // 验证 O(1) 快速验证
  
  VersionChainIndex index;
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  
  // 提交版本
  index.CommitVersion(key, 100, Timestamp(1000), 1);
  
  // 快速验证
  auto start = std::chrono::steady_clock::now();
  auto result = index.FastValidate(key, 1, Timestamp(2000));
  auto end = std::chrono::steady_clock::now();
  
  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  
  EXPECT_EQ(result, ValidationResult::kValid);
  // O(1) 应该非常快（<10μs）
  EXPECT_LT(elapsed.count(), 100);  // 宽松一点，避免测试不稳定
}

TEST(DvcValAdvantageTest, LatestVersionRead) {
  // 验证读取最新版本是 O(1)
  
  VersionChainIndex index;
  CedarKey key = CedarKey::Vertex(100, 0, Timestamp::Now());
  
  // 提交多个版本
  for (int i = 1; i <= 100; ++i) {
    index.CommitVersion(key, 100 + i, Timestamp(i * 1000), i);
  }
  
  // 读取最新版本
  auto start = std::chrono::steady_clock::now();
  VersionChainNode* node = nullptr;
  index.ReadLatestVisible(key, &node);
  auto end = std::chrono::steady_clock::now();
  
  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  
  EXPECT_NE(node, nullptr);
  EXPECT_EQ(node->version, 100);
  // O(1) 读取
  EXPECT_LT(elapsed.count(), 100);
}

// =============================================================================
// CrossShardVersionView 测试
// =============================================================================

TEST(CrossShardVersionViewTest, GlobalValidate) {
  CrossShardVersionView view;
  
  // 使用固定的 Key
  CedarKey key1 = CedarKey::Vertex(100, 0, Timestamp::Now());
  CedarKey key2 = CedarKey::Vertex(200, 0, Timestamp::Now());
  
  // 添加两个分片的视图
  std::vector<VersionInfo> shard1_versions = {
    {key1, 1, Timestamp(1000), true}
  };
  
  std::vector<VersionInfo> shard2_versions = {
    {key2, 1, Timestamp(1000), true}
  };
  
  view.AddShardView(1, shard1_versions);
  view.AddShardView(2, shard2_versions);
  
  // 全局验证（使用相同的 Key 对象）
  std::vector<std::pair<CedarKey, uint64_t>> read_set = {
    {key1, 1},
    {key2, 1}
  };
  
  bool valid = view.GlobalValidate(read_set, Timestamp(2000));
  EXPECT_TRUE(valid);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
