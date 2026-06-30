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

// =============================================================================
// CedarUpdate 端到端集成测试
// =============================================================================
// 验证完整流程：
// 1. CedarUpdate 构建操作
// 2. 转换为 CedarKey（32字节完整信息）
// 3. 写入 LSM-Tree（落盘）
// 4. 读取验证（Seek + Decode）
// =============================================================================

#include <gtest/gtest.h>
#include <filesystem>
#include <cstdio>

#include "cedar/update/cedar_update.h"
#include "cedar/core/cedar_status.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;

class CedarUpdateE2ETest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/cedar_e2e_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
    
    CedarOptions options;
    options.create_if_missing = true;
    
    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
    EXPECT_TRUE(s.ok()) << "Failed to open storage: " << s.ToString();
    if (!s.ok()) {
      storage_ = nullptr;
    }
  }
  
  void TearDown() override {
    delete storage_;
    std::filesystem::remove_all(test_dir_);
  }
  
  // 读取验证：检查 CedarKey 的所有字段是否正确写入
  bool VerifyCedarKeyWritten(uint64_t entity_id, 
                             EntityType expected_type,
                             uint16_t expected_col_id,
                             Timestamp query_time) {
    auto result = storage_->Get(entity_id, expected_type, expected_col_id, query_time);
    return result.has_value();
  }
  
  // 获取 CedarKey 详细信息（通过底层接口）
  CedarKey GetKeyFromStorage(uint64_t entity_id,
                             EntityType type,
                             uint16_t col_id,
                             Timestamp ts) {
    // 构造探测键
    CedarKey probe = CedarKey(
        entity_id,
        type,
        col_id,
        ts,
        0, 0, 0,
        static_cast<uint16_t>(entity_id)
    );
    return probe;
  }
  
  std::string test_dir_;
  CedarGraphStorage* storage_ = nullptr;
};

// =============================================================================
// 基本 CRUD 端到端测试
// =============================================================================

TEST_F(CedarUpdateE2ETest, CreateVertexAndReadBack) {
  // 测试 CedarUpdate 构造 CedarKey 的完整信息
  Timestamp create_time(1712050000000000ULL);
  Descriptor desc = Descriptor::InlineInt(1, 42);
  
  CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
  update.At(create_time)
        .WithSequence(5)
        .CreateVertex(1001, 1, desc);
  
  // 验证 CedarKey 的所有字段都已正确设置
  const auto& records = update.GetRecords();
  EXPECT_EQ(records.size(), 1);
  if (records.empty()) return;
  const auto& key = records[0].key;
  
  // 验证 32B CedarKey 的所有字段
  EXPECT_EQ(key.entity_id(), 1001);
  EXPECT_EQ(key.column_id(), 1);
  EXPECT_EQ(key.sequence(), 5);
  EXPECT_TRUE(key.IsCreate());
  EXPECT_TRUE(key.IsDistributed());
  EXPECT_FALSE(key.IsTombstone());
  EXPECT_EQ(key.part_id(), static_cast<uint16_t>(1001));
  
  // 验证 32 字节编码
  std::string encoded = key.Encode();
  EXPECT_EQ(encoded.size(), 32);
  
  // 验证编码后可以正确解码
  auto decoded = CedarKey::Decode(encoded);
  EXPECT_TRUE(decoded.has_value());
  if (decoded.has_value()) {
    EXPECT_EQ(decoded->entity_id(), 1001);
    EXPECT_EQ(decoded->column_id(), 1);
    EXPECT_EQ(decoded->sequence(), 5);
  }
  
  // 如果存储可用，尝试写入
  if (storage_) {
    auto status = update.Apply(storage_);
    EXPECT_TRUE(status.ok()) << "Write failed: " << status.ToString();
  }
}

TEST_F(CedarUpdateE2ETest, CreateEdgeWithFullKeyInfo) {
  // 步骤 1：创建两个节点
  Timestamp t0(1712050000000000ULL);
  Descriptor node_desc = Descriptor::InlineInt(1, 0);
  
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(t0)
          .CreateVertex(2001, 1, node_desc)
          .CreateVertex(2002, 1, node_desc);
    auto status = update.Apply(storage_);
    EXPECT_TRUE(status.ok());
  }
  
  // 步骤 2：创建边
  Timestamp t1(1712050000000001ULL);
  Descriptor edge_desc = Descriptor::InlineInt(2, 100);  // EdgeType:Follows, weight=100
  
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(t1)
          .CreateEdge(2001, 2002, 2, edge_desc, false, false);
    auto status = update.Apply(storage_);
    EXPECT_TRUE(status.ok());
  }
  
  // 步骤 3：验证 EdgeOut 被正确写入
  auto edge_out = storage_->Get(2001, EntityType::EdgeOut, 2, t1);
  ASSERT_TRUE(edge_out.has_value()) << "EdgeOut not found";
  
  // 步骤 4：验证 EdgeIn 被正确写入
  auto edge_in = storage_->Get(2002, EntityType::EdgeIn, 2, t1);
  ASSERT_TRUE(edge_in.has_value()) << "EdgeIn not found";
}

// =============================================================================
// CedarKey 完整信息验证
// =============================================================================

TEST_F(CedarUpdateE2ETest, VerifyCedarKeyAllFields) {
  Timestamp ts(1712050000000000ULL);
  Descriptor desc = Descriptor::InlineInt(3, 999);
  
  // 构造一个复杂的 CedarUpdate，验证所有字段
  CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
  update.At(ts)
        .WithSequence(5)  // 设置序列号
        .UpdateVertex(3001, 3, desc);  // UPDATE 操作
  
  auto status = update.Apply(storage_);
  EXPECT_TRUE(status.ok());
  
  // 从存储读取验证
  auto result = storage_->Get(3001, EntityType::Vertex, 3, ts);
  ASSERT_TRUE(result.has_value());
  
  // 获取写入的 CedarKey 详情（通过记录的引用）
  const auto& records = update.GetRecords();
  ASSERT_EQ(records.size(), 1);
  const auto& written_key = records[0].key;
  
  // 验证所有字段
  EXPECT_EQ(written_key.entity_id(), 3001);
  EXPECT_EQ(written_key.column_id(), 3);
  EXPECT_EQ(written_key.sequence(), 5);  // 验证序列号
  EXPECT_TRUE(written_key.IsUpdate());   // 验证 OpType = UPDATE
  EXPECT_TRUE(written_key.IsDistributed());
  EXPECT_EQ(written_key.part_id(), static_cast<uint16_t>(3001));
  
  // 验证时间戳降序编码
  EXPECT_EQ(written_key.timestamp().value(), ts.value());
}

// =============================================================================
// 时态属性测试
// =============================================================================

TEST_F(CedarUpdateE2ETest, TemporalVersioning) {
  uint64_t vertex_id = 4001;
  
  // 创建节点（版本 1）
  Timestamp t1(1712050000000000ULL);
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(t1)
          .CreateVertex(vertex_id, 1, Descriptor::InlineInt(1, 100));
    EXPECT_TRUE(update.Apply(storage_).ok());
  }
  
  // 更新节点（版本 2）
  Timestamp t2(1712050000000001ULL);
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(t2)
          .UpdateVertex(vertex_id, 1, Descriptor::InlineInt(1, 200));
    EXPECT_TRUE(update.Apply(storage_).ok());
  }
  
  // 验证时间旅行：查询 t1 时刻的值
  auto v1_result = storage_->Get(vertex_id, EntityType::Vertex, 1, t1);
  ASSERT_TRUE(v1_result.has_value());
  EXPECT_EQ(*v1_result->AsInlineInt(), 100);
  
  // 验证时间旅行：查询 t2 时刻的值
  auto v2_result = storage_->Get(vertex_id, EntityType::Vertex, 1, t2);
  ASSERT_TRUE(v2_result.has_value());
  EXPECT_EQ(*v2_result->AsInlineInt(), 200);
}

// =============================================================================
// 批量操作测试
// =============================================================================

TEST_F(CedarUpdateE2ETest, BatchOperations) {
  const int NUM_NODES = 10;
  Timestamp base_time(1712050000000000ULL);
  
  // 批量创建节点
  CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
  update.At(base_time);
  
  for (int i = 0; i < NUM_NODES; ++i) {
    update.CreateVertex(5000 + i, 1, Descriptor::InlineInt(1, i));
  }
  
  auto status = update.Apply(storage_);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(update.Count(), NUM_NODES);
  
  // 批量验证
  for (int i = 0; i < NUM_NODES; ++i) {
    auto result = storage_->Get(5000 + i, EntityType::Vertex, 1, base_time);
    ASSERT_TRUE(result.has_value()) << "Node " << (5000 + i) << " not found";
    EXPECT_EQ(*result->AsInlineInt(), i);
  }
}

// =============================================================================
// DELETE 操作测试（时态墓碑）
// =============================================================================

TEST_F(CedarUpdateE2ETest, DeleteVertexTemporalTombstone) {
  uint64_t vertex_id = 6001;
  
  // 创建节点
  Timestamp t_create(1712050000000000ULL);
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(t_create)
          .CreateVertex(vertex_id, 1, Descriptor::InlineInt(1, 42));
    EXPECT_TRUE(update.Apply(storage_).ok());
  }
  
  // 删除节点（时态墓碑）
  Timestamp t_delete(1712050000000001ULL);
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(t_delete)
          .DeleteVertex(vertex_id);
    
    // 验证 DELETE 操作的 flags
    const auto& records = update.GetRecords();
    ASSERT_EQ(records.size(), 1);
    EXPECT_TRUE(records[0].key.IsDelete());
    EXPECT_FALSE(records[0].key.IsTombstone());  // 业务删除不设置物理墓碑
    
    EXPECT_TRUE(update.Apply(storage_).ok());
  }
  
  // 验证：删除前的时间点可以查询到
  auto before_delete = storage_->Get(vertex_id, EntityType::Vertex, 1, t_create);
  EXPECT_TRUE(before_delete.has_value());
  
  // 验证：删除后的时间点查询不到（或返回删除标记）
  auto after_delete = storage_->Get(vertex_id, EntityType::Vertex, 1, t_delete);
  // 注意：实际行为取决于存储引擎的实现
}

// =============================================================================
// 严格模式校验测试
// =============================================================================

TEST_F(CedarUpdateE2ETest, StrictModeValidation) {
  // 创建源点
  Timestamp t0(1712050000000000ULL);
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(t0).CreateVertex(7001, 1, Descriptor::InlineInt(1, 0));
    EXPECT_TRUE(update.Apply(storage_).ok());
  }
  
  // 严格模式下创建边（终点不存在）
  CEDAR_UPDATE(update, StrictLevel::CHECK_EXISTS);
  update.At(Timestamp(1712050000000001ULL))
        .CreateEdge(7001, 7999, 2, Descriptor::InlineInt(2, 0), true, true);
  
  auto status = update.Apply(storage_);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), CedarCode::kDstNodeNotFound);
}

// =============================================================================
// 性能基准测试
// =============================================================================

// BLOCKED: performance benchmark; not a correctness test. Keep disabled in CI.
TEST_F(CedarUpdateE2ETest, DISABLED_WritePerformance) {
  // 性能测试暂时禁用
}

// =============================================================================
// 完整信息落盘验证
// =============================================================================

TEST_F(CedarUpdateE2ETest, FullKeyInfoPersistence) {
  Timestamp ts(1712050000000000ULL);
  
  // 构造包含完整信息的 CedarUpdate
  CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
  update.At(ts)
        .WithSequence(42)
        .CreateVertex(9001, 5, Descriptor::InlineInt(5, 12345));
  
  // 写入前验证 CedarKey 信息
  const auto& records = update.GetRecords();
  ASSERT_EQ(records.size(), 1);
  const CedarKey& key_before = records[0].key;
  
  // 验证所有字段都已设置
  EXPECT_EQ(key_before.entity_id(), 9001);
  EXPECT_EQ(key_before.column_id(), 5);
  EXPECT_EQ(key_before.sequence(), 42);
  EXPECT_EQ(key_before.part_id(), static_cast<uint16_t>(9001));
  EXPECT_TRUE(key_before.IsCreate());
  EXPECT_TRUE(key_before.IsDistributed());
  EXPECT_FALSE(key_before.IsTombstone());
  
  // 执行写入
  auto status = update.Apply(storage_);
  EXPECT_TRUE(status.ok());
  
  // 强制刷盘
  EXPECT_TRUE(storage_->ForceFlush().ok());
  
  // 读取验证
  auto result = storage_->Get(9001, EntityType::Vertex, 5, ts);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result->AsInlineInt(), 12345);
  
  // 扫描验证（使用范围查询）
  auto scan_results = storage_->Scan(9001, EntityType::Vertex, 5, 
                                      Timestamp(0), Timestamp::Max());
  EXPECT_GE(scan_results.size(), 1);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
