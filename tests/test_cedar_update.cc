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
#include <filesystem>
#include <cstdio>

#include "cedar/update/cedar_update.h"
#include "cedar/core/cedar_status.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;

class CedarUpdateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/cedar_update_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
    
    CedarOptions options;
    options.create_if_missing = true;
    
    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
    EXPECT_TRUE(s.ok()) << "Failed to open storage: " << s.ToString();
  }
  
  void TearDown() override {
    delete storage_;
    std::filesystem::remove_all(test_dir_);
  }
  
  std::string test_dir_;
  CedarGraphStorage* storage_ = nullptr;
};

// ============================================================================
// 基本功能测试
// ============================================================================

TEST_F(CedarUpdateTest, CreateUpdate) {
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  EXPECT_EQ(update.GetStrictLevel(), StrictLevel::PERMISSIVE);
  EXPECT_EQ(update.Count(), 0);
}

TEST_F(CedarUpdateTest, CedarStatusOK) {
  CedarStatus status;
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status.code(), CedarCode::kOk);
  EXPECT_EQ(std::string(status.message()), "kOk");
}

TEST_F(CedarUpdateTest, CedarStatusError) {
  CedarStatus status(CedarCode::kSrcNodeNotFound, "Node 999 not found");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), CedarCode::kSrcNodeNotFound);
  EXPECT_TRUE(status.IsTopologyError());
  EXPECT_FALSE(status.IsTemporalError());
  
  std::string str = status.ToString();
  EXPECT_NE(str.find("kSrcNodeNotFound"), std::string::npos);
}

TEST_F(CedarUpdateTest, CedarStatusWithContext) {
  auto status = CedarStatus(CedarCode::kTemporalAnachronism, "Time error")
      .WithEntity(1001)
      .WithTimestamp(12345);
  
  EXPECT_TRUE(status.IsTemporalError());
  std::string str = status.ToString();
  EXPECT_NE(str.find("1001"), std::string::npos);
  EXPECT_NE(str.find("12345"), std::string::npos);
}

TEST_F(CedarUpdateTest, PutVertex) {
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  
  Descriptor desc = Descriptor::InlineInt(1, 42);
  update.PutVertex(1001, 1, desc);
  
  EXPECT_EQ(update.Count(), 1);
  
  const auto& records = update.GetRecords();
  EXPECT_EQ(records[0].op, UpdateOpType::CREATE_VERTEX);
  EXPECT_EQ(records[0].key.entity_id(), 1001);
  EXPECT_EQ(records[0].key.column_id(), 1);
  EXPECT_TRUE(records[0].key.IsCreate());
  EXPECT_TRUE(records[0].key.IsDistributed());
}

TEST_F(CedarUpdateTest, DeleteVertex) {
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  
  update.DeleteVertex(1001);
  
  EXPECT_EQ(update.Count(), 1);
  
  const auto& records = update.GetRecords();
  EXPECT_EQ(records[0].op, UpdateOpType::DELETE_VERTEX);
  EXPECT_EQ(records[0].key.entity_id(), 1001);
  EXPECT_TRUE(records[0].key.IsDelete());
  EXPECT_FALSE(records[0].key.IsTombstone());  // 业务删除不设置 tombstone
}

TEST_F(CedarUpdateTest, CreateEdgeGeneratesDoubleEntry) {
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  
  Descriptor desc = Descriptor::InlineInt(2, 100);
  update.CreateEdge(1001, 1002, 2, desc, false, false);
  
  // 应该生成 EdgeOut + EdgeIn 两个记录
  EXPECT_EQ(update.Count(), 2);
  
  const auto& records = update.GetRecords();
  
  // EdgeOut
  EXPECT_EQ(records[0].op, UpdateOpType::CREATE_EDGE);
  EXPECT_EQ(records[0].key.entity_id(), 1001);
  EXPECT_EQ(records[0].key.target_id(), 1002);
  EXPECT_TRUE(records[0].key.IsEdgeOut());
  
  // EdgeIn
  EXPECT_EQ(records[1].op, UpdateOpType::CREATE_EDGE);
  EXPECT_EQ(records[1].key.entity_id(), 1002);
  EXPECT_EQ(records[1].key.target_id(), 1001);
  EXPECT_TRUE(records[1].key.IsEdgeIn());
}

TEST_F(CedarUpdateTest, CreateEdgeWithConstraintFlags) {
  // 测试严格模式下约束标记设置
  auto update = CedarUpdate::Create(StrictLevel::CHECK_EXISTS);
  Descriptor desc = Descriptor::InlineInt(2, 100);
  
  // 显式指定检查源点和终点
  update.CreateEdge(1001, 1002, 2, desc, true, true);
  
  const auto& records = update.GetRecords();
  ASSERT_EQ(records.size(), 2);
  
  // EdgeOut 应该标记了约束
  ASSERT_TRUE(records[0].edge_info.has_value());
  EXPECT_EQ(records[0].edge_info->src_id, 1001);
  EXPECT_EQ(records[0].edge_info->dst_id, 1002);
  EXPECT_TRUE(records[0].edge_info->check_src_exists);
  EXPECT_TRUE(records[0].edge_info->check_dst_exists);
  
  // EdgeIn 不需要约束标记
  EXPECT_FALSE(records[1].edge_info.has_value());
}

TEST_F(CedarUpdateTest, DeleteEdgeGeneratesDoubleEntry) {
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  
  update.DeleteEdge(1001, 1002, 2);
  
  EXPECT_EQ(update.Count(), 2);
  
  const auto& records = update.GetRecords();
  EXPECT_EQ(records[0].op, UpdateOpType::DELETE_EDGE);
  EXPECT_EQ(records[1].op, UpdateOpType::DELETE_EDGE);
  EXPECT_TRUE(records[0].key.IsDelete());
  EXPECT_TRUE(records[1].key.IsDelete());
}

// ============================================================================
// 事件映射规范测试
// ============================================================================

TEST_F(CedarUpdateTest, EventMappingCreateNode) {
  // 测试用例 1: CREATE NODE
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  update.At(Timestamp(1712050000000000ULL));
  
  Descriptor desc = Descriptor::InlineInt(1, 0);  // Label:Person
  update.CreateVertex(1001, 1, desc);
  
  const auto& records = update.GetRecords();
  const auto& key = records[0].key;
  
  EXPECT_EQ(key.entity_id(), 1001);
  EXPECT_EQ(key.column_id(), 1);
  EXPECT_TRUE(key.IsVertex());
  EXPECT_TRUE(key.IsCreate());
  EXPECT_TRUE(key.IsDistributed());
  EXPECT_EQ(key.part_id(), 1001);  // entity_id % 65536
}

TEST_F(CedarUpdateTest, EventMappingCreateEdge) {
  // 测试用例 2: CREATE EDGE
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  update.At(Timestamp(1712050000000000ULL));
  
  Descriptor desc = Descriptor::InlineInt(2, 0);  // Edge:Follows
  update.CreateEdge(1001, 1002, 2, desc, false, false);
  
  const auto& records = update.GetRecords();
  
  // EdgeOut
  EXPECT_EQ(records[0].key.entity_id(), 1001);
  EXPECT_EQ(records[0].key.target_id(), 1002);
  EXPECT_EQ(records[0].key.part_id(), 1001);  // 按 src 分区
  EXPECT_TRUE(records[0].key.IsEdgeOut());
  EXPECT_TRUE(records[0].key.IsCreate());
  
  // EdgeIn
  EXPECT_EQ(records[1].key.entity_id(), 1002);
  EXPECT_EQ(records[1].key.target_id(), 1001);
  EXPECT_EQ(records[1].key.part_id(), 1002);  // 按 dst 分区
  EXPECT_TRUE(records[1].key.IsEdgeIn());
  EXPECT_TRUE(records[1].key.IsCreate());
}

TEST_F(CedarUpdateTest, EventMappingUpdateProp) {
  // 测试用例 3: UPDATE PROP
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  update.At(Timestamp(1712050000000001ULL));  // T+1
  
  Descriptor desc = Descriptor::InlineInt(3, 1);  // Status:active
  update.UpdateVertex(1001, 3, desc);
  
  const auto& records = update.GetRecords();
  const auto& key = records[0].key;
  
  EXPECT_EQ(key.entity_id(), 1001);
  EXPECT_EQ(key.column_id(), 3);
  EXPECT_TRUE(key.IsUpdate());
  EXPECT_FALSE(key.IsCreate());
}

TEST_F(CedarUpdateTest, EventMappingDeleteEdge) {
  // 测试用例 4: DELETE EDGE
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  update.At(Timestamp(1712050000000002ULL));  // T+2
  
  update.DeleteEdge(1001, 1002, 2);
  
  const auto& records = update.GetRecords();
  const auto& key = records[0].key;
  
  EXPECT_EQ(key.entity_id(), 1001);
  EXPECT_EQ(key.target_id(), 1002);
  EXPECT_TRUE(key.IsDelete());
  // 关键：业务 DELETE 不设置 is_tombstone (bit 7)
  EXPECT_FALSE(key.IsTombstone());
}

// ============================================================================
// 严格模式级别测试
// ============================================================================

TEST_F(CedarUpdateTest, StrictModeLevel) {
  // 测试严格模式级别设置
  auto permissive = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  EXPECT_EQ(permissive.GetStrictLevel(), StrictLevel::PERMISSIVE);
  
  auto check_exists = CedarUpdate::Create(StrictLevel::CHECK_EXISTS);
  EXPECT_EQ(check_exists.GetStrictLevel(), StrictLevel::CHECK_EXISTS);
  
  auto strict_temporal = CedarUpdate::Create(StrictLevel::STRICT_TEMPORAL);
  EXPECT_EQ(strict_temporal.GetStrictLevel(), StrictLevel::STRICT_TEMPORAL);
}

TEST_F(CedarUpdateTest, StrictModeAutoCheckExists) {
  // 严格模式下 PutEdge 自动启用约束检查
  auto update = CedarUpdate::Create(StrictLevel::CHECK_EXISTS);
  Descriptor desc = Descriptor::InlineInt(2, 0);
  
  update.PutEdge(1001, 1002, 2, desc);
  
  const auto& records = update.GetRecords();
  ASSERT_EQ(records.size(), 2);
  
  // EdgeOut 应该自动标记检查
  ASSERT_TRUE(records[0].edge_info.has_value());
  EXPECT_TRUE(records[0].edge_info->check_src_exists);
  EXPECT_TRUE(records[0].edge_info->check_dst_exists);
}

// ============================================================================
// 链式调用测试
// ============================================================================

TEST_F(CedarUpdateTest, ChainedOperations) {
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  
  Descriptor desc1 = Descriptor::InlineInt(1, 0);
  Descriptor desc2 = Descriptor::InlineInt(2, 0);
  
  update.CreateVertex(1001, 1, desc1)
        .CreateVertex(1002, 1, desc1)
        .CreateEdge(1001, 1002, 2, desc2, false, false);
  
  EXPECT_EQ(update.Count(), 4);  // 2 vertices + 2 edges (EdgeOut + EdgeIn)
}

// ============================================================================
// 清空和复用测试
// ============================================================================

TEST_F(CedarUpdateTest, ClearAndReuse) {
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  
  Descriptor desc = Descriptor::InlineInt(1, 0);
  update.CreateVertex(1001, 1, desc);
  EXPECT_EQ(update.Count(), 1);
  
  update.Clear();
  EXPECT_EQ(update.Count(), 0);
  
  update.CreateVertex(1002, 1, desc);
  EXPECT_EQ(update.Count(), 1);
}

// ============================================================================
// 空操作测试
// ============================================================================

TEST_F(CedarUpdateTest, EmptyApply) {
  auto update = CedarUpdate::Create(StrictLevel::PERMISSIVE);
  
  // 空 update 应该返回 OK
  auto status = update.Apply(storage_);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status.code(), CedarCode::kOk);
}

// ============================================================================
// 便捷宏测试
// ============================================================================

TEST_F(CedarUpdateTest, ConvenienceMacros) {
  CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
  
  Descriptor desc = Descriptor::InlineInt(1, 0);
  update.CreateVertex(1001, 1, desc);
  
  EXPECT_EQ(update.Count(), 1);
}

// ============================================================================
// CEDAR_RETURN_IF_ERROR 宏测试
// ============================================================================

TEST_F(CedarUpdateTest, ReturnIfErrorMacro) {
  // 测试宏正确返回错误
  auto test_func = [](bool fail) -> CedarStatus {
    if (fail) {
      CEDAR_RETURN_IF_ERROR(CedarStatus(CedarCode::kInternalError, "Test"));
    }
    return CedarStatus::OK();
  };
  
  auto status_ok = test_func(false);
  EXPECT_TRUE(status_ok.ok());
  
  auto status_err = test_func(true);
  EXPECT_FALSE(status_err.ok());
  EXPECT_EQ(status_err.code(), CedarCode::kInternalError);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
