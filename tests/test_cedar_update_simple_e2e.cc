// Copyright 2025 The Cedar Authors
//
// Simple end-to-end test for CedarUpdate without storage

#include <gtest/gtest.h>
#include "cedar/update/cedar_update.h"
#include "cedar/core/cedar_status.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;

TEST(CedarUpdateSimpleE2E, CreateVertexFullKeyInfo) {
  // 创建 CedarUpdate 并验证所有 CedarKey 字段
  Timestamp create_time(1712050000000000ULL);
  Descriptor desc = Descriptor::InlineInt(1, 42);
  
  CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
  update.At(create_time)
        .WithSequence(5)
        .CreateVertex(1001, 1, desc);
  
  // 验证 CedarKey 的所有字段
  const auto& records = update.GetRecords();
  EXPECT_EQ(records.size(), 1);
  if (records.empty()) return;
  
  const auto& key = records[0].key;
  
  // 验证所有 32B 字段
  EXPECT_EQ(key.entity_id(), 1001);
  EXPECT_EQ(key.column_id(), 1);
  EXPECT_EQ(key.sequence(), 5);
  EXPECT_TRUE(key.IsCreate());
  EXPECT_TRUE(key.IsDistributed());
  EXPECT_FALSE(key.IsTombstone());
  EXPECT_EQ(key.part_id(), static_cast<uint16_t>(1001));
  EXPECT_EQ(key.timestamp().value(), create_time.value());
  
  // 验证 32 字节编码
  std::string encoded = key.Encode();
  EXPECT_EQ(encoded.size(), 32);
  
  // 验证解码
  auto decoded = CedarKey::Decode(encoded);
  EXPECT_TRUE(decoded.has_value());
  if (decoded.has_value()) {
    EXPECT_EQ(decoded->entity_id(), 1001);
    EXPECT_EQ(decoded->column_id(), 1);
    EXPECT_EQ(decoded->sequence(), 5);
    EXPECT_EQ(decoded->flags(), key.flags());
    EXPECT_EQ(decoded->part_id(), key.part_id());
  }
}

TEST(CedarUpdateSimpleE2E, CreateEdgeDoubleEntry) {
  Timestamp ts(1712050000000000ULL);
  Descriptor desc = Descriptor::InlineInt(2, 100);
  
  CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
  update.At(ts)
        .CreateEdge(1001, 1002, 2, desc, false, false);
  
  const auto& records = update.GetRecords();
  EXPECT_EQ(records.size(), 2);  // EdgeOut + EdgeIn
  
  // EdgeOut
  const auto& edge_out = records[0].key;
  EXPECT_EQ(edge_out.entity_id(), 1001);
  EXPECT_EQ(edge_out.target_id(), 1002);
  EXPECT_TRUE(edge_out.IsEdgeOut());
  EXPECT_EQ(edge_out.part_id(), static_cast<uint16_t>(1001));  // 按 src 分区
  
  // EdgeIn
  const auto& edge_in = records[1].key;
  EXPECT_EQ(edge_in.entity_id(), 1002);
  EXPECT_EQ(edge_in.target_id(), 1001);
  EXPECT_TRUE(edge_in.IsEdgeIn());
  EXPECT_EQ(edge_in.part_id(), static_cast<uint16_t>(1002));  // 按 dst 分区
}

TEST(CedarUpdateSimpleE2E, DeleteVertexFlags) {
  Timestamp ts(1712050000000000ULL);
  
  CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
  update.At(ts)
        .DeleteVertex(1001);
  
  const auto& records = update.GetRecords();
  EXPECT_EQ(records.size(), 1);
  if (records.empty()) return;
  
  const auto& key = records[0].key;
  EXPECT_TRUE(key.IsDelete());
  EXPECT_FALSE(key.IsTombstone());  // 业务删除不设置物理墓碑
  EXPECT_EQ(key.column_id(), 0);    // 生命周期列
}

TEST(CedarUpdateSimpleE2E, CedarStatusAPI) {
  CedarStatus ok_status = CedarStatus::OK();
  EXPECT_TRUE(ok_status.ok());
  EXPECT_EQ(ok_status.code(), CedarCode::kOk);
  
  CedarStatus err_status(CedarCode::kSrcNodeNotFound, "Node not found");
  EXPECT_FALSE(err_status.ok());
  EXPECT_EQ(err_status.code(), CedarCode::kSrcNodeNotFound);
  EXPECT_TRUE(err_status.IsTopologyError());
  
  std::string msg = err_status.ToString();
  EXPECT_NE(msg.find("kSrcNodeNotFound"), std::string::npos);
}

TEST(CedarUpdateSimpleE2E, CedarStatusWithContext) {
  auto status = CedarStatus(CedarCode::kTemporalAnachronism, "Time error")
      .WithEntity(1001)
      .WithTimestamp(12345);
  
  EXPECT_TRUE(status.IsTemporalError());
  std::string str = status.ToString();
  EXPECT_NE(str.find("1001"), std::string::npos);
  EXPECT_NE(str.find("12345"), std::string::npos);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
