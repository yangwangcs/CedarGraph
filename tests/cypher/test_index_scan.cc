// Copyright (c) 2025 The Cedar Authors
// IndexScan operator tests — with real secondary index

#include <gtest/gtest.h>
#include <filesystem>
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/types/descriptor.h"

using namespace cedar::cypher;
using namespace cedar;

class IndexScanRealIndexTest : public ::testing::Test {
 protected:
  std::string db_path_;
  CedarGraphStorage* storage_ = nullptr;

  void SetUp() override {
    char buf[] = "/tmp/cedar_indexscan_test_XXXXXX";
    char* dir = mkdtemp(buf);
    ASSERT_NE(dir, nullptr);
    db_path_ = dir;

    CedarOptions options;
    options.create_if_missing = true;
    Status s = CedarGraphStorage::Open(options, db_path_, &storage_);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    if (!db_path_.empty() && std::filesystem::exists(db_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }

  // Helper: write a static vertex property directly through storage
  void PutStaticProp(uint64_t entity_id, uint16_t col_id, int32_t value) {
    Descriptor desc = Descriptor::InlineInt(col_id, value);
    Status s = storage_->PutStaticVertex(entity_id, col_id, desc);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  static uint16_t PropertyNameToColumnId(const std::string& name) {
    return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
  }
};

TEST_F(IndexScanRealIndexTest, ConstructorStoresParameters) {
  auto index_scan = std::make_shared<IndexScan>(
      "n",
      std::optional<std::string>("Person"),
      "name",
      ComparisonExpr::EQ,
      Value("Alice"));

  EXPECT_EQ(index_scan->GetName(), "IndexScan");
  EXPECT_NE(index_scan->GetDetails().find("Person"), std::string::npos);
  EXPECT_NE(index_scan->GetDetails().find("name"), std::string::npos);
}

TEST_F(IndexScanRealIndexTest, ExplainOutputContainsDetails) {
  auto index_scan = std::make_shared<IndexScan>(
      "n",
      std::optional<std::string>("Person"),
      "age",
      ComparisonExpr::GE,
      Value(18));

  std::string explain = index_scan->Explain(0);
  EXPECT_NE(explain.find("IndexScan"), std::string::npos);
  EXPECT_NE(explain.find("Person"), std::string::npos);
  EXPECT_NE(explain.find("age"), std::string::npos);
}

TEST_F(IndexScanRealIndexTest, UsesPropertyIndexForEquality) {
  // Seed the index: entity 42 has age = 25
  PutStaticProp(42, PropertyNameToColumnId("age"), 25);
  PutStaticProp(43, PropertyNameToColumnId("age"), 30);

  auto index_scan = std::make_shared<IndexScan>(
      "n", std::nullopt, "age", ComparisonExpr::EQ, Value(25));

  ExecutionContext ctx;
  ctx.storage = storage_;
  ASSERT_TRUE(index_scan->Init(&ctx));

  int count = 0;
  while (index_scan->Next()) ++count;
  EXPECT_EQ(count, 1);  // Only entity 42 matches
}

TEST_F(IndexScanRealIndexTest, FallbackToRangeScanWhenNoIndexHit) {
  // No indexed data at all
  auto index_scan = std::make_shared<IndexScan>(
      "n", std::nullopt, "age", ComparisonExpr::EQ, Value(99));

  ExecutionContext ctx;
  ctx.storage = storage_;
  ASSERT_TRUE(index_scan->Init(&ctx));

  int count = 0;
  while (index_scan->Next()) ++count;
  // Fallback range scan produces ids 1..1000, none have property "age" = 99
  EXPECT_EQ(count, 0);
}

TEST_F(IndexScanRealIndexTest, UsesLabelIndexWhenAvailable) {
  // Seed label index via LsmEngine directly
  auto* engine = storage_->GetLsmEngine();
  ASSERT_NE(engine, nullptr);
  engine->IndexLabel(77, "Person");
  engine->IndexLabel(78, "Person");

  auto index_scan = std::make_shared<IndexScan>(
      "n", std::optional<std::string>("Person"), "name",
      ComparisonExpr::EQ, Value("Alice"));

  ExecutionContext ctx;
  ctx.storage = storage_;
  ASSERT_TRUE(index_scan->Init(&ctx));

  int count = 0;
  while (index_scan->Next()) ++count;
  EXPECT_EQ(count, 2);  // Entities 77 and 78
}
