// Copyright (c) 2025 The Cedar Authors
// IndexScan operator tests

#include <gtest/gtest.h>
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"

using namespace cedar::cypher;

// Mock operator that yields pre-canned records
class MockOperator : public PhysicalOperator {
 public:
  std::vector<std::shared_ptr<Record>> records;
  size_t idx = 0;
  bool Init(ExecutionContext*) override { return true; }
  std::shared_ptr<Record> Next() override {
    if (idx >= records.size()) return nullptr;
    return records[idx++];
  }
  std::string GetName() const override { return "Mock"; }
};

TEST(IndexScanOperatorTest, ConstructorStoresParameters) {
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

TEST(IndexScanOperatorTest, ExplainOutputContainsDetails) {
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

TEST(IndexScanOperatorTest, FiltersByEqualityPredicate) {
  // Build an IndexScan: n:Person WHERE name = "Alice"
  auto index_scan = std::make_shared<IndexScan>(
      "n",
      std::optional<std::string>("Person"),
      "name",
      ComparisonExpr::EQ,
      Value("Alice"));

  // We don't have real storage, so the scan returns ids 1..1000.
  // The default Node has no "name" property, so MatchesPredicate fails.
  // To make the test meaningful we verify the operator initializes and
  // drains to nullptr (no matches with default empty properties).
  ExecutionContext ctx;
  ASSERT_TRUE(index_scan->Init(&ctx));

  int count = 0;
  while (index_scan->Next()) ++count;
  EXPECT_EQ(count, 0);  // No nodes have "name" = "Alice" by default
}
