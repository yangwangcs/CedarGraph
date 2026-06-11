// Copyright 2025 The Cedar Authors

#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/value.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"

using namespace cedar::cypher;
using namespace cedar;

static std::string GetTempDbPath() {
  static std::atomic<int> counter{0};
  auto pid = getpid();
  auto seq = counter.fetch_add(1);
  auto tmp = std::filesystem::temp_directory_path() /
             ("cedar_merge_test_" + std::to_string(pid) + "_" + std::to_string(seq));
  std::filesystem::remove_all(tmp);
  std::filesystem::create_directories(tmp);
  return tmp.string();
}

// ============================================================================
// MergeOperator Tests
// ============================================================================

class MergeOperatorTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::string db_path_;

  void SetUp() override {
    db_path_ = GetTempDbPath();
    CedarOptions options;
    options.create_if_missing = true;
    auto s = CedarGraphStorage::Open(options, db_path_, &storage_);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  void TearDown() override {
    delete storage_;
    std::filesystem::remove_all(db_path_);
  }
};

TEST_F(MergeOperatorTest, MergeCreatesNodeWhenNotFound) {
  auto merge_clause = std::make_shared<MergeClause>();
  PathPattern pattern;
  NodePattern node;
  node.variable = "n";
  node.labels = {"Person"};
  node.properties["name"] = std::make_shared<LiteralExpr>(Value("Alice"));
  pattern.elements.push_back(node);
  merge_clause->patterns.push_back(std::move(pattern));

  MergeOperator op(merge_clause);
  ExecutionContext ctx;
  ctx.storage = storage_;

  ASSERT_TRUE(op.Init(&ctx));
  auto record = op.Next();
  ASSERT_NE(record, nullptr);
  EXPECT_TRUE(record->Get("n")->IsNode());
  EXPECT_EQ(record->Get("n")->GetNode().labels[0], "Person");
  EXPECT_EQ(op.Next(), nullptr);
}

// ============================================================================
// UnwindOperator Tests
// ============================================================================

class UnwindOperatorTest : public ::testing::Test {};

TEST_F(UnwindOperatorTest, UnwindLiteralList) {
  // Build a mock child that returns a single empty record
  class MockChild : public PhysicalOperator {
   public:
    bool emitted_ = false;
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override {
      if (emitted_) return nullptr;
      emitted_ = true;
      return std::make_shared<Record>();
    }
    std::string GetName() const override { return "MockChild"; }
    std::unique_ptr<PhysicalOperator> Clone() const override {
      return std::make_unique<MockChild>();
    }
  };

  auto mock = std::make_shared<MockChild>();

  // UNWIND [10, 20, 30] AS x
  std::vector<std::shared_ptr<Expression>> elems;
  elems.push_back(std::make_shared<LiteralExpr>(Value(10)));
  elems.push_back(std::make_shared<LiteralExpr>(Value(20)));
  elems.push_back(std::make_shared<LiteralExpr>(Value(30)));
  auto list_expr = std::make_shared<ListLiteralExpr>(elems);

  UnwindOperator op(list_expr, "x");
  op.AddChild(mock);

  ExecutionContext ctx;
  ASSERT_TRUE(op.Init(&ctx));

  auto r1 = op.Next();
  ASSERT_NE(r1, nullptr);
  EXPECT_EQ(r1->Get("x")->GetInt(), 10);

  auto r2 = op.Next();
  ASSERT_NE(r2, nullptr);
  EXPECT_EQ(r2->Get("x")->GetInt(), 20);

  auto r3 = op.Next();
  ASSERT_NE(r3, nullptr);
  EXPECT_EQ(r3->Get("x")->GetInt(), 30);

  EXPECT_EQ(op.Next(), nullptr);
}

TEST_F(UnwindOperatorTest, UnwindExhaustsAfterList) {
  class EmptyChild : public PhysicalOperator {
   public:
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override { return nullptr; }
    std::string GetName() const override { return "EmptyChild"; }
    std::unique_ptr<PhysicalOperator> Clone() const override {
      return std::make_unique<EmptyChild>();
    }
  };

  auto mock = std::make_shared<EmptyChild>();
  std::vector<std::shared_ptr<Expression>> elems;
  auto list_expr = std::make_shared<ListLiteralExpr>(elems);

  UnwindOperator op(list_expr, "x");
  op.AddChild(mock);

  ExecutionContext ctx;
  ASSERT_TRUE(op.Init(&ctx));
  EXPECT_EQ(op.Next(), nullptr);
}

// ============================================================================
// ExecutionPlanBuilder Tests
// ============================================================================

TEST(ExecutionPlanBuilderMergeWithUnwind, BuildPlanWithMergeClause) {
  auto stmt = std::make_shared<QueryStatement>();
  auto merge_clause = std::make_shared<MergeClause>();
  PathPattern pattern;
  NodePattern node;
  node.variable = "n";
  node.labels = {"TestLabel"};
  pattern.elements.push_back(node);
  merge_clause->patterns.push_back(std::move(pattern));
  stmt->clauses.push_back(merge_clause);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  EXPECT_NE(plan->Explain().find("Merge"), std::string::npos);
}

TEST(ExecutionPlanBuilderMergeWithUnwind, BuildPlanWithWithClause) {
  auto stmt = std::make_shared<QueryStatement>();

  auto match_clause = std::make_shared<MatchClause>();
  PathPattern pattern;
  NodePattern node;
  node.variable = "n";
  pattern.elements.push_back(node);
  match_clause->patterns.push_back(std::move(pattern));
  stmt->clauses.push_back(match_clause);

  auto with_clause = std::make_shared<WithClause>();
  ReturnItem item;
  item.expression = std::make_shared<PropertyExpr>("n", "name");
  item.alias = "personName";
  with_clause->items.push_back(item);
  stmt->clauses.push_back(with_clause);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  auto explain = plan->Explain();
  EXPECT_NE(explain.find("Project"), std::string::npos);
  EXPECT_NE(explain.find("NodeScan"), std::string::npos);
}

TEST(ExecutionPlanBuilderMergeWithUnwind, BuildPlanWithUnwindClause) {
  auto stmt = std::make_shared<QueryStatement>();

  auto unwind_clause = std::make_shared<UnwindClause>();
  std::vector<std::shared_ptr<Expression>> elems;
  elems.push_back(std::make_shared<LiteralExpr>(Value(1)));
  elems.push_back(std::make_shared<LiteralExpr>(Value(2)));
  unwind_clause->expression = std::make_shared<ListLiteralExpr>(elems);
  unwind_clause->alias = "x";
  stmt->clauses.push_back(unwind_clause);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  EXPECT_NE(plan->Explain().find("Unwind"), std::string::npos);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
