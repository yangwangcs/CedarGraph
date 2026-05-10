#include <gtest/gtest.h>
#include "cedar/cypher/parser.h"
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/cypher/ast.h"

using namespace cedar::cypher;

// Helper mock operator
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

// ============================================================================
// Filter Operator Tests
// ============================================================================

TEST(FilterOperatorTest, SimpleEqualityFilter) {
  auto mock = std::make_shared<MockOperator>();
  Node node;
  node.id = 1;
  node.labels.push_back("Person");
  node.properties["name"] = Value("Alice");
  auto rec = std::make_shared<Record>();
  rec->Set("n", Value(node));
  mock->records.push_back(rec);
  
  auto prop_expr = std::make_shared<PropertyExpr>("n", "name");
  auto lit_expr = std::make_shared<LiteralExpr>(Value("Alice"));
  auto comp_expr = std::make_shared<ComparisonExpr>(ComparisonExpr::EQ, prop_expr, lit_expr);
  
  ExecutionContext ctx;
  Filter filter(comp_expr);
  filter.AddChild(mock);
  filter.Init(&ctx);
  
  EXPECT_NE(filter.Next(), nullptr);
}

TEST(FilterOperatorTest, SimpleEqualityFilterNoMatch) {
  auto mock = std::make_shared<MockOperator>();
  Node node;
  node.id = 1;
  node.properties["name"] = Value("Bob");
  auto rec = std::make_shared<Record>();
  rec->Set("n", Value(node));
  mock->records.push_back(rec);
  
  auto prop_expr = std::make_shared<PropertyExpr>("n", "name");
  auto lit_expr = std::make_shared<LiteralExpr>(Value("Alice"));
  auto comp_expr = std::make_shared<ComparisonExpr>(ComparisonExpr::EQ, prop_expr, lit_expr);
  
  ExecutionContext ctx;
  Filter filter(comp_expr);
  filter.AddChild(mock);
  filter.Init(&ctx);
  
  EXPECT_EQ(filter.Next(), nullptr);
}

TEST(FilterOperatorTest, AndFilter) {
  auto mock = std::make_shared<MockOperator>();
  Node node;
  node.id = 1;
  node.properties["name"] = Value("Alice");
  node.properties["age"] = Value(25);
  auto rec = std::make_shared<Record>();
  rec->Set("n", Value(node));
  mock->records.push_back(rec);
  
  auto name_prop = std::make_shared<PropertyExpr>("n", "name");
  auto alice_lit = std::make_shared<LiteralExpr>(Value("Alice"));
  auto name_eq = std::make_shared<ComparisonExpr>(ComparisonExpr::EQ, name_prop, alice_lit);
  
  auto age_prop = std::make_shared<PropertyExpr>("n", "age");
  auto twentyfive_lit = std::make_shared<LiteralExpr>(Value(25));
  auto age_eq = std::make_shared<ComparisonExpr>(ComparisonExpr::EQ, age_prop, twentyfive_lit);
  
  auto and_expr = std::make_shared<LogicalExpr>(LogicalExpr::Op::AND, name_eq, age_eq);
  
  ExecutionContext ctx;
  Filter filter(and_expr);
  filter.AddChild(mock);
  filter.Init(&ctx);
  
  EXPECT_NE(filter.Next(), nullptr);
}

TEST(FilterOperatorTest, GreaterThanFilter) {
  auto mock = std::make_shared<MockOperator>();
  Node node;
  node.id = 1;
  node.properties["age"] = Value(30);
  auto rec = std::make_shared<Record>();
  rec->Set("n", Value(node));
  mock->records.push_back(rec);
  
  auto age_prop = std::make_shared<PropertyExpr>("n", "age");
  auto lit_expr = std::make_shared<LiteralExpr>(Value(18));
  auto comp_expr = std::make_shared<ComparisonExpr>(ComparisonExpr::GT, age_prop, lit_expr);
  
  ExecutionContext ctx;
  Filter filter(comp_expr);
  filter.AddChild(mock);
  filter.Init(&ctx);
  
  EXPECT_NE(filter.Next(), nullptr);
}

// ============================================================================
// Project Operator Tests
// ============================================================================

TEST(ProjectOperatorTest, ProjectProperty) {
  auto record = std::make_shared<Record>();
  Node node;
  node.id = 1;
  node.properties["name"] = Value("Alice");
  record->Set("n", Value(node));
  
  auto mock = std::make_shared<MockOperator>();
  mock->records.push_back(record);
  
  // Project n.name AS name
  std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections;
  projections.push_back({"name", std::make_shared<PropertyExpr>("n", "name")});
  
  Project project(projections);
  project.AddChild(mock);
  
  ExecutionContext ctx;
  project.Init(&ctx);
  
  auto result = project.Next();
  ASSERT_NE(result, nullptr);
  auto name_val = result->Get("name");
  ASSERT_TRUE(name_val.has_value());
  EXPECT_EQ(name_val->GetString(), "Alice");
}

// ============================================================================
// Sort Operator Tests
// ============================================================================

TEST(SortOperatorTest, SortByIntegerAscending) {
  auto mock = std::make_shared<MockOperator>();
  for (int i : {3, 1, 2}) {
    auto rec = std::make_shared<Record>();
    rec->Set("age", Value(i));
    mock->records.push_back(rec);
  }
  
  std::vector<std::pair<std::shared_ptr<Expression>, bool>> sort_items;
  sort_items.push_back({std::make_shared<VariableExpr>("age"), true});
  
  Sort sort(sort_items);
  sort.AddChild(mock);
  
  ExecutionContext ctx;
  sort.Init(&ctx);
  
  auto r1 = sort.Next();
  EXPECT_EQ(r1->Get("age")->GetInt(), 1);
  auto r2 = sort.Next();
  EXPECT_EQ(r2->Get("age")->GetInt(), 2);
  auto r3 = sort.Next();
  EXPECT_EQ(r3->Get("age")->GetInt(), 3);
  EXPECT_EQ(sort.Next(), nullptr);
}

TEST(SortOperatorTest, SortByIntegerDescending) {
  auto mock = std::make_shared<MockOperator>();
  for (int i : {3, 1, 2}) {
    auto rec = std::make_shared<Record>();
    rec->Set("age", Value(i));
    mock->records.push_back(rec);
  }
  
  std::vector<std::pair<std::shared_ptr<Expression>, bool>> sort_items;
  sort_items.push_back({std::make_shared<VariableExpr>("age"), false});
  
  Sort sort(sort_items);
  sort.AddChild(mock);
  
  ExecutionContext ctx;
  sort.Init(&ctx);
  
  auto r1 = sort.Next();
  EXPECT_EQ(r1->Get("age")->GetInt(), 3);
  auto r2 = sort.Next();
  EXPECT_EQ(r2->Get("age")->GetInt(), 2);
  auto r3 = sort.Next();
  EXPECT_EQ(r3->Get("age")->GetInt(), 1);
  EXPECT_EQ(sort.Next(), nullptr);
}

// ============================================================================
// Limit and Skip Operator Tests
// ============================================================================

TEST(LimitOperatorTest, LimitReturnsCorrectCount) {
  class CountingMock : public PhysicalOperator {
   public:
    int count = 0;
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override {
      if (count >= 10) return nullptr;
      auto rec = std::make_shared<Record>();
      rec->Set("n", Value(count++));
      return rec;
    }
    std::string GetName() const override { return "CountingMock"; }
  };
  
  auto mock = std::make_shared<CountingMock>();
  Limit limit(3);
  limit.AddChild(mock);
  
  ExecutionContext ctx;
  limit.Init(&ctx);
  
  int returned = 0;
  while (limit.Next()) ++returned;
  EXPECT_EQ(returned, 3);
}

TEST(SkipOperatorTest, SkipDiscardsCorrectCount) {
  class CountingMock : public PhysicalOperator {
   public:
    int count = 0;
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override {
      if (count >= 5) return nullptr;
      auto rec = std::make_shared<Record>();
      rec->Set("n", Value(count++));
      return rec;
    }
    std::string GetName() const override { return "CountingMock"; }
  };
  
  auto mock = std::make_shared<CountingMock>();
  Skip skip(2);
  skip.AddChild(mock);
  
  ExecutionContext ctx;
  skip.Init(&ctx);
  
  auto r1 = skip.Next();
  ASSERT_NE(r1, nullptr);
  EXPECT_EQ(r1->Get("n")->GetInt(), 2);
  
  auto r2 = skip.Next();
  ASSERT_NE(r2, nullptr);
  EXPECT_EQ(r2->Get("n")->GetInt(), 3);
  
  auto r3 = skip.Next();
  ASSERT_NE(r3, nullptr);
  EXPECT_EQ(r3->Get("n")->GetInt(), 4);
  
  EXPECT_EQ(skip.Next(), nullptr);
}

// ============================================================================
// Distinct Operator Tests
// ============================================================================

TEST(DistinctOperatorTest, DistinctRemovesDuplicates) {
  auto mock = std::make_shared<MockOperator>();
  for (const auto& name : {"Alice", "Bob", "Alice", "Charlie", "Bob"}) {
    auto rec = std::make_shared<Record>();
    rec->Set("name", Value(name));
    mock->records.push_back(rec);
  }
  
  std::vector<std::shared_ptr<Expression>> keys;
  keys.push_back(std::make_shared<VariableExpr>("name"));
  
  Distinct distinct(keys);
  distinct.AddChild(mock);
  
  ExecutionContext ctx;
  distinct.Init(&ctx);
  
  int returned = 0;
  while (distinct.Next()) ++returned;
  EXPECT_EQ(returned, 3);  // Alice, Bob, Charlie
}

// ============================================================================
// Execution Plan Builder Tests
// ============================================================================

TEST(ExecutionPlanBuilderTest, BuildSimpleMatchReturn) {
  CypherParser parser("MATCH (n) RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  EXPECT_TRUE(parser.GetError().empty());
  
  ExecutionPlanBuilder builder;
  auto plan = builder.Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  
  EXPECT_EQ(plan->GetName(), "ProduceResults");
}

TEST(ExecutionPlanBuilderTest, BuildMatchWhereReturn) {
  CypherParser parser("MATCH (n) WHERE n.age > 18 RETURN n.name");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  EXPECT_TRUE(parser.GetError().empty());
  
  ExecutionPlanBuilder builder;
  auto plan = builder.Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  
  auto explain = plan->Explain(0);
  EXPECT_NE(explain.find("ProduceResults"), std::string::npos);
  EXPECT_NE(explain.find("Project"), std::string::npos);
  EXPECT_NE(explain.find("Filter"), std::string::npos);
}

TEST(ExecutionPlanBuilderTest, BuildWithOrderByLimit) {
  CypherParser parser("MATCH (n) RETURN n.name ORDER BY n.name LIMIT 5");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  EXPECT_TRUE(parser.GetError().empty());
  
  ExecutionPlanBuilder builder;
  auto plan = builder.Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  
  auto explain = plan->Explain(0);
  EXPECT_NE(explain.find("Sort"), std::string::npos);
  EXPECT_NE(explain.find("Limit"), std::string::npos);
}
