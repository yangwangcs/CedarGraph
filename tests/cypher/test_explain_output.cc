// Copyright (c) 2025 The Cedar Authors
// EXPLAIN output tests

#include <gtest/gtest.h>
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

TEST(ExplainOutputTest, ContainsActualOperators) {
  CypherParser parser("MATCH (n:Person) WHERE n.age > 25 RETURN n.name");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);

  std::string explain = plan->Explain(0);
  std::cout << explain << std::endl;

  EXPECT_NE(explain.find("ProduceResults"), std::string::npos);
  EXPECT_NE(explain.find("Project"), std::string::npos);
  // After pushdown, should have IndexScan instead of NodeScan + Filter
  EXPECT_TRUE(explain.find("IndexScan") != std::string::npos ||
              explain.find("NodeScan") != std::string::npos);
}

TEST(ExplainOutputTest, IndentationReflectsTreeDepth) {
  CypherParser parser("MATCH (n) RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);

  std::string explain = plan->Explain(0);
  // Root should be at indent 0 (no leading spaces)
  EXPECT_EQ(explain.find("ProduceResults"), 0);
  // Child should be indented
  EXPECT_NE(explain.find("  NodeScan"), std::string::npos);
}

TEST(ExplainOutputTest, GraphServiceRouterStyleExplain) {
  // Simulate what GraphServiceRouter does for EXPLAIN
  std::string query = "MATCH (n:Person) WHERE n.age = 30 RETURN n.name, n.age";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);

  auto physical = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(physical, nullptr);

  ExecutionPlan plan(physical);
  std::string explain = plan.Explain();

  // Must contain all operators in the tree
  EXPECT_NE(explain.find("ProduceResults"), std::string::npos);
  EXPECT_NE(explain.find("Project"), std::string::npos);
  EXPECT_NE(explain.find("IndexScan"), std::string::npos);

  // Must NOT contain the old hardcoded text
  EXPECT_EQ(explain.find("Query Type:"), std::string::npos);
  EXPECT_EQ(explain.find("Target Partitions:"), std::string::npos);
}
