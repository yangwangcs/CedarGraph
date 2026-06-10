// Copyright (c) 2025 The Cedar Authors
// Predicate pushdown tests

#include <gtest/gtest.h>
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

TEST(PredicatePushdownTest, SimpleEqualityPushedToIndexScan) {
  // Parse: MATCH (n:Person) WHERE n.name = 'Alice' RETURN n
  CypherParser parser("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << parser.GetError();

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);

  std::string explain = plan->Explain(0);
  std::cout << "EXPLAIN:\n" << explain << std::endl;

  // After pushdown the plan should contain IndexScan, NOT NodeScan + Filter
  EXPECT_NE(explain.find("IndexScan"), std::string::npos);
  EXPECT_EQ(explain.find("NodeScan"), std::string::npos);
  EXPECT_EQ(explain.find("Filter"), std::string::npos);
}

TEST(PredicatePushdownTest, RangePredicatePushedToIndexScan) {
  CypherParser parser("MATCH (n:Person) WHERE n.age > 25 RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << parser.GetError();

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);

  std::string explain = plan->Explain(0);
  EXPECT_NE(explain.find("IndexScan"), std::string::npos);
  EXPECT_EQ(explain.find("NodeScan"), std::string::npos);
}

TEST(PredicatePushdownTest, NonPushablePredicateKeepsFilter) {
  // n.name = m.name is not pushable (right side is not a literal)
  CypherParser parser("MATCH (n:Person), (m:Person) WHERE n.name = m.name RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << parser.GetError();

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);

  std::string explain = plan->Explain(0);
  // Should still have a Filter because the predicate references two variables
  EXPECT_NE(explain.find("Filter"), std::string::npos);
}
