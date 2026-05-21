// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Planner Signature Test

#include <gtest/gtest.h>

#include "cedar/cypher/planner.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/value.h"

using namespace cedar::cypher;

TEST(PlannerSignature, PlanClauseAcceptsUniquePtr) {
  QueryPlanner planner;

  MatchClause match;
  NodePattern node;
  node.variable = "n";
  node.labels.push_back("Person");
  PathPattern pattern;
  pattern.elements.push_back(node);
  match.patterns.push_back(pattern);

  auto result = planner.PlanClause(match, nullptr);
  EXPECT_NE(result, nullptr);
}

TEST(PlannerSignature, CreateFilterPredicateLiteralTrue) {
  QueryPlanner planner;
  LiteralExpr expr(Value(true));
  auto predicate = planner.CreateFilterPredicate(expr);
  Record record;
  EXPECT_TRUE(predicate(record));
}

TEST(PlannerSignature, CreateFilterPredicateLiteralFalse) {
  QueryPlanner planner;
  LiteralExpr expr(Value(false));
  auto predicate = planner.CreateFilterPredicate(expr);
  Record record;
  EXPECT_FALSE(predicate(record));
}

TEST(PlannerSignature, EvaluateExpressionLiteral) {
  QueryPlanner planner;
  LiteralExpr expr(Value(42));
  Record record;
  auto val = planner.EvaluateExpression(expr, record);
  EXPECT_TRUE(val.IsInt());
  EXPECT_EQ(val.GetInt(), 42);
}

TEST(PlannerSignature, EvaluateExpressionVariable) {
  QueryPlanner planner;
  VariableExpr expr("x");
  Record record;
  record.Set("x", Value(99));
  auto val = planner.EvaluateExpression(expr, record);
  EXPECT_TRUE(val.IsInt());
  EXPECT_EQ(val.GetInt(), 99);
}

TEST(PlannerSignature, EvaluateExpressionMissingVariableReturnsNull) {
  QueryPlanner planner;
  VariableExpr expr("y");
  Record record;
  auto val = planner.EvaluateExpression(expr, record);
  EXPECT_TRUE(val.IsNull());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
