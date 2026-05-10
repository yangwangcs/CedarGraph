#include <gtest/gtest.h>
#include "cedar/cypher/parser.h"
#include "cedar/cypher/value.h"
#include "cedar/cypher/ast.h"
#include <map>

using namespace cedar::cypher;

// Helper to check if parsing succeeds
bool ParseAndCheck(const std::string& query) {
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  return stmt != nullptr && parser.GetError().empty();
}

// Helper to get where clause from query
std::shared_ptr<WhereClause> GetWhereClause(const std::string& query) {
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  if (!stmt || !parser.GetError().empty()) {
    return nullptr;
  }
  
  for (const auto& clause : stmt->clauses) {
    if (auto where = std::dynamic_pointer_cast<WhereClause>(clause)) {
      return where;
    }
  }
  return nullptr;
}

// ============================================================================
// Basic WHERE Tests
// ============================================================================

TEST(WhereClauseTest, SimpleEquality) {
  auto where = GetWhereClause("MATCH (n) WHERE n.name = 'Alice' RETURN n");
  ASSERT_NE(where, nullptr);
  ASSERT_NE(where->condition, nullptr);
  EXPECT_EQ(where->condition->expr_type, ExprType::COMPARISON);
}

TEST(WhereClauseTest, SimpleInequality) {
  auto where = GetWhereClause("MATCH (n) WHERE n.age <> 25 RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  EXPECT_EQ(comp->op, ComparisonExpr::NE);
}

TEST(WhereClauseTest, LessThan) {
  auto where = GetWhereClause("MATCH (n) WHERE n.age < 18 RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  EXPECT_EQ(comp->op, ComparisonExpr::LT);
}

TEST(WhereClauseTest, GreaterThan) {
  auto where = GetWhereClause("MATCH (n) WHERE n.age > 65 RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  EXPECT_EQ(comp->op, ComparisonExpr::GT);
}

TEST(WhereClauseTest, LessThanOrEqual) {
  auto where = GetWhereClause("MATCH (n) WHERE n.age <= 18 RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  EXPECT_EQ(comp->op, ComparisonExpr::LE);
}

TEST(WhereClauseTest, GreaterThanOrEqual) {
  auto where = GetWhereClause("MATCH (n) WHERE n.age >= 65 RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  EXPECT_EQ(comp->op, ComparisonExpr::GE);
}

// ============================================================================
// Logical Operators Tests
// ============================================================================

TEST(WhereClauseTest, SimpleAnd) {
  auto where = GetWhereClause("MATCH (n) WHERE n.age > 18 AND n.age < 65 RETURN n");
  ASSERT_NE(where, nullptr);
  auto logical = std::static_pointer_cast<LogicalExpr>(where->condition);
  EXPECT_EQ(logical->op, LogicalExpr::Op::AND);
}

TEST(WhereClauseTest, SimpleOr) {
  auto where = GetWhereClause("MATCH (n) WHERE n.name = 'Alice' OR n.name = 'Bob' RETURN n");
  ASSERT_NE(where, nullptr);
  auto logical = std::static_pointer_cast<LogicalExpr>(where->condition);
  EXPECT_EQ(logical->op, LogicalExpr::Op::OR);
}

TEST(WhereClauseTest, NotExpression) {
  auto where = GetWhereClause("MATCH (n) WHERE NOT n.active RETURN n");
  ASSERT_NE(where, nullptr);
  EXPECT_EQ(where->condition->expr_type, ExprType::NOT);
}

TEST(WhereClauseTest, ComplexAndOr) {
  auto where = GetWhereClause("MATCH (n) WHERE (n.age > 18 AND n.age < 65) OR n.name = 'Admin' RETURN n");
  ASSERT_NE(where, nullptr);
  // Should be OR with AND as left operand
  auto outer = std::static_pointer_cast<LogicalExpr>(where->condition);
  EXPECT_EQ(outer->op, LogicalExpr::Op::OR);
}

// ============================================================================
// String Operators Tests
// ============================================================================

TEST(WhereClauseTest, StartsWith) {
  auto where = GetWhereClause("MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n");
  ASSERT_NE(where, nullptr);
  EXPECT_EQ(where->condition->expr_type, ExprType::FUNCTION_CALL);
  auto func = std::static_pointer_cast<FunctionCallExpr>(where->condition);
  EXPECT_EQ(func->name, "STARTS_WITH");
}

TEST(WhereClauseTest, EndsWith) {
  auto where = GetWhereClause("MATCH (n) WHERE n.name ENDS WITH 'z' RETURN n");
  ASSERT_NE(where, nullptr);
  auto func = std::static_pointer_cast<FunctionCallExpr>(where->condition);
  EXPECT_EQ(func->name, "ENDS_WITH");
}

TEST(WhereClauseTest, Contains) {
  auto where = GetWhereClause("MATCH (n) WHERE n.bio CONTAINS 'developer' RETURN n");
  ASSERT_NE(where, nullptr);
  auto func = std::static_pointer_cast<FunctionCallExpr>(where->condition);
  EXPECT_EQ(func->name, "CONTAINS");
}

// ============================================================================
// NULL Checks Tests
// ============================================================================

TEST(WhereClauseTest, IsNull) {
  auto where = GetWhereClause("MATCH (n) WHERE n.email IS NULL RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  EXPECT_EQ(comp->op, ComparisonExpr::EQ);
}

TEST(WhereClauseTest, IsNotNull) {
  auto where = GetWhereClause("MATCH (n) WHERE n.email IS NOT NULL RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  EXPECT_EQ(comp->op, ComparisonExpr::NE);
}

// ============================================================================
// Arithmetic Tests
// ============================================================================

TEST(WhereClauseTest, ArithmeticExpression) {
  auto where = GetWhereClause("MATCH (n) WHERE n.x + n.y = 10 RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  EXPECT_EQ(comp->left->expr_type, ExprType::ARITHMETIC);
}

// ============================================================================
// Parameterized Query Tests
// ============================================================================

TEST(WhereClauseTest, ParameterReference) {
  auto where = GetWhereClause("MATCH (n) WHERE n.name = $name RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  auto right = std::static_pointer_cast<ParameterExpr>(comp->right);
  EXPECT_EQ(right->name, "name");
}

TEST(WhereClauseTest, MultipleParameters) {
  auto where = GetWhereClause("MATCH (n) WHERE n.age > $minAge AND n.age < $maxAge RETURN n");
  ASSERT_NE(where, nullptr);
  auto logical = std::static_pointer_cast<LogicalExpr>(where->condition);
  auto left_comp = std::static_pointer_cast<ComparisonExpr>(logical->left);
  auto right_comp = std::static_pointer_cast<ComparisonExpr>(logical->right);
  
  auto left_param = std::static_pointer_cast<ParameterExpr>(left_comp->right);
  auto right_param = std::static_pointer_cast<ParameterExpr>(right_comp->right);
  
  EXPECT_EQ(left_param->name, "minAge");
  EXPECT_EQ(right_param->name, "maxAge");
}

// ============================================================================
// Function Call Tests
// ============================================================================

TEST(WhereClauseTest, BuiltInFunction) {
  auto where = GetWhereClause("MATCH (n) WHERE n.name CONTAINS 'li' RETURN n");
  ASSERT_NE(where, nullptr);
  auto func = std::static_pointer_cast<FunctionCallExpr>(where->condition);
  EXPECT_EQ(func->name, "CONTAINS");
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST(WhereClauseTest, ParsesWithoutError) {
  EXPECT_TRUE(ParseAndCheck("MATCH (n) WHERE n.age > 0 RETURN n"));
  EXPECT_TRUE(ParseAndCheck("MATCH (n) WHERE n.name = 'test' AND n.active = true RETURN n"));
  EXPECT_TRUE(ParseAndCheck("MATCH (n) WHERE NOT n.deleted RETURN n"));
  EXPECT_TRUE(ParseAndCheck("MATCH (n) WHERE n.list[0] = 'first' RETURN n"));
}
