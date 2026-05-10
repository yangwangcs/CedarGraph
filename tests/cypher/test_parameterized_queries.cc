#include <gtest/gtest.h>
#include "cedar/cypher/parser.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/cypher_engine.h"

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
// Parameterized Query Parser Tests
// ============================================================================

TEST(ParameterizedQueryTest, ParseParameter) {
  auto where = GetWhereClause("MATCH (n) WHERE n.name = $name RETURN n");
  ASSERT_NE(where, nullptr);
  auto comp = std::static_pointer_cast<ComparisonExpr>(where->condition);
  auto param = std::static_pointer_cast<ParameterExpr>(comp->right);
  EXPECT_EQ(param->name, "name");
}

TEST(ParameterizedQueryTest, MultipleParameters) {
  auto where = GetWhereClause("MATCH (n) WHERE n.age > $min AND n.age < $max RETURN n");
  ASSERT_NE(where, nullptr);
  auto logical = std::static_pointer_cast<LogicalExpr>(where->condition);
  
  auto left_comp = std::static_pointer_cast<ComparisonExpr>(logical->left);
  auto right_comp = std::static_pointer_cast<ComparisonExpr>(logical->right);
  
  auto left_param = std::static_pointer_cast<ParameterExpr>(left_comp->right);
  auto right_param = std::static_pointer_cast<ParameterExpr>(right_comp->right);
  
  EXPECT_EQ(left_param->name, "min");
  EXPECT_EQ(right_param->name, "max");
}

TEST(ParameterizedQueryTest, ParameterInFunction) {
  auto where = GetWhereClause("MATCH (n) WHERE n.name CONTAINS $search RETURN n");
  ASSERT_NE(where, nullptr);
  auto func = std::static_pointer_cast<FunctionCallExpr>(where->condition);
  auto param = std::static_pointer_cast<ParameterExpr>(func->arguments[1]);
  EXPECT_EQ(param->name, "search");
}

// ============================================================================
// Expression Evaluator Tests with Parameters
// ============================================================================

TEST(ParameterizedQueryTest, EvaluateParameter) {
  ExecutionContext ctx;
  Record record;
  
  ExpressionEvaluator evaluator(&ctx);
  evaluator.AddParameter("limit", Value(10));
  
  auto expr = std::make_shared<ParameterExpr>("limit");
  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsInt());
  EXPECT_EQ(result.GetInt(), 10);
}

TEST(ParameterizedQueryTest, ParameterFallbackToContext) {
  ExecutionContext ctx;
  ctx.variables["x"] = Value(42);
  Record record;
  
  ExpressionEvaluator evaluator(&ctx);
  auto expr = std::make_shared<ParameterExpr>("x");
  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_EQ(result.GetInt(), 42);
}

TEST(ParameterizedQueryTest, MissingParameterReturnsNull) {
  ExecutionContext ctx;
  Record record;
  
  ExpressionEvaluator evaluator(&ctx);
  auto expr = std::make_shared<ParameterExpr>("undefined");
  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsNull());
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST(ParameterizedQueryIntegration, ParseComplexQuery) {
  std::string query = R"(
    MATCH (p:Person)-[r:KNOWS]->(friend)
    WHERE p.age > $minAge AND p.name CONTAINS $namePart 
      AND r.since > $year
    RETURN p, r, friend
  )";
  
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  EXPECT_NE(stmt, nullptr);
  EXPECT_TRUE(parser.GetError().empty());
  
  // Check WHERE clause exists
  bool has_where = false;
  for (const auto& clause : stmt->clauses) {
    if (std::dynamic_pointer_cast<WhereClause>(clause)) {
      has_where = true;
      break;
    }
  }
  EXPECT_TRUE(has_where);
}
