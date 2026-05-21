// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "cedar/cypher/parser.h"
#include "cedar/cypher/validator.h"

using namespace cedar::cypher;

// Helper to get SET clause from query
std::shared_ptr<SetClause> GetSetClause(const std::string& query) {
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  if (!stmt || !parser.GetError().empty()) {
    return nullptr;
  }
  for (const auto& clause : stmt->clauses) {
    if (auto set = std::dynamic_pointer_cast<SetClause>(clause)) {
      return set;
    }
  }
  return nullptr;
}

// Helper to get DELETE clause from query
std::shared_ptr<DeleteClause> GetDeleteClause(const std::string& query) {
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  if (!stmt || !parser.GetError().empty()) {
    return nullptr;
  }
  for (const auto& clause : stmt->clauses) {
    if (auto del = std::dynamic_pointer_cast<DeleteClause>(clause)) {
      return del;
    }
  }
  return nullptr;
}

// ============================================================================
// SET Clause Parsing Tests
// ============================================================================

TEST(SetClauseTest, SinglePropertyAssignment) {
  auto set = GetSetClause("MATCH (n) SET n.name = 'Alice' RETURN n");
  ASSERT_NE(set, nullptr);
  ASSERT_EQ(set->items.size(), 1);
  EXPECT_EQ(set->items[0].target->expr_type, ExprType::PROPERTY);
  auto prop = std::static_pointer_cast<PropertyExpr>(set->items[0].target);
  EXPECT_EQ(prop->variable, "n");
  EXPECT_EQ(prop->property, "name");
  EXPECT_EQ(set->items[0].value->expr_type, ExprType::LITERAL);
}

TEST(SetClauseTest, MultiplePropertyAssignments) {
  auto set = GetSetClause("MATCH (n) SET n.name = 'Alice', n.age = 30 RETURN n");
  ASSERT_NE(set, nullptr);
  ASSERT_EQ(set->items.size(), 2);
  EXPECT_EQ(set->items[0].target->expr_type, ExprType::PROPERTY);
  EXPECT_EQ(set->items[1].target->expr_type, ExprType::PROPERTY);
}

TEST(SetClauseTest, VariableAssignment) {
  auto set = GetSetClause("MATCH (n) SET n = 5 RETURN n");
  ASSERT_NE(set, nullptr);
  ASSERT_EQ(set->items.size(), 1);
  EXPECT_EQ(set->items[0].target->expr_type, ExprType::VARIABLE);
}

TEST(SetClauseTest, ArithmeticValue) {
  auto set = GetSetClause("MATCH (n) SET n.x = 1 + 2 RETURN n");
  ASSERT_NE(set, nullptr);
  ASSERT_EQ(set->items.size(), 1);
  EXPECT_EQ(set->items[0].value->expr_type, ExprType::ARITHMETIC);
}

// ============================================================================
// DELETE Clause Parsing Tests
// ============================================================================

TEST(DeleteClauseTest, SingleVariable) {
  auto del = GetDeleteClause("MATCH (n) DELETE n");
  ASSERT_NE(del, nullptr);
  ASSERT_EQ(del->expressions.size(), 1);
  EXPECT_EQ(del->expressions[0]->expr_type, ExprType::VARIABLE);
  auto var = std::static_pointer_cast<VariableExpr>(del->expressions[0]);
  EXPECT_EQ(var->name, "n");
}

TEST(DeleteClauseTest, MultipleVariables) {
  auto del = GetDeleteClause("MATCH (n), (m) DELETE n, m");
  ASSERT_NE(del, nullptr);
  ASSERT_EQ(del->expressions.size(), 2);
  EXPECT_EQ(del->expressions[0]->expr_type, ExprType::VARIABLE);
  EXPECT_EQ(del->expressions[1]->expr_type, ExprType::VARIABLE);
}

TEST(DeleteClauseTest, RelationshipVariable) {
  auto del = GetDeleteClause("MATCH (n)-[r]->(m) DELETE r");
  ASSERT_NE(del, nullptr);
  ASSERT_EQ(del->expressions.size(), 1);
  auto var = std::static_pointer_cast<VariableExpr>(del->expressions[0]);
  EXPECT_EQ(var->name, "r");
}

// ============================================================================
// Validation Tests
// ============================================================================

TEST(SetDeleteValidationTest, SetValidatesWithScope) {
  CypherParser parser("MATCH (n) SET n.name = 'Alice' RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  ASSERT_TRUE(parser.GetError().empty());

  QueryValidator validator(nullptr);
  auto status = validator.Validate(*stmt);
  EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST(SetDeleteValidationTest, DeleteValidatesWithScope) {
  CypherParser parser("MATCH (n) DELETE n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  ASSERT_TRUE(parser.GetError().empty());

  QueryValidator validator(nullptr);
  auto status = validator.Validate(*stmt);
  EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST(SetDeleteValidationTest, SetWithUndefinedVariableFails) {
  CypherParser parser("SET n.name = 'Alice' RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  ASSERT_TRUE(parser.GetError().empty());

  QueryValidator validator(nullptr);
  auto status = validator.Validate(*stmt);
  EXPECT_FALSE(status.ok());
}

TEST(SetDeleteValidationTest, DeleteWithUndefinedVariableFails) {
  CypherParser parser("DELETE n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  ASSERT_TRUE(parser.GetError().empty());

  QueryValidator validator(nullptr);
  auto status = validator.Validate(*stmt);
  EXPECT_FALSE(status.ok());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
