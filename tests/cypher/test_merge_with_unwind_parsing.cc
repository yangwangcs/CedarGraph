// Copyright 2025 The Cedar Authors

#include <gtest/gtest.h>
#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

// ============================================================================
// MERGE Parsing Tests
// ============================================================================

TEST(MergeClauseTest, ParseSingleNodeMerge) {
  CypherParser parser("MERGE (n:Person {name: 'Alice'}) RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  ASSERT_EQ(stmt->clauses.size(), 2);
  auto merge = std::dynamic_pointer_cast<MergeClause>(stmt->clauses[0]);
  ASSERT_NE(merge, nullptr);
  ASSERT_EQ(merge->patterns.size(), 1);
  auto* node = std::get_if<NodePattern>(&merge->patterns[0].elements[0]);
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->variable, "n");
  EXPECT_EQ(node->labels[0], "Person");
}

TEST(MergeClauseTest, ParseMergePatternWithRel) {
  CypherParser parser("MERGE (a)-[r:KNOWS]->(b) RETURN a, b");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  auto merge = std::dynamic_pointer_cast<MergeClause>(stmt->clauses[0]);
  ASSERT_NE(merge, nullptr);
  ASSERT_EQ(merge->patterns[0].elements.size(), 3);
}

// ============================================================================
// WITH Parsing Tests
// ============================================================================

TEST(WithClauseTest, ParseSimpleWith) {
  CypherParser parser("MATCH (n) WITH n.name AS personName RETURN personName");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  ASSERT_EQ(stmt->clauses.size(), 3);
  auto with_clause = std::dynamic_pointer_cast<WithClause>(stmt->clauses[1]);
  ASSERT_NE(with_clause, nullptr);
  ASSERT_EQ(with_clause->items.size(), 1);
  EXPECT_EQ(with_clause->items[0].alias, "personName");
}

TEST(WithClauseTest, ParseWithDistinct) {
  CypherParser parser("MATCH (n) WITH DISTINCT n.age AS age RETURN age");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  auto with_clause = std::dynamic_pointer_cast<WithClause>(stmt->clauses[1]);
  ASSERT_NE(with_clause, nullptr);
  EXPECT_TRUE(with_clause->distinct);
}

// ============================================================================
// UNWIND Parsing Tests
// ============================================================================

TEST(UnwindClauseTest, ParseUnwindLiteralList) {
  CypherParser parser("UNWIND [1, 2, 3] AS x RETURN x");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  ASSERT_EQ(stmt->clauses.size(), 2);
  auto unwind = std::dynamic_pointer_cast<UnwindClause>(stmt->clauses[0]);
  ASSERT_NE(unwind, nullptr);
  EXPECT_EQ(unwind->alias, "x");
  ASSERT_NE(unwind->expression, nullptr);
  EXPECT_EQ(unwind->expression->expr_type, ExprType::LIST_LITERAL);
}

TEST(UnwindClauseTest, ParseUnwindVariable) {
  CypherParser parser("MATCH (n) UNWIND n.tags AS tag RETURN tag");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  auto unwind = std::dynamic_pointer_cast<UnwindClause>(stmt->clauses[1]);
  ASSERT_NE(unwind, nullptr);
  EXPECT_EQ(unwind->alias, "tag");
  EXPECT_EQ(unwind->expression->expr_type, ExprType::PROPERTY);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
