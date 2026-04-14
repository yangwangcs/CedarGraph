#include <gtest/gtest.h>
#include "cedar/cypher/validator.h"
#include "cedar/queryd/meta_client.h"

using namespace cedar::cypher;
using namespace cedar::queryd;

TEST(QueryValidatorTest, EmptyQueryPasses) {
  GraphSchema schema;
  QueryValidator validator(&schema);
  QueryStatement stmt;
  EXPECT_TRUE(validator.Validate(stmt).ok());
}

TEST(QueryValidatorTest, UnknownNodeLabelFails) {
  GraphSchema schema;
  LabelSchema ls;
  ls.name = "Person";
  schema.node_labels["Person"] = ls;
  QueryValidator validator(&schema);

  QueryStatement stmt;
  auto match = std::make_shared<MatchClause>();
  NodePattern node;
  node.variable = "n";
  node.labels = {"UnknownLabel"};
  PathPattern pattern;
  pattern.elements.push_back(node);
  match->patterns.push_back(std::move(pattern));
  stmt.clauses.push_back(match);

  EXPECT_FALSE(validator.Validate(stmt).ok());
}

TEST(QueryValidatorTest, UndefinedVariableInWhereFails) {
  GraphSchema schema;
  LabelSchema ls;
  ls.name = "Person";
  schema.node_labels["Person"] = ls;
  QueryValidator validator(&schema);

  QueryStatement stmt;
  auto match = std::make_shared<MatchClause>();
  NodePattern node;
  node.variable = "n";
  node.labels = {"Person"};
  PathPattern pattern;
  pattern.elements.push_back(node);
  match->patterns.push_back(std::move(pattern));
  stmt.clauses.push_back(match);

  auto where = std::make_shared<WhereClause>();
  where->condition = std::make_shared<VariableExpr>("x");
  stmt.clauses.push_back(where);

  EXPECT_FALSE(validator.Validate(stmt).ok());
}

TEST(QueryValidatorTest, NonBooleanWhereFails) {
  GraphSchema schema;
  QueryValidator validator(&schema);

  QueryStatement stmt;
  auto match = std::make_shared<MatchClause>();
  NodePattern node;
  node.variable = "n";
  PathPattern pattern;
  pattern.elements.push_back(node);
  match->patterns.push_back(std::move(pattern));
  stmt.clauses.push_back(match);

  auto where = std::make_shared<WhereClause>();
  where->condition = std::make_shared<LiteralExpr>(Value(42));
  stmt.clauses.push_back(where);

  EXPECT_FALSE(validator.Validate(stmt).ok());
}
