// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include <gtest/gtest.h>
#include "cedar/cypher/parser.h"
#include "cedar/cypher/execution_plan.h"

using namespace cedar;
using namespace cedar::cypher;

TEST(SubQueryExecution, ParseAndPlanMatchNode) {
  std::string query = "MATCH (n {id: 100}) RETURN n";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  ASSERT_TRUE(stmt != nullptr);

  auto plan = ExecutionPlanBuilder::Build(stmt);
  ASSERT_TRUE(plan != nullptr);

  EXPECT_EQ(plan->GetName(), "ProduceResults");
}

TEST(SubQueryExecution, ParseAndPlanExpand) {
  std::string query = "MATCH (a)-[:KNOWS]->(b) RETURN a, b";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  ASSERT_TRUE(stmt != nullptr);

  auto plan = ExecutionPlanBuilder::Build(stmt);
  ASSERT_TRUE(plan != nullptr);

  EXPECT_EQ(plan->GetName(), "ProduceResults");
}
