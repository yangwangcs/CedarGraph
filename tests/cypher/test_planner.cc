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

#include "cedar/cypher/planner.h"
#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

TEST(QueryPlanner, PlanSimpleMatchReturn) {
  QueryPlanner planner;
  
  // Parse a simple query
  CypherParser parser("MATCH (n:Person) RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  
  // Plan the query
  auto plan = planner.Plan(*stmt);
  ASSERT_NE(plan, nullptr) << "Plan failed: " << planner.GetLastError();
  
  // The plan should have an EXPLAIN output
  std::string explain = plan->Explain();
  EXPECT_FALSE(explain.empty());
  
  // Should contain ProduceResults (from RETURN) and NodeScan (from MATCH)
  EXPECT_NE(explain.find("ProduceResults"), std::string::npos);
  EXPECT_NE(explain.find("NodeScan"), std::string::npos);
}

TEST(QueryPlanner, PlanMatchWhereReturn) {
  QueryPlanner planner;
  
  CypherParser parser("MATCH (n:Person) WHERE n.age > 25 RETURN n.name, n.age");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  
  auto plan = planner.Plan(*stmt);
  ASSERT_NE(plan, nullptr) << "Plan failed: " << planner.GetLastError();
  
  std::string explain = plan->Explain();
  EXPECT_FALSE(explain.empty());
  
  // Should contain Filter (from WHERE) and Project (from RETURN fields)
  EXPECT_NE(explain.find("Filter"), std::string::npos);
  EXPECT_NE(explain.find("Project"), std::string::npos);
}

TEST(QueryPlanner, PlanMatchOrderByLimit) {
  QueryPlanner planner;
  
  CypherParser parser("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 10");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  
  auto plan = planner.Plan(*stmt);
  ASSERT_NE(plan, nullptr) << "Plan failed: " << planner.GetLastError();
  
  std::string explain = plan->Explain();
  EXPECT_FALSE(explain.empty());
  
  // Should contain Sort and Limit
  EXPECT_NE(explain.find("Sort"), std::string::npos);
  EXPECT_NE(explain.find("Limit"), std::string::npos);
}

TEST(QueryPlanner, EmptyQueryReturnsNull) {
  QueryPlanner planner;
  
  // Create an empty statement manually
  QueryStatement stmt;
  
  auto plan = planner.Plan(stmt);
  EXPECT_EQ(plan, nullptr);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
