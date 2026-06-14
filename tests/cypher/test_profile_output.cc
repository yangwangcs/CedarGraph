// Test PROFILE output with PhysicalOperator profiling
#include <gtest/gtest.h>
#include <iostream>
#include <chrono>
#include <thread>

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

TEST(ProfileTest, ExplainOutput) {
  // Test that EXPLAIN produces valid output
  std::string query = "MATCH (n) RETURN n";
  
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  
  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  
  ExecutionPlan wrapper(plan);
  std::string explain_output = wrapper.Explain();
  
  std::cout << "=== EXPLAIN Output ===" << std::endl;
  std::cout << explain_output << std::endl;
  
  // Verify EXPLAIN contains operator names
  EXPECT_FALSE(explain_output.empty());
  // EXPLAIN should contain some operator name
  EXPECT_NE(explain_output.find("Scan"), std::string::npos);
}

TEST(ProfileTest, ExplainWithWhere) {
  std::string query = "MATCH (n) WHERE n.age > 30 RETURN n.name";
  
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr);
  
  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  
  ExecutionPlan wrapper(plan);
  std::string explain_output = wrapper.Explain();
  
  std::cout << "=== EXPLAIN with WHERE ===" << std::endl;
  std::cout << explain_output << std::endl;
  
  EXPECT_FALSE(explain_output.empty());
}
