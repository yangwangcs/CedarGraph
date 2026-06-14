// Test EXPLAIN and PROFILE Cypher syntax
#include <iostream>
#include "cedar/cypher/parser.h"
#include "cedar/cypher/execution_plan.h"

using namespace cedar::cypher;

void TestExplainPrefix(const std::string& query) {
  // Simulate what GraphServiceRouter does: strip EXPLAIN prefix
  std::string effective_query = query;
  size_t start = effective_query.find_first_not_of(" \t\n\r");
  if (start != std::string::npos) {
    effective_query = effective_query.substr(start);
  }
  
  bool is_explain = false;
  if (effective_query.size() > 8) {
    std::string prefix = effective_query.substr(0, 8);
    std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::toupper);
    if (prefix == "EXPLAIN ") {
      is_explain = true;
      effective_query = effective_query.substr(8);
      size_t s = effective_query.find_first_not_of(" \t\n\r");
      if (s != std::string::npos) effective_query = effective_query.substr(s);
    }
  }
  
  bool is_profile = false;
  if (effective_query.size() > 8) {
    std::string prefix = effective_query.substr(0, 8);
    std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::toupper);
    if (prefix == "PROFILE ") {
      is_profile = true;
      effective_query = effective_query.substr(8);
      size_t s = effective_query.find_first_not_of(" \t\n\r");
      if (s != std::string::npos) effective_query = effective_query.substr(s);
    }
  }
  
  std::cout << "Input:  \"" << query << "\"" << std::endl;
  std::cout << "  EXPLAIN: " << (is_explain ? "YES" : "NO") << std::endl;
  std::cout << "  PROFILE: " << (is_profile ? "YES" : "NO") << std::endl;
  std::cout << "  Query:   \"" << effective_query << "\"" << std::endl;
  
  // Parse the effective query
  CypherParser parser(effective_query);
  auto stmt = parser.ParseStatement();
  if (stmt) {
    std::cout << "  Parse:   OK (" << stmt->clauses.size() << " clauses)" << std::endl;
    
    // Build execution plan
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    if (plan) {
      ExecutionPlan plan_wrapper(plan);
      std::cout << "  Plan:" << std::endl;
      std::cout << plan_wrapper.Explain();
    } else {
      std::cout << "  Plan:    FAILED" << std::endl;
    }
  } else {
    std::cout << "  Parse:   FAILED - " << parser.GetError() << std::endl;
  }
  std::cout << "---" << std::endl;
}

int main() {
  std::cout << "=== EXPLAIN/PROFILE Cypher Syntax Test ===" << std::endl << std::endl;
  
  // Test EXPLAIN (uppercase)
  TestExplainPrefix("EXPLAIN MATCH (n) RETURN n");
  
  // Test explain (lowercase)
  TestExplainPrefix("explain MATCH (n) RETURN n");
  
  // Test Explain (mixed case)
  TestExplainPrefix("Explain MATCH (n) RETURN n");
  
  // Test PROFILE (uppercase)
  TestExplainPrefix("PROFILE MATCH (n) RETURN n");
  
  // Test profile (lowercase)
  TestExplainPrefix("profile MATCH (n) RETURN n");
  
  // Test without prefix
  TestExplainPrefix("MATCH (n) RETURN n");
  
  // Test with WHERE clause
  TestExplainPrefix("EXPLAIN MATCH (n) WHERE n.age > 30 RETURN n.name");
  
  std::cout << "=== All tests completed ===" << std::endl;
  return 0;
}
