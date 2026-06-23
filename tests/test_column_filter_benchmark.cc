// Column Filtering Performance Benchmark
// Tests the performance improvement of projection pushdown

#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <vector>

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

class ColumnFilterBenchmark : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a simple graph with nodes having multiple properties
    // We'll test the projection pushdown by measuring plan construction time
  }
};

TEST_F(ColumnFilterBenchmark, ProjectionPushdownBasic) {
  // Test 1: Query with single column
  {
    std::string query = "MATCH (n) RETURN n.name";
    CypherParser parser(query);
    auto stmt = parser.ParseStatement();
    ASSERT_NE(stmt, nullptr);
    
    auto start = std::chrono::high_resolution_clock::now();
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Single column query plan build: " << duration.count() << " μs" << std::endl;
    
    ASSERT_NE(plan, nullptr);
    
    // Verify plan structure
    ExecutionPlan wrapper(plan);
    std::string explain = wrapper.Explain();
    std::cout << "Plan:\n" << explain << std::endl;
  }
  
  // Test 2: Query with multiple columns
  {
    std::string query = "MATCH (n) RETURN n.name, n.age, n.email, n.phone, n.address";
    CypherParser parser(query);
    auto stmt = parser.ParseStatement();
    ASSERT_NE(stmt, nullptr);
    
    auto start = std::chrono::high_resolution_clock::now();
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Multiple columns query plan build: " << duration.count() << " μs" << std::endl;
    
    ASSERT_NE(plan, nullptr);
  }
  
  // Test 3: Query with all columns (no filtering)
  {
    std::string query = "MATCH (n) RETURN n";
    CypherParser parser(query);
    auto stmt = parser.ParseStatement();
    ASSERT_NE(stmt, nullptr);
    
    auto start = std::chrono::high_resolution_clock::now();
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "All columns query plan build: " << duration.count() << " μs" << std::endl;
    
    ASSERT_NE(plan, nullptr);
  }
}

TEST_F(ColumnFilterBenchmark, PredicatePushdownBasic) {
  // Test 1: Single predicate
  {
    std::string query = "MATCH (n) WHERE n.age > 30 RETURN n.name";
    CypherParser parser(query);
    auto stmt = parser.ParseStatement();
    ASSERT_NE(stmt, nullptr);
    
    auto start = std::chrono::high_resolution_clock::now();
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Single predicate plan build: " << duration.count() << " μs" << std::endl;
    
    ASSERT_NE(plan, nullptr);
    
    ExecutionPlan wrapper(plan);
    std::string explain = wrapper.Explain();
    std::cout << "Plan:\n" << explain << std::endl;
  }
  
  // Test 2: Multiple predicates (AND)
  {
    std::string query = "MATCH (n) WHERE n.age > 30 AND n.name = 'Alice' RETURN n.email";
    CypherParser parser(query);
    auto stmt = parser.ParseStatement();
    ASSERT_NE(stmt, nullptr);
    
    auto start = std::chrono::high_resolution_clock::now();
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Multiple predicates plan build: " << duration.count() << " μs" << std::endl;
    
    ASSERT_NE(plan, nullptr);
    
    ExecutionPlan wrapper(plan);
    std::string explain = wrapper.Explain();
    std::cout << "Plan:\n" << explain << std::endl;
  }
}

TEST_F(ColumnFilterBenchmark, CBOCostEstimation) {
  // Test CBO cost estimation for different plans
  CostBasedOptimizer optimizer;
  
  // Create a simple plan: NodeScan → Project
  auto scan = std::make_shared<NodeScan>("n", std::nullopt, 
      std::map<std::string, std::shared_ptr<Expression>>{},
      std::unordered_set<std::string>{"n.name", "n.age"});
  
  // Estimate cost
  auto cost = optimizer.EstimateCost(scan.get(), nullptr);
  
  std::cout << "NodeScan with 2 columns:" << std::endl;
  std::cout << "  I/O cost: " << cost.io_cost << std::endl;
  std::cout << "  CPU cost: " << cost.cpu_cost << std::endl;
  std::cout << "  Memory cost: " << cost.memory_cost << std::endl;
  std::cout << "  Total cost: " << cost.TotalCost() << std::endl;
  
  // Compare with full scan
  auto full_scan = std::make_shared<NodeScan>("n");
  auto full_cost = optimizer.EstimateCost(full_scan.get(), nullptr);
  
  std::cout << "\nNodeScan with all columns:" << std::endl;
  std::cout << "  I/O cost: " << full_cost.io_cost << std::endl;
  std::cout << "  CPU cost: " << full_cost.cpu_cost << std::endl;
  std::cout << "  Memory cost: " << full_cost.memory_cost << std::endl;
  std::cout << "  Total cost: " << full_cost.TotalCost() << std::endl;
}

TEST_F(ColumnFilterBenchmark, ExplainOutputComparison) {
  // Compare EXPLAIN output for different queries
  
  // Query 1: Single column
  {
    std::string query = "MATCH (n) RETURN n.name";
    CypherParser parser(query);
    auto stmt = parser.ParseStatement();
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    ExecutionPlan wrapper(plan);
    
    std::cout << "=== EXPLAIN: MATCH (n) RETURN n.name ===" << std::endl;
    std::cout << wrapper.Explain() << std::endl;
  }
  
  // Query 2: With predicate
  {
    std::string query = "MATCH (n) WHERE n.age > 30 RETURN n.name";
    CypherParser parser(query);
    auto stmt = parser.ParseStatement();
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    ExecutionPlan wrapper(plan);
    
    std::cout << "=== EXPLAIN: MATCH (n) WHERE n.age > 30 RETURN n.name ===" << std::endl;
    std::cout << wrapper.Explain() << std::endl;
  }
  
  // Query 3: With expand
  {
    std::string query = "MATCH (n)-[r]->(m) RETURN n.name, m.name";
    CypherParser parser(query);
    auto stmt = parser.ParseStatement();
    auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
    ExecutionPlan wrapper(plan);
    
    std::cout << "=== EXPLAIN: MATCH (n)-[r]->(m) RETURN n.name, m.name ===" << std::endl;
    std::cout << wrapper.Explain() << std::endl;
  }
}
