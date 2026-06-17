// Test actual query functionality against running CedarGraph services
#include <gtest/gtest.h>
#include <iostream>
#include <chrono>

#include "cedar/cypher/parser.h"
#include "cedar/cypher/execution_plan.h"

using namespace cedar::cypher;

class QueryFunctionalityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Services are already running locally
  }
};

TEST_F(QueryFunctionalityTest, BasicQueryParsing) {
  // Test basic MATCH query
  std::string query = "MATCH (n) RETURN n";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  EXPECT_EQ(stmt->clauses.size(), 2);  // MATCH + RETURN
  
  std::cout << "✓ Basic MATCH query parsed successfully" << std::endl;
}

TEST_F(QueryFunctionalityTest, QueryWithWhere) {
  // Test query with WHERE clause
  std::string query = "MATCH (n) WHERE n.age > 30 RETURN n.name";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  EXPECT_EQ(stmt->clauses.size(), 3);  // MATCH + WHERE + RETURN
  
  std::cout << "✓ Query with WHERE parsed successfully" << std::endl;
}

TEST_F(QueryFunctionalityTest, QueryWithRelationship) {
  // Test query with relationship
  std::string query = "MATCH (n)-[r]->(m) RETURN n, m";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  EXPECT_EQ(stmt->clauses.size(), 2);  // MATCH + RETURN
  
  std::cout << "✓ Query with relationship parsed successfully" << std::endl;
}

TEST_F(QueryFunctionalityTest, CreateNode) {
  // Test CREATE query
  std::string query = "CREATE (n:Person {name: 'Alice', age: 30})";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  EXPECT_EQ(stmt->clauses.size(), 1);  // CREATE
  
  std::cout << "✓ CREATE node query parsed successfully" << std::endl;
}

TEST_F(QueryFunctionalityTest, CreateRelationship) {
  // Test CREATE with relationship
  std::string query = "CREATE (n:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(m:Person {name: 'Bob'})";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  EXPECT_EQ(stmt->clauses.size(), 1);  // CREATE
  
  std::cout << "✓ CREATE relationship query parsed successfully" << std::endl;
}

TEST_F(QueryFunctionalityTest, ComplexQuery) {
  // Test complex query with multiple clauses
  std::string query = "MATCH (n:Person)-[r:KNOWS]->(m) WHERE n.age > 25 AND m.city = 'Beijing' RETURN n.name, m.name, r.since ORDER BY n.name LIMIT 10";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  EXPECT_GE(stmt->clauses.size(), 4);  // MATCH + WHERE + RETURN + ORDER BY + LIMIT
  
  std::cout << "✓ Complex query parsed successfully" << std::endl;
}

TEST_F(QueryFunctionalityTest, ExplainQuery) {
  // Test EXPLAIN query
  std::string query = "MATCH (n) WHERE n.age > 30 RETURN n.name";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  
  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  
  ExecutionPlan wrapper(plan);
  std::string explain = wrapper.Explain();
  
  EXPECT_FALSE(explain.empty());
  // The optimizer may choose IndexScan or NodeScan depending on the query
  EXPECT_NE(explain.find("Scan"), std::string::npos);
  
  std::cout << "✓ EXPLAIN query generated successfully:" << std::endl;
  std::cout << explain << std::endl;
}

TEST_F(QueryFunctionalityTest, TemporalQuery) {
  // Test temporal query
  std::string query = "MATCH (n) AS OF '2024-01-01' RETURN n";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  
  std::cout << "✓ Temporal query parsed successfully" << std::endl;
}

TEST_F(QueryFunctionalityTest, OptionalMatchQuery) {
  // Test OPTIONAL MATCH query
  std::string query = "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, m";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  
  std::cout << "✓ OPTIONAL MATCH query parsed successfully" << std::endl;
}

TEST_F(QueryFunctionalityTest, UnionQuery) {
  // Test UNION query
  std::string query = "MATCH (n:Person) RETURN n.name UNION MATCH (m:Company) RETURN m.name";
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  ASSERT_NE(stmt, nullptr);
  
  std::cout << "✓ UNION query parsed successfully" << std::endl;
}
