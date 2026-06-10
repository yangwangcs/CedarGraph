// Copyright (c) 2025 The Cedar Authors
// Variable-length expand tests

#include <gtest/gtest.h>
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/parser.h"
#include "cedar/graph/cedar_graph.h"

using namespace cedar::cypher;

// Mock operator that yields pre-canned records
class MockOperator : public PhysicalOperator {
 public:
  std::vector<std::shared_ptr<Record>> records;
  size_t idx = 0;
  bool Init(ExecutionContext*) override { return true; }
  std::shared_ptr<Record> Next() override {
    if (idx >= records.size()) return nullptr;
    return records[idx++];
  }
  std::string GetName() const override { return "Mock"; }
  std::unique_ptr<PhysicalOperator> Clone() const override {
    auto clone = std::make_unique<MockOperator>();
    clone->records = records;
    return clone;
  }
};

TEST(VariableLengthParserTest, ParsesHopRange) {
  CypherParser parser("MATCH (a)-[*1..3]->(b) RETURN a, b");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << parser.GetError();

  auto match = std::static_pointer_cast<MatchClause>(stmt->clauses[0]);
  ASSERT_EQ(match->patterns[0].elements.size(), 3);

  const auto& rel = std::get<RelationshipPattern>(match->patterns[0].elements[1]);
  EXPECT_EQ(rel.direction, Direction::OUTGOING);
  ASSERT_TRUE(rel.min_hops.has_value());
  ASSERT_TRUE(rel.max_hops.has_value());
  EXPECT_EQ(rel.min_hops.value(), 1);
  EXPECT_EQ(rel.max_hops.value(), 3);
}

TEST(VariableLengthParserTest, ParsesStarOnly) {
  CypherParser parser("MATCH (a)-[*]->(b) RETURN a");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << parser.GetError();

  auto match = std::static_pointer_cast<MatchClause>(stmt->clauses[0]);
  const auto& rel = std::get<RelationshipPattern>(match->patterns[0].elements[1]);
  EXPECT_FALSE(rel.min_hops.has_value());
  EXPECT_FALSE(rel.max_hops.has_value());
}

TEST(VariableLengthExpandOperatorTest, ConstructorAcceptsHops) {
  auto expand = std::make_shared<VariableLengthExpand>(
      "a", "r", "b",
      Direction::OUTGOING,
      std::nullopt,
      1, 3);

  EXPECT_EQ(expand->GetName(), "VariableLengthExpand");
  std::string details = expand->GetDetails();
  EXPECT_NE(details.find("1"), std::string::npos);
  EXPECT_NE(details.find("3"), std::string::npos);
}

// Mock graph that returns deterministic neighbors
class MockGraph {
 public:
  std::unordered_map<uint64_t, std::vector<uint64_t>> adjacency;

  std::vector<cedar::Neighbor> GetOutNeighbors(uint64_t node, uint16_t,
                                                cedar::Timestamp,
                                                cedar::Timestamp) const {
    std::vector<cedar::Neighbor> result;
    auto it = adjacency.find(node);
    if (it != adjacency.end()) {
      for (uint64_t nid : it->second) {
        cedar::Neighbor n;
        n.id = nid;
        n.edge_type = 0;
        n.timestamp = cedar::Timestamp(0);
        result.push_back(n);
      }
    }
    return result;
  }
};

TEST(VariableLengthExpandOperatorTest, RespectsMaxHops) {
  // Graph: 1 -> 2 -> 3
  MockGraph mock;
  mock.adjacency[1] = {2};
  mock.adjacency[2] = {3};

  // We can't easily inject MockGraph into CedarGraph, but we can test
  // the operator directly by providing a get_out_neighbors_fn in the context.
  ExecutionContext ctx;
  ctx.get_out_neighbors_fn = [&mock](uint64_t node_id, uint16_t edge_type,
                                      cedar::Timestamp, cedar::Timestamp) {
    return mock.GetOutNeighbors(node_id, edge_type, cedar::Timestamp(0),
                                cedar::Timestamp::Max());
  };

  // Build a simple input record with node "a" = id 1
  auto input_op = std::make_shared<MockOperator>();
  auto rec = std::make_shared<Record>();
  cedar::cypher::Node node;
  node.id = 1;
  node.labels.push_back("Node");
  rec->Set("a", Value(node));
  input_op->records.push_back(rec);

  // Expand 1..2 hops
  VariableLengthExpand expand("a", "r", "b",
                               Direction::OUTGOING, std::nullopt, 1, 2);
  expand.AddChild(input_op);
  ASSERT_TRUE(expand.Init(&ctx));

  // Should find: a=1->b=2 (1 hop), a=1->b=3 (2 hops)
  int count = 0;
  while (expand.Next()) ++count;
  EXPECT_EQ(count, 2);
}

TEST(VariableLengthExpandOperatorTest, RespectsMinHops) {
  MockGraph mock;
  mock.adjacency[1] = {2};
  mock.adjacency[2] = {3};

  ExecutionContext ctx;
  ctx.get_out_neighbors_fn = [&mock](uint64_t node_id, uint16_t edge_type,
                                      cedar::Timestamp, cedar::Timestamp) {
    return mock.GetOutNeighbors(node_id, edge_type, cedar::Timestamp(0),
                                cedar::Timestamp::Max());
  };

  auto input_op = std::make_shared<MockOperator>();
  auto rec = std::make_shared<Record>();
  cedar::cypher::Node node;
  node.id = 1;
  node.labels.push_back("Node");
  rec->Set("a", Value(node));
  input_op->records.push_back(rec);

  // Expand 2..2 hops only
  VariableLengthExpand expand("a", "r", "b",
                               Direction::OUTGOING, std::nullopt, 2, 2);
  expand.AddChild(input_op);
  ASSERT_TRUE(expand.Init(&ctx));

  // Should find only: a=1->b=3 (2 hops)
  int count = 0;
  while (expand.Next()) ++count;
  EXPECT_EQ(count, 1);
}
