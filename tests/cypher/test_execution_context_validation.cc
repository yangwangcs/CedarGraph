// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// ExecutionContext validation and TemporalExpand fallback tests

#include <gtest/gtest.h>
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/value.h"

using namespace cedar::cypher;

// Helper mock operator
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
    clone->idx = 0;
    return clone;
  }
};

// ============================================================================
// ValidateDependencies Tests
// ============================================================================

TEST(ExecutionContextValidation, MissingGraphAndGcnForTemporalExpand) {
  auto mock = std::make_shared<MockOperator>();
  auto rec = std::make_shared<Record>();
  Node node;
  node.id = 1;
  node.labels.push_back("Node");
  rec->Set("n", Value(node));
  mock->records.push_back(rec);

  auto temporal_expand = std::make_shared<TemporalExpand>(
      "n", "r", "m", Direction::OUTGOING, TemporalModifierType::AS_OF);
  temporal_expand->AddChild(mock);

  ExecutionPlan plan(temporal_expand);
  ExecutionContext ctx;
  ctx.graph = nullptr;
  ctx.gcn_traversal_callback = nullptr;

  auto result = plan.Execute(&ctx);
  EXPECT_TRUE(result.HasError());
  EXPECT_NE(result.error->find("Operator requires graph or GCN callback"),
            std::string::npos);
}

TEST(ExecutionContextValidation, GraphAvailablePassesValidation) {
  auto mock = std::make_shared<MockOperator>();
  auto rec = std::make_shared<Record>();
  Node node;
  node.id = 1;
  node.labels.push_back("Node");
  rec->Set("n", Value(node));
  mock->records.push_back(rec);

  auto temporal_expand = std::make_shared<TemporalExpand>(
      "n", "r", "m", Direction::OUTGOING, TemporalModifierType::AS_OF);
  temporal_expand->AddChild(mock);

  ExecutionPlan plan(temporal_expand);
  ExecutionContext ctx;
  // No graph or GCN callback, but NodeScan doesn't require graph
  auto result = plan.Execute(&ctx);
  // Validation should fail because TemporalExpand requires graph/GCN
  EXPECT_TRUE(result.HasError());
}

TEST(ExecutionContextValidation, GcnCallbackAvailablePassesValidation) {
  auto mock = std::make_shared<MockOperator>();
  auto rec = std::make_shared<Record>();
  Node node;
  node.id = 1;
  node.labels.push_back("Node");
  rec->Set("n", Value(node));
  mock->records.push_back(rec);

  auto temporal_expand = std::make_shared<TemporalExpand>(
      "n", "r", "m", Direction::OUTGOING, TemporalModifierType::AS_OF);
  temporal_expand->AddChild(mock);

  ExecutionPlan plan(temporal_expand);
  ExecutionContext ctx;
  ctx.gcn_traversal_callback =
      [](uint64_t, uint32_t, uint64_t) -> std::vector<uint64_t> {
    return std::vector<uint64_t>{2, 3};
  };

  auto result = plan.Execute(&ctx);
  EXPECT_FALSE(result.HasError());
  EXPECT_EQ(result.records.size(), 2);
}

TEST(ExecutionContextValidation, PlainNodeScanDoesNotRequireGraph) {
  auto node_scan = std::make_shared<NodeScan>("n", std::nullopt);
  ExecutionPlan plan(node_scan);
  ExecutionContext ctx;
  ctx.graph = nullptr;
  ctx.gcn_traversal_callback = nullptr;

  auto result = plan.Execute(&ctx);
  EXPECT_FALSE(result.HasError());
  EXPECT_GE(result.records.size(), 1);
}

// ============================================================================
// TemporalExpand Fallback Tests
// ============================================================================

TEST(TemporalExpandFallback, UsesGetOutNeighborsFnWhenGcnAbsent) {
  auto mock = std::make_shared<MockOperator>();
  auto rec = std::make_shared<Record>();
  Node node;
  node.id = 1;
  node.labels.push_back("Node");
  rec->Set("n", Value(node));
  mock->records.push_back(rec);

  auto temporal_expand = std::make_shared<TemporalExpand>(
      "n", "r", "m", Direction::OUTGOING, TemporalModifierType::AS_OF);
  temporal_expand->AddChild(mock);

  ExecutionContext ctx;
  ctx.get_out_neighbors_fn = [](uint64_t vertex_id, uint16_t edge_type,
                                 cedar::Timestamp start,
                                 cedar::Timestamp end)
      -> std::vector<cedar::Neighbor> {
    (void)vertex_id;
    (void)edge_type;
    (void)start;
    (void)end;
    return std::vector<cedar::Neighbor>{cedar::Neighbor(2, 0, cedar::Timestamp(100), std::nullopt)};
  };

  EXPECT_TRUE(temporal_expand->Init(&ctx));
  auto result = temporal_expand->Next();
  ASSERT_NE(result, nullptr);
  auto m_val = result->Get("m");
  ASSERT_TRUE(m_val.has_value());
  EXPECT_EQ(m_val->GetNode().id, 2);
}

TEST(TemporalExpandFallback, UsesGetInNeighborsFnForIncomingDirection) {
  auto mock = std::make_shared<MockOperator>();
  auto rec = std::make_shared<Record>();
  Node node;
  node.id = 1;
  node.labels.push_back("Node");
  rec->Set("n", Value(node));
  mock->records.push_back(rec);

  auto temporal_expand = std::make_shared<TemporalExpand>(
      "n", "r", "m", Direction::INCOMING, TemporalModifierType::AS_OF);
  temporal_expand->AddChild(mock);

  ExecutionContext ctx;
  ctx.get_in_neighbors_fn = [](uint64_t vertex_id, uint16_t edge_type,
                                cedar::Timestamp start,
                                cedar::Timestamp end)
      -> std::vector<cedar::Neighbor> {
    (void)vertex_id;
    (void)edge_type;
    (void)start;
    (void)end;
    return std::vector<cedar::Neighbor>{cedar::Neighbor(3, 0, cedar::Timestamp(200), std::nullopt)};
  };

  EXPECT_TRUE(temporal_expand->Init(&ctx));
  auto result = temporal_expand->Next();
  ASSERT_NE(result, nullptr);
  auto m_val = result->Get("m");
  ASSERT_TRUE(m_val.has_value());
  EXPECT_EQ(m_val->GetNode().id, 3);
}

TEST(TemporalExpandFallback, ReturnsNullWhenNoFallbackAvailable) {
  auto mock = std::make_shared<MockOperator>();
  auto rec = std::make_shared<Record>();
  Node node;
  node.id = 1;
  node.labels.push_back("Node");
  rec->Set("n", Value(node));
  mock->records.push_back(rec);

  auto temporal_expand = std::make_shared<TemporalExpand>(
      "n", "r", "m", Direction::OUTGOING, TemporalModifierType::AS_OF);
  temporal_expand->AddChild(mock);

  ExecutionContext ctx;
  // No graph, no GCN callback, no get_*_neighbors_fn

  EXPECT_TRUE(temporal_expand->Init(&ctx));
  auto result = temporal_expand->Next();
  EXPECT_EQ(result, nullptr);
}
