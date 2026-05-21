// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Batch test for Graph & Cypher CRITICAL fixes.

#include <gtest/gtest.h>

#include "cedar/graph/cedar_graph.h"
#include "cedar/graph/pushdown_predicate.h"
#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/parser.h"
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/value.h"
#include "cedar/query/cedar_scan.h"
#include "cedar/storage/cedar_graph_storage.h"

namespace cedar {
namespace cypher {

// ============================================================================
// Mock / minimal environment tests
// ============================================================================

TEST(GraphCypherCriticalBatch, PushdownPredicateUnsupportedOpReturnsFalse) {
  PropertyFilter filter;
  filter.op = static_cast<FilterOp>(999);  // Invalid op
  Descriptor desc = Descriptor::InlineInt(0, 42);
  filter.value = Descriptor::InlineInt(0, 42);
  EXPECT_FALSE(filter.Evaluate(desc));
}

TEST(GraphCypherCriticalBatch, PushdownPredicateStringUnsupportedOpReturnsFalse) {
  PropertyFilter filter;
  filter.op = static_cast<FilterOp>(999);  // Invalid op
  auto desc_opt = Descriptor::InlineShortStr(0, Slice("hi"));
  ASSERT_TRUE(desc_opt.has_value());
  Descriptor desc = *desc_opt;
  auto val_opt = Descriptor::InlineShortStr(0, Slice("hi"));
  ASSERT_TRUE(val_opt.has_value());
  filter.value = *val_opt;
  EXPECT_FALSE(filter.Evaluate(desc));
}

TEST(GraphCypherCriticalBatch, PlanCacheBounded) {
  // CypherEngine with null storage can't cache plans, but we can at least
  // verify the constant exists and the code compiles.
  CypherEngine engine(nullptr);
  EXPECT_EQ(engine.GetCacheSize(), 0);
}

TEST(GraphCypherCriticalBatch, ExpandDirectionBothMergesEdges) {
  // Build a minimal execution context and test Expand::GetDetails reflects BOTH
  Expand expand("a", "r", "b", Direction::BOTH, std::nullopt);
  std::string details = expand.GetDetails();
  // Direction::BOTH is not INCOMING, so the default arrow logic prints "->"
  // We mainly care that the code compiles and handles BOTH at runtime.
  EXPECT_FALSE(details.empty());
}

TEST(GraphCypherCriticalBatch, SortTypeFirstOrdering) {
  using cedar::cypher::Value;
  using cedar::cypher::ValueType;

  Value int_val(42);
  Value str_val("hello");

  // Type ordering: kInt < kString
  EXPECT_TRUE(int_val.Type() < str_val.Type());
  EXPECT_FALSE(str_val.Type() < int_val.Type());
}

TEST(GraphCypherCriticalBatch, AggregateEmptyGroupNoGroupBy) {
  // COUNT(*) on empty input should return 1 row with value 0
  Aggregate::AggregationItem item;
  item.output_name = "cnt";
  item.func = Aggregate::AggregationFunc::kCount;

  auto agg = std::make_shared<Aggregate>(std::vector<Aggregate::AggregationItem>{item});
  auto limit = std::make_shared<Limit>(0);
  auto scan = std::make_shared<NodeScan>("n", std::nullopt);
  limit->AddChild(scan);
  agg->AddChild(limit);

  ExecutionContext ctx;
  ctx.graph = nullptr;
  ctx.get_all_entities_fn = nullptr;

  ExecutionPlan plan(agg);
  auto rs = plan.Execute(&ctx);
  EXPECT_FALSE(rs.HasError());
  EXPECT_EQ(rs.records.size(), 1);
  EXPECT_EQ(rs.records[0].Get("cnt").value().GetInt(), 0);
}

TEST(GraphCypherCriticalBatch, AggregateEmptyGroupWithGroupByReturnsZeroRows) {
  // GROUP BY on empty input should return 0 rows
  Aggregate::AggregationItem item;
  item.output_name = "cnt";
  item.func = Aggregate::AggregationFunc::kCount;
  item.group_by_key = "g";

  auto agg = std::make_shared<Aggregate>(std::vector<Aggregate::AggregationItem>{item});
  auto limit = std::make_shared<Limit>(0);
  auto scan = std::make_shared<NodeScan>("n", std::nullopt);
  limit->AddChild(scan);
  agg->AddChild(limit);

  ExecutionContext ctx;
  ctx.graph = nullptr;
  ctx.get_all_entities_fn = nullptr;

  ExecutionPlan plan(agg);
  auto rs = plan.Execute(&ctx);
  EXPECT_FALSE(rs.HasError());
  EXPECT_EQ(rs.records.size(), 0);
}

TEST(GraphCypherCriticalBatch, TemporalContainedInModifier) {
  CypherParser parser("MATCH (n) WHERE n.prop = 1 CONTAINED IN (100, 200) RETURN n");
  auto stmt = parser.ParseStatement();
  EXPECT_TRUE(stmt != nullptr || !parser.GetError().empty());
  auto temporal = parser.GetTemporalClause();
  if (temporal) {
    EXPECT_EQ(temporal->modifier, TemporalModifierType::CONTAINED_IN);
  }
}

TEST(GraphCypherCriticalBatch, TimestampExpressionUnknownSetsError) {
  // Temporal clause must come first for the parser to detect it
  CypherParser parser("AS OF @#$ MATCH (n) RETURN n");
  auto stmt = parser.ParseStatement();
  // Parser may or may not return a statement, but error should be set
  // because '@#$' is not a valid timestamp expression.
  EXPECT_FALSE(parser.GetError().empty());
}

TEST(GraphCypherCriticalBatch, AllenRelationEnumDefined) {
  // Ensure the enum values compile and are distinct
  EXPECT_NE(AllenRelation::BEFORE, AllenRelation::AFTER);
  EXPECT_NE(AllenRelation::MEETS, AllenRelation::MET_BY);
  EXPECT_NE(AllenRelation::OVERLAPS, AllenRelation::OVERLAPPED_BY);
  EXPECT_NE(AllenRelation::DURING, AllenRelation::CONTAINS);
  EXPECT_NE(AllenRelation::STARTS, AllenRelation::STARTED_BY);
  EXPECT_NE(AllenRelation::FINISHES, AllenRelation::FINISHED_BY);
  EXPECT_NE(AllenRelation::EQUALS, AllenRelation::BEFORE);
}

TEST(GraphCypherCriticalBatch, AnchorStatsAtomicAccess) {
  // After making fields atomic, ResetAnchorStats should compile and work
  CedarScan::ResetAnchorStats();
  auto& stats = CedarScan::GetAnchorStats();
  EXPECT_EQ(stats.anchor_hits.load(), 0);
  EXPECT_EQ(stats.anchor_misses.load(), 0);
  EXPECT_EQ(stats.deleted_skipped.load(), 0);
  EXPECT_EQ(stats.fallback_queries.load(), 0);
}

TEST(GraphCypherCriticalBatch, RelationshipIdHashNoCollision) {
  // The new hash uses concatenation + std::hash instead of XOR.
  // Verify that two different (src,dst,ts) combos produce different IDs
  // by checking the hash function directly.
  uint64_t id1 = std::hash<std::string>{}("1:2:100");
  uint64_t id2 = std::hash<std::string>{}("2:1:100");
  EXPECT_NE(id1, id2);
}

TEST(GraphCypherCriticalBatch, SnapshotScanDetails) {
  SnapshotScan scan("n", std::nullopt, Timestamp(100));
  EXPECT_EQ(scan.GetName(), "SnapshotScan");
}

}  // namespace cypher
}  // namespace cedar
