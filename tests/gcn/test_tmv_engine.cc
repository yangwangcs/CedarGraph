#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "cedar/gcn/tmv_engine.h"

using namespace cedar::gcn;

TEST(TMVEngineTest, BootstrapOneVertex) {
  TMVEngine engine(16);

  std::vector<TMVEdge> edges;
  edges.push_back(
      {100, 1000, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  edges.push_back(
      {200, 2000, std::numeric_limits<uint32_t>::max(), 0, 1, 0});

  engine.BootstrapVertex(42, Direction::kOut, edges, true);

  // Scan outbound at t=1500: only edge to 100 is active (valid_from=1000)
  auto out_edges = engine.ScanAtTime(42, Direction::kOut, 1500);
  EXPECT_EQ(out_edges.size(), 1);
  EXPECT_EQ(out_edges[0].target_id, 100u);

  // Memory reversal: target 100 should have inbound edge from 42
  auto in_edges = engine.ScanAtTime(100, Direction::kIn, 1500);
  EXPECT_EQ(in_edges.size(), 1);
  EXPECT_EQ(in_edges[0].target_id, 42u);

  // Verify vertex and chunk counts
  EXPECT_EQ(engine.VertexCount(), 3);
  EXPECT_EQ(engine.ChunkCount(), 3);
}

TEST(TMVEngineTest, AppendEdgeAndScan) {
  TMVEngine engine(16);

  TMVEdge edge{50, 100, 500, 0, 1, 0};
  engine.AppendEdge(7, Direction::kOut, edge, false);

  auto out_edges = engine.ScanAtTime(7, Direction::kOut, 200);
  EXPECT_EQ(out_edges.size(), 1);
  EXPECT_EQ(out_edges[0].target_id, 50u);

  auto missing = engine.ScanAtTime(7, Direction::kOut, 600);
  EXPECT_EQ(missing.size(), 0);
}

TEST(TMVEngineTest, DropBelowWatermark) {
  TMVEngine engine(16);

  // Vertex 1 gets its own chunk with a short-lived edge
  std::vector<TMVEdge> old_edges;
  old_edges.push_back({10, 0, 100, 0, 1, 0});
  engine.BootstrapVertex(1, Direction::kOut, old_edges, false);

  // Vertex 2 gets its own chunk with a longer-lived edge
  std::vector<TMVEdge> new_edges;
  new_edges.push_back({20, 0, 300, 0, 1, 0});
  engine.BootstrapVertex(2, Direction::kOut, new_edges, false);

  EXPECT_EQ(engine.ScanAtTime(1, Direction::kOut, 50).size(), 1u);
  EXPECT_EQ(engine.ScanAtTime(2, Direction::kOut, 50).size(), 1u);
  EXPECT_EQ(engine.ChunkCount(), 2u);

  size_t dropped = engine.DropBelowWatermark(250);
  EXPECT_EQ(dropped, 1u);

  EXPECT_EQ(engine.ScanAtTime(1, Direction::kOut, 50).size(), 0u);
  EXPECT_EQ(engine.ScanAtTime(2, Direction::kOut, 50).size(), 1u);
  EXPECT_EQ(engine.ChunkCount(), 1u);
}

TEST(TMVEngineTest, TemporalFoldingWithDelete) {
  TMVEngine engine(16);

  // Bootstrap a CREATE edge to target 100 at t=1000 (valid_to=MAX)
  std::vector<TMVEdge> edges;
  edges.push_back(
      {100, 1000, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  engine.BootstrapVertex(1, Direction::kOut, edges, false);

  // Append a DELETE edge to target 100 at t=2000 (valid_to=2000)
  TMVEdge delete_edge{100, 2000, 2000, 0, 1, 0};
  engine.AppendEdge(1, Direction::kOut, delete_edge, false);

  // At t=1500: CREATE is the latest event, edge should exist
  auto at_1500 = engine.ScanAtTime(1, Direction::kOut, 1500);
  EXPECT_EQ(at_1500.size(), 1u);
  EXPECT_EQ(at_1500[0].target_id, 100u);

  // At t=2500: DELETE is the latest event, edge should not exist
  auto at_2500 = engine.ScanAtTime(1, Direction::kOut, 2500);
  EXPECT_EQ(at_2500.size(), 0u);
}

TEST(TMVEngineTest, SIMDScanFindsMultipleEdges) {
  TMVEngine engine(16);

  // Bootstrap 8 edges with different target_ids and times
  std::vector<TMVEdge> edges;
  for (uint32_t i = 0; i < 8; ++i) {
    edges.push_back(
        {100 + i, static_cast<uint32_t>(1000 + i * 100),
         std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  }
  engine.BootstrapVertex(1, Direction::kOut, edges, false);

  // Scan at t=5000: all 8 edges are valid (all valid_from <= 5000 < MAX)
  auto result = engine.ScanAtTime(1, Direction::kOut, 5000);
  EXPECT_EQ(result.size(), 8u);

  // Verify all target_ids are present and sorted
  for (uint32_t i = 0; i < 8; ++i) {
    EXPECT_EQ(result[i].target_id, 100u + i);
  }
}
