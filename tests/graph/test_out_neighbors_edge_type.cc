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

// =============================================================================
// Out-Neighbors Edge Type Filter Test
// =============================================================================
// Verifies that GetOutNeighbors respects the edge_type parameter and only
// returns edges matching the requested type.
// =============================================================================

#include <chrono>
#include <gtest/gtest.h>
#include <unistd.h>

#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;

class OutNeighborsEdgeTypeTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<CedarGraph> graph_;
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/test_out_neighbors_edge_type_" + std::to_string(getpid()) + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());

    CedarOptions options;
    options.create_if_missing = true;
    options.distributed_mode = false;

    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
    ASSERT_TRUE(s.ok());
    graph_ = std::make_unique<CedarGraph>(storage_);
  }

  void TearDown() override {
    graph_.reset();
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());
  }
};

TEST_F(OutNeighborsEdgeTypeTest, FiltersByEdgeType) {
  // Create edges: A -(knows)-> B, A -(follows)-> C
  uint64_t vertex_a = 100;
  uint64_t vertex_b = 200;
  uint64_t vertex_c = 300;
  Timestamp ts(1000);
  Descriptor desc = Descriptor::InlineInt(0, 0);

  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_b, 1, ts, desc, ts).ok());
  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_c, 2, ts, desc, ts).ok());

  // Query outgoing neighbors of A with edge_type = 1 (knows)
  auto knows_neighbors = graph_->GetOutNeighbors(vertex_a, 1, Timestamp(0), Timestamp::Max());
  ASSERT_EQ(knows_neighbors.size(), 1);
  EXPECT_EQ(knows_neighbors[0].id, vertex_b);
  EXPECT_EQ(knows_neighbors[0].edge_type, 1);

  // Query outgoing neighbors of A with edge_type = 2 (follows)
  auto follows_neighbors = graph_->GetOutNeighbors(vertex_a, 2, Timestamp(0), Timestamp::Max());
  ASSERT_EQ(follows_neighbors.size(), 1);
  EXPECT_EQ(follows_neighbors[0].id, vertex_c);
  EXPECT_EQ(follows_neighbors[0].edge_type, 2);
}

TEST_F(OutNeighborsEdgeTypeTest, ReturnsEmptyForNonMatchingEdgeType) {
  // Create edge: A -(knows)-> B
  uint64_t vertex_a = 400;
  uint64_t vertex_b = 500;
  Timestamp ts(2000);
  Descriptor desc = Descriptor::InlineInt(0, 0);

  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_b, 1, ts, desc, ts).ok());

  // Query with a different edge_type
  auto neighbors = graph_->GetOutNeighbors(vertex_a, 99, Timestamp(0), Timestamp::Max());
  EXPECT_TRUE(neighbors.empty());
}

TEST_F(OutNeighborsEdgeTypeTest, ReturnsMultipleOutNeighborsOfSameType) {
  // A -(knows)-> B, A -(knows)-> C
  uint64_t vertex_a = 600;
  uint64_t vertex_b = 700;
  uint64_t vertex_c = 800;
  uint16_t edge_type = 1;
  Timestamp ts(3000);
  Descriptor desc = Descriptor::InlineInt(0, 0);

  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_b, edge_type, ts, desc, ts).ok());
  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_c, edge_type, ts, desc, ts).ok());

  auto neighbors = graph_->GetOutNeighbors(vertex_a, edge_type, Timestamp(0), Timestamp::Max());
  ASSERT_EQ(neighbors.size(), 2);

  std::unordered_set<uint64_t> ids;
  for (const auto& n : neighbors) {
    ids.insert(n.id);
  }
  EXPECT_EQ(ids.count(vertex_b), 1);
  EXPECT_EQ(ids.count(vertex_c), 1);
}

TEST_F(OutNeighborsEdgeTypeTest, ReturnsEmptyForVertexWithNoOutgoingEdges) {
  uint64_t isolated_vertex = 999;
  auto neighbors = graph_->GetOutNeighbors(isolated_vertex, 1, Timestamp(0), Timestamp::Max());
  EXPECT_TRUE(neighbors.empty());
}

TEST_F(OutNeighborsEdgeTypeTest, RespectsTimestampSnapshot) {
  // Create edge at ts=1000
  uint64_t vertex_a = 1001;
  uint64_t vertex_b = 1002;
  uint16_t edge_type = 1;
  Descriptor desc = Descriptor::InlineInt(0, 0);

  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_b, edge_type, Timestamp(1000), desc, Timestamp(1000)).ok());

  // Query with snapshot before edge creation
  auto before = graph_->GetOutNeighbors(vertex_a, edge_type, Timestamp(0), Timestamp(500));
  EXPECT_TRUE(before.empty());

  // Query with snapshot after edge creation
  auto after = graph_->GetOutNeighbors(vertex_a, edge_type, Timestamp(0), Timestamp(1500));
  ASSERT_EQ(after.size(), 1);
  EXPECT_EQ(after[0].id, vertex_b);
}
