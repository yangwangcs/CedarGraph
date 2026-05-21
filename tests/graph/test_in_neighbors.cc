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
// In-Neighbors Test — Reverse edge lookup via EdgeIn index
// =============================================================================
// Verifies that GetInNeighbors returns the source vertices of edges that
// point to a given vertex, using the EdgeIn index maintained by PutEdge.
// =============================================================================

#include <chrono>
#include <gtest/gtest.h>
#include <unistd.h>

#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;

class InNeighborsTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<CedarGraph> graph_;
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/test_in_neighbors_" + std::to_string(getpid()) + "_" +
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

TEST_F(InNeighborsTest, ReturnsReverseEdges) {
  // Create edge: A -(knows)-> B
  // Edge type 1 = "knows"
  uint64_t vertex_a = 100;
  uint64_t vertex_b = 200;
  uint16_t edge_type = 1;
  Timestamp ts(1000);

  Descriptor desc = Descriptor::InlineInt(0, 0);
  Status s = storage_->PutEdge(vertex_a, vertex_b, edge_type, ts, desc, ts);
  ASSERT_TRUE(s.ok());

  // Query incoming neighbors of B
  auto neighbors = graph_->GetInNeighbors(vertex_b, edge_type, Timestamp(0), Timestamp::Max());
  ASSERT_EQ(neighbors.size(), 1);
  EXPECT_EQ(neighbors[0].id, vertex_a);
  EXPECT_EQ(neighbors[0].edge_type, edge_type);
}

TEST_F(InNeighborsTest, ReturnsEmptyForVertexWithNoIncomingEdges) {
  uint64_t isolated_vertex = 999;
  auto neighbors = graph_->GetInNeighbors(isolated_vertex, 1, Timestamp(0), Timestamp::Max());
  EXPECT_TRUE(neighbors.empty());
}

TEST_F(InNeighborsTest, ReturnsMultipleInNeighbors) {
  // A -> C, B -> C
  uint64_t vertex_a = 101;
  uint64_t vertex_b = 102;
  uint64_t vertex_c = 103;
  uint16_t edge_type = 2;
  Timestamp ts(2000);
  Descriptor desc = Descriptor::InlineInt(0, 0);

  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_c, edge_type, ts, desc, ts).ok());
  ASSERT_TRUE(storage_->PutEdge(vertex_b, vertex_c, edge_type, ts, desc, ts).ok());

  auto neighbors = graph_->GetInNeighbors(vertex_c, edge_type, Timestamp(0), Timestamp::Max());
  ASSERT_EQ(neighbors.size(), 2);

  std::unordered_set<uint64_t> ids;
  for (const auto& n : neighbors) {
    ids.insert(n.id);
  }
  EXPECT_EQ(ids.count(vertex_a), 1);
  EXPECT_EQ(ids.count(vertex_b), 1);
}

TEST_F(InNeighborsTest, FiltersByEdgeType) {
  // A -(knows)-> B
  // A -(follows)-> B
  uint64_t vertex_a = 201;
  uint64_t vertex_b = 202;
  Timestamp ts(3000);
  Descriptor desc = Descriptor::InlineInt(0, 0);

  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_b, 1, ts, desc, ts).ok());
  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_b, 2, ts, desc, ts).ok());

  auto knows_neighbors = graph_->GetInNeighbors(vertex_b, 1, Timestamp(0), Timestamp::Max());
  ASSERT_EQ(knows_neighbors.size(), 1);
  EXPECT_EQ(knows_neighbors[0].edge_type, 1);

  auto follows_neighbors = graph_->GetInNeighbors(vertex_b, 2, Timestamp(0), Timestamp::Max());
  ASSERT_EQ(follows_neighbors.size(), 1);
  EXPECT_EQ(follows_neighbors[0].edge_type, 2);
}

TEST_F(InNeighborsTest, RespectsTimestampSnapshot) {
  // Create edge at ts=1000
  uint64_t vertex_a = 301;
  uint64_t vertex_b = 302;
  uint16_t edge_type = 1;
  Descriptor desc = Descriptor::InlineInt(0, 0);

  ASSERT_TRUE(storage_->PutEdge(vertex_a, vertex_b, edge_type, Timestamp(1000), desc, Timestamp(1000)).ok());

  // Query with snapshot before edge creation
  auto before = graph_->GetInNeighbors(vertex_b, edge_type, Timestamp(0), Timestamp(500));
  EXPECT_TRUE(before.empty());

  // Query with snapshot after edge creation
  auto after = graph_->GetInNeighbors(vertex_b, edge_type, Timestamp(0), Timestamp(1500));
  ASSERT_EQ(after.size(), 1);
  EXPECT_EQ(after[0].id, vertex_a);
}
