#include <gtest/gtest.h>

#include "cedar/gcn/tmv_chunk.h"
#include "cedar/gcn/tmv_edge.h"

using namespace cedar::gcn;

TEST(TMVChunkTest, AppendAndCount) {
  TMVChunk chunk;
  TMVEdge edge{};
  edge.target_id = 42;
  edge.valid_from = 100;
  edge.valid_to = 200;
  edge.attr_offset = 0;
  edge.edge_type = 1;

  int idx = chunk.Append(edge);
  EXPECT_EQ(idx, 0);
  EXPECT_EQ(chunk.event_count.load(), 1);
  EXPECT_EQ(chunk.edges[0].target_id, 42);
  EXPECT_EQ(chunk.edges[0].valid_from, 100);
  EXPECT_EQ(chunk.edges[0].valid_to, 200);
  EXPECT_EQ(chunk.min_valid_from.load(), 100);
  EXPECT_EQ(chunk.max_valid_to.load(), 200);
  EXPECT_TRUE(chunk.CanAppend());
}

TEST(TMVChunkTest, SealPreventsAppend) {
  TMVChunk chunk;
  chunk.Seal();
  EXPECT_FALSE(chunk.CanAppend());

  TMVEdge edge{};
  int idx = chunk.Append(edge);
  EXPECT_EQ(idx, -1);
}

TEST(TMVChunkTest, CapacityOverflow) {
  TMVChunk chunk;
  TMVEdge edge{};
  for (uint32_t i = 0; i < TMVChunk::kCapacity; ++i) {
    int idx = chunk.Append(edge);
    EXPECT_GE(idx, 0);
  }
  // One more should fail
  int idx = chunk.Append(edge);
  EXPECT_EQ(idx, -1);
}
