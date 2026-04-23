#include <gtest/gtest.h>

#include "cedar/gcn/tmv_edge.h"

using namespace cedar::gcn;

TEST(TMVEdgeTest, SizeAndAlignment) {
  EXPECT_EQ(sizeof(TMVEdge), 32);
  EXPECT_EQ(alignof(TMVEdge), 32);
}

TEST(TMVEdgeTest, FieldOffsets) {
  TMVEdge edge{};

  edge.target_id = 0xDEADBEEFCAFEBABEULL;
  EXPECT_EQ(edge.target_id, 0xDEADBEEFCAFEBABEULL);

  edge.valid_from = 0x12345678;
  EXPECT_EQ(edge.valid_from, 0x12345678);

  edge.valid_to = 0x87654321;
  EXPECT_EQ(edge.valid_to, 0x87654321);

  edge.attr_offset = 0xAABBCCDDEEFF0011ULL;
  EXPECT_EQ(edge.attr_offset, 0xAABBCCDDEEFF0011ULL);

  edge.edge_type = 0xABCD;
  EXPECT_EQ(edge.edge_type, 0xABCD);

  edge.reserved = 0xEF01;
  EXPECT_EQ(edge.reserved, 0xEF01);
}

TEST(TMVEdgeTest, EdgeOpValues) {
  EXPECT_EQ(static_cast<uint8_t>(EdgeOp::kCreate), 0);
  EXPECT_EQ(static_cast<uint8_t>(EdgeOp::kDelete), 1);
}
