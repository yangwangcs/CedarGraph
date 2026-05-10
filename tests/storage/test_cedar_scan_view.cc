//===----------------------------------------------------------------------===//
// CedarScan View Object Unit Tests (no storage engine needed)
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include "cedar/query/cedar_scan.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

// Test NodeView basic functionality
TEST(NodeViewTest, BasicAccessors) {
  Descriptor desc = Descriptor::InlineInt(1, 999);
  CedarKey key = CedarKey::Vertex(123, 1, Timestamp(100), 3, 7, 888);
  key.SetFlags(key_flags::kHasVInline);
  
  NodeView view(key, desc);
  
  EXPECT_EQ(view.node_id(), 123);
  EXPECT_EQ(view.timestamp().value(), 100);
  EXPECT_EQ(view.column_id(), 1);
  EXPECT_EQ(view.sequence(), 3);
  EXPECT_TRUE(view.has_inline_value());
  EXPECT_EQ(view.inline_value(), 888);
  EXPECT_FALSE(view.is_deleted());
}

// Test NodeView without inline value
TEST(NodeViewTest, NoInlineValue) {
  Descriptor desc = Descriptor::InlineInt(1, 42);
  CedarKey key = CedarKey::Vertex(456, 2, Timestamp(200));
  // No kHasVInline flag
  
  NodeView view(key, desc);
  
  EXPECT_EQ(view.node_id(), 456);
  EXPECT_FALSE(view.has_inline_value());
  EXPECT_EQ(view.inline_value(), 0);  // target_id defaults to 0
}

// Test EdgeView for OutEdge
TEST(EdgeViewTest, OutEdgeAccessors) {
  Descriptor desc = Descriptor::InlineInt(1, 100);
  CedarKey key = CedarKey::EdgeOut(10, 20, EdgeTypeId(5), Timestamp(100), 2, 3);
  
  EdgeView view(key, desc, true);
  
  EXPECT_EQ(view.src_id(), 10);
  EXPECT_EQ(view.dst_id(), 20);
  EXPECT_EQ(view.edge_type(), 5);
  EXPECT_EQ(view.timestamp().value(), 100);
  EXPECT_EQ(view.sequence(), 2);
  EXPECT_TRUE(view.is_out_edge());
  EXPECT_FALSE(view.is_in_edge());
}

// Test EdgeView for InEdge
TEST(EdgeViewTest, InEdgeAccessors) {
  Descriptor desc = Descriptor::InlineInt(1, 100);
  CedarKey key = CedarKey::EdgeIn(20, 10, EdgeTypeId(5), Timestamp(100));
  
  EdgeView view(key, desc, false);
  
  // For InEdge: src comes from target_id, dst is entity_id
  EXPECT_EQ(view.src_id(), 10);
  EXPECT_EQ(view.dst_id(), 20);
  EXPECT_EQ(view.edge_type(), 5);
  EXPECT_TRUE(view.is_in_edge());
  EXPECT_FALSE(view.is_out_edge());
}

// Test EdgeView with inline value
TEST(EdgeViewTest, InlineValue) {
  Descriptor desc = Descriptor::InlineInt(1, 100);
  CedarKey key = CedarKey::EdgeOut(1, 2, EdgeTypeId(1), Timestamp(100));
  key.SetFlags(key_flags::kHasVInline);
  
  EdgeView view(key, desc, true);
  
  EXPECT_TRUE(view.has_inline_value());
  // target_id for EdgeOut is dst_id
  EXPECT_EQ(view.inline_value(), 2);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
