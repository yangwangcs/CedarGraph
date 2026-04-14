// Copyright 2025 The Cedar Authors
//
// Test CedarKey encode/decode roundtrip without storage

#include <gtest/gtest.h>
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

TEST(CedarKeyRoundtrip, EncodeDecodeBasic) {
  // Create a CedarKey with all fields set
  CedarKey key = CedarKey::Vertex(
      1001,                    // entity_id
      5,                       // column_id
      Timestamp(1712050000000000ULL),  // timestamp
      7,                       // sequence
      1001,                    // part_id (entity_id % 65536)
      0,                       // extension
      0x04                     // flags: CREATE + DISTRIBUTED
  );
  
  // Verify all fields
  EXPECT_EQ(key.entity_id(), 1001);
  EXPECT_EQ(key.column_id(), 5);
  EXPECT_EQ(key.sequence(), 7);
  EXPECT_EQ(key.part_id(), 1001);
  EXPECT_EQ(key.flags(), 0x04);
  EXPECT_TRUE(key.IsVertex());
  EXPECT_TRUE(key.IsCreate());
  EXPECT_TRUE(key.IsDistributed());
  EXPECT_FALSE(key.IsTombstone());
  
  // Encode to 32 bytes
  std::string encoded = key.Encode();
  EXPECT_EQ(encoded.size(), 32);
  
  // Decode back
  auto decoded_opt = CedarKey::Decode(encoded);
  ASSERT_TRUE(decoded_opt.has_value());
  
  CedarKey decoded = *decoded_opt;
  
  // Verify all fields preserved
  EXPECT_EQ(decoded.entity_id(), 1001);
  EXPECT_EQ(decoded.column_id(), 5);
  EXPECT_EQ(decoded.timestamp().value(), 1712050000000000ULL);
  EXPECT_EQ(decoded.sequence(), 7);
  EXPECT_EQ(decoded.part_id(), 1001);
  EXPECT_EQ(decoded.flags(), 0x04);
  EXPECT_TRUE(decoded.IsVertex());
  EXPECT_TRUE(decoded.IsCreate());
  EXPECT_TRUE(decoded.IsDistributed());
}

TEST(CedarKeyRoundtrip, EncodeDecodeEdge) {
  // Create EdgeOut key
  CedarKey edge_out = CedarKey::EdgeOut(
      1001,                    // src_id
      1002,                    // dst_id
      3,                       // edge_type
      Timestamp(1712050000000000ULL),
      5,                       // sequence
      1001,                    // part_id (by src)
      0x04                     // flags
  );
  
  EXPECT_EQ(edge_out.entity_id(), 1001);
  EXPECT_EQ(edge_out.target_id(), 1002);
  EXPECT_TRUE(edge_out.IsEdgeOut());
  
  // Encode and decode
  std::string encoded = edge_out.Encode();
  auto decoded = CedarKey::Decode(encoded);
  
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entity_id(), 1001);
  EXPECT_EQ(decoded->target_id(), 1002);
  EXPECT_TRUE(decoded->IsEdgeOut());
  
  // Create EdgeIn key
  CedarKey edge_in = CedarKey::EdgeIn(
      1002,                    // dst_id (becomes entity_id)
      1001,                    // src_id (becomes target_id)
      3,                       // edge_type
      Timestamp(1712050000000000ULL),
      6,                       // sequence
      1002,                    // part_id (by dst)
      0x04                     // flags
  );
  
  EXPECT_EQ(edge_in.entity_id(), 1002);
  EXPECT_EQ(edge_in.target_id(), 1001);
  EXPECT_TRUE(edge_in.IsEdgeIn());
  
  // Encode and decode
  encoded = edge_in.Encode();
  decoded = CedarKey::Decode(encoded);
  
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entity_id(), 1002);
  EXPECT_EQ(decoded->target_id(), 1001);
  EXPECT_TRUE(decoded->IsEdgeIn());
}

TEST(CedarKeyRoundtrip, DifferentOpTypes) {
  // CREATE
  CedarKey create = CedarKey::Vertex(1001, 1, Timestamp(1000), 0, 1001, 0, 0x00);
  EXPECT_TRUE(create.IsCreate());
  EXPECT_EQ(create.flags() & 0x03, 0x00);
  
  // UPDATE
  CedarKey update = CedarKey::Vertex(1001, 1, Timestamp(1000), 0, 1001, 0, 0x01);
  EXPECT_TRUE(update.IsUpdate());
  EXPECT_EQ(update.flags() & 0x03, 0x01);
  
  // DELETE
  CedarKey del = CedarKey::Vertex(1001, 1, Timestamp(1000), 0, 1001, 0, 0x02);
  EXPECT_TRUE(del.IsDelete());
  EXPECT_EQ(del.flags() & 0x03, 0x02);
  
  // Roundtrip test for UPDATE
  std::string encoded = update.Encode();
  auto decoded = CedarKey::Decode(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(decoded->IsUpdate());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
