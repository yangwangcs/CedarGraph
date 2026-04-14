//===----------------------------------------------------------------------===//
// CedarKey Enhanced Features Unit Tests
// Testing: endianness, ToString, sequence overflow handling
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <sstream>
#include "cedar/types/cedar_key.h"

using namespace cedar;

// Test byte-order correctness for uint16_t fields
TEST(CedarKeyEndiannessTest, Uint16FieldsAreBigEndian) {
  CedarKey key = CedarKey::Vertex(100, 5, Timestamp(1000), 42, 12345);
  
  // Verify fields are correctly stored and retrieved
  EXPECT_EQ(key.column_id(), 5);
  EXPECT_EQ(key.sequence(), 42);
  EXPECT_EQ(key.part_id(), 12345);
}

// Test target_id is stored in big-endian
TEST(CedarKeyEndiannessTest, TargetIdIsBigEndian) {
  uint64_t target = 0x123456789ABCDEF0ULL;
  CedarKey key = CedarKey::EdgeOut(1, target, EdgeTypeId(1), Timestamp(100));
  
  EXPECT_EQ(key.target_id(), target);
}

// Test ToString produces human-readable output
TEST(CedarKeyToStringTest, VertexToString) {
  CedarKey key = CedarKey::Vertex(12345, 5, Timestamp(1000000), 3, 100);
  key.SetFlags(key_flags::kHasVInline);
  
  std::string str = key.ToString();
  
  // Check that key fields appear in output
  EXPECT_NE(str.find("ID:12345"), std::string::npos);
  EXPECT_NE(str.find("Vertex"), std::string::npos);
  EXPECT_NE(str.find("Col:5"), std::string::npos);
  EXPECT_NE(str.find("Seq:3"), std::string::npos);
}

// Test ToString for EdgeOut
TEST(CedarKeyToStringTest, EdgeOutToString) {
  CedarKey key = CedarKey::EdgeOut(100, 200, EdgeTypeId(5), Timestamp(2000000), 1, 10);
  
  std::string str = key.ToString();
  
  EXPECT_NE(str.find("EdgeOut(100->200)"), std::string::npos);
  EXPECT_NE(str.find("EdgeType:5"), std::string::npos);
}

// Test ToString for EdgeIn
TEST(CedarKeyToStringTest, EdgeInToString) {
  CedarKey key = CedarKey::EdgeIn(200, 100, EdgeTypeId(3), Timestamp(3000000));
  
  std::string str = key.ToString();
  
  EXPECT_NE(str.find("EdgeIn(100->200)"), std::string::npos);
}

// Test ToString for Delete operation
TEST(CedarKeyToStringTest, DeleteOpToString) {
  CedarKey key = CedarKey::Vertex(999, 1, Timestamp(1000));
  key.SetOpType(op_type::kDelete);
  
  std::string str = key.ToString();
  
  EXPECT_NE(str.find("Delete"), std::string::npos);
}

// Test ToHexString
TEST(CedarKeyToStringTest, HexStringFormat) {
  CedarKey key = CedarKey::Vertex(1, 1, Timestamp(1));
  
  std::string hex = key.ToHexString();
  
  // Should start with 0x and have 64 hex chars (32 bytes * 2)
  EXPECT_EQ(hex.substr(0, 2), "0x");
  EXPECT_EQ(hex.length(), 2 + 64);
}

// Test DebugString format
TEST(CedarKeyToStringTest, DebugStringFormat) {
  CedarKey key = CedarKey::Vertex(100, 5, Timestamp(1000), 1, 10);
  
  std::string debug = key.DebugString();
  
  EXPECT_NE(debug.find("CK{"), std::string::npos);
  EXPECT_NE(debug.find("eid=100"), std::string::npos);
  EXPECT_NE(debug.find("ts=1000"), std::string::npos);
}

// Test sequence overflow handling
TEST(CedarKeySequenceTest, HandleSequenceOverflowNormalCase) {
  auto [ts, seq] = CedarKey::HandleSequenceOverflow(Timestamp(1000), 100);
  
  EXPECT_EQ(ts.value(), 1000);
  EXPECT_EQ(seq, 100);
}

TEST(CedarKeySequenceTest, HandleSequenceOverflowNearThreshold) {
  // Test at threshold - 5
  auto [ts, seq] = CedarKey::HandleSequenceOverflow(Timestamp(1000), 65530);
  
  // Should advance timestamp and reset sequence
  EXPECT_EQ(ts.value(), 1001);
  EXPECT_EQ(seq, 0);
}

TEST(CedarKeySequenceTest, HandleSequenceOverflowWellBelowThreshold) {
  auto [ts, seq] = CedarKey::HandleSequenceOverflow(Timestamp(1000), 65529);
  
  // Should not trigger overflow
  EXPECT_EQ(ts.value(), 1000);
  EXPECT_EQ(seq, 65529);
}

// Test CedarKey size and alignment assertions
TEST(CedarKeyLayoutTest, SizeAndAlignment) {
  // These are compile-time assertions, but we verify at runtime too
  EXPECT_EQ(sizeof(CedarKey), 32);
  EXPECT_EQ(alignof(CedarKey), 8);
}

// Test Encode/Decode preserves all fields including uint16_t
TEST(CedarKeyEncodingTest, RoundTripPreservesUint16Fields) {
  CedarKey original = CedarKey::Vertex(
      0x123456789ABCDEF0ULL,  // entity_id
      0xABCD,                  // column_id (large uint16)
      Timestamp(0xFEDCBA9876543210ULL),
      0x1234,                  // sequence (large uint16)
      0x5678,                  // part_id
      0x1122334455667788ULL,   // extension
      0x42                     // flags
  );
  
  // Encode
  std::string encoded = original.Encode();
  EXPECT_EQ(encoded.length(), 32);
  
  // Decode
  auto decoded_opt = CedarKey::Decode(encoded);
  ASSERT_TRUE(decoded_opt.has_value());
  
  CedarKey decoded = decoded_opt.value();
  
  // Verify all fields preserved
  EXPECT_EQ(decoded.entity_id(), original.entity_id());
  EXPECT_EQ(decoded.column_id(), original.column_id());
  EXPECT_EQ(decoded.timestamp().value(), original.timestamp().value());
  EXPECT_EQ(decoded.sequence(), original.sequence());
  EXPECT_EQ(decoded.part_id(), original.part_id());
  EXPECT_EQ(decoded.target_id(), original.target_id());
  EXPECT_EQ(decoded.flags(), original.flags());
}

// Test that encoding produces correct byte order
TEST(CedarKeyEncodingTest, EncodedBytesAreBigEndian) {
  // Use values where byte order matters
  CedarKey key = CedarKey::Vertex(0x0102030405060708ULL, 0x1234, Timestamp(0));
  
  std::string encoded = key.Encode();
  
  // First 8 bytes should be entity_id in big-endian
  // 0x0102030405060708 in big-endian: 01 02 03 04 05 06 07 08
  EXPECT_EQ(static_cast<uint8_t>(encoded[0]), 0x01);
  EXPECT_EQ(static_cast<uint8_t>(encoded[1]), 0x02);
  EXPECT_EQ(static_cast<uint8_t>(encoded[7]), 0x08);
  
  // column_id at offset 24-25: 0x1234 in big-endian: 12 34
  EXPECT_EQ(static_cast<uint8_t>(encoded[24]), 0x12);
  EXPECT_EQ(static_cast<uint8_t>(encoded[25]), 0x34);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
