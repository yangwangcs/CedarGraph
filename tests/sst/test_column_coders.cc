#include <gtest/gtest.h>
#include "cedar/sst/column_coders.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

TEST(VarintTest, EncodeDecodeRoundTrip) {
  std::string buf;
  EncodeVarUint64(0, &buf);
  EncodeVarUint64(1, &buf);
  EncodeVarUint64(127, &buf);
  EncodeVarUint64(128, &buf);
  EncodeVarUint64(16383, &buf);
  EncodeVarUint64(12345678901234ULL, &buf);

  const char* p = buf.data();
  size_t remaining = buf.size();

  EXPECT_EQ(DecodeVarUint64(&p, &remaining).value(), 0);
  EXPECT_EQ(DecodeVarUint64(&p, &remaining).value(), 1);
  EXPECT_EQ(DecodeVarUint64(&p, &remaining).value(), 127);
  EXPECT_EQ(DecodeVarUint64(&p, &remaining).value(), 128);
  EXPECT_EQ(DecodeVarUint64(&p, &remaining).value(), 16383);
  EXPECT_EQ(DecodeVarUint64(&p, &remaining).value(), 12345678901234ULL);
}

TEST(VarintTest, DecodeEmptyReturnsNullopt) {
  const char* p = "";
  size_t remaining = 0;
  EXPECT_FALSE(DecodeVarUint64(&p, &remaining).has_value());
}

TEST(ZigZagTest, EncodeDecode) {
  EXPECT_EQ(ZigZagEncode(0), 0);
  EXPECT_EQ(ZigZagDecode(0), 0);
  EXPECT_EQ(ZigZagEncode(-1), 1);
  EXPECT_EQ(ZigZagDecode(1), -1);
  EXPECT_EQ(ZigZagEncode(1), 2);
  EXPECT_EQ(ZigZagDecode(2), 1);
}

TEST(EntityIdColumnTest, AddAndFinish) {
  EntityIdColumn col;
  col.Add(100);
  col.Add(101);
  col.Add(102);
  EXPECT_EQ(col.Count(), 3);
  std::string data = col.Finish();
  EXPECT_FALSE(data.empty());
}

TEST(EntityIdColumnTest, Reset) {
  EntityIdColumn col;
  col.Add(1);
  col.Reset();
  EXPECT_EQ(col.Count(), 0);
}

TEST(TimestampColumnTest, AddAndFinish) {
  TimestampColumn col;
  col.Add(1000000);
  col.Add(1000100);
  col.Add(1000200);
  EXPECT_EQ(col.Count(), 3);
  std::string data = col.Finish();
  EXPECT_FALSE(data.empty());
}

TEST(TimestampColumnTest, LastValue) {
  TimestampColumn col;
  col.Add(500);
  col.Add(600);
  EXPECT_EQ(col.LastValue(), 600);
}

TEST(TargetIdColumnTest, AddAndFinish) {
  TargetIdColumn col;
  col.Add(1000);
  col.Add(2000);
  EXPECT_EQ(col.Count(), 2);
  std::string data = col.Finish();
  EXPECT_FALSE(data.empty());
}

TEST(SequenceColumnTest, AddAndFinish) {
  SequenceColumn col;
  col.Add(0);
  col.Add(1);
  col.Add(2);
  EXPECT_EQ(col.Count(), 3);
  std::string data = col.Finish();
  EXPECT_FALSE(data.empty());
}

TEST(SequenceColumnTest, AllZeroTracking) {
  SequenceColumn col;
  col.Add(0);
  col.Add(0);
  EXPECT_TRUE(col.AllZero());
  col.Add(1);
  EXPECT_FALSE(col.AllZero());
}

TEST(FlagsColumnTest, AddAndFinish) {
  FlagsColumn col;
  col.Add(0x01);
  col.Add(0x02);
  EXPECT_EQ(col.Count(), 2);
  std::string data = col.Finish();
  EXPECT_FALSE(data.empty());
}

TEST(DescriptorColumnTest, AddAndFinish) {
  DescriptorColumn col;
  Descriptor desc = Descriptor::InlineInt(0, 42);
  col.Add(desc);
  EXPECT_EQ(col.Count(), 1);
  CedarCompressionType actual;
  std::string data = col.Finish(CedarCompressionType::None, &actual);
  EXPECT_FALSE(data.empty());
  EXPECT_EQ(actual, CedarCompressionType::None);
}

TEST(DescriptorColumnTest, RawSize) {
  DescriptorColumn col;
  col.Add(Descriptor::InlineInt(0, 1));
  col.Add(Descriptor::InlineInt(0, 2));
  EXPECT_EQ(col.RawSize(), 16);  // 2 * 8 bytes
}
