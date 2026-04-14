#include <gtest/gtest.h>
#include "cedar/types/descriptor.h"

using namespace cedar;

TEST(DescriptorSchemaVersionTest, DefaultIsZero) {
  Descriptor d = Descriptor::InlineInt(1, 42);
  EXPECT_EQ(d.GetSchemaVersion(), 0);
}

TEST(DescriptorSchemaVersionTest, SetAndGetRoundTrip) {
  Descriptor d = Descriptor::InlineInt(1, 42);
  d.SetSchemaVersion(17);
  EXPECT_EQ(d.GetSchemaVersion(), 17);
  EXPECT_EQ(d.GetColumnId(), 1);
  auto val = d.AsInlineInt();
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, 42);
}

TEST(DescriptorSchemaVersionTest, MaxValue) {
  Descriptor d = Descriptor::InlineShortStr(2, Slice("abcd")).value();
  d.SetSchemaVersion(Descriptor::kMaxSchemaVersion);
  EXPECT_EQ(d.GetSchemaVersion(), 63);
  EXPECT_EQ(d.AsInlineShortStr(), "abcd");
}

TEST(DescriptorSchemaVersionTest, OverflowIsMasked) {
  Descriptor d;
  d.SetSchemaVersion(255);
  EXPECT_EQ(d.GetSchemaVersion(), 63);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
