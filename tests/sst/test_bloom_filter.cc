#include <gtest/gtest.h>
#include "cedar/sst/bloom_filter.h"

using namespace cedar;

TEST(BloomFilterTest, EmptyFilter) {
  BloomFilter filter;
  EXPECT_TRUE(filter.empty());
  // Empty filter returns true (assume present — conservative no-filter behavior)
  EXPECT_TRUE(filter.MayContain("hello", 5));
}

TEST(BloomFilterTest, AddAndMayContain) {
  BloomFilter filter(10, 100);
  filter.Add("hello", 5);
  EXPECT_TRUE(filter.MayContain("hello", 5));
}

TEST(BloomFilterTest, FalsePositivePossibleButNotFalseNegative) {
  BloomFilter filter(10, 1000);
  for (int i = 0; i < 100; ++i) {
    filter.Add(std::to_string(i).c_str(), std::to_string(i).size());
  }
  // All inserted keys must be found
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(filter.MayContain(std::to_string(i).c_str(), std::to_string(i).size()))
        << "Key " << i << " should be present";
  }
}

TEST(BloomFilterTest, AddUint64) {
  BloomFilter filter(10, 100);
  filter.Add(123456789ULL);
  EXPECT_TRUE(filter.MayContain(123456789ULL));
  EXPECT_FALSE(filter.MayContain(987654321ULL));
}

TEST(BloomFilterTest, EncodeDecode) {
  BloomFilter filter(10, 100);
  filter.Add("key1", 4);
  filter.Add("key2", 4);

  std::string buf;
  filter.EncodeTo(&buf);

  BloomFilter decoded;
  EXPECT_TRUE(decoded.DecodeFrom(buf.data(), buf.size(), 2));
  EXPECT_TRUE(decoded.MayContain("key1", 4));
  EXPECT_TRUE(decoded.MayContain("key2", 4));
  EXPECT_FALSE(decoded.MayContain("key3", 4));
}

TEST(BloomFilterTest, FinishAndInit) {
  BloomFilter filter(10, 100);
  filter.Add("alpha", 5);
  std::vector<char> data = filter.Finish();

  BloomFilter reader;
  reader.Init(data.data(), data.size());
  EXPECT_TRUE(reader.MayContain("alpha", 5));
}

TEST(BloomFilterTest, Clear) {
  BloomFilter filter(10, 100);
  filter.Add("x", 1);
  EXPECT_FALSE(filter.empty());
  filter.Clear();
  EXPECT_TRUE(filter.empty());
}

TEST(BloomFilterTest, NumKeysTracked) {
  BloomFilter filter(10, 50);
  EXPECT_EQ(filter.NumKeys(), 50);
}
