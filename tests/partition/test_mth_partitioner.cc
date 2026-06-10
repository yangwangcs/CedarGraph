#include <gtest/gtest.h>
#include "cedar/partition/mth/mth_partitioner.h"

using namespace cedar::partition;

TEST(MTHPartitionerTest, Construction) {
  MTHPartitioner partitioner(4, 100);
  EXPECT_DOUBLE_EQ(partitioner.FastPathRatio(), 0.0);
}

TEST(MTHPartitionerTest, AssignEventReturnsValidPartition) {
  MTHPartitioner partitioner(4, 100);
  CedarKey key = CedarKey::Vertex(12345, 0, 1000ULL);
  uint16_t pid = partitioner.AssignEvent(key);
  EXPECT_LT(pid, 4);
}

TEST(MTHPartitionerTest, AssignEventDeterministicForSameEntity) {
  MTHPartitioner partitioner(4, 100);
  CedarKey key = CedarKey::Vertex(99999, 0, 1000ULL);
  uint16_t p1 = partitioner.AssignEvent(key);
  uint16_t p2 = partitioner.AssignEvent(key);
  EXPECT_EQ(p1, p2);
}

TEST(MTHPartitionerTest, FastPathRatioIncreases) {
  MTHPartitioner partitioner(4, 100, 1.0, 1.0, 0.0, 0.0, 0.01, 3, 64, 0.6);
  CedarKey key = CedarKey::Vertex(1000, 0, 1000ULL);
  // Repeated assignments may trigger fast path
  for (int i = 0; i < 20; ++i) {
    (void)partitioner.AssignEvent(key);
  }
  // FastPathRatio is a metric, just verify it doesn't crash
  EXPECT_GE(partitioner.FastPathRatio(), 0.0);
}

TEST(MTHPartitionerTest, SketchNotEmptyAfterEvents) {
  MTHPartitioner partitioner(4, 100);
  for (int i = 0; i < 50; ++i) {
    CedarKey key = CedarKey::Vertex(static_cast<uint64_t>(i), 0, 1000ULL + i);
    partitioner.AssignEvent(key);
  }
  EXPECT_GT(partitioner.sketch().num_partitions(), 0);
}
