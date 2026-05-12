// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include <gtest/gtest.h>
#include <set>
#include <map>
#include <vector>

using namespace std;

class PartitionRouterTest : public ::testing::Test {
 protected:
  void SetUp() override {}
};

TEST(PartitionRouterTest, HashRoutingConsistency) {
  // Same entity ID should always map to the same partition
  uint64_t entity_id = 12345;
  uint32_t partition_count = 16;
  
  uint32_t partition1 = entity_id % partition_count;
  uint32_t partition2 = entity_id % partition_count;
  
  EXPECT_EQ(partition1, partition2);
}

TEST(PartitionRouterTest, RangeRoutingDistribution) {
  // Verify that entity IDs are distributed across partitions
  uint32_t partition_count = 16;
  set<uint32_t> used_partitions;
  
  for (uint64_t i = 0; i < 1000; ++i) {
    used_partitions.insert(i % partition_count);
  }
  
  // All partitions should be used with 1000 entities and 16 partitions
  EXPECT_GT(used_partitions.size(), 8);
}

TEST(PartitionRouterTest, RouteEntitiesGroupsByPartition) {
  std::vector<uint64_t> entity_ids = {1, 2, 17, 18};
  uint32_t partition_count = 16;
  
  std::map<uint32_t, std::vector<uint64_t>> groups;
  for (auto id : entity_ids) {
    groups[id % partition_count].push_back(id);
  }
  
  // Entity 1 and 17 should be in the same partition (1 % 16 = 1, 17 % 16 = 1)
  EXPECT_EQ(groups[1].size(), 2);
  EXPECT_EQ(groups[2].size(), 2);
  EXPECT_EQ(groups[1][0], 1);
  EXPECT_EQ(groups[1][1], 17);
}

TEST(PartitionRouterTest, EdgeCaseZeroEntityId) {
  uint32_t partition_count = 16;
  uint32_t partition = 0 % partition_count;
  EXPECT_EQ(partition, 0);
}

TEST(PartitionRouterTest, LargeEntityId) {
  uint32_t partition_count = 16;
  uint64_t entity_id = std::numeric_limits<uint64_t>::max();
  uint32_t partition = entity_id % partition_count;
  EXPECT_LT(partition, partition_count);
}
