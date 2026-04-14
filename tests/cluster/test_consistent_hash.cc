#include <gtest/gtest.h>
#include <map>
#include "cedar/storage/consistent_hash_ring.h"

using namespace cedar;
using namespace cedar::storage;

TEST(ConsistentHashRingTest, BasicAddAndGet) {
  HashRingConfig config;
  config.virtual_nodes_per_physical = 10;
  
  ConsistentHashRing ring(config);
  ring.AddNode("node-1");
  ring.AddNode("node-2");
  ring.AddNode("node-3");
  
  EXPECT_EQ(ring.PhysicalNodeCount(), 3);
  EXPECT_EQ(ring.Size(), 30);
  
  std::map<std::string, int> distribution;
  for (int i = 0; i < 1000; i++) {
    std::string key = "key-" + std::to_string(i);
    std::string node = ring.GetNode(key);
    distribution[node]++;
  }
  
  EXPECT_EQ(distribution.size(), 3);
  for (const auto& [node, count] : distribution) {
    EXPECT_GT(count, 200);
    EXPECT_LT(count, 500);
  }
}

TEST(ConsistentHashRingTest, NodeRemovalMinimizesMigration) {
  HashRingConfig config;
  config.virtual_nodes_per_physical = 100;
  
  ConsistentHashRing ring(config);
  
  ring.AddNode("A");
  ring.AddNode("B");
  ring.AddNode("C");
  ring.AddNode("D");
  
  std::map<std::string, std::string> original_placement;
  for (int i = 0; i < 10000; i++) {
    std::string key = "key-" + std::to_string(i);
    original_placement[key] = ring.GetNode(key);
  }
  
  ring.RemoveNode("C");
  
  int migrated = 0;
  for (const auto& [key, original_node] : original_placement) {
    std::string new_node = ring.GetNode(key);
    if (original_node == "C" && new_node != "C") {
      migrated++;
    }
  }
  
  double migration_rate = static_cast<double>(migrated) / original_placement.size();
  EXPECT_LT(migration_rate, 0.3);
  EXPECT_GT(migration_rate, 0.2);
}

TEST(ConsistentHashRingTest, GetReplicas) {
  HashRingConfig config;
  config.virtual_nodes_per_physical = 50;
  config.replication_factor = 3;
  
  ConsistentHashRing ring(config);
  ring.AddNode("n1");
  ring.AddNode("n2");
  ring.AddNode("n3");
  ring.AddNode("n4");
  ring.AddNode("n5");
  
  auto replicas = ring.GetNodes("test-key", 3);
  EXPECT_EQ(replicas.size(), 3);
  
  std::set<std::string> unique(replicas.begin(), replicas.end());
  EXPECT_EQ(unique.size(), 3);
}

TEST(ConsistentHashRingTest, EmptyRing) {
  HashRingConfig config;
  ConsistentHashRing ring(config);
  
  EXPECT_EQ(ring.Size(), 0);
  EXPECT_EQ(ring.PhysicalNodeCount(), 0);
  
  std::string node = ring.GetNode("any-key");
  EXPECT_EQ(node, "");
}

TEST(ConsistentHashRingTest, DuplicateNodeAdd) {
  HashRingConfig config;
  config.virtual_nodes_per_physical = 10;
  
  ConsistentHashRing ring(config);
  ring.AddNode("node-1");
  ring.AddNode("node-1");  // Duplicate
  
  EXPECT_EQ(ring.PhysicalNodeCount(), 1);
  EXPECT_EQ(ring.Size(), 10);
}

TEST(ConsistentHashRingTest, GetDistribution) {
  HashRingConfig config;
  config.virtual_nodes_per_physical = 50;
  
  ConsistentHashRing ring(config);
  ring.AddNode("node-a");
  ring.AddNode("node-b");
  ring.AddNode("node-c");
  
  auto dist = ring.GetDistribution();
  EXPECT_EQ(dist.size(), 3);
  
  for (const auto& [node, count] : dist) {
    EXPECT_EQ(count, 50);
  }
}
