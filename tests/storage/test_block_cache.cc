#include <gtest/gtest.h>
#include "cedar/storage/block_cache.h"

using namespace cedar;

TEST(BlockCacheTest, InsertAndGet) {
  BlockCache cache(1024);
  cache.Insert("key1", "value1");
  auto block = cache.Get("key1");
  ASSERT_NE(block, nullptr);
  EXPECT_EQ(block->data, "value1");
}

TEST(BlockCacheTest, MissReturnsNull) {
  BlockCache cache(1024);
  auto block = cache.Get("missing");
  EXPECT_EQ(block, nullptr);
}

TEST(BlockCacheTest, LRU_eviction) {
  BlockCache cache(15);  // very small: each entry = key + data = 6 bytes
  cache.Insert("a", "12345");
  cache.Insert("b", "12345");
  cache.Insert("c", "12345");  // should evict "a"
  EXPECT_EQ(cache.Get("a"), nullptr);
  EXPECT_NE(cache.Get("b"), nullptr);
  EXPECT_NE(cache.Get("c"), nullptr);
}

TEST(BlockCacheTest, UpdateExistingKey) {
  BlockCache cache(1024);
  cache.Insert("key1", "old");
  cache.Insert("key1", "new");
  auto block = cache.Get("key1");
  ASSERT_NE(block, nullptr);
  EXPECT_EQ(block->data, "new");
}

TEST(BlockCacheTest, Clear) {
  BlockCache cache(1024);
  cache.Insert("k", "v");
  cache.Clear();
  EXPECT_EQ(cache.Get("k"), nullptr);
}

TEST(BlockCacheTest, StatsTracking) {
  BlockCache cache(1024);
  cache.Insert("k", "v");
  (void)cache.Get("k");   // hit
  (void)cache.Get("x");   // miss
  auto stats = cache.GetStats();
  EXPECT_EQ(stats.hits, 1);
  EXPECT_EQ(stats.misses, 1);
  EXPECT_EQ(stats.insertions, 1);
}

TEST(BlockCacheTest, SetCapacityEvicts) {
  BlockCache cache(1000);
  cache.Insert("a", std::string(600, 'x'));
  cache.Insert("b", std::string(600, 'x'));
  cache.SetCapacity(500);
  auto stats = cache.GetStats();
  EXPECT_LE(stats.used_bytes, 500);
}

TEST(BlockCacheManagerTest, InstanceAndGetCache) {
  auto& mgr = BlockCacheManager::Instance();
  auto* c1 = mgr.GetCache("/tmp/db1");
  auto* c2 = mgr.GetCache("/tmp/db1");
  auto* c3 = mgr.GetCache("/tmp/db2");
  EXPECT_EQ(c1, c2);
  EXPECT_NE(c1, c3);
  mgr.ClearAll();
}
