#include <gtest/gtest.h>
#include "cedar/storage/active_entity_bitmap.h"

using namespace cedar;

TEST(ActiveEntityBitmapTest, MarkActiveAndQuery) {
  ActiveEntityBitmap bitmap;
  bitmap.MarkActive(42);
  EXPECT_TRUE(bitmap.IsActive(42));
  EXPECT_TRUE(bitmap.Contains(42));
  EXPECT_EQ(bitmap.ActiveCount(), 1);
}

TEST(ActiveEntityBitmapTest, MarkDeletedRemovesActive) {
  ActiveEntityBitmap bitmap;
  bitmap.MarkActive(42);
  bitmap.MarkDeleted(42);
  EXPECT_FALSE(bitmap.IsActive(42));
  EXPECT_TRUE(bitmap.Contains(42));
  EXPECT_EQ(bitmap.ActiveCount(), 0);
  EXPECT_EQ(bitmap.DeletedCount(), 1);
}

TEST(ActiveEntityBitmapTest, BatchOperations) {
  ActiveEntityBitmap bitmap;
  bitmap.MarkActiveBatch({1, 2, 3});
  EXPECT_EQ(bitmap.ActiveCount(), 3);
  bitmap.MarkDeletedBatch({1, 2});
  EXPECT_EQ(bitmap.ActiveCount(), 1);
  EXPECT_EQ(bitmap.DeletedCount(), 2);
}

TEST(ActiveEntityBitmapTest, FilterActive) {
  ActiveEntityBitmap bitmap;
  bitmap.MarkActiveBatch({10, 20, 30});
  bitmap.MarkDeleted(20);
  auto active = bitmap.FilterActive({10, 20, 30, 40});
  EXPECT_EQ(active.size(), 2);
  EXPECT_EQ(active[0], 10);
  EXPECT_EQ(active[1], 30);
}

TEST(ActiveEntityBitmapTest, Clear) {
  ActiveEntityBitmap bitmap;
  bitmap.MarkActiveBatch({1, 2, 3});
  bitmap.Clear();
  EXPECT_EQ(bitmap.ActiveCount(), 0);
  EXPECT_EQ(bitmap.DeletedCount(), 0);
}

TEST(ActiveEntityBitmapTest, MemoryUsage) {
  ActiveEntityBitmap bitmap;
  auto before = bitmap.MemoryUsage();
  bitmap.MarkActiveBatch({1, 2, 3, 4, 5});
  auto after = bitmap.MemoryUsage();
  EXPECT_GT(after, before);
}

TEST(VSLNodeHintTest, FromCedarKeyAndQuery) {
  CedarKey key = CedarKey::Vertex(1, 0, Timestamp::Now());
  uint8_t hint = VSLNodeHint::FromCedarKey(key);
  EXPECT_FALSE(VSLNodeHint::IsDeleted(hint));
}

TEST(AnchorCacheTest, PutAndGet) {
  AnchorCache cache(100);
  StateAnchor anchor(Timestamp(1000), EntityState::Active, 1);
  cache.Put(42, EntityType::Vertex, anchor);
  auto got = cache.Get(42, EntityType::Vertex);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->state, EntityState::Active);
}

TEST(AnchorCacheTest, Invalidate) {
  AnchorCache cache(100);
  StateAnchor anchor(Timestamp(1000), EntityState::Active);
  cache.Put(1, EntityType::Vertex, anchor);
  cache.Invalidate(1, EntityType::Vertex);
  EXPECT_FALSE(cache.Get(1, EntityType::Vertex).has_value());
}

TEST(AnchorCacheTest, HitRate) {
  AnchorCache cache(10);
  StateAnchor anchor(Timestamp(1000), EntityState::Active);
  cache.Put(1, EntityType::Vertex, anchor);
  (void)cache.Get(1, EntityType::Vertex);  // hit
  (void)cache.Get(2, EntityType::Vertex);  // miss
  EXPECT_DOUBLE_EQ(cache.HitRate(), 0.5);
}
