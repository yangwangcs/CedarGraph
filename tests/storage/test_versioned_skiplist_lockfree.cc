#include <gtest/gtest.h>
#include "cedar/storage/versioned_skiplist_lockfree.h"

using namespace cedar;

TEST(LFNodeTest, ConstructionAndGetters) {
  CedarKey key = CedarKey::Vertex(100, 1, Timestamp(1000), 0, 5);
  Descriptor desc = Descriptor::InlineInt(1, 42);
  LFNode node(key, desc, 4, Timestamp(2000));

  EXPECT_EQ(node.entity_id(), 100);
  EXPECT_EQ(node.timestamp(), 1000);
  EXPECT_EQ(node.txn_version(), 2000);
  EXPECT_EQ(node.column_id(), 1);
  EXPECT_EQ(node.part_id(), 5);
  EXPECT_EQ(node.height(), 4);
}

TEST(LFNodeTest, GetKeyRoundTrip) {
  CedarKey key = CedarKey::Vertex(100, 1, Timestamp(1000), 0, 5);
  Descriptor desc = Descriptor::InlineInt(1, 42);
  LFNode node(key, desc, 4, Timestamp(2000));

  CedarKey reconstructed = node.GetKey();
  EXPECT_EQ(reconstructed.entity_id(), key.entity_id());
  EXPECT_EQ(reconstructed.timestamp().value(), key.timestamp().value());
  EXPECT_EQ(reconstructed.column_id(), key.column_id());
}

TEST(LFNodeTest, NextAndSetNext) {
  CedarKey k1 = CedarKey::Vertex(1, 0, Timestamp(100));
  CedarKey k2 = CedarKey::Vertex(2, 0, Timestamp(200));
  LFNode node1(k1, Descriptor::InlineInt(0, 1), 4, Timestamp(1));
  LFNode node2(k2, Descriptor::InlineInt(0, 2), 4, Timestamp(2));

  node1.SetNext(0, &node2);
  EXPECT_EQ(node1.Next(0), &node2);
}

TEST(LFNodeTest, CASNext) {
  CedarKey k1 = CedarKey::Vertex(1, 0, Timestamp(100));
  CedarKey k2 = CedarKey::Vertex(2, 0, Timestamp(200));
  CedarKey k3 = CedarKey::Vertex(3, 0, Timestamp(300));
  LFNode node1(k1, Descriptor::InlineInt(0, 1), 4, Timestamp(1));
  LFNode node2(k2, Descriptor::InlineInt(0, 2), 4, Timestamp(2));
  LFNode node3(k3, Descriptor::InlineInt(0, 3), 4, Timestamp(3));

  node1.SetNext(0, &node2);
  bool swapped = node1.CASNext(0, &node2, &node3);
  EXPECT_TRUE(swapped);
  EXPECT_EQ(node1.Next(0), &node3);
}

TEST(LFNodeTest, VersionChainPointers) {
  CedarKey k = CedarKey::Vertex(1, 0, Timestamp(100));
  LFNode v1(k, Descriptor::InlineInt(0, 1), 4, Timestamp(1));
  LFNode v2(k, Descriptor::InlineInt(0, 2), 4, Timestamp(2));

  v1.SetOlderVersion(&v2);
  EXPECT_EQ(v1.OlderVersion(), &v2);
  // SetNewerVersion/NewerVersion not yet implemented
}

TEST(LFNodeTest, MarkDeleted) {
  CedarKey k = CedarKey::Vertex(1, 0, Timestamp(100));
  LFNode node(k, Descriptor::InlineInt(0, 1), 4, Timestamp(1));
  EXPECT_FALSE(node.MarkDeleted());  // Returns previous value (false = was not deleted)
  EXPECT_TRUE(node.MarkDeleted());   // Returns previous value (true = was already deleted)
}

TEST(LockedVSLTest, InsertAndGetLatest) {
  LockedVSL vsl;
  CedarKey key = CedarKey::Vertex(42, 0, Timestamp(1000));
  Descriptor desc = Descriptor::InlineInt(0, 123);

  bool inserted = vsl.Insert(key, desc, Timestamp(1));
  EXPECT_TRUE(inserted);

  auto latest = vsl.GetLatest(42, EntityType::Vertex, 0);
  ASSERT_TRUE(latest.has_value());
  EXPECT_EQ(latest->AsInlineInt().value(), 123);
}

TEST(LockedVSLTest, GetAtTime) {
  LockedVSL vsl;
  CedarKey key = CedarKey::Vertex(42, 0, Timestamp(1000));
  Descriptor desc = Descriptor::InlineInt(0, 100);

  vsl.Insert(key, desc, Timestamp(1));

  auto got = vsl.GetAtTime(42, EntityType::Vertex, 0, Timestamp(1000));
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->AsInlineInt().value(), 100);
}

TEST(LockedVSLTest, ScanRange) {
  LockedVSL vsl;
  for (int i = 0; i < 5; ++i) {
    CedarKey key = CedarKey::Vertex(1, 0, Timestamp(1000 + i));
    vsl.Insert(key, Descriptor::InlineInt(0, i), Timestamp(i + 1));
  }

  auto versions = vsl.ScanRange(1, EntityType::Vertex, 0, Timestamp(1000), Timestamp(1004));
  EXPECT_EQ(versions.size(), 5);
}

TEST(LockedVSLTest, SizeAndMemoryUsage) {
  LockedVSL vsl;
  EXPECT_EQ(vsl.size(), 0);
  CedarKey key = CedarKey::Vertex(1, 0, Timestamp(1000));
  vsl.Insert(key, Descriptor::InlineInt(0, 1), Timestamp(1));
  EXPECT_EQ(vsl.size(), 1);
  EXPECT_GT(vsl.ApproximateMemoryUsage(), 0);
}
