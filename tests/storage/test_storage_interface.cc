// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include <gtest/gtest.h>
#include "cedar/storage/storage_interface.h"

using namespace cedar;
using namespace cedar::storage;

TEST(StorageInterfaceTest, NullStorageReturnsError) {
  StorageInterface iface(nullptr);
  Vertex v;
  v.id = 1;
  Status s = iface.InsertVertex(v, 0);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(StorageInterfaceTest, GetVertexWithNullStorage) {
  StorageInterface iface(nullptr);
  Descriptor desc;
  bool found = false;
  Status s = iface.GetVertex(1, 0, &desc, &found);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(StorageInterfaceTest, InsertEdgeWithNullStorage) {
  StorageInterface iface(nullptr);
  Edge e;
  e.src_id = 1;
  e.dst_id = 2;
  Status s = iface.InsertEdge(e, 0);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(StorageInterfaceTest, GetEdgeWithNullStorage) {
  StorageInterface iface(nullptr);
  Descriptor desc;
  bool found = false;
  Status s = iface.GetEdge(1, 2, "", 0, &desc, &found);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(StorageInterfaceTest, ScanVerticesWithNullStorage) {
  StorageInterface iface(nullptr);
  std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>> results;
  std::vector<PropertyPredicateItem> predicates;
  Status s = iface.ScanVertices(1, 0, 10, predicates, &results);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(StorageInterfaceTest, ScanOutEdgesWithNullStorage) {
  StorageInterface iface(nullptr);
  std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>> results;
  std::vector<PropertyPredicateItem> predicates;
  Status s = iface.ScanOutEdges(1, 0, 0, 10, predicates, &results);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(StorageInterfaceTest, ScanInEdgesWithNullStorage) {
  StorageInterface iface(nullptr);
  std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>> results;
  std::vector<PropertyPredicateItem> predicates;
  Status s = iface.ScanInEdges(1, 0, 0, 10, predicates, &results);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}
