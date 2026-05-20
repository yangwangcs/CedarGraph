// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// VertexLocationTable Unit Test
// =============================================================================

#include <gtest/gtest.h>

#include "cedar/coordinator/location_table.h"

using namespace cedar::coordinator;

class LocationTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    table_ = std::make_unique<VertexLocationTable>();
  }

  std::unique_ptr<VertexLocationTable> table_;
};

// ---------------------------------------------------------------------------
// Locate
// ---------------------------------------------------------------------------

TEST_F(LocationTableTest, LocateMissingReturnsNullopt) {
  auto result = table_->Locate(42, 100);
  EXPECT_FALSE(result.has_value());
}

TEST_F(LocationTableTest, LocateExistingWithinWindow) {
  CacheWindow w;
  w.entity_id = 42;
  w.cached_from = 50;
  w.cached_to = 150;
  w.gcn_node_id = 7;
  w.version = 1;
  w.expire_at = 1000;
  table_->ReportCache(w);

  auto result = table_->Locate(42, 100);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->entity_id, 42);
  EXPECT_EQ(result->cached_from, 50);
  EXPECT_EQ(result->cached_to, 150);
  EXPECT_EQ(result->gcn_node_id, 7);
}

TEST_F(LocationTableTest, LocateOutsideWindowReturnsNullopt) {
  CacheWindow w;
  w.entity_id = 42;
  w.cached_from = 50;
  w.cached_to = 150;
  w.gcn_node_id = 7;
  w.version = 1;
  w.expire_at = 1000;
  table_->ReportCache(w);

  EXPECT_FALSE(table_->Locate(42, 10).has_value());
  EXPECT_FALSE(table_->Locate(42, 200).has_value());
}

// ---------------------------------------------------------------------------
// ReportCache versioning
// ---------------------------------------------------------------------------

TEST_F(LocationTableTest, ReportCacheOverwritesWithHigherVersion) {
  CacheWindow w1{42, 50, 150, 7, 1, 1000};
  table_->ReportCache(w1);

  CacheWindow w2{42, 60, 160, 8, 2, 1000};
  table_->ReportCache(w2);

  auto result = table_->Locate(42, 100);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->gcn_node_id, 8);
  EXPECT_EQ(result->cached_from, 60);
}

TEST_F(LocationTableTest, ReportCacheIgnoresLowerVersion) {
  CacheWindow w1{42, 50, 150, 7, 2, 1000};
  table_->ReportCache(w1);

  CacheWindow w2{42, 60, 160, 8, 1, 1000};
  table_->ReportCache(w2);

  auto result = table_->Locate(42, 100);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->gcn_node_id, 7);
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

TEST_F(LocationTableTest, HeartbeatRefreshesAndAddsWindows) {
  CacheWindow w1{42, 50, 150, 7, 1, 1000};
  table_->ReportCache(w1);

  CacheWindow w2{43, 10, 90, 8, 1, 1000};
  table_->Heartbeat({w2});

  EXPECT_TRUE(table_->Locate(42, 100).has_value());
  EXPECT_TRUE(table_->Locate(43, 50).has_value());
}

// ---------------------------------------------------------------------------
// GCExpired
// ---------------------------------------------------------------------------

TEST_F(LocationTableTest, GCExpiredRemovesStaleEntries) {
  CacheWindow w1{42, 50, 150, 7, 1, 100};   // expires at 100
  CacheWindow w2{43, 10, 90, 8, 1, 500};    // expires at 500
  table_->ReportCache(w1);
  table_->ReportCache(w2);

  EXPECT_EQ(table_->Size(), 2);

  table_->GCExpired(200);  // now = 200

  EXPECT_EQ(table_->Size(), 1);
  EXPECT_FALSE(table_->Locate(42, 100).has_value());
  EXPECT_TRUE(table_->Locate(43, 50).has_value());
}

TEST_F(LocationTableTest, GCExpiredKeepsFreshEntries) {
  CacheWindow w1{42, 50, 150, 7, 1, 1000};
  CacheWindow w2{43, 10, 90, 8, 1, 1000};
  table_->ReportCache(w1);
  table_->ReportCache(w2);

  table_->GCExpired(500);

  EXPECT_EQ(table_->Size(), 2);
}

TEST_F(LocationTableTest, GCExpiredOnEmptyTableIsNoOp) {
  table_->GCExpired(1000);
  EXPECT_EQ(table_->Size(), 0);
}

TEST_F(LocationTableTest, GCExpiredExactBoundary) {
  CacheWindow w{42, 50, 150, 7, 1, 100};
  table_->ReportCache(w);

  table_->GCExpired(100);  // expire_at == now, should be kept (expire_at < now)
  EXPECT_EQ(table_->Size(), 1);

  table_->GCExpired(101);  // expire_at < now, should be removed
  EXPECT_EQ(table_->Size(), 0);
}
