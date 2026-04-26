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

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "cedar/gcn/storage_backfill_service.h"
#include "cedar/gcn/tmv_engine.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::gcn;

class StorageBackfillTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;

  void SetUp() override {
    CedarGraphStorage::DestroyDB("/tmp/test_backfill", CedarOptions());

    CedarOptions options;
    options.create_if_missing = true;

    Status s = CedarGraphStorage::Open(options, "/tmp/test_backfill", &storage_);
    ASSERT_TRUE(s.ok());
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    CedarGraphStorage::DestroyDB("/tmp/test_backfill", CedarOptions());
  }
};

TEST_F(StorageBackfillTest, BackfillVertexAndScan) {
  // Insert edges into storage
  Descriptor desc = Descriptor::InlineInt(0, 42);
  Status s = storage_->PutEdge(1, 100, 1, Timestamp(1000), desc, Timestamp(1));
  ASSERT_TRUE(s.ok());
  s = storage_->PutEdge(1, 200, 1, Timestamp(2000), desc, Timestamp(2));
  ASSERT_TRUE(s.ok());

  // Create TMVEngine and backfill
  TMVEngine engine(16);
  StorageBackfillService backfill(&engine, storage_);
  backfill.BackfillVertex(1, 1);

  // Verify edges are visible via TMV ScanAtTime
  auto edges_at_1500 = engine.ScanAtTime(1, Direction::kOut, 1500);
  EXPECT_EQ(edges_at_1500.size(), 1u);
  EXPECT_EQ(edges_at_1500[0].target_id, 100u);

  auto edges_at_2500 = engine.ScanAtTime(1, Direction::kOut, 2500);
  EXPECT_EQ(edges_at_2500.size(), 2u);
}

TEST_F(StorageBackfillTest, BackfillRange) {
  Descriptor desc = Descriptor::InlineInt(0, 1);

  for (uint64_t vid = 10; vid <= 12; ++vid) {
    Status s = storage_->PutEdge(vid, vid + 100, 1, Timestamp(1000), desc, Timestamp(1));
    ASSERT_TRUE(s.ok());
  }

  TMVEngine engine(16);
  StorageBackfillService backfill(&engine, storage_);
  backfill.BackfillRange(10, 12, 1);

  for (uint64_t vid = 10; vid <= 12; ++vid) {
    auto edges = engine.ScanAtTime(vid, Direction::kOut, 1500);
    ASSERT_EQ(edges.size(), 1u);
    EXPECT_EQ(edges[0].target_id, vid + 100);
  }
}
