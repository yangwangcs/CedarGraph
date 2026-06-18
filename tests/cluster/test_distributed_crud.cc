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

#include <chrono>
#include <gtest/gtest.h>
#include <unistd.h>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"

using namespace cedar;

class DistributedCrudTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/test_crud_" + std::to_string(getpid()) + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    // Clean up any existing test data
    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());

    CedarOptions options;
    options.create_if_missing = true;
    options.distributed_mode = false;  // 先用单机模式测试 Delete 语义

    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
    ASSERT_TRUE(s.ok());
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    // Clean up test data
    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());
  }
};

TEST_F(DistributedCrudTest, DeleteCreatesTombstone) {
  // 1. Put a value
  Descriptor write_desc = Descriptor::InlineInt(0, 42);
  Status s = storage_->Put(1001, 1000000, write_desc, Timestamp(1));
  ASSERT_TRUE(s.ok());

  // 2. Read it back
  auto result = storage_->Get(1001, 1000000);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInlineInt().value_or(0), 42);

  // 3. Delete it
  s = storage_->Delete(1001, 1000000, Timestamp(2));
  ASSERT_TRUE(s.ok());

  // 4. Read should return tombstone or not found
  result = storage_->Get(1001, 1000000);
  if (result.has_value()) {
    EXPECT_TRUE(result->IsTombstone()) << "Expected tombstone after delete, got kind=" << static_cast<int>(result->GetKind());
  }
}

TEST_F(DistributedCrudTest, DeleteNonExistentKey) {
  Status s = storage_->Delete(9999, 1000000, Timestamp(1));
  EXPECT_TRUE(s.ok()) << "Delete of non-existent key should succeed";
}

TEST_F(DistributedCrudTest, DescriptorTombstoneApi) {
  Descriptor tombstone = Descriptor::Tombstone();
  EXPECT_TRUE(tombstone.IsTombstone());
  EXPECT_EQ(tombstone.GetKind(), EntryKind::Tombstone);
}

TEST_F(DistributedCrudTest, TombstoneWithColumnId) {
  // Test the existing Tombstone(column_id) method
  Descriptor tombstone = Descriptor::Tombstone(5);
  EXPECT_TRUE(tombstone.IsTombstone());
  EXPECT_EQ(tombstone.GetKind(), EntryKind::Tombstone);
  EXPECT_EQ(tombstone.GetColumnId(), 5);
}

TEST_F(DistributedCrudTest, DefaultDescriptorIsTombstone) {
  // Default constructor should create a Tombstone
  Descriptor d;
  EXPECT_TRUE(d.IsTombstone());
  EXPECT_EQ(d.GetKind(), EntryKind::Tombstone);
}

TEST_F(DistributedCrudTest, CrudWorkflow) {
  const uint64_t entity_id = 2001;
  const uint64_t tx_time = 1000000;

  // Create
  Descriptor create_desc = Descriptor::InlineInt(1, 100);
  Status s = storage_->Put(entity_id, tx_time, create_desc, Timestamp(1));
  ASSERT_TRUE(s.ok());

  // Read
  auto result = storage_->Get(entity_id, tx_time);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInlineInt().value_or(0), 100);

  // Update
  Descriptor update_desc = Descriptor::InlineInt(1, 200);
  s = storage_->Put(entity_id, tx_time + 1, update_desc, Timestamp(2));
  ASSERT_TRUE(s.ok());

  // Read updated value
  result = storage_->Get(entity_id, tx_time + 1);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInlineInt().value_or(0), 200);

  // Delete
  s = storage_->Delete(entity_id, tx_time + 2, Timestamp(3));
  ASSERT_TRUE(s.ok());

  // Read after delete - should be not found (tombstone filtered by Get)
  result = storage_->Get(entity_id, tx_time + 2);
  EXPECT_FALSE(result.has_value());
}
