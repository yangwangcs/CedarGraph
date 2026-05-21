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
#include <filesystem>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class TombstoneFlushTest : public ::testing::Test {
 protected:
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/tombstone_flush_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }
};

TEST_F(TombstoneFlushTest, DeleteSurvivesFlush) {
  CedarOptions opts;
  opts.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(opts, test_dir_, &storage).ok());
  ASSERT_NE(storage, nullptr);

  // Write a value, then delete it
  ASSERT_TRUE(storage->Put(1, 100, Descriptor::InlineInt(0, 42), Timestamp(1)).ok());
  ASSERT_TRUE(storage->Delete(1, 200, Timestamp(2)).ok());

  // Before flush: Get should see no value (tombstone in memtable)
  auto result_before = storage->Get(1, 200);
  EXPECT_FALSE(result_before.has_value());

  // Force flush to SST
  ASSERT_TRUE(storage->ForceFlush().ok());

  // After flush: tombstone must survive; Get should still see no value
  auto result_after = storage->Get(1, 200);
  EXPECT_FALSE(result_after.has_value())
      << "Tombstone was lost on flush — old value reappeared from SST";

  delete storage;
}
