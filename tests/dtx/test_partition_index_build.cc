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
#include "cedar/dtx/partition_index.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(PartitionIndexBuild, BuildIndexOnEmptyStorage) {
  std::string data_dir = "/tmp/test_index_empty";
  std::filesystem::remove_all(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  options.enable_accumulated_flush = false;
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, data_dir, &storage);
  ASSERT_TRUE(status.ok());
  ASSERT_NE(storage, nullptr);

  PartitionIndex index(storage);
  auto s = index.BuildIndex();
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto partitions = index.GetAllPartitions();
  EXPECT_TRUE(partitions.empty());

  delete storage;
  std::filesystem::remove_all(data_dir);
}

TEST(PartitionIndexBuild, BuildIndexAfterFlush) {
  std::string data_dir = "/tmp/test_index_flush";
  std::filesystem::remove_all(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  options.enable_accumulated_flush = false;
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, data_dir, &storage);
  ASSERT_TRUE(status.ok());

  // Write data to multiple partitions
  for (uint64_t i = 1; i <= 100; i++) {
    Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(i));
    storage->Put(i, i * 1000, desc, Timestamp(i));
  }
  storage->ForceFlush();

  PartitionIndex index(storage);
  auto s = index.BuildIndex();
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto partitions = index.GetAllPartitions();
  EXPECT_FALSE(partitions.empty());

  // Check that partition 1 (entity_id 1 has part_id = 1) has a valid range
  auto range = index.GetPartitionRange(1);
  EXPECT_LE(range.min_entity_id, 1);
  EXPECT_GE(range.max_entity_id, 1);
  EXPECT_GE(range.estimated_key_count, 1);

  delete storage;
  std::filesystem::remove_all(data_dir);
}
