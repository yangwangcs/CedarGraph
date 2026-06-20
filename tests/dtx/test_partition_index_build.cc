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
#include <unistd.h>
#include "cedar/dtx/partition_index.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(PartitionIndexBuild, BuildIndexOnEmptyStorage) {
  std::string data_dir = "/tmp/test_index_empty_" + std::to_string(getpid());
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
  std::string data_dir = "/tmp/test_index_flush_" + std::to_string(getpid());
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

  // Check that the partition containing entity_id 1 has a valid range
  // Note: With MurmurHash2, entity_id 1 may not be in partition 1
  // So we check that at least one partition has valid data
  bool found_valid_range = false;
  for (const auto& pid : partitions) {
    auto range = index.GetPartitionRange(pid);
    if (range.estimated_key_count > 0) {
      found_valid_range = true;
      break;
    }
  }
  EXPECT_TRUE(found_valid_range) << "No partition has valid entity range";

  delete storage;
  std::filesystem::remove_all(data_dir);
}
