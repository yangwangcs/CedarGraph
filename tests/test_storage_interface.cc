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
#include <chrono>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class StorageInterfaceTest : public ::testing::Test {
 protected:
  std::string db_path_;
  CedarGraphStorage* storage_ = nullptr;

  void SetUp() override {
    db_path_ = "/tmp/test_storage_interface_" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(db_path_);
    
    CedarOptions options;
    options.create_if_missing = true;
    auto status = CedarGraphStorage::Open(options, db_path_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(db_path_);
  }
};

TEST_F(StorageInterfaceTest, PutAndGetVertex) {
  Descriptor desc = Descriptor::InlineInt(0, 42);
  Timestamp ts(1000);
  
  auto status = storage_->Put(100, 1000, desc, ts);
  EXPECT_TRUE(status.ok()) << status.ToString();
  
  // Verify by reading back
  auto result = storage_->Get(100, 1000);
  EXPECT_TRUE(result.has_value());
}

TEST_F(StorageInterfaceTest, ScanReturnsVersions) {
  // Write multiple versions
  for (uint64_t i = 1; i <= 5; ++i) {
    Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(i * 10));
    Timestamp ts(i * 100);
    auto status = storage_->Put(200, i * 100, desc, ts);
    EXPECT_TRUE(status.ok()) << status.ToString();
  }
  
  auto versions = storage_->Scan(200, Timestamp(0), Timestamp::Max());
  EXPECT_EQ(versions.size(), 5);
}

TEST_F(StorageInterfaceTest, BatchGet) {
  // Write multiple entities
  for (uint64_t i = 1; i <= 10; ++i) {
    Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(i));
    storage_->Put(i, i * 100, desc, Timestamp(i * 100));
  }
  
  // Batch get
  std::vector<uint64_t> entity_ids = {1, 3, 5, 7, 9};
  for (auto id : entity_ids) {
    auto result = storage_->Get(id, id * 100);
    EXPECT_TRUE(result.has_value()) << "Entity " << id << " not found";
  }
}

TEST_F(StorageInterfaceTest, ForceFlush) {
  Descriptor desc = Descriptor::InlineInt(0, 99);
  storage_->Put(300, 1000, desc, Timestamp(1000));
  
  auto status = storage_->ForceFlush();
  EXPECT_TRUE(status.ok()) << status.ToString();
}
