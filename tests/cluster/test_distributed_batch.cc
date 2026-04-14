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
#include <chrono>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;

class DistributedBatchTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::string test_dir_ = "/tmp/test_batch";
  
  void SetUp() override {
    // Clean up any previous test data
    std::filesystem::remove_all(test_dir_);
    
    CedarOptions options;
    options.create_if_missing = true;
    options.distributed_mode = false;  // Single-node mode test
    
    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
    ASSERT_TRUE(s.ok()) << "Failed to open storage: " << s.ToString();
  }
  
  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    // Clean up test data
    std::filesystem::remove_all(test_dir_);
  }
};

TEST_F(DistributedBatchTest, BatchWriteReturnsSuccess) {
  std::vector<CedarGraphStorage::BatchWriteItem> items;
  for (int i = 0; i < 100; i++) {
    Descriptor desc = Descriptor::InlineInt(0, i);
    items.emplace_back(1000 + i, EntityType::Vertex, 1, desc, Timestamp(i + 1));
  }
  
  Status s = storage_->BatchWrite(items);
  ASSERT_TRUE(s.ok()) << "BatchWrite failed: " << s.ToString();
  
  // Force flush to ensure data is persisted
  s = storage_->ForceFlush();
  ASSERT_TRUE(s.ok()) << "ForceFlush failed: " << s.ToString();
}

TEST_F(DistributedBatchTest, BatchWriteLargeDataset) {
  std::vector<CedarGraphStorage::BatchWriteItem> items;
  const int count = 1000;
  items.reserve(count);
  
  for (int i = 0; i < count; i++) {
    Descriptor desc = Descriptor::InlineInt(0, i);
    items.emplace_back(2000 + i, EntityType::Vertex, 1, desc, Timestamp(i + 1));
  }
  
  auto start = std::chrono::steady_clock::now();
  Status s = storage_->BatchWrite(items, 100);
  auto end = std::chrono::steady_clock::now();
  
  ASSERT_TRUE(s.ok()) << "BatchWrite failed: " << s.ToString();
  
  // Force flush
  s = storage_->ForceFlush();
  ASSERT_TRUE(s.ok()) << "ForceFlush failed: " << s.ToString();
  
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "BatchWrite " << count << " items in " << duration.count() << "ms" << std::endl;
  
  // Verify throughput (at least 100 TPS)
  double tps = count * 1000.0 / std::max(duration.count(), static_cast<int64_t>(1));
  EXPECT_GT(tps, 100.0) << "Write throughput too low: " << tps << " TPS";
}

TEST_F(DistributedBatchTest, EmptyBatchWrite) {
  std::vector<CedarGraphStorage::BatchWriteItem> items;
  Status s = storage_->BatchWrite(items);
  EXPECT_TRUE(s.ok()) << "Empty batch should succeed";
}

TEST_F(DistributedBatchTest, BatchWriteWithBatchSize) {
  std::vector<CedarGraphStorage::BatchWriteItem> items;
  for (int i = 0; i < 500; i++) {
    Descriptor desc = Descriptor::InlineInt(0, i * 10);
    items.emplace_back(3000 + i, EntityType::Vertex, 1, desc, Timestamp(i + 1));
  }
  
  // Use smaller batch size
  Status s = storage_->BatchWrite(items, 50);
  ASSERT_TRUE(s.ok()) << "BatchWrite with custom batch size failed: " << s.ToString();
  
  // Force flush
  s = storage_->ForceFlush();
  ASSERT_TRUE(s.ok()) << "ForceFlush failed: " << s.ToString();
}

TEST_F(DistributedBatchTest, BatchWriteStaticProperties) {
  std::vector<std::pair<uint64_t, Descriptor>> items;
  for (int i = 0; i < 50; i++) {
    items.emplace_back(4000 + i, Descriptor::InlineInt(0, i + 100));
  }
  
  Status s = storage_->BatchPutStaticVertex(items, 1);
  ASSERT_TRUE(s.ok()) << "BatchPutStaticVertex failed: " << s.ToString();
  
  // Force flush
  s = storage_->ForceFlush();
  ASSERT_TRUE(s.ok()) << "ForceFlush failed: " << s.ToString();
}

TEST_F(DistributedBatchTest, BatchWriteDynamicProperties) {
  std::vector<std::tuple<uint64_t, Timestamp, Descriptor>> items;
  for (int i = 0; i < 50; i++) {
    items.emplace_back(5000 + i, Timestamp(i + 1), Descriptor::InlineInt(0, i + 200));
  }
  
  Status s = storage_->BatchPutDynamicVertex(items, 1);
  ASSERT_TRUE(s.ok()) << "BatchPutDynamicVertex failed: " << s.ToString();
  
  // Force flush
  s = storage_->ForceFlush();
  ASSERT_TRUE(s.ok()) << "ForceFlush failed: " << s.ToString();
}
