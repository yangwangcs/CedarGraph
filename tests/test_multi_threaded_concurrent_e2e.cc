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
#include <thread>
#include <vector>
#include <atomic>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
using namespace cedar::dtx;

class MultiThreadedConcurrentTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<StoragePartitionManager> partition_manager_;
  PartitionStorage* partition_ = nullptr;

  void SetUp() override {
    data_dir_ = "/tmp/cedar_mt_e2e_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    CedarOptions options;
    options.create_if_missing = true;
    options.enable_accumulated_flush = false;
    auto status = CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);

    partition_manager_ = std::make_unique<StoragePartitionManager>();
    StoragePartitionManager::PartitionConfig config;
    config.data_root = data_dir_;
    ASSERT_TRUE(partition_manager_->Initialize(config).ok());
    ASSERT_TRUE(partition_manager_->AddPartition(1).ok());

    partition_ = partition_manager_->GetPartition(1);
    ASSERT_NE(partition_, nullptr);
  }

  void TearDown() override {
    partition_manager_->Shutdown();
    partition_manager_.reset();
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};

TEST_F(MultiThreadedConcurrentTest, ConcurrentIndependentWriters) {
  constexpr int kNumThreads = 4;
  constexpr int kWritesPerThread = 50;
  constexpr int kTotalWrites = kNumThreads * kWritesPerThread;

  std::vector<std::thread> writers;
  std::atomic<int> success_count{0};

  for (int t = 0; t < kNumThreads; ++t) {
    writers.emplace_back([this, t, &success_count]() {
      for (int i = 0; i < kWritesPerThread; ++i) {
        uint64_t entity_id = static_cast<uint64_t>(t * kWritesPerThread + i + 1);
        int32_t expected_value = static_cast<int32_t>(entity_id * 100);

        CedarKey key;
        key.SetEntityId(entity_id);
        key.SetColumnId(1);
        key.SetEntityType(1);
        key.SetPartId(1);

        TxnID txn_id = static_cast<TxnID>(entity_id);
        std::vector<CedarKey> write_set = {key};
        std::unordered_map<uint64_t, Descriptor> write_descriptors;
        write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, expected_value);

        auto status = partition_->Prepare(txn_id, {}, write_set, write_descriptors, Timestamp(1000));
        if (!status.ok()) {
          continue;
        }
        status = partition_->Commit(txn_id, Timestamp(2000));
        if (status.ok()) {
          success_count.fetch_add(1);
        }
      }
    });
  }

  for (auto& t : writers) {
    t.join();
  }

  EXPECT_EQ(success_count.load(), kTotalWrites)
      << "Not all writes succeeded. Expected " << kTotalWrites;

  // Flush to ensure writes are persisted
  ASSERT_TRUE(partition_manager_->GetSharedStorage()->ForceFlush().ok());

  // Verify all entities are readable
  for (int t = 0; t < kNumThreads; ++t) {
    for (int i = 0; i < kWritesPerThread; ++i) {
      uint64_t entity_id = static_cast<uint64_t>(t * kWritesPerThread + i + 1);
      int32_t expected_value = static_cast<int32_t>(entity_id * 100);

      std::vector<std::pair<Timestamp, Descriptor>> versions;
      auto status = partition_manager_->GetSharedStorage()->ScanNode(entity_id, Timestamp::Max(), &versions);
      ASSERT_TRUE(status.ok()) << "ScanNode failed for entity " << entity_id;
      ASSERT_FALSE(versions.empty()) << "Entity " << entity_id << " not found";

      const auto& latest = versions.back();
      EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
      EXPECT_EQ(latest.second.GetPayload(), expected_value)
          << "Value mismatch for entity " << entity_id;
    }
  }
}
