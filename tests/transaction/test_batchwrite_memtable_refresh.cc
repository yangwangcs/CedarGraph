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
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/occ_transaction.h"

using namespace cedar;

TEST(BatchWriteMemtableRefreshTest, NoLostWritesDuringFlush) {
  std::string data_dir = "/tmp/test_batchwrite_memtable_refresh_" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, data_dir, &storage).ok());
  ASSERT_NE(storage, nullptr);

  LsmEngine* lsm = storage->GetLsmEngine();
  ASSERT_NE(lsm, nullptr);

  const int kTotalItems = 500;
  const size_t kBatchSize = 50;

  std::vector<CedarGraphStorage::BatchWriteItem> items;
  items.reserve(kTotalItems);
  for (int i = 0; i < kTotalItems; ++i) {
    // Use non-static timestamps != 1 to avoid the static-column flag that
    // BatchWrite adds when timestamp.IsStatic() is true.
    items.emplace_back(
        static_cast<uint64_t>(i),
        EntityType::Vertex,
        0,
        Descriptor::InlineInt(0, i * 10),
        Timestamp(static_cast<uint64_t>(i + 2)),
        0);
  }

  std::atomic<bool> batch_done{false};

  // Background thread: hammer ForceFlush to trigger memtable swaps
  std::thread flusher([&]() {
    while (!batch_done.load()) {
      lsm->ForceFlush();
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });

  // Main thread: run BatchWrite while flusher is active
  Status s = storage->BatchWrite(items, kBatchSize);
  ASSERT_TRUE(s.ok()) << s.ToString();
  batch_done.store(true);
  flusher.join();

  // One final flush to ensure everything is persisted
  ASSERT_TRUE(lsm->ForceFlush().ok());

  // Verify every item is readable via the storage API (uses GetAtTime,
  // which correctly falls back to levels_ when the compaction engine
  // metadata is temporarily out of sync).
  for (int i = 0; i < kTotalItems; ++i) {
    auto desc_opt = storage->GetDynamicVertex(
        static_cast<uint64_t>(i), 0, Timestamp::Max());
    ASSERT_TRUE(desc_opt.has_value())
        << "Key " << i << " lost after BatchWrite + concurrent flush";

    auto val = desc_opt.value().AsInlineInt();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), i * 10);
  }

  delete storage;
  std::filesystem::remove_all(data_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
