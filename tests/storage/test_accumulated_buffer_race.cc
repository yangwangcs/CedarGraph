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
#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class AccumulatedBufferRaceTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  cedar::CedarGraphStorage* storage_ = nullptr;
  cedar::LsmEngine* engine_ = nullptr;

  void SetUp() override {
    data_dir_ = "/tmp/test_accumulated_race_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    cedar::CedarOptions options;
    options.create_if_missing = true;
    options.enable_accumulated_flush = true;
    options.accumulated_flush_size_mb = 64;
    options.memtable_threshold = 64 * 1024;

    auto status = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);

    engine_ = storage_->GetLsmEngine();
    ASSERT_NE(engine_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};

TEST_F(AccumulatedBufferRaceTest, QueryAndFlushConcurrently) {
  const uint64_t kEntityId = 42;
  const uint16_t kColumnId = 7;
  const EntityType kType = EntityType::Vertex;

  // Pre-populate accumulated buffer by writing enough to trigger memtable flush.
  for (int i = 0; i < 1024; ++i) {
    CedarKey key = CedarKey::Vertex(kEntityId, kColumnId, Timestamp(static_cast<uint64_t>(i) + 1));
    Descriptor desc = Descriptor::InlineInt(kColumnId, i);
    Status s = engine_->Put(key, desc, Timestamp(1));
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Wait for background flushes to move data into accumulated_entries_.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  std::atomic<bool> stop{false};
  std::atomic<int> read_errors{0};
  std::atomic<int> flush_errors{0};
  std::atomic<int> reads{0};

  // Reader thread: repeatedly queries the accumulated buffer via GetAtTime.
  std::thread reader([&]() {
    while (!stop.load()) {
      auto result = engine_->GetAtTime(kEntityId, kType, kColumnId,
                                       Timestamp(1024));
      (void)result;  // Exercise the code path under the lock.
      reads.fetch_add(1);
    }
  });

  // Flusher thread: repeatedly flushes accumulated entries.
  std::thread flusher([&]() {
    while (!stop.load()) {
      Status s = engine_->FlushAccumulated();
      if (!s.ok()) {
        flush_errors.fetch_add(1);
      }
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  });

  // Let the threads race for a short duration.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop.store(true);

  reader.join();
  flusher.join();

  EXPECT_EQ(read_errors.load(), 0);
  EXPECT_EQ(flush_errors.load(), 0);
  EXPECT_GT(reads.load(), 0);
}
