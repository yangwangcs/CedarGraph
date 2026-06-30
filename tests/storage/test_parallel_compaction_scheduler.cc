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
#include <future>
#include <thread>

#include "cedar/storage/parallel_compaction_engine.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/core/env.h"

using namespace cedar;

class ParallelCompactionSchedulerTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  Env* env_ = nullptr;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_parallel_compaction_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
    env_ = Env::Default();
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }
};

TEST_F(ParallelCompactionSchedulerTest, ScheduleIfNeededEnqueuesTask) {
  // Configure with low thresholds so 2 files trigger compaction
  SizeTieredConfig config;
  config.l0_max_files = 1;
  config.enable_background_compaction = false;

  SizeTieredCompactionEngine engine(test_dir_, config, env_);
  ASSERT_TRUE(engine.Open().ok());

  // Create 2 small SST files in L0
  for (uint64_t i = 1; i <= 2; ++i) {
    ZoneSstMeta meta;
    meta.file_number = i;
    meta.file_size = 1024;  // 1KB, well below 8MB small-file threshold
    meta.level = 0;
    meta.min_entity_id = i * 100;
    meta.max_entity_id = i * 100 + 99;
    meta.min_timestamp = 1000;
    meta.max_timestamp = 2000;
    meta.column_id = 1;
    meta.entity_type = 0;
    ASSERT_TRUE(engine.AddSSTFile(meta).ok());
  }

  ASSERT_TRUE(engine.NeedsCompaction());

  ParallelCompactionConfig parallel_config;
  parallel_config.num_threads = 1;
  ParallelCompactionEngine parallel_engine(&engine, parallel_config);

  // Before scheduling, queue should be empty
  EXPECT_EQ(parallel_engine.PendingTasks(), 0);

  // Schedule a compaction task
  bool scheduled = parallel_engine.ScheduleIfNeeded();
  EXPECT_TRUE(scheduled);

  // After scheduling, queue should have the task
  EXPECT_GT(parallel_engine.PendingTasks(), 0);

  parallel_engine.Stop();
  engine.Close().IgnoreError();
}

TEST_F(ParallelCompactionSchedulerTest, WaitForAllObservesAcceptedTasks) {
  SizeTieredConfig config;
  config.l0_max_files = 1;
  config.enable_background_compaction = false;

  SizeTieredCompactionEngine engine(test_dir_, config, env_);
  ASSERT_TRUE(engine.Open().ok());

  for (uint64_t i = 1; i <= 2; ++i) {
    ZoneSstMeta meta;
    meta.file_number = i;
    meta.file_size = 1024;
    meta.level = 0;
    meta.min_entity_id = i * 100;
    meta.max_entity_id = i * 100 + 99;
    meta.min_timestamp = 1000;
    meta.max_timestamp = 2000;
    meta.column_id = 1;
    meta.entity_type = 0;
    ASSERT_TRUE(engine.AddSSTFile(meta).ok());
  }

  ParallelCompactionConfig parallel_config;
  parallel_config.num_threads = 1;
  ParallelCompactionEngine parallel_engine(&engine, parallel_config);
  parallel_engine.Start();

  ASSERT_TRUE(parallel_engine.ScheduleIfNeeded());

  auto wait_future = std::async(std::launch::async, [&]() {
    parallel_engine.WaitForAll();
  });

  EXPECT_EQ(wait_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_EQ(parallel_engine.PendingTasks(), 0u);
  EXPECT_EQ(parallel_engine.ActiveTasks(), 0);

  parallel_engine.Stop();
  engine.Close().IgnoreError();
}

TEST_F(ParallelCompactionSchedulerTest, CloseWakesPausedBackgroundCompaction) {
  SizeTieredConfig config;
  config.l0_max_files = 1;
  config.enable_background_compaction = true;
  config.compaction_threads = 1;

  SizeTieredCompactionEngine engine(test_dir_, config, env_);
  ASSERT_TRUE(engine.Open().ok());

  engine.PauseCompaction();

  for (uint64_t i = 1; i <= 2; ++i) {
    ZoneSstMeta meta;
    meta.file_number = i;
    meta.file_size = 1024;
    meta.level = 0;
    meta.min_entity_id = i * 100;
    meta.max_entity_id = i * 100 + 99;
    meta.min_timestamp = 1000;
    meta.max_timestamp = 2000;
    meta.column_id = 1;
    meta.entity_type = 0;
    ASSERT_TRUE(engine.AddSSTFile(meta).ok());
  }

  auto close_future = std::async(std::launch::async, [&]() {
    return engine.Close();
  });

  ASSERT_EQ(close_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_TRUE(close_future.get().ok());
}

TEST_F(ParallelCompactionSchedulerTest, ColumnAvailabilityNotificationWakesWaiter) {
  ParallelCompactionConfig config;
  ColumnGroupTaskQueue queue(config);
  queue.MarkColumnBusy("vertex:1", true);

  auto wait_future = std::async(std::launch::async, [&]() {
    return queue.WaitForColumnAvailable("vertex:1", std::chrono::seconds(30));
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  auto start = std::chrono::steady_clock::now();
  queue.MarkColumnBusy("vertex:1", false);

  ASSERT_EQ(wait_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_TRUE(wait_future.get());
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}
