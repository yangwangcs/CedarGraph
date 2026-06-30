// Copyright 2025 The Cedar Authors
//
// Regression tests for AsyncIndexBuilder lifecycle and wait semantics.

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

#include "cedar/storage/async_index_builder.h"
#include "cedar/storage/cedar_memtable.h"

using namespace cedar;

TEST(AsyncIndexBuilderLifecycleTest, WaitForAllIncludesInFlightBuilds) {
  AsyncIndexBuilderOptions options;
  options.num_worker_threads = 1;
  options.enable_batch_building = false;
  options.enable_build_cache = false;

  AsyncIndexBuilder builder(options);
  ASSERT_TRUE(builder.Initialize().ok());

  std::vector<std::unique_ptr<TemporalVersionNode>> nodes;
  nodes.emplace_back(std::make_unique<TemporalVersionNode>(
      Timestamp(3), Descriptor::InlineInt(0, 3), Timestamp(3)));
  nodes.emplace_back(std::make_unique<TemporalVersionNode>(
      Timestamp(2), Descriptor::InlineInt(0, 2), Timestamp(2)));
  nodes.emplace_back(std::make_unique<TemporalVersionNode>(
      Timestamp(1), Descriptor::InlineInt(0, 1), Timestamp(1)));
  nodes[0]->older = nodes[1].get();
  nodes[1]->newer = nodes[0].get();
  nodes[1]->older = nodes[2].get();
  nodes[2]->newer = nodes[1].get();

  std::mutex mutex;
  std::condition_variable cv;
  bool callback_entered = false;
  bool release_callback = false;

  IndexBuildTask task;
  task.entity_id = 1;
  task.entity_type = EntityType::Vertex;
  task.column_id = 1;
  task.version_head = nodes[0].get();
  task.version_count = nodes.size();
  task.on_complete = [&](VersionChainIndex* index) {
    delete index;
    std::unique_lock<std::mutex> lock(mutex);
    callback_entered = true;
    cv.notify_all();
    cv.wait(lock, [&]() { return release_callback; });
  };

  ASSERT_TRUE(builder.SubmitTask(task).ok());

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1),
                            [&]() { return callback_entered; }));
  }

  EXPECT_EQ(builder.GetQueueDepth(), 0u);
  EXPECT_FALSE(builder.WaitForAll(100).ok());

  {
    std::lock_guard<std::mutex> lock(mutex);
    release_callback = true;
  }
  cv.notify_all();

  EXPECT_TRUE(builder.WaitForAll(1000).ok());
  EXPECT_TRUE(builder.Shutdown().ok());
}
