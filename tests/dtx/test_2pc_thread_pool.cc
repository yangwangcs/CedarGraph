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

// =============================================================================
// 2PC Thread Pool Test — Verify fixed-size pool replaces thread-per-RPC
// =============================================================================
// Verifies that the ThreadPool used by Optimized2PCEngine:
//   1. Never exceeds its configured thread count
//   2. Correctly schedules and executes concurrent tasks
//   3. Waits for all tasks to finish before shutdown
// =============================================================================

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "cedar/core/threading.h"

using namespace cedar;

TEST(ThreadPoolTest, DoesNotExceedMaxThreads) {
  constexpr size_t kPoolSize = 4;
  constexpr int kTasks = 100;

  ThreadPool pool(kPoolSize);
  std::atomic<int> active{0};
  std::atomic<int> max_active{0};
  std::vector<std::promise<void>> promises(kTasks);
  std::vector<std::future<void>> futures;
  futures.reserve(kTasks);
  for (auto& p : promises) {
    futures.push_back(p.get_future());
  }

  for (int i = 0; i < kTasks; ++i) {
    pool.Schedule([&active, &max_active, &promises, i]() {
      int a = ++active;
      int expected = max_active.load();
      while (a > expected && !max_active.compare_exchange_weak(expected, a)) {
        // retry
      }
      std::this_thread::sleep_for(std::chrono::microseconds(500));
      --active;
      promises[i].set_value();
    });
  }

  for (auto& f : futures) {
    f.wait();
  }

  EXPECT_LE(max_active.load(), static_cast<int>(kPoolSize))
      << "Peak concurrent threads exceeded pool size";
  EXPECT_EQ(active.load(), 0)
      << "All tasks should have finished";
}

TEST(ThreadPoolTest, WaitForAllBlocksUntilCompletion) {
  ThreadPool pool(2);
  std::atomic<int> counter{0};

  for (int i = 0; i < 10; ++i) {
    pool.Schedule([&counter]() {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      counter.fetch_add(1);
    });
  }

  pool.WaitForAll();
  EXPECT_EQ(counter.load(), 10)
      << "WaitForAll should block until all tasks complete";
}

TEST(ThreadPoolTest, ConcurrentTaskExecution) {
  ThreadPool pool(4);
  std::atomic<int> sum{0};
  constexpr int kTasks = 1000;
  std::vector<std::promise<void>> promises(kTasks);
  std::vector<std::future<void>> futures;
  futures.reserve(kTasks);
  for (auto& p : promises) {
    futures.push_back(p.get_future());
  }

  for (int i = 0; i < kTasks; ++i) {
    pool.Schedule([&sum, &promises, i]() {
      sum.fetch_add(1);
      promises[i].set_value();
    });
  }

  for (auto& f : futures) {
    f.wait();
  }

  EXPECT_EQ(sum.load(), kTasks)
      << "All scheduled tasks should execute exactly once";
}
