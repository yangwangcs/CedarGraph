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
// Query Slot Rate Limiter Race Test — CAS-loop concurrency safety
// =============================================================================
// Verifies that the compare-exchange-weak loop used in AcquireQuerySlot
// never allows concurrent threads to exceed the max_concurrent_queries limit.
// =============================================================================

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>

// Reproduce the exact CAS logic from QueryServiceImpl::Impl::AcquireQuerySlot
static bool TryAcquireSlot(std::atomic<uint32_t>& current,
                           uint32_t max_slots) {
  uint32_t c = current.load(std::memory_order_relaxed);
  do {
    if (c >= max_slots) {
      return false;
    }
  } while (!current.compare_exchange_weak(
      c, c + 1,
      std::memory_order_relaxed,
      std::memory_order_relaxed));
  return true;
}

TEST(RateLimiterRaceTest, ConcurrentAcquireDoesNotExceedLimit) {
  constexpr uint32_t kMaxSlots = 10;
  constexpr int kThreads = 32;
  constexpr int kIterations = 1000;

  std::atomic<uint32_t> current{0};
  std::atomic<uint32_t> max_observed{0};
  std::atomic<uint64_t> acquired_count{0};

  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < kIterations; ++j) {
        if (TryAcquireSlot(current, kMaxSlots)) {
          // Record peak concurrency
          uint32_t after = current.load(std::memory_order_relaxed);
          uint32_t expected = max_observed.load(std::memory_order_relaxed);
          while (after > expected &&
                 !max_observed.compare_exchange_weak(
                     expected, after,
                     std::memory_order_relaxed,
                     std::memory_order_relaxed)) {
            // retry
          }
          acquired_count.fetch_add(1, std::memory_order_relaxed);
          // Release slot
          current.fetch_sub(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_LE(max_observed.load(), kMaxSlots)
      << "Peak concurrent slots exceeded limit";
  EXPECT_EQ(current.load(), 0u)
      << "All slots should be released after test";
  EXPECT_GT(acquired_count.load(), 0u)
      << "At least some acquisitions should succeed";
}

TEST(RateLimiterRaceTest, RejectsAllWhenAtLimit) {
  constexpr uint32_t kMaxSlots = 3;
  std::atomic<uint32_t> current{3};  // Already at limit

  EXPECT_FALSE(TryAcquireSlot(current, kMaxSlots));
  EXPECT_EQ(current.load(), 3u);
}

TEST(RateLimiterRaceTest, AllowsAcquisitionWhenBelowLimit) {
  constexpr uint32_t kMaxSlots = 5;
  std::atomic<uint32_t> current{2};

  EXPECT_TRUE(TryAcquireSlot(current, kMaxSlots));
  EXPECT_EQ(current.load(), 3u);
}
