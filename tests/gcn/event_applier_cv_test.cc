#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "cedar/gcn/event_applier.h"

using namespace cedar::gcn;

TEST(EventApplierCvTest, WakesInstantlyOnBufferDrain) {
  EventApplier applier(nullptr, 5);

  // Fill buffer with out-of-order events (versions 2-6).
  // applied_version_ starts at 0, so version 1 is missing.
  for (uint64_t v = 2; v <= 6; ++v) {
    GraphCDCEvent e{v, 100, 200, 1000, 0, 1, CDCEventOp::kCreate};
    EXPECT_TRUE(applier.ApplyUnordered(e).ok());
  }

  std::atomic<bool> done{false};
  auto start = std::chrono::steady_clock::now();
  std::thread t([&]() {
    GraphCDCEvent e{7, 100, 200, 1000, 0, 1, CDCEventOp::kCreate};
    auto s = applier.ApplyUnordered(e);
    EXPECT_TRUE(s.ok());
    done = true;
  });

  // Give the thread time to start and hit the full buffer.
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(done.load())
      << "Thread should have blocked on full buffer";

  // Apply the missing event. This should drain the buffer and
  // wake the blocked thread via condition_variable, not polling.
  GraphCDCEvent e1{1, 100, 200, 1000, 0, 1, CDCEventOp::kCreate};
  EXPECT_TRUE(applier.ApplyUnordered(e1).ok());

  t.join();
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(done.load());
  EXPECT_LT(elapsed, std::chrono::milliseconds(200))
      << "Thread should wake instantly via CV, not poll for 10ms+";
}
