#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <thread>

#include "cedar/gcn/event_applier.h"

using namespace cedar::gcn;

TEST(EventApplierBackpressureTest, BlocksOnFullBuffer) {
  EventApplier applier(nullptr, 10);

  // Fill buffer with out-of-order events (versions 2-11).
  // applied_version_ starts at 0, so version 1 is missing.
  for (uint64_t v = 2; v <= 11; ++v) {
    GraphCDCEvent e{v, 100, 200, 1000, 0, 1, CDCEventOp::kCreate};
    EXPECT_TRUE(applier.ApplyUnordered(e).ok());
  }

  std::atomic<bool> done{false};
  std::thread t([&]() {
    GraphCDCEvent e{12, 100, 200, 1000, 0, 1, CDCEventOp::kCreate};
    auto s = applier.ApplyUnordered(e);
    EXPECT_TRUE(s.ok());
    done = true;
  });

  // Give the thread time to start and hit the full buffer.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(done.load())
      << "Thread should have blocked on full buffer, not returned immediately";

  // Apply the missing event to drain the buffer and unblock the thread.
  GraphCDCEvent e1{1, 100, 200, 1000, 0, 1, CDCEventOp::kCreate};
  EXPECT_TRUE(applier.ApplyUnordered(e1).ok());

  t.join();
  EXPECT_TRUE(done.load());
}
