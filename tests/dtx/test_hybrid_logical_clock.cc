#include <gtest/gtest.h>
#include <chrono>
#include "cedar/dtx/hybrid_logical_clock.h"

TEST(HybridLogicalClockTest, ClockSkewDoesNotBlockForever) {
  cedar::dtx::HybridLogicalClock hlc;
  cedar::dtx::HlcTimestamp future(9999999999999ULL, 65535);
  hlc.Update(future);

  auto start = std::chrono::steady_clock::now();
  auto ts = hlc.Now();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(elapsed, std::chrono::milliseconds(500))
      << "HLC::Now blocked for too long after clock skew";
  EXPECT_GE(ts.physical, future.physical);
}

TEST(HybridLogicalClockTest, LogicalOverflowForcesMonotonicAdvancePromptly) {
  cedar::dtx::HybridLogicalClock hlc;
  cedar::dtx::HlcTimestamp future(9999999999999ULL, 65535);
  hlc.Update(future);

  auto start = std::chrono::steady_clock::now();
  auto ts = hlc.Now();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(elapsed, std::chrono::milliseconds(10));
  EXPECT_GT(ts.physical, future.physical);
  EXPECT_EQ(ts.logical, 0u);
}
