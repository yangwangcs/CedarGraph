#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <thread>
#include <vector>

#include "cedar/gcn/tmv_engine.h"
#include "cedar/gcn/watermark_gc.h"

using namespace cedar::gcn;

TEST(WatermarkGcTest, WatermarkAdvancesAndDrops) {
  TMVEngine engine(16);
  WatermarkGc gc(&engine);

  // Bootstrap a vertex with old edges
  engine.BootstrapVertex(42, Direction::kOut,
                         {{100, 0, 100, 0, 1, 0}}, false);
  engine.BootstrapVertex(42, Direction::kOut,
                         {{200, 0, 200, 0, 1, 0}}, false);
  engine.BootstrapVertex(
      42, Direction::kOut,
      {{300, 200, std::numeric_limits<uint32_t>::max(), 0, 1, 0}}, false);

  gc.Start(100);  // 100ms interval for fast test

  // Advance watermark to 250 — chunks with valid_to <= 250 should be dropped
  gc.UpdateWatermark(250);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  gc.Stop();

  // After GC, only chunk 3 remains (valid_from=200, valid_to=MAX)
  auto at_200 = engine.ScanAtTime(42, Direction::kOut, 200);
  EXPECT_EQ(at_200.size(), 1u);
  EXPECT_EQ(at_200[0].target_id, 300u);

  auto at_100 = engine.ScanAtTime(42, Direction::kOut, 100);
  EXPECT_EQ(at_100.size(), 0u);
}

TEST(WatermarkGcTest, GcDropsOldChunks) {
  TMVEngine engine(16);

  // Create 3 chunks for vertex 42 "at time" 100, 200, 300.
  // Chunk 1: valid_to=100  (will be dropped by watermark 250)
  engine.BootstrapVertex(42, Direction::kOut,
                         {{100, 0, 100, 0, 1, 0}}, false);
  // Chunk 2: valid_to=200  (will be dropped by watermark 250)
  engine.BootstrapVertex(42, Direction::kOut,
                         {{200, 0, 200, 0, 1, 0}}, false);
  // Chunk 3: valid_from=200, valid_to=MAX  (survives watermark 250)
  engine.BootstrapVertex(
      42, Direction::kOut,
      {{300, 200, std::numeric_limits<uint32_t>::max(), 0, 1, 0}}, false);

  WatermarkGc gc(&engine);
  gc.UpdateWatermark(250);
  gc.Start(10);  // 10 ms interval

  // Wait long enough for at least one GC cycle.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  gc.Stop();

  // After GC, only chunk 3 remains.  Its edge is visible at t=200
  // (valid_from=200 <= 200) but invisible at t=100 (min_valid_from=200 > 100).
  auto at_200 = engine.ScanAtTime(42, Direction::kOut, 200);
  EXPECT_EQ(at_200.size(), 1u);
  EXPECT_EQ(at_200[0].target_id, 300u);

  auto at_100 = engine.ScanAtTime(42, Direction::kOut, 100);
  EXPECT_EQ(at_100.size(), 0u);
}
