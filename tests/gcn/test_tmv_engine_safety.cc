#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "cedar/gcn/tmv_engine.h"

using namespace cedar::gcn;

TEST(TMVEngineSafetyTest, BootstrapRollbackOnAllocFailure) {
  // Engine with only 1 chunk
  TMVEngine engine(1);

  // Need more than 1 chunk worth of edges
  std::vector<TMVEdge> edges;
  edges.reserve(TMVChunk::kCapacity + 1);
  for (size_t i = 0; i < TMVChunk::kCapacity + 1; ++i) {
    edges.push_back({100 + i, static_cast<uint32_t>(i),
                     std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  }

  cedar::Status s = engine.BootstrapVertex(1, Direction::kOut, edges, false);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsResourceExhausted());

  // No chunks should be leaked
  EXPECT_EQ(engine.ChunkCount(), 0u);
}

TEST(TMVEngineSafetyTest, ScanDuringConcurrentInvalidate) {
  TMVEngine engine(64);

  // Bootstrap a vertex
  std::vector<TMVEdge> edges;
  for (uint32_t i = 0; i < 100; ++i) {
    edges.push_back({1000 + i, i * 10,
                     std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  }
  engine.BootstrapVertex(1, Direction::kOut, edges, false);

  std::atomic<bool> stop{false};
  std::atomic<int> scan_count{0};
  std::atomic<int> invalidate_count{0};

  std::thread scanner([&]() {
    while (!stop.load()) {
      auto result = engine.ScanAtTime(1, Direction::kOut, 5000);
      scan_count.fetch_add(1);
      (void)result;
    }
  });

  std::thread invalidator([&]() {
    while (!stop.load()) {
      engine.InvalidateVertex(1);
      invalidate_count.fetch_add(1);
      // Re-bootstrap for next iteration
      engine.BootstrapVertex(1, Direction::kOut, edges, false);
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  stop.store(true);

  scanner.join();
  invalidator.join();

  EXPECT_GT(scan_count.load(), 0);
  EXPECT_GT(invalidate_count.load(), 0);
}

TEST(TMVEngineSafetyTest, AppendEdgeReverseFailureNoLeak) {
  // Engine with only 2 chunks total
  TMVEngine engine(2);

  // Bootstrap vertex 1 with exactly 1 chunk of outgoing edges
  std::vector<TMVEdge> edges;
  edges.reserve(TMVChunk::kCapacity);
  for (size_t i = 0; i < TMVChunk::kCapacity; ++i) {
    edges.push_back({200 + i, static_cast<uint32_t>(i),
                     std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  }
  engine.BootstrapVertex(1, Direction::kOut, edges, false);
  EXPECT_EQ(engine.ChunkCount(), 1u);

  // Try to append an edge from vertex 2 to vertex 1 with reverse=true.
  // Forward for vertex 2 needs a new chunk (uses the 1 free chunk).
  // Reverse for vertex 1 also needs a new chunk (its tail is full),
  // but pool is empty -> reverse fails. Forward should be rolled back.
  size_t chunks_before = engine.ChunkCount();
  TMVEdge edge{1, 100, std::numeric_limits<uint32_t>::max(), 0, 1, 0};
  cedar::Status s = engine.AppendEdge(2, Direction::kOut, edge, true);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsResourceExhausted());
  EXPECT_EQ(engine.ChunkCount(), chunks_before);
}
