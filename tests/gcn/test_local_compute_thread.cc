#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <vector>

#include "cedar/gcn/local_compute_thread.h"
#include "cedar/gcn/tmv_engine.h"

using namespace cedar::gcn;

TEST(LocalComputeThreadTest, BFSOneHop) {
  TMVEngine engine(16);

  std::vector<TMVEdge> edges;
  edges.push_back(
      {100, 0, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  edges.push_back(
      {200, 0, std::numeric_limits<uint32_t>::max(), 0, 1, 0});

  engine.BootstrapVertex(42, Direction::kOut, edges, true);

  LocalComputeThread compute;
  auto results = compute.ExecuteBFS(42, 100, 1, &engine);

  EXPECT_EQ(results.size(), 2u);
  EXPECT_TRUE(std::find(results.begin(), results.end(), 100) != results.end());
  EXPECT_TRUE(std::find(results.begin(), results.end(), 200) != results.end());
}

TEST(LocalComputeThreadTest, DFSOneHop) {
  TMVEngine engine(16);

  std::vector<TMVEdge> edges;
  edges.push_back(
      {100, 0, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  edges.push_back(
      {200, 0, std::numeric_limits<uint32_t>::max(), 0, 1, 0});

  engine.BootstrapVertex(42, Direction::kOut, edges, true);

  LocalComputeThread compute;
  auto results = compute.ExecuteDFS(42, 100, 1, &engine);

  EXPECT_EQ(results.size(), 2u);
  EXPECT_TRUE(std::find(results.begin(), results.end(), 100) != results.end());
  EXPECT_TRUE(std::find(results.begin(), results.end(), 200) != results.end());
}
