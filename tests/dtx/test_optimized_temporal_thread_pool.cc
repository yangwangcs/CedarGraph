// Copyright 2025 The Cedar Authors
//
// Regression tests for optimized temporal benchmark helpers.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <vector>

#include "cedar/dtx/optimized_temporal_benchmark.h"

using cedar::dtx::ThreadPool;
using cedar::dtx::BatchBuffer;
using cedar::dtx::OptimizedTemporalBenchmark;
using cedar::dtx::OptimizedTemporalConfig;

TEST(OptimizedTemporalThreadPoolTest, ZeroThreadsStillExecutesScheduledTasks) {
  ThreadPool pool(0);
  std::atomic<int> counter{0};

  std::function<void()> task = [&counter]() { counter.fetch_add(1); };
  auto task_future = pool.Submit(std::move(task));

  auto waiter = std::async(std::launch::async, [&pool]() { pool.WaitForAll(); });
  ASSERT_EQ(waiter.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  waiter.get();
  task_future.get();

  EXPECT_EQ(counter.load(), 1);
}

TEST(OptimizedTemporalBatchBufferTest, ZeroBatchSizeFlushesSingleItem) {
  auto future = std::async(std::launch::async, [] {
    std::atomic<int> flushed_count{0};
    BatchBuffer<int> buffer(
        0, std::chrono::seconds(30),
        [&flushed_count](std::vector<int>& batch) {
          flushed_count.fetch_add(static_cast<int>(batch.size()));
        });
    buffer.Add(7);
    return flushed_count.load();
  });

  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_EQ(future.get(), 1);
}

TEST(OptimizedTemporalBenchmarkTest, RejectsInvalidInitializationConfig) {
  OptimizedTemporalConfig config;
  config.node_count = 1;
  config.vertex_count = 0;

  OptimizedTemporalBenchmark benchmark(config);
  auto status = benchmark.Initialize();

  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(OptimizedTemporalBenchmarkTest, RejectsAllZeroQueryRatios) {
  OptimizedTemporalConfig config;
  config.node_count = 1;
  config.vertex_count = 1;
  config.edge_count = 0;
  config.point_query_ratio = 0;
  config.range_query_ratio = 0;
  config.graph_analytics_ratio = 0;
  config.write_ratio = 0;

  OptimizedTemporalBenchmark benchmark(config);
  auto status = benchmark.Initialize();

  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(OptimizedTemporalBenchmarkTest, RejectsZeroParallelScanThreads) {
  OptimizedTemporalConfig config;
  config.node_count = 1;
  config.vertex_count = 1;
  config.edge_count = 0;
  config.parallel_scan_threads = 0;

  OptimizedTemporalBenchmark benchmark(config);
  auto status = benchmark.Initialize();

  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(OptimizedTemporalBenchmarkTest, RejectsShortTimeRangeWhenRangeQueriesEnabled) {
  OptimizedTemporalConfig config;
  config.node_count = 1;
  config.vertex_count = 1;
  config.edge_count = 0;
  config.time_range_seconds = 10;
  config.point_query_ratio = 0;
  config.range_query_ratio = 100;
  config.graph_analytics_ratio = 0;
  config.write_ratio = 0;

  OptimizedTemporalBenchmark benchmark(config);
  auto status = benchmark.Initialize();

  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}
