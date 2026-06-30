// Copyright 2025 The Cedar Authors
//
// Regression tests for AdaptiveThreadPool lifecycle behavior.

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <future>

#include "cedar/storage/adaptive_thread_pool.h"

using namespace cedar;

TEST(AdaptiveThreadPoolLifecycleTest, ShutdownWakesAdjusterThreadPromptly) {
  AdaptiveConfig config;
  config.min_threads = 1;
  config.max_threads = 2;
  config.initial_threads = 1;
  config.enable_adaptive = true;
  config.adjust_interval_ms = 30000;

  AdaptiveThreadPool<std::function<void()>> pool(config);
  pool.Start();

  auto start = std::chrono::steady_clock::now();
  pool.Shutdown();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}

TEST(AdaptiveThreadPoolLifecycleTest, ScaleDownWakesIdleWorkerBeforeJoin) {
  AdaptiveConfig config;
  config.min_threads = 1;
  config.max_threads = 2;
  config.initial_threads = 2;
  config.scale_down_idle_seconds = 0;
  config.enable_adaptive = true;
  config.adjust_interval_ms = 10;

  AdaptiveThreadPool<std::function<void()>> pool(config);
  pool.Start();

  auto start = std::chrono::steady_clock::now();
  pool.Shutdown();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}

TEST(AdaptiveThreadPoolLifecycleTest, ZeroInitialThreadsStillExecutesTasks) {
  AdaptiveConfig config;
  config.min_threads = 0;
  config.max_threads = 0;
  config.initial_threads = 0;
  config.enable_adaptive = false;

  AdaptiveThreadPool<std::function<int()>> pool(config);
  pool.Start();

  auto future = pool.Submit([] { return 42; });
  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_EQ(future.get(), 42);

  pool.Shutdown();
}
