// Copyright 2025 The Cedar Authors
//
// Regression tests for StorageClientPool lifecycle behavior.

#include <gtest/gtest.h>

#include <chrono>

#include "cedar/dtx/storage_service_impl.h"

using namespace cedar::dtx;

TEST(StorageClientPoolTest, ShutdownWakesIdleCleanupThreadPromptly) {
  StorageClientPool pool;
  StorageClientPool::PoolConfig config;
  config.idle_timeout = std::chrono::seconds(30);

  ASSERT_TRUE(pool.Initialize(config).ok());

  auto start = std::chrono::steady_clock::now();
  pool.Shutdown();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}
