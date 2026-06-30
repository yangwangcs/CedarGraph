// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>

#include <chrono>

#include "cedar/dtx/storage/metrics_collector.h"

using cedar::dtx::storage::MetricsCollector;

TEST(StorageMetricsCollectorTest, ShutdownWakesCollectionThreadPromptly) {
  MetricsCollector collector;
  MetricsCollector::Config config;
  config.collection_interval = std::chrono::seconds(60);
  config.enable_http_server = false;

  ASSERT_TRUE(collector.Initialize(config).ok());
  auto start = std::chrono::steady_clock::now();
  collector.Shutdown();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

TEST(StorageMetricsCollectorTest, ShutdownWakesHttpServerPromptly) {
  MetricsCollector collector;
  MetricsCollector::Config config;
  config.collection_interval = std::chrono::seconds(60);
  config.endpoint = ":19091";
  config.enable_http_server = true;

  ASSERT_TRUE(collector.Initialize(config).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto start = std::chrono::steady_clock::now();
  collector.Shutdown();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}
