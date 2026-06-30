// Copyright 2025 The Cedar Authors
//
// Regression tests for StorageClient retry lifecycle behavior.

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#define private public
#include "cedar/dtx/storage_service_impl.h"
#undef private

using namespace cedar;
using namespace cedar::dtx;

TEST(StorageClientRetryTest, StatusRetryStopsPromptlyWhenClientShutsDown) {
  StorageClient client;
  StorageClient::ClientConfig config;
  config.max_retries = 3;
  config.retry_base_delay = std::chrono::seconds(30);
  config.operation_timeout = std::chrono::seconds(120);
  client.config_ = config;

  size_t attempts = 0;
  std::thread shutdown_thread([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    client.Shutdown();
  });

  auto start = std::chrono::steady_clock::now();
  auto status = client.RetryWithBackoff([&]() {
    attempts++;
    return Status::IOError("transient UNAVAILABLE");
  });
  auto elapsed = std::chrono::steady_clock::now() - start;

  shutdown_thread.join();

  EXPECT_FALSE(status.ok());
  EXPECT_EQ(attempts, 1u);
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}

TEST(StorageClientRetryTest, StatusOrRetryStopsPromptlyWhenClientShutsDown) {
  StorageClient client;
  StorageClient::ClientConfig config;
  config.max_retries = 3;
  config.retry_base_delay = std::chrono::seconds(30);
  config.operation_timeout = std::chrono::seconds(120);
  client.config_ = config;

  size_t attempts = 0;
  auto start = std::chrono::steady_clock::now();
  auto result = client.RetryWithBackoff<int>([&]() -> StatusOr<int> {
    attempts++;
    client.Shutdown();
    return Status::IOError("transient UNAVAILABLE");
  });
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(attempts, 1u);
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}
