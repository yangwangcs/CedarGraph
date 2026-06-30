// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "cedar/queryd/meta_client.h"

namespace cedar {
namespace queryd {
namespace {

class TestQueryMetaClient : public QueryMetaClient {
 public:
  explicit TestQueryMetaClient(const Options& options) : QueryMetaClient(options) {}

  void StartRefreshLoop() { StartRefreshLoopForTesting(); }
  void SetClusterState(const ClusterState& state) {
    SetCachedClusterStateForTesting(state);
  }
};

TEST(QueryMetaClientShutdownTest, DestructorWakesRefreshThreadPromptly) {
  QueryMetaClient::Options options;
  options.refresh_interval = std::chrono::seconds(60);

  auto start = std::chrono::steady_clock::now();
  {
    TestQueryMetaClient client(options);
    client.StartRefreshLoop();
  }
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

TEST(QueryMetaClientShutdownTest, StopWakesWatchLoopPromptly) {
  QueryMetaClient::Options options;
  TestQueryMetaClient client(options);

  ClusterState state;
  state.version = 1;
  client.SetClusterState(state);
  client.StartRefreshLoop();

  std::atomic<int> callbacks{0};
  auto watch_future = std::async(std::launch::async, [&] {
    return client.WatchClusterChanges([&](const ClusterState&) {
      callbacks.fetch_add(1, std::memory_order_relaxed);
    });
  });

  for (int i = 0; i < 100 && callbacks.load(std::memory_order_relaxed) == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_GT(callbacks.load(std::memory_order_relaxed), 0);

  auto start = std::chrono::steady_clock::now();
  client.Stop();
  ASSERT_EQ(watch_future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(watch_future.get().ok());
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

}  // namespace
}  // namespace queryd
}  // namespace cedar
