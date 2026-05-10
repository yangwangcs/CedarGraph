// Copyright 2025 The Cedar Authors
//
// Test: Failover controller lock order — no deadlock between IsLeaseExpired
// and RenewLeaderLease under concurrent access.

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include "cedar/dtx/failover_manager.h"

using namespace cedar::dtx;

TEST(PartitionFailoverController, NoDeadlockBetweenLeaseAndRenew) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.lease_renew_interval = std::chrono::milliseconds(10);
  config.health_check_interval = std::chrono::milliseconds(10);
  config.health_check_timeout = std::chrono::milliseconds(100);
  config.leader_lease_duration = std::chrono::milliseconds(200);
  config.max_consecutive_failures = 3;
  controller.Initialize(config);

  controller.RegisterPartition(1, 100, {100, 101});
  controller.UpdateNodeHeartbeat(100);

  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;

  // Thread A: repeatedly check lease expiration
  threads.emplace_back([&]() {
    while (!stop.load()) {
      controller.IsLeaseExpired(1);
    }
  });

  // Thread B: repeatedly renew lease
  threads.emplace_back([&]() {
    while (!stop.load()) {
      controller.RenewLeaderLease(1);
    }
  });

  // Thread C: report node failure (triggers failover path)
  threads.emplace_back([&]() {
    while (!stop.load()) {
      controller.ReportNodeFailure(100);
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  stop.store(true);
  for (auto& t : threads) t.join();

  SUCCEED();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
