// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <set>
#include <string>
#include <thread>

#ifdef __APPLE__
#include <libproc.h>
#include <sys/proc_info.h>
#include <unistd.h>
#endif

#include "cedar/dtx/failover_manager.h"

using namespace cedar::dtx;

namespace {

int GetCurrentThreadCount() {
#ifdef __linux__
  std::ifstream status("/proc/self/status");
  std::string line;
  while (std::getline(status, line)) {
    if (line.find("Threads:") == 0) {
      return std::stoi(line.substr(line.find_first_of("0123456789")));
    }
  }
  return -1;
#elif defined(__APPLE__)
  struct proc_taskinfo pti;
  if (proc_pidinfo(getpid(), PROC_PIDTASKINFO, 0, &pti, sizeof(pti)) > 0) {
    return static_cast<int>(pti.pti_threadnum);
  }
  return -1;
#else
  return -1;
#endif
}

}  // namespace

TEST(FailoverThreadBound, ReportNodeFailureDoesNotSpawnUnboundedThreads) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::milliseconds(20);
  config.check_interval = std::chrono::milliseconds(100);
  config.detection_config.timeout = std::chrono::milliseconds(200);

  ASSERT_TRUE(controller.Initialize(config).ok());

  int baseline_threads = GetCurrentThreadCount();
  ASSERT_GT(baseline_threads, 0);

  // Register 100 partitions with node 1 as leader and no replicas.
  // In the old implementation, ReportNodeFailure(1) would spawn 100
  // detached threads — one per partition.
  for (int i = 1; i <= 100; ++i) {
    controller.RegisterPartition(i, 1, {});
  }

  ASSERT_TRUE(controller.ReportNodeFailure(1).ok());

  // Allow the worker pool to pick up tasks.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int current_threads = GetCurrentThreadCount();

  // The bounded pool has 16 worker threads.  With the two monitor threads
  // (lease + health) the total increase should be well under 100.
  EXPECT_LE(current_threads, baseline_threads + 25);

  controller.Shutdown();
}

TEST(FailoverThreadBound, HealthCheckLoopUsesBoundedPool) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::milliseconds(20);
  config.check_interval = std::chrono::milliseconds(50);
  config.detection_config.timeout = std::chrono::milliseconds(100);

  ASSERT_TRUE(controller.Initialize(config).ok());

  int baseline_threads = GetCurrentThreadCount();
  ASSERT_GT(baseline_threads, 0);

  // Register many partitions with the same leader and no replicas.
  for (int i = 1; i <= 50; ++i) {
    controller.RegisterPartition(i, 1, {});
  }

  // Let the health check loop run for several iterations.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  int current_threads = GetCurrentThreadCount();

  // Should not have spawned a thread per partition / per check iteration.
  EXPECT_LE(current_threads, baseline_threads + 25);

  // Shutdown must complete cleanly (no UAF from detached threads).
  controller.Shutdown();
}

TEST(FailoverThreadBound, RapidLeaderFailureReportsAreBounded) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::milliseconds(20);
  config.check_interval = std::chrono::milliseconds(100);

  ASSERT_TRUE(controller.Initialize(config).ok());

  int baseline_threads = GetCurrentThreadCount();
  ASSERT_GT(baseline_threads, 0);

  for (int i = 1; i <= 50; ++i) {
    controller.RegisterPartition(i, 1, {});
  }

  // Rapidly report leader failures.  After the first report for a
  // partition, subsequent reports are no-ops because is_failover_in_progress
  // is true, but the first burst still schedules many tasks.
  for (int round = 0; round < 10; ++round) {
    for (int i = 1; i <= 50; ++i) {
      controller.ReportLeaderFailure(i);
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int current_threads = GetCurrentThreadCount();
  EXPECT_LE(current_threads, baseline_threads + 25);

  controller.Shutdown();
}
