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
#include <thread>
#include <vector>

#include "cedar/storage/failover_manager.h"
#include "cedar/storage/storage_health_monitor.h"
#include "cedar/governance/health_checker.h"

using namespace cedar;
using namespace cedar::storage;
using namespace cedar::governance;

class FailoverRaceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    health_checker_ = std::make_shared<HealthChecker>();
    health_monitor_ = std::make_shared<StorageHealthMonitor>();

    HealthMonitorConfig health_config;
    health_config.check_interval = std::chrono::seconds(1);
    health_config.failure_threshold = 1;
    health_config.success_threshold = 1;

    Status s = health_monitor_->Initialize(health_config, health_checker_);
    ASSERT_TRUE(s.ok());

    failover_manager_ = std::make_unique<FailoverManager>();

    FailoverConfig failover_config;
    failover_config.enable_auto_failover = true;
    failover_config.enable_read_from_follower = true;

    s = failover_manager_->Initialize(failover_config, health_monitor_);
    ASSERT_TRUE(s.ok());
  }

  void TearDown() override {
    if (failover_manager_) {
      failover_manager_->Stop();
    }
    if (health_monitor_) {
      health_monitor_->Stop();
    }
  }

  void RegisterNodes() {
    Status s = health_monitor_->RegisterNode("n1", "127.0.0.1", 9779);
    ASSERT_TRUE(s.ok());
    s = health_monitor_->RegisterNode("n2", "127.0.0.1", 9780);
    ASSERT_TRUE(s.ok());
    s = health_monitor_->RegisterNode("n3", "127.0.0.1", 9781);
    ASSERT_TRUE(s.ok());

    s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
    ASSERT_TRUE(s.ok());
    s = failover_manager_->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
    ASSERT_TRUE(s.ok());
    s = failover_manager_->RegisterNode("n3", "127.0.0.1:9781", NodeRole::kFollower);
    ASSERT_TRUE(s.ok());
  }

  void MakeNodesHealthy() {
    // Force a health check so nodes become kHealthy with success_threshold=1
    auto s = health_monitor_->CheckNodeHealth("n1");
    (void)s;
    s = health_monitor_->CheckNodeHealth("n2");
    (void)s;
    s = health_monitor_->CheckNodeHealth("n3");
    (void)s;
  }

  void RegisterHealthyNodes() {
    // Register with health monitor first, make healthy, then register with failover manager
    Status s = health_monitor_->RegisterNode("n1", "127.0.0.1", 9779);
    ASSERT_TRUE(s.ok());
    s = health_monitor_->RegisterNode("n2", "127.0.0.1", 9780);
    ASSERT_TRUE(s.ok());
    s = health_monitor_->RegisterNode("n3", "127.0.0.1", 9781);
    ASSERT_TRUE(s.ok());

    MakeNodesHealthy();

    s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
    ASSERT_TRUE(s.ok());
    s = failover_manager_->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
    ASSERT_TRUE(s.ok());
    s = failover_manager_->RegisterNode("n3", "127.0.0.1:9781", NodeRole::kFollower);
    ASSERT_TRUE(s.ok());
  }

  std::shared_ptr<HealthChecker> health_checker_;
  std::shared_ptr<StorageHealthMonitor> health_monitor_;
  std::unique_ptr<FailoverManager> failover_manager_;
};

TEST_F(FailoverRaceTest, StartAndGetNodeForReadRace) {
  RegisterNodes();

  std::atomic<bool> start_done{false};
  std::atomic<bool> error_occurred{false};

  std::thread start_thread([&]() {
    auto s = failover_manager_->Start();
    if (!s.ok()) {
      error_occurred.store(true);
    }
    start_done.store(true);
  });

  std::thread read_thread([&]() {
    while (!start_done.load()) {
      auto node = failover_manager_->GetNodeForRead();
      (void)node;  // Exercise the code path
    }
  });

  start_thread.join();
  read_thread.join();

  EXPECT_FALSE(error_occurred.load());
}

TEST_F(FailoverRaceTest, DeregisterAndGetNodeForReadRace) {
  RegisterNodes();

  auto s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());

  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};

  std::vector<std::thread> threads;

  // Thread that repeatedly deregisters and re-registers nodes,
  // triggering SelectNewLeader() via DeregisterNode.
  threads.emplace_back([&]() {
    for (int i = 0; i < 100; ++i) {
      auto s = failover_manager_->DeregisterNode("n1");
      if (s.ok() || s.IsNotFound()) {
        s = failover_manager_->RegisterNode(
            "n1", "127.0.0.1:9779", NodeRole::kLeader);
        if (!s.ok()) {
          errors.fetch_add(1);
        }
      }
    }
    stop.store(true);
  });

  // Thread that repeatedly calls GetNodeForRead
  threads.emplace_back([&]() {
    while (!stop.load()) {
      auto node = failover_manager_->GetNodeForRead();
      (void)node;
    }
  });

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(errors.load(), 0);
}

TEST_F(FailoverRaceTest, GetNodeForReadRespectsCanFailover) {
  RegisterHealthyNodes();

  auto s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());

  // With failover enabled and enable_read_from_follower=true,
  // GetNodeForRead may return any healthy node.
  bool got_follower = false;
  for (int i = 0; i < 10; ++i) {
    auto node = failover_manager_->GetNodeForRead();
    ASSERT_TRUE(node.ok());
    if (node.ValueOrDie().node_id != "n1") {
      got_follower = true;
      break;
    }
  }
  EXPECT_TRUE(got_follower);

  // Create a new manager with failover disabled
  auto fm2 = std::make_unique<FailoverManager>();
  FailoverConfig config;
  config.enable_auto_failover = false;  // CanFailover() returns false
  config.enable_read_from_follower = true;

  s = fm2->Initialize(config, health_monitor_);
  ASSERT_TRUE(s.ok());

  s = fm2->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  s = fm2->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  s = fm2->RegisterNode("n3", "127.0.0.1:9781", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());

  s = fm2->Start();
  ASSERT_TRUE(s.ok());

  // With failover disabled, GetNodeForRead should only return the leader
  for (int i = 0; i < 10; ++i) {
    auto node = fm2->GetNodeForRead();
    ASSERT_TRUE(node.ok());
    EXPECT_EQ(node.ValueOrDie().node_id, "n1");
    EXPECT_EQ(node.ValueOrDie().role, NodeRole::kLeader);
  }
}
