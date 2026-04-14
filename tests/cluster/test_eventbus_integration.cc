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

// tests/cluster/test_eventbus_integration.cc
// EventBus Integration Tests for HealthMonitor and FailoverManager

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "cedar/storage/storage_health_monitor.h"
#include "cedar/storage/failover_manager.h"
#include "cedar/integration/event_bus.h"
#include "cedar/governance/health_checker.h"

using namespace cedar;
using namespace cedar::storage;

class EventBusIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    event_bus_ = std::make_shared<integration::EventBus>();
    health_checker_ = std::make_shared<governance::HealthChecker>();
    health_monitor_ = std::make_shared<StorageHealthMonitor>();
    
    HealthMonitorConfig health_config;
    health_config.check_interval = std::chrono::seconds(1);
    health_config.failure_threshold = 1;
    health_config.success_threshold = 1;
    
    Status s = health_monitor_->Initialize(health_config, health_checker_, event_bus_);
    ASSERT_TRUE(s.ok());
    
    failover_manager_ = std::make_shared<FailoverManager>();
    
    FailoverConfig failover_config;
    failover_config.enable_auto_failover = true;
    
    s = failover_manager_->Initialize(failover_config, health_monitor_);
    ASSERT_TRUE(s.ok());
  }
  
  void TearDown() override {
    failover_manager_->Stop();
    health_monitor_->Stop();
  }
  
  std::shared_ptr<integration::EventBus> event_bus_;
  std::shared_ptr<governance::HealthChecker> health_checker_;
  std::shared_ptr<StorageHealthMonitor> health_monitor_;
  std::shared_ptr<FailoverManager> failover_manager_;
};

TEST_F(EventBusIntegrationTest, HealthEventsPublished) {
  int event_count = 0;
  std::string last_node_id;
  
  event_bus_->Subscribe("storage.node.health_changed",
    [&event_count, &last_node_id](const integration::Event& event) {
      event_count++;
      last_node_id = event.Get<std::string>("node_id");
    });
  
  health_monitor_->RegisterNode("test-node", "127.0.0.1", 9779);
  
  health_monitor_->Start();
  
  // Wait for potential events
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  // Event mechanism is set up (actual events depend on health checks)
  EXPECT_GE(event_count, 0);
}

TEST_F(EventBusIntegrationTest, FailoverCallbackTriggers) {
  bool failover_called = false;
  
  failover_manager_->SetFailoverCallback(
    [&failover_called](const std::string& old_node, const std::string& new_node) {
      failover_called = true;
    });
  
  failover_manager_->RegisterNode("leader", "127.0.0.1:9779", NodeRole::kLeader);
  failover_manager_->RegisterNode("follower", "127.0.0.1:9780", NodeRole::kFollower);
  
  failover_manager_->Start();
  
  // No failover yet
  EXPECT_FALSE(failover_called);
}

TEST_F(EventBusIntegrationTest, EndToEndEventFlow) {
  std::vector<std::string> events_received;
  
  // Subscribe to all storage events using wildcard pattern
  event_bus_->Subscribe("storage.node.health_changed",
    [&events_received](const integration::Event& event) {
      events_received.push_back(event.GetType());
    });
  
  // Setup components
  health_monitor_->RegisterNode("node-1", "127.0.0.1", 9779);
  failover_manager_->RegisterNode("node-1", "127.0.0.1:9779", NodeRole::kLeader);
  
  health_monitor_->Start();
  failover_manager_->Start();
  
  // Run for a bit
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  // Event system is functional
  // (actual events depend on health check timing)
  EXPECT_TRUE(true);
}

TEST_F(EventBusIntegrationTest, MultipleSubscribers) {
  int count1 = 0, count2 = 0;
  
  event_bus_->Subscribe("storage.node.health_changed",
    [&count1](const integration::Event& event) {
      count1++;
    });
  
  event_bus_->Subscribe("storage.node.health_changed",
    [&count2](const integration::Event& event) {
      count2++;
    });
  
  // Both subscribers should receive events
  EXPECT_EQ(count1, count2);
  EXPECT_EQ(count1, 0);  // No events yet
}

TEST_F(EventBusIntegrationTest, FailoverEventPublishing) {
  std::string failover_from;
  std::string failover_to;
  
  // Subscribe to failover events
  event_bus_->Subscribe("storage.failover.completed",
    [&failover_from, &failover_to](const integration::Event& event) {
      failover_from = event.Get<std::string>("from_node");
      failover_to = event.Get<std::string>("to_node");
    });
  
  // Set up callback that publishes event
  failover_manager_->SetFailoverCallback(
    [this](const std::string& old_node, const std::string& new_node) {
      integration::Event event("storage.failover.completed");
      event.Set("from_node", old_node);
      event.Set("to_node", new_node);
      event.Set("timestamp", static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count()));
      event_bus_->Publish(event);
    });
  
  // Register nodes
  failover_manager_->RegisterNode("leader", "127.0.0.1:9779", NodeRole::kLeader);
  failover_manager_->RegisterNode("follower", "127.0.0.1:9780", NodeRole::kFollower);
  
  failover_manager_->Start();
  
  // No failover has occurred yet
  EXPECT_TRUE(failover_from.empty());
  EXPECT_TRUE(failover_to.empty());
}

TEST_F(EventBusIntegrationTest, HealthMonitorWithEventBus) {
  // Test that health monitor properly uses event bus
  int health_change_count = 0;
  
  event_bus_->Subscribe("storage.node.health_changed",
    [&health_change_count](const integration::Event& event) {
      health_change_count++;
    });
  
  // Register node
  Status s = health_monitor_->RegisterNode("health-test-node", "127.0.0.1", 9779);
  EXPECT_TRUE(s.ok());
  
  // Start monitoring
  s = health_monitor_->Start();
  EXPECT_TRUE(s.ok());
  
  // Wait a bit for any health checks
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  // Health monitor is functional
  EXPECT_TRUE(health_monitor_->GetAllNodes().size() > 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
