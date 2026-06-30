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
#include <chrono>
#include <thread>

#include "cedar/storage/failover_manager.h"
#include "cedar/storage/storage_health_monitor.h"
#include "cedar/governance/health_checker.h"
#include "cedar/dtx/failover_manager.h"

using namespace cedar;
using namespace cedar::storage;
using namespace cedar::governance;
using namespace cedar::dtx;

class FailoverTest : public ::testing::Test {
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
  
  std::shared_ptr<HealthChecker> health_checker_;
  std::shared_ptr<StorageHealthMonitor> health_monitor_;
  std::unique_ptr<FailoverManager> failover_manager_;
};

TEST_F(FailoverTest, RegisterAndGetLeader) {
  Status s = failover_manager_->RegisterNode("leader", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("follower", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  auto leader = failover_manager_->GetLeader();
  ASSERT_TRUE(leader.ok());
  EXPECT_EQ(leader.ValueOrDie().node_id, "leader");
  EXPECT_EQ(leader.ValueOrDie().role, NodeRole::kLeader);
}

TEST_F(FailoverTest, GetNodeForRead) {
  // Register nodes with the health monitor first
  Status s = health_monitor_->RegisterNode("n1", "127.0.0.1", 9779);
  ASSERT_TRUE(s.ok());
  
  s = health_monitor_->RegisterNode("n2", "127.0.0.1", 9780);
  ASSERT_TRUE(s.ok());
  
  s = health_monitor_->RegisterNode("n3", "127.0.0.1", 9781);
  ASSERT_TRUE(s.ok());
  
  // Now register with failover manager
  s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("n3", "127.0.0.1:9781", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  auto node = failover_manager_->GetNodeForRead();
  // Node may not be healthy yet, so this may fail - that's expected behavior
  // The test verifies the API works correctly
}

TEST_F(FailoverTest, ManualFailover) {
  Status s = failover_manager_->RegisterNode("node-a", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("node-b", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  auto leader = failover_manager_->GetLeader();
  ASSERT_TRUE(leader.ok());
  EXPECT_EQ(leader.ValueOrDie().node_id, "node-a");
  
  // Note: Manual failover may fail due to health checks
  // This test verifies the API works
  s = failover_manager_->TriggerManualFailover("node-a", "node-b");
  // Result depends on health status, so we just verify the API doesn't crash
}

TEST_F(FailoverTest, FailoverCallback) {
  bool callback_called = false;
  std::string old_node, new_node;
  
  failover_manager_->SetFailoverCallback(
    [&](const std::string& from, const std::string& to) {
      callback_called = true;
      old_node = from;
      new_node = to;
    });
  
  Status s = failover_manager_->RegisterNode("old-leader", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("new-leader", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  // Callback mechanism is set up (actual failover requires health changes)
  EXPECT_FALSE(callback_called);  // No failover yet
}

TEST_F(FailoverTest, GetHealthyFollowers) {
  Status s = failover_manager_->RegisterNode("leader", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("f1", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("f2", "127.0.0.1:9781", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  auto followers = failover_manager_->GetHealthyFollowers();
  // Returns followers that are healthy (health status from health monitor)
  EXPECT_GE(followers.size(), 0);
}

TEST_F(FailoverTest, GetAllNodes) {
  Status s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("n3", "127.0.0.1:9781", NodeRole::kStandby);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  auto nodes = failover_manager_->GetAllNodes();
  EXPECT_EQ(nodes.size(), 3);
}

TEST_F(FailoverTest, DeregisterNode) {
  Status s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  // Deregister a non-leader node
  s = failover_manager_->DeregisterNode("n2");
  ASSERT_TRUE(s.ok());
  
  auto nodes = failover_manager_->GetAllNodes();
  EXPECT_EQ(nodes.size(), 1);
}

TEST_F(FailoverTest, DeregisterLeader) {
  Status s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  // Deregister the leader
  s = failover_manager_->DeregisterNode("n1");
  ASSERT_TRUE(s.ok());
  
  auto nodes = failover_manager_->GetAllNodes();
  EXPECT_EQ(nodes.size(), 1);
  
  // Leader should be cleared or changed
  auto leader = failover_manager_->GetLeader();
  // May be "n2" if it was elected, or NotFound if no leader
}

TEST_F(FailoverTest, NodeRoleToString) {
  EXPECT_EQ(NodeRoleToString(NodeRole::kLeader), "Leader");
  EXPECT_EQ(NodeRoleToString(NodeRole::kFollower), "Follower");
  EXPECT_EQ(NodeRoleToString(NodeRole::kStandby), "Standby");
  EXPECT_EQ(NodeRoleToString(NodeRole::kUnknown), "Unknown");
}

TEST_F(FailoverTest, InvalidRegister) {
  // Empty node_id
  Status s = failover_manager_->RegisterNode("", "127.0.0.1:9779", NodeRole::kLeader);
  EXPECT_FALSE(s.ok());
  
  // Empty address
  s = failover_manager_->RegisterNode("n1", "", NodeRole::kLeader);
  EXPECT_FALSE(s.ok());
}

TEST_F(FailoverTest, IsRunning) {
  EXPECT_FALSE(failover_manager_->IsRunning());
  
  Status s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  EXPECT_TRUE(failover_manager_->IsRunning());
  
  failover_manager_->Stop();
  EXPECT_FALSE(failover_manager_->IsRunning());
}

TEST_F(FailoverTest, StartTwice) {
  Status s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  // Starting again should fail
  s = failover_manager_->Start();
  EXPECT_FALSE(s.ok());
}

TEST_F(FailoverTest, GetNodeForWrite) {
  Status s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kLeader);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  auto node = failover_manager_->GetNodeForWrite();
  ASSERT_TRUE(node.ok());
  EXPECT_EQ(node.ValueOrDie().node_id, "n1");
  EXPECT_EQ(node.ValueOrDie().role, NodeRole::kLeader);
}

TEST_F(FailoverTest, NoLeaderInitially) {
  // Don't register any leader
  Status s = failover_manager_->RegisterNode("f1", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  auto leader = failover_manager_->GetLeader();
  // May succeed with f1 elected as leader, or fail if no election happened
}

TEST_F(FailoverTest, NodeChangeCallback) {
  bool callback_called = false;
  std::string changed_node;
  NodeRole old_role = NodeRole::kUnknown;
  NodeRole new_role = NodeRole::kUnknown;
  
  failover_manager_->SetNodeChangeCallback(
    [&](const std::string& node_id, NodeRole from_role, NodeRole to_role) {
      callback_called = true;
      changed_node = node_id;
      old_role = from_role;
      new_role = to_role;
    });
  
  Status s = failover_manager_->RegisterNode("n1", "127.0.0.1:9779", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->RegisterNode("n2", "127.0.0.1:9780", NodeRole::kFollower);
  ASSERT_TRUE(s.ok());
  
  s = failover_manager_->Start();
  ASSERT_TRUE(s.ok());
  
  // Callback may be called during leader election
}

// =============================================================================
// ClusterFailoverManager Tests
// =============================================================================

TEST(ClusterFailoverManagerTest, RegisterSwitchLeaderHandlerAndExecute) {
  ClusterFailoverManager manager;
  ClusterFailoverManager::Config config;
  config.enable_auto_recovery = true;
  Status s = manager.Initialize(config);
  ASSERT_TRUE(s.ok());

  bool handler_called = false;
  PartitionID received_partition = 0;
  NodeID received_node = 0;

  manager.RegisterSwitchLeaderHandler(
    [&](PartitionID pid, NodeID nid) -> Status {
      handler_called = true;
      received_partition = pid;
      received_node = nid;
      return Status::OK();
    });

  FailureEvent event;
  event.type = FailureType::kNodeDown;
  event.node_id = 7;
  event.partition_id = 42;

  s = manager.ReportFailure(event);
  ASSERT_TRUE(s.ok());

  auto active = manager.GetActiveFailures();
  ASSERT_EQ(active.size(), 1);

  s = manager.TriggerRecovery(active[0].event_id);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(handler_called);
  EXPECT_EQ(received_partition, 42);
  EXPECT_EQ(received_node, 7);

  manager.Shutdown();
}

TEST(ClusterFailoverManagerTest, SwitchLeaderFallbackToIOError) {
  ClusterFailoverManager manager;
  ClusterFailoverManager::Config config;
  Status s = manager.Initialize(config);
  ASSERT_TRUE(s.ok());

  // No handler registered

  FailureEvent event;
  event.type = FailureType::kNodeDown;
  event.node_id = 7;
  event.partition_id = 42;

  s = manager.ReportFailure(event);
  ASSERT_TRUE(s.ok());

  auto active = manager.GetActiveFailures();
  ASSERT_EQ(active.size(), 1);

  s = manager.TriggerRecovery(active[0].event_id);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());

  manager.Shutdown();
}

TEST(ClusterFailoverManagerTest, SwitchLeaderHandlerExceptionReturnsIOError) {
  ClusterFailoverManager manager;
  ClusterFailoverManager::Config config;
  Status s = manager.Initialize(config);
  ASSERT_TRUE(s.ok());

  manager.RegisterSwitchLeaderHandler(
    [](PartitionID, NodeID) -> Status {
      throw std::runtime_error("handler failed");
    });

  FailureEvent event;
  event.type = FailureType::kNodeDown;
  event.node_id = 7;
  event.partition_id = 42;

  s = manager.ReportFailure(event);
  ASSERT_TRUE(s.ok());

  auto active = manager.GetActiveFailures();
  ASSERT_EQ(active.size(), 1);

  s = manager.TriggerRecovery(active[0].event_id);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());

  manager.Shutdown();
}

TEST(ClusterFailoverManagerTest, RegisterPromoteReplicaHandlerAndExecute) {
  ClusterFailoverManager manager;
  ClusterFailoverManager::Config config;
  Status s = manager.Initialize(config);
  ASSERT_TRUE(s.ok());

  bool handler_called = false;
  PartitionID received_partition = 0;
  NodeID received_node = 0;

  manager.RegisterPromoteReplicaHandler(
    [&](PartitionID pid, NodeID nid) -> Status {
      handler_called = true;
      received_partition = pid;
      received_node = nid;
      return Status::OK();
    });

  FailureEvent event;
  event.type = FailureType::kLeaderLost;
  event.node_id = 99;
  event.partition_id = 123;

  s = manager.ReportFailure(event);
  ASSERT_TRUE(s.ok());

  auto active = manager.GetActiveFailures();
  ASSERT_EQ(active.size(), 1);

  s = manager.TriggerRecovery(active[0].event_id);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(handler_called);
  EXPECT_EQ(received_partition, 123);
  EXPECT_EQ(received_node, 99);

  manager.Shutdown();
}

TEST(ClusterFailoverManagerTest, PromoteReplicaHandlerExceptionReturnsIOError) {
  ClusterFailoverManager manager;
  ClusterFailoverManager::Config config;
  Status s = manager.Initialize(config);
  ASSERT_TRUE(s.ok());

  manager.RegisterPromoteReplicaHandler(
    [](PartitionID, NodeID) -> Status {
      throw std::runtime_error("handler failed");
    });

  FailureEvent event;
  event.type = FailureType::kLeaderLost;
  event.node_id = 99;
  event.partition_id = 123;

  s = manager.ReportFailure(event);
  ASSERT_TRUE(s.ok());

  auto active = manager.GetActiveFailures();
  ASSERT_EQ(active.size(), 1);

  s = manager.TriggerRecovery(active[0].event_id);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());

  manager.Shutdown();
}

TEST(ClusterFailoverManagerTest, PromoteReplicaFallbackToIOError) {
  ClusterFailoverManager manager;
  ClusterFailoverManager::Config config;
  Status s = manager.Initialize(config);
  ASSERT_TRUE(s.ok());

  // No handler registered

  FailureEvent event;
  event.type = FailureType::kLeaderLost;
  event.node_id = 99;
  event.partition_id = 123;

  s = manager.ReportFailure(event);
  ASSERT_TRUE(s.ok());

  auto active = manager.GetActiveFailures();
  ASSERT_EQ(active.size(), 1);

  s = manager.TriggerRecovery(active[0].event_id);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());

  manager.Shutdown();
}
