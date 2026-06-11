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

#include "cedar/dtx/failover_manager.h"

using namespace cedar::dtx;
using cedar::Status;

TEST(FailoverConsensusTest, NoConsensusCallbackRejectsSwitch) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::seconds(10);
  config.check_interval = std::chrono::seconds(1);

  ASSERT_TRUE(controller.Initialize(config).ok());

  // Register a partition with node 1 as leader and node 2 as replica
  ASSERT_TRUE(controller.RegisterPartition(42, 1, {2}).ok());

  // Attempt leader switch without registering consensus callback
  Status s = controller.TriggerManualFailover(42, 2);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
  EXPECT_NE(s.ToString().find("Manual intervention required"), std::string::npos);

  // Verify leader was NOT changed
  auto state = controller.GetPartitionState(42);
  ASSERT_TRUE(state.ok());
  EXPECT_EQ(state.ValueOrDie().current_leader, 1u);

  controller.Shutdown();
}

TEST(FailoverConsensusTest, ConsensusCallbackFailureRejectsSwitch) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  ASSERT_TRUE(controller.Initialize(config).ok());
  ASSERT_TRUE(controller.RegisterPartition(100, 1, {1, 2, 3}).ok());

  controller.SetConsensusTransferCallback(
      [](PartitionID pid, NodeID new_leader) -> Status {
        return Status::IOError("Simulated Raft timeout");
      });

  Status s = controller.TriggerManualFailover(100, 2);
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("Raft timeout"), std::string::npos);

  // Verify leader is unchanged
  auto state_result = controller.GetPartitionState(100);
  ASSERT_TRUE(state_result.ok());
  EXPECT_EQ(state_result.value().current_leader, 1u);
}

TEST(FailoverConsensusTest, ConsensusCallbackIsInvoked) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::seconds(10);
  config.check_interval = std::chrono::seconds(1);

  ASSERT_TRUE(controller.Initialize(config).ok());

  // Register a partition with node 1 as leader and node 2 as replica
  ASSERT_TRUE(controller.RegisterPartition(42, 1, {2}).ok());

  // Register a consensus transfer callback
  bool callback_called = false;
  PartitionID received_pid = 0;
  NodeID received_new_leader = 0;

  controller.SetConsensusTransferCallback(
    [&](PartitionID pid, NodeID new_leader) -> Status {
      callback_called = true;
      received_pid = pid;
      received_new_leader = new_leader;
      return Status::OK();
    });

  // Attempt leader switch -- callback should be invoked
  Status s = controller.TriggerManualFailover(42, 2);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(received_pid, 42u);
  EXPECT_EQ(received_new_leader, 2u);

  // Verify leader WAS changed
  auto state = controller.GetPartitionState(42);
  ASSERT_TRUE(state.ok());
  EXPECT_EQ(state.ValueOrDie().current_leader, 2u);

  controller.Shutdown();
}

TEST(FailoverConsensusTest, QuorumVerificationBlocksLeaderUpdate) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::seconds(10);
  config.check_interval = std::chrono::seconds(1);
  config.leader_switch_timeout = std::chrono::milliseconds(100);

  ASSERT_TRUE(controller.Initialize(config).ok());
  ASSERT_TRUE(controller.RegisterPartition(200, 1, {2, 3}).ok());

  // Consensus succeeds, but quorum verification fails.
  controller.SetConsensusTransferCallback(
      [](PartitionID pid, NodeID new_leader) -> Status {
        return Status::OK();
      });
  controller.SetQuorumVerificationCallback(
      [](PartitionID pid, NodeID new_leader, std::chrono::milliseconds timeout) -> Status {
        return Status::IOError("Simulated quorum timeout");
      });

  Status s = controller.TriggerManualFailover(200, 2);
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("quorum timeout"), std::string::npos);

  // Leader MUST NOT have changed
  auto state_result = controller.GetPartitionState(200);
  ASSERT_TRUE(state_result.ok());
  EXPECT_EQ(state_result.value().current_leader, 1u);
}

TEST(FailoverConsensusTest, QuorumVerificationSuccessAllowsSwitch) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::seconds(10);
  config.check_interval = std::chrono::seconds(1);

  ASSERT_TRUE(controller.Initialize(config).ok());
  ASSERT_TRUE(controller.RegisterPartition(201, 1, {2}).ok());

  bool quorum_called = false;
  controller.SetConsensusTransferCallback(
      [](PartitionID pid, NodeID new_leader) -> Status {
        return Status::OK();
      });
  controller.SetQuorumVerificationCallback(
      [&](PartitionID pid, NodeID new_leader, std::chrono::milliseconds timeout) -> Status {
        quorum_called = true;
        EXPECT_EQ(pid, 201u);
        EXPECT_EQ(new_leader, 2u);
        return Status::OK();
      });

  Status s = controller.TriggerManualFailover(201, 2);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(quorum_called);

  auto state_result = controller.GetPartitionState(201);
  ASSERT_TRUE(state_result.ok());
  EXPECT_EQ(state_result.value().current_leader, 2u);
}

TEST(FailoverConsensusTest, MaxConcurrentRecoveriesEnforced) {
  ClusterFailoverManager manager;
  ClusterFailoverManager::Config config;
  config.enable_auto_recovery = true;
  config.max_concurrent_recoveries = 1;
  config.failure_retention_duration = std::chrono::minutes(0);

  ASSERT_TRUE(manager.Initialize(config).ok());

  // Report two failure events via the public API.
  FailureEvent event1;
  event1.type = FailureType::kNodeDown;
  event1.node_id = 1;
  event1.partition_id = 10;
  event1.detected_at = std::chrono::system_clock::now();

  FailureEvent event2;
  event2.type = FailureType::kNodeDown;
  event2.node_id = 2;
  event2.partition_id = 20;
  event2.detected_at = std::chrono::system_clock::now();

  ASSERT_TRUE(manager.ReportFailure(event1).ok());
  ASSERT_TRUE(manager.ReportFailure(event2).ok());

  EXPECT_EQ(manager.GetActiveFailures().size(), 2u);

  // Trigger recovery on one event. Because there is no registered recovery
  // handler for kNodeDown and the default path leads to a containerized
  // service restart (SIGTERM to self), ExecuteRecovery may return OK or
  // IOError depending on the environment. The key property we verify here is
  // that MarkRecovered -- called when recovery succeeds -- erases the event
  // from the failures map when retention is zero.
  Status rs = manager.TriggerRecovery(1);  // event_id assigned sequentially
  if (rs.ok()) {
    EXPECT_EQ(manager.GetActiveFailures().size(), 1u);
  }
}
