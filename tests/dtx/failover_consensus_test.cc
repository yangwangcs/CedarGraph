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

  // Attempt leader switch — callback should be invoked
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
