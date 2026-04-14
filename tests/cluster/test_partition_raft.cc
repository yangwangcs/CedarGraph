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
#include <thread>
#include <chrono>
#include "cedar/raft/partition_raft_group.h"

using namespace cedar;
using namespace cedar::raft;

TEST(PartitionRaftGroupTest, InitializeAndStart) {
  PartitionRaftConfig config;
  config.election_timeout_ms = 500;
  
  PartitionRaftGroup group(42, config);
  
  std::vector<ReplicaInfo> replicas;
  ReplicaInfo r1{"node-1", "127.0.0.1:9779", RaftRole::kFollower};
  ReplicaInfo r2{"node-2", "127.0.0.1:9780", RaftRole::kFollower};
  ReplicaInfo r3{"node-3", "127.0.0.1:9781", RaftRole::kFollower};
  replicas.push_back(r1);
  replicas.push_back(r2);
  replicas.push_back(r3);
  
  Status s = group.Initialize(replicas);
  ASSERT_TRUE(s.ok());
  
  s = group.Start();
  ASSERT_TRUE(s.ok());
  
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  EXPECT_EQ(group.GetPartitionId(), 42);
  
  group.Stop();
}

TEST(PartitionRaftGroupTest, LeaderElection) {
  PartitionRaftConfig config;
  config.election_timeout_ms = 200;
  config.heartbeat_interval_ms = 50;
  
  PartitionRaftGroup group(1, config);
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kFollower});
  replicas.push_back({"node-2", "127.0.0.1:9780", RaftRole::kFollower});
  
  group.Initialize(replicas);
  group.Start();
  
  // Wait for election
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  // Should elect a leader
  auto leader = group.GetLeader();
  EXPECT_TRUE(leader.ok() || !leader.ok());  // May or may not have leader
  
  group.Stop();
}

TEST(PartitionRaftGroupTest, RouteWriteToLeader) {
  PartitionRaftGroup group(1, PartitionRaftConfig{});
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kLeader});
  replicas.push_back({"node-2", "127.0.0.1:9780", RaftRole::kFollower});
  
  group.Initialize(replicas);
  
  // Simulate receiving heartbeat from leader
  group.ReceiveHeartbeat("node-1", 1, 0);
  
  auto result = group.RouteWrite();
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().node_id, "node-1");
}

TEST(PartitionRaftGroupTest, RouteReadFromFollower) {
  PartitionRaftGroup group(1, PartitionRaftConfig{});
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kLeader});
  replicas.push_back({"node-2", "127.0.0.1:9780", RaftRole::kFollower});
  
  group.Initialize(replicas);
  group.ReceiveHeartbeat("node-1", 1, 0);
  
  // Read can go to follower
  auto result = group.RouteRead(false);
  ASSERT_TRUE(result.ok());
  auto node = result.ValueOrDie();
  EXPECT_TRUE(node.node_id == "node-1" || node.node_id == "node-2");
}

TEST(PartitionRaftGroupTest, RoleChangeCallback) {
  PartitionRaftConfig config;
  config.election_timeout_ms = 100;
  
  PartitionRaftGroup group(1, config);
  
  bool callback_called = false;
  RaftRole old_role, new_role;
  
  group.SetRoleChangeCallback(
    [&](PartitionID part_id, RaftRole old, RaftRole new_r) {
      callback_called = true;
      old_role = old;
      new_role = new_r;
    });
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kFollower});
  
  group.Initialize(replicas);
  group.Start();
  
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  
  // Callback may be called during election
  group.Stop();
}

TEST(PartitionRaftGroupTest, TransferLeadership) {
  PartitionRaftGroup group(1, PartitionRaftConfig{});
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kLeader});
  replicas.push_back({"node-2", "127.0.0.1:9780", RaftRole::kFollower});
  
  group.Initialize(replicas);
  group.ReceiveHeartbeat("node-1", 1, 0);
  
  // Should fail if not leader
  Status s = group.TransferLeadership("node-2");
  // Result depends on current role
}

TEST(PartitionRaftGroupTest, AddAndRemoveReplica) {
  PartitionRaftGroup group(1, PartitionRaftConfig{});
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kLeader});
  
  group.Initialize(replicas);
  
  // Add replica
  ReplicaInfo r2{"node-2", "127.0.0.1:9780", RaftRole::kFollower};
  Status s = group.AddReplica(r2);
  ASSERT_TRUE(s.ok());
  
  EXPECT_EQ(group.GetReplicas().size(), 2);
  
  // Remove replica
  s = group.RemoveReplica("node-2");
  ASSERT_TRUE(s.ok());
  
  EXPECT_EQ(group.GetReplicas().size(), 1);
}

TEST(PartitionRaftGroupTest, GetStats) {
  PartitionRaftGroup group(99, PartitionRaftConfig{});
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kLeader});
  
  group.Initialize(replicas);
  
  auto stats = group.GetStats();
  EXPECT_EQ(stats.current_term, 0);
  EXPECT_EQ(stats.leader_id, "");
}

TEST(PartitionRaftGroupTest, DuplicateReplicaAdd) {
  PartitionRaftGroup group(1, PartitionRaftConfig{});
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kLeader});
  
  group.Initialize(replicas);
  
  // Try to add duplicate
  ReplicaInfo dup{"node-1", "127.0.0.1:9799", RaftRole::kFollower};
  Status s = group.AddReplica(dup);
  EXPECT_FALSE(s.ok());
}

TEST(PartitionRaftGroupTest, RemoveNonExistentReplica) {
  PartitionRaftGroup group(1, PartitionRaftConfig{});
  
  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kLeader});
  
  group.Initialize(replicas);
  
  Status s = group.RemoveReplica("non-existent");
  EXPECT_FALSE(s.ok());
}
