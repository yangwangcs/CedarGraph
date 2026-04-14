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

#include "cedar/raft/partition_raft_group.h"

#include <algorithm>
#include <random>
#include <thread>

namespace cedar {
namespace raft {

PartitionRaftGroup::PartitionRaftGroup(cedar::PartitionID part_id,
                                        const PartitionRaftConfig& config)
    : part_id_(part_id), config_(config) {
  // Randomize election timeout to avoid split votes
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(
      config.election_timeout_ms, 
      config.election_timeout_ms + 500);
  config_.election_timeout_ms = dis(gen);
}

PartitionRaftGroup::~PartitionRaftGroup() {
  Stop();
}

Status PartitionRaftGroup::Initialize(
    const std::vector<ReplicaInfo>& replicas,
    const std::string& initial_leader) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  replicas_ = replicas;
  for (size_t i = 0; i < replicas.size(); i++) {
    replica_index_[replicas[i].node_id] = i;
  }
  
  // Set initial leader if provided
  if (!initial_leader.empty()) {
    current_leader_ = initial_leader;
  } else {
    current_leader_.clear();
  }
  
  return Status::OK();
}

Status PartitionRaftGroup::Start() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Already running");
  }
  
  // Initialize election timeout
  last_heartbeat_time_ = std::chrono::steady_clock::now();
  election_timeout_ = last_heartbeat_time_ + 
      std::chrono::milliseconds(config_.election_timeout_ms);
  
  return Status::OK();
}

void PartitionRaftGroup::Stop() {
  running_.store(false);
}

StatusOr<std::string> PartitionRaftGroup::GetLeader() const {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  if (current_leader_.empty()) {
    return Status::NotFound("No leader elected");
  }
  
  return current_leader_;
}

std::vector<ReplicaInfo> PartitionRaftGroup::GetReplicas() const {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  return replicas_;
}

std::vector<ReplicaInfo> PartitionRaftGroup::GetHealthyReplicas() const {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  std::vector<ReplicaInfo> healthy;
  for (const auto& replica : replicas_) {
    if (replica.is_healthy) {
      healthy.push_back(replica);
    }
  }
  
  return healthy;
}

StatusOr<ReplicaInfo> PartitionRaftGroup::RouteWrite() {
  // Write must go to the leader
  auto leader_id = GetLeader();
  if (!leader_id.ok()) {
    return leader_id.status();
  }
  
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  auto it = replica_index_.find(leader_id.ValueOrDie());
  if (it == replica_index_.end()) {
    return Status::NotFound("Leader not found in replica list");
  }
  
  return replicas_[it->second];
}

StatusOr<ReplicaInfo> PartitionRaftGroup::RouteRead(bool require_leader) {
  if (require_leader) {
    return RouteWrite();
  }
  
  // Can read from any healthy replica
  auto healthy = GetHealthyReplicas();
  if (healthy.empty()) {
    return Status::IOError("No healthy replicas available");
  }
  
  // Prefer leader, then round-robin among followers
  for (const auto& replica : healthy) {
    if (replica.role == RaftRole::kLeader) {
      return replica;
    }
  }
  
  // Simple round-robin for followers
  static std::atomic<size_t> counter{0};
  return healthy[counter++ % healthy.size()];
}

Status PartitionRaftGroup::AddReplica(const ReplicaInfo& replica) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  if (replica_index_.find(replica.node_id) != replica_index_.end()) {
    return Status::InvalidArgument("Replica already exists: " + replica.node_id);
  }
  
  replica_index_[replica.node_id] = replicas_.size();
  replicas_.push_back(replica);
  
  return Status::OK();
}

Status PartitionRaftGroup::RemoveReplica(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  auto it = replica_index_.find(node_id);
  if (it == replica_index_.end()) {
    return Status::NotFound("PartitionRaftGroup::RemoveReplica",
        "Replica not found: " + node_id);
  }
  
  size_t index = it->second;
  replicas_.erase(replicas_.begin() + index);
  replica_index_.erase(it);
  
  // Update indices
  for (size_t i = index; i < replicas_.size(); i++) {
    replica_index_[replicas_[i].node_id] = i;
  }
  
  return Status::OK();
}

Status PartitionRaftGroup::TransferLeadership(const std::string& target_node) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  if (current_role_ != RaftRole::kLeader) {
    return Status::InvalidArgument("Not the leader");
  }
  
  if (replica_index_.find(target_node) == replica_index_.end()) {
    return Status::NotFound("Target node not found: " + target_node);
  }
  
  // Simulate leadership transfer
  UpdateLeader(target_node);
  BecomeFollower(current_term_);
  
  return Status::OK();
}

Status PartitionRaftGroup::ReceiveHeartbeat(const std::string& from_node,
                                             uint64_t term,
                                             uint64_t log_index) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  // Update heartbeat time
  last_heartbeat_time_ = std::chrono::steady_clock::now();
  election_timeout_ = last_heartbeat_time_ + 
      std::chrono::milliseconds(config_.election_timeout_ms);
  
  // If received heartbeat with higher term, become follower
  if (term > current_term_) {
    current_term_ = term;
    BecomeFollower(term);
  }
  
  // Update leader
  if (from_node != current_leader_) {
    UpdateLeader(from_node);
  }
  
  // Update replica info
  auto replica = FindReplica(from_node);
  if (replica) {
    replica->last_heartbeat = std::chrono::steady_clock::now();
    replica->log_index = log_index;
    replica->role = RaftRole::kLeader;
  }
  
  return Status::OK();
}

Status PartitionRaftGroup::ReceiveVoteRequest(const std::string& candidate,
                                               uint64_t term,
                                               bool* granted) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  *granted = false;
  
  if (term < current_term_) {
    return Status::OK();  // Reject vote
  }
  
  if (term > current_term_) {
    current_term_ = term;
    voted_for_.clear();
    BecomeFollower(term);
  }
  
  if (voted_for_.empty() || voted_for_ == candidate) {
    voted_for_ = candidate;
    *granted = true;
    
    // Reset election timeout
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    election_timeout_ = last_heartbeat_time_ + 
        std::chrono::milliseconds(config_.election_timeout_ms);
  }
  
  return Status::OK();
}

PartitionRaftGroup::Stats PartitionRaftGroup::GetStats() const {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  Stats stats;
  stats.current_term = current_term_;
  stats.leader_id = current_leader_;
  stats.role = current_role_;
  
  return stats;
}

void PartitionRaftGroup::SetRoleChangeCallback(
    std::function<void(cedar::PartitionID, RaftRole, RaftRole)> callback) {
  role_change_callback_ = callback;
}

void PartitionRaftGroup::SetLeaderChangeCallback(
    std::function<void(cedar::PartitionID, const std::string&, const std::string&)> callback) {
  leader_change_callback_ = callback;
}

std::chrono::milliseconds PartitionRaftGroup::RaftTick() {
  if (!running_) {
    return std::chrono::milliseconds(10);
  }
  
  auto now = std::chrono::steady_clock::now();
  
  switch (current_role_) {
    case RaftRole::kLeader:
      SendHeartbeats();
      return std::chrono::milliseconds(config_.heartbeat_interval_ms);
      
    case RaftRole::kFollower:
      if (now >= election_timeout_) {
        BecomeCandidate();
      }
      return std::chrono::milliseconds(10);
      
    case RaftRole::kCandidate:
      CheckElectionTimeout();
      return std::chrono::milliseconds(10);
      
    default:
      return std::chrono::milliseconds(10);
  }
}

void PartitionRaftGroup::SendHeartbeats() {
  // Leader sends heartbeats to all followers
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  
  for (auto& replica : replicas_) {
    if (replica.node_id != current_leader_) {
      // In actual implementation: send heartbeat via gRPC
      // Simplified here
      replica.last_heartbeat = std::chrono::steady_clock::now();
    }
  }
}

void PartitionRaftGroup::CheckElectionTimeout() {
  auto now = std::chrono::steady_clock::now();
  
  if (now >= election_timeout_) {
    // Election timeout, start new election
    BecomeCandidate();
  }
}

void PartitionRaftGroup::BecomeFollower(uint64_t term) {
  auto old_role = current_role_.exchange(RaftRole::kFollower);
  current_term_ = term;
  
  if (role_change_callback_ && old_role != RaftRole::kFollower) {
    role_change_callback_(static_cast<cedar::PartitionID>(part_id_), old_role, RaftRole::kFollower);
  }
}

void PartitionRaftGroup::BecomeCandidate() {
  auto old_role = current_role_.exchange(RaftRole::kCandidate);
  current_term_++;
  voted_for_.clear();
  
  // Vote for self
  voted_for_ = "self";  // Should be current node ID in actual implementation
  
  // Reset election timeout
  last_heartbeat_time_ = std::chrono::steady_clock::now();
  election_timeout_ = last_heartbeat_time_ + 
      std::chrono::milliseconds(config_.election_timeout_ms * 2);
  
  if (role_change_callback_ && old_role != RaftRole::kCandidate) {
    role_change_callback_(static_cast<cedar::PartitionID>(part_id_), old_role, RaftRole::kCandidate);
  }
  
  // In actual implementation: send vote requests to other nodes
  // If majority votes received, become leader
  // Simplified: assume we always succeed
  BecomeLeader();
}

void PartitionRaftGroup::BecomeLeader() {
  auto old_role = current_role_.exchange(RaftRole::kLeader);
  
  // Update own replica info
  {
    std::lock_guard<std::mutex> lock(replicas_mutex_);
    for (auto& replica : replicas_) {
      if (replica.node_id == current_leader_) {
        replica.role = RaftRole::kLeader;
      } else {
        replica.role = RaftRole::kFollower;
      }
    }
  }
  
  if (role_change_callback_ && old_role != RaftRole::kLeader) {
    role_change_callback_(static_cast<cedar::PartitionID>(part_id_), old_role, RaftRole::kLeader);
  }
  
  // Send heartbeats immediately
  SendHeartbeats();
}

void PartitionRaftGroup::UpdateLeader(const std::string& new_leader) {
  if (new_leader == current_leader_) {
    return;
  }
  
  std::string old_leader = current_leader_;
  current_leader_ = new_leader;
  
  if (leader_change_callback_) {
    leader_change_callback_(static_cast<cedar::PartitionID>(part_id_), old_leader, new_leader);
  }
}

ReplicaInfo* PartitionRaftGroup::FindReplica(const std::string& node_id) {
  auto it = replica_index_.find(node_id);
  if (it == replica_index_.end()) {
    return nullptr;
  }
  return &replicas_[it->second];
}

}  // namespace raft
}  // namespace cedar
