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

#include "cedar/dtx/raft/embedded_raft.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// EmbeddedRaftNode Implementation
// =============================================================================

EmbeddedRaftNode::EmbeddedRaftNode(const Options& options,
                                   RaftTransport* transport,
                                   EmbeddedStateMachine* state_machine,
                                   RaftStorage* storage)
    : options_(options),
      transport_(transport),
      state_machine_(state_machine),
      storage_(storage) {
  
  // Initialize peers
  for (const auto& [id, addr] : options.peers) {
    if (id != options.node_id) {
      peers_[id] = addr;
    }
  }
  
  // Reserve log[0] as dummy entry
  ::cedar::dtx::raft::LogEntry dummy;
  dummy.term = 0;
  dummy.index = 0;
  dummy.data = "";
  log_.push_back(dummy);
}

EmbeddedRaftNode::~EmbeddedRaftNode() {
  Shutdown();
}

::cedar::Status EmbeddedRaftNode::Start() {
  if (running_.exchange(true)) {
    return ::cedar::Status::InvalidArgument("Already started");
  }
  
  // Load persistent state
  Term term = 0;
  NodeId voted_for = 0;
  if (storage_->LoadState(&term, &voted_for).ok()) {
    current_term_ = term;
    voted_for_ = voted_for;
  }
  
  // Load snapshot if exists
  auto snapshot_result = storage_->LoadSnapshot();
  if (snapshot_result.ok()) {
    const auto& snapshot = snapshot_result.value();
    if (state_machine_->RestoreSnapshot(snapshot.data).ok()) {
      last_applied_ = snapshot.last_included_index;
      commit_index_ = snapshot.last_included_index;
      
      // Add dummy log entry for snapshot
      log_.clear();
      ::cedar::dtx::raft::LogEntry snapshot_entry;
      snapshot_entry.term = snapshot.last_included_term;
      snapshot_entry.index = snapshot.last_included_index;
      snapshot_entry.data = "";
      log_.push_back(snapshot_entry);
    }
  }
  
  // Start event loop
  event_loop_thread_ = std::thread(&EmbeddedRaftNode::RunEventLoop, this);
  
  return ::cedar::Status::OK();
}

void EmbeddedRaftNode::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  cv_.notify_all();
  
  if (event_loop_thread_.joinable()) {
    event_loop_thread_.join();
  }
}

::cedar::Status EmbeddedRaftNode::Propose(const std::string& data) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (state_ != NodeState::kLeader) {
    return ::cedar::Status::NotLeader("Not leader");
  }
  
  // Append to local log
  ::cedar::dtx::raft::LogEntry entry;
  entry.term = current_term_.load();
  entry.index = log_.back().index + 1;
  entry.data = data;
  
  log_.push_back(entry);
  
  // Persist
  storage_->AppendLog({entry});
  
  // Trigger replication
  cv_.notify_all();
  
  return ::cedar::Status::OK();
}

std::optional<NodeId> EmbeddedRaftNode::GetLeaderId() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (state_ == NodeState::kLeader) {
    return options_.node_id;
  }
  return leader_id_;
}

EmbeddedRaftNode::NodeStatus EmbeddedRaftNode::GetStatus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  NodeStatus status;
  status.state = state_;
  status.current_term = current_term_.load();
  status.voted_for = voted_for_.load();
  status.commit_index = commit_index_.load();
  status.last_log_index = log_.empty() ? 0 : log_.back().index;
  status.last_applied = last_applied_.load();
  status.leader_id = GetLeaderId();
  
  return status;
}

void EmbeddedRaftNode::RunEventLoop() {
  std::cout << "[Raft] Event loop started" << std::endl;
  last_election_reset_ = std::chrono::steady_clock::now();
  int loop_count = 0;
  
  while (running_.load()) {
    loop_count++;
    if (loop_count % 100 == 0) {
      std::cout << "[Raft] Event loop running, state=" << static_cast<int>(state_.load()) << std::endl;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    auto timeout = std::chrono::milliseconds(10);
    
    switch (state_.load()) {
      case NodeState::kFollower:
      case NodeState::kCandidate: {
        // Check election timeout
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_election_reset_);
        if (elapsed >= GetRandomElectionTimeout()) {
          lock.unlock();
          BecomeCandidate();
          lock.lock();
        }
        timeout = std::chrono::milliseconds(10);
        break;
      }
      
      case NodeState::kLeader: {
        // Send heartbeats
        auto heartbeat_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_heartbeat_);
        if (heartbeat_elapsed >= std::chrono::milliseconds(options_.heartbeat_interval)) {
          lock.unlock();
          SendHeartbeats();
          MaybeSnapshot();
          lock.lock();
          last_heartbeat_ = std::chrono::steady_clock::now();
        }
        timeout = std::chrono::milliseconds(options_.heartbeat_interval / 2);
        break;
      }
    }
    
    // Apply committed entries
    ApplyCommittedEntries();
    
    lock.unlock();
    
    // Wait for next event or timeout
    std::unique_lock<std::mutex> wait_lock(mutex_);
    cv_.wait_for(wait_lock, timeout, [this] {
      return !running_.load();
    });
  }
}

void EmbeddedRaftNode::BecomeFollower(Term term) {
  NodeState old_state;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    old_state = state_.exchange(NodeState::kFollower);
    current_term_ = term;
    voted_for_ = 0;
    leader_state_.reset();
    
    last_election_reset_ = std::chrono::steady_clock::now();
  }
  
  // Persist state
  storage_->SaveState(term, 0);
  
  if (old_state == NodeState::kLeader) {
    state_machine_->OnStepDown(term);
  }
}

void EmbeddedRaftNode::BecomeCandidate() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = NodeState::kCandidate;
    current_term_++;
    voted_for_ = options_.node_id;
    leader_id_.reset();
    
    last_election_reset_ = std::chrono::steady_clock::now();
  }
  
  // Persist state
  storage_->SaveState(current_term_.load(), options_.node_id);
  
  StartElection();
}

void EmbeddedRaftNode::BecomeLeader() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ != NodeState::kCandidate) {
      return;
    }
    
    state_ = NodeState::kLeader;
    leader_id_ = options_.node_id;
    
    // Initialize leader state
    LeaderState ls;
    LogIndex last_log = log_.empty() ? 0 : log_.back().index;
    
    std::shared_lock<std::shared_mutex> peer_lock(peers_mutex_);
    for (const auto& [id, _] : peers_) {
      ls.next_index.push_back(last_log + 1);
      ls.match_index.push_back(0);
    }
    leader_state_ = std::move(ls);
    
    last_heartbeat_ = std::chrono::steady_clock::now();
  }
  
  state_machine_->OnBecomeLeader(current_term_.load());
  
  // Send immediate heartbeat
  SendHeartbeats();
}

void EmbeddedRaftNode::StartElection() {
  std::cout << "[Raft] Starting election for term " << current_term_.load() 
            << ", peers=" << peers_.size() << std::endl;
  
  VoteRequest req;
  req.term = current_term_.load();
  req.candidate_id = options_.node_id;
  req.last_log_index = log_.empty() ? 0 : log_.back().index;
  req.last_log_term = log_.empty() ? 0 : log_.back().term;
  
  int votes = 1;  // Vote for self
  int needed = (peers_.size() + 1) / 2 + 1;
  std::cout << "[Raft] Need " << needed << " votes, have 1 (self)" << std::endl;
  
  std::shared_lock<std::shared_mutex> peer_lock(peers_mutex_);
  
  for (const auto& [id, _] : peers_) {
    VoteResponse resp;
    std::cout << "[Raft] Requesting vote from node " << id << std::endl;
    if (RequestVote(id, req, &resp)) {
      std::cout << "[Raft] Got vote from node " << id << ": granted=" << resp.vote_granted << std::endl;
      if (resp.vote_granted) {
        votes++;
        if (votes >= needed) {
          std::cout << "[Raft] Got enough votes, becoming leader!" << std::endl;
          BecomeLeader();
          return;
        }
      } else if (resp.term > current_term_.load()) {
        BecomeFollower(resp.term);
        return;
      }
    } else {
      std::cout << "[Raft] Failed to get vote from node " << id << std::endl;
    }
  }
  
  // Check if we have enough votes (important for single-node case)
  if (votes >= needed) {
    std::cout << "[Raft] Got enough votes (including self), becoming leader!" << std::endl;
    BecomeLeader();
    return;
  }
  
  std::cout << "[Raft] Election completed with " << votes << " votes, needed " << needed << std::endl;
}

bool EmbeddedRaftNode::RequestVote(NodeId target, const VoteRequest& req, 
                                     VoteResponse* resp) {
  // In production, this would use network RPC
  // For now, use the transport interface
  return transport_->SendVoteRequest(target, req, resp).ok();
}

void EmbeddedRaftNode::SendHeartbeats() {
  std::shared_lock<std::shared_mutex> peer_lock(peers_mutex_);
  
  for (const auto& [id, _] : peers_) {
    ReplicateLog(id);
  }
}

void EmbeddedRaftNode::ReplicateLog(NodeId target) {
  if (!leader_state_.has_value()) {
    return;
  }
  
  AppendEntriesRequest req;
  
  {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Find peer index
    int peer_idx = -1;
    int idx = 0;
    for (const auto& [id, _] : peers_) {
      if (id == target) {
        peer_idx = idx;
        break;
      }
      idx++;
    }
    
    if (peer_idx < 0) return;
    
    LogIndex next_idx = leader_state_->next_index[peer_idx];
    
    // Build request
    req.term = current_term_.load();
    req.leader_id = options_.node_id;
    req.leader_commit = commit_index_.load();
    
    // Find prev_log_index and prev_log_term
    LogIndex prev_idx = next_idx - 1;
    auto it = std::find_if(log_.begin(), log_.end(),
                           [prev_idx](const ::cedar::dtx::raft::LogEntry& e) { return e.index == prev_idx; });
    if (it != log_.end()) {
      req.prev_log_index = prev_idx;
      req.prev_log_term = it->term;
    } else if (prev_idx == 0) {
      req.prev_log_index = 0;
      req.prev_log_term = 0;
    } else {
      // Need to send snapshot
      return;
    }
    
    // Get entries to send
    auto start_it = std::find_if(log_.begin(), log_.end(),
                                 [next_idx](const ::cedar::dtx::raft::LogEntry& e) { return e.index == next_idx; });
    for (auto it = start_it; it != log_.end() && req.entries.size() < 100; ++it) {
      req.entries.push_back(*it);
    }
  }
  
  AppendEntriesResponse resp;
  if (SendAppendEntries(target, req, &resp)) {
    ProcessAppendEntriesResponse(target, resp);
  }
}

bool EmbeddedRaftNode::SendAppendEntries(NodeId target, const AppendEntriesRequest& req,
                                          AppendEntriesResponse* resp) {
  return transport_->SendAppendEntries(target, req, resp).ok();
}

void EmbeddedRaftNode::ProcessAppendEntriesResponse(NodeId from, 
                                                     const AppendEntriesResponse& resp) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (resp.term > current_term_.load()) {
    BecomeFollower(resp.term);
    return;
  }
  
  if (state_ != NodeState::kLeader || !leader_state_.has_value()) {
    return;
  }
  
  // Find peer index
  int peer_idx = -1;
  int idx = 0;
  for (const auto& [id, _] : peers_) {
    if (id == from) {
      peer_idx = idx;
      break;
    }
    idx++;
  }
  
  if (peer_idx < 0) return;
  
  if (resp.success) {
    if (resp.match_index != 0) {
      leader_state_->match_index[peer_idx] = resp.match_index;
      leader_state_->next_index[peer_idx] = resp.match_index + 1;
    }
    AdvanceCommitIndex();
  } else {
    // Decrement next_index and retry
    if (leader_state_->next_index[peer_idx] > 1) {
      leader_state_->next_index[peer_idx]--;
    }
    cv_.notify_all();
  }
}

void EmbeddedRaftNode::AdvanceCommitIndex() {
  if (!leader_state_.has_value()) return;
  
  LogIndex new_commit = commit_index_.load();
  LogIndex last_log = log_.empty() ? 0 : log_.back().index;
  
  for (LogIndex idx = commit_index_.load() + 1; idx <= last_log; idx++) {
    // Count replicas
    int count = 1;  // Self
    for (size_t i = 0; i < leader_state_->match_index.size(); i++) {
      if (leader_state_->match_index[i] >= idx) {
        count++;
      }
    }
    
    if (count >= (peers_.size() + 1) / 2 + 1) {
      // Find entry at this index and check term
      auto it = std::find_if(log_.begin(), log_.end(),
                             [idx](const ::cedar::dtx::raft::LogEntry& e) { return e.index == idx; });
      if (it != log_.end() && it->term == current_term_.load()) {
        new_commit = idx;
      }
    }
  }
  
  if (new_commit > commit_index_.load()) {
    commit_index_ = new_commit;
    cv_.notify_all();
  }
}

void EmbeddedRaftNode::ApplyCommittedEntries() {
  LogIndex last_applied = last_applied_.load();
  LogIndex commit_index = commit_index_.load();
  
  while (last_applied < commit_index) {
    last_applied++;
    
    auto it = std::find_if(log_.begin(), log_.end(),
                           [last_applied](const ::cedar::dtx::raft::LogEntry& e) { 
                             return e.index == last_applied; 
                           });
    
    if (it != log_.end()) {
      state_machine_->Apply(*it);
    }
    
    last_applied_ = last_applied;
  }
}

VoteResponse EmbeddedRaftNode::HandleVoteRequest(const VoteRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  VoteResponse resp;
  resp.term = current_term_.load();
  resp.vote_granted = false;
  
  if (req.term < current_term_.load()) {
    return resp;
  }
  
  if (req.term > current_term_.load()) {
    current_term_ = req.term;
    voted_for_ = 0;
    state_ = NodeState::kFollower;
    storage_->SaveState(req.term, 0);
  }
  
  resp.term = current_term_.load();
  
  NodeId voted = voted_for_.load();
  if ((voted == 0 || voted == req.candidate_id)) {
    // Check if candidate's log is at least as up-to-date
    LogIndex last_idx = log_.empty() ? 0 : log_.back().index;
    Term last_term = log_.empty() ? 0 : log_.back().term;
    
    if (req.last_log_term > last_term ||
        (req.last_log_term == last_term && req.last_log_index >= last_idx)) {
      resp.vote_granted = true;
      voted_for_ = req.candidate_id;
      last_election_reset_ = std::chrono::steady_clock::now();
      storage_->SaveState(current_term_.load(), req.candidate_id);
    }
  }
  
  return resp;
}

AppendEntriesResponse EmbeddedRaftNode::HandleAppendEntries(
    const AppendEntriesRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  AppendEntriesResponse resp;
  resp.term = current_term_.load();
  resp.success = false;
  
  if (req.term < current_term_.load()) {
    return resp;
  }
  
  // Reset election timeout
  last_election_reset_ = std::chrono::steady_clock::now();
  leader_id_ = req.leader_id;
  
  if (req.term > current_term_.load()) {
    current_term_ = req.term;
    voted_for_ = 0;
    state_ = NodeState::kFollower;
    storage_->SaveState(req.term, 0);
  }
  
  resp.term = current_term_.load();
  
  // Check prev_log_index/prev_log_term
  if (req.prev_log_index > 0) {
    auto it = std::find_if(log_.begin(), log_.end(),
                           [&req](const ::cedar::dtx::raft::LogEntry& e) { 
                             return e.index == req.prev_log_index; 
                           });
    if (it == log_.end() || it->term != req.prev_log_term) {
      return resp;
    }
  }
  
  // Success - append entries
  resp.success = true;
  resp.match_index = req.prev_log_index;
  
  if (!req.entries.empty()) {
    // Find insertion point
    LogIndex insert_after = req.prev_log_index;
    
    for (const auto& entry : req.entries) {
      // Remove any conflicting entries
      auto existing = std::find_if(log_.begin(), log_.end(),
                                   [&entry](const ::cedar::dtx::raft::LogEntry& e) { 
                                     return e.index == entry.index; 
                                   });
      if (existing != log_.end()) {
        if (existing->term != entry.term) {
          // Conflict - truncate log
          log_.erase(existing, log_.end());
          storage_->TruncateLog(entry.index);
        } else {
          // Duplicate - skip
          insert_after = entry.index;
          continue;
        }
      }
      
      // Append entry
      log_.push_back(entry);
      storage_->AppendLog({entry});
      insert_after = entry.index;
    }
    
    resp.match_index = insert_after;
  }
  
  // Update commit index
  if (req.leader_commit > commit_index_.load()) {
    commit_index_ = std::min(req.leader_commit, 
                              log_.empty() ? 0 : log_.back().index);
    cv_.notify_all();
  }
  
  return resp;
}

std::chrono::milliseconds EmbeddedRaftNode::GetRandomElectionTimeout() const {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(
      options_.election_timeout_min, options_.election_timeout_max);
  return std::chrono::milliseconds(dis(gen));
}

// Stub implementations
::cedar::Status EmbeddedRaftNode::TriggerSnapshot() {
  return ::cedar::Status::NotSupported("Snapshot not implemented");
}

::cedar::Status EmbeddedRaftNode::AddPeer(NodeId id, const std::string& address) {
  std::unique_lock<std::shared_mutex> lock(peers_mutex_);
  peers_[id] = address;
  return ::cedar::Status::OK();
}

::cedar::Status EmbeddedRaftNode::RemovePeer(NodeId id) {
  std::unique_lock<std::shared_mutex> lock(peers_mutex_);
  peers_.erase(id);
  return ::cedar::Status::OK();
}

::cedar::Status EmbeddedRaftNode::InstallSnapshotToFollower(NodeId target) {
  // TODO: Implement snapshot installation
  (void)target;
  return ::cedar::Status::NotSupported("Snapshot not implemented");
}

InstallSnapshotResponse EmbeddedRaftNode::HandleInstallSnapshot(
    const InstallSnapshotRequest& req) {
  // TODO: Implement snapshot handling
  (void)req;
  InstallSnapshotResponse resp;
  resp.term = current_term_.load();
  resp.success = false;
  return resp;
}

void EmbeddedRaftNode::MaybeSnapshot() {
  // TODO: Check if snapshot is needed
}

}  // namespace raft
}  // namespace dtx
}  // namespace cedar
