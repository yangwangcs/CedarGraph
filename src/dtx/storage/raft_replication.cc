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

// =============================================================================
// Storage Layer Raft Replication Implementation
// =============================================================================

#include "cedar/dtx/storage/raft_replication.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>

#include "raft_service.grpc.pb.h"

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// StorageLogEntry Serialization
// =============================================================================

std::string StorageLogEntry::Serialize() const {
  std::string result;
  result.reserve(64);
  
  // Header: term(8) + index(8) + type(1)
  result.append(reinterpret_cast<const char*>(&term), sizeof(term));
  result.append(reinterpret_cast<const char*>(&index), sizeof(index));
  uint8_t type_val = static_cast<uint8_t>(type);
  result.append(reinterpret_cast<const char*>(&type_val), sizeof(type_val));
  
  // For data operations
  if (type == Type::kPut || type == Type::kDelete) {
    std::string key_data = key.ToString();
    uint32_t key_len = static_cast<uint32_t>(key_data.size());
    result.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    result.append(key_data);
    
    if (descriptor.has_value()) {
      uint64_t desc_raw = descriptor->AsRaw();
      result.append(reinterpret_cast<const char*>(&desc_raw), sizeof(desc_raw));
    } else {
      uint64_t zero = 0;
      result.append(reinterpret_cast<const char*>(&zero), sizeof(zero));
    }
    
    uint64_t ts = txn_version.value();
    result.append(reinterpret_cast<const char*>(&ts), sizeof(ts));
  } else if (type == Type::kBatch) {
    uint32_t count = static_cast<uint32_t>(batch_data.size());
    result.append(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& [k, d] : batch_data) {
      std::string key_data = k.ToString();
      uint32_t key_len = static_cast<uint32_t>(key_data.size());
      result.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
      result.append(key_data);
      uint64_t desc_raw = d.AsRaw();
      result.append(reinterpret_cast<const char*>(&desc_raw), sizeof(desc_raw));
    }
  }
  
  return result;
}

StatusOr<StorageLogEntry> StorageLogEntry::Deserialize(const std::string& data) {
  if (data.size() < 17) {
    return Status::InvalidArgument("StorageLogEntry", "data too short");
  }
  
  StorageLogEntry entry;
  size_t pos = 0;
  
  std::memcpy(&entry.term, data.data() + pos, sizeof(entry.term));
  pos += sizeof(entry.term);
  
  std::memcpy(&entry.index, data.data() + pos, sizeof(entry.index));
  pos += sizeof(entry.index);
  
  uint8_t type_val;
  std::memcpy(&type_val, data.data() + pos, sizeof(type_val));
  entry.type = static_cast<Type>(type_val);
  pos += sizeof(type_val);
  
  if (entry.type == Type::kPut || entry.type == Type::kDelete) {
    if (pos + sizeof(uint32_t) > data.size()) {
      return Status::InvalidArgument("StorageLogEntry", "truncated key length");
    }
    uint32_t key_len;
    std::memcpy(&key_len, data.data() + pos, sizeof(key_len));
    pos += sizeof(key_len);
    
    if (pos + key_len > data.size()) {
      return Status::InvalidArgument("StorageLogEntry", "truncated key data");
    }
    std::string key_data(data.data() + pos, key_len);
    // TODO: entry.key = CedarKey::FromString(key_data);
    pos += key_len;
    
    if (pos + sizeof(uint64_t) > data.size()) {
      return Status::InvalidArgument("StorageLogEntry", "truncated descriptor");
    }
    uint64_t desc_raw;
    std::memcpy(&desc_raw, data.data() + pos, sizeof(desc_raw));
    if (desc_raw != 0) {
      entry.descriptor = Descriptor(desc_raw);
    }
    pos += sizeof(desc_raw);
    
    if (pos + sizeof(uint64_t) > data.size()) {
      return Status::InvalidArgument("StorageLogEntry", "truncated timestamp");
    }
    uint64_t ts;
    std::memcpy(&ts, data.data() + pos, sizeof(ts));
    entry.txn_version = Timestamp(ts);
  }
  
  return entry;
}

// =============================================================================
// StorageRaftGroup Implementation
// =============================================================================

StorageRaftGroup::StorageRaftGroup(const RaftGroupConfig& config)
    : config_(config),
      log_file_path_(config.data_dir + "/raft.log"),
      state_file_path_(config.data_dir + "/raft.state"),
      snapshot_dir_(config.data_dir + "/snapshots") {
  // Add placeholder at index 0
  log_.emplace_back();
}

StorageRaftGroup::~StorageRaftGroup() {
  Shutdown();
}

Status StorageRaftGroup::Initialize() {
  // Create directories
  std::filesystem::create_directories(config_.data_dir);
  std::filesystem::create_directories(snapshot_dir_);

  // Initialize RPC client for peer communication
  RaftRpcClient::Options rpc_options;
  rpc_options.rpc_timeout_ms = config_.election_timeout_ms;
  rpc_client_ = std::make_unique<RaftRpcClient>(rpc_options);

  // Build peer address map
  std::unordered_map<NodeID, std::string> peer_addresses;
  for (const auto& peer : config_.initial_peers) {
    if (peer.node_id != config_.partition_id) {  // Don't add self
      peer_addresses[peer.node_id] = peer.address;
    }
  }

  auto status = rpc_client_->Initialize(peer_addresses);
  if (!status.ok()) {
    return status;
  }

  // Load persistent state
  status = LoadState();
  if (!status.ok()) {
    return status;
  }

  // Initialize follower progress
  for (const auto& peer : config_.initial_peers) {
    if (peer.node_id != config_.partition_id) {  // Don't track self
      ReplicaProgress progress;
      progress.node_id = peer.node_id;
      progress.next_index = GetLastLogIndex() + 1;
      progress_.emplace(peer.node_id, progress);
    }
  }

  return Status::OK();
}

Status StorageRaftGroup::Start() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("StorageRaftGroup", "already started");
  }
  
  last_election_reset_ = std::chrono::steady_clock::now();
  raft_thread_ = std::make_unique<std::thread>(&StorageRaftGroup::RaftLoop, this);
  
  return Status::OK();
}

void StorageRaftGroup::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  commit_cv_.notify_all();
  
  if (raft_thread_ && raft_thread_->joinable()) {
    raft_thread_->join();
  }
}

void StorageRaftGroup::RaftLoop() {
  while (running_.load()) {
    auto now = std::chrono::steady_clock::now();
    auto state = state_.load();

    if (state == ReplicaState::kLeader) {
      // Leader: send heartbeats
      if (now - last_heartbeat_sent_ >=
          std::chrono::milliseconds(config_.heartbeat_interval_ms)) {
        SendHeartbeats();
        last_heartbeat_sent_ = now;
      }
      AdvanceCommitIndex();
    } else {
      // Follower/Candidate: check election timeout
      if (now - last_election_reset_ >= election_timeout_) {
        if (state == ReplicaState::kFollower) {
          BecomeCandidate();
        } else {
          // Candidate timeout, start new election
          BecomeCandidate();
        }
      }
    }

    // Apply committed entries
    ApplyEntries();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void StorageRaftGroup::BecomeFollower(uint64_t term) {
  auto old_state = state_.exchange(ReplicaState::kFollower);
  current_term_.store(term);
  voted_for_ = 0;
  
  if (old_state != ReplicaState::kFollower && state_change_callback_) {
    state_change_callback_(old_state, ReplicaState::kFollower);
  }
  
  // Randomize election timeout once per role transition
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(
      config_.election_timeout_ms,
      config_.election_timeout_ms + config_.election_timeout_ms / 2);
  election_timeout_ = std::chrono::milliseconds(dis(gen));
  
  last_election_reset_ = std::chrono::steady_clock::now();
  PersistState();
}

void StorageRaftGroup::BecomeCandidate() {
  auto old_state = state_.exchange(ReplicaState::kCandidate);
  current_term_.fetch_add(1);
  voted_for_ = config_.partition_id;  // Vote for self
  votes_received_.store(1);           // Count self vote
  voters_.clear();
  voters_.insert(config_.partition_id);

  if (state_change_callback_) {
    state_change_callback_(old_state, ReplicaState::kCandidate);
  }

  // Randomize election timeout once per role transition
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(
      config_.election_timeout_ms,
      config_.election_timeout_ms + config_.election_timeout_ms / 2);
  election_timeout_ = std::chrono::milliseconds(dis(gen));

  last_election_reset_ = std::chrono::steady_clock::now();
  PersistState();

  // Send vote requests to all peers
  SendVoteRequests();
}

void StorageRaftGroup::BecomeLeader() {
  auto old_state = state_.exchange(ReplicaState::kLeader);

  // Reset progress for each follower and initialize next_index
  std::unique_lock<std::shared_mutex> lock(progress_mutex_);
  for (auto& [node_id, progress] : progress_) {
    progress.next_index = GetLastLogIndex() + 1;
    progress.match_index = 0;
  }
  lock.unlock();

  if (state_change_callback_) {
    state_change_callback_(old_state, ReplicaState::kLeader);
  }

  last_heartbeat_sent_ = std::chrono::steady_clock::now();

  // Send initial heartbeat immediately
  SendHeartbeats();
}

StorageRaftGroup::VoteResponse StorageRaftGroup::HandleVoteRequest(
    const VoteRequest& req) {
  VoteResponse resp;
  resp.term = current_term_.load();
  resp.vote_granted = false;
  
  if (req.term < current_term_.load()) {
    return resp;
  }
  
  if (req.term > current_term_.load()) {
    BecomeFollower(req.term);
  }
  
  resp.term = current_term_.load();
  
  // Check if we can vote for this candidate
  if ((voted_for_ == 0 || voted_for_ == req.candidate_id) &&
      IsLogUpToDate(req.last_log_index, req.last_log_term)) {
    resp.vote_granted = true;
    voted_for_ = req.candidate_id;
    last_election_reset_ = std::chrono::steady_clock::now();
    PersistState();
  }
  
  return resp;
}

StorageRaftGroup::AppendEntriesResponse StorageRaftGroup::HandleAppendEntries(
    const AppendEntriesRequest& req) {
  AppendEntriesResponse resp;
  resp.term = current_term_.load();
  resp.success = false;
  resp.match_index = 0;
  
  if (req.term < current_term_.load()) {
    return resp;
  }
  
  // Reset election timer
  last_election_reset_ = std::chrono::steady_clock::now();
  
  if (req.term > current_term_.load()) {
    BecomeFollower(req.term);
  } else if (state_.load() == ReplicaState::kCandidate) {
    BecomeFollower(req.term);
  }
  
  resp.term = current_term_.load();
  
  // Check log consistency
  std::unique_lock<std::shared_mutex> lock(log_mutex_);
  if (req.prev_log_index > 0) {
    if (req.prev_log_index >= log_.size()) {
      // Log doesn't have prev_log_index
      resp.match_index = GetLastLogIndex();
      return resp;
    }
    if (log_[req.prev_log_index].term != req.prev_log_term) {
      // Term mismatch
      resp.match_index = req.prev_log_index - 1;
      return resp;
    }
  }
  
  // Append new entries
  if (!req.entries.empty()) {
    // Truncate conflicting entries
    for (size_t i = 0; i < req.entries.size(); ++i) {
      uint64_t idx = req.prev_log_index + 1 + i;
      if (idx < log_.size()) {
        if (log_[idx].term != req.entries[i].term) {
          // Conflict found, truncate from here
          log_.resize(idx);
          break;
        }
      } else {
        break;
      }
    }
    
    // Append new entries
    for (const auto& entry : req.entries) {
      uint64_t idx = entry.index;
      if (idx >= log_.size()) {
        log_.push_back(entry);
      }
    }
    
    // Persist log
    AppendLog(req.entries);
  }
  
  // Update commit index
  if (req.leader_commit > commit_index_.load()) {
    commit_index_.store(std::min(req.leader_commit, GetLastLogIndex()));
    commit_cv_.notify_all();
  }
  
  resp.success = true;
  resp.match_index = req.prev_log_index + req.entries.size();
  return resp;
}

void StorageRaftGroup::ApplyEntries() {
  uint64_t last_applied = last_applied_.load();
  uint64_t commit_index = commit_index_.load();
  
  while (last_applied < commit_index) {
    uint64_t next_index = last_applied + 1;
    
    StorageLogEntry entry;
    {
      std::shared_lock<std::shared_mutex> lock(log_mutex_);
      if (next_index >= log_.size()) {
        break;
      }
      entry = log_[next_index];
    }
    
    // Apply entry
    if (apply_callback_) {
      auto status = apply_callback_(next_index, entry);
      if (!status.ok()) {
        // Log error but continue
        // TODO: handle apply error
      }
    }
    
    last_applied_.store(next_index);
    last_applied = next_index;
  }
}

Status StorageRaftGroup::Propose(const StorageLogEntry& entry) {
  if (state_.load() != ReplicaState::kLeader) {
    return Status::NotLeader("Not leader");
  }
  
  StorageLogEntry new_entry = entry;
  new_entry.term = current_term_.load();
  new_entry.index = GetLastLogIndex() + 1;
  
  // Append to local log
  {
    std::unique_lock<std::shared_mutex> lock(log_mutex_);
    log_.push_back(new_entry);
  }
  
  // Persist
  auto status = AppendLog({new_entry});
  if (!status.ok()) {
    return status;
  }
  
  // Update leader progress
  std::unique_lock<std::shared_mutex> lock(progress_mutex_);
  auto it = progress_.find(config_.partition_id);
  if (it != progress_.end()) {
    it->second.match_index = new_entry.index;
    it->second.next_index = new_entry.index + 1;
  }
  lock.unlock();
  
  return Status::OK();
}

Status StorageRaftGroup::ProposeBatch(const std::vector<StorageLogEntry>& entries) {
  if (entries.empty()) {
    return Status::OK();
  }
  if (state_.load() != ReplicaState::kLeader) {
    return Status::NotLeader("Not leader");
  }

  StorageLogEntry batch_entry;
  batch_entry.type = StorageLogEntry::Type::kBatch;
  batch_entry.batch_data.reserve(entries.size());
  for (const auto& e : entries) {
    batch_entry.batch_data.push_back({e.key, e.descriptor.value_or(Descriptor())});
  }

  return Propose(batch_entry);
}

Status StorageRaftGroup::WaitForApplied(uint64_t index, 
                                        std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(commit_cv_mutex_);
  auto deadline = std::chrono::steady_clock::now() + timeout;
  
  while (last_applied_.load() < index && running_.load()) {
    if (commit_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
      return Status::IOError("WaitForApplied timeout");
    }
  }
  
  return last_applied_.load() >= index ? Status::OK() 
                                       : Status::IOError("Raft stopped");
}

Status StorageRaftGroup::PersistState() {
  std::ofstream file(state_file_path_, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Cannot open state file");
  }
  
  uint64_t term = current_term_.load();
  NodeID voted = voted_for_;
  
  file.write(reinterpret_cast<const char*>(&term), sizeof(term));
  file.write(reinterpret_cast<const char*>(&voted), sizeof(voted));
  
  file.flush();
  return file.good() ? Status::OK() : Status::IOError("Failed to persist state");
}

Status StorageRaftGroup::LoadState() {
  if (!std::filesystem::exists(state_file_path_)) {
    return Status::OK();  // Fresh start
  }
  
  std::ifstream file(state_file_path_, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Cannot open state file");
  }
  
  uint64_t term;
  NodeID voted;
  
  file.read(reinterpret_cast<char*>(&term), sizeof(term));
  file.read(reinterpret_cast<char*>(&voted), sizeof(voted));
  
  if (file.good()) {
    current_term_.store(term);
    voted_for_ = voted;
  }
  
  return Status::OK();
}

Status StorageRaftGroup::AppendLog(const std::vector<StorageLogEntry>& entries) {
  std::ofstream file(log_file_path_, std::ios::binary | std::ios::app);
  if (!file.is_open()) {
    return Status::IOError("Cannot open log file");
  }
  
  for (const auto& entry : entries) {
    std::string data = entry.Serialize();
    uint32_t len = static_cast<uint32_t>(data.size());
    file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file.write(data.data(), len);
  }
  
  file.flush();
  // fsync to ensure durability
  sync();
  return file.good() ? Status::OK() : Status::IOError("Failed to append log");
}

uint64_t StorageRaftGroup::GetLastLogIndex() const {
  std::shared_lock<std::shared_mutex> lock(log_mutex_);
  return log_.empty() ? 0 : log_.size() - 1;
}

uint64_t StorageRaftGroup::GetLastLogTerm() const {
  std::shared_lock<std::shared_mutex> lock(log_mutex_);
  return log_.empty() ? 0 : log_.back().term;
}

bool StorageRaftGroup::IsLogUpToDate(uint64_t last_index, uint64_t last_term) const {
  uint64_t my_last_term = GetLastLogTerm();
  uint64_t my_last_index = GetLastLogIndex();
  
  if (last_term != my_last_term) {
    return last_term > my_last_term;
  }
  return last_index >= my_last_index;
}

void StorageRaftGroup::SendHeartbeats() {
  if (!rpc_client_) {
    return;
  }

  uint64_t term = current_term_.load();
  uint64_t commit_index = commit_index_.load();
  uint64_t last_log_index = GetLastLogIndex();

  std::shared_lock<std::shared_mutex> lock(progress_mutex_);
  for (const auto& [node_id, progress] : progress_) {
    // Send heartbeat via RPC (async, don't wait)
    std::thread([this, node_id, term, commit_index]() {
      auto resp = rpc_client_->Heartbeat(
          node_id, term, config_.partition_id, commit_index);
      if (resp.ok()) {
        // Update last heartbeat time
        std::unique_lock<std::shared_mutex> lock(progress_mutex_);
        auto it = progress_.find(node_id);
        if (it != progress_.end()) {
          it->second.last_heartbeat = std::chrono::steady_clock::now();
        }
      }
    }).detach();
  }
}

void StorageRaftGroup::ReplicateLogToPeer(NodeID peer_id) {
  if (state_.load() != ReplicaState::kLeader) {
    return;
  }

  if (!rpc_client_) {
    return;
  }

  ReplicaProgress* progress = nullptr;
  {
    std::shared_lock<std::shared_mutex> lock(progress_mutex_);
    auto it = progress_.find(peer_id);
    if (it == progress_.end()) {
      return;
    }
    progress = &it->second;
  }

  // Get entries to send
  std::vector<StorageLogEntry> entries_to_send;
  uint64_t prev_log_index = 0;
  uint64_t prev_log_term = 0;

  {
    std::shared_lock<std::shared_mutex> lock(log_mutex_);
    prev_log_index = progress->next_index > 0 ? progress->next_index - 1 : 0;

    if (prev_log_index > 0 && prev_log_index < log_.size()) {
      prev_log_term = log_[prev_log_index].term;
    }

    for (uint64_t i = progress->next_index; i < log_.size(); ++i) {
      entries_to_send.push_back(log_[i]);
      if (entries_to_send.size() >= config_.max_log_entries_per_append) {
        break;
      }
    }
  }

  if (entries_to_send.empty()) {
    return;  // Nothing to replicate
  }

  // Convert to proto format
  std::vector<cedar::raft::internal::LogEntry> proto_entries;
  for (const auto& entry : entries_to_send) {
    cedar::raft::internal::LogEntry proto_entry;
    proto_entry.set_term(entry.term);
    proto_entry.set_index(entry.index);
    proto_entry.set_command(entry.Serialize());
    proto_entries.push_back(std::move(proto_entry));
  }

  // Send AppendEntries RPC
  std::thread([this, peer_id, prev_log_index, prev_log_term, proto_entries = std::move(proto_entries)]() mutable {
    auto resp = rpc_client_->AppendEntries(
        peer_id,
        current_term_.load(),
        config_.partition_id,
        prev_log_index,
        prev_log_term,
        commit_index_.load(),
        proto_entries);

    if (resp.ok()) {
      // Convert RaftRpcClient::AppendEntriesResponse to local type
      AppendEntriesResponse local_resp;
      local_resp.term = resp.ValueOrDie().term;
      local_resp.success = resp.ValueOrDie().success;
      local_resp.match_index = resp.ValueOrDie().match_index;
      HandleAppendEntriesResponse(peer_id, local_resp);
    }
  }).detach();
}

void StorageRaftGroup::HandleAppendEntriesResponse(NodeID peer_id,
                                                  const AppendEntriesResponse& resp) {
  // Check for term update
  if (resp.term > current_term_.load()) {
    BecomeFollower(resp.term);
    return;
  }

  if (state_.load() != ReplicaState::kLeader) {
    return;
  }

  std::unique_lock<std::shared_mutex> lock(progress_mutex_);
  auto it = progress_.find(peer_id);
  if (it == progress_.end()) {
    return;
  }

  auto& progress = it->second;

  if (resp.success) {
    // Update match_index and next_index
    uint64_t new_match_index = resp.match_index > 0 ? resp.match_index :
        progress.next_index + resp.match_index;
    progress.match_index = std::max(progress.match_index, new_match_index);
    progress.next_index = progress.match_index + 1;
    progress.last_heartbeat = std::chrono::steady_clock::now();
    progress.is_healthy = true;

    // Unlock before advancing commit
    lock.unlock();
    AdvanceCommitIndex();
  } else {
    // Decrement next_index and retry
    if (progress.next_index > 1) {
      progress.next_index--;
    }
    progress.is_healthy = false;
  }
}

void StorageRaftGroup::SendVoteRequests() {
  if (!rpc_client_) {
    return;
  }

  uint64_t term = current_term_.load();
  uint64_t last_log_index = GetLastLogIndex();
  uint64_t last_log_term = GetLastLogTerm();

  std::shared_lock<std::shared_mutex> lock(progress_mutex_);
  for (const auto& [node_id, _] : progress_) {
    std::thread([this, node_id, term, last_log_index, last_log_term]() {
      auto resp = rpc_client_->RequestVote(
          node_id, term, config_.partition_id, last_log_index, last_log_term);

      if (resp.ok()) {
        // Convert RaftRpcClient::VoteResponse to local type
        VoteResponse local_resp;
        local_resp.term = resp.ValueOrDie().term;
        local_resp.vote_granted = resp.ValueOrDie().vote_granted;
        HandleVoteResponse(node_id, local_resp);
      }
    }).detach();
  }
}

void StorageRaftGroup::HandleVoteResponse(NodeID peer_id, const VoteResponse& resp) {
  // Check for term update
  if (resp.term > current_term_.load()) {
    BecomeFollower(resp.term);
    return;
  }

  if (state_.load() != ReplicaState::kCandidate) {
    return;
  }

  if (resp.term != current_term_.load()) {
    return;  // Response from previous term
  }

  if (resp.vote_granted) {
    std::lock_guard<std::mutex> lock(vote_mutex_);
    if (voters_.insert(peer_id).second) {
      votes_received_.fetch_add(1);

      // Check if we have quorum
      size_t total_nodes = config_.initial_peers.size();
      size_t quorum = total_nodes / 2 + 1;

      if (votes_received_.load() >= quorum) {
        BecomeLeader();
      }
    }
  }
}

void StorageRaftGroup::AdvanceCommitIndex() {
  uint64_t current_commit = commit_index_.load();
  uint64_t new_commit = current_commit;
  
  // Find the highest index that is replicated to majority
  std::vector<uint64_t> match_indices;
  {
    std::shared_lock<std::shared_mutex> lock(progress_mutex_);
    for (const auto& [node_id, progress] : progress_) {
      match_indices.push_back(progress.match_index);
    }
  }
  match_indices.push_back(GetLastLogIndex());  // Leader's own match index
  
  std::sort(match_indices.begin(), match_indices.end(), std::greater<uint64_t>());
  
  size_t quorum = (match_indices.size() / 2) + 1;
  if (quorum <= match_indices.size()) {
    uint64_t candidate = match_indices[quorum - 1];
    if (candidate > current_commit) {
      // Check term
      std::shared_lock<std::shared_mutex> lock(log_mutex_);
      if (candidate < log_.size() && log_[candidate].term == current_term_.load()) {
        new_commit = candidate;
      }
    }
  }
  
  if (new_commit > current_commit) {
    commit_index_.store(new_commit);
    commit_cv_.notify_all();
  }
}

// =============================================================================
// RaftStorageManager Implementation
// =============================================================================

RaftStorageManager::RaftStorageManager() = default;

RaftStorageManager::~RaftStorageManager() {
  Shutdown();
}

Status RaftStorageManager::Initialize(const std::string& base_data_dir) {
  base_data_dir_ = base_data_dir;
  std::filesystem::create_directories(base_data_dir_);
  initialized_.store(true);
  return Status::OK();
}

void RaftStorageManager::Shutdown() {
  if (!initialized_.exchange(false)) {
    return;
  }
  
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  for (auto& [pid, group] : groups_) {
    group->Shutdown();
  }
  groups_.clear();
}

StatusOr<StorageRaftGroup*> RaftStorageManager::CreateRaftGroup(
    const RaftGroupConfig& config,
    StorageRaftGroup::ApplyCallback apply_cb,
    StorageRaftGroup::StateChangeCallback state_cb) {
  if (!initialized_.load()) {
    return Status::InvalidArgument("RaftStorageManager", "not initialized");
  }
  
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  
  if (groups_.count(config.partition_id) > 0) {
    return Status::InvalidArgument("Raft group already exists");
  }
  
  auto group = std::make_unique<StorageRaftGroup>(config);
  group->SetApplyCallback(std::move(apply_cb));
  group->SetStateChangeCallback(std::move(state_cb));
  
  auto status = group->Initialize();
  if (!status.ok()) {
    return status;
  }
  
  auto* ptr = group.get();
  groups_[config.partition_id] = std::move(group);
  
  // Start the group
  status = ptr->Start();
  if (!status.ok()) {
    groups_.erase(config.partition_id);
    return status;
  }
  
  return ptr;
}

StorageRaftGroup* RaftStorageManager::GetRaftGroup(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  auto it = groups_.find(pid);
  return it != groups_.end() ? it->second.get() : nullptr;
}

void RaftStorageManager::RemoveRaftGroup(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  auto it = groups_.find(pid);
  if (it != groups_.end()) {
    it->second->Shutdown();
    groups_.erase(it);
  }
}

std::vector<PartitionID> RaftStorageManager::GetAllPartitionIDs() const {
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  std::vector<PartitionID> result;
  for (const auto& [pid, _] : groups_) {
    result.push_back(pid);
  }
  return result;
}

RaftStorageManager::Stats RaftStorageManager::GetStats() const {
  Stats stats;
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  stats.num_groups = groups_.size();
  for (const auto& [pid, group] : groups_) {
    switch (group->GetState()) {
      case ReplicaState::kLeader:
        stats.num_leaders++;
        break;
      case ReplicaState::kFollower:
        stats.num_followers++;
        break;
      case ReplicaState::kCandidate:
        stats.num_candidates++;
        break;
      default:
        break;
    }
  }
  return stats;
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
