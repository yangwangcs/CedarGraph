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

#include "src/raft/partition_raft_service.h"

#include <algorithm>
#include <chrono>

#include "src/raft/partition_log_store.h"

namespace cedar {
namespace raft {

PartitionRaftServiceImpl::PartitionRaftServiceImpl() {}

PartitionRaftServiceImpl::~PartitionRaftServiceImpl() {
  // Stop all batch committers
  for (auto& [pid, committer] : batch_committers_) {
    committer->Stop();
  }
  // All partition states will be cleaned up automatically
}

Status PartitionRaftServiceImpl::InitializePartition(uint32_t partition_id,
                                                      const std::string& data_dir) {
  std::lock_guard<std::mutex> lock(partitions_mutex_);

  auto it = partitions_.find(partition_id);
  if (it != partitions_.end()) {
    return Status::OK();  // Already initialized
  }

  auto state = std::make_shared<PartitionState>(partition_id);
  state->log_store = std::make_unique<PartitionLogStore>(partition_id, data_dir);

  auto status = state->log_store->Initialize();
  if (!status.ok()) {
    return status;
  }

  // Load persisted metadata
  uint64_t saved_term = 0;
  std::string saved_voted_for;
  auto load_status = state->log_store->LoadMetadata(&saved_term, &saved_voted_for);
  if (load_status.ok()) {
    state->current_term = saved_term;
    state->voted_for = saved_voted_for;
  }

  state->commit_index = state->log_store->GetCommittedIndex();

  // Initialize per-partition batch committer
  if (batch_committers_.find(partition_id) == batch_committers_.end()) {
    BatchCommitConfig batch_config;
    batch_config.max_batch_size = 100;
    batch_config.max_wait_ms = 5;
    auto committer = std::make_unique<BatchLogCommitter>(partition_id, batch_config);
    auto batch_status = committer->Start();
    if (!batch_status.ok()) {
      return batch_status;
    }
    committer->SetLogStore(state->log_store.get());
    batch_committers_[partition_id] = std::move(committer);
  }

  partitions_[partition_id] = std::move(state);
  return Status::OK();
}

bool PartitionRaftServiceImpl::IsPartitionInitialized(uint32_t partition_id) const {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  return partitions_.find(partition_id) != partitions_.end();
}

bool PartitionRaftServiceImpl::IsPartitionLeader(uint32_t partition_id) const {
  auto state = GetPartitionState(partition_id);
  if (!state) {
    return false;
  }
  std::lock_guard<std::mutex> lock(state->mutex);
  return state->current_role == RaftRole::kLeader;
}

StatusOr<std::string> PartitionRaftServiceImpl::GetPartitionLeader(uint32_t partition_id) const {
  auto state = GetPartitionState(partition_id);
  if (!state) {
    return Status::NotFound("Partition not found: " + std::to_string(partition_id));
  }
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->leader_id.empty()) {
    return Status::NotFound("No leader elected for partition " + std::to_string(partition_id));
  }
  return state->leader_id;
}

StatusOr<uint64_t> PartitionRaftServiceImpl::GetPartitionTerm(uint32_t partition_id) const {
  auto state = GetPartitionState(partition_id);
  if (!state) {
    return Status::NotFound("Partition not found: " + std::to_string(partition_id));
  }
  std::lock_guard<std::mutex> lock(state->mutex);
  return state->current_term;
}

std::shared_ptr<PartitionState> PartitionRaftServiceImpl::GetPartitionState(uint32_t partition_id) {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  auto it = partitions_.find(partition_id);
  if (it != partitions_.end()) {
    return it->second;
  }
  return nullptr;
}

std::shared_ptr<PartitionState> PartitionRaftServiceImpl::GetPartitionState(
    uint32_t partition_id) const {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  auto it = partitions_.find(partition_id);
  if (it != partitions_.end()) {
    return it->second;
  }
  return nullptr;
}

void PartitionRaftServiceImpl::BecomeFollower(std::shared_ptr<PartitionState> state,
                                               uint64_t term) {
  state->current_role = RaftRole::kFollower;
  state->current_term = term;
  state->voted_for.clear();
  // Persist metadata
  state->log_store->SaveMetadata(state->current_term, state->voted_for);
}

void PartitionRaftServiceImpl::BecomeLeader(std::shared_ptr<PartitionState> state) {
  state->current_role = RaftRole::kLeader;
  state->leader_id = "self";  // Should be current node ID in actual implementation
}

void PartitionRaftServiceImpl::UpdateTerm(std::shared_ptr<PartitionState> state,
                                           uint64_t new_term) {
  if (new_term > state->current_term) {
    state->current_term = new_term;
    state->voted_for.clear();
    state->current_role = RaftRole::kFollower;
    // Persist metadata
    state->log_store->SaveMetadata(state->current_term, state->voted_for);
  }
}

bool PartitionRaftServiceImpl::IsLogUpToDate(std::shared_ptr<PartitionState> state,
                                              uint64_t candidate_last_log_index,
                                              uint64_t candidate_last_log_term) const {
  uint64_t local_last_log_index = state->log_store->GetLastLogIndex();
  uint64_t local_last_log_term = state->log_store->GetLastLogTerm();

  // Raft determines which of two logs is more up-to-date by comparing the index
  // and term of the last entries. If the logs have last entries with different terms,
  // then the log with the later term is more up-to-date. If the logs end with the
  // same term, then whichever log is longer is more up-to-date.
  if (candidate_last_log_term != local_last_log_term) {
    return candidate_last_log_term >= local_last_log_term;
  }
  return candidate_last_log_index >= local_last_log_index;
}

::grpc::Status PartitionRaftServiceImpl::RequestVote(
    ::grpc::ServerContext* context,
    const ::cedar::raft::internal::VoteRequest* request,
    ::cedar::raft::internal::VoteResponse* response) {
  (void)context;

  uint32_t partition_id = request->partition_id();
  auto state = GetPartitionState(partition_id);
  if (!state) {
    response->set_term(0);
    response->set_vote_granted(false);
    return ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                          "Partition not initialized: " + std::to_string(partition_id));
  }

  std::lock_guard<std::mutex> lock(state->mutex);

  uint64_t request_term = request->term();
  const std::string& candidate_id = request->candidate_id();
  uint64_t candidate_last_log_index = request->last_log_index();
  uint64_t candidate_last_log_term = request->last_log_term();

  // Set response term to current term (may have been updated)
  response->set_term(state->current_term);

  // If request term < current term, reject vote
  if (request_term < state->current_term) {
    response->set_vote_granted(false);
    return ::grpc::Status::OK;
  }

  // If request term > current term, update term, become follower, reset voted_for
  if (request_term > state->current_term) {
    UpdateTerm(state, request_term);
    response->set_term(state->current_term);
  }

  // Grant vote if:
  // 1. voted_for is empty or matches candidate, AND
  // 2. candidate's log is at least as up-to-date as ours
  bool can_vote = (state->voted_for.empty() || state->voted_for == candidate_id);
  bool log_is_current = IsLogUpToDate(state, candidate_last_log_index, candidate_last_log_term);

  if (can_vote && log_is_current) {
    state->voted_for = candidate_id;
    response->set_vote_granted(true);
    // Persist metadata after voting
    state->log_store->SaveMetadata(state->current_term, state->voted_for);
  } else {
    response->set_vote_granted(false);
  }

  return ::grpc::Status::OK;
}

::grpc::Status PartitionRaftServiceImpl::AppendEntries(
    ::grpc::ServerContext* context,
    const ::cedar::raft::internal::AppendEntriesRequest* request,
    ::cedar::raft::internal::AppendEntriesResponse* response) {
  (void)context;

  uint32_t partition_id = request->partition_id();
  auto state = GetPartitionState(partition_id);
  if (!state) {
    response->set_term(0);
    response->set_success(false);
    return ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                          "Partition not initialized: " + std::to_string(partition_id));
  }

  std::lock_guard<std::mutex> lock(state->mutex);

  uint64_t request_term = request->term();
  const std::string& leader_id = request->leader_id();
  uint64_t prev_log_index = request->prev_log_index();
  uint64_t prev_log_term = request->prev_log_term();
  uint64_t leader_commit = request->leader_commit();

  // Set response term to current term
  response->set_term(state->current_term);

  // If request term < current term, reject
  if (request_term < state->current_term) {
    response->set_success(false);
    return ::grpc::Status::OK;
  }

  // If request term >= current term, update term and become follower
  if (request_term >= state->current_term) {
    if (request_term > state->current_term) {
      UpdateTerm(state, request_term);
    }
    if (state->current_role != RaftRole::kFollower) {
      BecomeFollower(state, request_term);
    }
    state->leader_id = leader_id;
    response->set_term(state->current_term);
  }

  // Check prev_log_index and prev_log_term match
  if (prev_log_index > 0) {
    auto entry_result = state->log_store->GetEntry(prev_log_index);
    if (!entry_result.ok()) {
      // Log doesn't have entry at prev_log_index
      response->set_success(false);
      return ::grpc::Status::OK;
    }
    const auto& entry = entry_result.ValueOrDie();
    if (entry.term() != prev_log_term) {
      // Term mismatch at prev_log_index
      response->set_success(false);
      return ::grpc::Status::OK;
    }
  }

  // Truncate conflicting entries and append new ones
  uint64_t current_index = prev_log_index + 1;
  for (int i = 0; i < request->entries_size(); ++i) {
    const auto& new_entry = request->entries(i);

    // Check if there's an existing entry at this index
    auto existing_result = state->log_store->GetEntry(current_index);
    if (existing_result.ok()) {
      const auto& existing_entry = existing_result.ValueOrDie();
      if (existing_entry.term() != new_entry.term()) {
        // Conflict found: truncate from here and append new entry
        auto truncate_status = state->log_store->TruncateFrom(current_index);
        if (!truncate_status.ok()) {
          response->set_success(false);
          return ::grpc::Status::OK;
        }
        // Append the new entry
        auto append_status = state->log_store->AppendEntry(new_entry);
        if (!append_status.ok()) {
          response->set_success(false);
          return ::grpc::Status::OK;
        }
      }
      // If terms match, entry is already the same, skip
    } else {
      // No existing entry, just append
      auto append_status = state->log_store->AppendEntry(new_entry);
      if (!append_status.ok()) {
        response->set_success(false);
        return ::grpc::Status::OK;
      }
    }
    current_index++;
  }

  // Update commit_index if leader_commit > commit_index
  if (leader_commit > state->commit_index) {
    uint64_t last_new_index = prev_log_index + request->entries_size();
    state->commit_index = std::min(leader_commit, last_new_index);
    state->log_store->SetCommittedIndex(state->commit_index);

    // Apply committed entries
    auto apply_status = ApplyCommittedEntries(state);
    if (!apply_status.ok()) {
      // Log but don't fail the AppendEntries
    }
  }

  response->set_success(true);
  return ::grpc::Status::OK;
}

::grpc::Status PartitionRaftServiceImpl::InstallSnapshot(
    ::grpc::ServerContext* context,
    const ::cedar::raft::internal::SnapshotRequest* request,
    ::cedar::raft::internal::SnapshotResponse* response) {
  (void)context;

  uint32_t partition_id = request->partition_id();
  auto state = GetPartitionState(partition_id);
  if (!state) {
    response->set_term(0);
    return ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                          "Partition not initialized: " + std::to_string(partition_id));
  }

  std::lock_guard<std::mutex> lock(state->mutex);

  uint64_t request_term = request->term();

  // If request term < current term, reject
  if (request_term < state->current_term) {
    response->set_term(state->current_term);
    return ::grpc::Status::OK;
  }

  // Update term and become follower if needed
  if (request_term > state->current_term) {
    UpdateTerm(state, request_term);
  }
  if (state->current_role != RaftRole::kFollower) {
    BecomeFollower(state, request_term);
  }

  // In a full implementation, we would:
  // 1. Save snapshot data to disk
  // 2. Truncate log up to the snapshot index
  // 3. Update commit_index and last_applied
  // For now, we just acknowledge the snapshot

  response->set_term(state->current_term);
  return ::grpc::Status::OK;
}

::grpc::Status PartitionRaftServiceImpl::SendHeartbeat(
    ::grpc::ServerContext* context,
    const ::cedar::raft::internal::HeartbeatRequest* request,
    ::cedar::raft::internal::HeartbeatResponse* response) {
  (void)context;

  uint32_t partition_id = request->partition_id();
  auto state = GetPartitionState(partition_id);
  if (!state) {
    response->set_term(0);
    response->set_success(false);
    return ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                          "Partition not initialized: " + std::to_string(partition_id));
  }

  std::lock_guard<std::mutex> lock(state->mutex);

  uint64_t request_term = request->term();

  // If request term < current term, reject
  if (request_term < state->current_term) {
    response->set_term(state->current_term);
    response->set_success(false);
    return ::grpc::Status::OK;
  }

  // If request term >= current term, update term and become follower
  if (request_term >= state->current_term) {
    if (request_term > state->current_term) {
      UpdateTerm(state, request_term);
    }
    if (state->current_role != RaftRole::kFollower) {
      BecomeFollower(state, request_term);
    }
    state->leader_id = request->leader_id();
  }

  response->set_term(state->current_term);
  response->set_success(true);
  return ::grpc::Status::OK;
}

Status PartitionRaftServiceImpl::ApplyCommittedEntries(std::shared_ptr<PartitionState> state) {
  // Apply entries from last_applied + 1 to commit_index
  while (state->last_applied < state->commit_index) {
    uint64_t apply_index = state->last_applied + 1;
    auto entry_result = state->log_store->GetEntry(apply_index);
    if (!entry_result.ok()) {
      return entry_result.status();
    }

    // In a full implementation, apply the entry to the state machine here
    // For now, we just track that it was applied

    state->last_applied = apply_index;
  }
  return Status::OK();
}

}  // namespace raft
}  // namespace cedar
