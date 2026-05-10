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
// Storage Layer Raft Replication
// =============================================================================
// Each partition has its own Raft group for strong consistency
// Leader handles writes, followers replicate log entries

#ifndef CEDAR_DTX_STORAGE_RAFT_REPLICATION_H_
#define CEDAR_DTX_STORAGE_RAFT_REPLICATION_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "raft_rpc_client.h"

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// Forward Declarations
// =============================================================================

class PartitionStorage;
class StorageRaftGroup;

// =============================================================================
// Replication Types
// =============================================================================

enum class ReplicaState : uint8_t {
  kFollower = 0,
  kCandidate = 1,
  kLeader = 2,
  kLearner = 3,      // 只读副本，不参与选举
};

enum class ReplicationStatus : uint8_t {
  kOK = 0,
  kNotLeader = 1,
  kTimeout = 2,
  kQuorumNotMet = 3,
  kNetworkError = 4,
};

inline std::string ReplicaStateToString(ReplicaState state) {
  switch (state) {
    case ReplicaState::kFollower: return "Follower";
    case ReplicaState::kCandidate: return "Candidate";
    case ReplicaState::kLeader: return "Leader";
    case ReplicaState::kLearner: return "Learner";
    default: return "Unknown";
  }
}

// =============================================================================
// Raft Log Entry for Storage Layer
// =============================================================================

struct StorageLogEntry {
  uint64_t term = 0;
  uint64_t index = 0;
  enum class Type : uint8_t {
    kPut = 0,
    kDelete = 1,
    kBatch = 2,
    kConfigChange = 3,
    kNoOp = 4,
  } type = Type::kNoOp;
  
  // For kPut/kDelete
  CedarKey key;
  std::optional<Descriptor> descriptor;
  Timestamp txn_version;
  
  // For kBatch
  std::vector<std::pair<CedarKey, Descriptor>> batch_data;
  
  // Serialization
  std::string Serialize() const;
  static StatusOr<StorageLogEntry> Deserialize(const std::string& data);
};

// =============================================================================
// Raft Configuration
// =============================================================================

struct RaftReplicaConfig {
  NodeID node_id;
  std::string address;
  bool is_learner = false;
};

struct RaftGroupConfig {
  PartitionID partition_id;
  std::string data_dir;
  uint64_t election_timeout_ms = 1000;
  uint64_t heartbeat_interval_ms = 100;
  uint64_t max_log_entries_per_append = 100;
  uint64_t snapshot_threshold = 10000;
  
  std::vector<RaftReplicaConfig> initial_peers;
};

// =============================================================================
// Replication Progress
// =============================================================================

struct ReplicaProgress {
  NodeID node_id;
  uint64_t match_index = 0;    // 已知的最高匹配索引
  uint64_t next_index = 1;     // 下一个要发送的索引
  bool is_healthy = true;
  std::chrono::steady_clock::time_point last_heartbeat;
  
  // In-flight requests (not atomic for simplicity, protected by mutex)
  uint64_t inflight_requests = 0;
  static constexpr uint64_t kMaxInflight = 100;
};

// =============================================================================
// Storage Raft Group
// =============================================================================

class StorageRaftGroup {
 public:
  explicit StorageRaftGroup(const RaftGroupConfig& config);
  ~StorageRaftGroup();
  
  // Lifecycle
  Status Initialize();
  Status Start();
  void Shutdown();
  bool IsRunning() const { return running_.load(); }
  
  // State
  ReplicaState GetState() const { return state_.load(); }
  bool IsLeader() const { return state_.load() == ReplicaState::kLeader; }
  uint64_t GetCurrentTerm() const { return current_term_.load(); }
  uint64_t GetCommitIndex() const { return commit_index_.load(); }
  uint64_t GetLastApplied() const { return last_applied_.load(); }
  NodeID GetLeaderId() const { return leader_id_.load(); }
  
  // Leader operations
  Status Propose(const StorageLogEntry& entry);
  Status ProposeBatch(const std::vector<StorageLogEntry>& entries);
  
  // Read index for linearizable reads
  StatusOr<uint64_t> ReadIndex();
  
  // Wait for applied
  Status WaitForApplied(uint64_t index, std::chrono::milliseconds timeout);
  
  // Configuration changes
  Status AddMember(const RaftReplicaConfig& config);
  Status RemoveMember(NodeID node_id);
  
  // Snapshot
  Status CreateSnapshot();
  Status InstallSnapshot(NodeID from_node, const std::string& snapshot_data);
  
  // RPC handlers (called by network layer)
  struct VoteRequest {
    uint64_t term;
    NodeID candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
  };
  struct VoteResponse {
    uint64_t term;
    bool vote_granted;
  };
  VoteResponse HandleVoteRequest(const VoteRequest& req);
  
  struct AppendEntriesRequest {
    uint64_t term;
    NodeID leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    std::vector<StorageLogEntry> entries;
    uint64_t leader_commit;
  };
  struct AppendEntriesResponse {
    uint64_t term;
    bool success;
    uint64_t match_index;
  };
  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& req);
  
  struct InstallSnapshotRequest {
    uint64_t term;
    NodeID leader_id;
    uint64_t last_included_index;
    uint64_t last_included_term;
    uint64_t offset;
    std::string data;
    bool done;
  };
  struct InstallSnapshotResponse {
    uint64_t term;
    bool success;
  };
  InstallSnapshotResponse HandleInstallSnapshot(const InstallSnapshotRequest& req);
  
  // Callbacks (set by PartitionStorage)
  using ApplyCallback = std::function<Status(uint64_t index, const StorageLogEntry& entry)>;
  using StateChangeCallback = std::function<void(ReplicaState old_state, ReplicaState new_state)>;
  
  void SetApplyCallback(ApplyCallback cb) { apply_callback_ = std::move(cb); }
  void SetStateChangeCallback(StateChangeCallback cb) { state_change_callback_ = std::move(cb); }

 private:
  // Raft state
  std::atomic<bool> running_{false};
  RaftGroupConfig config_;
  
  std::atomic<ReplicaState> state_{ReplicaState::kFollower};
  std::atomic<uint64_t> current_term_{0};
  NodeID voted_for_{0};
  std::atomic<NodeID> leader_id_{0};
  
  // Log
  mutable std::shared_mutex log_mutex_;
  std::vector<StorageLogEntry> log_;  // index 0 is placeholder
  
  // Commit state
  std::atomic<uint64_t> commit_index_{0};
  std::atomic<uint64_t> last_applied_{0};
  
  // Leader state
  std::unordered_map<NodeID, ReplicaProgress> progress_;
  mutable std::shared_mutex progress_mutex_;
  
  // Persistent state
  std::string log_file_path_;
  std::string state_file_path_;
  std::string snapshot_dir_;
  
  // Timers
  std::chrono::steady_clock::time_point last_election_reset_{};
  std::chrono::steady_clock::time_point last_heartbeat_sent_{};
  std::chrono::milliseconds election_timeout_{1000};
  
  // Threads
  std::unique_ptr<std::thread> raft_thread_;

  // Background RPC futures (heartbeat / vote / append entries)
  std::vector<std::future<void>> bg_futures_;
  std::mutex bg_future_mutex_;

  // Callbacks
  ApplyCallback apply_callback_;
  StateChangeCallback state_change_callback_;

  // Condition variable for commit notification
  std::mutex commit_cv_mutex_;
  std::condition_variable commit_cv_;

  // RPC client for peer communication
  std::unique_ptr<RaftRpcClient> rpc_client_;

  // Voting state
  std::atomic<int> votes_received_{0};
  std::set<NodeID> voters_;  // Track who voted for us
  std::mutex vote_mutex_;  // Mutex for voting operations
  
  // Private methods
  void RaftLoop();
  void BecomeFollower(uint64_t term);
  void BecomeCandidate();
  void BecomeLeader();
  void PruneBgFutures();
  
  Status PersistState();
  Status LoadState();
  Status AppendLog(const std::vector<StorageLogEntry>& entries);
  Status TruncateLog(uint64_t from_index);
  
  // Leader methods
  void SendHeartbeats();
  void ReplicateLogToPeer(NodeID peer_id);
  void HandleAppendEntriesResponse(NodeID peer_id, const AppendEntriesResponse& resp);
  void AdvanceCommitIndex();

  // Voting
  void SendVoteRequest(NodeID target);
  void SendVoteRequests();
  void HandleVoteResponse(NodeID peer_id, const VoteResponse& resp);
  
  // Apply committed entries
  void ApplyEntries();
  
  // Helpers
  uint64_t GetLastLogIndex() const;
  uint64_t GetLastLogTerm() const;
  bool IsLogUpToDate(uint64_t last_index, uint64_t last_term) const;
};

// =============================================================================
// Raft Storage Manager
// =============================================================================

class RaftStorageManager {
 public:
  RaftStorageManager();
  ~RaftStorageManager();
  
  Status Initialize(const std::string& base_data_dir);
  void Shutdown();
  
  // Create or get Raft group for partition
  StatusOr<StorageRaftGroup*> CreateRaftGroup(
      const RaftGroupConfig& config,
      StorageRaftGroup::ApplyCallback apply_cb,
      StorageRaftGroup::StateChangeCallback state_cb);
  
  StorageRaftGroup* GetRaftGroup(PartitionID pid);
  void RemoveRaftGroup(PartitionID pid);
  
  // Get all groups
  std::vector<PartitionID> GetAllPartitionIDs() const;
  
  // Statistics
  struct Stats {
    size_t num_groups = 0;
    size_t num_leaders = 0;
    size_t num_followers = 0;
    size_t num_candidates = 0;
  };
  Stats GetStats() const;

 private:
  std::string base_data_dir_;
  std::atomic<bool> initialized_{false};
  
  mutable std::shared_mutex groups_mutex_;
  std::unordered_map<PartitionID, std::unique_ptr<StorageRaftGroup>> groups_;
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_RAFT_REPLICATION_H_
