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
// Embedded Raft - Simplified Raft Implementation for CedarGraph
// =============================================================================
// A lightweight, self-contained Raft consensus implementation.
// No external dependencies - uses standard C++ only.
// Based on the Raft paper: https://raft.github.io/raft.pdf
// =============================================================================

#ifndef CEDAR_DTX_RAFT_EMBEDDED_RAFT_H_
#define CEDAR_DTX_RAFT_EMBEDDED_RAFT_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/raft/raft_interface.h"

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// Core Types (aliases to raft_interface.h types)
// =============================================================================

using Term = LogTerm;
using LogIndex = ::cedar::dtx::raft::LogIndex;
using NodeId = ::cedar::dtx::NodeID;

enum class NodeState : uint8_t {
  kFollower = 0,
  kCandidate = 1,
  kLeader = 2,
};

inline std::string NodeStateToString(NodeState state) {
  switch (state) {
    case NodeState::kFollower: return "Follower";
    case NodeState::kCandidate: return "Candidate";
    case NodeState::kLeader: return "Leader";
    default: return "Unknown";
  }
}

// =============================================================================
// Raft Message Types
// =============================================================================

struct VoteRequest {
  Term term;
  NodeId candidate_id;
  LogIndex last_log_index;
  Term last_log_term;
};

struct VoteResponse {
  Term term;
  bool vote_granted;
};

struct AppendEntriesRequest {
  Term term;
  NodeId leader_id;
  LogIndex prev_log_index;
  Term prev_log_term;
  std::vector<::cedar::dtx::raft::LogEntry> entries;
  LogIndex leader_commit;
};

struct AppendEntriesResponse {
  Term term;
  bool success;
  LogIndex match_index;  // For optimization
};

struct SnapshotMetadata {
  Term last_included_term;
  LogIndex last_included_index;
  std::string data;
};

struct InstallSnapshotRequest {
  Term term;
  NodeId leader_id;
  SnapshotMetadata snapshot;
  uint32_t offset;  // For chunking large snapshots
  bool done;
};

struct InstallSnapshotResponse {
  Term term;
  bool success;
};

// =============================================================================
// Network Transport Interface
// =============================================================================

class RaftTransport {
 public:
  virtual ~RaftTransport() = default;
  
  // RPC methods
  virtual Status SendVoteRequest(NodeId target, const VoteRequest& req,
                                  VoteResponse* resp) = 0;
  virtual Status SendAppendEntries(NodeId target, const AppendEntriesRequest& req,
                                    AppendEntriesResponse* resp) = 0;
  virtual Status SendInstallSnapshot(NodeId target, const InstallSnapshotRequest& req,
                                      InstallSnapshotResponse* resp) = 0;
  
  // Get all peer nodes
  virtual std::vector<NodeId> GetPeers() const = 0;
  virtual std::string GetNodeAddress(NodeId id) const = 0;
};

// =============================================================================
// Embedded State Machine Interface
// =============================================================================

class EmbeddedStateMachine {
 public:
  virtual ~EmbeddedStateMachine() = default;
  
  // Apply committed log entry
  virtual void Apply(const ::cedar::dtx::raft::LogEntry& entry) = 0;
  
  // Get last applied index
  virtual LogIndex GetLastAppliedIndex() const = 0;
  
  // Create snapshot
  virtual Status CreateSnapshot(LogIndex up_to_index, std::string* data) = 0;
  
  // Restore from snapshot
  virtual Status RestoreSnapshot(const std::string& data) = 0;
  
  // Called when node becomes leader
  virtual void OnBecomeLeader(Term term) {}
  
  // Called when node steps down
  virtual void OnStepDown(Term term) {}
};

// =============================================================================
// Embedded State Machine Adapter
// =============================================================================
// Wraps raft::StateMachine (from raft_interface.h) for use with EmbeddedRaftNode

class StateMachineAdapter : public EmbeddedStateMachine {
 public:
  explicit StateMachineAdapter(::cedar::dtx::raft::StateMachine* inner) 
      : inner_(inner), last_applied_(0) {}
  
  void Apply(const ::cedar::dtx::raft::LogEntry& entry) override {
    if (inner_) {
      inner_->Apply(entry);
      last_applied_ = entry.index;
    }
  }
  
  LogIndex GetLastAppliedIndex() const override {
    return last_applied_;
  }
  
  Status CreateSnapshot(LogIndex up_to_index, std::string* data) override {
    (void)up_to_index;
    if (!inner_) return ::cedar::Status::OK();
    auto snapshot = inner_->CreateSnapshot();
    *data = snapshot.data;
    return ::cedar::Status::OK();
  }
  
  Status RestoreSnapshot(const std::string& data) override {
    if (!inner_) return ::cedar::Status::OK();
    ::cedar::dtx::raft::Snapshot snapshot;
    snapshot.data = data;
    return inner_->RestoreSnapshot(snapshot);
  }
  
  void OnBecomeLeader(Term term) override {
    (void)term;
    // raft::StateMachine doesn't have this callback
  }
  
  void OnStepDown(Term term) override {
    (void)term;
    // raft::StateMachine doesn't have this callback
  }

 private:
  ::cedar::dtx::raft::StateMachine* inner_;
  LogIndex last_applied_;
};

// =============================================================================
// Storage Interface
// =============================================================================

class RaftStorage {
 public:
  virtual ~RaftStorage() = default;
  
  // Log operations
  virtual Status AppendLog(const std::vector<::cedar::dtx::raft::LogEntry>& entries) = 0;
  virtual Status TruncateLog(LogIndex from_index) = 0;
  virtual StatusOr<::cedar::dtx::raft::LogEntry> GetLogEntry(LogIndex index) = 0;
  virtual StatusOr<std::vector<::cedar::dtx::raft::LogEntry>> GetLogEntries(LogIndex start, LogIndex end) = 0;
  virtual LogIndex GetLastLogIndex() const = 0;
  virtual Term GetLastLogTerm() const = 0;
  
  // State operations
  virtual Status SaveState(Term current_term, NodeId voted_for) = 0;
  virtual Status LoadState(Term* current_term, NodeId* voted_for) = 0;
  
  // Snapshot operations
  virtual Status SaveSnapshot(const SnapshotMetadata& snapshot) = 0;
  virtual StatusOr<SnapshotMetadata> LoadSnapshot() = 0;
};

// =============================================================================
// Embedded Raft Node
// =============================================================================

class EmbeddedRaftNode {
 public:
  struct Options {
    NodeId node_id;
    std::vector<std::pair<NodeId, std::string>> peers;  // id -> address
    std::string data_dir;
    
    // Timing parameters (in milliseconds)
    uint64_t election_timeout_min = 150;
    uint64_t election_timeout_max = 300;
    uint64_t heartbeat_interval = 50;
    
    // Snapshot
    uint64_t snapshot_threshold = 10000;  // Log entries
  };
  
  EmbeddedRaftNode(const Options& options,
                   RaftTransport* transport,
                   EmbeddedStateMachine* state_machine,
                   RaftStorage* storage);
  ~EmbeddedRaftNode();
  
  // Lifecycle
  Status Start();
  void Shutdown();
  
  // Propose a new entry (only works if leader)
  Status Propose(const std::string& data);
  
  // Check if this node is the leader
  bool IsLeader() const { return state_ == NodeState::kLeader; }
  
  // Get current leader (if known)
  std::optional<NodeId> GetLeaderId() const;
  
  // Get current term
  Term GetCurrentTerm() const { return current_term_.load(); }
  
  // Get commit index
  LogIndex GetCommitIndex() const { return commit_index_.load(); }
  
  // Get status info
  struct NodeStatus {
    NodeState state;
    Term current_term;
    NodeId voted_for;
    LogIndex commit_index;
    LogIndex last_log_index;
    LogIndex last_applied;
    std::optional<NodeId> leader_id;
  };
  NodeStatus GetStatus() const;
  
  // Manual trigger snapshot
  Status TriggerSnapshot();
  
  // Add/remove peers (membership change)
  Status AddPeer(NodeId id, const std::string& address);
  Status RemovePeer(NodeId id);
  
  // Handle incoming requests (called by transport layer)
  VoteResponse HandleVoteRequest(const VoteRequest& req);
  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& req);
  InstallSnapshotResponse HandleInstallSnapshot(const InstallSnapshotRequest& req);

 private:
  // Main loop
  void RunEventLoop();
  
  // State machine transitions
  void BecomeFollower(Term term);
  void BecomeCandidate();
  void BecomeLeader();
  
  // Election
  void StartElection();
  bool RequestVote(NodeId target, const VoteRequest& req, VoteResponse* resp);
  void ProcessVoteResponse(const VoteResponse& resp);
  
  // Log replication
  void SendHeartbeats();
  void ReplicateLog(NodeId target);
  bool SendAppendEntries(NodeId target, const AppendEntriesRequest& req,
                          AppendEntriesResponse* resp);
  void ProcessAppendEntriesResponse(NodeId from, const AppendEntriesResponse& resp);
  
  // Advance commit index
  void AdvanceCommitIndex();
  
  // Apply committed entries to state machine
  void ApplyCommittedEntries();
  
  // Snapshot
  void MaybeSnapshot();
  Status InstallSnapshotToFollower(NodeId target);
  
  // Random election timeout
  std::chrono::milliseconds GetRandomElectionTimeout() const;
  
  // Options
  Options options_;
  
  // Dependencies
  RaftTransport* transport_;
  EmbeddedStateMachine* state_machine_;
  RaftStorage* storage_;
  
  // State
  std::atomic<NodeState> state_{NodeState::kFollower};
  std::atomic<Term> current_term_{0};
  std::atomic<NodeId> voted_for_{0};
  
  // Log state
  std::vector<::cedar::dtx::raft::LogEntry> log_;  // In-memory log (starts from index 1)
  std::atomic<LogIndex> commit_index_{0};
  std::atomic<LogIndex> last_applied_{0};
  
  // Leader state (only valid when state_ == kLeader)
  struct LeaderState {
    std::vector<LogIndex> next_index;   // For each peer
    std::vector<LogIndex> match_index;  // For each peer
  };
  std::optional<LeaderState> leader_state_;
  std::optional<NodeId> leader_id_;  // Known leader (for followers)
  
  // Timing
  std::chrono::steady_clock::time_point last_election_reset_;
  std::chrono::steady_clock::time_point last_heartbeat_;
  
  // Threading
  std::atomic<bool> running_{false};
  std::thread event_loop_thread_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  
  // Peer management
  std::unordered_map<NodeId, std::string> peers_;
  mutable std::shared_mutex peers_mutex_;
};

// =============================================================================
// File-based Storage Implementation
// =============================================================================

class FileRaftStorage : public RaftStorage {
 public:
  explicit FileRaftStorage(const std::string& data_dir);
  ~FileRaftStorage() override;
  
  Status Initialize();
  
  // RaftStorage interface
  Status AppendLog(const std::vector<::cedar::dtx::raft::LogEntry>& entries) override;
  Status TruncateLog(LogIndex from_index) override;
  StatusOr<::cedar::dtx::raft::LogEntry> GetLogEntry(LogIndex index) override;
  StatusOr<std::vector<::cedar::dtx::raft::LogEntry>> GetLogEntries(LogIndex start, LogIndex end) override;
  LogIndex GetLastLogIndex() const override;
  Term GetLastLogTerm() const override;
  
  Status SaveState(Term current_term, NodeId voted_for) override;
  Status LoadState(Term* current_term, NodeId* voted_for) override;
  
  Status SaveSnapshot(const SnapshotMetadata& snapshot) override;
  StatusOr<SnapshotMetadata> LoadSnapshot() override;
  
 private:
  std::string data_dir_;
  std::string log_file_;
  std::string state_file_;
  std::string snapshot_file_;
  
  mutable std::mutex mutex_;
  LogIndex last_log_index_ = 0;
  Term last_log_term_ = 0;
};

}  // namespace raft
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_RAFT_EMBEDDED_RAFT_H_
