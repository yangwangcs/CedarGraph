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
// Raft RPC Client for Storage Layer
// =============================================================================

#ifndef CEDAR_DTX_STORAGE_RAFT_RPC_CLIENT_H_
#define CEDAR_DTX_STORAGE_RAFT_RPC_CLIENT_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "raft_service.grpc.pb.h"

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// Raft RPC Client
// =============================================================================

// Connection pool entry for each peer
struct PeerConnection {
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<cedar::raft::internal::PartitionRaftService::Stub> stub;
  std::atomic<bool> healthy{true};
  std::chrono::steady_clock::time_point last_success;
  std::atomic<size_t> consecutive_failures{0};
};

class RaftRpcClient {
 public:
  struct Options {
    uint64_t rpc_timeout_ms = 1000;
    uint64_t max_retry_attempts = 3;
    uint64_t retry_backoff_ms = 100;
  };

  RaftRpcClient();
  explicit RaftRpcClient(const Options& options);
  ~RaftRpcClient();

  // Initialize with peer addresses
  Status Initialize(const std::unordered_map<NodeID, std::string>& peer_addresses);

  // Add or update a peer
  Status AddPeer(NodeID node_id, const std::string& address);

  // Remove a peer
  Status RemovePeer(NodeID node_id);

  // RPC methods with retry logic
  struct VoteResponse {
    uint64_t term{0};
    bool vote_granted{false};
    std::string voter_id;
  };

  struct AppendEntriesResponse {
    uint64_t term{0};
    bool success{false};
    uint64_t match_index{0};
    std::string follower_id;
  };

  struct HeartbeatResponse {
    uint64_t term{0};
    bool success{false};
  };

  struct SnapshotResponse {
    uint64_t term{0};
    bool success{false};
  };

  // Send RequestVote RPC
  StatusOr<VoteResponse> RequestVote(NodeID target,
                                     uint64_t term,
                                     uint32_t partition_id,
                                     uint64_t last_log_index,
                                     uint64_t last_log_term);

  // Send AppendEntries RPC
  StatusOr<AppendEntriesResponse> AppendEntries(
      NodeID target,
      uint64_t term,
      uint32_t partition_id,
      uint64_t prev_log_index,
      uint64_t prev_log_term,
      uint64_t leader_commit,
      const std::vector<cedar::raft::internal::LogEntry>& entries);

  // Send heartbeat (optimized AppendEntries without entries)
  StatusOr<HeartbeatResponse> Heartbeat(NodeID target,
                                         uint64_t term,
                                         uint32_t partition_id,
                                         uint64_t commit_index);

  // Send InstallSnapshot RPC
  StatusOr<SnapshotResponse> InstallSnapshot(NodeID target,
                                            uint64_t term,
                                            uint32_t partition_id,
                                            uint64_t last_included_index,
                                            uint64_t last_included_term,
                                            uint64_t offset,
                                            const std::string& data,
                                            bool done);

  // Check peer health
  bool IsPeerHealthy(NodeID node_id) const;

  // Get peer statistics
  struct PeerStats {
    bool healthy;
    size_t consecutive_failures;
    std::chrono::steady_clock::time_point last_success;
  };
  StatusOr<PeerStats> GetPeerStats(NodeID node_id) const;

  // Shutdown
  void Shutdown();

 private:
  PeerConnection* GetOrCreateConnection(NodeID node_id);
  StatusOr<PeerConnection*> GetConnection(NodeID node_id);

  Options options_;
  std::atomic<bool> running_{false};

  mutable std::mutex peers_mutex_;
  std::unordered_map<NodeID, std::string> peer_addresses_;
  std::unordered_map<NodeID, std::unique_ptr<PeerConnection>> connections_;

  std::shared_ptr<grpc::ChannelCredentials> credentials_;
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_RAFT_RPC_CLIENT_H_
