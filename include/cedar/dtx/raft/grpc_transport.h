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
// gRPC Transport for Embedded Raft
// =============================================================================

#ifndef CEDAR_DTX_RAFT_GRPC_TRANSPORT_H_
#define CEDAR_DTX_RAFT_GRPC_TRANSPORT_H_

#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

#include "cedar/core/status.h"
#include "cedar/dtx/raft/embedded_raft.h"
#include "cedar/dtx/raft/grpc_tls.h"

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// gRPC Transport Client
// =============================================================================

class GrpcRaftTransport : public RaftTransport {
 public:
  struct Options {
    // Connection timeout
    uint64_t rpc_timeout_ms = 1000;
    // Connection pool size per peer
    size_t max_channels_per_peer = 4;
    // TLS configuration
    TlsConfig tls_config;
  };

  GrpcRaftTransport(const Options& options, 
                    const std::vector<std::pair<NodeId, std::string>>& peers);
  ~GrpcRaftTransport() override;

  // RaftTransport interface
  Status SendVoteRequest(NodeId target, const VoteRequest& req,
                         VoteResponse* resp) override;
  Status SendAppendEntries(NodeId target, const AppendEntriesRequest& req,
                           AppendEntriesResponse* resp) override;
  Status SendInstallSnapshot(NodeId target, const InstallSnapshotRequest& req,
                             InstallSnapshotResponse* resp) override;

  std::vector<NodeId> GetPeers() const override;
  std::string GetNodeAddress(NodeId id) const override;

  // Dynamic membership management
  Status AddPeer(NodeId id, const std::string& address);
  Status RemovePeer(NodeId id);
  
  // Update TLS configuration (for certificate rotation)
  void UpdateTlsConfig(const TlsConfig& config);
  
  // Connection health check
  bool IsPeerHealthy(NodeId id) const;
  
  // Get connection statistics
  struct ConnectionStats {
    size_t total_channels;
    size_t active_channels;
    size_t failed_requests;
    size_t successful_requests;
  };
  ConnectionStats GetStats(NodeId id) const;

 private:
  struct PeerChannel;
  struct PeerState;

  PeerChannel* GetChannel(NodeId target);
  std::string GetPeerAddress(NodeId id) const;
  void RefreshChannels(NodeId id);

  Options options_;
  std::shared_ptr<grpc::ChannelCredentials> credentials_;

  // Peer address map
  mutable std::mutex peers_mutex_;
  std::unordered_map<NodeId, std::string> peer_addresses_;

  // Connection pool and stats
  mutable std::mutex channels_mutex_;
  std::unordered_map<NodeId, std::unique_ptr<PeerState>> peers_;
};

}  // namespace raft
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_RAFT_GRPC_TRANSPORT_H_
