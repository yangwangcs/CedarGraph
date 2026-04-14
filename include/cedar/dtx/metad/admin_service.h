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
// Metadata Server Admin Service
// =============================================================================

#ifndef CEDAR_DTX_METAD_ADMIN_SERVICE_H_
#define CEDAR_DTX_METAD_ADMIN_SERVICE_H_

#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "cedar/dtx/raft/embedded_raft.h"
#include "cedar/dtx/raft/grpc_transport.h"
#include "metad_admin.grpc.pb.h"

namespace cedar {
namespace dtx {
namespace metad {

// Forward declaration
class MetadataService;

// =============================================================================
// Admin Service Implementation
// =============================================================================

class MetadAdminServiceImpl final : public MetadAdminService::Service {
 public:
  struct Options {
    // Allow follower reads
    bool enable_follower_read = true;
    // Staleness tolerance for follower reads (ms)
    uint64_t max_follower_staleness_ms = 1000;
    // Require leader for write operations
    bool require_leader_for_writes = true;
  };

  MetadAdminServiceImpl(raft::EmbeddedRaftNode* raft_node,
                        MetadataService* meta_service,
                        const Options& options);

  // Membership management
  grpc::Status AddNode(grpc::ServerContext* context,
                       const AddNodeRequest* request,
                       AddNodeResponse* response) override;

  grpc::Status RemoveNode(grpc::ServerContext* context,
                          const RemoveNodeRequest* request,
                          RemoveNodeResponse* response) override;

  // Cluster status
  grpc::Status GetClusterStatus(grpc::ServerContext* context,
                                const GetClusterStatusRequest* request,
                                GetClusterStatusResponse* response) override;

  grpc::Status GetNodeMetrics(grpc::ServerContext* context,
                              const GetNodeMetricsRequest* request,
                              GetNodeMetricsResponse* response) override;

  // Read-only query with follower read support
  grpc::Status Query(grpc::ServerContext* context,
                     const QueryRequest* request,
                     QueryResponse* response) override;

  // Update Raft node reference (for leadership changes)
  void SetRaftNode(raft::EmbeddedRaftNode* node);

 private:
  // Check if this node can serve read requests
  bool CanServeRead(bool require_leader, uint64_t min_index) const;
  
  // Check if this node can serve write requests
  bool CanServeWrite() const;
  
  // Get current node ID
  uint32_t GetNodeId() const;

  raft::EmbeddedRaftNode* raft_node_;
  MetadataService* meta_service_;
  Options options_;
  
  // Connection stats for metrics
  mutable std::mutex stats_mutex_;
  std::unordered_map<std::string, uint64_t> rpc_counters_;
};

// =============================================================================
// Cluster Manager for Membership Changes
// =============================================================================

class ClusterManager {
 public:
  ClusterManager(raft::EmbeddedRaftNode* raft_node,
                 raft::GrpcRaftTransport* transport);

  // Propose membership change (add node)
  Status ProposeAddNode(uint32_t node_id, const std::string& address);
  
  // Propose membership change (remove node)
  Status ProposeRemoveNode(uint32_t node_id);
  
  // Get current cluster configuration
  std::vector<std::pair<uint32_t, std::string>> GetClusterConfig() const;
  
  // Check if membership change is in progress
  bool IsMembershipChangeInProgress() const;

 private:
  raft::EmbeddedRaftNode* raft_node_;
  raft::GrpcRaftTransport* transport_;
  std::atomic<bool> membership_change_in_progress_{false};
};

}  // namespace metad
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_METAD_ADMIN_SERVICE_H_
