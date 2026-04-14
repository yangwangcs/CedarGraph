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

#include "cedar/dtx/metad/admin_service.h"

#include <chrono>
#include <iostream>

#include "cedar/governance/config_manager.h"

namespace cedar {
namespace dtx {
namespace metad {

// =============================================================================
// MetadAdminServiceImpl
// =============================================================================

// Static configuration manager for admin service
static governance::ConfigManager g_admin_config;
static std::once_flag g_config_initialized;

MetadAdminServiceImpl::MetadAdminServiceImpl(
    raft::EmbeddedRaftNode* raft_node,
    MetadataService* meta_service,
    const Options& options)
    : raft_node_(raft_node), meta_service_(meta_service), options_(options) {
  
  // Initialize configuration from governance layer
  std::call_once(g_config_initialized, []() {
    // Try to load configuration from standard locations
    const char* config_paths[] = {
      "/etc/cedar/config.yaml",
      "/opt/cedar/config/config.yaml",
      "./config/cedar.yaml",
      nullptr
    };
    
    for (const char** path = config_paths; *path != nullptr; ++path) {
      if (g_admin_config.LoadFromFile(*path).ok()) {
        std::cout << "[Admin] Loaded configuration from: " << *path << std::endl;
        break;
      }
    }
    
    // Apply environment variable overrides
    g_admin_config.ApplyEnvironmentOverrides();
  });
}

void MetadAdminServiceImpl::SetRaftNode(raft::EmbeddedRaftNode* node) {
  raft_node_ = node;
}

grpc::Status MetadAdminServiceImpl::AddNode(grpc::ServerContext* context,
                                            const AddNodeRequest* request,
                                            AddNodeResponse* response) {
  (void)context;
  
  if (!CanServeWrite()) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "Not leader - cannot process membership change");
  }

  uint32_t node_id = request->node_id();
  std::string address = request->address();

  std::cout << "[Admin] Adding node " << node_id << " at " << address << std::endl;

  // TODO(P0): Propose membership change through Raft
  // For now, just add to transport
  // This should be done through Raft log in production
  // Configuration: raft.timeout_ms = " 
  //   << g_admin_config.GetInt("dtx.raft.timeout_ms", 5000)

  response->set_success(true);
  auto* node_info = response->add_cluster_members();
  node_info->set_node_id(node_id);
  node_info->set_address(address);
  node_info->set_is_healthy(true);
  node_info->set_role("follower");

  return grpc::Status::OK;
}

grpc::Status MetadAdminServiceImpl::RemoveNode(grpc::ServerContext* context,
                                               const RemoveNodeRequest* request,
                                               RemoveNodeResponse* response) {
  (void)context;
  
  if (!CanServeWrite()) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "Not leader - cannot process membership change");
  }

  uint32_t node_id = request->node_id();
  bool force = request->force();

  std::cout << "[Admin] Removing node " << node_id << " (force=" << force << ")" << std::endl;

  // TODO(P0): Propose membership change through Raft
  // Configuration: raft.timeout_ms = "
  //   << g_admin_config.GetInt("dtx.raft.timeout_ms", 5000)

  response->set_success(true);
  return grpc::Status::OK;
}

grpc::Status MetadAdminServiceImpl::GetClusterStatus(grpc::ServerContext* context,
                                                     const GetClusterStatusRequest* request,
                                                     GetClusterStatusResponse* response) {
  (void)context;
  (void)request;

  if (!raft_node_) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Raft node not initialized");
  }

  auto status = raft_node_->GetStatus();
  
  response->set_cluster_id("cedar-metad-cluster");
  response->set_current_term(status.current_term);
  response->set_raft_state(raft::NodeStateToString(status.state));
  response->set_commit_index(status.commit_index);
  response->set_last_applied(status.last_applied);
  
  if (status.leader_id.has_value()) {
    response->set_leader_id(status.leader_id.value());
  }

  // Add self as a node
  auto* self = response->add_nodes();
  self->set_node_id(static_cast<uint32_t>(GetNodeId()));
  self->set_role(raft::NodeStateToString(status.state));
  self->set_is_healthy(true);
  self->set_last_contact_ms(0);

  return grpc::Status::OK;
}

grpc::Status MetadAdminServiceImpl::GetNodeMetrics(grpc::ServerContext* context,
                                                   const GetNodeMetricsRequest* request,
                                                   GetNodeMetricsResponse* response) {
  (void)context;
  
  if (!raft_node_) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Raft node not initialized");
  }

  auto status = raft_node_->GetStatus();
  
  auto* metrics = response->add_metrics();
  metrics->set_node_id(static_cast<uint32_t>(GetNodeId()));
  metrics->set_raft_term(status.current_term);
  metrics->set_raft_log_index(status.last_log_index);
  metrics->set_raft_commit_index(status.commit_index);
  metrics->set_raft_applied_index(status.last_applied);
  
  // RPC metrics would be populated from transport stats
  metrics->set_rpc_requests_total(0);
  metrics->set_rpc_requests_failed(0);
  metrics->set_rpc_latency_ms_avg(0.0);

  response->set_timestamp_ms(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());

  return grpc::Status::OK;
}

grpc::Status MetadAdminServiceImpl::Query(grpc::ServerContext* context,
                                          const QueryRequest* request,
                                          QueryResponse* response) {
  (void)context;

  bool require_leader = request->require_leader();
  uint64_t min_index = request->min_applied_index();

  if (!CanServeRead(require_leader, min_index)) {
    if (require_leader) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "Not leader - follower read not allowed for this query");
    } else {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "Follower is too far behind - retry or use leader");
    }
  }

  // Record read metadata
  if (raft_node_) {
    auto status = raft_node_->GetStatus();
    response->set_served_by_leader(status.state == raft::NodeState::kLeader);
    response->set_serving_node_id(GetNodeId());
    response->set_applied_index_at_read(status.last_applied);
  }

  // Handle different query types
  std::string query_type = request->query_type();
  
  if (query_type == "cluster_status") {
    response->set_success(true);
    response->set_result_data("Cluster is healthy");
  } else if (query_type == "node_list") {
    response->set_success(true);
    response->set_result_data("Node list would be returned here");
  } else {
    response->set_success(false);
    response->set_error_message("Unknown query type: " + query_type);
  }

  return grpc::Status::OK;
}

bool MetadAdminServiceImpl::CanServeRead(bool require_leader, uint64_t min_index) const {
  if (!raft_node_) return false;

  auto status = raft_node_->GetStatus();
  
  // Leader can always serve reads
  if (status.state == raft::NodeState::kLeader) {
    return true;
  }

  // If leader is required, followers cannot serve
  if (require_leader) {
    return false;
  }

  // Check if follower is too stale
  if (min_index > 0 && status.last_applied < min_index) {
    return false;
  }

  return options_.enable_follower_read;
}

bool MetadAdminServiceImpl::CanServeWrite() const {
  if (!raft_node_) return false;
  
  if (!options_.require_leader_for_writes) {
    return true;
  }
  
  auto status = raft_node_->GetStatus();
  return status.state == raft::NodeState::kLeader;
}

uint32_t MetadAdminServiceImpl::GetNodeId() const {
  // This would come from configuration
  // For now return 0 as unknown
  return 0;
}

// =============================================================================
// ClusterManager
// =============================================================================

ClusterManager::ClusterManager(raft::EmbeddedRaftNode* raft_node,
                               raft::GrpcRaftTransport* transport)
    : raft_node_(raft_node), transport_(transport) {}

Status ClusterManager::ProposeAddNode(uint32_t node_id, const std::string& address) {
  if (!raft_node_ || !raft_node_->IsLeader()) {
    return Status::NotLeader("Only leader can propose membership changes");
  }

  if (membership_change_in_progress_.exchange(true)) {
    return Status::Busy("Another membership change is in progress");
  }

  // TODO(P0): Create membership change log entry and propose to Raft
  // This should be done through the normal Raft propose mechanism
  // with a special command type for membership changes
  // Using configured timeout: " << g_admin_config.GetInt("dtx.raft.timeout_ms", 5000) << "ms

  std::cout << "[ClusterManager] Proposed add node " << node_id << " at " << address << std::endl;

  // For now, directly add to transport (in production, this should go through Raft)
  transport_->AddPeer(node_id, address);

  membership_change_in_progress_ = false;
  return Status::OK();
}

Status ClusterManager::ProposeRemoveNode(uint32_t node_id) {
  if (!raft_node_ || !raft_node_->IsLeader()) {
    return Status::NotLeader("Only leader can propose membership changes");
  }

  if (membership_change_in_progress_.exchange(true)) {
    return Status::Busy("Another membership change is in progress");
  }

  std::cout << "[ClusterManager] Proposed remove node " << node_id << std::endl;

  transport_->RemovePeer(node_id);

  membership_change_in_progress_ = false;
  return Status::OK();
}

std::vector<std::pair<uint32_t, std::string>> ClusterManager::GetClusterConfig() const {
  std::vector<std::pair<uint32_t, std::string>> result;
  
  if (!transport_) return result;
  
  auto peers = transport_->GetPeers();
  for (auto node_id : peers) {
    std::string addr = transport_->GetNodeAddress(node_id);
    result.push_back({static_cast<uint32_t>(node_id), addr});
  }
  
  return result;
}

bool ClusterManager::IsMembershipChangeInProgress() const {
  return membership_change_in_progress_.load();
}

}  // namespace metad
}  // namespace dtx
}  // namespace cedar
