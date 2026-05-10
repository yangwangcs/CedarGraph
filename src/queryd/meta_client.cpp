// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Meta Client Implementation

#include "cedar/queryd/meta_client.h"

#include <chrono>
#include <thread>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

// Protobuf generated headers
#include "meta_service.pb.h"
#include "meta_service.grpc.pb.h"

namespace cedar {
namespace queryd {

using namespace std::chrono;

// ============================================================================
// GraphSchema
// ============================================================================

const LabelSchema* GraphSchema::GetNodeLabel(const std::string& name) const {
  auto it = node_labels.find(name);
  if (it != node_labels.end()) {
    return &it->second;
  }
  return nullptr;
}

const LabelSchema* GraphSchema::GetEdgeType(const std::string& name) const {
  auto it = edge_types.find(name);
  if (it != edge_types.end()) {
    return &it->second;
  }
  return nullptr;
}

// ============================================================================
// ClusterState
// ============================================================================

const PartitionInfo* ClusterState::GetPartition(uint32_t partition_id) const {
  for (const auto& p : partitions) {
    if (p.partition_id == partition_id) {
      return &p;
    }
  }
  return nullptr;
}

uint32_t ClusterState::GetPartitionForEntity(uint64_t entity_id) const {
  if (partition_count == 0) {
    return 0;
  }
  return static_cast<uint32_t>(entity_id % partition_count);
}

// ============================================================================
// MetaClient
// ============================================================================

QueryMetaClient::QueryMetaClient(const Options& options) : options_(options) {}

QueryMetaClient::~QueryMetaClient() {
  running_ = false;
  if (refresh_thread_.joinable()) {
    refresh_thread_.join();
  }
}

Status QueryMetaClient::Init() {
  // Create gRPC channel
  channel_ = grpc::CreateChannel(
      options_.meta_service_address,
      grpc::InsecureChannelCredentials());
  
  meta_stub_ = cedar::meta::MetaService::NewStub(channel_);
  
  // Wait for connection (gRPC API compatibility: skip WaitForConnected)
  (void)options_.rpc_timeout;
  
  // Initial fetch
  Status s = FetchSchemaFromMeta(&cached_schema_);
  if (!s.ok()) {
    return s;
  }
  
  s = FetchClusterStateFromMeta(&cached_cluster_state_);
  if (!s.ok()) {
    return s;
  }
  
  // Start background refresh
  running_ = true;
  refresh_thread_ = std::thread(&QueryMetaClient::RefreshLoop, this);
  
  return Status::OK();
}

Status QueryMetaClient::GetSchema(GraphSchema* schema) {
  if (options_.enable_cache) {
    std::shared_lock<std::shared_mutex> lock(schema_mutex_);
    *schema = cached_schema_;
    return Status::OK();
  }
  return FetchSchemaFromMeta(schema);
}

Status QueryMetaClient::RefreshSchema() {
  return FetchSchemaFromMeta(&cached_schema_);
}

Status QueryMetaClient::GetClusterState(ClusterState* state) {
  if (options_.enable_cache) {
    std::shared_lock<std::shared_mutex> lock(cluster_mutex_);
    *state = cached_cluster_state_;
    return Status::OK();
  }
  return FetchClusterStateFromMeta(state);
}

Status QueryMetaClient::RefreshClusterState() {
  return FetchClusterStateFromMeta(&cached_cluster_state_);
}

Status QueryMetaClient::GetPartitionForEntity(uint64_t entity_id, 
                                         PartitionInfo* info) {
  std::shared_lock<std::shared_mutex> lock(cluster_mutex_);
  
  uint32_t partition_id = cached_cluster_state_.GetPartitionForEntity(entity_id);
  const auto* p = cached_cluster_state_.GetPartition(partition_id);
  
  if (p) {
    *info = *p;
    return Status::OK();
  }
  
  return Status::NotFound("Partition not found");
}

Status QueryMetaClient::GetStorageNode(uint32_t partition_id, std::string* address) {
  std::shared_lock<std::shared_mutex> lock(cluster_mutex_);
  
  const auto* p = cached_cluster_state_.GetPartition(partition_id);
  if (p) {
    *address = p->leader_address;
    return Status::OK();
  }
  
  return Status::NotFound("Partition not found");
}

Status QueryMetaClient::WatchClusterChanges(
    std::function<void(const ClusterState&)> callback) {
  
  // In production, this would use gRPC streaming
  // For now, poll periodically
  uint32_t last_version = 0;
  
  while (running_) {
    {
      std::shared_lock<std::shared_mutex> lock(cluster_mutex_);
      if (cached_cluster_state_.version != last_version) {
        last_version = cached_cluster_state_.version;
        callback(cached_cluster_state_);
      }
    }
    
    std::this_thread::sleep_for(seconds(1));
  }
  
  return Status::OK();
}

Status QueryMetaClient::RegisterQueryD(const std::string& listen_address) {
  (void)listen_address;
  // Stubbed: RegisterQueryD RPC removed from meta_service.proto
  return Status::OK();
}

Status QueryMetaClient::Heartbeat(uint32_t active_queries,
                                  uint32_t queued_queries) {
  (void)active_queries;
  (void)queued_queries;
  // Stubbed: QueryDHeartbeat RPC removed from meta_service.proto
  return Status::OK();
}

const GraphSchema* QueryMetaClient::GetCachedSchema() const {
  std::shared_lock<std::shared_mutex> lock(schema_mutex_);
  return &cached_schema_;
}

const ClusterState* QueryMetaClient::GetCachedClusterState() const {
  std::shared_lock<std::shared_mutex> lock(cluster_mutex_);
  return &cached_cluster_state_;
}

void QueryMetaClient::RefreshLoop() {
  while (running_) {
    std::this_thread::sleep_for(options_.refresh_interval);

    if (!running_) break;

    FetchSchemaFromMeta(&cached_schema_);
    {
      std::unique_lock<std::shared_mutex> lock(cluster_mutex_);
      FetchClusterStateFromMeta(&cached_cluster_state_);
    }
  }
}

Status QueryMetaClient::FetchSchemaFromMeta(GraphSchema* schema) {
  (void)schema;
  // Stubbed: GetSchema RPC removed from meta_service.proto
  return Status::OK();
}
Status QueryMetaClient::FetchClusterStateFromMeta(ClusterState* state) {
  if (!channel_) {
    return Status::IOError("Channel not initialized");
  }

  auto stub = cedar::meta::MetaService::NewStub(channel_);
  grpc::ClientContext context;
  cedar::meta::GetSpacePartitionMapRequest request;
  cedar::meta::GetSpacePartitionMapResponse response;

  request.set_space_name("default");
  context.set_deadline(std::chrono::system_clock::now() + options_.rpc_timeout);

  grpc::Status status = stub->GetSpacePartitionMap(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("GetSpacePartitionMap failed: " + status.error_message());
  }
  if (!response.success()) {
    return Status::IOError("GetSpacePartitionMap rejected: " + response.error_msg());
  }

  state->Clear();

  // Build node address lookup from alive nodes
  cedar::meta::GetAliveNodesRequest alive_req;
  cedar::meta::GetAliveNodesResponse alive_resp;
  grpc::ClientContext alive_ctx;
  alive_ctx.set_deadline(std::chrono::system_clock::now() + options_.rpc_timeout);
  grpc::Status alive_status = stub->GetAliveNodes(&alive_ctx, alive_req, &alive_resp);

  std::unordered_map<uint32_t, std::string> node_addresses;
  if (alive_status.ok() && alive_resp.success()) {
    for (const auto& node : alive_resp.nodes()) {
      node_addresses[node.node_id()] = node.address();
    }
  }

  for (const auto& kv : response.partition_map().assignments()) {
    const auto& assign = kv.second;
    PartitionInfo info;
    info.partition_id = assign.partition_id();
    auto it = node_addresses.find(assign.leader_node());
    if (it != node_addresses.end()) {
      info.leader_address = it->second;
    } else {
      info.leader_address = "127.0.0.1:" + std::to_string(9779 + assign.leader_node() % 3);
    }
    info.is_healthy = true;
    for (uint32_t follower : assign.follower_nodes()) {
      auto fit = node_addresses.find(follower);
      if (fit != node_addresses.end()) {
        info.follower_addresses.push_back(fit->second);
      }
    }
    state->partitions.push_back(std::move(info));
  }

  for (const auto& node : alive_resp.nodes()) {
    StorageNode sn;
    sn.node_id = node.node_id();
    sn.address = node.address();
    sn.is_healthy = node.state() == "ONLINE";
    state->nodes.push_back(sn);
  }

  return Status::OK();
}

// ============================================================================
// SchemaCache
// ============================================================================

SchemaCache::SchemaCache(size_t max_entries) : max_entries_(max_entries) {}

std::string SchemaCache::GetPlan(const std::string& query_pattern) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_.find(query_pattern);
  if (it != cache_.end()) {
    return it->second;
  }
  
  return "";
}

void SchemaCache::CachePlan(const std::string& query_pattern, 
                            const std::string& plan) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (cache_.size() >= max_entries_) {
    // Evict oldest (simplified)
    cache_.erase(cache_.begin());
  }
  
  cache_[query_pattern] = plan;
}

void SchemaCache::InvalidateAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_.clear();
}

size_t SchemaCache::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return cache_.size();
}

}  // namespace queryd
}  // namespace cedar
