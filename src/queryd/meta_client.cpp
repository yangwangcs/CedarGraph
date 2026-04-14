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
  
  // Wait for connection (gRPC API compatibility: skip WaitForConnected)
  (void)options_.rpc_timeout;
  // auto deadline = steady_clock::now() + options_.rpc_timeout;
  // if (!channel_->WaitForConnected(deadline)) {
  //   return Status::IOError("Failed to connect to meta service");
  // }
  
  // Initial fetch
  Status s = FetchSchemaFromMeta();
  if (!s.ok()) {
    return s;
  }
  
  s = FetchClusterStateFromMeta();
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
  return FetchSchemaFromMeta();
}

Status QueryMetaClient::RefreshSchema() {
  return FetchSchemaFromMeta();
}

Status QueryMetaClient::GetClusterState(ClusterState* state) {
  if (options_.enable_cache) {
    std::shared_lock<std::shared_mutex> lock(cluster_mutex_);
    *state = cached_cluster_state_;
    return Status::OK();
  }
  return FetchClusterStateFromMeta();
}

Status QueryMetaClient::RefreshClusterState() {
  return FetchClusterStateFromMeta();
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
  // TODO: Implement with correct proto types when meta service API stabilizes
  return Status::OK();
}

Status QueryMetaClient::Heartbeat(uint32_t active_queries, 
                             uint32_t queued_queries) {
  (void)active_queries;
  (void)queued_queries;
  // TODO: Implement with correct proto types when meta service API stabilizes
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
    
    FetchSchemaFromMeta();
    FetchClusterStateFromMeta();
  }
}

Status QueryMetaClient::FetchSchemaFromMeta() {
  // TODO: Use actual gRPC call
  // For now, return cached schema
  
  // Simulated schema
  GraphSchema schema;
  
  // Add sample node labels
  LabelSchema person;
  person.name = "Person";
  person.is_node = true;
  person.properties.push_back({"id", "STRING", false, true, ""});
  person.properties.push_back({"name", "STRING", false, false, ""});
  person.properties.push_back({"age", "INT", true, false, "0"});
  schema.node_labels["Person"] = std::move(person);
  
  LabelSchema company;
  company.name = "Company";
  company.is_node = true;
  company.properties.push_back({"id", "STRING", false, true, ""});
  company.properties.push_back({"name", "STRING", false, false, ""});
  schema.node_labels["Company"] = std::move(company);
  
  // Add sample edge types
  LabelSchema works_for;
  works_for.name = "WORKS_FOR";
  works_for.is_node = false;
  works_for.properties.push_back({"since", "STRING", true, false, ""});
  schema.edge_types["WORKS_FOR"] = std::move(works_for);
  
  {
    std::unique_lock<std::shared_mutex> lock(schema_mutex_);
    cached_schema_ = std::move(schema);
  }
  
  return Status::OK();
}

Status QueryMetaClient::FetchClusterStateFromMeta() {
  // TODO: Use actual gRPC call
  
  // Simulated cluster state
  ClusterState state;
  state.version = 1;
  state.partition_count = 4;
  
  for (uint32_t i = 0; i < state.partition_count; ++i) {
    PartitionInfo info;
    info.partition_id = i;
    info.leader_address = "127.0.0.1:" + std::to_string(9779 + i);
    info.data_size = 1024 * 1024 * 100;  // 100MB
    info.key_count = 10000;
    info.is_healthy = true;
    state.partitions.push_back(std::move(info));
  }
  
  {
    std::unique_lock<std::shared_mutex> lock(cluster_mutex_);
    cached_cluster_state_ = std::move(state);
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
