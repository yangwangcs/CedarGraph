// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Meta Client - Metadata Service Access

#ifndef CEDAR_QUERYD_META_CLIENT_H_
#define CEDAR_QUERYD_META_CLIENT_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/cedar_types.h"

namespace grpc {
class Channel;
}

namespace cedar {
namespace queryd {

// ============================================================================
// Schema Information
// ============================================================================

struct PropertySchema {
  std::string name;
  std::string type;  // STRING, INT, FLOAT, BOOL, LIST, MAP
  bool nullable = true;
  bool indexed = false;
  std::string default_value;
};

struct LabelSchema {
  std::string name;
  std::vector<PropertySchema> properties;
  std::vector<std::string> indexes;
  bool is_node = true;  // true = node label, false = edge type
};

struct GraphSchema {
  std::unordered_map<std::string, LabelSchema> node_labels;
  std::unordered_map<std::string, LabelSchema> edge_types;
  
  // Get schema for label
  const LabelSchema* GetNodeLabel(const std::string& name) const;
  const LabelSchema* GetEdgeType(const std::string& name) const;
};

// ============================================================================
// Partition Information
// ============================================================================

struct PartitionInfo {
  uint32_t partition_id;
  std::string leader_address;
  std::vector<std::string> follower_addresses;
  uint64_t data_size = 0;
  uint64_t key_count = 0;
  uint64_t qps = 0;
  bool is_healthy = true;
  
  // Key range [start, end)
  uint64_t key_range_start = 0;
  uint64_t key_range_end = 0;
};

// ============================================================================
// Cluster State
// ============================================================================

struct ClusterState {
  uint32_t version = 0;  // Cluster topology version
  uint32_t partition_count = 0;
  std::vector<PartitionInfo> partitions;
  
  // Quick lookup: partition_id -> info
  const PartitionInfo* GetPartition(uint32_t partition_id) const;
  
  // Calculate partition ID for entity
  uint32_t GetPartitionForEntity(uint64_t entity_id) const;
};

// ============================================================================
// Query Meta Client - 查询层元数据客户端
// ============================================================================
// 注意：与 dtx::MetaClient 区分，专门用于查询层的元数据获取

class QueryMetaClient {
 public:
  struct Options {
    std::string meta_service_address;
    std::chrono::seconds refresh_interval{30};
    std::chrono::milliseconds rpc_timeout{5000};
    bool enable_cache = true;
  };

  explicit QueryMetaClient(const Options& options);
  virtual ~QueryMetaClient();

  // Initialize and connect to meta service
  Status Init();

  // Get current schema
  virtual Status GetSchema(GraphSchema* schema);
  
  // Refresh schema from meta service
  Status RefreshSchema();
  
  // Get cluster state (partition routing)
  virtual Status GetClusterState(ClusterState* state);
  
  // Refresh cluster state
  Status RefreshClusterState();
  
  // Get partition info for entity ID
  Status GetPartitionForEntity(uint64_t entity_id, PartitionInfo* info);
  
  // Get storage node for partition
  Status GetStorageNode(uint32_t partition_id, std::string* address);
  
  // Watch for cluster changes (blocking)
  Status WatchClusterChanges(std::function<void(const ClusterState&)> callback);
  
  // Register this queryd instance
  Status RegisterQueryD(const std::string& listen_address);
  
  // Heartbeat to meta service
  Status Heartbeat(uint32_t active_queries, uint32_t queued_queries);
  
  // Get cached schema (no RPC)
  const GraphSchema* GetCachedSchema() const;
  
  // Get cached cluster state (no RPC)
  const ClusterState* GetCachedClusterState() const;

 private:
  Options options_;
  
  // gRPC stub
  std::shared_ptr<grpc::Channel> channel_;
  
  // Cached data
  mutable std::shared_mutex schema_mutex_;
  GraphSchema cached_schema_;
  
  mutable std::shared_mutex cluster_mutex_;
  ClusterState cached_cluster_state_;
  
  // Background refresh
  std::atomic<bool> running_{false};
  std::thread refresh_thread_;
  
  void RefreshLoop();
  Status FetchSchemaFromMeta();
  Status FetchClusterStateFromMeta();
};

// ============================================================================
// Local Schema Cache
// ============================================================================

class SchemaCache {
 public:
  explicit SchemaCache(size_t max_entries = 1000);
  
  // Get cached execution plan for query pattern
  std::string GetPlan(const std::string& query_pattern);
  
  // Cache execution plan
  void CachePlan(const std::string& query_pattern, const std::string& plan);
  
  // Invalidate on schema change
  void InvalidateAll();
  
  // Stats
  size_t Size() const;

 private:
  size_t max_entries_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::string> cache_;
};

}  // namespace queryd
}  // namespace cedar

#endif  // CEDAR_QUERYD_META_CLIENT_H_
