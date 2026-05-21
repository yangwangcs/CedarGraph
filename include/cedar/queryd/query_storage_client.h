// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Query Storage Client - 查询层特化存储客户端
// 
// 注意：这是 cedar-queryd 专用的存储客户端，它组合了底层的 dtx::StorageClient
// 并添加了查询层特有的功能：分区路由、断路器、扫描接口等

#ifndef CEDAR_QUERYD_QUERY_STORAGE_CLIENT_H_
#define CEDAR_QUERYD_QUERY_STORAGE_CLIENT_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/cypher/value.h"
#include "cedar/types/cedar_types.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/query/cedar_scan.h"

#include <grpcpp/grpcpp.h>

// 前向声明 - 使用底层 dtx 的 StorageClient
namespace cedar {
namespace dtx {
class StorageClient;
}
}

namespace cedar {
namespace queryd {

// ============================================================================
// Query Storage Client - 查询层存储客户端
// ============================================================================
// 
// 设计原则：
// 1. 不重复实现 RPC 功能，而是组合底层的 dtx::StorageClient
// 2. 添加查询层特有的：分区路由、扫描接口、断路器等
// 3. 与 dtx::StorageClient 明确区分，避免命名冲突

class QueryStorageClient {
 public:
  struct Options {
    // 重试选项
    int max_retries;
    std::chrono::milliseconds retry_delay;
    std::chrono::milliseconds operation_timeout;
    
    // 断路器选项
    size_t failure_threshold;
    std::chrono::seconds recovery_timeout;
    
    Options()
        : max_retries(3),
          retry_delay(100),
          operation_timeout(30000),
          failure_threshold(5),
          recovery_timeout(30) {}
  };

  explicit QueryStorageClient(const Options& options = Options{});
  ~QueryStorageClient();

  // 禁用拷贝
  QueryStorageClient(const QueryStorageClient&) = delete;
  QueryStorageClient& operator=(const QueryStorageClient&) = delete;

  // 初始化 - 传入底层的 dtx::StorageClient
  void SetBaseClient(std::shared_ptr<cedar::dtx::StorageClient> base_client);
  
  // 初始化 - 直接连接模式（独立使用）
  Status Init(const std::string& meta_service_address);

  // 注册存储节点
  void RegisterNode(uint32_t partition_id, const std::string& node_address);

  // Mark a partition as locally hosted (for Adaptive Execution Path).
  void MarkPartitionLocal(uint32_t partition_id);

  // ========== 基础操作 (转发到底层客户端) ==========
  
  Status Get(const CedarKey& key, Descriptor* descriptor, bool* found);
  Status Put(const CedarKey& key, const Descriptor& descriptor);
  Status Delete(const CedarKey& key);
  
  // 批量操作
  Status BatchGet(const std::vector<CedarKey>& keys,
                  std::vector<Descriptor>* descriptors,
                  std::vector<bool>* found);

  // ========== 查询层特化接口 ==========
  
  // 扫描节点版本
  Status ScanNode(uint64_t node_id,
                  Timestamp as_of_time,
                  std::vector<std::pair<Timestamp, Descriptor>>* versions);
  
  // 扫描出边 - 利用 CedarKey 的 EdgeOut 存储
  Status ScanOutEdges(uint64_t node_id,
                      uint16_t edge_type,
                      Timestamp as_of_time,
                      std::vector<EdgeScanEntry>* edges);
  
  // 扫描入边 - 利用 CedarKey 的 EdgeIn 存储
  Status ScanInEdges(uint64_t node_id,
                     uint16_t edge_type,
                     Timestamp as_of_time,
                     std::vector<EdgeScanEntry>* edges);

  // 获取指定时间点的实体版本 (O(log N))
  Status GetAtTime(uint64_t entity_id,
                   EntityType entity_type,
                   Timestamp snapshot_ts,
                   Descriptor* descriptor,
                   bool* found);

  // 批量并行获取
  Status ParallelBatchGet(
      const std::vector<std::pair<uint32_t, uint64_t>>& partition_entity_pairs,
      EntityType entity_type,
      Timestamp timestamp,
      std::vector<std::pair<bool, Descriptor>>* results);

  // 创建 CedarScan 用于快照读取
  std::unique_ptr<CedarScan> CreateScan(Timestamp snapshot_ts);

  // ========== Partition Node Client (used by DistributedExecutor) ==========
  
  class NodeClient {
   public:
    virtual ~NodeClient() = default;
    virtual Status ScanEntity(uint64_t entity_id,
                              EntityType entity_type,
                              Timestamp start_ts,
                              Timestamp end_ts,
                              std::vector<std::pair<Timestamp, Descriptor>>* results) = 0;

    // NEW: Execute a sub-query fragment on this storage node
    virtual Status ExecuteSubQuery(
        const std::string& query_fragment,
        const std::unordered_map<std::string, cypher::Value>& parameters,
        cypher::ResultSet* result) = 0;
  };
  
  std::shared_ptr<NodeClient> GetNodeClient(uint32_t partition_id);

  // Check if a partition is hosted locally (short-circuit path).
  bool IsLocalPartition(uint32_t partition_id) const;

  // Get or create gRPC channel to a remote partition.
  std::shared_ptr<grpc::Channel> GetOrCreateChannel(uint32_t partition_id);

  // Get the registered node address for a partition (empty if unknown).
  std::string GetNodeAddress(uint32_t partition_id) const;

  // Report success or failure for a node address (used by circuit breaker tracking).
  void ReportNodeResult(const std::string& node_address, bool success);

  // ========== 健康检查与统计 ==========
  
  Status HealthCheck();
  
  struct Stats {
    uint64_t total_requests = 0;
    uint64_t failed_requests = 0;
    uint64_t retried_requests = 0;
    uint64_t circuit_breaker_opens = 0;
    double avg_latency_ms = 0.0;
  };
  Stats GetStats() const;

 private:
  Options options_;
  
  // 底层客户端（可选，用于转发基础操作）
  std::shared_ptr<cedar::dtx::StorageClient> base_client_;
  bool use_base_client_ = false;
  
  // 分区到节点的映射
  mutable std::shared_mutex nodes_mutex_;
  std::unordered_map<uint32_t, std::string> partition_routing_;
  
  // 断路器状态
  struct CircuitBreaker {
    std::atomic<size_t> failures{0};
    std::atomic<bool> open{false};
    std::chrono::steady_clock::time_point last_failure;
  };
  std::unordered_map<std::string, CircuitBreaker> circuit_breakers_;
  mutable std::mutex cb_mutex_;
  
  // Local partition IDs (for AEP local/remote routing)
  mutable std::shared_mutex local_partitions_mutex_;
  std::unordered_set<uint32_t> local_partition_ids_;

  // Remote partition gRPC channels
  mutable std::shared_mutex channels_mutex_;
  std::unordered_map<uint32_t, std::shared_ptr<grpc::Channel>> partition_channels_;

  // 统计
  mutable std::mutex stats_mutex_;
  Stats stats_;
  
  bool CheckCircuitBreaker(const std::string& node_address);
  void RecordSuccess(const std::string& node_address);
  void RecordFailure(const std::string& node_address);
};

// ============================================================================
// Query Cache
// ============================================================================

class QueryCache {
 public:
  struct Options {
    size_t max_entries;
    std::chrono::seconds ttl;
    
    Options() : max_entries(100000), ttl(60) {}
  };

  explicit QueryCache(const Options& options = Options{});
  
  bool Get(const CedarKey& key, Descriptor* descriptor);
  void Put(const CedarKey& key, const Descriptor& descriptor);
  void Invalidate(const CedarKey& key);
  void Clear();

 private:
  Options options_;
  
  struct CacheEntry {
    Descriptor descriptor;
    std::chrono::steady_clock::time_point expires_at;
  };
  
  // Cache key combining entity_id and timestamp to avoid collisions
  struct CacheKey {
    uint64_t entity_id;
    uint64_t timestamp;
    
    bool operator==(const CacheKey& other) const {
      return entity_id == other.entity_id && timestamp == other.timestamp;
    }
  };
  
  struct CacheKeyHash {
    size_t operator()(const CacheKey& key) const {
      // FNV-1a hash combining both fields
      size_t hash = 14695981039346656037ull;
      hash ^= key.entity_id;
      hash *= 1099511628211ull;
      hash ^= key.timestamp;
      hash *= 1099511628211ull;
      return hash;
    }
  };
  
  std::unordered_map<CacheKey, CacheEntry, CacheKeyHash> cache_;
  mutable std::mutex mutex_;
  
  void EvictIfNeeded();
};

}  // namespace queryd
}  // namespace cedar

#endif  // CEDAR_QUERYD_QUERY_STORAGE_CLIENT_H_
