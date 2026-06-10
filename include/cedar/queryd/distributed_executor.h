// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Distributed Query Executor - Event-Driven Engine

#ifndef CEDAR_QUERYD_DISTRIBUTED_EXECUTOR_H_
#define CEDAR_QUERYD_DISTRIBUTED_EXECUTOR_H_

#include <atomic>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/value.h"
#include "cedar/cypher/validator.h"
#include "cedar/types/cedar_types.h"

namespace cedar {

// Forward declarations
namespace dtx {
class StorageClient;
}
namespace queryd {
class QueryStorageClient;
class QueryMetaClient;
}
namespace partition {
class PartitionStrategyManager;
}

#include "cedar/queryd/meta_client.h"

class LsmEngine;

namespace queryd {



// ============================================================================
// Distributed Execution Context
// ============================================================================

struct DistributedExecutionContext {
  // Query metadata
  std::string query_id;
  std::string session_id;
  uint64_t start_time_us;
  uint64_t timeout_ms;
  
  // Consistency level
  enum class Consistency {
    kReadYourWrites,
    kEventual,
    kStrong
  };
  Consistency consistency = Consistency::kReadYourWrites;
  
  // Temporal context
  Timestamp snapshot_ts = 0;
  bool use_snapshot = false;
  
  // Cancellation check (e.g., grpc::ServerContext::IsCancelled)
  std::function<bool()> is_cancelled;

  // Execution statistics
  struct Stats {
    std::atomic<uint64_t> rows_scanned{0};
    std::atomic<uint64_t> rows_returned{0};
    std::atomic<uint32_t> storage_nodes_accessed{0};
    std::atomic<uint32_t> network_roundtrips{0};
    std::atomic<uint64_t> execution_time_us{0};
  };
  Stats stats;
};

// ============================================================================
// Partition Routing
// ============================================================================

class PartitionRouter {
 public:
  explicit PartitionRouter(QueryMetaClient* meta_client);
  ~PartitionRouter();

  uint32_t GetPartitionId(uint64_t entity_id);
  Status GetStorageNode(uint32_t partition_id, std::string* address);
  Status GetPartitionInfo(uint32_t partition_id, PartitionInfo* info);
  Status CheckIsLeader(uint32_t partition_id, const std::string& address);
  std::vector<uint32_t> GetPartitionsForRange(uint64_t start_id, uint64_t end_id);
  std::unordered_map<uint32_t, std::vector<uint64_t>> RouteEntities(
      const std::vector<uint64_t>& entity_ids);
  void SetRequireLeaderOnly(bool require) { require_leader_only_ = require; }
  void SetStrategyManager(partition::PartitionStrategyManager* mgr) {
    strategy_manager_ = mgr;
  }

 private:
  QueryMetaClient* meta_client_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint32_t, std::string> partition_cache_;
  std::unordered_map<uint32_t, PartitionInfo> partition_info_cache_;
  uint32_t partition_count_ = 0;
  bool require_leader_only_ = true;
  partition::PartitionStrategyManager* strategy_manager_ = nullptr;
  void RefreshPartitionCache();
};

// ============================================================================
// Sub-Query Task
// ============================================================================

struct SubQueryTask {
  uint32_t partition_id;
  std::string storage_node;
  std::string sub_query;
  std::unordered_map<std::string, cypher::Value> parameters;
  
  // For result ordering
  uint32_t sequence;
};

struct SubQueryResult {
  uint32_t partition_id;
  uint32_t sequence;
  cypher::ResultSet result;
  Status status;
};

// ============================================================================
// Parallel Executor
// ============================================================================

class ParallelExecutor {
 public:
  explicit ParallelExecutor(size_t worker_count = 16);
  ~ParallelExecutor();

  // Execute sub-queries in parallel
  std::vector<SubQueryResult> ExecuteParallel(
      const std::vector<SubQueryTask>& tasks,
      QueryStorageClient* storage_client,
      DistributedExecutionContext* ctx);
  
  // Execute with streaming results
  void ExecuteParallelStreaming(
      const std::vector<SubQueryTask>& tasks,
      QueryStorageClient* storage_client,
      DistributedExecutionContext* ctx,
      std::function<bool(const SubQueryResult&)> result_callback);

 private:
  size_t worker_count_;
  
  struct Worker {
    std::thread thread;
    std::atomic<bool> running{false};
  };
  std::vector<std::unique_ptr<Worker>> workers_;
  
  // Task queue
  struct TaskQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::queue<std::function<void()>> tasks;
    std::atomic<bool> shutdown{false};
  };
  TaskQueue task_queue_;
  
  void WorkerLoop();
};

// ============================================================================
// Result Merger
// ============================================================================

class ResultMerger {
 public:
  ResultMerger() = default;
  
  // Merge results from multiple partitions
  cypher::ResultSet Merge(
      const std::vector<SubQueryResult>& results,
      const std::vector<std::pair<std::string, bool>>& sort_keys = {});
  
  // Stream merge with limit
  void MergeStreaming(
      const std::vector<SubQueryResult>& results,
      size_t limit,
      size_t offset,
      std::function<bool(const cypher::Record&)> callback);
  
  // Aggregate results (for COUNT, SUM, etc.)
  cypher::ResultSet MergeAggregate(
      const std::vector<SubQueryResult>& results,
      const std::vector<std::string>& group_keys,
      const std::vector<std::pair<std::string, std::string>>& aggregations);

 private:
  void SortResults(std::vector<cypher::Record>& records,
                   const std::vector<std::pair<std::string, bool>>& sort_keys);
};

// ============================================================================
// Distributed Executor
// ============================================================================

class DistributedExecutor {
 public:
  DistributedExecutor(
      QueryStorageClient* storage_client,
      QueryMetaClient* meta_client,
      size_t worker_count = 16);
  ~DistributedExecutor();

  // Execute a distributed query
  Status Execute(
      const std::string& query,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      DistributedExecutionContext* ctx,
      cypher::ResultSet* result);
  
  // Execute with EXPLAIN output
  Status ExecuteExplain(
      const std::string& query,
      std::string* explain_output);
  
  // Stream execute for large results
  Status ExecuteStreaming(
      const std::string& query,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      DistributedExecutionContext* ctx,
      std::function<bool(const cypher::Record&)> record_callback);

  // Optimized graph traversal using CedarKey properties
  Status Traverse(
      uint64_t start_node_id,
      cypher::Direction direction,
      const std::vector<uint16_t>& edge_types,
      uint32_t max_depth,
      uint32_t max_branch,
      Timestamp as_of_ts,
      std::vector<std::unique_ptr<cypher::Path>>* paths);

  // Temporal query using CedarKey's descending timestamp
  Status TemporalQuery(
      uint64_t entity_id,
      EntityType entity_type,
      DistributedExecutionContext::Consistency consistency,
      std::vector<cypher::VersionedEntity>* versions);

  // Get entity at specific time (O(log N) using binary search)
  Status GetEntityAtTime(
      uint64_t entity_id,
      EntityType entity_type,
      Timestamp timestamp,
      cypher::VersionedEntity* entity);

  // Access the underlying storage client for routing setup
  QueryStorageClient* GetStorageClient() const { return storage_client_; }

 private:
  QueryStorageClient* storage_client_;
  QueryMetaClient* meta_client_;
  std::unique_ptr<PartitionRouter> router_;
  std::unique_ptr<ParallelExecutor> parallel_executor_;
  std::unique_ptr<ResultMerger> result_merger_;
  std::unique_ptr<cypher::QueryValidator> validator_;
  GraphSchema schema_;
  
  // Analyze query to determine if it's single-partition
  bool IsSinglePartitionQuery(
      const std::string& query,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      uint32_t* partition_id);
  
  // Execute single-partition query (optimized path)
  Status ExecuteSinglePartition(
      const std::string& query,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      uint32_t partition_id,
      DistributedExecutionContext* ctx,
      cypher::ResultSet* result);
  
  // Execute cross-partition query
  Status ExecuteCrossPartition(
      const std::string& query,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      DistributedExecutionContext* ctx,
      cypher::ResultSet* result);
  
  // Split query into sub-queries per partition
  Status SplitQuery(
      const std::string& query,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      std::vector<SubQueryTask>* tasks);
  
  // Optimize traversal using CedarKey physical clustering
  Status TraverseOptimized(
      uint64_t start_node_id,
      const std::vector<uint16_t>& edge_types,
      uint32_t max_depth,
      std::vector<std::unique_ptr<cypher::Path>>* paths);
};

// ============================================================================
// Query Cache
// ============================================================================

class QueryPlanCache {
 public:
  explicit QueryPlanCache(size_t max_size = 1000);
  
  // Get cached plan
  std::shared_ptr<cypher::ExecutionPlan> Get(const std::string& query);
  
  // Cache a plan
  void Put(const std::string& query, std::shared_ptr<cypher::ExecutionPlan> plan);
  
  // Invalidate cache entry
  void Invalidate(const std::string& query);
  
  // Clear all
  void Clear();
  
  // Get stats
  struct Stats {
    size_t size;
    size_t hits;
    size_t misses;
    double hit_rate;
  };
  Stats GetStats() const;

 private:
  size_t max_size_;
  mutable std::shared_mutex mutex_;
  
  struct CacheEntry {
    std::shared_ptr<cypher::ExecutionPlan> plan;
    uint64_t last_access;
    size_t access_count;
  };
  
  std::unordered_map<std::string, CacheEntry> cache_;
  std::atomic<uint64_t> access_counter_{0};
  std::atomic<size_t> hits_{0};
  std::atomic<size_t> misses_{0};
  
  void EvictIfNeeded();
};

}  // namespace queryd
}  // namespace cedar

#endif  // CEDAR_QUERYD_DISTRIBUTED_EXECUTOR_H_
