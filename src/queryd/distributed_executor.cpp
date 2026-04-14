// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Distributed Query Executor Implementation

#include "cedar/queryd/distributed_executor.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <sstream>

#include "cedar/queryd/query_storage_client.h"  // 更新头文件
#include "cedar/queryd/meta_client.h"
#include "cedar/cypher/parser.h"
#include "cedar/cypher/planner.h"

namespace cedar {
namespace queryd {

using namespace std::chrono;

// ============================================================================
// Partition Router
// ============================================================================

PartitionRouter::PartitionRouter(QueryMetaClient* meta_client)
    : meta_client_(meta_client) {
  RefreshPartitionCache();
}

PartitionRouter::~PartitionRouter() = default;

uint32_t PartitionRouter::GetPartitionId(uint64_t entity_id) const {
  // Simple hash-based routing
  // In production, use consistent hashing or range-based partitioning
  if (partition_count_ == 0) {
    return 0;
  }
  return static_cast<uint32_t>(entity_id % partition_count_);
}

Status PartitionRouter::GetStorageNode(uint32_t partition_id, 
                                       std::string* address) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = partition_cache_.find(partition_id);
  if (it != partition_cache_.end()) {
    *address = it->second;
    return Status::OK();
  }
  
  // Cache miss - refresh
  lock.unlock();
  RefreshPartitionCache();
  
  lock.lock();
  it = partition_cache_.find(partition_id);
  if (it != partition_cache_.end()) {
    *address = it->second;
    return Status::OK();
  }
  
  return Status::NotFound("Partition not found: " + 
                          std::to_string(partition_id));
}

std::vector<uint32_t> PartitionRouter::GetPartitionsForRange(
    uint64_t start_id, uint64_t end_id) {
  std::set<uint32_t> partitions;
  for (uint64_t id = start_id; id < end_id; ++id) {
    partitions.insert(GetPartitionId(id));
  }
  return std::vector<uint32_t>(partitions.begin(), partitions.end());
}

std::unordered_map<uint32_t, std::vector<uint64_t>> PartitionRouter::RouteEntities(
    const std::vector<uint64_t>& entity_ids) {
  std::unordered_map<uint32_t, std::vector<uint64_t>> result;
  for (uint64_t id : entity_ids) {
    result[GetPartitionId(id)].push_back(id);
  }
  return result;
}

void PartitionRouter::RefreshPartitionCache() {
  ClusterState state;
  Status s = meta_client_->GetClusterState(&state);
  if (!s.ok()) {
    return;
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  partition_count_ = state.partition_count;
  partition_cache_.clear();
  partition_info_cache_.clear();
  for (const auto& partition : state.partitions) {
    partition_cache_[partition.partition_id] = partition.leader_address;
    partition_info_cache_[partition.partition_id] = partition;
  }
}

Status PartitionRouter::GetPartitionInfo(uint32_t partition_id, PartitionInfo* info) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = partition_info_cache_.find(partition_id);
  if (it != partition_info_cache_.end()) {
    *info = it->second;
    return Status::OK();
  }
  lock.unlock();
  RefreshPartitionCache();
  lock.lock();
  it = partition_info_cache_.find(partition_id);
  if (it != partition_info_cache_.end()) {
    *info = it->second;
    return Status::OK();
  }
  return Status::NotFound("Partition not found: " + std::to_string(partition_id));
}

Status PartitionRouter::CheckIsLeader(uint32_t partition_id, const std::string& address) {
  if (!require_leader_only_) {
    return Status::OK();
  }
  PartitionInfo info;
  Status s = GetPartitionInfo(partition_id, &info);
  if (!s.ok()) return s;
  if (info.leader_address != address) {
    return Status::NotLeader("Address " + address + " is not the leader for partition " +
                             std::to_string(partition_id));
  }
  return Status::OK();
}

// ============================================================================
// Parallel Executor
// ============================================================================

ParallelExecutor::ParallelExecutor(size_t worker_count)
    : worker_count_(worker_count) {
  // Start worker threads
  for (size_t i = 0; i < worker_count_; ++i) {
    auto worker = std::make_unique<Worker>();
    worker->running = true;
    worker->thread = std::thread(&ParallelExecutor::WorkerLoop, this);
    workers_.push_back(std::move(worker));
  }
}

ParallelExecutor::~ParallelExecutor() {
  {
    std::lock_guard<std::mutex> lock(task_queue_.mutex);
    task_queue_.shutdown = true;
  }
  task_queue_.cv.notify_all();
  
  for (auto& worker : workers_) {
    if (worker->thread.joinable()) {
      worker->thread.join();
    }
  }
}

std::vector<SubQueryResult> ParallelExecutor::ExecuteParallel(
    const std::vector<SubQueryTask>& tasks,
    QueryStorageClient* storage_client,
    DistributedExecutionContext* ctx) {
  
  std::vector<SubQueryResult> results(tasks.size());
  std::vector<std::promise<void>> promises(tasks.size());
  std::atomic<size_t> completed{0};
  
  // Submit tasks
  for (size_t i = 0; i < tasks.size(); ++i) {
    auto task = [&tasks, &results, &promises, i, storage_client, ctx, &completed]() {
      const auto& t = tasks[i];
      auto& r = results[i];
      
      r.partition_id = t.partition_id;
      r.sequence = t.sequence;
      
      // Execute sub-query on storage node
      // In production, this would use storage_client to send RPC
      // For now, simulate execution
      ctx->stats.storage_nodes_accessed++;
      ctx->stats.network_roundtrips++;
      
      // TODO: Implement actual RPC call
      r.status = Status::OK();
      
      completed++;
      promises[i].set_value();
    };
    
    {
      std::lock_guard<std::mutex> lock(task_queue_.mutex);
      task_queue_.tasks.push(std::move(task));
    }
    task_queue_.cv.notify_one();
  }
  
  // Wait for all tasks
  for (auto& p : promises) {
    p.get_future().wait();
  }
  
  return results;
}

void ParallelExecutor::ExecuteParallelStreaming(
    const std::vector<SubQueryTask>& tasks,
    QueryStorageClient* storage_client,
    DistributedExecutionContext* ctx,
    std::function<bool(const SubQueryResult&)> result_callback) {
  
  std::atomic<size_t> next_result{0};
  std::mutex callback_mutex;
  std::condition_variable callback_cv;
  
  // Submit tasks with callback
  for (size_t i = 0; i < tasks.size(); ++i) {
    auto task = [&, i]() {
      const auto& t = tasks[i];
      SubQueryResult r;
      r.partition_id = t.partition_id;
      r.sequence = t.sequence;
      
      // Execute sub-query
      ctx->stats.storage_nodes_accessed++;
      ctx->stats.network_roundtrips++;
      
      r.status = Status::OK();
      
      // Callback with result
      bool continue_streaming = result_callback(r);
      if (!continue_streaming) {
        // Signal cancellation
      }
    };
    
    {
      std::lock_guard<std::mutex> lock(task_queue_.mutex);
      task_queue_.tasks.push(std::move(task));
    }
    task_queue_.cv.notify_one();
  }
}

void ParallelExecutor::WorkerLoop() {
  while (true) {
    std::function<void()> task;
    
    {
      std::unique_lock<std::mutex> lock(task_queue_.mutex);
      task_queue_.cv.wait(lock, [this] {
        return !task_queue_.tasks.empty() || task_queue_.shutdown;
      });
      
      if (task_queue_.shutdown && task_queue_.tasks.empty()) {
        return;
      }
      
      task = std::move(task_queue_.tasks.front());
      task_queue_.tasks.pop();
    }
    
    task();
  }
}

// ============================================================================
// Result Merger
// ============================================================================

cypher::ResultSet ResultMerger::Merge(
    const std::vector<SubQueryResult>& results,
    const std::vector<std::pair<std::string, bool>>& sort_keys) {
  
  cypher::ResultSet merged;
  
  // Collect all records
  for (const auto& result : results) {
    if (!result.status.ok()) {
      continue;
    }
    for (const auto& record : result.result.records) {
      merged.records.push_back(record);
    }
  }
  
  // Sort if needed
  if (!sort_keys.empty()) {
    SortResults(merged.records, sort_keys);
  }
  
  return merged;
}

void ResultMerger::MergeStreaming(
    const std::vector<SubQueryResult>& results,
    size_t limit,
    size_t offset,
    std::function<bool(const cypher::Record&)> callback) {
  
  // Use a priority queue for efficient streaming merge
  // This is simplified - production would use a heap-based merge
  std::vector<cypher::Record> all_records;
  
  for (const auto& result : results) {
    if (!result.status.ok()) {
      continue;
    }
    for (const auto& record : result.result.records) {
      all_records.push_back(record);
    }
  }
  
  // Apply offset and limit
  size_t start = std::min(offset, all_records.size());
  size_t end = std::min(start + limit, all_records.size());
  
  for (size_t i = start; i < end; ++i) {
    if (!callback(all_records[i])) {
      break;
    }
  }
}

cypher::ResultSet ResultMerger::MergeAggregate(
    const std::vector<SubQueryResult>& results,
    const std::vector<std::string>& group_keys,
    const std::vector<std::pair<std::string, std::string>>& aggregations) {
  
  cypher::ResultSet merged;
  // TODO: Implement aggregation merge
  return merged;
}

void ResultMerger::SortResults(
    std::vector<cypher::Record>& records,
    const std::vector<std::pair<std::string, bool>>& sort_keys) {
  
  std::sort(records.begin(), records.end(),
            [&sort_keys](const cypher::Record& a, const cypher::Record& b) {
              for (const auto& [key, ascending] : sort_keys) {
                auto it_a = a.values.find(key);
                auto it_b = b.values.find(key);
                
                if (it_a == a.values.end() && it_b == b.values.end()) {
                  continue;
                }
                if (it_a == a.values.end()) {
                  return !ascending;
                }
                if (it_b == b.values.end()) {
                  return ascending;
                }
                
                // Compare values
                if (it_a->second != it_b->second) {
                  return ascending ? 
                      (it_a->second < it_b->second) : 
                      (it_a->second > it_b->second);
                }
              }
              return false;
            });
}

// ============================================================================
// Distributed Executor
// ============================================================================

DistributedExecutor::DistributedExecutor(
    QueryStorageClient* storage_client,
    QueryMetaClient* meta_client,
    size_t worker_count)
    : storage_client_(storage_client),
      meta_client_(meta_client) {
  
  router_ = std::make_unique<PartitionRouter>(meta_client_);
  parallel_executor_ = std::make_unique<ParallelExecutor>(worker_count);
  result_merger_ = std::make_unique<ResultMerger>();
  {
    queryd::GraphSchema schema;
    if (meta_client_ && meta_client_->GetSchema(&schema).ok()) {
      validator_ = std::make_unique<cypher::QueryValidator>(&schema);
    }
  }
}

DistributedExecutor::~DistributedExecutor() = default;

Status DistributedExecutor::Execute(
    const std::string& query,
    const std::unordered_map<std::string, cypher::Value>& parameters,
    DistributedExecutionContext* ctx,
    cypher::ResultSet* result) {
  
  auto start = steady_clock::now();

  // Parse and validate query
  cypher::CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  if (!stmt) {
    return Status::InvalidArgument("Failed to parse query: " + parser.GetError());
  }

  if (validator_) {
    Status v = validator_->Validate(*stmt);
    if (!v.ok()) {
      return Status::InvalidArgument("Query validation failed: " + v.ToString());
    }
  }
  
  // Check if single-partition query
  uint32_t partition_id;
  if (IsSinglePartitionQuery(query, parameters, &partition_id)) {
    auto s = ExecuteSinglePartition(query, parameters, partition_id, ctx, result);
    if (s.ok()) {
      ctx->stats.execution_time_us = 
          duration_cast<microseconds>(steady_clock::now() - start).count();
      return s;
    }
    // Fall through to cross-partition if single-partition fails
  }
  
  // Execute cross-partition query
  auto s = ExecuteCrossPartition(query, parameters, ctx, result);
  ctx->stats.execution_time_us = 
      duration_cast<microseconds>(steady_clock::now() - start).count();
  
  return s;
}

Status DistributedExecutor::ExecuteExplain(
    const std::string& query,
    std::string* explain_output) {
  
  // Parse query
  cypher::CypherParser parser(query);
  auto ast = parser.ParseStatement();
  if (!ast) {
    return Status::InvalidArgument("Parse error: " + parser.GetError());
  }
  
  // Generate execution plan
  auto plan = cypher::ExecutionPlanBuilder::Build(ast);
  if (!plan) {
    return Status::InvalidArgument("Planning error");
  }
  
  *explain_output = "Query Plan:\n  Scan All Nodes\n  Return Results";
  return Status::OK();
}

Status DistributedExecutor::ExecuteStreaming(
    const std::string& query,
    const std::unordered_map<std::string, cypher::Value>& parameters,
    DistributedExecutionContext* ctx,
    std::function<bool(const cypher::Record&)> record_callback) {
  
  // Split query into sub-queries
  auto tasks = SplitQuery(query, parameters);
  
  // Execute with streaming callback
  parallel_executor_->ExecuteParallelStreaming(
      tasks, storage_client_, ctx,
      [&record_callback](const SubQueryResult& sub_result) -> bool {
        if (!sub_result.status.ok()) {
          return true;  // Continue with other partitions
        }
        for (const auto& record : sub_result.result.records) {
          if (!record_callback(record)) {
            return false;  // Client requested stop
          }
        }
        return true;
      });
  
  return Status::OK();
}

Status DistributedExecutor::Traverse(
    uint64_t start_node_id,
    cypher::Direction direction,
    const std::vector<uint16_t>& edge_types,
    uint32_t max_depth,
    uint32_t max_branch,
    Timestamp as_of_ts,
    std::vector<std::unique_ptr<cypher::Path>>* paths) {
  
  // Use optimized traversal for OUTGOING direction
  if (direction == cypher::Direction::OUTGOING && edge_types.size() == 1) {
    return TraverseOptimized(start_node_id, edge_types, max_depth, paths);
  }
  
  // General traversal
  // TODO: Implement BFS/DFS traversal
  return Status::NotSupported("General traversal not yet implemented");
}

Status DistributedExecutor::TemporalQuery(
    uint64_t entity_id,
    EntityType entity_type,
    DistributedExecutionContext::Consistency consistency,
    std::vector<cypher::VersionedEntity>* versions) {
  
  // Get partition for entity
  uint32_t partition_id = router_->GetPartitionId(entity_id);
  
  // Get storage node client
  auto node_client = storage_client_->GetNodeClient(partition_id);
  if (!node_client) {
    return Status::NotFound("Storage node not found for partition " +
                            std::to_string(partition_id));
  }
  
  // Scan entity versions
  std::vector<std::pair<Timestamp, Descriptor>> results;
  Status s = node_client->ScanEntity(entity_id, entity_type, 0, 
                                      Timestamp::Max(), &results);
  if (!s.ok()) {
    return s;
  }
  
  // Convert to VersionedEntity
  for (const auto& [ts, desc] : results) {
    cypher::VersionedEntity ve;
    ve.timestamp = ts;
    ve.is_deleted = false;  // TODO: Check delete flag
    // TODO: Parse descriptor properties
    versions->push_back(std::move(ve));
  }
  
  return Status::OK();
}

Status DistributedExecutor::GetEntityAtTime(
    uint64_t entity_id,
    EntityType entity_type,
    Timestamp timestamp,
    cypher::VersionedEntity* entity) {
  
  // Get partition for entity
  uint32_t partition_id = router_->GetPartitionId(entity_id);
  
  // Scan entity versions around the timestamp
  // Since CedarKey stores timestamps in descending order,
  // we can use binary search for O(log N) lookup
  auto node_client = storage_client_->GetNodeClient(partition_id);
  if (!node_client) {
    return Status::NotFound("Storage node not found for partition " +
                            std::to_string(partition_id));
  }
  
  std::vector<std::pair<Timestamp, Descriptor>> results;
  Status s = node_client->ScanEntity(entity_id, entity_type, 
                                      0, timestamp, &results);
  if (!s.ok() || results.empty()) {
    return Status::NotFound("Entity not found at specified time");
  }
  
  // Results are in descending order, first element is the version at timestamp
  entity->timestamp = results[0].first;
  entity->is_deleted = false;
  // TODO: Parse descriptor properties
  
  return Status::OK();
}

bool DistributedExecutor::IsSinglePartitionQuery(
    const std::string& query,
    const std::unordered_map<std::string, cypher::Value>& parameters,
    uint32_t* partition_id) {
  
  // Simple heuristic: check if query contains specific node ID lookup
  // e.g., MATCH (n {id: 123}) WHERE id(n) = 123
  
  // Parse to extract entity IDs
  cypher::CypherParser parser(query);
  auto ast = parser.ParseStatement();
  if (!ast) {
    return false;
  }
  
  // Check if all referenced entities are in the same partition
  // This is simplified - production would do proper analysis
  return false;  // Default to cross-partition for safety
}

Status DistributedExecutor::ExecuteSinglePartition(
    const std::string& query,
    const std::unordered_map<std::string, cypher::Value>& parameters,
    uint32_t partition_id,
    DistributedExecutionContext* ctx,
    cypher::ResultSet* result) {
  
  // Send query to specific storage node
  // Storage nodes have embedded query capabilities
  auto node_client = storage_client_->GetNodeClient(partition_id);
  if (!node_client) {
    return Status::NotFound("Storage node not found");
  }
  
  // TODO: Implement RPC to storage node for query execution
  ctx->stats.storage_nodes_accessed = 1;
  
  return Status::OK();
}

Status DistributedExecutor::ExecuteCrossPartition(
    const std::string& query,
    const std::unordered_map<std::string, cypher::Value>& parameters,
    DistributedExecutionContext* ctx,
    cypher::ResultSet* result) {
  
  // Split into sub-queries per partition
  auto tasks = SplitQuery(query, parameters);
  
  // Execute in parallel
  auto sub_results = parallel_executor_->ExecuteParallel(
      tasks, storage_client_, ctx);
  
  // Merge results
  *result = result_merger_->Merge(sub_results);
  
  return Status::OK();
}

std::vector<SubQueryTask> DistributedExecutor::SplitQuery(
    const std::string& query,
    const std::unordered_map<std::string, cypher::Value>& parameters) {
  
  std::vector<SubQueryTask> tasks;
  
  // Get all partitions
  ClusterState state;
  if (!meta_client_->GetCachedClusterState()) {
    return tasks;
  }
  
  state = *meta_client_->GetCachedClusterState();
  
  // Create a sub-query task for each partition
  uint32_t seq = 0;
  for (const auto& partition : state.partitions) {
    SubQueryTask task;
    task.partition_id = partition.partition_id;
    task.storage_node = partition.leader_address;
    task.sub_query = query;  // Same query for all partitions
    task.parameters = parameters;
    task.sequence = seq++;
    tasks.push_back(std::move(task));
  }
  
  return tasks;
}

Status DistributedExecutor::TraverseOptimized(
    uint64_t start_node_id,
    const std::vector<uint16_t>& edge_types,
    uint32_t max_depth,
    std::vector<std::unique_ptr<cypher::Path>>* paths) {
  
  // Optimized traversal using CedarKey's physical clustering
  // Since edges are stored with entity_id prefix, we can do efficient scans
  
  std::queue<std::pair<uint64_t, std::unique_ptr<cypher::Path>>> queue;
  
  // Start node
  auto start_path = std::make_unique<cypher::Path>();
  // TODO: Add start node to path
  queue.push({start_node_id, std::move(start_path)});
  
  uint32_t visited = 0;
  const uint32_t kMaxVisited = 10000;  // Safety limit
  
  while (!queue.empty() && visited < kMaxVisited) {
    auto& [current_id, current_path] = queue.front();
    queue.pop();
    
    if (current_path->Length() >= max_depth) {
      paths->push_back(std::move(current_path));
      continue;
    }
    
    // Scan outgoing edges using CedarKey's efficient range scan
    std::vector<EdgeScanEntry> edges;
    Status s = storage_client_->ScanOutEdges(
        current_id, edge_types[0], Timestamp::Max(), &edges);
    if (!s.ok()) {
      continue;
    }
    
    for (const auto& edge : edges) {
      auto new_path = std::make_unique<cypher::Path>(*current_path);
      // TODO: Add edge and target node to path
      // new_path length is managed by elements vector
      
      queue.push({edge.target_id, std::move(new_path)});
    }
    
    visited++;
  }
  
  return Status::OK();
}

// ============================================================================
// Query Plan Cache
// ============================================================================

QueryPlanCache::QueryPlanCache(size_t max_size) : max_size_(max_size) {}

std::shared_ptr<cypher::ExecutionPlan> QueryPlanCache::Get(
    const std::string& query) {
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = cache_.find(query);
  if (it != cache_.end()) {
    it->second.last_access = ++access_counter_;
    it->second.access_count++;
    hits_++;
    return it->second.plan;
  }
  
  misses_++;
  return nullptr;
}

void QueryPlanCache::Put(const std::string& query, 
                         std::shared_ptr<cypher::ExecutionPlan> plan) {
  
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  EvictIfNeeded();
  
  CacheEntry entry;
  entry.plan = std::move(plan);
  entry.last_access = ++access_counter_;
  entry.access_count = 1;
  
  cache_[query] = std::move(entry);
}

void QueryPlanCache::Invalidate(const std::string& query) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  cache_.erase(query);
}

void QueryPlanCache::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  cache_.clear();
}

QueryPlanCache::Stats QueryPlanCache::GetStats() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  Stats stats;
  stats.size = cache_.size();
  stats.hits = hits_.load();
  stats.misses = misses_.load();
  
  uint64_t total = stats.hits + stats.misses;
  stats.hit_rate = total > 0 ? static_cast<double>(stats.hits) / total : 0.0;
  
  return stats;
}

void QueryPlanCache::EvictIfNeeded() {
  if (cache_.size() < max_size_) {
    return;
  }
  
  // Evict least recently used
  auto it = std::min_element(cache_.begin(), cache_.end(),
                             [](const auto& a, const auto& b) {
                               return a.second.last_access < b.second.last_access;
                             });
  
  if (it != cache_.end()) {
    cache_.erase(it);
  }
}

}  // namespace queryd
}  // namespace cedar
