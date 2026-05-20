// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Distributed Query Executor Implementation

#include "cedar/queryd/distributed_executor.h"

#include "cedar/cypher/fingerprint.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <sstream>

#include "cedar/queryd/query_storage_client.h"  // 更新头文件
#include "cedar/queryd/meta_client.h"
#include "cedar/storage/adaptive_thread_pool.h"
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
      
      auto node_client = storage_client->GetNodeClient(t.partition_id);
      if (!node_client) {
          r.status = Status::NotFound("Storage node not found for partition " +
                                      std::to_string(t.partition_id));
          promises[i].set_value();
          completed++;
          return;
      }

      Status s = node_client->ExecuteSubQuery(t.sub_query, t.parameters, &r.result);
      r.status = s;

      ctx->stats.storage_nodes_accessed++;
      ctx->stats.network_roundtrips++;
      
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
  if (tasks.empty()) return;

  std::atomic<size_t> completed{0};
  std::mutex error_mutex;
  Status first_error = Status::OK();

  cedar::AdaptiveConfig pool_config;
  pool_config.min_threads = 4;
  pool_config.max_threads = 16;
  pool_config.initial_threads = 4;
  cedar::AdaptiveThreadPool<std::function<void()>> pool(pool_config);
  pool.Start();

  for (const auto& task : tasks) {
    pool.Submit([&, task]() {
      SubQueryResult r;
      r.partition_id = task.partition_id;
      r.sequence = task.sequence;

      auto node_client = storage_client->GetNodeClient(task.partition_id);
      if (!node_client) {
        r.status = Status::IOError("No node client for partition " +
                                    std::to_string(task.partition_id));
      } else {
        r.status = node_client->ExecuteSubQuery(
            task.sub_query, task.parameters, &r.result);
      }

      ctx->stats.storage_nodes_accessed++;
      ctx->stats.network_roundtrips++;

      bool continue_streaming = result_callback(r);
      if (!continue_streaming) {
        // Client requested stop; no special action needed since we still
        // wait for all tasks to finish to keep references valid.
      }
      completed.fetch_add(1);
    });
  }

  // Wait for all with timeout
  auto start = std::chrono::steady_clock::now();
  while (completed.load() < tasks.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(300)) {
      break;  // Timeout
    }
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
  
  // Collect all records from all partitions
  std::vector<cypher::Record> all_records;
  for (const auto& r : results) {
    if (r.status.ok()) {
      all_records.insert(all_records.end(), r.result.records.begin(), r.result.records.end());
    }
  }
  
  if (all_records.empty()) {
    return merged;
  }
  
  // Build group key for a record
  auto make_group_key = [&group_keys](const cypher::Record& rec) -> std::string {
    std::string key;
    for (const auto& gk : group_keys) {
      auto it = rec.values.find(gk);
      if (it != rec.values.end()) {
        key += it->second.ToString() + "\x01";
      } else {
        key += "\x01";
      }
    }
    return key;
  };
  
  // Group records
  std::map<std::string, std::vector<const cypher::Record*>> groups;
  for (const auto& rec : all_records) {
    groups[make_group_key(rec)].push_back(&rec);
  }
  
  // Process each group
  for (const auto& [group_key, group_records] : groups) {
    cypher::Record aggregated_rec;
    
    // Copy group key values from first record
    for (const auto& gk : group_keys) {
      auto it = group_records[0]->values.find(gk);
      if (it != group_records[0]->values.end()) {
        aggregated_rec.values[gk] = it->second;
      }
    }
    
    // Apply aggregations
    for (const auto& [func, col] : aggregations) {
      if (func == "count") {
        aggregated_rec.values[col] = cypher::Value(static_cast<int64_t>(group_records.size()));
      } else if (func == "sum" || func == "avg") {
        double total = 0.0;
        int64_t count = 0;
        for (const auto* rec : group_records) {
          auto it = rec->values.find(col);
          if (it != rec->values.end() && it->second.IsNumeric()) {
            if (it->second.IsInt()) {
              total += static_cast<double>(it->second.GetInt());
            } else if (it->second.IsFloat()) {
              total += it->second.GetFloat();
            }
            count++;
          }
        }
        if (func == "avg" && count > 0) {
          aggregated_rec.values[col] = cypher::Value(total / static_cast<double>(count));
        } else {
          aggregated_rec.values[col] = cypher::Value(total);
        }
      } else if (func == "min" || func == "max") {
        bool first = true;
        double best = 0.0;
        for (const auto* rec : group_records) {
          auto it = rec->values.find(col);
          if (it != rec->values.end() && it->second.IsNumeric()) {
            double val = 0.0;
            if (it->second.IsInt()) {
              val = static_cast<double>(it->second.GetInt());
            } else if (it->second.IsFloat()) {
              val = it->second.GetFloat();
            }
            if (first) {
              best = val;
              first = false;
            } else if (func == "min" && val < best) {
              best = val;
            } else if (func == "max" && val > best) {
              best = val;
            }
          }
        }
        if (!first) {
          aggregated_rec.values[col] = cypher::Value(best);
        }
      }
    }
    
    merged.records.push_back(std::move(aggregated_rec));
  }
  
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
  if (meta_client_ && meta_client_->GetSchema(&schema_).ok()) {
    validator_ = std::make_unique<cypher::QueryValidator>(&schema_);
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
  
  // General BFS traversal supporting all directions
  std::queue<std::pair<uint64_t, std::unique_ptr<cypher::Path>>> queue;
  
  auto start_path = std::make_unique<cypher::Path>();
  cypher::Node start_node;
  start_node.id = start_node_id;
  start_path->elements.push_back(start_node);
  queue.push({start_node_id, std::move(start_path)});
  
  uint32_t visited = 0;
  const uint32_t kMaxVisited = 10000;
  
  while (!queue.empty() && visited < kMaxVisited) {
    auto [current_id, current_path] = std::move(queue.front());
    queue.pop();
    
    if (current_path->Length() >= max_depth) {
      paths->push_back(std::move(current_path));
      continue;
    }
    
    // Scan edges based on direction
    std::vector<EdgeScanEntry> all_edges;
    for (uint16_t et : edge_types) {
      if (direction == cypher::Direction::OUTGOING || 
          direction == cypher::Direction::BOTH) {
        std::vector<EdgeScanEntry> out_edges;
        Status s = storage_client_->ScanOutEdges(current_id, et, as_of_ts, &out_edges);
        if (s.ok()) {
          all_edges.insert(all_edges.end(), out_edges.begin(), out_edges.end());
        }
      }
      if (direction == cypher::Direction::INCOMING || 
          direction == cypher::Direction::BOTH) {
        std::vector<EdgeScanEntry> in_edges;
        Status s = storage_client_->ScanInEdges(current_id, et, as_of_ts, &in_edges);
        if (s.ok()) {
          all_edges.insert(all_edges.end(), in_edges.begin(), in_edges.end());
        }
      }
    }
    
    // Limit branch factor
    if (all_edges.size() > max_branch && max_branch > 0) {
      all_edges.resize(max_branch);
    }
    
    for (const auto& edge : all_edges) {
      auto new_path = std::make_unique<cypher::Path>(*current_path);
      cypher::Relationship rel;
      rel.id = edge.key.entity_id() ^ edge.target_id ^ edge.edge_type;
      rel.start_id = current_id;
      rel.end_id = edge.target_id;
      rel.type = std::to_string(edge.edge_type);
      new_path->elements.push_back(rel);
      
      cypher::Node target_node;
      target_node.id = edge.target_id;
      new_path->elements.push_back(target_node);
      
      queue.push({edge.target_id, std::move(new_path)});
    }
    
    visited++;
  }
  
  return Status::OK();
}

Status DistributedExecutor::TemporalQuery(
    uint64_t entity_id,
    EntityType entity_type,
    DistributedExecutionContext::Consistency consistency,
    std::vector<cypher::VersionedEntity>* versions) {
  
  // Get partition for entity
  uint32_t partition_id = router_->GetPartitionId(entity_id);
  
  // Leader check
  std::string leader_address;
  Status rs = router_->GetStorageNode(partition_id, &leader_address);
  if (!rs.ok()) return rs;
  rs = router_->CheckIsLeader(partition_id, leader_address);
  if (!rs.ok()) return rs;
  
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
    ve.is_deleted = (desc.GetKind() == cedar::EntryKind::Tombstone);
    // Descriptor properties parsing requires storage layer access for ExternalRef entries
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
  
  // Leader check
  std::string leader_address;
  Status rs = router_->GetStorageNode(partition_id, &leader_address);
  if (!rs.ok()) return rs;
  rs = router_->CheckIsLeader(partition_id, leader_address);
  if (!rs.ok()) return rs;
  
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
  entity->is_deleted = (results[0].second.GetKind() == cedar::EntryKind::Tombstone);
  // Descriptor properties parsing requires storage layer access for ExternalRef entries
  
  return Status::OK();
}

bool DistributedExecutor::IsSinglePartitionQuery(
    const std::string& query,
    const std::unordered_map<std::string, cypher::Value>& parameters,
    uint32_t* partition_id) {
  
  cypher::CypherParser parser(query);
  auto ast = parser.ParseStatement();
  if (!ast) {
    return false;
  }
  
  // Extract entity IDs from MATCH ... (n {id: X}) patterns
  std::vector<uint64_t> entity_ids;
  for (const auto& clause : ast->clauses) {
    if (clause->clause_type != cypher::ClauseType::MATCH) {
      continue;
    }
    auto* match = static_cast<cypher::MatchClause*>(clause.get());
    for (const auto& pattern : match->patterns) {
      for (const auto& element : pattern.elements) {
        if (!std::holds_alternative<cypher::NodePattern>(element)) {
          continue;
        }
        const auto& node = std::get<cypher::NodePattern>(element);
        auto it = node.properties.find("id");
        if (it == node.properties.end()) {
          continue;
        }
        if (it->second->expr_type != cypher::ExprType::LITERAL) {
          continue;
        }
        auto* literal = static_cast<cypher::LiteralExpr*>(it->second.get());
        if (literal->value.IsInt()) {
          entity_ids.push_back(static_cast<uint64_t>(literal->value.GetInt()));
        }
      }
    }
  }
  
  // Also check WHERE id(n) = X patterns
  for (const auto& clause : ast->clauses) {
    if (clause->clause_type != cypher::ClauseType::WHERE) {
      continue;
    }
    auto* where = static_cast<cypher::WhereClause*>(clause.get());
    if (!where->condition) {
      continue;
    }
    if (where->condition->expr_type != cypher::ExprType::COMPARISON) {
      continue;
    }
    auto* comp = static_cast<cypher::ComparisonExpr*>(where->condition.get());
    if (comp->op != cypher::ComparisonExpr::EQ) {
      continue;
    }
    if (comp->left->expr_type != cypher::ExprType::PROPERTY ||
        comp->right->expr_type != cypher::ExprType::LITERAL) {
      continue;
    }
    auto* prop = static_cast<cypher::PropertyExpr*>(comp->left.get());
    if (prop->property != "id") {
      continue;
    }
    auto* literal = static_cast<cypher::LiteralExpr*>(comp->right.get());
    if (literal->value.IsInt()) {
      entity_ids.push_back(static_cast<uint64_t>(literal->value.GetInt()));
    }
  }
  
  if (entity_ids.empty()) {
    return false;
  }
  
  if (!router_) {
    return false;
  }
  
  uint32_t target_partition = router_->GetPartitionId(entity_ids[0]);
  for (size_t i = 1; i < entity_ids.size(); ++i) {
    if (router_->GetPartitionId(entity_ids[i]) != target_partition) {
      return false;
    }
  }
  
  *partition_id = target_partition;
  return true;
}

Status DistributedExecutor::ExecuteSinglePartition(
    const std::string& query,
    const std::unordered_map<std::string, cypher::Value>& parameters,
    uint32_t partition_id,
    DistributedExecutionContext* ctx,
    cypher::ResultSet* result) {
  
  // Leader check
  std::string leader_address;
  Status rs = router_->GetStorageNode(partition_id, &leader_address);
  if (!rs.ok()) return rs;
  rs = router_->CheckIsLeader(partition_id, leader_address);
  if (!rs.ok()) return rs;
  
  // Send query to specific storage node
  // Storage nodes have embedded query capabilities
  auto node_client = storage_client_->GetNodeClient(partition_id);
  if (!node_client) {
    return Status::NotFound("Storage node not found");
  }
  
  // Execute query on the single partition
  Status s = node_client->ExecuteSubQuery(query, parameters, result);
  if (!s.ok()) {
      return Status::IOError("Query execution failed on partition " +
                             std::to_string(partition_id) + ": " + s.ToString());
  }

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
    Status s = router_->GetStorageNode(task.partition_id, &task.storage_node);
    if (!s.ok()) {
      tasks.clear();
      return tasks;
    }
    s = router_->CheckIsLeader(task.partition_id, task.storage_node);
    if (!s.ok()) {
      tasks.clear();
      return tasks;
    }
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
  
  // Determine partition for start node
  uint32_t partition_id = router_->GetPartitionId(start_node_id);
  
  // Leader check
  std::string leader_address;
  Status rs = router_->GetStorageNode(partition_id, &leader_address);
  if (!rs.ok()) return rs;
  rs = router_->CheckIsLeader(partition_id, leader_address);
  if (!rs.ok()) return rs;
  
  // Optimized traversal using CedarKey's physical clustering
  // Since edges are stored with entity_id prefix, we can do efficient scans
  
  std::queue<std::pair<uint64_t, std::unique_ptr<cypher::Path>>> queue;
  
  // Start node
  auto start_path = std::make_unique<cypher::Path>();
  cypher::Node start_node;
  start_node.id = start_node_id;
  start_path->elements.push_back(start_node);
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
      // Add edge and target node to path
      cypher::Relationship rel;
      rel.id = edge.key.entity_id() ^ edge.target_id ^ edge.edge_type;
      rel.start_id = current_id;
      rel.end_id = edge.target_id;
      rel.type = std::to_string(edge.edge_type);
      new_path->elements.push_back(rel);
      
      cypher::Node target_node;
      target_node.id = edge.target_id;
      new_path->elements.push_back(target_node);
      
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
  std::string fp = cedar::cypher::ComputeFingerprint(query);

  std::shared_lock<std::shared_mutex> lock(mutex_);

  auto it = cache_.find(fp);
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
  std::string fp = cedar::cypher::ComputeFingerprint(query);

  std::unique_lock<std::shared_mutex> lock(mutex_);

  EvictIfNeeded();

  CacheEntry entry;
  entry.plan = std::move(plan);
  entry.last_access = ++access_counter_;
  entry.access_count = 1;

  cache_[fp] = std::move(entry);
}

void QueryPlanCache::Invalidate(const std::string& query) {
  std::string fp = cedar::cypher::ComputeFingerprint(query);
  std::unique_lock<std::shared_mutex> lock(mutex_);
  cache_.erase(fp);
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
