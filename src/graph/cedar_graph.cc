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

#include "cedar/graph/cedar_graph.h"

#include <cstdlib>
#include <queue>
#include <thread>
#include <vector>
#include <unordered_set>
#include <future>

#include "cedar/graph/graph_semantic_layer.h"
#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/value.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// Constructor
CedarGraph::CedarGraph(CedarGraphStorage* storage) 
    : storage_(storage) {
  if (storage_) {
    semantic_layer_ = std::make_unique<GraphSemanticLayer>(storage_);
  }
}

// Set TMV engine for temporal queries
void CedarGraph::SetTMVEngine(cedar::gcn::TMVEngine* engine) {
  tmv_engine_ = engine;
}

// Destructor
CedarGraph::~CedarGraph() = default;

// Get neighbors (outgoing edges)
std::vector<Neighbor> CedarGraph::GetOutNeighbors(uint64_t vertex_id,
                                                  uint16_t edge_type,
                                                  Timestamp start_time,
                                                  Timestamp end_time) {
  std::vector<Neighbor> result;
  
  if (!storage_) {
    return result;
  }
  
  (void)start_time;
  
  // Scan EdgeOut index for all edges from vertex_id, filtering by edge_type
  auto edges = storage_->ScanEdgesWithFolding(vertex_id, EntityType::EdgeOut, edge_type, end_time);
  for (const auto& e : edges) {
    result.push_back(Neighbor{e.target_id, e.edge_type, e.timestamp, std::nullopt});
  }
  
  return result;
}

// Get neighbors (incoming edges)
std::vector<Neighbor> CedarGraph::GetInNeighbors(uint64_t vertex_id,
                                                 uint16_t edge_type,
                                                 Timestamp start_time,
                                                 Timestamp end_time) {
  std::vector<Neighbor> result;
  
  if (!storage_) {
    return result;
  }
  
  (void)start_time;
  
  // Scan EdgeIn index for all edges pointing to vertex_id
  auto edges = storage_->ScanEdgesWithFolding(vertex_id, EntityType::EdgeIn, edge_type, end_time);
  for (const auto& e : edges) {
    result.push_back(Neighbor{e.target_id, e.edge_type, e.timestamp, std::nullopt});
  }
  
  return result;
}

// ========== Transaction-aware Query Implementation ==========

std::vector<Neighbor> CedarGraph::GetOutNeighborsInTxn(OCCTransaction* txn,
                                                       uint64_t vertex_id,
                                                       uint16_t edge_type) {
  std::vector<Neighbor> result;
  
  if (!storage_ || !txn) {
    return result;
  }
  
  // 获取事务的读时间戳作为快照点
  Timestamp read_ts = txn->GetReadTimestamp();
  
  // 使用 ScanEdgesWithFolding 获取所有出边（支持多边）
  auto edges = storage_->ScanEdgesWithFolding(vertex_id, EntityType::EdgeOut, edge_type, read_ts);
  for (const auto& e : edges) {
    result.push_back(Neighbor{e.target_id, e.edge_type, e.timestamp, std::nullopt});
  }
  
  return result;
}

std::vector<std::vector<uint64_t>> CedarGraph::BfsInTxn(OCCTransaction* txn,
                                                        uint64_t start,
                                                        uint16_t edge_type,
                                                        size_t max_depth) {
  std::vector<std::vector<uint64_t>> result;
  
  if (!storage_ || !txn || max_depth == 0) {
    return result;
  }
  
  // 获取事务的读时间戳
  Timestamp read_ts = txn->GetReadTimestamp();
  
  std::unordered_set<uint64_t> visited;
  std::vector<uint64_t> current_level = {start};
  visited.insert(start);
  
  for (size_t depth = 0; depth < max_depth && !current_level.empty(); ++depth) {
    std::vector<uint64_t> next_level;
    std::vector<uint64_t> current_result;
    
    for (uint64_t vertex_id : current_level) {
      // 在事务快照内查询邻居
      auto neighbors = GetOutNeighborsInTxn(txn, vertex_id, edge_type);
      
      for (const auto& neighbor : neighbors) {
        if (visited.insert(neighbor.id).second) {
          next_level.push_back(neighbor.id);
          current_result.push_back(neighbor.id);
        }
      }
    }
    
    if (!current_result.empty()) {
      result.push_back(std::move(current_result));
    }
    
    current_level = std::move(next_level);
  }
  
  return result;
}

// Get vertex property history
std::vector<std::pair<Timestamp, int32_t>> CedarGraph::GetVertexHistory(
    uint64_t vertex_id, uint16_t column_id,
    Timestamp start_time, Timestamp end_time) {
  std::vector<std::pair<Timestamp, int32_t>> history;

  if (!storage_) {
    return history;
  }

  (void)column_id;
  
  auto versions = storage_->Scan(vertex_id, start_time, end_time);

  for (const auto& [ts, desc] : versions) {
    if (auto int_val = desc.AsInlineInt()) {
      history.emplace_back(ts, *int_val);
    }
  }

  return history;
}

// Batch get neighbors
std::vector<BatchNeighborResult> CedarGraph::BatchGetNeighbors(
    const std::vector<uint64_t>& vertex_ids,
    uint16_t edge_type,
    const PushdownPredicate& predicate,
    size_t num_threads) {
  if (semantic_layer_) {
    return semantic_layer_->BatchGetNeighbors(vertex_ids, edge_type, predicate, num_threads);
  }
  
  // Fallback: use storage directly
  std::vector<BatchNeighborResult> results;
  for (uint64_t vertex_id : vertex_ids) {
    BatchNeighborResult result(vertex_id);
    result.neighbors = GetOutNeighbors(vertex_id, edge_type, 
                                       predicate.time_start.value_or(0),
                                       predicate.time_end.value_or(Timestamp::Max()));
    results.push_back(std::move(result));
  }
  return results;
}

// Batch get neighbors (simplified version)
std::vector<BatchNeighborResult> CedarGraph::BatchGetNeighbors(
    const std::vector<uint64_t>& vertex_ids,
    uint16_t edge_type,
    size_t num_threads) {
  return BatchGetNeighbors(vertex_ids, edge_type, PushdownPredicate(), num_threads);
}

// BFS traversal (with time range)
std::vector<std::vector<uint64_t>> CedarGraph::Bfs(
    uint64_t start, uint16_t edge_type, size_t max_depth,
    Timestamp start_time, Timestamp end_time) {
  PushdownPredicate predicate;
  predicate.time_start = start_time;
  predicate.time_end = end_time;
  return Bfs(start, edge_type, max_depth, predicate, 0);
}

// BFS traversal (with pushdown predicate)
std::vector<std::vector<uint64_t>> CedarGraph::Bfs(
    uint64_t start, uint16_t edge_type, size_t max_depth,
    const PushdownPredicate& predicate,
    size_t num_threads) {
  if (semantic_layer_) {
    return semantic_layer_->BfsWithPushdown(start, edge_type, max_depth, predicate, num_threads);
  }
  return {};
}

// Degree centrality calculation
std::unordered_map<uint64_t, size_t> CedarGraph::DegreeCentrality(
    const std::vector<uint64_t>& vertex_ids,
    uint16_t edge_type,
    const PushdownPredicate& predicate) {
  if (semantic_layer_) {
    return semantic_layer_->DegreeCentrality(vertex_ids, edge_type, predicate);
  }
  return {};
}

// Connected components
std::vector<std::vector<uint64_t>> CedarGraph::ConnectedComponents(
    const std::vector<uint64_t>& seed_vertices,
    uint16_t edge_type) {
  if (semantic_layer_) {
    return semantic_layer_->ConnectedComponents(seed_vertices, edge_type);
  }
  return {};
}

// K-hop neighbors query
std::vector<uint64_t> CedarGraph::GetKHopNeighbors(
    uint64_t vertex_id, uint16_t edge_type, size_t k,
    Timestamp start_time, Timestamp end_time) {
  std::vector<uint64_t> result;
  
  if (!storage_ || k == 0) {
    return result;
  }

  std::unordered_set<uint64_t> visited;
  std::vector<uint64_t> current_level = {vertex_id};
  visited.insert(vertex_id);

  for (size_t hop = 0; hop < k; ++hop) {
    auto batch_results = BatchGetNeighbors(current_level, edge_type, 
                                           PushdownPredicate(), 4);
    
    std::vector<uint64_t> next_level;
    for (const auto& batch_result : batch_results) {
      for (const auto& neighbor : batch_result.neighbors) {
        if (visited.insert(neighbor.id).second) {
          next_level.push_back(neighbor.id);
          if (hop == k - 1) {
            result.push_back(neighbor.id);
          }
        }
      }
    }
    
    current_level = std::move(next_level);
    if (current_level.empty()) {
      break;
    }
  }

  return result;
}

// Get all versions of an edge
std::vector<Neighbor> CedarGraph::GetEdgeHistory(
    uint64_t src_id, uint64_t dst_id, uint16_t edge_type,
    Timestamp start_time, Timestamp end_time) {
  std::vector<Neighbor> history;

  if (!storage_) {
    return history;
  }

  auto results = storage_->Scan(src_id, start_time, end_time);

  for (const auto& [ts, desc] : results) {
    if (auto int_val = desc.AsInlineInt()) {
      history.emplace_back(dst_id, edge_type, ts, *int_val);
    }
  }

  return history;
}

// ========== Configuration Interface ==========

void CedarGraph::EnableBlockCache(bool enable) {
  if (semantic_layer_) {
    semantic_layer_->EnableBlockCache(enable);
  }
}

void CedarGraph::EnablePrefetch(bool enable) {
  if (semantic_layer_) {
    semantic_layer_->EnablePrefetch(enable);
  }
}

void CedarGraph::GetCacheStats(size_t* cache_size, double* hit_rate) const {
  if (semantic_layer_) {
    semantic_layer_->GetCacheStats(cache_size, hit_rate);
  } else {
    *cache_size = 0;
    *hit_rate = 0.0;
  }
}

void CedarGraph::ClearCache() {
  if (semantic_layer_) {
    semantic_layer_->ClearCache();
  }
}

// ========== Cypher Query Interface ==========

cypher::ResultSet CedarGraph::ExecuteCypher(const std::string& query) {
  (void)query;
  return cypher::ResultSet();
}

std::string CedarGraph::ExplainCypher(const std::string& query) {
  (void)query;
  return "CedarGraph Cypher not yet implemented";
}

bool CedarGraph::IsValidCypher(const std::string& query) {
  (void)query;
  return false;
}

// ========== Temporal Query Interface ==========



// ========== Entity Enumeration ==========

std::vector<uint64_t> CedarGraph::GetAllEntities(
    uint64_t min_entity_id,
    uint64_t max_entity_id,
    uint64_t step) {
  std::vector<uint64_t> entities;
  
  if (step == 0) step = 1;
  if (min_entity_id > max_entity_id) return entities;
  
  size_t estimated_count = (max_entity_id - min_entity_id) / step + 1;
  entities.reserve(std::min(estimated_count, static_cast<size_t>(1000000)));
  
  for (uint64_t entity_id = min_entity_id; entity_id <= max_entity_id; entity_id += step) {
    entities.push_back(entity_id);
  }
  
  return entities;
}

std::vector<uint64_t> CedarGraph::ScanVertices(Timestamp start, Timestamp end) {
  std::vector<uint64_t> result;
  if (!storage_) return result;

  // Use the same configurable range as NodeScan
  uint64_t min_entity_id = 1;
  uint64_t max_entity_id = 1000;
  const char* env_max = std::getenv("CEDAR_SCAN_MAX_ENTITIES");
  if (env_max) {
    max_entity_id = std::max(min_entity_id, static_cast<uint64_t>(std::strtoull(env_max, nullptr, 10)));
  }

  for (uint64_t entity_id = min_entity_id; entity_id <= max_entity_id; ++entity_id) {
    auto versions = storage_->Scan(entity_id, start, end);
    if (!versions.empty()) {
      result.push_back(entity_id);
    }
  }

  return result;
}

std::vector<std::pair<Timestamp, Descriptor>> CedarGraph::GetTimeSeries(
    uint64_t entity_id,
    Timestamp start_time,
    Timestamp end_time) {
  if (!storage_) {
    return {};
  }
  return storage_->Scan(entity_id, start_time, end_time);
}

// ========== Transaction Interface ==========

OCCTransaction* CedarGraph::BeginTransaction(const TransactionOptions* options) {
  if (!storage_) {
    return nullptr;
  }
  
  // 委托给存储层的事务管理
  return storage_->BeginTransaction(options);
}

}  // namespace cedar
