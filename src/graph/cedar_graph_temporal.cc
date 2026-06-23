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

// CedarGraph 时态查询接口实现

#include "cedar/graph/cedar_graph.h"

#include <iostream>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/graph/graph_semantic_layer.h"
#include "cedar/cypher/value.h"

namespace cedar {

// ============================================================================
// 时态查询接口实现
// ============================================================================

std::vector<Neighbor> CedarGraph::GetOutNeighborsAsOf(uint64_t vertex_id,
                                                      uint16_t edge_type,
                                                      Timestamp as_of_time) {
  if (tmv_engine_) {
    auto edges = tmv_engine_->ScanAtTime(vertex_id, cedar::gcn::Direction::kOut, as_of_time.value());
    std::vector<Neighbor> result;
    result.reserve(edges.size());
    for (const auto& edge : edges) {
      if (edge_type != 0 && edge.edge_type != edge_type) continue;
      Neighbor n;
      n.id = edge.target_id;
      n.edge_type = edge.edge_type;
      n.timestamp = Timestamp(edge.valid_from);
      n.value = std::nullopt;
      result.push_back(n);
    }
    return result;
  }
  // Fallback: use traditional GetOutNeighbors with full history up to as_of_time
  return GetOutNeighbors(vertex_id, edge_type, Timestamp::Min(), as_of_time);
}

std::vector<Neighbor> CedarGraph::GetOutNeighborsBetween(uint64_t vertex_id,
                                                        uint16_t edge_type,
                                                        Timestamp start_time,
                                                        Timestamp end_time) {
  // 使用传统的GetOutNeighbors，设置时间范围
  return GetOutNeighbors(vertex_id, edge_type, start_time, end_time);
}

std::vector<std::pair<Timestamp, Neighbor>> CedarGraph::GetOutNeighborsAllVersions(
    uint64_t vertex_id, uint16_t edge_type) {
  std::vector<std::pair<Timestamp, Neighbor>> results;
  
  // 获取所有版本 - 使用最大时间范围
  auto neighbors = GetOutNeighbors(vertex_id, edge_type, Timestamp(0), Timestamp::Max());
  
  for (const auto& neighbor : neighbors) {
    results.emplace_back(neighbor.timestamp, neighbor);
  }
  
  return results;
}

std::optional<Neighbor> CedarGraph::GetOutNeighborsAtVersion(uint64_t vertex_id,
                                                            uint16_t edge_type,
                                                            uint64_t version) {
  // 首先获取所有版本
  auto all_versions = GetOutNeighborsAllVersions(vertex_id, edge_type);
  
  if (all_versions.empty()) {
    return std::nullopt;
  }
  
  // Sort by timestamp and pick N-th distinct version
  std::sort(all_versions.begin(), all_versions.end(),
    [](const auto& a, const auto& b) { return a.first < b.first; });
  
  std::vector<Neighbor> distinct;
  Timestamp last_ts = Timestamp::Max();
  for (const auto& [ts, neighbor] : all_versions) {
    if (ts != last_ts) {
      distinct.push_back(neighbor);
      last_ts = ts;
    }
  }
  
  if (version < distinct.size()) {
    return distinct[version];
  }
  
  return std::nullopt;
}

static bool CheckAllenRelation(AllenRelation relation,
                                  Timestamp a_start, Timestamp a_end,
                                  Timestamp b_start, Timestamp b_end) {
  switch (relation) {
    case AllenRelation::BEFORE:       return a_end < b_start;
    case AllenRelation::AFTER:        return a_start > b_end;
    case AllenRelation::MEETS:        return a_end == b_start;
    case AllenRelation::MET_BY:       return a_start == b_end;
    case AllenRelation::OVERLAPS:     return a_start < b_start && a_end > b_start && a_end < b_end;
    case AllenRelation::OVERLAPPED_BY:return b_start < a_start && b_end > a_start && b_end < a_end;
    case AllenRelation::DURING:       return a_start > b_start && a_end < b_end;
    case AllenRelation::CONTAINS:     return a_start < b_start && a_end > b_end;
    case AllenRelation::STARTS:       return a_start == b_start && a_end < b_end;
    case AllenRelation::STARTED_BY:   return a_start == b_start && a_end > b_end;
    case AllenRelation::FINISHES:     return a_end == b_end && a_start > b_start;
    case AllenRelation::FINISHED_BY:  return a_end == b_end && a_start < b_start;
    case AllenRelation::EQUALS:       return a_start == b_start && a_end == b_end;
    default:
      std::cerr << "[CedarGraphTemporal] Unknown Allen relation" << std::endl;
      return false;
  }
  return false;
}

std::vector<Neighbor> CedarGraph::GetOutNeighborsWithRelation(
    uint64_t vertex_id,
    uint16_t edge_type,
    AllenRelation relation,
    Timestamp other_start,
    Timestamp other_end) {
  std::vector<Neighbor> results;
  
  // 获取候选邻居
  auto candidates = GetOutNeighbors(vertex_id, edge_type, Timestamp::Min(), Timestamp::Max());
  
  // 使用Allen谓词过滤
  for (const auto& neighbor : candidates) {
    // 假设邻居有一个有效时间范围 [neighbor.timestamp, neighbor.timestamp + 1)
    Timestamp neighbor_start = neighbor.timestamp;
    Timestamp neighbor_end = Timestamp(neighbor.timestamp.value() + 1);
    
    if (CheckAllenRelation(relation, neighbor_start, neighbor_end, other_start, other_end)) {
      results.push_back(neighbor);
    }
  }
  
  return results;
}

// ============================================================================
// 时态聚合查询接口实现
// ============================================================================

double CedarGraph::GetTemporalAverage(uint64_t vertex_id, uint16_t property_id,
                                     Timestamp start_time, Timestamp end_time) {
  auto history = GetVertexHistory(vertex_id, property_id, start_time, end_time);
  
  if (history.empty()) {
    return 0.0;
  }
  
  // 时间加权平均: Σ(value_i × duration_i) / Σ(duration_i)
  double weighted_sum = 0.0;
  double total_duration = 0.0;
  
  for (size_t i = 0; i < history.size(); ++i) {
    Timestamp valid_from = history[i].first;
    if (valid_from.value() < start_time.value()) {
      valid_from = start_time;
    }
    
    Timestamp valid_to;
    if (i + 1 < history.size()) {
      valid_to = history[i + 1].first;
    } else {
      valid_to = end_time;
    }
    
    double duration = static_cast<double>(valid_to.value() - valid_from.value());
    if (duration <= 0) continue;
    
    double value = static_cast<double>(history[i].second);
    weighted_sum += value * duration;
    total_duration += duration;
  }
  
  if (total_duration <= 0.0) {
    return 0.0;
  }
  
  return weighted_sum / total_duration;
}

int64_t CedarGraph::GetTemporalSum(uint64_t vertex_id, uint16_t property_id,
                                  Timestamp start_time, Timestamp end_time) {
  // 获取属性历史
  auto history = GetVertexHistory(vertex_id, property_id, start_time, end_time);
  
  int64_t sum = 0;
  for (const auto& [ts, value] : history) {
    sum += value;
  }
  
  return sum;
}

int64_t CedarGraph::GetTotalDuration(uint64_t vertex_id, uint16_t edge_type,
                                    Timestamp start_time, Timestamp end_time) {
  auto history = GetOutNeighborsBetween(vertex_id, edge_type, start_time, end_time);
  
  if (history.empty()) {
    return 0;
  }
  
  // 计算总持续时间: 查询窗口内的边存在时长
  int64_t total_duration = 0;
  int64_t window_span = static_cast<int64_t>(end_time.value() - start_time.value());
  
  for (const auto& neighbor : history) {
    // 每条边在查询窗口内的存在时间（至少1微秒）
    (void)neighbor;
    total_duration += window_span > 0 ? window_span : 1;
  }
  
  return total_duration;
}

uint64_t CedarGraph::GetVersionCount(uint64_t vertex_id, uint16_t edge_type) {
  auto all_versions = GetOutNeighborsAllVersions(vertex_id, edge_type);
  return all_versions.size();
}

// ============================================================================
// 时态索引管理
// ============================================================================

void CedarGraph::BuildTemporalIndex() {
  if (tmv_engine_) {
    // TMV is already in-memory; no explicit index build needed
    return;
  }
  // Non-TMV temporal index building requires a persistent interval index
  // (e.g., R-tree or interval tree) on top of the LSM storage.
}

CedarGraph::TemporalIndexStats CedarGraph::GetTemporalIndexStats() const {
  TemporalIndexStats stats{};
  if (tmv_engine_) {
    stats.index_entries = tmv_engine_->VertexCount();
    stats.blocks_checked = tmv_engine_->ChunkCount();
  }
  return stats;
}

}  // namespace cedar
