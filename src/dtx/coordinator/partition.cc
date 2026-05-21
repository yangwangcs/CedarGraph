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

#include "cedar/dtx/partition.h"

#include <algorithm>
#include <sstream>

namespace cedar {
namespace dtx {

// =============================================================================
// PartitionMeta 实现
// =============================================================================

std::string PartitionMeta::Serialize() const {
  std::ostringstream oss;
  oss << partition_id << ","
      << primary_node << ","
      << vertex_count.load() << ","
      << edge_count.load() << ","
      << txn_rate.load() << ","
      << conflict_rate.load();
  
  // 序列化子图列表
  oss << "," << subgraphs.size();
  for (const auto& sid : subgraphs) {
    oss << "," << sid;
  }
  
  return oss.str();
}

PartitionMeta PartitionMeta::Deserialize(const std::string& data) {
  PartitionMeta meta;
  std::istringstream iss(data);
  std::string token;
  
  auto safe_getline = [&iss, &token]() -> bool {
    if (!std::getline(iss, token, ',')) return false;
    return !token.empty();
  };
  
  try {
    // 解析基本字段
    if (!safe_getline()) return PartitionMeta{};
    meta.partition_id = static_cast<PartitionID>(std::stoul(token));
    
    if (!safe_getline()) return PartitionMeta{};
    meta.primary_node = static_cast<NodeID>(std::stoul(token));
    
    if (!safe_getline()) return PartitionMeta{};
    meta.vertex_count.store(std::stoull(token));
    
    if (!safe_getline()) return PartitionMeta{};
    meta.edge_count.store(std::stoull(token));
    
    if (!safe_getline()) return PartitionMeta{};
    meta.txn_rate.store(std::stoull(token));
    
    if (!safe_getline()) return PartitionMeta{};
    meta.conflict_rate.store(std::stoull(token));
    
    // 解析子图列表
    if (!safe_getline()) return PartitionMeta{};
    size_t subgraph_count = std::stoul(token);
    
    for (size_t i = 0; i < subgraph_count; ++i) {
      if (!safe_getline()) return PartitionMeta{};
      meta.subgraphs.insert(static_cast<SubgraphID>(std::stoul(token)));
    }
  } catch (const std::exception& e) {
    // Corrupted or tampered data: return default-constructed meta
    // Caller should check partition_id == 0 to detect failure
    return PartitionMeta{};
  }
  
  return meta;
}

// =============================================================================
// PartitionLoadStats 实现
// =============================================================================

double PartitionLoadStats::ComputeLoadScore() const {
  // 综合负载分数计算
  // 考虑：数据大小、事务率、冲突率、延迟
  
  double score = 0.0;
  
  // 数据大小权重 (30%)
  double data_score = std::min(1.0, static_cast<double>(data_size_bytes) / (1ULL << 30));  // 以1GB为基准
  score += data_score * 0.3;
  
  // 事务率权重 (30%)
  double txn_score = std::min(1.0, static_cast<double>(txn_count_1min) / 100000.0);  // 以10万/分钟为基准
  score += txn_score * 0.3;
  
  // 冲突率权重 (20%)
  double conflict_rate = (txn_count_1min > 0) ? 
      static_cast<double>(conflict_count_1min) / txn_count_1min : 0.0;
  score += conflict_rate * 0.2;
  
  // 延迟权重 (20%)
  double latency_score = std::min(1.0, p99_latency_ms / 100.0);  // 以100ms为基准
  score += latency_score * 0.2;
  
  return score;
}

// =============================================================================
// SubgraphBoundary 实现
// =============================================================================

bool SubgraphBoundary::IsLocalTransaction(const std::vector<CedarKey>& keys) const {
  if (keys.empty()) return true;
  
  // 获取第一个Key的分区
  PartitionID first_partition = keys[0].part_id();
  
  // 检查所有Key是否都在同一分区
  for (const auto& key : keys) {
    if (key.part_id() != first_partition) {
      return false;
    }
  }
  
  return true;
}

bool SubgraphBoundary::IsCrossPartition(const std::vector<CedarKey>& keys) const {
  return !IsLocalTransaction(keys);
}

std::vector<PartitionID> SubgraphBoundary::GetInvolvedPartitions(
    const std::vector<CedarKey>& keys) const {
  
  std::unordered_set<PartitionID> unique_partitions;
  
  for (const auto& key : keys) {
    unique_partitions.insert(key.part_id());
  }
  
  return std::vector<PartitionID>(unique_partitions.begin(), unique_partitions.end());
}

// =============================================================================
// PartitionManager 实现
// =============================================================================

PartitionManager::PartitionManager(const DTxConfig& config) 
    : config_(config) {}

Status PartitionManager::Initialize(
    PartitionID num_partitions,
    std::unique_ptr<PartitionStrategy> strategy) {
  
  if (num_partitions == 0 || num_partitions > 65535) {
    return Status::InvalidArgument("PartitionManager", 
                                   "num_partitions must be in [1, 65535]");
  }
  
  num_partitions_ = num_partitions;
  strategy_ = std::move(strategy);
  
  // 初始化分区元数据
  std::unique_lock<std::shared_mutex> lock(meta_mutex_);
  for (PartitionID i = 0; i < num_partitions_; ++i) {
    auto meta = std::make_shared<PartitionMeta>(i);
    partition_metas_[i] = meta;
  }
  
  return Status::OK();
}

Status PartitionManager::InitializeDualMode(
    const DualModePartitionStrategy::Config& config) {
  
  if (config.num_partitions == 0 || config.num_partitions > 65535) {
    return Status::InvalidArgument("PartitionManager", 
                                   "num_partitions must be in [1, 65535]");
  }
  
  num_partitions_ = config.num_partitions;
  
  // 创建双模式分区策略
  strategy_ = std::make_unique<DualModePartitionStrategy>(config);
  
  // 初始化分区元数据
  std::unique_lock<std::shared_mutex> lock(meta_mutex_);
  for (PartitionID i = 0; i < num_partitions_; ++i) {
    auto meta = std::make_shared<PartitionMeta>(i);
    partition_metas_[i] = meta;
  }
  
  return Status::OK();
}

DualModePartitionStrategy* PartitionManager::GetDualModeStrategy() const {
  return dynamic_cast<DualModePartitionStrategy*>(strategy_.get());
}

Status PartitionManager::SetPartitionMode(DualModePartitionStrategy::Mode mode) {
  auto* dual_mode = GetDualModeStrategy();
  if (!dual_mode) {
    return Status::InvalidArgument("PartitionManager", 
                                   "Not using DualModePartitionStrategy");
  }
  
  dual_mode->SetMode(mode);
  return Status::OK();
}

DualModePartitionStrategy::Mode PartitionManager::GetPartitionMode() const {
  auto* dual_mode = GetDualModeStrategy();
  if (!dual_mode) {
    return DualModePartitionStrategy::Mode::STATIC_HASH;  // 默认
  }
  
  return dual_mode->GetMode();
}

void PartitionManager::ReportQueryStats(bool is_temporal_query, bool has_locality) {
  auto* dual_mode = GetDualModeStrategy();
  if (dual_mode) {
    dual_mode->UpdateQueryStats(is_temporal_query, has_locality);
  }
}

PartitionID PartitionManager::GetPartition(const CedarKey& key) const {
  // 如果Key已经有分区ID，直接返回
  if (key.part_id() != kInvalidPartitionID && key.part_id() < num_partitions_) {
    return key.part_id();
  }
  
  // 否则使用分区策略计算
  if (strategy_) {
    return strategy_->ComputePartition(key, num_partitions_);
  }
  
  // 默认哈希分区
  return HashToPartition(key, num_partitions_);
}

std::shared_ptr<PartitionMeta> PartitionManager::GetPartitionMeta(
    PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(meta_mutex_);
  
  auto it = partition_metas_.find(pid);
  if (it != partition_metas_.end()) {
    return it->second;
  }
  
  return nullptr;
}

NodeID PartitionManager::GetPartitionLeader(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(meta_mutex_);
  auto it = partition_metas_.find(pid);
  return (it != partition_metas_.end()) ? it->second->primary_node : kInvalidNodeID;
}

Status PartitionManager::SetPartitionLeader(PartitionID pid, NodeID node_id) {
  std::unique_lock<std::shared_mutex> lock(meta_mutex_);
  std::unique_lock<std::shared_mutex> node_lock(node_partition_mutex_);
  
  auto it = partition_metas_.find(pid);
  if (it == partition_metas_.end()) {
    return Status::NotFound("PartitionManager", "Partition not found");
  }
  
  NodeID old_node = it->second->primary_node;
  it->second->primary_node = node_id;
  
  // Atomically remove from old node, add to new node
  if (old_node != kInvalidNodeID && old_node != node_id) {
    auto old_it = node_partitions_.find(old_node);
    if (old_it != node_partitions_.end()) {
      auto& vec = old_it->second;
      vec.erase(std::remove(vec.begin(), vec.end(), pid), vec.end());
      if (vec.empty()) {
        node_partitions_.erase(old_it);
      }
    }
  }
  
  if (node_id != kInvalidNodeID) {
    auto& vec = node_partitions_[node_id];
    if (std::find(vec.begin(), vec.end(), pid) == vec.end()) {
      vec.push_back(pid);
    }
  }
  
  return Status::OK();
}

Status PartitionManager::AssignPartitionsToNodes(const std::vector<NodeID>& node_ids) {
  if (node_ids.empty()) {
    return Status::InvalidArgument("PartitionManager", "node_ids cannot be empty");
  }

  std::unique_lock<std::shared_mutex> lock(meta_mutex_);
  std::unique_lock<std::shared_mutex> node_lock(node_partition_mutex_);

  // Clear existing node->partition mappings
  node_partitions_.clear();

  // Round-robin assignment
  for (PartitionID pid = 0; pid < num_partitions_; ++pid) {
    auto meta_it = partition_metas_.find(pid);
    if (meta_it == partition_metas_.end()) {
      continue;
    }
    NodeID node_id = node_ids[pid % node_ids.size()];
    meta_it->second->primary_node = node_id;
    node_partitions_[node_id].push_back(pid);
  }

  return Status::OK();
}

std::vector<PartitionID> PartitionManager::GetPartitionsForKeys(
    const std::vector<CedarKey>& keys) const {
  
  std::unordered_set<PartitionID> unique_partitions;
  
  for (const auto& key : keys) {
    unique_partitions.insert(GetPartition(key));
  }
  
  return std::vector<PartitionID>(unique_partitions.begin(), unique_partitions.end());
}

std::shared_ptr<SubgraphBoundary> PartitionManager::GetSubgraphBoundary(
    SubgraphID sid) const {
  std::shared_lock<std::shared_mutex> lock(subgraph_mutex_);
  
  auto it = subgraph_boundaries_.find(sid);
  if (it != subgraph_boundaries_.end()) {
    return it->second;
  }
  
  return nullptr;
}

void PartitionManager::UpdatePartitionStats(
    PartitionID pid, const PartitionLoadStats& stats) {
  auto meta = GetPartitionMeta(pid);
  if (!meta) return;
  
  meta->UpdateLoadStats(stats.txn_count_1min, stats.conflict_count_1min);
  
  // 其他统计信息更新...
  meta->avg_latency_ms.store(stats.avg_latency_ms);
}

Status PartitionManager::RebalanceIfNeeded() {
  auto plan = ComputeRebalancePlan();
  if (plan.empty()) {
    return Status::OK();
  }
  return Status::NotSupported("Rebalance", "Use explicit MigratePartition calls from the plan");
}

std::vector<PartitionManager::MigrationPlan> PartitionManager::ComputeRebalancePlan() const {
  std::vector<MigrationPlan> migrations;
  
  std::shared_lock<std::shared_mutex> lock(meta_mutex_);
  std::shared_lock<std::shared_mutex> node_lock(node_partition_mutex_);
  
  if (node_partitions_.size() < 2) {
    return migrations;
  }
  
  size_t total = partition_metas_.size();
  size_t node_count = node_partitions_.size();
  size_t target_per_node = total / node_count;
  
  struct NodeLoad {
    NodeID node_id;
    size_t count;
  };
  std::vector<NodeLoad> overloaded;
  std::vector<NodeLoad> underloaded;
  
  for (const auto& [node_id, parts] : node_partitions_) {
    size_t count = parts.size();
    if (count > target_per_node + 1) {
      overloaded.push_back({node_id, count - target_per_node});
    } else if (count < target_per_node) {
      underloaded.push_back({node_id, target_per_node - count});
    }
  }
  
  for (auto& from : overloaded) {
    for (auto& to : underloaded) {
      if (from.count == 0 || to.count == 0) continue;
      size_t to_move = std::min(from.count, to.count);
      for (const auto& [pid, meta] : partition_metas_) {
        if (to_move == 0) break;
        if (meta->primary_node == from.node_id) {
          migrations.push_back({pid, from.node_id, to.node_id});
          to_move--;
          from.count--;
          to.count--;
        }
      }
    }
  }
  
  return migrations;
}

Status PartitionManager::MigratePartition(PartitionID pid, NodeID new_node) {
  std::unique_lock<std::shared_mutex> lock(meta_mutex_);
  auto it = partition_metas_.find(pid);
  if (it == partition_metas_.end()) {
    return Status::NotFound("PartitionManager", "Partition not found");
  }
  
  NodeID old_node = it->second->primary_node;
  it->second->primary_node = new_node;
  
  {
    std::unique_lock<std::shared_mutex> node_lock(node_partition_mutex_);
    auto& old_vec = node_partitions_[old_node];
    old_vec.erase(std::remove(old_vec.begin(), old_vec.end(), pid), old_vec.end());
    node_partitions_[new_node].push_back(pid);
  }
  
  return Status::OK();
}

std::vector<PartitionID> PartitionManager::GetAllPartitions() const {
  std::shared_lock<std::shared_mutex> lock(meta_mutex_);
  
  std::vector<PartitionID> result;
  result.reserve(partition_metas_.size());
  
  for (const auto& [pid, _] : partition_metas_) {
    result.push_back(pid);
  }
  
  return result;
}

std::vector<PartitionID> PartitionManager::GetPartitionsOnNode(NodeID node_id) const {
  std::shared_lock<std::shared_mutex> lock(node_partition_mutex_);
  
  auto it = node_partitions_.find(node_id);
  if (it != node_partitions_.end()) {
    return it->second;
  }
  
  return {};
}

void PartitionManager::ReportLoad(PartitionID pid, const PartitionLoadStats& stats) {
  UpdatePartitionStats(pid, stats);
}

// =============================================================================
// CedarKeyPartitionHelper 实现
// =============================================================================

std::vector<CedarKey> CedarKeyPartitionHelper::SetPartitionIDs(
    const std::vector<CedarKey>& keys, 
    PartitionID pid) {
  
  std::vector<CedarKey> result;
  result.reserve(keys.size());
  
  for (const auto& key : keys) {
    result.push_back(SetPartitionID(key, pid));
  }
  
  return result;
}

}  // namespace dtx
}  // namespace cedar
