// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/service/partition_allocator.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <random>

namespace cedar {
namespace service {

// ============================================================================
// NodeLoadInfo
// ============================================================================

double NodeLoadInfo::CalculateLoadScore() const {
  // 综合负载分数计算
  // CPU: 40%, Memory: 30%, Disk: 20%, QPS负载: 10%
  double cpu_score = cpu_usage * 0.4;
  double memory_score = memory_usage * 0.3;
  double disk_score = disk_usage * 0.2;
  
  // QPS 负载（假设最大 10000 QPS）
  double qps_ratio = std::min(1.0, static_cast<double>(qps) / 10000.0);
  double qps_score = qps_ratio * 0.1;
  
  return cpu_score + memory_score + disk_score + qps_score;
}

// ============================================================================
// PartitionAllocator
// ============================================================================

PartitionAllocator::PartitionAllocator(AllocationStrategy strategy)
    : strategy_(strategy) {}

PartitionAllocator::~PartitionAllocator() = default;

Status PartitionAllocator::RegisterNode(NodeID node_id, const std::string& address) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  NodeLoadInfo info;
  info.node_id = node_id;
  info.address = address;
  info.is_healthy = true;
  info.last_heartbeat = std::chrono::system_clock::now().time_since_epoch().count();
  
  nodes_[node_id] = info;
  
  if (strategy_ == AllocationStrategy::CONSISTENT_HASH) {
    BuildConsistentHashRing();
  }
  
  std::cerr << "[PartitionAllocator] Node " << node_id << " registered at " << address << std::endl;
  return Status::OK();
}

Status PartitionAllocator::UnregisterNode(NodeID node_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  nodes_.erase(node_id);
  
  if (strategy_ == AllocationStrategy::CONSISTENT_HASH) {
    BuildConsistentHashRing();
  }
  
  // 标记该节点上的分区需要重新分配
  std::vector<PartitionID> affected_partitions;
  for (auto& [part_id, allocation] : allocations_) {
    if (allocation.leader_node == node_id) {
      affected_partitions.push_back(part_id);
    }
    // 从 followers 中移除
    auto it = std::remove(allocation.followers.begin(), allocation.followers.end(), node_id);
    allocation.followers.erase(it, allocation.followers.end());
  }
  
  std::cerr << "[PartitionAllocator] Node " << node_id << " unregistered, "
            << affected_partitions.size() << " partitions need reallocation" << std::endl;
  
  return Status::OK();
}

Status PartitionAllocator::UpdateNodeLoad(NodeID node_id, const NodeLoadInfo& load) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("Node not found: " + std::to_string(node_id));
  }
  
  it->second = load;
  it->second.last_heartbeat = std::chrono::system_clock::now().time_since_epoch().count();
  
  return Status::OK();
}

Status PartitionAllocator::AllocatePartition(PartitionID partition_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (nodes_.empty()) {
    return Status::InvalidArgument("No available nodes");
  }
  
  // 选择 leader
  NodeID leader = SelectLeaderNode(partition_id);
  if (leader == 0) {
    return Status::InvalidArgument("Failed to select leader node");
  }
  
  // 选择 followers
  auto followers = SelectFollowerNodes(partition_id, leader);
  
  // 创建分配
  PartitionAllocation alloc;
  alloc.partition_id = partition_id;
  alloc.leader_node = leader;
  alloc.followers = followers;
  alloc.version = 1;
  alloc.created_at = std::chrono::system_clock::now().time_since_epoch().count();
  
  allocations_[partition_id] = alloc;
  
  // 更新节点统计
  nodes_[leader].leader_count++;
  for (uint32_t follower : followers) {
    nodes_[follower].follower_count++;
  }
  
  return Status::OK();
}

Status PartitionAllocator::AllocateAllPartitions() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (nodes_.empty()) {
    return Status::InvalidArgument("No available nodes");
  }
  
  std::cerr << "[PartitionAllocator] Allocating " << total_partitions_ 
            << " partitions to " << nodes_.size() << " nodes" << std::endl;
  
  allocations_.clear();
  
  for (uint32_t i = 0; i < total_partitions_; ++i) {
    NodeID leader = SelectLeaderNode(i);
    if (leader == 0) continue;
    
    auto followers = SelectFollowerNodes(i, leader);
    
    PartitionAllocation alloc;
    alloc.partition_id = i;
    alloc.leader_node = leader;
    alloc.followers = followers;
    alloc.version = 1;
    alloc.created_at = std::chrono::system_clock::now().time_since_epoch().count();
    
    allocations_[i] = alloc;
  }
  
  std::cerr << "[PartitionAllocator] Allocation complete" << std::endl;
  return Status::OK();
}

Status PartitionAllocator::Rebalance() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (nodes_.size() < 2) {
    return Status::OK();  // 不需要重新平衡
  }
  
  std::cerr << "[PartitionAllocator] Starting rebalance..." << std::endl;
  
  // 计算当前负载方差
  double variance_before = CalculateClusterLoadVariance();
  
  // 生成迁移计划
  auto migrations = ComputeMigrationPlan();
  
  if (migrations.empty()) {
    std::cerr << "[PartitionAllocator] No rebalancing needed" << std::endl;
    return Status::OK();
  }
  
  // 执行迁移（标记为 pending，实际迁移由外部处理）
  for (const auto& [part_id, target_node] : migrations) {
    MigrationTask task;
    task.partition_id = part_id;
    task.source_node = allocations_[part_id].leader_node;
    task.target_node = target_node;
    task.started_at = std::chrono::system_clock::now().time_since_epoch().count();
    task.status = "pending";
    pending_migrations_.push_back(task);
  }
  
  double variance_after = CalculateClusterLoadVariance();
  
  std::cerr << "[PartitionAllocator] Rebalance plan: " << migrations.size() 
            << " migrations, variance: " << variance_before << " -> " << variance_after << std::endl;
  
  return Status::OK();
}

StatusOr<PartitionAllocation> PartitionAllocator::GetAllocation(PartitionID partition_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = allocations_.find(partition_id);
  if (it == allocations_.end()) {
    return Status::NotFound("Partition not found: " + std::to_string(partition_id));
  }
  
  return it->second;
}

std::vector<PartitionAllocation> PartitionAllocator::GetAllAllocations() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<PartitionAllocation> result;
  for (const auto& [part_id, alloc] : allocations_) {
    result.push_back(alloc);
  }
  
  return result;
}

std::vector<PartitionID> PartitionAllocator::GetNodePartitions(NodeID node_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<PartitionID> result;
  for (const auto& [part_id, alloc] : allocations_) {
    if (alloc.leader_node == node_id) {
      result.push_back(part_id);
    }
  }
  
  return result;
}

Status PartitionAllocator::MigratePartition(PartitionID partition_id, NodeID new_leader) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = allocations_.find(partition_id);
  if (it == allocations_.end()) {
    return Status::NotFound("Partition not found");
  }
  
  auto& alloc = it->second;
  uint32_t old_leader = alloc.leader_node;
  
  // 更新分配
  alloc.leader_node = new_leader;
  alloc.version++;
  
  // 更新节点统计
  auto old_it = nodes_.find(old_leader);
  if (old_it != nodes_.end()) {
    old_it->second.leader_count--;
  }
  auto new_it = nodes_.find(new_leader);
  if (new_it != nodes_.end()) {
    new_it->second.leader_count++;
  }
  
  std::cerr << "[PartitionAllocator] Migrated partition " << partition_id 
            << " from node " << old_leader << " to node " << new_leader << std::endl;
  
  return Status::OK();
}

Status PartitionAllocator::MigratePartitions(
    const std::vector<std::pair<PartitionID, NodeID>>& migrations) {
  for (const auto& [partition_id, new_leader] : migrations) {
    auto s = MigratePartition(partition_id, new_leader);
    if (!s.ok()) return s;
  }
  return Status::OK();
}

std::vector<std::pair<PartitionID, NodeID>> PartitionAllocator::ComputeMigrationPlan() {
  std::vector<std::pair<PartitionID, NodeID>> migrations;
  
  if (nodes_.size() < 2) {
    return migrations;
  }
  
  // 计算每个节点的目标分区数
  size_t total_partitions = allocations_.size();
  size_t node_count = nodes_.size();
  size_t target_per_node = total_partitions / node_count;
  
  // 预计算每个节点的分区数量和归属映射（O(allocations) 一次遍历）
  std::unordered_map<uint32_t, size_t> node_partition_count;
  std::unordered_map<uint32_t, std::vector<PartitionID>> node_to_partitions;
  for (const auto& [part_id, alloc] : allocations_) {
    node_partition_count[alloc.leader_node]++;
    node_to_partitions[alloc.leader_node].push_back(part_id);
  }

  // 找出过载和欠载的节点
  std::vector<std::pair<uint32_t, size_t>> overloaded;  // node_id, excess
  std::vector<std::pair<uint32_t, size_t>> underloaded; // node_id, deficit
  
  for (const auto& [node_id, node] : nodes_) {
    if (!node.is_healthy) continue;
    
    size_t count = node_partition_count[node_id];
    
    if (count > target_per_node + 1) {
      overloaded.push_back({node_id, count - target_per_node});
    } else if (count < target_per_node) {
      underloaded.push_back({node_id, target_per_node - count});
    }
  }
  
  // 生成迁移计划：O(overloaded × underloaded × avg_partitions_per_node)
  for (auto& [from_node, excess] : overloaded) {
    for (auto& [to_node, deficit] : underloaded) {
      if (excess == 0 || deficit == 0) continue;
      
      size_t to_move = std::min(excess, deficit);
      
      // 直接从 from_node 的分区列表中选取（O(avg_partitions_per_node)）
      auto& from_partitions = node_to_partitions[from_node];
      for (auto it = from_partitions.begin(); it != from_partitions.end() && to_move > 0;) {
        // 确保该分区仍由 from_node 领导（可能已被前面的迁移改变）
        auto alloc_it = allocations_.find(*it);
        if (alloc_it != allocations_.end() && alloc_it->second.leader_node == from_node) {
          migrations.push_back({*it, to_node});
          to_move--;
          excess--;
          deficit--;
          it = from_partitions.erase(it);
        } else {
          ++it;
        }
      }
    }
  }
  
  return migrations;
}

PartitionAllocator::Stats PartitionAllocator::GetStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  Stats stats;
  stats.total_partitions = allocations_.size();
  stats.active_nodes = nodes_.size();
  stats.pending_migrations = pending_migrations_.size();
  
  if (!nodes_.empty()) {
    stats.avg_partitions_per_node = static_cast<double>(allocations_.size()) / nodes_.size();
  }
  
  stats.load_variance = CalculateClusterLoadVariance();
  
  return stats;
}

// ============================================================================
// 私有方法
// ============================================================================

uint32_t PartitionAllocator::SelectLeaderNode(PartitionID partition_id) {
  switch (strategy_) {
    case AllocationStrategy::ROUND_ROBIN:
      if (!nodes_.empty()) {
        auto it = nodes_.begin();
        std::advance(it, partition_id % nodes_.size());
        return it->first;
      }
      break;
      
    case AllocationStrategy::CONSISTENT_HASH:
      return FindConsistentHashNode(partition_id);
      
    case AllocationStrategy::LOAD_BALANCED:
    case AllocationStrategy::CAPACITY_BASED:
      return SelectLeastLoadedNode();
      
    default:
      break;
  }
  
  return 0;
}

std::vector<NodeID> PartitionAllocator::SelectFollowerNodes(PartitionID partition_id, NodeID leader) {
  std::vector<NodeID> followers;
  
  for (const auto& [node_id, node] : nodes_) {
    if (node_id == leader) continue;
    if (!node.is_healthy) continue;
    
    followers.push_back(node_id);
    if (followers.size() >= replication_factor_ - 1) break;
  }
  
  return followers;
}

void PartitionAllocator::BuildConsistentHashRing() {
  consistent_hash_ring_.clear();
  
  // 每个节点在环上放置 150 个虚拟节点
  const int virtual_nodes = 150;
  std::hash<std::string> hasher;
  
  for (const auto& [node_id, node] : nodes_) {
    for (int i = 0; i < virtual_nodes; ++i) {
      std::string key = node.address + "#" + std::to_string(i);
      uint64_t hash = hasher(key);
      consistent_hash_ring_[hash] = node_id;
    }
  }
}

uint32_t PartitionAllocator::FindConsistentHashNode(uint64_t hash) const {
  if (consistent_hash_ring_.empty()) {
    return 0;
  }
  
  auto it = consistent_hash_ring_.lower_bound(hash);
  if (it == consistent_hash_ring_.end()) {
    it = consistent_hash_ring_.begin();
  }
  
  return it->second;
}

uint32_t PartitionAllocator::SelectLeastLoadedNode() {
  uint32_t best_node = 0;
  double best_score = std::numeric_limits<double>::max();
  
  for (const auto& [node_id, node] : nodes_) {
    if (!node.is_healthy) continue;
    
    double score = node.CalculateLoadScore();
    // 考虑当前 leader 数量（leader_count 在 AllocatePartition/MigratePartition 中维护）
    score += static_cast<double>(node.leader_count) * 0.01;  // 轻微偏好负载均衡
    
    if (score < best_score) {
      best_score = score;
      best_node = node_id;
    }
  }
  
  return best_node;
}

double PartitionAllocator::CalculateClusterLoadVariance() const {
  if (nodes_.empty()) return 0.0;
  
  std::vector<double> loads;
  for (const auto& [node_id, node] : nodes_) {
    loads.push_back(static_cast<double>(node.leader_count));
  }
  
  if (loads.empty()) return 0.0;
  
  double sum = std::accumulate(loads.begin(), loads.end(), 0.0);
  double mean = sum / loads.size();
  
  double variance = 0.0;
  for (double load : loads) {
    variance += (load - mean) * (load - mean);
  }
  variance /= loads.size();
  
  return variance;
}

}  // namespace service
}  // namespace cedar
