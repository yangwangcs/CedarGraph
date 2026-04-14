// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_SERVICE_PARTITION_ALLOCATOR_H_
#define CEDAR_SERVICE_PARTITION_ALLOCATOR_H_

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace service {

using dtx::PartitionID;
using dtx::NodeID;

// 分区分配策略
enum class AllocationStrategy {
  ROUND_ROBIN,      // 轮询分配
  CONSISTENT_HASH,  // 一致性哈希
  LOAD_BALANCED,    // 负载均衡（考虑节点负载）
  CAPACITY_BASED    // 基于容量
};

// 节点负载信息
struct NodeLoadInfo {
  NodeID node_id;
  std::string address;
  
  // 负载指标
  double cpu_usage = 0.0;
  double memory_usage = 0.0;
  double disk_usage = 0.0;
  uint64_t qps = 0;
  
  // 分区统计
  size_t leader_count = 0;
  size_t follower_count = 0;
  
  // 容量
  uint64_t total_disk_bytes = 0;
  uint64_t used_disk_bytes = 0;
  
  // 健康状态
  bool is_healthy = true;
  int64_t last_heartbeat = 0;
  
  // 计算综合负载分数（越低越好）
  double CalculateLoadScore() const;
};

// 分区分配信息
struct PartitionAllocation {
  PartitionID partition_id;
  NodeID leader_node;
  std::vector<NodeID> followers;
  uint64_t version = 0;
  int64_t created_at = 0;
};

// 分区分配管理器
class PartitionAllocator {
 public:
  explicit PartitionAllocator(AllocationStrategy strategy = AllocationStrategy::LOAD_BALANCED);
  ~PartitionAllocator();

  // 禁止拷贝
  PartitionAllocator(const PartitionAllocator&) = delete;
  PartitionAllocator& operator=(const PartitionAllocator&) = delete;

  // 配置
  void SetReplicationFactor(uint32_t rf) { replication_factor_ = rf; }
  void SetTotalPartitions(uint32_t num) { total_partitions_ = num; }
  
  // 节点管理
  Status RegisterNode(NodeID node_id, const std::string& address);
  Status UnregisterNode(NodeID node_id);
  Status UpdateNodeLoad(NodeID node_id, const NodeLoadInfo& load);
  
  // 分区分配
  Status AllocatePartition(PartitionID partition_id);
  Status AllocateAllPartitions();
  
  // 重新平衡（在节点加入/离开或负载不均时调用）
  Status Rebalance();
  
  // 获取分配信息
  StatusOr<PartitionAllocation> GetAllocation(PartitionID partition_id);
  std::vector<PartitionAllocation> GetAllAllocations();
  std::vector<PartitionID> GetNodePartitions(NodeID node_id);
  
  // 分区迁移
  Status MigratePartition(PartitionID partition_id, NodeID new_leader);
  Status MigratePartitions(const std::vector<std::pair<PartitionID, NodeID>>& migrations);
  
  // 计算迁移计划（用于负载均衡）
  std::vector<std::pair<PartitionID, NodeID>> ComputeMigrationPlan();
  
  // 统计信息
  struct Stats {
    size_t total_partitions = 0;
    size_t active_nodes = 0;
    double avg_partitions_per_node = 0.0;
    double load_variance = 0.0;  // 负载方差
    size_t pending_migrations = 0;
  };
  Stats GetStats() const;

 private:
  // 分配策略实现
  NodeID SelectLeaderNode(PartitionID partition_id);
  std::vector<NodeID> SelectFollowerNodes(PartitionID partition_id, NodeID leader);
  
  // 一致性哈希环
  std::map<uint64_t, uint32_t> consistent_hash_ring_;
  void BuildConsistentHashRing();
  uint32_t FindConsistentHashNode(uint64_t hash) const;
  
  // 负载均衡算法
  uint32_t SelectLeastLoadedNode();
  double CalculateClusterLoadVariance() const;
  
  AllocationStrategy strategy_;
  uint32_t replication_factor_ = 3;
  uint32_t total_partitions_ = 65536;
  
  mutable std::mutex mutex_;
  std::unordered_map<NodeID, NodeLoadInfo> nodes_;
  std::unordered_map<PartitionID, PartitionAllocation> allocations_;
  
  // 分区迁移状态
  struct MigrationTask {
    PartitionID partition_id;
    NodeID source_node;
    NodeID target_node;
    int64_t started_at;
    std::string status;  // "pending", "in_progress", "completed", "failed"
  };
  std::vector<MigrationTask> pending_migrations_;
};

}  // namespace service
}  // namespace cedar

#endif  // CEDAR_SERVICE_PARTITION_ALLOCATOR_H_
