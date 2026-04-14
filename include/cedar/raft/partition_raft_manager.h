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

#ifndef CEDAR_RAFT_PARTITION_RAFT_MANAGER_H_
#define CEDAR_RAFT_PARTITION_RAFT_MANAGER_H_

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <chrono>
#include <condition_variable>
#include <queue>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/core/threading.h"
#include "cedar/raft/partition_raft_group.h"
#include "cedar/storage/consistent_hash_ring.h"
#include "cedar/storage/storage_health_monitor.h"

namespace cedar {
namespace raft {

// 分区放置策略
enum class PartitionPlacementStrategy {
  kRoundRobin,      // 轮询放置
  kConsistentHash,  // 一致性哈希
  kLoadAware        // 负载感知
};

// 分区 Raft 管理器配置
struct PartitionRaftManagerConfig {
  uint32_t default_replica_count = 3;
  PartitionPlacementStrategy placement_strategy = 
      PartitionPlacementStrategy::kConsistentHash;
  bool enable_auto_rebalance = true;
  double rebalance_threshold = 0.2;  // 负载差异超过 20% 触发重平衡
  uint32_t max_partitions_per_node = 1024;
  uint32_t raft_worker_threads = 4;  // 共享 Raft tick worker 线程数
};

// 分区信息
struct PartitionInfo {
  PartitionID part_id;
  std::string leader_node;
  std::vector<std::string> replica_nodes;
  size_t key_count = 0;
  size_t data_size = 0;
  bool is_healthy = true;
};

// 节点分区负载
struct NodePartitionLoad {
  std::string node_id;
  std::vector<PartitionID> leader_partitions;
  std::vector<PartitionID> follower_partitions;
  double load_score = 0.0;
};

class PartitionRaftManager {
 public:
  using PartitionLeaderChangeCallback = std::function<void(
      PartitionID part_id,
      const std::string& old_leader,
      const std::string& new_leader)>;
  
  using NodeLoadChangeCallback = std::function<void(
      const std::string& node_id,
      double old_load,
      double new_load)>;

  PartitionRaftManager();
  ~PartitionRaftManager();

  PartitionRaftManager(const PartitionRaftManager&) = delete;
  PartitionRaftManager& operator=(const PartitionRaftManager&) = delete;

  // 初始化
  Status Initialize(const PartitionRaftManagerConfig& config,
                    std::shared_ptr<storage::StorageHealthMonitor> health_monitor);

  // 启动/停止
  Status Start();
  void Stop();

  // 注册存储节点
  Status RegisterNode(const std::string& node_id,
                      const std::string& address,
                      uint16_t port);
  
  // 注销存储节点
  Status DeregisterNode(const std::string& node_id);

  // 创建分区 Raft 组
  Status CreatePartitionGroup(PartitionID part_id,
                               const std::vector<std::string>& replica_nodes);
  
  // 删除分区
  Status RemovePartitionGroup(PartitionID part_id);

  // 获取分区的 Raft 组
  PartitionRaftGroup* GetPartitionGroup(PartitionID part_id);

  // 根据 entity_id 计算 part_id
  static PartitionID ComputePartitionId(uint64_t entity_id);
  
  // 根据 key 获取分区 ID
  PartitionID GetPartitionIdForKey(const std::string& key);

  // 路由写入（自动计算 part_id 并路由到 Leader）
  StatusOr<std::string> RouteWrite(uint64_t entity_id);
  StatusOr<std::string> RouteWriteByPartition(PartitionID part_id);
  
  // 路由读取
  StatusOr<std::string> RouteRead(uint64_t entity_id, bool require_leader = false);
  StatusOr<std::string> RouteReadByPartition(PartitionID part_id, 
                                              bool require_leader = false);

  // 批量路由（将多个 key 按分区分组）
  struct PartitionedBatch {
    PartitionID part_id;
    std::vector<std::string> keys;
    std::string target_node;
  };
  std::vector<PartitionedBatch> RouteBatchWrite(
      const std::vector<std::string>& keys);
  std::vector<PartitionedBatch> RouteBatchRead(
      const std::vector<std::string>& keys,
      bool require_leader = false);

  // 获取所有分区信息
  std::vector<PartitionInfo> GetAllPartitions() const;
  
  // 获取节点的分区负载
  NodePartitionLoad GetNodeLoad(const std::string& node_id) const;
  std::vector<NodePartitionLoad> GetAllNodesLoad() const;

  // 获取 Leader 分布统计
  struct LeaderDistribution {
    std::string node_id;
    size_t leader_count;
    double percentage;
  };
  std::vector<LeaderDistribution> GetLeaderDistribution() const;

  // 手动触发重平衡
  Status RebalancePartitions();
  
  // 迁移分区（将分区的 Leader 转移到指定节点）
  Status MigratePartitionLeader(PartitionID part_id, 
                                 const std::string& target_node);

  // 获取统计信息
  struct Stats {
    size_t total_partitions = 0;
    size_t active_partitions = 0;
    size_t healthy_partitions = 0;
    size_t total_nodes = 0;
    size_t healthy_nodes = 0;
  };
  Stats GetStats() const;

  // 设置回调
  void SetPartitionLeaderChangeCallback(PartitionLeaderChangeCallback callback);
  void SetNodeLoadChangeCallback(NodeLoadChangeCallback callback);

 private:
  void OnPartitionLeaderChanged(PartitionID part_id,
                                 const std::string& old_leader,
                                 const std::string& new_leader);
  std::vector<std::string> SelectReplicasForPartition(PartitionID part_id);
  void UpdateNodeLoad(const std::string& node_id);
  void CheckAndTriggerRebalance();
  
  PartitionRaftManagerConfig config_;
  std::shared_ptr<storage::StorageHealthMonitor> health_monitor_;
  
  mutable std::mutex groups_mutex_;
  // 使用 unordered_map 而不是 array，因为不是所有分区都会创建
  std::unordered_map<PartitionID, std::unique_ptr<PartitionRaftGroup>> partition_groups_;
  
  mutable std::mutex nodes_mutex_;
  std::unordered_map<std::string, std::pair<std::string, uint16_t>> nodes_;  // node_id -> (address, port)
  std::unordered_map<std::string, NodePartitionLoad> node_loads_;
  
  std::unique_ptr<storage::ConsistentHashRing> hash_ring_;
  
  std::atomic<bool> running_{false};
  std::thread rebalance_thread_;
  
  // 共享 Raft 调度器：1 个调度线程 + N 个 worker 线程支持任意数量 partition
  struct ScheduledPartition {
    PartitionID part_id;
    std::chrono::steady_clock::time_point next_tick;
    bool operator>(const ScheduledPartition& other) const {
      return next_tick > other.next_tick;
    }
  };
  
  std::unique_ptr<ThreadPool> raft_worker_pool_;
  std::thread scheduler_thread_;
  mutable std::mutex schedule_mutex_;
  std::condition_variable schedule_cv_;
  std::priority_queue<ScheduledPartition, std::vector<ScheduledPartition>, std::greater<>> schedule_queue_;
  
  void SchedulerLoop();
  
  PartitionLeaderChangeCallback leader_change_callback_;
  NodeLoadChangeCallback load_change_callback_;
  
  static constexpr PartitionID kMaxPartitions = 65535;
};

}  // namespace raft
}  // namespace cedar

#endif  // CEDAR_RAFT_PARTITION_RAFT_MANAGER_H_
