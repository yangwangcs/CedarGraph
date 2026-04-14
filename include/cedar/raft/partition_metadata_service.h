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

#ifndef CEDAR_RAFT_PARTITION_METADATA_SERVICE_H_
#define CEDAR_RAFT_PARTITION_METADATA_SERVICE_H_

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <functional>
#include <chrono>
#include <mutex>
#include <shared_mutex>

#include "cedar/core/status.h"
#include "cedar/raft/partition_raft_manager.h"

namespace cedar {
namespace raft {

// =============================================================================
// 分区元数据 - 描述分区的完整拓扑信息
// =============================================================================

struct PartitionMetadata {
  PartitionID part_id = 0;
  std::string leader_node;
  std::vector<std::string> replica_nodes;
  std::string space_name;  // 图空间名称
  
  enum class State : uint8_t {
    kNormal = 0,      // 正常运行
    kCreating = 1,    // 创建中
    kMigrating = 2,   // 迁移中
    kOffline = 3,     // 离线
    kDeleting = 4,    // 删除中
  };
  State state = State::kNormal;
  
  // 统计信息
  size_t key_count = 0;
  size_t data_size = 0;
  uint64_t version = 0;
  
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point updated_at;
  
  bool IsValid() const {
    return !leader_node.empty() && !replica_nodes.empty();
  }
  
  bool IsLeaderOn(const std::string& node_id) const {
    return leader_node == node_id;
  }
  
  bool IsReplicaOn(const std::string& node_id) const {
    if (leader_node == node_id) return true;
    for (const auto& node : replica_nodes) {
      if (node == node_id) return true;
    }
    return false;
  }
  
  std::string Serialize() const;
  static StatusOr<PartitionMetadata> Deserialize(const std::string& data);
};

// =============================================================================
// 节点元数据
// =============================================================================

struct StorageNodeMetadata {
  std::string node_id;
  std::string address;
  uint16_t port = 0;
  std::string dc_id;  // 数据中心ID
  
  enum class State : uint8_t {
    kOnline = 0,
    kOffline = 1,
    kSuspected = 2,
    kMaintenance = 3,
  };
  State state = State::kOnline;
  
  // 资源信息
  uint32_t num_partitions = 0;
  uint64_t total_disk_bytes = 0;
  uint64_t used_disk_bytes = 0;
  double cpu_usage_percent = 0.0;
  double memory_usage_percent = 0.0;
  
  std::chrono::system_clock::time_point registered_at;
  std::chrono::system_clock::time_point last_heartbeat;
  
  bool IsOnline() const { return state == State::kOnline; }
  
  std::string Serialize() const;
  static StatusOr<StorageNodeMetadata> Deserialize(const std::string& data);
};

// =============================================================================
// 拓扑变更类型
// =============================================================================

enum class TopologyChangeType : uint8_t {
  kLeaderChanged = 0,     // Leader 变更
  kReplicaAdded = 1,      // 添加副本
  kReplicaRemoved = 2,    // 移除副本
  kPartitionMigrated = 3, // 分区迁移
  kNodeJoined = 4,        // 节点加入
  kNodeLeft = 5,          // 节点离开
};

struct TopologyChange {
  TopologyChangeType type;
  PartitionID part_id = 0;
  std::string space_name;
  std::string old_node;
  std::string new_node;
  uint64_t version = 0;
  std::chrono::system_clock::time_point timestamp;
};

// =============================================================================
// 分区拓扑配置
// =============================================================================

struct PartitionTopologyConfig {
  // 默认分区数
  uint32_t default_partition_count = 65536;
  
  // 默认副本数
  uint32_t default_replica_count = 3;
  
  // 放置策略
  PartitionPlacementStrategy placement_strategy = 
      PartitionPlacementStrategy::kConsistentHash;
  
  // 自动重平衡
  bool enable_auto_rebalance = true;
  double rebalance_threshold = 0.1;  // 负载差异阈值
  
  // 心跳配置
  uint64_t heartbeat_timeout_sec = 30;
  uint64_t heartbeat_check_interval_sec = 10;
};

// =============================================================================
// 分区元数据服务 - 管理分区拓扑
// =============================================================================

class PartitionMetadataService {
 public:
  using TopologyChangeCallback = std::function<void(const TopologyChange&)>;
  
  PartitionMetadataService();
  ~PartitionMetadataService();
  
  PartitionMetadataService(const PartitionMetadataService&) = delete;
  PartitionMetadataService& operator=(const PartitionMetadataService&) = delete;
  
  // 初始化和关闭
  Status Initialize(const PartitionTopologyConfig& config);
  Status Shutdown();
  
  // ===== 节点管理 =====
  
  // 注册存储节点
  Status RegisterNode(const StorageNodeMetadata& node);
  
  // 节点心跳
  Status Heartbeat(const std::string& node_id);
  
  // 获取节点信息
  StatusOr<StorageNodeMetadata> GetNode(const std::string& node_id) const;
  
  // 获取所有在线节点
  std::vector<StorageNodeMetadata> GetOnlineNodes() const;
  
  // 获取所有节点
  std::vector<StorageNodeMetadata> GetAllNodes() const;
  
  // ===== 分区拓扑管理 =====
  
  // 创建图空间（自动分配分区）
  Status CreateSpace(const std::string& space_name, 
                     uint32_t partition_count = 0,  // 0 表示使用默认值
                     uint32_t replica_count = 0);
  
  // 删除图空间
  Status DropSpace(const std::string& space_name);
  
  // 获取分区元数据
  StatusOr<PartitionMetadata> GetPartitionMetadata(
      const std::string& space_name, 
      PartitionID part_id) const;
  
  // 获取图空间的所有分区
  std::vector<PartitionMetadata> GetSpacePartitions(
      const std::string& space_name) const;
  
  // 获取节点上的所有分区
  std::vector<PartitionMetadata> GetNodePartitions(
      const std::string& node_id) const;
  
  // ===== 拓扑变更 =====
  
  // 更新分区 Leader（由 Storage 上报）
  Status UpdatePartitionLeader(const std::string& space_name,
                                PartitionID part_id,
                                const std::string& new_leader);
  
  // 迁移分区到新节点
  Status MigratePartition(const std::string& space_name,
                          PartitionID part_id,
                          const std::string& from_node,
                          const std::string& to_node);
  
  // 添加分区副本
  Status AddPartitionReplica(const std::string& space_name,
                             PartitionID part_id,
                             const std::string& node_id);
  
  // 移除分区副本
  Status RemovePartitionReplica(const std::string& space_name,
                                PartitionID part_id,
                                const std::string& node_id);
  
  // ===== 负载均衡 =====
  
  // 检查是否需要重平衡
  bool NeedsRebalance() const;
  
  // 执行重平衡
  Status Rebalance();
  
  // ===== 订阅/通知 =====
  
  void WatchTopologyChanges(TopologyChangeCallback callback);
  
  // ===== 统计 =====
  
  struct Stats {
    size_t total_spaces = 0;
    size_t total_partitions = 0;
    size_t healthy_partitions = 0;
    size_t total_nodes = 0;
    size_t online_nodes = 0;
    size_t migrating_partitions = 0;
  };
  Stats GetStats() const;
  
 private:
  // 心跳检查线程
  void HeartbeatCheckLoop();
  
  // 选择最佳 Leader 节点
  std::string SelectBestLeader(const std::vector<std::string>& candidates);
  
  // 选择副本节点（一致性哈希）
  std::vector<std::string> SelectReplicaNodes(PartitionID part_id,
                                               uint32_t replica_count);
  
  // 通知拓扑变更
  void NotifyTopologyChange(const TopologyChange& change);
  
  // 内部数据结构
  mutable std::shared_mutex mutex_;
  
  // 配置
  PartitionTopologyConfig config_;
  
  // 节点信息
  std::unordered_map<std::string, StorageNodeMetadata> nodes_;
  
  // 图空间 -> 分区元数据
  std::unordered_map<std::string, std::unordered_map<PartitionID, PartitionMetadata>> space_partitions_;
  
  // 变更回调
  std::mutex callbacks_mutex_;
  std::vector<TopologyChangeCallback> topology_callbacks_;
  
  // 心跳检查
  std::atomic<bool> running_{false};
  std::thread heartbeat_thread_;
  
  std::atomic<bool> initialized_{false};
};

}  // namespace raft
}  // namespace cedar

#endif  // CEDAR_RAFT_PARTITION_METADATA_SERVICE_H_
