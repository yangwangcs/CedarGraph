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

#ifndef CEDAR_RAFT_PARTITION_RAFT_GROUP_H_
#define CEDAR_RAFT_PARTITION_RAFT_GROUP_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {

using PartitionID = uint16_t;

namespace raft {

// Raft 角色
enum class RaftRole {
  kLeader,
  kFollower,
  kCandidate,
  kObserver
};

// 副本信息
struct ReplicaInfo {
  std::string node_id;
  std::string address;
  RaftRole role;
  uint64_t log_index = 0;
  uint64_t commit_index = 0;
  std::chrono::steady_clock::time_point last_heartbeat;
  bool is_healthy = true;
};

// 分区 Raft 配置
struct PartitionRaftConfig {
  uint32_t election_timeout_ms = 1000;
  uint32_t heartbeat_interval_ms = 100;
  uint32_t max_log_entries = 10000;
  uint32_t snapshot_threshold = 1000;
};

// 分区 Raft 组
class PartitionRaftGroup {
 public:
  using RoleChangeCallback = std::function<void(
      cedar::PartitionID part_id,
      RaftRole old_role,
      RaftRole new_role)>;
  
  using LeaderChangeCallback = std::function<void(
      cedar::PartitionID part_id,
      const std::string& old_leader,
      const std::string& new_leader)>;

  PartitionRaftGroup(PartitionID part_id, 
                     const PartitionRaftConfig& config);
  ~PartitionRaftGroup();

  PartitionRaftGroup(const PartitionRaftGroup&) = delete;
  PartitionRaftGroup& operator=(const PartitionRaftGroup&) = delete;

  // 初始化副本组
  Status Initialize(const std::vector<ReplicaInfo>& replicas,
                    const std::string& initial_leader = "");
  
  // 启动/停止
  Status Start();
  void Stop();
  bool IsRunning() const { return running_.load(); }
  
  // 执行一次 Raft 状态 tick，返回下次 tick 的推荐延迟
  std::chrono::milliseconds RaftTick();

  // 当前角色
  RaftRole GetCurrentRole() const { return current_role_; }
  bool IsLeader() const { return current_role_ == RaftRole::kLeader; }
  
  // 获取当前 Leader
  StatusOr<std::string> GetLeader() const;
  
  // 获取所有副本
  std::vector<ReplicaInfo> GetReplicas() const;
  
  // 获取健康副本（用于读）
  std::vector<ReplicaInfo> GetHealthyReplicas() const;
  
  // 路由选择
  StatusOr<ReplicaInfo> RouteWrite();  // 必须是 Leader
  StatusOr<ReplicaInfo> RouteRead(bool require_leader = false);

  // 添加/移除副本（成员变更）
  Status AddReplica(const ReplicaInfo& replica);
  Status RemoveReplica(const std::string& node_id);
  
  // 手动触发 Leader 转移
  Status TransferLeadership(const std::string& target_node);

  // 接收心跳（来自其他副本）
  Status ReceiveHeartbeat(const std::string& from_node,
                          uint64_t term,
                          uint64_t log_index);
  
  // 投票请求
  Status ReceiveVoteRequest(const std::string& candidate,
                            uint64_t term,
                            bool* granted);

  // 获取分区 ID
  PartitionID GetPartitionId() const { return part_id_; }
  
  // 获取 Raft 统计
  struct Stats {
    uint64_t current_term = 0;
    uint64_t log_size = 0;
    uint64_t commit_index = 0;
    uint64_t last_applied = 0;
    std::string leader_id;
    RaftRole role;
  };
  Stats GetStats() const;

  // 设置回调
  void SetRoleChangeCallback(RoleChangeCallback callback);
  void SetLeaderChangeCallback(LeaderChangeCallback callback);

 private:
  void SendHeartbeats();
  void CheckElectionTimeout();
  void BecomeFollower(uint64_t term);
  void BecomeCandidate();
  void BecomeLeader();
  void UpdateLeader(const std::string& new_leader);
  ReplicaInfo* FindReplica(const std::string& node_id);
  
  PartitionID part_id_;
  PartitionRaftConfig config_;
  
  std::atomic<RaftRole> current_role_{RaftRole::kFollower};
  std::atomic<uint64_t> current_term_{0};
  std::string voted_for_;
  std::string current_leader_;
  
  mutable std::mutex replicas_mutex_;
  std::vector<ReplicaInfo> replicas_;
  std::unordered_map<std::string, size_t> replica_index_;
  
  std::atomic<bool> running_{false};
  
  std::chrono::steady_clock::time_point last_heartbeat_time_;
  std::chrono::steady_clock::time_point election_timeout_;
  
  RoleChangeCallback role_change_callback_;
  LeaderChangeCallback leader_change_callback_;
};

}  // namespace raft
}  // namespace cedar

#endif  // CEDAR_RAFT_PARTITION_RAFT_GROUP_H_
