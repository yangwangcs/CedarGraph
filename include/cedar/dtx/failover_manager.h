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

// =============================================================================
// Failover Manager - 高可用故障转移管理器
// =============================================================================
// Features:
// - 自动故障检测
// - 分区 Leader 自动切换
// - 脑裂检测与处理
// - 故障恢复自动化
// - 多层级容错策略
// =============================================================================

#ifndef CEDAR_DTX_FAILOVER_MANAGER_H_
#define CEDAR_DTX_FAILOVER_MANAGER_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// =============================================================================
// 故障类型定义
// =============================================================================

enum class FailureType : uint8_t {
  kNodeDown = 0,           // 节点宕机
  kNetworkPartition = 1,   // 网络分区
  kDiskFailure = 2,        // 磁盘故障
  kMemoryExhaustion = 3,   // 内存耗尽
  kCPULoadHigh = 4,        // CPU 负载过高
  kProcessCrash = 5,       // 进程崩溃
  kServiceTimeout = 6,     // 服务超时
  kLeaderLost = 7,         // Leader 丢失
  kReplicationLag = 8,     // 复制延迟过高
};

inline std::string FailureTypeToString(FailureType type) {
  switch (type) {
    case FailureType::kNodeDown: return "NodeDown";
    case FailureType::kNetworkPartition: return "NetworkPartition";
    case FailureType::kDiskFailure: return "DiskFailure";
    case FailureType::kMemoryExhaustion: return "MemoryExhaustion";
    case FailureType::kCPULoadHigh: return "CPULoadHigh";
    case FailureType::kProcessCrash: return "ProcessCrash";
    case FailureType::kServiceTimeout: return "ServiceTimeout";
    case FailureType::kLeaderLost: return "LeaderLost";
    case FailureType::kReplicationLag: return "ReplicationLag";
    default: return "Unknown";
  }
}

// =============================================================================
// 故障事件
// =============================================================================

struct FailureEvent {
  uint64_t event_id{0};
  FailureType type;
  NodeID node_id{0};
  PartitionID partition_id{0};
  std::string description;
  std::chrono::system_clock::time_point detected_at;
  std::chrono::system_clock::time_point recovered_at;
  bool is_recovered{false};
  uint32_t retry_count{0};
  std::string recovery_action;
};

// =============================================================================
// 故障检测策略
// =============================================================================

enum class DetectionStrategy : uint8_t {
  kHeartbeat = 0,          // 心跳检测
  kPhiAccrual = 1,         // Phi Accrual 算法
  kProbeBased = 2,         // 主动探测
  kPassiveObserver = 3,    // 被动观察
};

struct FailureDetectionConfig {
  DetectionStrategy strategy{DetectionStrategy::kPhiAccrual};
  std::chrono::milliseconds check_interval{1000};
  std::chrono::milliseconds timeout{5000};
  uint32_t consecutive_failures{3};
  double phi_threshold{8.0};
  bool enable_auto_recovery{true};
};

// =============================================================================
// 故障恢复策略
// =============================================================================

enum class RecoveryStrategy : uint8_t {
  kRestartService = 0,     // 重启服务
  kSwitchLeader = 1,       // 切换 Leader
  kPromoteReplica = 2,     // 提升副本
  kMigratePartition = 3,   // 迁移分区
  kScaleOut = 4,           // 扩容
  kManualIntervention = 5, // 人工介入
};

struct RecoveryAction {
  RecoveryStrategy strategy;
  NodeID target_node{0};
  PartitionID target_partition{0};
  std::string details;
  std::chrono::milliseconds timeout{30000};
  uint32_t max_retries{3};
};

// =============================================================================
// 分区故障转移控制器
// =============================================================================

class PartitionFailoverController {
 public:
  struct Config {
    std::chrono::milliseconds leader_lease_duration{10000};  // Leader 租约
    uint32_t min_replicas{2};                                 // 最小副本数
    bool enable_auto_promote{true};                          // 自动提升副本
  };
  
  PartitionFailoverController();
  ~PartitionFailoverController();
  
  // 禁止拷贝
  PartitionFailoverController(const PartitionFailoverController&) = delete;
  PartitionFailoverController& operator=(const PartitionFailoverController&) = delete;
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // 注册分区及其副本
  Status RegisterPartition(PartitionID pid, NodeID leader,
                           const std::vector<NodeID>& replicas);
  
  // 注销分区
  Status UnregisterPartition(PartitionID pid);
  
  // 报告节点故障
  Status ReportNodeFailure(NodeID node_id);
  
  // 报告 Leader 故障
  Status ReportLeaderFailure(PartitionID pid);
  
  // 手动触发故障转移
  Status TriggerManualFailover(PartitionID pid, NodeID new_leader);
  
  // 获取分区当前状态
  struct PartitionState {
    PartitionID pid;
    NodeID current_leader;
    std::vector<NodeID> replicas;
    NodeID failover_target;
    bool is_failover_in_progress{false};
    std::chrono::steady_clock::time_point last_failover;
  };
  StatusOr<PartitionState> GetPartitionState(PartitionID pid) const;
  
  // 注册状态变更回调
  using FailoverCallback = std::function<void(PartitionID, NodeID old_leader, 
                                               NodeID new_leader)>;
  void RegisterFailoverCallback(FailoverCallback callback);
  
  // 注册路由更新回调
  using RouteUpdateCallback = std::function<void(PartitionID, NodeID new_leader)>;
  void SetRouteUpdateCallback(RouteUpdateCallback callback);

 private:
  // 故障转移流程
  void ExecuteFailover(PartitionID pid);
  Status SelectNewLeader(PartitionID pid, NodeID* new_leader);
  Status PerformLeaderSwitch(PartitionID pid, NodeID new_leader);
  Status UpdatePartitionRoute(PartitionID pid, NodeID new_leader);
  
  // 后台任务
  void LeaseRenewalLoop();
  void HealthCheckLoop();
  
  Config config_;
  std::atomic<bool> running_{false};
  
  mutable std::mutex partitions_mutex_;
  std::unordered_map<PartitionID, PartitionState> partitions_;
  
  std::vector<FailoverCallback> callbacks_;
  mutable std::mutex callbacks_mutex_;
  
  RouteUpdateCallback route_update_callback_;
  
  std::thread lease_thread_;
  std::thread health_thread_;
};

// =============================================================================
// 集群级故障转移管理器
// =============================================================================

class ClusterFailoverManager {
 public:
  struct Config {
    FailureDetectionConfig detection_config;
    bool enable_auto_recovery{true};
    uint32_t max_concurrent_recoveries{3};
    std::chrono::milliseconds recovery_cooldown{60000};  // 恢复冷却期
  };
  
  ClusterFailoverManager();
  ~ClusterFailoverManager();
  
  // 禁止拷贝
  ClusterFailoverManager(const ClusterFailoverManager&) = delete;
  ClusterFailoverManager& operator=(const ClusterFailoverManager&) = delete;
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // 注册故障检测器
  void RegisterFailureDetector(
      std::function<std::vector<FailureEvent>()> detector);
  
  // 注册恢复处理器
  void RegisterRecoveryHandler(FailureType type,
      std::function<Status(const FailureEvent&)> handler);
  
  // 报告故障事件
  Status ReportFailure(const FailureEvent& event);
  
  // 手动触发恢复
  Status TriggerRecovery(uint64_t event_id);
  
  // 获取故障历史
  std::vector<FailureEvent> GetFailureHistory(NodeID node_id = 0) const;
  
  // 获取当前活跃故障
  std::vector<FailureEvent> GetActiveFailures() const;
  
  // 故障统计
  struct FailureStats {
    uint64_t total_failures{0};
    uint64_t recovered_failures{0};
    uint64_t manual_recoveries{0};
    uint64_t auto_recoveries{0};
    uint64_t failed_recoveries{0};
    std::map<FailureType, uint64_t> failures_by_type;
    double avg_recovery_time_ms{0.0};
  };
  FailureStats GetStats() const;
  
  // 设置维护模式
  Status SetMaintenanceMode(NodeID node_id, bool enable);
  bool IsInMaintenanceMode(NodeID node_id) const;

 private:
  // 故障处理流程
  void DetectionLoop();
  void RecoveryLoop();
  Status ExecuteRecovery(const FailureEvent& event);
  bool ShouldAttemptRecovery(const FailureEvent& event);
  RecoveryAction DetermineRecoveryAction(const FailureEvent& event);
  
  // 记录故障历史
  void RecordFailure(const FailureEvent& event);
  void MarkRecovered(uint64_t event_id);
  
  Config config_;
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> next_event_id_{1};
  
  // 故障检测器
  std::vector<std::function<std::vector<FailureEvent>()>> detectors_;
  mutable std::mutex detectors_mutex_;
  
  // 恢复处理器
  std::unordered_map<FailureType, 
                     std::function<Status(const FailureEvent&)>> handlers_;
  mutable std::mutex handlers_mutex_;
  
  // 故障记录
  mutable std::mutex failures_mutex_;
  std::unordered_map<uint64_t, FailureEvent> failures_;
  std::vector<uint64_t> active_failures_;
  
  // 维护模式
  mutable std::mutex maintenance_mutex_;
  std::unordered_set<NodeID> maintenance_nodes_;
  
  // 统计
  mutable std::mutex stats_mutex_;
  FailureStats stats_;
  
  // 后台线程
  std::thread detection_thread_;
  std::thread recovery_thread_;
};

// =============================================================================
// 全局访问
// =============================================================================

ClusterFailoverManager* GetGlobalFailoverManager();
PartitionFailoverController* GetGlobalPartitionFailoverController();

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_FAILOVER_MANAGER_H_
