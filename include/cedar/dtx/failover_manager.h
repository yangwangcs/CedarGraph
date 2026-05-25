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
#include "cedar/core/threading.h"
#include "cedar/dtx/phi_accrual.h"
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
// Multi-Dimensional Health Metrics
// =============================================================================

struct HealthMetrics {
  double tcp_latency_ms{0};
  double raft_replication_lag{0};
  double disk_io_latency_ms{0};
  double memory_pressure_ratio{0};  // used / total
  double cpu_load_1m{0};
  double error_rate_1m{0};          // errors / sec
  std::chrono::steady_clock::time_point sampled_at;
};

struct HealthScore {
  double overall{100.0};     // 0-100
  HealthMetrics breakdown;
  bool is_degraded{false};   // predictive flag: trend is degrading
  bool is_unhealthy{false};  // hard failure: phi threshold exceeded or critical metric
};

// Callback to collect health metrics from external subsystems (Raft, storage, etc.)
using HealthMetricsCollector = std::function<HealthMetrics(NodeID)>;

// =============================================================================
// 故障恢复策略
// =============================================================================

enum class RecoveryStrategy : uint8_t {
  kRestartService = 0,     // 重启服务（容器化环境感知）
  kSwitchLeader = 1,       // 切换 Leader
  kPromoteReplica = 2,     // 提升副本
  kMigratePartition = 3,   // 迁移分区
  kScaleOut = 4,           // 扩容
  kManualIntervention = 5, // 人工介入
};

// =============================================================================
// 容器化运行环境检测
// =============================================================================

enum class ContainerRuntime : uint8_t {
  kBareMetal = 0,    // 裸机/虚拟机（传统进程管理）
  kKubernetes = 1,   // Kubernetes Pod
  kSystemd = 2,      // systemd service
  kDocker = 3,       // Docker 容器（非 K8s）
  kUnknown = 4,      // 无法确定
};

// 检测当前运行环境
ContainerRuntime DetectContainerRuntime();
std::string ContainerRuntimeToString(ContainerRuntime runtime);

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
    NodeID local_node_id{0};                                  // 本地节点ID
    std::chrono::milliseconds check_interval{1000};          // 健康检查间隔
    FailureDetectionConfig detection_config;                 // 故障检测配置
  };
  
  PartitionFailoverController();
  ~PartitionFailoverController();
  
  // 禁止拷贝
  PartitionFailoverController(const PartitionFailoverController&) = delete;
  PartitionFailoverController& operator=(const PartitionFailoverController&) = delete;
  
  Status Initialize(const Config& config);
  void Shutdown() noexcept;
  
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

  // Register consensus transfer callback (Raft-based leader transfer)
  using ConsensusTransferCallback = std::function<Status(PartitionID pid, NodeID new_leader)>;
  void SetConsensusTransferCallback(ConsensusTransferCallback callback);
  
  // Register node address for health probing
  void RegisterNodeAddress(NodeID node_id, const std::string& address);

  // Register an external metrics collector (e.g., Raft lag, disk I/O)
  void RegisterHealthMetricsCollector(HealthMetricsCollector collector);

  // Get the latest computed health score for a node (for external consumers)
  std::optional<HealthScore> GetHealthScore(NodeID node_id) const;

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
  mutable std::mutex cv_mutex_;
  std::condition_variable cv_;
  
  mutable std::mutex partitions_mutex_;
  std::unordered_map<PartitionID, PartitionState> partitions_;
  
  std::vector<FailoverCallback> callbacks_;
  mutable std::mutex callbacks_mutex_;
  
  RouteUpdateCallback route_update_callback_;
  mutable std::mutex route_mutex_;

  ConsensusTransferCallback consensus_transfer_callback_;
  mutable std::mutex consensus_callback_mutex_;
  
  std::thread lease_thread_;
  std::thread health_thread_;

  // Bounded worker pool for failover tasks (prevents unbounded thread spawning)
  std::unique_ptr<cedar::ThreadPool> failover_worker_pool_;
  
  bool CheckReplicaHealth(NodeID node_id);
  bool PerformActiveHealthCheck(NodeID node_id);

  // Multi-dimensional health score computation
  HealthMetrics CollectMetrics(NodeID node_id, bool* tcp_ok_out = nullptr);
  HealthScore ComputeScore(const HealthMetrics& metrics);
  bool IsTrendDegrading(NodeID node_id, const HealthScore& current);
  void TriggerGraduatedDegradation(NodeID node_id);
  void CancelGraduatedDegradation(NodeID node_id);

  // Legacy boolean health (kept for backward compat)
  std::unordered_map<NodeID, bool> replica_health_;
  mutable std::mutex replica_health_mutex_;

  // Multi-dimensional health scores
  std::unordered_map<NodeID, HealthScore> health_scores_;
  mutable std::mutex health_scores_mutex_;

  // Score history for trend detection (per node, last N samples)
  std::unordered_map<NodeID, std::deque<double>> score_history_;
  mutable std::mutex score_history_mutex_;

  // Nodes currently in graduated degradation
  std::unordered_set<NodeID> degraded_nodes_;
  mutable std::mutex degraded_nodes_mutex_;

  // Phi Accrual detectors per node
  std::unordered_map<NodeID, std::unique_ptr<PhiAccrualDetector>> phi_detectors_;
  mutable std::mutex phi_detectors_mutex_;

  // External metrics collectors
  std::vector<HealthMetricsCollector> metrics_collectors_;
  mutable std::mutex collectors_mutex_;

  // Node address registry (populated by RegisterPartition or external caller)
  std::unordered_map<NodeID, std::string> node_addresses_;
  mutable std::mutex node_addresses_mutex_;

  std::unordered_map<PartitionID, std::chrono::steady_clock::time_point> lease_expiry_;
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
  void Shutdown() noexcept;
  
  // 注册故障检测器
  void RegisterFailureDetector(
      std::function<std::vector<FailureEvent>()> detector);
  
  // 注册恢复处理器
  void RegisterRecoveryHandler(FailureType type,
      std::function<Status(const FailureEvent&)> handler);
  
  // 注册 Leader 切换和副本提升处理器
  using SwitchLeaderHandler = std::function<Status(PartitionID, NodeID)>;
  using PromoteReplicaHandler = std::function<Status(PartitionID, NodeID)>;
  
  void RegisterSwitchLeaderHandler(SwitchLeaderHandler handler);
  void RegisterPromoteReplicaHandler(PromoteReplicaHandler handler);
  
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
  
  // 容器化恢复策略
  Status ExecuteContainerizedRecovery(const FailureEvent& event, ContainerRuntime runtime);
  Status RestartViaKubernetes(const FailureEvent& event);
  Status RestartViaSystemd(const FailureEvent& event);
  Status RestartViaSignal(const FailureEvent& event);
  
  // 运行环境（延迟检测）
  ContainerRuntime DetectedRuntime() const;
  mutable std::atomic<ContainerRuntime> detected_runtime_{ContainerRuntime::kUnknown};
  mutable std::once_flag detect_runtime_once_;
  
  // 记录故障历史
  void RecordFailure(const FailureEvent& event);
  void MarkRecovered(uint64_t event_id);
  
  Config config_;
  std::atomic<bool> running_{false};
  mutable std::mutex cv_mutex_;
  std::condition_variable cv_;
  std::atomic<uint64_t> next_event_id_{1};
  
  // 故障检测器
  std::vector<std::function<std::vector<FailureEvent>()>> detectors_;
  mutable std::mutex detectors_mutex_;
  
  // 恢复处理器
  std::unordered_map<FailureType, 
                     std::function<Status(const FailureEvent&)>> handlers_;
  mutable std::mutex handlers_mutex_;
  
  // Leader 切换和副本提升处理器
  SwitchLeaderHandler switch_leader_handler_;
  PromoteReplicaHandler promote_replica_handler_;
  mutable std::mutex switch_leader_mutex_;
  mutable std::mutex promote_replica_mutex_;
  
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
