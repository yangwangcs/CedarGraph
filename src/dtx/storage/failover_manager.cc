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

#include "cedar/dtx/failover_manager.h"

#include <algorithm>

namespace cedar {
namespace dtx {

// =============================================================================
// PartitionFailoverController Implementation
// =============================================================================

PartitionFailoverController::PartitionFailoverController() = default;

PartitionFailoverController::~PartitionFailoverController() {
  Shutdown();
}

Status PartitionFailoverController::Initialize(const Config& config) {
  config_ = config;
  running_.store(true);
  
  lease_thread_ = std::thread(&PartitionFailoverController::LeaseRenewalLoop, this);
  health_thread_ = std::thread(&PartitionFailoverController::HealthCheckLoop, this);
  
  return Status::OK();
}

void PartitionFailoverController::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  if (lease_thread_.joinable()) {
    lease_thread_.join();
  }
  if (health_thread_.joinable()) {
    health_thread_.join();
  }
}

Status PartitionFailoverController::RegisterPartition(
    PartitionID pid, NodeID leader, const std::vector<NodeID>& replicas) {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  
  PartitionState state;
  state.pid = pid;
  state.current_leader = leader;
  state.replicas = replicas;
  state.is_failover_in_progress = false;
  
  partitions_[pid] = state;
  
  return Status::OK();
}

Status PartitionFailoverController::UnregisterPartition(PartitionID pid) {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  partitions_.erase(pid);
  return Status::OK();
}

Status PartitionFailoverController::ReportNodeFailure(NodeID node_id) {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  
  // 检查该节点是否是某个分区的 Leader
  for (auto& partition : partitions_) {
    PartitionID pid = partition.first;
    auto& state = partition.second;
    if (state.current_leader == node_id) {
      if (!state.is_failover_in_progress) {
        state.is_failover_in_progress = true;
        
        // 异步执行故障转移
        std::thread([this, pid]() {
          ExecuteFailover(pid);
        }).detach();
      }
    }
  }
  
  return Status::OK();
}

Status PartitionFailoverController::ReportLeaderFailure(PartitionID pid) {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  
  auto it = partitions_.find(pid);
  if (it == partitions_.end()) {
    return Status::NotFound("Partition not found");
  }
  
  if (!it->second.is_failover_in_progress) {
    it->second.is_failover_in_progress = true;
    
    std::thread([this, pid]() {
      ExecuteFailover(pid);
    }).detach();
  }
  
  return Status::OK();
}

Status PartitionFailoverController::TriggerManualFailover(
    PartitionID pid, NodeID new_leader) {
  return PerformLeaderSwitch(pid, new_leader);
}

StatusOr<PartitionFailoverController::PartitionState> 
PartitionFailoverController::GetPartitionState(PartitionID pid) const {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  
  auto it = partitions_.find(pid);
  if (it == partitions_.end()) {
    return Status::NotFound("Partition not found");
  }
  
  return it->second;
}

void PartitionFailoverController::RegisterFailoverCallback(FailoverCallback callback) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  callbacks_.push_back(std::move(callback));
}

void PartitionFailoverController::ExecuteFailover(PartitionID pid) {
  NodeID old_leader;
  NodeID new_leader;
  
  // 选择新的 Leader
  auto status = SelectNewLeader(pid, &new_leader);
  if (!status.ok()) {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it != partitions_.end()) {
      it->second.is_failover_in_progress = false;
    }
    return;
  }
  
  {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it != partitions_.end()) {
      old_leader = it->second.current_leader;
      it->second.failover_target = new_leader;
    }
  }
  
  // 执行 Leader 切换
  status = PerformLeaderSwitch(pid, new_leader);
  
  {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it != partitions_.end()) {
      it->second.is_failover_in_progress = false;
      it->second.last_failover = std::chrono::steady_clock::now();
      
      if (status.ok()) {
        it->second.current_leader = new_leader;
      }
    }
  }
  
  // 通知回调
  if (status.ok()) {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    for (auto& callback : callbacks_) {
      callback(pid, old_leader, new_leader);
    }
  }
}

Status PartitionFailoverController::SelectNewLeader(PartitionID pid, 
                                                     NodeID* new_leader) {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  
  auto it = partitions_.find(pid);
  if (it == partitions_.end()) {
    return Status::NotFound("Partition not found");
  }
  
  // 从副本中选择新的 Leader（选择 ID 最小的健康副本）
  for (NodeID replica : it->second.replicas) {
    if (replica != it->second.current_leader) {
      // TODO: 检查副本健康状态
      *new_leader = replica;
      return Status::OK();
    }
  }
  
  return Status::IOError("No healthy replica available");
}

void PartitionFailoverController::SetRouteUpdateCallback(
    RouteUpdateCallback callback) {
  route_update_callback_ = std::move(callback);
}

Status PartitionFailoverController::PerformLeaderSwitch(PartitionID pid,
                                                         NodeID new_leader) {
  NodeID old_leader = kInvalidNodeID;
  {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it == partitions_.end()) {
      return Status::NotFound("Partition not found");
    }
    old_leader = it->second.current_leader;
    it->second.current_leader = new_leader;
  }
  
  // Update routing metadata
  auto status = UpdatePartitionRoute(pid, new_leader);
  if (!status.ok()) {
    // Rollback on failure
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it != partitions_.end()) {
      it->second.current_leader = old_leader;
    }
    return status;
  }
  
  return Status::OK();
}

Status PartitionFailoverController::UpdatePartitionRoute(PartitionID pid,
                                                          NodeID new_leader) {
  if (route_update_callback_) {
    route_update_callback_(pid, new_leader);
  }
  return Status::OK();
}

void PartitionFailoverController::LeaseRenewalLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(config_.leader_lease_duration / 2);
    
    if (!running_.load()) break;
    
    // Leader 续约逻辑
    // TODO: 实现租约续期
  }
}

void PartitionFailoverController::HealthCheckLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    if (!running_.load()) break;
    
    // 健康检查逻辑
    // TODO: 定期检查 Leader 健康状态
  }
}

// =============================================================================
// ClusterFailoverManager Implementation
// =============================================================================

ClusterFailoverManager::ClusterFailoverManager() = default;

ClusterFailoverManager::~ClusterFailoverManager() {
  Shutdown();
}

Status ClusterFailoverManager::Initialize(const Config& config) {
  config_ = config;
  running_.store(true);
  
  detection_thread_ = std::thread(&ClusterFailoverManager::DetectionLoop, this);
  recovery_thread_ = std::thread(&ClusterFailoverManager::RecoveryLoop, this);
  
  return Status::OK();
}

void ClusterFailoverManager::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  if (detection_thread_.joinable()) {
    detection_thread_.join();
  }
  if (recovery_thread_.joinable()) {
    recovery_thread_.join();
  }
}

void ClusterFailoverManager::RegisterFailureDetector(
    std::function<std::vector<FailureEvent>()> detector) {
  std::lock_guard<std::mutex> lock(detectors_mutex_);
  detectors_.push_back(std::move(detector));
}

void ClusterFailoverManager::RegisterRecoveryHandler(
    FailureType type,
    std::function<Status(const FailureEvent&)> handler) {
  std::lock_guard<std::mutex> lock(handlers_mutex_);
  handlers_[type] = std::move(handler);
}

Status ClusterFailoverManager::ReportFailure(const FailureEvent& event) {
  FailureEvent new_event = event;
  new_event.event_id = next_event_id_.fetch_add(1);
  new_event.detected_at = std::chrono::system_clock::now();
  new_event.is_recovered = false;
  
  RecordFailure(new_event);
  
  {
    std::lock_guard<std::mutex> lock(failures_mutex_);
    active_failures_.push_back(new_event.event_id);
  }
  
  return Status::OK();
}

Status ClusterFailoverManager::TriggerRecovery(uint64_t event_id) {
  std::lock_guard<std::mutex> lock(failures_mutex_);
  
  auto it = failures_.find(event_id);
  if (it == failures_.end()) {
    return Status::NotFound("Failure event not found");
  }
  
  if (it->second.is_recovered) {
    return Status::InvalidArgument("Failure already recovered");
  }
  
  // 执行恢复
  return ExecuteRecovery(it->second);
}

std::vector<FailureEvent> ClusterFailoverManager::GetFailureHistory(
    NodeID node_id) const {
  std::lock_guard<std::mutex> lock(failures_mutex_);
  
  std::vector<FailureEvent> result;
  
  for (const auto& [id, event] : failures_) {
    if (node_id == 0 || event.node_id == node_id) {
      result.push_back(event);
    }
  }
  
  // 按时间排序
  std::sort(result.begin(), result.end(),
            [](const FailureEvent& a, const FailureEvent& b) {
              return a.detected_at > b.detected_at;
            });
  
  return result;
}

std::vector<FailureEvent> ClusterFailoverManager::GetActiveFailures() const {
  std::lock_guard<std::mutex> lock(failures_mutex_);
  
  std::vector<FailureEvent> result;
  
  for (uint64_t event_id : active_failures_) {
    auto it = failures_.find(event_id);
    if (it != failures_.end() && !it->second.is_recovered) {
      result.push_back(it->second);
    }
  }
  
  return result;
}

ClusterFailoverManager::FailureStats ClusterFailoverManager::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

Status ClusterFailoverManager::SetMaintenanceMode(NodeID node_id, bool enable) {
  std::lock_guard<std::mutex> lock(maintenance_mutex_);
  
  if (enable) {
    maintenance_nodes_.insert(node_id);
  } else {
    maintenance_nodes_.erase(node_id);
  }
  
  return Status::OK();
}

bool ClusterFailoverManager::IsInMaintenanceMode(NodeID node_id) const {
  std::lock_guard<std::mutex> lock(maintenance_mutex_);
  return maintenance_nodes_.find(node_id) != maintenance_nodes_.end();
}

void ClusterFailoverManager::DetectionLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(config_.detection_config.check_interval);
    
    if (!running_.load()) break;
    
    // 运行所有注册的故障检测器
    std::vector<std::function<std::vector<FailureEvent>()>> detectors;
    {
      std::lock_guard<std::mutex> lock(detectors_mutex_);
      detectors = detectors_;
    }
    
    for (auto& detector : detectors) {
      auto events = detector();
      for (auto& event : events) {
        ReportFailure(event);
      }
    }
  }
}

void ClusterFailoverManager::RecoveryLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    if (!running_.load()) break;
    
    // 处理活跃故障
    std::vector<uint64_t> active;
    {
      std::lock_guard<std::mutex> lock(failures_mutex_);
      active = active_failures_;
    }
    
    for (uint64_t event_id : active) {
      FailureEvent event;
      {
        std::lock_guard<std::mutex> lock(failures_mutex_);
        auto it = failures_.find(event_id);
        if (it == failures_.end() || it->second.is_recovered) {
          continue;
        }
        event = it->second;
      }
      
      // 检查是否处于维护模式
      if (IsInMaintenanceMode(event.node_id)) {
        continue;
      }
      
      // 检查是否应该尝试恢复
      if (ShouldAttemptRecovery(event)) {
        auto status = ExecuteRecovery(event);
        
        if (status.ok()) {
          MarkRecovered(event_id);
          
          std::lock_guard<std::mutex> lock(stats_mutex_);
          stats_.recovered_failures++;
          stats_.auto_recoveries++;
        } else {
          // 增加重试计数
          std::lock_guard<std::mutex> lock(failures_mutex_);
          auto it = failures_.find(event_id);
          if (it != failures_.end()) {
            it->second.retry_count++;
          }
        }
      }
    }
  }
}

Status ClusterFailoverManager::ExecuteRecovery(const FailureEvent& event) {
  // 确定恢复策略
  auto action = DetermineRecoveryAction(event);
  
  // 查找对应的处理器
  std::function<Status(const FailureEvent&)> handler;
  {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    auto it = handlers_.find(event.type);
    if (it != handlers_.end()) {
      handler = it->second;
    }
  }
  
  if (handler) {
    return handler(event);
  }
  
  // 默认恢复逻辑
  switch (action.strategy) {
    case RecoveryStrategy::kRestartService:
      // TODO: 重启服务
      break;
    case RecoveryStrategy::kSwitchLeader:
      // TODO: 切换 Leader
      break;
    case RecoveryStrategy::kPromoteReplica:
      // TODO: 提升副本
      break;
    case RecoveryStrategy::kManualIntervention:
      return Status::IOError("Requires manual intervention");
    default:
      break;
  }
  
  return Status::OK();
}

bool ClusterFailoverManager::ShouldAttemptRecovery(const FailureEvent& event) {
  if (!config_.enable_auto_recovery) {
    return false;
  }
  
  if (event.retry_count >= 3) {
    return false;
  }
  
  // 检查冷却期
  auto elapsed = std::chrono::system_clock::now() - event.detected_at;
  if (elapsed < config_.recovery_cooldown) {
    return false;
  }
  
  return true;
}

RecoveryAction ClusterFailoverManager::DetermineRecoveryAction(
    const FailureEvent& event) {
  RecoveryAction action;
  
  switch (event.type) {
    case FailureType::kNodeDown:
    case FailureType::kProcessCrash:
      action.strategy = RecoveryStrategy::kSwitchLeader;
      break;
    case FailureType::kLeaderLost:
      action.strategy = RecoveryStrategy::kPromoteReplica;
      break;
    case FailureType::kDiskFailure:
      action.strategy = RecoveryStrategy::kMigratePartition;
      break;
    case FailureType::kServiceTimeout:
      action.strategy = RecoveryStrategy::kRestartService;
      break;
    default:
      action.strategy = RecoveryStrategy::kManualIntervention;
      break;
  }
  
  action.target_node = event.node_id;
  action.target_partition = event.partition_id;
  action.timeout = std::chrono::milliseconds(30000);
  action.max_retries = 3;
  
  return action;
}

void ClusterFailoverManager::RecordFailure(const FailureEvent& event) {
  std::lock_guard<std::mutex> lock(failures_mutex_);
  failures_[event.event_id] = event;
  
  std::lock_guard<std::mutex> stats_lock(stats_mutex_);
  stats_.total_failures++;
  stats_.failures_by_type[event.type]++;
}

void ClusterFailoverManager::MarkRecovered(uint64_t event_id) {
  std::lock_guard<std::mutex> lock(failures_mutex_);
  
  auto it = failures_.find(event_id);
  if (it != failures_.end()) {
    it->second.is_recovered = true;
    it->second.recovered_at = std::chrono::system_clock::now();
    
    // 从活跃列表移除
    active_failures_.erase(
        std::remove(active_failures_.begin(), active_failures_.end(), event_id),
        active_failures_.end());
  }
}

// =============================================================================
// Global Instances
// =============================================================================

ClusterFailoverManager* GetGlobalFailoverManager() {
  static ClusterFailoverManager instance;
  return &instance;
}

PartitionFailoverController* GetGlobalPartitionFailoverController() {
  static PartitionFailoverController instance;
  return &instance;
}

}  // namespace dtx
}  // namespace cedar
