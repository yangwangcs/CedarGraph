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
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <thread>
#include <unistd.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <cstring>

// For Unix domain socket (sd_notify protocol)
#ifdef __linux__
#include <sys/socket.h>
#include <sys/un.h>
#endif

namespace cedar {
namespace dtx {

// =============================================================================
// Container Runtime Detection
// =============================================================================

ContainerRuntime DetectContainerRuntime() {
#ifdef __linux__
  // Check for Kubernetes
  // K8s mounts service account token at this path
  std::ifstream k8s_token("/var/run/secrets/kubernetes.io/serviceaccount/token");
  if (k8s_token.good()) {
    return ContainerRuntime::kKubernetes;
  }
  
  // Check for systemd
  const char* notify_socket = std::getenv("NOTIFY_SOCKET");
  if (notify_socket != nullptr && notify_socket[0] != '\0') {
    return ContainerRuntime::kSystemd;
  }
  
  // Check for Docker (/.dockerenv or cgroup contains "docker")
  std::ifstream docker_env("/.dockerenv");
  if (docker_env.good()) {
    return ContainerRuntime::kDocker;
  }
  
  // Check cgroup for "docker" or "kubepods"
  std::ifstream cgroup("/proc/self/cgroup");
  if (cgroup.good()) {
    std::string line;
    while (std::getline(cgroup, line)) {
      if (line.find("docker") != std::string::npos) {
        return ContainerRuntime::kDocker;
      }
      if (line.find("kubepods") != std::string::npos) {
        return ContainerRuntime::kKubernetes;
      }
    }
  }
  
  return ContainerRuntime::kBareMetal;
#else
  // macOS and other platforms: default to bare metal
  return ContainerRuntime::kBareMetal;
#endif
}

std::string ContainerRuntimeToString(ContainerRuntime runtime) {
  switch (runtime) {
    case ContainerRuntime::kBareMetal: return "BareMetal";
    case ContainerRuntime::kKubernetes: return "Kubernetes";
    case ContainerRuntime::kSystemd: return "Systemd";
    case ContainerRuntime::kDocker: return "Docker";
    case ContainerRuntime::kUnknown: return "Unknown";
  }
  return "Unknown";
}

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

void PartitionFailoverController::Shutdown() noexcept {
  if (!running_.exchange(false)) {
    return;
  }
  
  try {
    if (lease_thread_.joinable()) {
      lease_thread_.join();
    }
  } catch (...) {
    std::cerr << "[FailoverManager] Lease thread join exception" << std::endl;
  }
  
  try {
    if (health_thread_.joinable()) {
      health_thread_.join();
    }
  } catch (...) {
    std::cerr << "[FailoverManager] Health thread join exception" << std::endl;
  }
  
  // Join all failover threads to prevent UAF
  std::vector<std::thread> threads_to_join;
  {
    std::lock_guard<std::mutex> lock(thread_mutex_);
    threads_to_join = std::move(failover_threads_);
    failover_threads_.clear();
  }
  for (auto& t : threads_to_join) {
    if (t.joinable()) {
      try {
        t.join();
      } catch (...) {
        std::cerr << "[FailoverManager] Failover thread join exception" << std::endl;
      }
    }
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

void PartitionFailoverController::RegisterNodeAddress(NodeID node_id, 
                                                       const std::string& address) {
    std::lock_guard<std::mutex> lock(node_addresses_mutex_);
    node_addresses_[node_id] = address;
}

Status PartitionFailoverController::UnregisterPartition(PartitionID pid) {
  std::lock_guard<std::mutex> lock(partitions_mutex_);
  partitions_.erase(pid);
  return Status::OK();
}

Status PartitionFailoverController::ReportNodeFailure(NodeID node_id) {
  if (!running_.load()) return Status::IOError("Failover controller is shutting down");
  
  std::vector<PartitionID> pending_failovers;
  {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    
    // 检查该节点是否是某个分区的 Leader
    for (auto& partition : partitions_) {
      PartitionID pid = partition.first;
      auto& state = partition.second;
      if (state.current_leader == node_id) {
        if (!state.is_failover_in_progress) {
          state.is_failover_in_progress = true;
          pending_failovers.push_back(pid);
        }
      }
    }
  }
  
  // 在锁外创建线程，避免死锁（线程内部也会获取 partitions_mutex_）
  {
    std::lock_guard<std::mutex> lock(thread_mutex_);
    for (PartitionID pid : pending_failovers) {
      failover_threads_.emplace_back([this, pid]() {
        ExecuteFailover(pid);
      });
    }
  }
  
  return Status::OK();
}

Status PartitionFailoverController::ReportLeaderFailure(PartitionID pid) {
  if (!running_.load()) return Status::IOError("Failover controller is shutting down");
  
  bool should_failover = false;
  {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    
    auto it = partitions_.find(pid);
    if (it == partitions_.end()) {
      return Status::NotFound("Partition not found");
    }
    
    if (!it->second.is_failover_in_progress) {
      it->second.is_failover_in_progress = true;
      should_failover = true;
    }
  }
  
  if (should_failover) {
    std::lock_guard<std::mutex> lock(thread_mutex_);
    failover_threads_.emplace_back([this, pid]() {
      ExecuteFailover(pid);
    });
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
    std::vector<FailoverCallback> callbacks_copy;
    {
      std::lock_guard<std::mutex> lock(callbacks_mutex_);
      callbacks_copy = callbacks_;
    }
    for (auto& callback : callbacks_copy) {
      try {
        callback(pid, old_leader, new_leader);
      } catch (...) {
        std::cerr << "[FailoverManager] Failover callback exception" << std::endl;
      }
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
    if (replica != it->second.current_leader && CheckReplicaHealth(replica)) {
      *new_leader = replica;
      return Status::OK();
    }
  }
  
  return Status::IOError("No healthy replica available");
}

bool PartitionFailoverController::PerformActiveHealthCheck(NodeID node_id) {
    std::string address;
    {
        std::lock_guard<std::mutex> lock(node_addresses_mutex_);
        auto it = node_addresses_.find(node_id);
        if (it == node_addresses_.end()) {
            // No address known — we cannot probe, so conservatively mark unhealthy
            // to force the operator to register addresses.
            std::lock_guard<std::mutex> health_lock(replica_health_mutex_);
            replica_health_[node_id] = false;
            return false;
        }
        address = it->second;
    }

    // Parse host:port
    size_t colon = address.rfind(':');
    if (colon == std::string::npos) {
        std::lock_guard<std::mutex> health_lock(replica_health_mutex_);
        replica_health_[node_id] = false;
        return false;
    }

    std::string host = address.substr(0, colon);
    int port = std::stoi(address.substr(colon + 1));

    // TCP connect probe with 500ms timeout
    bool healthy = false;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

        // Set non-blocking for timeout control
        int flags = fcntl(sock, F_GETFL, 0);
        fcntl(sock, F_SETFL, flags | O_NONBLOCK);

        int rc = connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
        if (rc == 0) {
            healthy = true;  // Immediate connect
        } else if (errno == EINPROGRESS) {
            fd_set fdset;
            FD_ZERO(&fdset);
            FD_SET(sock, &fdset);
            struct timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 500000;  // 500ms
            rc = select(sock + 1, nullptr, &fdset, nullptr, &tv);
            if (rc > 0) {
                int so_error;
                socklen_t len = sizeof(so_error);
                getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len);
                healthy = (so_error == 0);
            }
        }
        close(sock);
    }

    {
        std::lock_guard<std::mutex> health_lock(replica_health_mutex_);
        replica_health_[node_id] = healthy;
    }

    if (!healthy) {
        std::cerr << "[Failover] Health check FAILED for node " << node_id
                  << " at " << address << std::endl;
    }

    return healthy;
}

bool PartitionFailoverController::CheckReplicaHealth(NodeID node_id) {
    std::unique_lock<std::mutex> lock(replica_health_mutex_);
    auto it = replica_health_.find(node_id);
    if (it != replica_health_.end()) {
        return it->second;
    }
    // No health record — trigger active probe instead of blindly trusting
    lock.unlock();  // release before calling to avoid deadlock
    return PerformActiveHealthCheck(node_id);
}

void PartitionFailoverController::SetRouteUpdateCallback(
    RouteUpdateCallback callback) {
  std::lock_guard<std::mutex> lock(route_mutex_);
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
  RouteUpdateCallback callback;
  {
    std::lock_guard<std::mutex> lock(route_mutex_);
    callback = route_update_callback_;
  }
  if (callback) {
    callback(pid, new_leader);
  }
  return Status::OK();
}

void PartitionFailoverController::LeaseRenewalLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(config_.leader_lease_duration / 2);
    
    if (!running_.load()) break;
    
    try {
      std::lock_guard<std::mutex> lock(partitions_mutex_);
      auto now = std::chrono::steady_clock::now();
      for (auto& [pid, state] : partitions_) {
        if (state.current_leader == config_.local_node_id) {
          lease_expiry_[pid] = now + config_.leader_lease_duration;
        }
      }
    } catch (...) {
      std::cerr << "[FailoverManager] Lease renewal exception" << std::endl;
    }
  }
}

void PartitionFailoverController::HealthCheckLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    if (!running_.load()) break;
    
    try {
      // --- Existing leader lease expiry check ---
      auto now = std::chrono::steady_clock::now();
      std::vector<PartitionID> expired_leaders;
      
      {
        std::lock_guard<std::mutex> lock(partitions_mutex_);
        for (const auto& [pid, state] : partitions_) {
          auto lease_it = lease_expiry_.find(pid);
          if (lease_it != lease_expiry_.end()) {
            if (lease_it->second < now) {
              // Lease expired
              if (!state.is_failover_in_progress) {
                expired_leaders.push_back(pid);
              }
            } else if (lease_it->second - now < std::chrono::seconds(2)) {
              // Lease expiring soon - log warning
              std::cerr << "[Failover] Leader lease for partition " << pid
                        << " expiring soon" << std::endl;
            }
          }
        }
      }
      
      for (PartitionID pid : expired_leaders) {
        std::cerr << "[Failover] Leader lease expired for partition " << pid
                  << ", triggering failover" << std::endl;
        ReportLeaderFailure(pid);
      }
      
      // --- NEW: Active replica health probing ---
      std::vector<NodeID> replicas_to_probe;
      {
        std::lock_guard<std::mutex> lock(partitions_mutex_);
        for (const auto& [pid, state] : partitions_) {
          for (NodeID replica : state.replicas) {
            replicas_to_probe.push_back(replica);
          }
        }
      }
      // Deduplicate
      std::sort(replicas_to_probe.begin(), replicas_to_probe.end());
      replicas_to_probe.erase(
          std::unique(replicas_to_probe.begin(), replicas_to_probe.end()),
          replicas_to_probe.end());

      for (NodeID node_id : replicas_to_probe) {
        PerformActiveHealthCheck(node_id);
      }
    } catch (...) {
      std::cerr << "[FailoverManager] Health check exception" << std::endl;
    }
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

void ClusterFailoverManager::Shutdown() noexcept {
  if (!running_.exchange(false)) {
    return;
  }
  
  try {
    if (detection_thread_.joinable()) {
      detection_thread_.join();
    }
  } catch (...) {
    std::cerr << "[FailoverManager] Detection thread join exception" << std::endl;
  }
  
  try {
    if (recovery_thread_.joinable()) {
      recovery_thread_.join();
    }
  } catch (...) {
    std::cerr << "[FailoverManager] Recovery thread join exception" << std::endl;
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
  FailureEvent event;
  {
    std::lock_guard<std::mutex> lock(failures_mutex_);
    
    auto it = failures_.find(event_id);
    if (it == failures_.end()) {
      return Status::NotFound("Failure event not found");
    }
    
    if (it->second.is_recovered) {
      return Status::InvalidArgument("Failure already recovered");
    }
    
    event = it->second;
  }
  
  // 在锁外执行恢复，避免 handler 回调内部再次获取 failures_mutex_ 导致死锁
  return ExecuteRecovery(event);
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
    
    try {
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
    } catch (...) {
      std::cerr << "[FailoverManager] Detection loop exception" << std::endl;
    }
  }
}

void ClusterFailoverManager::RecoveryLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    if (!running_.load()) break;
    
    try {
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
    } catch (...) {
      std::cerr << "[FailoverManager] Recovery loop exception" << std::endl;
    }
  }
}

ContainerRuntime ClusterFailoverManager::DetectedRuntime() const {
  std::call_once(detect_runtime_once_, [this]() {
    detected_runtime_.store(DetectContainerRuntime());
    std::cerr << "[ClusterFailover] Detected runtime: "
              << ContainerRuntimeToString(detected_runtime_.load()) << std::endl;
  });
  return detected_runtime_.load();
}

Status ClusterFailoverManager::RestartViaKubernetes(const FailureEvent& event) {
  std::cerr << "[ClusterFailover] K8s restart: sending SIGTERM to self (PID "
            << getpid() << ") for graceful pod restart. Node=" << event.node_id << std::endl;
  
  // 发送 SIGTERM 给自身进程，让 K8s 重新调度 pod
  // K8s 的 preStop hook 和 grace period 会处理优雅退出
  int rc = std::raise(SIGTERM);
  if (rc != 0) {
    std::cerr << "[ClusterFailover] raise(SIGTERM) failed, trying SIGKILL" << std::endl;
    rc = std::raise(SIGKILL);
    if (rc != 0) {
      return Status::IOError("Failed to send termination signal to self");
    }
  }
  
  // 如果 raise 没有立即终止进程（某些信号处理程序可能捕获了 SIGTERM）
  // 等待一小段时间让 K8s 处理终止
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return Status::OK();
}

Status ClusterFailoverManager::RestartViaSystemd(const FailureEvent& event) {
#ifdef __linux__
  const char* notify_socket = std::getenv("NOTIFY_SOCKET");
  if (!notify_socket || notify_socket[0] == '\0') {
    return RestartViaSignal(event);
  }
  
  std::cerr << "[ClusterFailover] systemd restart via NOTIFY_SOCKET="
            << notify_socket << ", node=" << event.node_id << std::endl;
  
  // 实现 sd_notify 协议：向 NOTIFY_SOCKET 发送 WATCHDOG=trigger
  // 这会触发 systemd 的重启策略（Restart=on-failure/on-abnormal 等）
  int fd = ::socket(AF_UNIX, SOCK_DGRAM, 0);
  if (fd < 0) {
    return RestartViaSignal(event);
  }
  
  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  
  // NOTIFY_SOCKET may be absolute path (@ for abstract namespace on Linux)
  if (notify_socket[0] == '@') {
    // Abstract namespace socket
    addr.sun_path[0] = '\0';
    std::strncpy(addr.sun_path + 1, notify_socket + 1, sizeof(addr.sun_path) - 2);
  } else {
    std::strncpy(addr.sun_path, notify_socket, sizeof(addr.sun_path) - 1);
  }
  
  if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return RestartViaSignal(event);
  }
  
  // 发送 WATCHDOG=trigger 让 systemd 认为服务不健康并触发重启
  const char* msg = "WATCHDOG=trigger\n";
  ssize_t sent = ::send(fd, msg, std::strlen(msg), MSG_NOSIGNAL);
  ::close(fd);
  
  if (sent < 0) {
    return RestartViaSignal(event);
  }
  
  std::cerr << "[ClusterFailover] systemd WATCHDOG=trigger sent successfully" << std::endl;
  
  // 同时发送 SIGTERM 以加速重启
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  return RestartViaSignal(event);
#else
  return RestartViaSignal(event);
#endif
}

Status ClusterFailoverManager::RestartViaSignal(const FailureEvent& event) {
  std::cerr << "[ClusterFailover] Sending SIGTERM to self (PID " << getpid()
            << ") for service restart. Node=" << event.node_id << std::endl;
  
  int rc = std::raise(SIGTERM);
  if (rc != 0) {
    return Status::IOError("Failed to raise SIGTERM");
  }
  
  std::this_thread::sleep_for(std::chrono::seconds(3));
  return Status::OK();
}

Status ClusterFailoverManager::ExecuteContainerizedRecovery(
    const FailureEvent& event, ContainerRuntime runtime) {
  switch (runtime) {
    case ContainerRuntime::kKubernetes:
      return RestartViaKubernetes(event);
    case ContainerRuntime::kSystemd:
      return RestartViaSystemd(event);
    case ContainerRuntime::kDocker:
      // Docker 环境下也发送 SIGTERM，让容器重启策略处理
      return RestartViaSignal(event);
    case ContainerRuntime::kBareMetal:
    case ContainerRuntime::kUnknown:
    default:
      std::cerr << "[ClusterFailover] Service restart on bare metal: "
                << "node=" << event.node_id
                << ". Consider configuring systemd or K8s for automatic restart."
                << std::endl;
      // 裸机环境：发送 SIGTERM 后返回 OK，依赖外部进程管理器（如 systemd）重启
      return RestartViaSignal(event);
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
    case RecoveryStrategy::kRestartService: {
      auto runtime = DetectedRuntime();
      std::cerr << "[ClusterFailover] Service restart for node " << event.node_id
                << " in " << ContainerRuntimeToString(runtime) << " environment" << std::endl;
      return ExecuteContainerizedRecovery(event, runtime);
    }
    case RecoveryStrategy::kSwitchLeader: {
      std::cerr << "[ClusterFailoverManager] Leader switch requested for partition "
                << event.partition_id << " to node " << event.node_id << std::endl;
      return Status::IOError("No recovery handler registered for leader switch");
    }
    case RecoveryStrategy::kPromoteReplica: {
      std::cerr << "[ClusterFailoverManager] Replica promotion requested for partition "
                << event.partition_id << " on node " << event.node_id << std::endl;
      return Status::IOError("No recovery handler registered for replica promotion");
    }
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
