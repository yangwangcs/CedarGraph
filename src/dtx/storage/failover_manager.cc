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
    default:
      std::cerr << "[FailoverManager] Unknown container runtime" << std::endl;
      return "Unknown";
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
  failover_worker_pool_ = std::make_unique<cedar::ThreadPool>(
      config_.max_failover_threads > 0 ? config_.max_failover_threads : 16);
  running_.store(true);
  
  lease_thread_ = std::thread(&PartitionFailoverController::LeaseRenewalLoop, this);
  health_thread_ = std::thread(&PartitionFailoverController::HealthCheckLoop, this);
  
  return Status::OK();
}

void PartitionFailoverController::Shutdown() noexcept {
  if (!running_.exchange(false)) {
    return;
  }
  
  {
    std::lock_guard<std::mutex> lock(cv_mutex_);
    cv_.notify_all();
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
  
  // Wait for all queued failover tasks to finish before destruction
  if (failover_worker_pool_) {
    failover_worker_pool_->WaitForAll();
    failover_worker_pool_.reset();
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
  
  // Schedule failover tasks on the bounded worker pool
  for (PartitionID pid : pending_failovers) {
    failover_worker_pool_->Schedule([this, pid]() {
      ExecuteFailover(pid);
    });
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
    failover_worker_pool_->Schedule([this, pid]() {
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
  std::lock_guard<std::mutex> lock(callback_mutex_);
  callbacks_.push_back(std::move(callback));
}

void PartitionFailoverController::ExecuteFailover(PartitionID pid) {
  NodeID old_leader;
  
  auto candidates = GetLeaderCandidates(pid);
  if (candidates.empty()) {
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
      it->second.failover_target = candidates[0];
    }
  }
  
  // 执行 Leader 切换，按优先级逐个尝试候选节点
  Status status;
  NodeID successful_leader = 0;
  for (NodeID candidate : candidates) {
    status = PerformLeaderSwitch(pid, candidate);
    if (status.ok()) {
      successful_leader = candidate;
      break;
    }
    std::cerr << "[FailoverManager] Candidate " << candidate
              << " failed for partition=" << pid
              << ", trying next candidate..." << std::endl;
  }
  
  {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it != partitions_.end()) {
      it->second.is_failover_in_progress = false;
      it->second.last_failover = std::chrono::steady_clock::now();
      // PerformLeaderSwitch already updates current_leader on success
    }
  }
  
  // 通知回调
  if (status.ok()) {
    std::vector<FailoverCallback> callbacks_copy;
    {
      std::lock_guard<std::mutex> lock(callback_mutex_);
      callbacks_copy = callbacks_;
    }
    for (auto& callback : callbacks_copy) {
      try {
        callback(pid, old_leader, successful_leader);
      } catch (...) {
        std::cerr << "[FailoverManager] Failover callback exception" << std::endl;
      }
    }
  }
}

std::vector<NodeID> PartitionFailoverController::GetLeaderCandidates(PartitionID pid) {
  NodeID current_leader = 0;
  NodeID failover_target = 0;
  std::chrono::steady_clock::time_point last_failover;
  std::vector<NodeID> replicas;
  
  {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it == partitions_.end()) {
      return {};
    }
    current_leader = it->second.current_leader;
    failover_target = it->second.failover_target;
    last_failover = it->second.last_failover;
    replicas = it->second.replicas;
  }
  
  std::vector<std::pair<NodeID, double>> scored_candidates;
  
  // Perform all health checks atomically under health_state_mutex_
  // to ensure replica health and scores are consistent.
  {
    std::lock_guard<std::mutex> health_lock(health_state_mutex_);
    for (NodeID replica : replicas) {
      if (replica == current_leader) continue;
      auto health_it = replica_health_.find(replica);
      if (health_it == replica_health_.end() || !health_it->second) continue;
      
      double score = 0.0;
      // Prefer nodes with higher health scores
      auto score_it = health_scores_.find(replica);
      if (score_it != health_scores_.end()) {
        score += score_it->second.overall;
      }
      // Penalize recently failed-over targets to avoid flapping
      if (replica == failover_target) {
        auto now = std::chrono::steady_clock::now();
        if (last_failover != std::chrono::steady_clock::time_point() &&
            now - last_failover < std::chrono::seconds(30)) {
          score -= 50.0;
        }
      }
      scored_candidates.emplace_back(replica, score);
    }
  }
  
  // Sort by score descending, then by NodeID ascending as tiebreaker
  std::sort(scored_candidates.begin(), scored_candidates.end(),
            [](const auto& a, const auto& b) {
              if (a.second != b.second) return a.second > b.second;
              return a.first < b.first;
            });
  
  std::vector<NodeID> result;
  result.reserve(scored_candidates.size());
  for (const auto& [node_id, s] : scored_candidates) {
    result.push_back(node_id);
  }
  return result;
}

bool PartitionFailoverController::PerformActiveHealthCheck(NodeID node_id) {
    std::string address;
    {
        std::lock_guard<std::mutex> lock(node_addresses_mutex_);
        auto it = node_addresses_.find(node_id);
        if (it != node_addresses_.end()) {
            address = it->second;
        }
    }
    if (address.empty()) {
        // No address known — we cannot probe, so conservatively mark unhealthy
        // to force the operator to register addresses.
        std::lock_guard<std::mutex> health_lock(health_state_mutex_);
        replica_health_[node_id] = false;
        return false;
    }

    // Parse host:port
    size_t colon = address.rfind(':');
    if (colon == std::string::npos) {
        std::lock_guard<std::mutex> health_lock(health_state_mutex_);
        replica_health_[node_id] = false;
        return false;
    }

    std::string host = address.substr(0, colon);
    int port = 0;
    try {
      port = std::stoi(address.substr(colon + 1));
    } catch (const std::exception& e) {
      std::cerr << "[Failover] Invalid port in address '" << address
                << "': " << e.what() << std::endl;
      std::lock_guard<std::mutex> health_lock(health_state_mutex_);
      replica_health_[node_id] = false;
      return false;
    }

    // TCP connect probe with 500ms timeout
    bool healthy = false;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
            std::cerr << "[Failover] Invalid IPv4 address: '" << host << "'" << std::endl;
            close(sock);
            std::lock_guard<std::mutex> health_lock(health_state_mutex_);
            replica_health_[node_id] = false;
            return false;
        }

        // Set non-blocking for timeout control
        int flags = fcntl(sock, F_GETFL, 0);
        if (flags < 0 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
            std::cerr << "[Failover] fcntl failed for health probe socket" << std::endl;
            close(sock);
            std::lock_guard<std::mutex> health_lock(health_state_mutex_);
            replica_health_[node_id] = false;
            return false;
        }

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
                int so_error = 0;
                socklen_t len = sizeof(so_error);
                if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len) == 0) {
                  healthy = (so_error == 0);
                } else {
                  healthy = false;
                }
            }
        }
        close(sock);
    }

    // Deep health probe: if a callback is registered, perform app-level checks
    if (healthy) {
        std::lock_guard<std::mutex> probe_lock(callback_mutex_);
        if (health_probe_callback_) {
            healthy = health_probe_callback_(node_id, address);
        }
    }

    {
        std::lock_guard<std::mutex> health_lock(health_state_mutex_);
        replica_health_[node_id] = healthy;
    }

    if (!healthy) {
        std::cerr << "[Failover] Health check FAILED for node " << node_id
                  << " at " << address << std::endl;
    }

    return healthy;
}

bool PartitionFailoverController::CheckReplicaHealth(NodeID node_id) {
    std::lock_guard<std::mutex> lock(health_state_mutex_);
    auto it = replica_health_.find(node_id);
    if (it != replica_health_.end()) {
        return it->second;
    }
    // No health record available yet — conservatively mark unhealthy.
    // All health checks are performed asynchronously by HealthCheckLoop
    // to avoid blocking failover worker threads.
    return false;
}

void PartitionFailoverController::SetRouteUpdateCallback(
    RouteUpdateCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  route_update_callback_ = std::move(callback);
}

void PartitionFailoverController::SetConsensusTransferCallback(
    ConsensusTransferCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  consensus_transfer_callback_ = std::move(callback);
}

void PartitionFailoverController::SetQuorumVerificationCallback(
    QuorumVerificationCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  quorum_verification_callback_ = std::move(callback);
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
  }

  ConsensusTransferCallback consensus_callback;
  QuorumVerificationCallback quorum_callback;
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    consensus_callback = consensus_transfer_callback_;
    quorum_callback = quorum_verification_callback_;
  }

  if (!consensus_callback) {
    std::cerr << "[FailoverManager] WARNING: No consensus transfer callback registered. "
              << "Refusing to perform leader switch for partition=" << pid
              << " in production mode. Marking for manual intervention." << std::endl;
    {
      std::lock_guard<std::mutex> lock(partitions_mutex_);
      auto it = partitions_.find(pid);
      if (it != partitions_.end()) {
        it->second.is_failover_in_progress = false;
      }
    }
    return Status::IOError(
        "No consensus layer available for leader transfer. "
        "Manual intervention required for partition " + std::to_string(pid));
  }

  // Step 1: Request consensus layer to transfer leadership.
  Status consensus_status = consensus_callback(pid, new_leader);
  if (!consensus_status.ok()) {
    std::cerr << "[FailoverManager] Consensus transfer failed for partition="
              << pid << " target=" << new_leader
              << " error=" << consensus_status.ToString() << std::endl;
    {
      std::lock_guard<std::mutex> lock(partitions_mutex_);
      auto it = partitions_.find(pid);
      if (it != partitions_.end()) {
        it->second.is_failover_in_progress = false;
      }
    }
    return consensus_status;
  }

  // Step 2: Quorum verification -- do NOT update current_leader until
  // the consensus layer confirms the new leader is recognized by a quorum.
  if (quorum_callback) {
    Status quorum_status = quorum_callback(pid, new_leader, config_.leader_switch_timeout);
    if (!quorum_status.ok()) {
      std::cerr << "[FailoverManager] Quorum verification FAILED for partition="
                << pid << " target=" << new_leader
                << " error=" << quorum_status.ToString()
                << ". Old leader remains authoritative." << std::endl;
      {
        std::lock_guard<std::mutex> lock(partitions_mutex_);
        auto it = partitions_.find(pid);
        if (it != partitions_.end()) {
          it->second.is_failover_in_progress = false;
        }
      }
      return quorum_status;
    }
  } else {
    std::cerr << "[FailoverManager] WARNING: No quorum verification callback. "
              << "Proceeding with leader switch for partition=" << pid
              << " WITHOUT quorum confirmation. This is unsafe in production."
              << std::endl;
  }

  // Step 3: Safe to update current_leader.
  {
    std::lock_guard<std::mutex> plock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it != partitions_.end()) {
      it->second.current_leader = new_leader;
      it->second.is_failover_in_progress = false;
    }
  }
  return UpdatePartitionRoute(pid, new_leader);
}

Status PartitionFailoverController::UpdatePartitionRoute(PartitionID pid,
                                                          NodeID new_leader) {
  RouteUpdateCallback callback;
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback = route_update_callback_;
  }
  if (callback) {
    callback(pid, new_leader);
  }
  return Status::OK();
}

void PartitionFailoverController::LeaseRenewalLoop() {
  while (running_.load()) {
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, config_.leader_lease_duration / 2,
                   [this]() { return !running_.load(); });
    }
    
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
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, config_.check_interval,
                   [this]() { return !running_.load(); });
    }

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

      // --- Multi-Dimensional Health Check with Phi Accrual ---
      std::vector<NodeID> replicas_to_probe;
      {
        std::lock_guard<std::mutex> lock(partitions_mutex_);
        for (const auto& [pid, state] : partitions_) {
          for (NodeID replica : state.replicas) {
            replicas_to_probe.push_back(replica);
          }
        }
      }
      std::sort(replicas_to_probe.begin(), replicas_to_probe.end());
      replicas_to_probe.erase(
          std::unique(replicas_to_probe.begin(), replicas_to_probe.end()),
          replicas_to_probe.end());

      for (NodeID node_id : replicas_to_probe) {
        // 1. Collect metrics
        bool tcp_ok = false;
        HealthMetrics metrics = CollectMetrics(node_id, &tcp_ok);

        // 2. Compute multi-dimensional score
        HealthScore score = ComputeScore(metrics);

        bool newly_degraded = false;
        bool was_degraded = false;

        // 3. Phi Accrual: update detector and evaluate suspicion
        // 4. Predictive degradation: trend detection
        // 5. Store results
        // All health state updates are done under a single lock.
        {
          std::lock_guard<std::mutex> lock(health_state_mutex_);
          auto it = phi_detectors_.find(node_id);
          if (it == phi_detectors_.end()) {
            it = phi_detectors_.emplace(node_id,
                std::make_unique<PhiAccrualDetector>(500)).first;
          }
          // Record heartbeat only on successful TCP probe so the detector's
          // distribution models healthy intervals. On failure, silence grows
          // automatically because no heartbeat is recorded.
          if (tcp_ok) {
            it->second->RecordHeartbeat();
          }

          if (it->second->SampleCount() >= 5) {
            double phi = it->second->Phi();
            if (phi >= config_.detection_config.phi_threshold) {
              score.is_unhealthy = true;
              std::cerr << "[Failover] Phi Accrual suspects node " << node_id
                        << " (phi=" << phi << ")" << std::endl;
            }
          }

          // Predictive degradation: trend detection
          if (!score.is_unhealthy && IsTrendDegradingLocked(node_id, score)) {
            score.is_degraded = true;
            newly_degraded = TriggerGraduatedDegradationLocked(node_id);
          } else if (score.overall >= 70.0) {
            was_degraded = CancelGraduatedDegradationLocked(node_id);
          }

          replica_health_[node_id] = !score.is_unhealthy;
          health_scores_[node_id] = score;
        }

        // Fire routing callbacks outside the health lock to respect lock ordering.
        if (newly_degraded) {
          std::cerr << "[Failover] Predictive degradation triggered for node " << node_id
                    << ". Redirecting new read traffic." << std::endl;
          RouteUpdateCallback cb;
          {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            cb = route_update_callback_;
          }
          if (cb) {
            // Use a reserved partition_id 0 to signal node-level degradation
            cb(0, node_id);
          }
        }
        if (was_degraded) {
          std::cerr << "[Failover] Predictive degradation cancelled for node " << node_id
                    << ". Restoring full traffic." << std::endl;
        }

        // 6. If hard-unhealthy, report failure for leader roles
        if (score.is_unhealthy) {
          std::lock_guard<std::mutex> lock(partitions_mutex_);
          for (const auto& partition_pair : partitions_) {
            PartitionID pid = partition_pair.first;
            const auto& state = partition_pair.second;
            if (state.current_leader == node_id && !state.is_failover_in_progress) {
              std::cerr << "[Failover] Node " << node_id
                        << " unhealthy, triggering failover for partition " << pid << std::endl;
              // Launch failover asynchronously on the bounded worker pool
              failover_worker_pool_->Schedule([this, pid]() { ReportLeaderFailure(pid); });
              // Removed break: trigger failover for ALL partitions where this node is leader
            }
          }
        }
      }
    } catch (...) {
      std::cerr << "[FailoverManager] Health check exception" << std::endl;
    }
  }
}

HealthMetrics PartitionFailoverController::CollectMetrics(NodeID node_id, bool* tcp_ok_out) {
  HealthMetrics metrics;
  metrics.sampled_at = std::chrono::steady_clock::now();

  // TCP probe latency (ms). PerformActiveHealthCheck returns bool and caches.
  // We re-probe here to get fresh latency. Simplification: use the internal
  // TCP connect timeout as a proxy for latency if the node is reachable.
  auto tcp_start = std::chrono::steady_clock::now();
  bool tcp_ok = PerformActiveHealthCheck(node_id);
  auto tcp_end = std::chrono::steady_clock::now();
  metrics.tcp_latency_ms = static_cast<double>(
      std::chrono::duration_cast<std::chrono::milliseconds>(tcp_end - tcp_start).count());
  if (!tcp_ok) {
    metrics.tcp_latency_ms = 9999.0;  // Mark as extremely high
  }
  if (tcp_ok_out) {
    *tcp_ok_out = tcp_ok;
  }

  // Invoke external collectors for subsystem metrics (Raft lag, disk I/O, etc.)
  {
    std::lock_guard<std::mutex> lock(collectors_mutex_);
    for (const auto& collector : metrics_collectors_) {
      HealthMetrics ext = collector(node_id);
      // Merge: prefer external values if they are non-zero / meaningful
      if (ext.raft_replication_lag > 0) metrics.raft_replication_lag = ext.raft_replication_lag;
      if (ext.disk_io_latency_ms > 0) metrics.disk_io_latency_ms = ext.disk_io_latency_ms;
      if (ext.memory_pressure_ratio > 0) metrics.memory_pressure_ratio = ext.memory_pressure_ratio;
      if (ext.cpu_load_1m > 0) metrics.cpu_load_1m = ext.cpu_load_1m;
      if (ext.error_rate_1m > 0) metrics.error_rate_1m = ext.error_rate_1m;
    }
  }

  // Fallback: synthesize rough memory / CPU from local process if no collector
  // and this is the local node. This avoids zero-values skewing the score.
  if (node_id == config_.local_node_id) {
    if (metrics.memory_pressure_ratio <= 0.0) {
      // Rough estimate: not available without platform-specific APIs
      metrics.memory_pressure_ratio = 0.3;
    }
    if (metrics.cpu_load_1m <= 0.0) {
      metrics.cpu_load_1m = 0.3;
    }
  }

  return metrics;
}

HealthScore PartitionFailoverController::ComputeScore(const HealthMetrics& m) {
  HealthScore score;
  score.breakdown = m;

  // Weighted scoring. Weights sum to 1.0.
  // TCP latency: ideal < 10ms, death at > 2000ms
  double tcp_score = 100.0;
  if (m.tcp_latency_ms >= 2000.0) {
    tcp_score = 0.0;
  } else if (m.tcp_latency_ms > 10.0) {
    tcp_score = 100.0 * (1.0 - (m.tcp_latency_ms - 10.0) / 1990.0);
  }

  // Raft lag: ideal < 100, death at > 10000
  double raft_score = 100.0;
  if (m.raft_replication_lag >= 10000.0) {
    raft_score = 0.0;
  } else if (m.raft_replication_lag > 100.0) {
    raft_score = 100.0 * (1.0 - (m.raft_replication_lag - 100.0) / 9900.0);
  }

  // Disk I/O: ideal < 5ms, death at > 500ms
  double disk_score = 100.0;
  if (m.disk_io_latency_ms >= 500.0) {
    disk_score = 0.0;
  } else if (m.disk_io_latency_ms > 5.0) {
    disk_score = 100.0 * (1.0 - (m.disk_io_latency_ms - 5.0) / 495.0);
  }

  // Memory pressure: ideal < 50%, death at > 95%
  double mem_score = 100.0;
  if (m.memory_pressure_ratio >= 0.95) {
    mem_score = 0.0;
  } else if (m.memory_pressure_ratio > 0.5) {
    mem_score = 100.0 * (1.0 - (m.memory_pressure_ratio - 0.5) / 0.45);
  }

  // CPU load: ideal < 50%, death at > 95%
  double cpu_score = 100.0;
  if (m.cpu_load_1m >= 0.95) {
    cpu_score = 0.0;
  } else if (m.cpu_load_1m > 0.5) {
    cpu_score = 100.0 * (1.0 - (m.cpu_load_1m - 0.5) / 0.45);
  }

  // Error rate: ideal 0, death at > 100/sec
  double err_score = 100.0;
  if (m.error_rate_1m >= 100.0) {
    err_score = 0.0;
  } else {
    err_score = 100.0 * (1.0 - m.error_rate_1m / 100.0);
  }

  static constexpr double w_tcp  = 0.25;
  static constexpr double w_raft = 0.20;
  static constexpr double w_disk = 0.15;
  static constexpr double w_mem  = 0.15;
  static constexpr double w_cpu  = 0.15;
  static constexpr double w_err  = 0.10;

  score.overall = w_tcp * tcp_score
                + w_raft * raft_score
                + w_disk * disk_score
                + w_mem * mem_score
                + w_cpu * cpu_score
                + w_err * err_score;

  // Hard failure: any single dimension is completely dead or overall < 20
  if (tcp_score <= 0.0 || mem_score <= 0.0 || score.overall < 20.0) {
    score.is_unhealthy = true;
  }

  return score;
}

bool PartitionFailoverController::IsTrendDegrading(NodeID node_id,
                                                     const HealthScore& current) {
  std::lock_guard<std::mutex> lock(health_state_mutex_);
  return IsTrendDegradingLocked(node_id, current);
}

bool PartitionFailoverController::IsTrendDegradingLocked(NodeID node_id,
                                                     const HealthScore& current) {
  auto& history = score_history_[node_id];
  history.push_back(current.overall);
  if (history.size() > 10) {
    history.pop_front();
  }
  if (history.size() < 3) {
    return false;
  }
  // Check if last 3 samples are monotonically decreasing and overall < 60
  size_t n = history.size();
  bool decreasing = (history[n - 1] < history[n - 2]) &&
                    (history[n - 2] < history[n - 3]);
  return decreasing && current.overall < 60.0;
}

void PartitionFailoverController::TriggerGraduatedDegradation(NodeID node_id) {
  bool newly_degraded = false;
  {
    std::lock_guard<std::mutex> lock(health_state_mutex_);
    newly_degraded = TriggerGraduatedDegradationLocked(node_id);
  }
  if (newly_degraded) {
    std::cerr << "[Failover] Predictive degradation triggered for node " << node_id
              << ". Redirecting new read traffic." << std::endl;
    // Notify routing layer to deprioritize this node for new requests
    RouteUpdateCallback cb;
    {
      std::lock_guard<std::mutex> lock(callback_mutex_);
      cb = route_update_callback_;
    }
    if (cb) {
      // Use a reserved partition_id 0 to signal node-level degradation
      cb(0, node_id);
    }
  }
}

bool PartitionFailoverController::TriggerGraduatedDegradationLocked(NodeID node_id) {
  bool already_degraded = degraded_nodes_.find(node_id) != degraded_nodes_.end();
  if (!already_degraded) {
    degraded_nodes_.insert(node_id);
  }
  return !already_degraded;
}

void PartitionFailoverController::CancelGraduatedDegradation(NodeID node_id) {
  bool was_degraded = false;
  {
    std::lock_guard<std::mutex> lock(health_state_mutex_);
    was_degraded = CancelGraduatedDegradationLocked(node_id);
  }
  if (was_degraded) {
    std::cerr << "[Failover] Predictive degradation cancelled for node " << node_id
              << ". Restoring full traffic." << std::endl;
  }
}

bool PartitionFailoverController::CancelGraduatedDegradationLocked(NodeID node_id) {
  return degraded_nodes_.erase(node_id) > 0;
}

void PartitionFailoverController::RegisterHealthMetricsCollector(
    HealthMetricsCollector collector) {
  std::lock_guard<std::mutex> lock(collectors_mutex_);
  metrics_collectors_.push_back(std::move(collector));
}

void PartitionFailoverController::RegisterHealthProbeCallback(
    HealthProbeCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  health_probe_callback_ = std::move(callback);
}

std::optional<HealthScore> PartitionFailoverController::GetHealthScore(NodeID node_id) const {
  std::lock_guard<std::mutex> lock(health_state_mutex_);
  auto it = health_scores_.find(node_id);
  if (it != health_scores_.end()) {
    return it->second;
  }
  return std::nullopt;
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
  
  {
    std::lock_guard<std::mutex> lock(cv_mutex_);
    cv_.notify_all();
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

void ClusterFailoverManager::RegisterSwitchLeaderHandler(SwitchLeaderHandler handler) {
  std::lock_guard<std::mutex> lock(switch_leader_mutex_);
  switch_leader_handler_ = std::move(handler);
}

void ClusterFailoverManager::RegisterPromoteReplicaHandler(PromoteReplicaHandler handler) {
  std::lock_guard<std::mutex> lock(promote_replica_mutex_);
  promote_replica_handler_ = std::move(handler);
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
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, config_.detection_config.check_interval,
                   [this]() { return !running_.load(); });
    }
    
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
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, std::chrono::seconds(1),
                   [this]() { return !running_.load(); });
    }
    
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
  pid_t pid = getpid();
  std::cerr << "[ClusterFailover] K8s restart: sending SIGTERM to self (PID "
            << pid << ") for graceful pod restart. Node=" << event.node_id << std::endl;

  constexpr int kMaxTermAttempts = 3;
  constexpr auto kTermDelay = std::chrono::seconds(2);

  for (int attempt = 1; attempt <= kMaxTermAttempts; ++attempt) {
    int rc = std::raise(SIGTERM);
    if (rc != 0) {
      std::cerr << "[ClusterFailover] raise(SIGTERM) attempt " << attempt
                << " failed: " << strerror(errno) << std::endl;
      if (attempt == kMaxTermAttempts) {
        return Status::IOError(
            "Failed to send SIGTERM to self after " +
            std::to_string(kMaxTermAttempts) + " attempts");
      }
      std::this_thread::sleep_for(kTermDelay);
      continue;
    }
    std::this_thread::sleep_for(kTermDelay);
    if (kill(pid, 0) != 0) {
      std::cerr << "[ClusterFailover] Process no longer responding to kill(0), "
                << "assuming graceful shutdown is in progress." << std::endl;
      return Status::OK();
    }
    std::cerr << "[ClusterFailover] SIGTERM attempt " << attempt
              << " sent but process still alive. Retrying..." << std::endl;
  }

  std::cerr << "[ClusterFailover] All SIGTERM attempts exhausted. "
            << "Relying on external orchestrator for final termination." << std::endl;
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
    addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';
  } else {
    std::strncpy(addr.sun_path, notify_socket, sizeof(addr.sun_path) - 1);
    addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';
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

  // Enforce max_concurrent_recoveries
  {
    std::lock_guard<std::mutex> lock(active_recovery_mutex_);
    if (active_recovery_count_.load(std::memory_order_relaxed) >=
        config_.max_concurrent_recoveries) {
      return Status::ResourceExhausted(
          "Max concurrent recoveries (" +
          std::to_string(config_.max_concurrent_recoveries) +
          ") reached. Recovery action queued for later.");
    }
    active_recovery_count_.fetch_add(1, std::memory_order_relaxed);
    active_recovery_ids_.insert(action.action_id);
  }

  // RAII guard to ensure the in-flight counter is decremented on every exit path.
  struct RecoveryGuard {
    ClusterFailoverManager* mgr;
    uint64_t id;
    RecoveryGuard(ClusterFailoverManager* m, uint64_t i) : mgr(m), id(i) {}
    ~RecoveryGuard() {
      std::lock_guard<std::mutex> lock(mgr->active_recovery_mutex_);
      mgr->active_recovery_count_.fetch_sub(1, std::memory_order_relaxed);
      mgr->active_recovery_ids_.erase(id);
    }
  } guard(this, action.action_id);

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
      SwitchLeaderHandler handler;
      {
        std::lock_guard<std::mutex> lock(switch_leader_mutex_);
        handler = switch_leader_handler_;
      }
      if (handler) {
        return handler(event.partition_id, event.node_id);
      }
      return Status::IOError("No recovery handler registered for leader switch");
    }
    case RecoveryStrategy::kPromoteReplica: {
      std::cerr << "[ClusterFailoverManager] Replica promotion requested for partition "
                << event.partition_id << " on node " << event.node_id << std::endl;
      PromoteReplicaHandler handler;
      {
        std::lock_guard<std::mutex> lock(promote_replica_mutex_);
        handler = promote_replica_handler_;
      }
      if (handler) {
        return handler(event.partition_id, event.node_id);
      }
      return Status::IOError("No recovery handler registered for replica promotion");
    }
    case RecoveryStrategy::kMigratePartition:
      return Status::NotSupported("Partition migration recovery not yet implemented");
    case RecoveryStrategy::kManualIntervention:
      return Status::IOError("Requires manual intervention");
    default:
      break;
  }
  
  return Status::IOError("Unhandled recovery strategy");
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

  static std::atomic<uint64_t> next_action_id{1};
  action.action_id = next_action_id.fetch_add(1, std::memory_order_relaxed);

  return action;
}

void ClusterFailoverManager::RecordFailure(const FailureEvent& event) {
  {
    std::lock_guard<std::mutex> lock(failures_mutex_);
    failures_[event.event_id] = event;
  }
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    stats_.total_failures++;
    stats_.failures_by_type[event.type]++;
  }
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

    if (config_.failure_retention_duration == std::chrono::minutes(0)) {
      failures_.erase(it);
    }
    // Note: if retention > 0, CleanupOldFailures() purges old entries.
  }
}

void ClusterFailoverManager::CleanupOldFailures() {
  std::lock_guard<std::mutex> lock(failures_mutex_);
  auto now = std::chrono::system_clock::now();
  auto threshold = config_.failure_retention_duration;

  for (auto it = failures_.begin(); it != failures_.end();) {
    if (it->second.is_recovered &&
        (now - it->second.recovered_at) > threshold) {
      it = failures_.erase(it);
    } else {
      ++it;
    }
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
