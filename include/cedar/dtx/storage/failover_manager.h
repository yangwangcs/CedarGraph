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
// Automatic Failover Manager
// =============================================================================
// Monitors partition health and triggers automatic leader election on failure

#ifndef CEDAR_DTX_STORAGE_FAILOVER_MANAGER_H_
#define CEDAR_DTX_STORAGE_FAILOVER_MANAGER_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// Forward Declarations
// =============================================================================

class StorageRaftGroup;
class RaftStorageManager;

// =============================================================================
// Health Check Types
// =============================================================================

enum class NodeHealth : uint8_t {
  kHealthy = 0,
  kSuspected = 1,
  kUnhealthy = 2,
  kUnknown = 3,
};

enum class FailoverAction : uint8_t {
  kNone = 0,
  kStartElection = 1,
  kTransferLeadership = 2,
  kAddReplica = 3,
  kRemoveReplica = 4,
};

struct HealthCheckResult {
  NodeID node_id;
  PartitionID partition_id;
  NodeHealth health;
  std::chrono::steady_clock::time_point last_seen;
  uint64_t missed_heartbeats = 0;
  std::string reason;
};

// =============================================================================
// Failover Configuration
// =============================================================================

struct FailoverConfig {
  // Health check intervals
  std::chrono::milliseconds health_check_interval{5000};      // 5 seconds
  std::chrono::milliseconds leader_check_interval{3000};      // 3 seconds
  
  // Failure detection thresholds
  uint32_t max_missed_heartbeats = 3;
  std::chrono::milliseconds suspected_timeout{10000};         // 10 seconds
  std::chrono::milliseconds unhealthy_timeout{30000};         // 30 seconds
  
  // Failover settings
  bool auto_failover_enabled = true;
  std::chrono::milliseconds failover_cooldown{60000};         // 1 minute
  uint32_t max_failovers_per_hour = 10;
  
  // Leader transfer settings
  bool prefer_local_replica = true;
  uint32_t min_replicas_for_failover = 2;  // Minimum replicas to allow failover
};

// =============================================================================
// Partition Health Monitor
// =============================================================================

class PartitionHealthMonitor {
 public:
  explicit PartitionHealthMonitor(PartitionID pid);
  ~PartitionHealthMonitor();
  
  // Record heartbeat from node
  void RecordHeartbeat(NodeID node_id, bool is_leader);
  
  // Get current health
  NodeHealth GetNodeHealth(NodeID node_id) const;
  NodeHealth GetPartitionHealth() const;
  
  // Check if leader is healthy
  bool IsLeaderHealthy() const;
  NodeID GetCurrentLeader() const { return current_leader_.load(); }
  
  // Get nodes that can be promoted to leader
  std::vector<NodeID> GetCandidateNodes() const;
  
  // Get health check results for all nodes
  std::vector<HealthCheckResult> GetHealthResults() const;
  
  // Configuration
  void SetConfig(const FailoverConfig& config) { config_ = config; }

 private:
  PartitionID partition_id_;
  FailoverConfig config_;
  
  struct NodeState {
    NodeID node_id;
    NodeHealth health = NodeHealth::kUnknown;
    std::chrono::steady_clock::time_point last_heartbeat;
    uint64_t missed_count = 0;
    bool is_leader = false;
  };
  
  mutable std::shared_mutex nodes_mutex_;
  std::unordered_map<NodeID, NodeState> nodes_;
  std::atomic<NodeID> current_leader_{0};
  
  void UpdateHealthLocked(NodeState& state);
};

// =============================================================================
// Failover Manager
// =============================================================================

class FailoverManager {
 public:
  using FailoverCallback = std::function<Status(PartitionID, NodeID, FailoverAction)>;
  using AlertCallback = std::function<void(const std::string& alert_type, 
                                            const std::string& message)>;
  
  FailoverManager();
  ~FailoverManager();
  
  // Lifecycle
  Status Initialize(const FailoverConfig& config);
  void Shutdown();
  bool IsRunning() const { return running_.load(); }
  
  // Register partition for monitoring
  Status RegisterPartition(PartitionID pid, 
                           RaftStorageManager* raft_manager);
  void UnregisterPartition(PartitionID pid);
  
  // Manual operations
  Status TriggerManualFailover(PartitionID pid, NodeID preferred_leader);
  Status TransferLeadership(PartitionID pid, NodeID new_leader);
  
  // Set callbacks
  void SetFailoverCallback(FailoverCallback cb) { failover_callback_ = std::move(cb); }
  void SetAlertCallback(AlertCallback cb) { alert_callback_ = std::move(cb); }
  
  // Statistics
  struct Stats {
    uint64_t total_failovers = 0;
    uint64_t successful_failovers = 0;
    uint64_t failed_failovers = 0;
    uint64_t manual_failovers = 0;
    std::chrono::system_clock::time_point last_failover_time;
  };
  Stats GetStats() const;
  
  // Get partition health
  NodeHealth GetPartitionHealth(PartitionID pid) const;
  std::vector<HealthCheckResult> GetPartitionHealthDetails(PartitionID pid) const;

 private:
  void MonitorLoop();
  void CheckPartitionHealth(PartitionID pid);
  FailoverAction DecideFailoverAction(const PartitionHealthMonitor& monitor) const;
  bool CanPerformFailover(PartitionID pid) const;
  void RecordFailoverAttempt(PartitionID pid, bool success);
  void SendAlert(const std::string& type, const std::string& message);
  
  std::atomic<bool> running_{false};
  FailoverConfig config_;
  
  mutable std::shared_mutex monitors_mutex_;
  std::unordered_map<PartitionID, std::unique_ptr<PartitionHealthMonitor>> monitors_;
  std::unordered_map<PartitionID, RaftStorageManager*> raft_managers_;
  
  std::unique_ptr<std::thread> monitor_thread_;
  
  // Failover history
  mutable std::mutex history_mutex_;
  std::unordered_map<PartitionID, std::vector<std::chrono::system_clock::time_point>> failover_history_;
  Stats stats_;
  
  // Callbacks
  FailoverCallback failover_callback_;
  AlertCallback alert_callback_;
};

// =============================================================================
// Leader Election Controller
// =============================================================================

class LeaderElectionController {
 public:
  LeaderElectionController();
  ~LeaderElectionController();
  
  Status Initialize(StorageRaftGroup* raft_group);
  
  // Request vote from peers
  Status RequestVote();
  
  // Handle vote response
  void RecordVote(NodeID voter, bool granted);
  
  // Check if we have majority
  bool HasMajority() const;
  
  // Reset election state
  void Reset();
  
  // Get current vote count
  uint32_t GetVoteCount() const { return vote_count_.load(); }
  uint32_t GetNeededVotes() const { return needed_votes_.load(); }

 private:
  StorageRaftGroup* raft_group_ = nullptr;
  std::atomic<uint32_t> vote_count_{0};
  std::atomic<uint32_t> needed_votes_{0};
  std::atomic<bool> vote_granted_to_self_{false};
  
  mutable std::mutex voted_mutex_;
  std::unordered_set<NodeID> voted_nodes_;
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_FAILOVER_MANAGER_H_
