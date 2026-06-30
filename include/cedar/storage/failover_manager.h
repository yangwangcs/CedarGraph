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

#ifndef CEDAR_STORAGE_FAILOVER_MANAGER_H_
#define CEDAR_STORAGE_FAILOVER_MANAGER_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/governance/health_checker.h"
#include "cedar/storage/storage_health_monitor.h"

namespace cedar {
namespace storage {

// =============================================================================
// Node Role Enumeration
// =============================================================================

enum class NodeRole {
  kLeader,
  kFollower,
  kStandby,
  kUnknown
};

// Convert NodeRole to string
std::string NodeRoleToString(NodeRole role);

// =============================================================================
// Failover Configuration
// =============================================================================

struct FailoverConfig {
  bool enable_auto_failover = true;
  std::chrono::seconds failover_delay{5};
  uint32_t max_failover_retries = 3;
  bool enable_read_from_follower = true;
  bool enable_sticky_session = true;

  bool CanFailover() const { return enable_auto_failover; }
};

// =============================================================================
// Storage Node Information
// =============================================================================

struct StorageNode {
  std::string node_id;
  std::string address;
  NodeRole role = NodeRole::kUnknown;
  governance::HealthStatus health;
  std::chrono::steady_clock::time_point last_failover;
  uint32_t failover_count = 0;

  StorageNode()
      : health(governance::HealthStatus::kUnknown),
        last_failover(std::chrono::steady_clock::time_point::min()) {}
};

// =============================================================================
// Failover Manager
// =============================================================================

class FailoverManager {
 public:
  using FailoverCallback = std::function<void(
      const std::string& old_node,
      const std::string& new_node)>;
  
  using NodeChangeCallback = std::function<void(
      const std::string& node_id,
      NodeRole old_role,
      NodeRole new_role)>;

  FailoverManager();
  ~FailoverManager();

  FailoverManager(const FailoverManager&) = delete;
  FailoverManager& operator=(const FailoverManager&) = delete;
  FailoverManager(FailoverManager&&) = delete;
  FailoverManager& operator=(FailoverManager&&) = delete;

  // ---------------------------------------------------------------------------
  // Initialization
  // ---------------------------------------------------------------------------

  Status Initialize(const FailoverConfig& config,
                    std::shared_ptr<StorageHealthMonitor> health_monitor);

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  Status Start();
  void Stop();

  // ---------------------------------------------------------------------------
  // Node Management
  // ---------------------------------------------------------------------------

  Status RegisterNode(const std::string& node_id,
                      const std::string& address,
                      NodeRole role);

  Status DeregisterNode(const std::string& node_id);

  // ---------------------------------------------------------------------------
  // Node Queries
  // ---------------------------------------------------------------------------

  StatusOr<StorageNode> GetLeader() const;
  std::vector<StorageNode> GetHealthyFollowers() const;
  StatusOr<StorageNode> GetNodeForRead();
  StatusOr<StorageNode> GetNodeForWrite();
  std::vector<StorageNode> GetAllNodes() const;

  // ---------------------------------------------------------------------------
  // Failover Operations
  // ---------------------------------------------------------------------------

  Status TriggerManualFailover(const std::string& from_node_id,
                               const std::string& to_node_id);

  // ---------------------------------------------------------------------------
  // Callbacks
  // ---------------------------------------------------------------------------

  void SetFailoverCallback(FailoverCallback callback);
  void SetNodeChangeCallback(NodeChangeCallback callback);

  // ---------------------------------------------------------------------------
  // Status
  // ---------------------------------------------------------------------------

  bool IsRunning() const { return running_.load(); }

 private:
  struct RoleChangeEvent {
    std::string node_id;
    NodeRole old_role;
    NodeRole new_role;
  };

  void OnHealthChanged(const std::string& node_id,
                       governance::HealthStatus old_status,
                       governance::HealthStatus new_status);
  void PerformFailover(const std::string& failed_node_id,
                       std::vector<RoleChangeEvent>* role_events);
  void SelectNewLeader(std::vector<RoleChangeEvent>* role_events);
  void UpdateNodeRole(const std::string& node_id,
                      NodeRole new_role,
                      std::vector<RoleChangeEvent>* role_events);
  void NotifyFailover(const std::string& old_node, const std::string& new_node);
  void NotifyNodeChanges(const std::vector<RoleChangeEvent>& role_events);
  bool CanFailover(const StorageNode& node);

  FailoverConfig config_;
  std::shared_ptr<StorageHealthMonitor> health_monitor_;
  
  mutable std::recursive_mutex nodes_mutex_;
  std::unordered_map<std::string, StorageNode> nodes_;
  std::string current_leader_;
  size_t read_round_robin_index_{0};
  
  std::atomic<bool> running_{false};
  mutable std::mutex callback_mutex_;
  FailoverCallback failover_callback_;
  NodeChangeCallback node_change_callback_;
};

}  // namespace storage
}  // namespace cedar

#endif  // CEDAR_STORAGE_FAILOVER_MANAGER_H_
