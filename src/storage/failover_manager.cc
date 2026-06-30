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

#include "cedar/storage/failover_manager.h"

#include <algorithm>
#include <iostream>

namespace cedar {
namespace storage {

// =============================================================================
// Utility Functions
// =============================================================================

std::string NodeRoleToString(NodeRole role) {
  switch (role) {
    case NodeRole::kLeader:
      return "Leader";
    case NodeRole::kFollower:
      return "Follower";
    case NodeRole::kStandby:
      return "Standby";
    case NodeRole::kUnknown:
      return "Unknown";
    default:
      return "Invalid";
  }
}

// =============================================================================
// Constructor & Destructor
// =============================================================================

FailoverManager::FailoverManager() = default;

FailoverManager::~FailoverManager() {
  Stop();
}

// =============================================================================
// Initialization
// =============================================================================

Status FailoverManager::Initialize(
    const FailoverConfig& config,
    std::shared_ptr<StorageHealthMonitor> health_monitor) {
  
  if (!health_monitor) {
    return Status::InvalidArgument("HealthMonitor cannot be null");
  }
  
  config_ = config;
  health_monitor_ = health_monitor;
  
  health_monitor_->SetHealthChangeCallback(
    [this](const std::string& node_id,
           governance::HealthStatus old_status,
           governance::HealthStatus new_status) {
      OnHealthChanged(node_id, old_status, new_status);
    });
  
  return Status::OK();
}

// =============================================================================
// Lifecycle
// =============================================================================

Status FailoverManager::Start() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Already running");
  }
  
  std::vector<RoleChangeEvent> role_events;
  {
    std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);
    SelectNewLeader(&role_events);
  }
  NotifyNodeChanges(role_events);
  return Status::OK();
}

void FailoverManager::Stop() {
  running_ = false;
}

// =============================================================================
// Node Management
// =============================================================================

Status FailoverManager::RegisterNode(const std::string& node_id,
                                     const std::string& address,
                                     NodeRole role) {
  if (node_id.empty()) {
    return Status::InvalidArgument("Node ID cannot be empty");
  }
  
  if (address.empty()) {
    return Status::InvalidArgument("Address cannot be empty");
  }
  
  std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);
  
  StorageNode node;
  node.node_id = node_id;
  node.address = address;
  node.role = role;
  
  auto health = health_monitor_->GetNodeHealth(node_id);
  if (health.ok()) {
    node.health = health.ValueOrDie().status;
  } else {
    node.health = governance::HealthStatus::kUnknown;
  }
  
  nodes_[node_id] = node;
  
  if (role == NodeRole::kLeader && current_leader_.empty()) {
    current_leader_ = node_id;
  }
  
  return Status::OK();
}

Status FailoverManager::DeregisterNode(const std::string& node_id) {
  std::vector<RoleChangeEvent> role_events;
  {
    std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);

    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
      return Status::NotFound("Node not found: " + node_id);
    }

    nodes_.erase(it);

    if (current_leader_ == node_id) {
      current_leader_.clear();
      // Trigger leader re-election
      SelectNewLeader(&role_events);
    }
  }

  NotifyNodeChanges(role_events);
  return Status::OK();
}

// =============================================================================
// Node Queries
// =============================================================================

StatusOr<StorageNode> FailoverManager::GetLeader() const {
  std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);
  
  if (current_leader_.empty()) {
    return Status::NotFound("No leader elected");
  }
  
  auto it = nodes_.find(current_leader_);
  if (it == nodes_.end()) {
    return Status::NotFound("Leader node not found: " + current_leader_);
  }
  
  return it->second;
}

std::vector<StorageNode> FailoverManager::GetHealthyFollowers() const {
  std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);
  
  std::vector<StorageNode> followers;
  for (const auto& [id, node] : nodes_) {
    if (node.role == NodeRole::kFollower && 
        node.health == governance::HealthStatus::kHealthy) {
      followers.push_back(node);
    }
  }
  
  return followers;
}

StatusOr<StorageNode> FailoverManager::GetNodeForRead() {
  std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);
  
  // If failover is disabled, only use leader
  if (!config_.CanFailover()) {
    if (!current_leader_.empty()) {
      auto it = nodes_.find(current_leader_);
      if (it != nodes_.end() && it->second.health == governance::HealthStatus::kHealthy) {
        return it->second;
      }
    }
    return Status::NotFound("No healthy leader available");
  }
  
  // If read from follower is disabled, only use leader
  if (!config_.enable_read_from_follower && !current_leader_.empty()) {
    auto it = nodes_.find(current_leader_);
    if (it != nodes_.end() && it->second.health == governance::HealthStatus::kHealthy) {
      return it->second;
    }
  }
  
  // Use round-robin among all healthy nodes
  if (config_.enable_read_from_follower) {
    std::vector<StorageNode> healthy_nodes;
    for (const auto& [id, node] : nodes_) {
      if (node.health == governance::HealthStatus::kHealthy) {
        healthy_nodes.push_back(node);
      }
    }
    
    if (!healthy_nodes.empty()) {
      read_round_robin_index_ = (read_round_robin_index_ + 1) % healthy_nodes.size();
      return healthy_nodes[read_round_robin_index_];
    }
  }
  
  return Status::NotFound("No healthy nodes available");
}

StatusOr<StorageNode> FailoverManager::GetNodeForWrite() {
  // Writes always go to leader
  return GetLeader();
}

std::vector<StorageNode> FailoverManager::GetAllNodes() const {
  std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);
  
  std::vector<StorageNode> result;
  result.reserve(nodes_.size());
  for (const auto& [id, node] : nodes_) {
    result.push_back(node);
  }
  
  return result;
}

// =============================================================================
// Failover Operations
// =============================================================================

Status FailoverManager::TriggerManualFailover(const std::string& from_node_id,
                                              const std::string& to_node_id) {
  std::vector<RoleChangeEvent> role_events;
  {
    std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);

    if (from_node_id != current_leader_) {
      return Status::InvalidArgument("Can only failover from current leader");
    }
    if (from_node_id == to_node_id) {
      return Status::InvalidArgument("Manual failover target must differ from source");
    }

    auto from_it = nodes_.find(from_node_id);
    if (from_it == nodes_.end()) {
      return Status::NotFound("Source node not found: " + from_node_id);
    }

    auto to_it = nodes_.find(to_node_id);
    if (to_it == nodes_.end()) {
      return Status::NotFound("Target node not found: " + to_node_id);
    }

    if (to_it->second.health != governance::HealthStatus::kHealthy) {
      return Status::InvalidArgument("Target node is not healthy");
    }

    current_leader_ = to_node_id;
    UpdateNodeRole(to_node_id, NodeRole::kLeader, &role_events);
    UpdateNodeRole(from_node_id, NodeRole::kFollower, &role_events);

    auto now = std::chrono::steady_clock::now();
    to_it = nodes_.find(to_node_id);
    if (to_it != nodes_.end()) {
      to_it->second.last_failover = now;
      to_it->second.failover_count++;
    }
  }

  NotifyNodeChanges(role_events);
  NotifyFailover(from_node_id, to_node_id);
  return Status::OK();
}

// =============================================================================
// Callbacks
// =============================================================================

void FailoverManager::SetFailoverCallback(FailoverCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  failover_callback_ = std::move(callback);
}

void FailoverManager::SetNodeChangeCallback(NodeChangeCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  node_change_callback_ = std::move(callback);
}

// =============================================================================
// Private Methods
// =============================================================================

void FailoverManager::OnHealthChanged(const std::string& node_id,
                                      governance::HealthStatus old_status,
                                      governance::HealthStatus new_status) {
  if (!running_ || !config_.enable_auto_failover) {
    return;
  }
  
  std::string new_leader;
  std::vector<RoleChangeEvent> role_events;
  {
    std::lock_guard<std::recursive_mutex> lock(nodes_mutex_);

    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return;

    it->second.health = new_status;

    // If leader becomes unhealthy, trigger failover
    if (node_id == current_leader_ &&
        new_status == governance::HealthStatus::kUnhealthy) {
      if (!CanFailover(it->second)) {
        return;
      }
      std::string old_leader = current_leader_;
      PerformFailover(node_id, &role_events);
      if (current_leader_ != old_leader) {
        new_leader = current_leader_;
      }
    }
  }

  NotifyNodeChanges(role_events);
  if (!new_leader.empty()) {
    NotifyFailover(node_id, new_leader);
  }
}

void FailoverManager::PerformFailover(
    const std::string& failed_node_id,
    std::vector<RoleChangeEvent>* role_events) {
  auto now = std::chrono::steady_clock::now();
  
  SelectNewLeader(role_events);
  
  if (!current_leader_.empty() && current_leader_ != failed_node_id) {
    auto it = nodes_.find(current_leader_);
    if (it != nodes_.end()) {
      it->second.last_failover = now;
      it->second.failover_count++;
    }
  }
}

void FailoverManager::SelectNewLeader(std::vector<RoleChangeEvent>* role_events) {
  std::string new_leader;
  
  for (const auto& [id, node] : nodes_) {
    if (node.health == governance::HealthStatus::kHealthy) {
      if (new_leader.empty() || node.role == NodeRole::kLeader) {
        new_leader = id;
      }
    }
  }
  
  if (!new_leader.empty() && new_leader != current_leader_) {
    std::string old_leader = current_leader_;
    current_leader_ = new_leader;
    
    UpdateNodeRole(new_leader, NodeRole::kLeader, role_events);
    if (!old_leader.empty()) {
      UpdateNodeRole(old_leader, NodeRole::kFollower, role_events);
    }
  }
}

void FailoverManager::UpdateNodeRole(
    const std::string& node_id,
    NodeRole new_role,
    std::vector<RoleChangeEvent>* role_events) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return;
  
  NodeRole old_role = it->second.role;
  if (old_role == new_role) return;
  
  it->second.role = new_role;
  
  if (role_events) {
    role_events->push_back({node_id, old_role, new_role});
  }
}

void FailoverManager::NotifyFailover(const std::string& old_node,
                                     const std::string& new_node) {
  FailoverCallback callback;
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback = failover_callback_;
  }
  if (callback) {
    try {
      callback(old_node, new_node);
    } catch (const std::exception& e) {
      std::cerr << "Failover callback exception: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "Failover callback unknown exception" << std::endl;
    }
  }
}

void FailoverManager::NotifyNodeChanges(
    const std::vector<RoleChangeEvent>& role_events) {
  NodeChangeCallback callback;
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback = node_change_callback_;
  }
  if (!callback) {
    return;
  }

  for (const auto& event : role_events) {
    try {
      callback(event.node_id, event.old_role, event.new_role);
    } catch (const std::exception& e) {
      std::cerr << "Node change callback exception: " << e.what()
                << std::endl;
    } catch (...) {
      std::cerr << "Node change callback unknown exception" << std::endl;
    }
  }
}

bool FailoverManager::CanFailover(const StorageNode& node) {
  // Check if failover delay has passed since last failover
  auto now = std::chrono::steady_clock::now();
  if (node.last_failover != std::chrono::steady_clock::time_point::min()) {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        now - node.last_failover);
    if (elapsed < config_.failover_delay) {
      return false;
    }
  }
  
  // Check max retry count
  if (node.failover_count >= config_.max_failover_retries) {
    return false;
  }
  
  return true;
}

}  // namespace storage
}  // namespace cedar
