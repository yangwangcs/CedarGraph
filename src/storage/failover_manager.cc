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
  
  SelectNewLeader();
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
  
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
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
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("Node not found: " + node_id);
  }
  
  nodes_.erase(it);
  
  if (current_leader_ == node_id) {
    current_leader_.clear();
    // Trigger leader re-election
    SelectNewLeader();
  }
  
  return Status::OK();
}

// =============================================================================
// Node Queries
// =============================================================================

StatusOr<StorageNode> FailoverManager::GetLeader() const {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
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
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
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
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
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
      static size_t last_index = 0;
      last_index = (last_index + 1) % healthy_nodes.size();
      return healthy_nodes[last_index];
    }
  }
  
  return Status::NotFound("No healthy nodes available");
}

StatusOr<StorageNode> FailoverManager::GetNodeForWrite() {
  // Writes always go to leader
  return GetLeader();
}

std::vector<StorageNode> FailoverManager::GetAllNodes() const {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
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
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  if (from_node_id != current_leader_) {
    return Status::InvalidArgument("Can only failover from current leader");
  }
  
  auto to_it = nodes_.find(to_node_id);
  if (to_it == nodes_.end()) {
    return Status::NotFound("Target node not found: " + to_node_id);
  }
  
  if (to_it->second.health != governance::HealthStatus::kHealthy) {
    return Status::InvalidArgument("Target node is not healthy");
  }
  
  PerformFailover(from_node_id);
  
  return Status::OK();
}

// =============================================================================
// Callbacks
// =============================================================================

void FailoverManager::SetFailoverCallback(FailoverCallback callback) {
  failover_callback_ = std::move(callback);
}

void FailoverManager::SetNodeChangeCallback(NodeChangeCallback callback) {
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
  
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return;
  
  it->second.health = new_status;
  
  // If leader becomes unhealthy, trigger failover
  if (node_id == current_leader_ && 
      new_status == governance::HealthStatus::kUnhealthy) {
    PerformFailover(node_id);
  }
}

void FailoverManager::PerformFailover(const std::string& failed_node_id) {
  auto now = std::chrono::steady_clock::now();
  
  SelectNewLeader();
  
  if (!current_leader_.empty() && current_leader_ != failed_node_id) {
    auto it = nodes_.find(current_leader_);
    if (it != nodes_.end()) {
      it->second.last_failover = now;
      it->second.failover_count++;
    }
    
    if (failover_callback_) {
      failover_callback_(failed_node_id, current_leader_);
    }
  }
}

void FailoverManager::SelectNewLeader() {
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
    
    UpdateNodeRole(new_leader, NodeRole::kLeader);
    if (!old_leader.empty()) {
      UpdateNodeRole(old_leader, NodeRole::kFollower);
    }
  }
}

void FailoverManager::UpdateNodeRole(const std::string& node_id, NodeRole new_role) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return;
  
  NodeRole old_role = it->second.role;
  if (old_role == new_role) return;
  
  it->second.role = new_role;
  
  if (node_change_callback_) {
    node_change_callback_(node_id, old_role, new_role);
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
