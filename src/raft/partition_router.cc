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

#include "cedar/raft/partition_router.h"

namespace cedar {
namespace raft {

PartitionRouter::PartitionRouter() = default;

PartitionRouter::~PartitionRouter() {
  Stop();
}

Status PartitionRouter::Initialize(
    const PartitionRouterConfig& config,
    std::shared_ptr<storage::StorageHealthMonitor> health_monitor) {
  
  config_ = config;
  health_monitor_ = health_monitor;
  
  // Initialize PartitionRaftManager
  raft_manager_ = std::make_unique<PartitionRaftManager>();
  
  PartitionRaftManagerConfig raft_config;
  raft_config.default_replica_count = config.default_replica_count;
  raft_config.placement_strategy = config.placement_strategy;
  raft_config.enable_auto_rebalance = true;
  
  Status s = raft_manager_->Initialize(raft_config, health_monitor);
  if (!s.ok()) {
    return s;
  }
  
  // Set up leader change callback
  raft_manager_->SetPartitionLeaderChangeCallback(
    [this](PartitionID part_id, const std::string& old_leader,
           const std::string& new_leader) {
      OnPartitionLeaderChanged(part_id, old_leader, new_leader);
    });
  
  return Status::OK();
}

Status PartitionRouter::Start() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Already running");
  }
  
  if (raft_manager_) {
    return raft_manager_->Start();
  }
  
  return Status::OK();
}

void PartitionRouter::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  if (raft_manager_) {
    raft_manager_->Stop();
  }
}

Status PartitionRouter::RegisterNode(const std::string& node_id,
                                      const std::string& address,
                                      uint16_t port,
                                      const std::string& dc_id) {
  {
    std::lock_guard<std::mutex> lock(address_mutex_);
    node_addresses_[node_id] = {address, port};
    if (!dc_id.empty()) {
      node_dc_ids_[node_id] = dc_id;
    }
  }
  
  if (raft_manager_) {
    return raft_manager_->RegisterNode(node_id, address, port);
  }
  
  return Status::OK();
}

Status PartitionRouter::DeregisterNode(const std::string& node_id) {
  {
    std::lock_guard<std::mutex> lock(address_mutex_);
    node_addresses_.erase(node_id);
    node_dc_ids_.erase(node_id);
  }
  
  if (raft_manager_) {
    return raft_manager_->DeregisterNode(node_id);
  }
  
  return Status::OK();
}

StatusOr<RoutingTarget> PartitionRouter::RouteWrite(uint64_t entity_id) {
  return RouteWriteByPartition(PartitionRaftManager::ComputePartitionId(entity_id));
}

StatusOr<RoutingTarget> PartitionRouter::RouteWrite(const std::string& key) {
  if (!raft_manager_) {
    return Status::InvalidArgument("Router not initialized");
  }
  return RouteWriteByPartition(raft_manager_->GetPartitionIdForKey(key));
}

StatusOr<RoutingTarget> PartitionRouter::RouteRead(uint64_t entity_id,
                                                    bool require_leader) {
  return RouteReadByPartition(
      PartitionRaftManager::ComputePartitionId(entity_id), require_leader);
}

StatusOr<RoutingTarget> PartitionRouter::RouteRead(const std::string& key,
                                                    bool require_leader) {
  if (!raft_manager_) {
    return Status::InvalidArgument("Router not initialized");
  }
  return RouteReadByPartition(
      raft_manager_->GetPartitionIdForKey(key), require_leader);
}

StatusOr<RoutingTarget> PartitionRouter::RouteWriteByPartition(
    PartitionID part_id) {
  if (!raft_manager_) {
    return Status::InvalidArgument("Router not initialized");
  }
  
  auto result = raft_manager_->RouteWriteByPartition(part_id);
  if (!result.ok()) {
    return result.status();
  }
  
  return BuildRoutingTarget(result.ValueOrDie(), part_id, true);
}

StatusOr<RoutingTarget> PartitionRouter::RouteReadByPartition(
    PartitionID part_id,
    bool require_leader) {
  if (!raft_manager_) {
    return Status::InvalidArgument("Router not initialized");
  }
  
  auto result = raft_manager_->RouteReadByPartition(part_id, require_leader);
  if (!result.ok()) {
    return result.status();
  }
  
  return BuildRoutingTarget(result.ValueOrDie(), part_id, 
                            require_leader);  // is_leader only if require_leader
}

std::vector<BatchRouteResult> PartitionRouter::RouteBatchWrite(
    const std::vector<std::string>& keys) {
  std::vector<BatchRouteResult> results;
  
  if (!raft_manager_ || keys.empty()) {
    return results;
  }
  
  auto batches = raft_manager_->RouteBatchWrite(keys);
  
  for (auto& batch : batches) {
    auto target = BuildRoutingTarget(batch.target_node, batch.part_id, true);
    if (!target.node_id.empty()) {
      BatchRouteResult result;
      result.partition_id = batch.part_id;
      result.target = target;
      result.keys = std::move(batch.keys);
      results.push_back(std::move(result));
    }
  }
  
  return results;
}

std::vector<BatchRouteResult> PartitionRouter::RouteBatchRead(
    const std::vector<std::string>& keys,
    bool require_leader) {
  std::vector<BatchRouteResult> results;
  
  if (!raft_manager_ || keys.empty()) {
    return results;
  }
  
  auto batches = raft_manager_->RouteBatchRead(keys, require_leader);
  
  for (auto& batch : batches) {
    auto target = BuildRoutingTarget(batch.target_node, batch.part_id, 
                                     require_leader);
    if (!target.node_id.empty()) {
      BatchRouteResult result;
      result.partition_id = batch.part_id;
      result.target = target;
      result.keys = std::move(batch.keys);
      results.push_back(std::move(result));
    }
  }
  
  return results;
}

Status PartitionRouter::CreatePartition(
    PartitionID part_id,
    const std::vector<std::string>& replica_nodes) {
  if (!raft_manager_) {
    return Status::InvalidArgument("Router not initialized");
  }
  
  return raft_manager_->CreatePartitionGroup(part_id, replica_nodes);
}

Status PartitionRouter::RemovePartition(PartitionID part_id) {
  if (!raft_manager_) {
    return Status::InvalidArgument("Router not initialized");
  }
  
  return raft_manager_->RemovePartitionGroup(part_id);
}

StatusOr<PartitionInfo> PartitionRouter::GetPartitionInfo(PartitionID part_id) {
  if (!raft_manager_) {
    return Status::InvalidArgument("Router not initialized");
  }
  
  auto group = raft_manager_->GetPartitionGroup(part_id);
  if (!group) {
    return Status::NotFound("Partition not found");
  }
  
  PartitionInfo info;
  info.part_id = part_id;
  
  auto leader = group->GetLeader();
  if (leader.ok()) {
    info.leader_node = leader.ValueOrDie();
  }
  
  auto replicas = group->GetReplicas();
  for (const auto& replica : replicas) {
    info.replica_nodes.push_back(replica.node_id);
    if (replica.is_healthy) {
      info.is_healthy = true;
    }
  }
  
  return info;
}

std::vector<PartitionInfo> PartitionRouter::GetAllPartitions() const {
  if (!raft_manager_) {
    return {};
  }
  
  return raft_manager_->GetAllPartitions();
}

PartitionRouter::Stats PartitionRouter::GetStats() const {
  Stats stats;
  
  if (!raft_manager_) {
    return stats;
  }
  
  auto raft_stats = raft_manager_->GetStats();
  stats.total_partitions = raft_stats.total_partitions;
  stats.healthy_partitions = raft_stats.healthy_partitions;
  stats.active_partitions = raft_stats.active_partitions;
  stats.total_nodes = raft_stats.total_nodes;
  stats.healthy_nodes = raft_stats.healthy_nodes;
  
  return stats;
}

void PartitionRouter::SetRouteChangeCallback(RouteChangeCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  route_change_callback_ = callback;
}

void PartitionRouter::SetPartitionChangeCallback(
    PartitionChangeCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  partition_change_callback_ = callback;
}

StatusOr<RoutingTarget> PartitionRouter::GetLeader() const {
  // For backward compatibility, use partition 0
  auto partitions = GetAllPartitions();
  for (const auto& info : partitions) {
    if (!info.leader_node.empty()) {
      return BuildRoutingTarget(info.leader_node, info.part_id, true);
    }
  }
  return Status::NotFound("No leader found");
}

std::vector<RoutingTarget> PartitionRouter::GetHealthyFollowers() const {
  std::vector<RoutingTarget> followers;
  
  if (!raft_manager_) {
    return followers;
  }
  
  auto partitions = raft_manager_->GetAllPartitions();
  for (const auto& info : partitions) {
    if (!info.is_healthy) continue;
    
    // Get replicas for this partition
    auto group = raft_manager_->GetPartitionGroup(info.part_id);
    if (!group) continue;
    
    auto replicas = group->GetReplicas();
    for (const auto& replica : replicas) {
      if (replica.role != RaftRole::kLeader && replica.is_healthy) {
        auto target = BuildRoutingTarget(replica.node_id, info.part_id, false);
        if (!target.node_id.empty()) {
          followers.push_back(target);
        }
      }
    }
  }
  
  return followers;
}

StatusOr<RoutingTarget> PartitionRouter::GetNodeForRead() {
  // For backward compatibility, pick any healthy partition
  auto partitions = GetAllPartitions();
  for (const auto& info : partitions) {
    if (info.is_healthy) {
      return RouteReadByPartition(info.part_id, false);
    }
  }
  
  return Status::NotFound("No healthy node available");
}

StatusOr<RoutingTarget> PartitionRouter::GetNodeForWrite() {
  // For backward compatibility, pick any healthy partition with leader
  auto partitions = GetAllPartitions();
  for (const auto& info : partitions) {
    if (info.is_healthy && !info.leader_node.empty()) {
      return RouteWriteByPartition(info.part_id);
    }
  }
  
  return Status::NotFound("No healthy leader available");
}

void PartitionRouter::OnPartitionLeaderChanged(PartitionID part_id,
                                                const std::string& old_leader,
                                                const std::string& new_leader) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  if (route_change_callback_) {
    route_change_callback_(part_id, old_leader, new_leader);
  }
}

RoutingTarget PartitionRouter::BuildRoutingTarget(const std::string& node_id,
                                                   PartitionID part_id,
                                                   bool is_leader) const {
  RoutingTarget target;
  target.node_id = node_id;
  target.partition_id = part_id;
  target.is_leader = is_leader;
  
  {
    std::lock_guard<std::mutex> lock(address_mutex_);
    auto it = node_addresses_.find(node_id);
    if (it != node_addresses_.end()) {
      target.address = it->second.first;
      target.port = it->second.second;
    }
    
    auto dc_it = node_dc_ids_.find(node_id);
    if (dc_it != node_dc_ids_.end()) {
      target.dc_id = dc_it->second;
    }
  }
  
  return target;
}

}  // namespace raft
}  // namespace cedar
