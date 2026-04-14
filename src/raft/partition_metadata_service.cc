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

#include "cedar/raft/partition_metadata_service.h"

#include <sstream>
#include <algorithm>
#include <random>

namespace cedar {
namespace raft {

// =============================================================================
// PartitionMetadata 序列化
// =============================================================================

std::string PartitionMetadata::Serialize() const {
  std::ostringstream oss;
  oss << static_cast<int>(part_id) << "|"
      << leader_node << "|"
      << replica_nodes.size() << "|";
  for (const auto& node : replica_nodes) {
    oss << node << "|";
  }
  oss << space_name << "|"
      << static_cast<int>(static_cast<uint8_t>(state)) << "|"
      << key_count << "|"
      << data_size << "|"
      << version << "|"
      << created_at.time_since_epoch().count() << "|"
      << updated_at.time_since_epoch().count();
  return oss.str();
}

StatusOr<PartitionMetadata> PartitionMetadata::Deserialize(
    const std::string& data) {
  PartitionMetadata metadata;
  std::istringstream iss(data);
  std::string token;
  
  std::getline(iss, token, '|');
  metadata.part_id = static_cast<PartitionID>(std::stoul(token));
  
  std::getline(iss, metadata.leader_node, '|');
  
  std::getline(iss, token, '|');
  size_t replica_count = std::stoul(token);
  
  for (size_t i = 0; i < replica_count; i++) {
    std::getline(iss, token, '|');
    metadata.replica_nodes.push_back(token);
  }
  
  std::getline(iss, metadata.space_name, '|');
  
  std::getline(iss, token, '|');
  metadata.state = static_cast<PartitionMetadata::State>(std::stoul(token));
  
  std::getline(iss, token, '|');
  metadata.key_count = std::stoul(token);
  
  std::getline(iss, token, '|');
  metadata.data_size = std::stoul(token);
  
  std::getline(iss, token, '|');
  metadata.version = std::stoul(token);
  
  std::getline(iss, token, '|');
  auto created_ticks = std::chrono::system_clock::time_point(
      std::chrono::system_clock::duration(std::stoll(token)));
  metadata.created_at = created_ticks;
  
  std::getline(iss, token, '|');
  auto updated_ticks = std::chrono::system_clock::time_point(
      std::chrono::system_clock::duration(std::stoll(token)));
  metadata.updated_at = updated_ticks;
  
  return metadata;
}

// =============================================================================
// StorageNodeMetadata 序列化
// =============================================================================

std::string StorageNodeMetadata::Serialize() const {
  std::ostringstream oss;
  oss << node_id << "|"
      << address << "|"
      << static_cast<int>(port) << "|"
      << dc_id << "|"
      << static_cast<int>(static_cast<uint8_t>(state)) << "|"
      << static_cast<int>(num_partitions) << "|"
      << total_disk_bytes << "|"
      << used_disk_bytes << "|"
      << cpu_usage_percent << "|"
      << memory_usage_percent << "|"
      << registered_at.time_since_epoch().count() << "|"
      << last_heartbeat.time_since_epoch().count();
  return oss.str();
}

StatusOr<StorageNodeMetadata> StorageNodeMetadata::Deserialize(
    const std::string& data) {
  StorageNodeMetadata metadata;
  std::istringstream iss(data);
  std::string token;
  
  std::getline(iss, metadata.node_id, '|');
  std::getline(iss, metadata.address, '|');
  
  std::getline(iss, token, '|');
  metadata.port = static_cast<uint16_t>(std::stoul(token));
  
  std::getline(iss, metadata.dc_id, '|');
  
  std::getline(iss, token, '|');
  metadata.state = static_cast<StorageNodeMetadata::State>(std::stoul(token));
  
  std::getline(iss, token, '|');
  metadata.num_partitions = static_cast<uint32_t>(std::stoul(token));
  
  std::getline(iss, token, '|');
  metadata.total_disk_bytes = std::stoull(token);
  
  std::getline(iss, token, '|');
  metadata.used_disk_bytes = std::stoull(token);
  
  std::getline(iss, token, '|');
  metadata.cpu_usage_percent = std::stod(token);
  
  std::getline(iss, token, '|');
  metadata.memory_usage_percent = std::stod(token);
  
  std::getline(iss, token, '|');
  auto registered_ticks = std::chrono::system_clock::time_point(
      std::chrono::system_clock::duration(std::stoll(token)));
  metadata.registered_at = registered_ticks;
  
  std::getline(iss, token, '|');
  auto heartbeat_ticks = std::chrono::system_clock::time_point(
      std::chrono::system_clock::duration(std::stoll(token)));
  metadata.last_heartbeat = heartbeat_ticks;
  
  return metadata;
}

// =============================================================================
// PartitionMetadataService 实现
// =============================================================================

PartitionMetadataService::PartitionMetadataService() = default;

PartitionMetadataService::~PartitionMetadataService() {
  Shutdown();
}

Status PartitionMetadataService::Initialize(const PartitionTopologyConfig& config) {
  if (initialized_.exchange(true)) {
    return Status::InvalidArgument("Already initialized");
  }
  
  config_ = config;
  running_ = true;
  
  // 启动心跳检查线程
  heartbeat_thread_ = std::thread(&PartitionMetadataService::HeartbeatCheckLoop, this);
  
  return Status::OK();
}

Status PartitionMetadataService::Shutdown() {
  if (!initialized_.exchange(false)) {
    return Status::OK();
  }
  
  running_ = false;
  
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
  
  return Status::OK();
}

// =============================================================================
// 节点管理
// =============================================================================

Status PartitionMetadataService::RegisterNode(const StorageNodeMetadata& node) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  if (nodes_.find(node.node_id) != nodes_.end()) {
    return Status::InvalidArgument("Node already registered: " + node.node_id);
  }
  
  StorageNodeMetadata new_node = node;
  new_node.registered_at = std::chrono::system_clock::now();
  new_node.last_heartbeat = new_node.registered_at;
  new_node.state = StorageNodeMetadata::State::kOnline;
  
  nodes_[node.node_id] = new_node;
  
  // 通知节点加入
  TopologyChange change;
  change.type = TopologyChangeType::kNodeJoined;
  change.old_node = node.node_id;
  change.new_node = node.node_id;
  change.version = 1;
  change.timestamp = std::chrono::system_clock::now();
  NotifyTopologyChange(change);
  
  return Status::OK();
}

Status PartitionMetadataService::Heartbeat(const std::string& node_id) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("Node not found: " + node_id);
  }
  
  it->second.last_heartbeat = std::chrono::system_clock::now();
  
  // 如果节点之前离线，现在恢复
  if (it->second.state == StorageNodeMetadata::State::kOffline ||
      it->second.state == StorageNodeMetadata::State::kSuspected) {
    it->second.state = StorageNodeMetadata::State::kOnline;
    
    TopologyChange change;
    change.type = TopologyChangeType::kNodeJoined;
    change.old_node = node_id;
    change.new_node = node_id;
    change.version = 1;
    change.timestamp = std::chrono::system_clock::now();
    NotifyTopologyChange(change);
  }
  
  return Status::OK();
}

StatusOr<StorageNodeMetadata> PartitionMetadataService::GetNode(
    const std::string& node_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("Node not found: " + node_id);
  }
  
  return it->second;
}

std::vector<StorageNodeMetadata> PartitionMetadataService::GetOnlineNodes() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<StorageNodeMetadata> online_nodes;
  for (const auto& [id, node] : nodes_) {
    if (node.IsOnline()) {
      online_nodes.push_back(node);
    }
  }
  
  return online_nodes;
}

std::vector<StorageNodeMetadata> PartitionMetadataService::GetAllNodes() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<StorageNodeMetadata> all_nodes;
  for (const auto& [id, node] : nodes_) {
    all_nodes.push_back(node);
  }
  
  return all_nodes;
}

// =============================================================================
// 分区拓扑管理
// =============================================================================

Status PartitionMetadataService::CreateSpace(const std::string& space_name,
                                              uint32_t partition_count,
                                              uint32_t replica_count) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  if (space_partitions_.find(space_name) != space_partitions_.end()) {
    return Status::InvalidArgument("Space already exists: " + space_name);
  }
  
  uint32_t num_partitions = partition_count > 0 ? partition_count : config_.default_partition_count;
  uint32_t num_replicas = replica_count > 0 ? replica_count : config_.default_replica_count;
  
  // 获取在线节点
  std::vector<std::string> online_nodes;
  for (const auto& [id, node] : nodes_) {
    if (node.IsOnline()) {
      online_nodes.push_back(id);
    }
  }
  
  if (online_nodes.size() < num_replicas) {
    return Status::InvalidArgument("Not enough online nodes for replica factor");
  }
  
  // 创建分区
  auto now = std::chrono::system_clock::now();
  for (uint32_t i = 0; i < num_partitions; i++) {
    PartitionMetadata metadata;
    metadata.part_id = i;
    metadata.space_name = space_name;
    metadata.state = PartitionMetadata::State::kCreating;
    metadata.version = 1;
    metadata.created_at = now;
    metadata.updated_at = now;
    
    // 选择副本节点（一致性哈希）
    metadata.replica_nodes = SelectReplicaNodes(i, num_replicas);
    if (metadata.replica_nodes.empty()) {
      return Status::InvalidArgument("Failed to select replica nodes for partition " + 
                                      std::to_string(i));
    }
    
    // 选择 Leader
    metadata.leader_node = SelectBestLeader(metadata.replica_nodes);
    metadata.state = PartitionMetadata::State::kNormal;
    
    space_partitions_[space_name][i] = metadata;
  }
  
  return Status::OK();
}

Status PartitionMetadataService::DropSpace(const std::string& space_name) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  auto it = space_partitions_.find(space_name);
  if (it == space_partitions_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  space_partitions_.erase(it);
  return Status::OK();
}

StatusOr<PartitionMetadata> PartitionMetadataService::GetPartitionMetadata(
    const std::string& space_name,
    PartitionID part_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto space_it = space_partitions_.find(space_name);
  if (space_it == space_partitions_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  auto part_it = space_it->second.find(part_id);
  if (part_it == space_it->second.end()) {
    return Status::NotFound("Partition not found");
  }
  
  return part_it->second;
}

std::vector<PartitionMetadata> PartitionMetadataService::GetSpacePartitions(
    const std::string& space_name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<PartitionMetadata> partitions;
  
  auto space_it = space_partitions_.find(space_name);
  if (space_it == space_partitions_.end()) {
    return partitions;
  }
  
  for (const auto& [id, metadata] : space_it->second) {
    partitions.push_back(metadata);
  }
  
  return partitions;
}

std::vector<PartitionMetadata> PartitionMetadataService::GetNodePartitions(
    const std::string& node_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<PartitionMetadata> partitions;
  
  for (const auto& [space_name, space_parts] : space_partitions_) {
    for (const auto& [part_id, metadata] : space_parts) {
      if (metadata.IsReplicaOn(node_id)) {
        partitions.push_back(metadata);
      }
    }
  }
  
  return partitions;
}

// =============================================================================
// 拓扑变更
// =============================================================================

Status PartitionMetadataService::UpdatePartitionLeader(const std::string& space_name,
                                                        PartitionID part_id,
                                                        const std::string& new_leader) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  auto space_it = space_partitions_.find(space_name);
  if (space_it == space_partitions_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  auto part_it = space_it->second.find(part_id);
  if (part_it == space_it->second.end()) {
    return Status::NotFound("Partition not found");
  }
  
  auto& metadata = part_it->second;
  std::string old_leader = metadata.leader_node;
  
  if (old_leader == new_leader) {
    return Status::OK();  // 无变化
  }
  
  metadata.leader_node = new_leader;
  metadata.version++;
  metadata.updated_at = std::chrono::system_clock::now();
  
  // 通知 Leader 变更
  TopologyChange change;
  change.type = TopologyChangeType::kLeaderChanged;
  change.space_name = space_name;
  change.part_id = part_id;
  change.old_node = old_leader;
  change.new_node = new_leader;
  change.version = metadata.version;
  change.timestamp = std::chrono::system_clock::now();
  NotifyTopologyChange(change);
  
  return Status::OK();
}

Status PartitionMetadataService::MigratePartition(const std::string& space_name,
                                                   PartitionID part_id,
                                                   const std::string& from_node,
                                                   const std::string& to_node) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  auto space_it = space_partitions_.find(space_name);
  if (space_it == space_partitions_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  auto part_it = space_it->second.find(part_id);
  if (part_it == space_it->second.end()) {
    return Status::NotFound("Partition not found");
  }
  
  auto& metadata = part_it->second;
  
  // 检查 from_node 是否是副本
  auto it = std::find(metadata.replica_nodes.begin(), metadata.replica_nodes.end(), from_node);
  if (it == metadata.replica_nodes.end()) {
    return Status::InvalidArgument("Source node is not a replica");
  }
  
  // 更新副本列表
  *it = to_node;
  
  // 如果 Leader 被迁移，更新 Leader
  if (metadata.leader_node == from_node) {
    metadata.leader_node = to_node;
  }
  
  metadata.state = PartitionMetadata::State::kNormal;
  metadata.version++;
  metadata.updated_at = std::chrono::system_clock::now();
  
  // 通知迁移完成
  TopologyChange change;
  change.type = TopologyChangeType::kPartitionMigrated;
  change.space_name = space_name;
  change.part_id = part_id;
  change.old_node = from_node;
  change.new_node = to_node;
  change.version = metadata.version;
  change.timestamp = std::chrono::system_clock::now();
  NotifyTopologyChange(change);
  
  return Status::OK();
}

Status PartitionMetadataService::AddPartitionReplica(const std::string& space_name,
                                                      PartitionID part_id,
                                                      const std::string& node_id) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  auto space_it = space_partitions_.find(space_name);
  if (space_it == space_partitions_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  auto part_it = space_it->second.find(part_id);
  if (part_it == space_it->second.end()) {
    return Status::NotFound("Partition not found");
  }
  
  auto& metadata = part_it->second;
  
  // 检查是否已经是副本
  if (metadata.IsReplicaOn(node_id)) {
    return Status::InvalidArgument("Node is already a replica");
  }
  
  metadata.replica_nodes.push_back(node_id);
  metadata.version++;
  metadata.updated_at = std::chrono::system_clock::now();
  
  // 通知副本添加
  TopologyChange change;
  change.type = TopologyChangeType::kReplicaAdded;
  change.space_name = space_name;
  change.part_id = part_id;
  change.new_node = node_id;
  change.version = metadata.version;
  change.timestamp = std::chrono::system_clock::now();
  NotifyTopologyChange(change);
  
  return Status::OK();
}

Status PartitionMetadataService::RemovePartitionReplica(const std::string& space_name,
                                                         PartitionID part_id,
                                                         const std::string& node_id) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  auto space_it = space_partitions_.find(space_name);
  if (space_it == space_partitions_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  auto part_it = space_it->second.find(part_id);
  if (part_it == space_it->second.end()) {
    return Status::NotFound("Partition not found");
  }
  
  auto& metadata = part_it->second;
  
  // 不能移除 Leader
  if (metadata.leader_node == node_id) {
    return Status::InvalidArgument("Cannot remove leader replica");
  }
  
  // 从副本列表中移除
  auto it = std::find(metadata.replica_nodes.begin(), metadata.replica_nodes.end(), node_id);
  if (it == metadata.replica_nodes.end()) {
    return Status::NotFound("Node is not a replica");
  }
  
  metadata.replica_nodes.erase(it);
  metadata.version++;
  metadata.updated_at = std::chrono::system_clock::now();
  
  // 通知副本移除
  TopologyChange change;
  change.type = TopologyChangeType::kReplicaRemoved;
  change.space_name = space_name;
  change.part_id = part_id;
  change.old_node = node_id;
  change.version = metadata.version;
  change.timestamp = std::chrono::system_clock::now();
  NotifyTopologyChange(change);
  
  return Status::OK();
}

// =============================================================================
// 负载均衡
// =============================================================================

bool PartitionMetadataService::NeedsRebalance() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  // 计算每个节点的分区数
  std::unordered_map<std::string, size_t> node_partition_count;
  for (const auto& [space_name, space_parts] : space_partitions_) {
    for (const auto& [part_id, metadata] : space_parts) {
      for (const auto& node : metadata.replica_nodes) {
        node_partition_count[node]++;
      }
    }
  }
  
  if (node_partition_count.size() < 2) {
    return false;
  }
  
  // 计算平均值和最大差异
  size_t total = 0;
  size_t max_count = 0;
  size_t min_count = SIZE_MAX;
  
  for (const auto& [node, count] : node_partition_count) {
    total += count;
    max_count = std::max(max_count, count);
    min_count = std::min(min_count, count);
  }
  
  double avg = static_cast<double>(total) / node_partition_count.size();
  double diff_ratio = (max_count - min_count) / avg;
  
  return diff_ratio > config_.rebalance_threshold;
}

Status PartitionMetadataService::Rebalance() {
  // TODO: 实现重平衡逻辑
  // 1. 计算负载
  // 2. 选择需要迁移的分区
  // 3. 执行迁移
  return Status::OK();
}

// =============================================================================
// 订阅/通知
// =============================================================================

void PartitionMetadataService::WatchTopologyChanges(TopologyChangeCallback callback) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  topology_callbacks_.push_back(callback);
}

// =============================================================================
// 统计
// =============================================================================

PartitionMetadataService::Stats PartitionMetadataService::GetStats() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  Stats stats;
  stats.total_spaces = space_partitions_.size();
  
  for (const auto& [space_name, space_parts] : space_partitions_) {
    stats.total_partitions += space_parts.size();
    
    for (const auto& [part_id, metadata] : space_parts) {
      if (metadata.state == PartitionMetadata::State::kNormal) {
        stats.healthy_partitions++;
      }
      if (metadata.state == PartitionMetadata::State::kMigrating) {
        stats.migrating_partitions++;
      }
    }
  }
  
  stats.total_nodes = nodes_.size();
  for (const auto& [id, node] : nodes_) {
    if (node.IsOnline()) {
      stats.online_nodes++;
    }
  }
  
  return stats;
}

// =============================================================================
// 内部方法
// =============================================================================

void PartitionMetadataService::HeartbeatCheckLoop() {
  // 初始等待，避免服务刚启动就检查
  for (uint64_t i = 0; i < config_.heartbeat_check_interval_sec && running_; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  
  while (running_) {
    {
      std::lock_guard<std::shared_mutex> lock(mutex_);
      
      auto now = std::chrono::system_clock::now();
      auto timeout = std::chrono::seconds(config_.heartbeat_timeout_sec);
      
      for (auto& [node_id, node] : nodes_) {
        if (node.IsOnline() && (now - node.last_heartbeat) > timeout) {
          node.state = StorageNodeMetadata::State::kSuspected;
          
          // 通知节点疑似离线
          TopologyChange change;
          change.type = TopologyChangeType::kNodeLeft;
          change.old_node = node_id;
          change.version = 1;
          change.timestamp = now;
          NotifyTopologyChange(change);
        }
      }
    }
    
    // 分段睡眠以便更快响应关闭
    for (uint64_t i = 0; i < config_.heartbeat_check_interval_sec && running_; i++) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

std::string PartitionMetadataService::SelectBestLeader(
    const std::vector<std::string>& candidates) {
  // 简单实现：选择第一个在线的节点
  // 实际应该考虑负载、延迟等因素
  for (const auto& node_id : candidates) {
    auto it = nodes_.find(node_id);
    if (it != nodes_.end() && it->second.IsOnline()) {
      return node_id;
    }
  }
  
  // 如果没有在线节点，返回第一个
  return candidates.empty() ? "" : candidates[0];
}

std::vector<std::string> PartitionMetadataService::SelectReplicaNodes(
    PartitionID part_id,
    uint32_t replica_count) {
  std::vector<std::string> online_nodes;
  for (const auto& [id, node] : nodes_) {
    if (node.IsOnline()) {
      online_nodes.push_back(id);
    }
  }
  
  if (online_nodes.size() < replica_count) {
    return {};  // 在线节点不足
  }
  
  // 使用一致性哈希选择节点
  std::vector<std::string> selected;
  
  // 简单实现：基于分区 ID 选择节点
  // 实际应该使用一致性哈希环
  size_t start_idx = part_id % online_nodes.size();
  for (uint32_t i = 0; i < replica_count; i++) {
    size_t idx = (start_idx + i) % online_nodes.size();
    selected.push_back(online_nodes[idx]);
  }
  
  return selected;
}

void PartitionMetadataService::NotifyTopologyChange(const TopologyChange& change) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  
  for (const auto& callback : topology_callbacks_) {
    callback(change);
  }
}

}  // namespace raft
}  // namespace cedar
