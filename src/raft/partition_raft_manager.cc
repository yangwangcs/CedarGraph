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

#include "cedar/raft/partition_raft_manager.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <numeric>

namespace cedar {
namespace raft {

PartitionRaftManager::PartitionRaftManager() = default;

PartitionRaftManager::~PartitionRaftManager() {
  Stop();
}

Status PartitionRaftManager::Initialize(
    const PartitionRaftManagerConfig& config,
    std::shared_ptr<storage::StorageHealthMonitor> health_monitor) {
  
  config_ = config;
  health_monitor_ = health_monitor;
  
  // 初始化一致性哈希环（用于副本放置）
  storage::HashRingConfig hash_config;
  hash_config.virtual_nodes_per_physical = 100;
  hash_ring_ = std::make_unique<storage::ConsistentHashRing>(hash_config);
  
  return Status::OK();
}

Status PartitionRaftManager::Start() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Already running");
  }
  
  raft_worker_pool_ = std::make_unique<ThreadPool>(
      config_.raft_worker_threads > 0 ? config_.raft_worker_threads : 4);
  
  // 启动所有分区组并加入调度队列
  {
    std::lock_guard<std::mutex> lock(groups_mutex_);
    for (auto& [part_id, group] : partition_groups_) {
      if (group) {
        group->Start();
        std::lock_guard<std::mutex> slock(schedule_mutex_);
        schedule_queue_.push({part_id, std::chrono::steady_clock::now()});
      }
    }
  }
  
  schedule_cv_.notify_one();
  scheduler_thread_ = std::thread(&PartitionRaftManager::SchedulerLoop, this);
  
  return Status::OK();
}

void PartitionRaftManager::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  schedule_cv_.notify_all();
  if (scheduler_thread_.joinable()) {
    scheduler_thread_.join();
  }
  
  if (raft_worker_pool_) {
    raft_worker_pool_->WaitForAll();
  }
  
  {
    std::lock_guard<std::mutex> lock(groups_mutex_);
    for (auto& [part_id, group] : partition_groups_) {
      if (group) {
        group->Stop();
      }
    }
  }
  
  raft_worker_pool_.reset();
}

Status PartitionRaftManager::RegisterNode(const std::string& node_id,
                                          const std::string& address,
                                          uint16_t port) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  nodes_[node_id] = {address, port};
  
  // 添加到哈希环（用于副本放置）
  hash_ring_->AddNode(node_id);
  
  // 初始化节点负载
  NodePartitionLoad load;
  load.node_id = node_id;
  node_loads_[node_id] = load;
  
  return Status::OK();
}

Status PartitionRaftManager::DeregisterNode(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("Node not found: " + node_id);
  }
  
  nodes_.erase(it);
  hash_ring_->RemoveNode(node_id);
  node_loads_.erase(node_id);
  
  return Status::OK();
}

Status PartitionRaftManager::CreatePartitionGroup(
    PartitionID part_id,
    const std::vector<std::string>& replica_nodes) {
  
  if (part_id >= kMaxPartitions) {
    return Status::InvalidArgument("Partition ID out of range");
  }
  
  // 初始化副本（在锁外准备）
  std::vector<ReplicaInfo> replicas;
  {
    std::lock_guard<std::mutex> node_lock(nodes_mutex_);
    for (const auto& node_id : replica_nodes) {
      auto it = nodes_.find(node_id);
      if (it != nodes_.end()) {
        ReplicaInfo replica;
        replica.node_id = node_id;
        replica.address = it->second.first + ":" + std::to_string(it->second.second);
        replica.role = RaftRole::kFollower;
        replicas.push_back(replica);
      }
    }
  }
  
  // 至少需要 1 个副本（简化测试要求）
  if (replicas.empty()) {
    return Status::InvalidArgument("No valid replicas found");
  }
  
  // 创建 Raft 组
  PartitionRaftConfig raft_config;
  auto group = std::make_unique<PartitionRaftGroup>(part_id, raft_config);
  
  // 使用第一个副本作为初始 Leader
  std::string initial_leader = replicas.empty() ? "" : replicas[0].node_id;
  
  Status s = group->Initialize(replicas, initial_leader);
  if (!s.ok()) {
    return s;
  }
  
  // 设置 Leader 变化回调
  group->SetLeaderChangeCallback(
    [this](cedar::PartitionID pid, const std::string& old_leader,
           const std::string& new_leader) {
      OnPartitionLeaderChanged(pid, old_leader, new_leader);
    });
  
  if (running_) {
    s = group->Start();
    if (!s.ok()) {
      return s;
    }
    std::lock_guard<std::mutex> slock(schedule_mutex_);
    schedule_queue_.push({part_id, std::chrono::steady_clock::now()});
    schedule_cv_.notify_one();
  }
  
  // 在锁内添加分区组
  {
    std::lock_guard<std::mutex> lock(groups_mutex_);
    if (partition_groups_.find(part_id) != partition_groups_.end()) {
      return Status::InvalidArgument("Partition group already exists");
    }
    partition_groups_[part_id] = std::move(group);
  }
  
  // 更新节点负载（在锁外）
  for (const auto& node_id : replica_nodes) {
    UpdateNodeLoad(node_id);
  }
  
  return Status::OK();
}

Status PartitionRaftManager::RemovePartitionGroup(PartitionID part_id) {
  std::unique_ptr<PartitionRaftGroup> group;
  
  {
    std::lock_guard<std::mutex> lock(groups_mutex_);
    auto it = partition_groups_.find(part_id);
    if (it == partition_groups_.end()) {
      return Status::NotFound("Partition group not found");
    }
    group = std::move(it->second);
    partition_groups_.erase(it);
  }
  
  // 在锁外停止组
  if (group) {
    group->Stop();
  }
  
  return Status::OK();
}

PartitionRaftGroup* PartitionRaftManager::GetPartitionGroup(PartitionID part_id) {
  std::lock_guard<std::mutex> lock(groups_mutex_);
  
  auto it = partition_groups_.find(part_id);
  if (it != partition_groups_.end()) {
    return it->second.get();
  }
  
  return nullptr;
}

PartitionID PartitionRaftManager::ComputePartitionId(uint64_t entity_id) {
  // 使用实体 ID 的低 16 位作为分区 ID
  return static_cast<PartitionID>(entity_id & 0xFFFF);
}

PartitionID PartitionRaftManager::GetPartitionIdForKey(const std::string& key) {
  // 简单的哈希取模
  uint64_t hash = 0;
  for (char c : key) {
    hash = hash * 131 + c;
  }
  return static_cast<PartitionID>(hash % kMaxPartitions);
}

StatusOr<std::string> PartitionRaftManager::RouteWrite(uint64_t entity_id) {
  PartitionID part_id = ComputePartitionId(entity_id);
  return RouteWriteByPartition(part_id);
}

StatusOr<std::string> PartitionRaftManager::RouteWriteByPartition(PartitionID part_id) {
  auto group = GetPartitionGroup(part_id);
  if (!group) {
    // 如果分区不存在，尝试自动创建
    auto replicas = SelectReplicasForPartition(part_id);
    if (replicas.size() >= config_.default_replica_count / 2 + 1) {
      Status s = CreatePartitionGroup(part_id, replicas);
      if (s.ok()) {
        group = GetPartitionGroup(part_id);
      }
    }
    
    if (!group) {
      return Status::NotFound("Partition group not found and cannot be created");
    }
  }
  
  auto result = group->RouteWrite();
  if (!result.ok()) {
    return result.status();
  }
  
  return result.ValueOrDie().node_id;
}

StatusOr<std::string> PartitionRaftManager::RouteRead(uint64_t entity_id, 
                                                       bool require_leader) {
  PartitionID part_id = ComputePartitionId(entity_id);
  return RouteReadByPartition(part_id, require_leader);
}

StatusOr<std::string> PartitionRaftManager::RouteReadByPartition(
    PartitionID part_id,
    bool require_leader) {
  
  auto group = GetPartitionGroup(part_id);
  if (!group) {
    return Status::NotFound("Partition group not found");
  }
  
  auto result = group->RouteRead(require_leader);
  if (!result.ok()) {
    return result.status();
  }
  
  return result.ValueOrDie().node_id;
}

std::vector<PartitionRaftManager::PartitionedBatch> 
PartitionRaftManager::RouteBatchWrite(const std::vector<std::string>& keys) {
  std::unordered_map<PartitionID, std::vector<std::string>> partitioned;
  
  // 按分区 ID 分组
  for (const auto& key : keys) {
    PartitionID part_id = GetPartitionIdForKey(key);
    partitioned[part_id].push_back(key);
  }
  
  // 为每个分区确定目标节点
  std::vector<PartitionedBatch> batches;
  for (auto& [part_id, keys_in_partition] : partitioned) {
    auto node_result = RouteWriteByPartition(part_id);
    if (node_result.ok()) {
      PartitionedBatch batch;
      batch.part_id = part_id;
      batch.keys = std::move(keys_in_partition);
      batch.target_node = node_result.ValueOrDie();
      batches.push_back(std::move(batch));
    }
  }
  
  return batches;
}

std::vector<PartitionRaftManager::PartitionedBatch>
PartitionRaftManager::RouteBatchRead(const std::vector<std::string>& keys,
                                      bool require_leader) {
  std::unordered_map<PartitionID, std::vector<std::string>> partitioned;
  
  for (const auto& key : keys) {
    PartitionID part_id = GetPartitionIdForKey(key);
    partitioned[part_id].push_back(key);
  }
  
  std::vector<PartitionedBatch> batches;
  for (auto& [part_id, keys_in_partition] : partitioned) {
    auto node_result = RouteReadByPartition(part_id, require_leader);
    if (node_result.ok()) {
      PartitionedBatch batch;
      batch.part_id = part_id;
      batch.keys = std::move(keys_in_partition);
      batch.target_node = node_result.ValueOrDie();
      batches.push_back(std::move(batch));
    }
  }
  
  return batches;
}

std::vector<PartitionInfo> PartitionRaftManager::GetAllPartitions() const {
  std::lock_guard<std::mutex> lock(groups_mutex_);
  
  std::vector<PartitionInfo> infos;
  for (const auto& [part_id, group] : partition_groups_) {
    if (group) {
      PartitionInfo info;
      info.part_id = part_id;
      
      auto leader = group->GetLeader();
      if (leader.ok()) {
        info.leader_node = leader.ValueOrDie();
      }
      
      auto replicas = group->GetReplicas();
      for (const auto& replica : replicas) {
        info.replica_nodes.push_back(replica.node_id);
      }
      
      // 简化：假设所有分区都健康
      info.is_healthy = !info.leader_node.empty();
      
      infos.push_back(info);
    }
  }
  
  return infos;
}

NodePartitionLoad PartitionRaftManager::GetNodeLoad(
    const std::string& node_id) const {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  auto it = node_loads_.find(node_id);
  if (it != node_loads_.end()) {
    return it->second;
  }
  
  return NodePartitionLoad{};
}

std::vector<NodePartitionLoad> PartitionRaftManager::GetAllNodesLoad() const {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  std::vector<NodePartitionLoad> loads;
  for (const auto& [node_id, load] : node_loads_) {
    loads.push_back(load);
  }
  
  return loads;
}

std::vector<PartitionRaftManager::LeaderDistribution>
PartitionRaftManager::GetLeaderDistribution() const {
  auto partitions = GetAllPartitions();
  
  std::unordered_map<std::string, size_t> leader_counts;
  for (const auto& info : partitions) {
    leader_counts[info.leader_node]++;
  }
  
  std::vector<LeaderDistribution> distribution;
  for (const auto& [node_id, count] : leader_counts) {
    LeaderDistribution dist;
    dist.node_id = node_id;
    dist.leader_count = count;
    dist.percentage = static_cast<double>(count) / partitions.size() * 100;
    distribution.push_back(dist);
  }
  
  return distribution;
}

Status PartitionRaftManager::RebalancePartitions() {
  // 获取当前 Leader 分布
  auto distribution = GetLeaderDistribution();
  
  // 计算平均每个节点应该有多少 Leader
  auto partitions = GetAllPartitions();
  size_t avg_leaders = partitions.size() / nodes_.size();
  
  // 找出负载过高和过低的节点
  std::vector<std::string> overloaded_nodes;
  std::vector<std::string> underloaded_nodes;
  
  for (const auto& dist : distribution) {
    if (dist.leader_count > avg_leaders * (1 + config_.rebalance_threshold)) {
      overloaded_nodes.push_back(dist.node_id);
    } else if (dist.leader_count < avg_leaders * (1 - config_.rebalance_threshold)) {
      underloaded_nodes.push_back(dist.node_id);
    }
  }
  
  // 从过载节点迁移分区到欠载节点
  // 简化实现：实际应该考虑数据大小、网络延迟等因素
  
  return Status::OK();
}

Status PartitionRaftManager::MigratePartitionLeader(
    PartitionID part_id,
    const std::string& target_node) {
  
  auto group = GetPartitionGroup(part_id);
  if (!group) {
    return Status::NotFound("Partition group not found");
  }
  
  return group->TransferLeadership(target_node);
}

PartitionRaftManager::Stats PartitionRaftManager::GetStats() const {
  Stats stats;
  
  auto partitions = GetAllPartitions();
  stats.total_partitions = partitions.size();
  
  for (const auto& info : partitions) {
    if (info.is_healthy) {
      stats.healthy_partitions++;
    }
    if (!info.leader_node.empty()) {
      stats.active_partitions++;
    }
  }
  
  {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    stats.total_nodes = nodes_.size();
    
    // 统计健康节点
    for (const auto& [node_id, _] : nodes_) {
      // 简化：假设所有节点都健康
      stats.healthy_nodes++;
    }
  }
  
  return stats;
}

void PartitionRaftManager::SetPartitionLeaderChangeCallback(
    PartitionLeaderChangeCallback callback) {
  leader_change_callback_ = callback;
}

void PartitionRaftManager::SetNodeLoadChangeCallback(
    NodeLoadChangeCallback callback) {
  load_change_callback_ = callback;
}

void PartitionRaftManager::OnPartitionLeaderChanged(
    PartitionID part_id,
    const std::string& old_leader,
    const std::string& new_leader) {
  
  // 更新节点负载
  if (!old_leader.empty()) {
    UpdateNodeLoad(old_leader);
  }
  if (!new_leader.empty()) {
    UpdateNodeLoad(new_leader);
  }
  
  // 触发回调
  if (leader_change_callback_) {
    leader_change_callback_(part_id, old_leader, new_leader);
  }
  
  // 检查是否需要重平衡
  if (config_.enable_auto_rebalance) {
    CheckAndTriggerRebalance();
  }
}

std::vector<std::string> PartitionRaftManager::SelectReplicasForPartition(
    PartitionID part_id) {
  std::vector<std::string> replicas;
  
  // 使用一致性哈希选择副本节点
  std::string key = "partition_" + std::to_string(part_id);
  auto nodes = hash_ring_->GetNodes(key, config_.default_replica_count);
  
  // 检查节点健康状态
  {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    for (const auto& node_id : nodes) {
      if (nodes_.find(node_id) != nodes_.end()) {
        replicas.push_back(node_id);
      }
    }
  }
  
  return replicas;
}

void PartitionRaftManager::UpdateNodeLoad(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  auto it = node_loads_.find(node_id);
  if (it == node_loads_.end()) {
    return;
  }
  
  auto& load = it->second;
  load.leader_partitions.clear();
  load.follower_partitions.clear();
  
  // 统计该节点的分区负载
  {
    std::lock_guard<std::mutex> group_lock(groups_mutex_);
    for (const auto& [part_id, group] : partition_groups_) {
      if (!group) continue;
      
      auto leader = group->GetLeader();
      if (leader.ok() && leader.ValueOrDie() == node_id) {
        load.leader_partitions.push_back(part_id);
      } else {
        auto replicas = group->GetReplicas();
        for (const auto& replica : replicas) {
          if (replica.node_id == node_id) {
            load.follower_partitions.push_back(part_id);
            break;
          }
        }
      }
    }
  }
  
  // 计算负载分数（Leader 权重更高）
  double old_load = load.load_score;
  load.load_score = load.leader_partitions.size() * 2.0 + 
                    load.follower_partitions.size() * 0.5;
  
  if (load_change_callback_ && old_load != load.load_score) {
    load_change_callback_(node_id, old_load, load.load_score);
  }
}

void PartitionRaftManager::CheckAndTriggerRebalance() {
  if (!config_.enable_auto_rebalance) {
    return;
  }
  
  // 简化：立即触发重平衡
  // 实际实现中应该有更复杂的逻辑，比如检查负载差异是否超过阈值
  RebalancePartitions();
}

void PartitionRaftManager::SchedulerLoop() {
  while (running_) {
    std::unique_lock<std::mutex> lock(schedule_mutex_);
    if (schedule_queue_.empty()) {
      schedule_cv_.wait(lock, [this] { return !schedule_queue_.empty() || !running_; });
      continue;
    }
    
    auto now = std::chrono::steady_clock::now();
    auto next = schedule_queue_.top();
    if (next.next_tick > now) {
      schedule_cv_.wait_until(lock, next.next_tick, [this] { return !running_; });
      continue;
    }
    
    schedule_queue_.pop();
    lock.unlock();
    
    raft_worker_pool_->Schedule([this, part_id = next.part_id]() {
      auto* group = GetPartitionGroup(part_id);
      if (group && group->IsRunning()) {
        auto delay = group->RaftTick();
        std::lock_guard<std::mutex> l(schedule_mutex_);
        schedule_queue_.push({part_id, std::chrono::steady_clock::now() + delay});
        schedule_cv_.notify_one();
      }
    });
  }
}

}  // namespace raft
}  // namespace cedar
