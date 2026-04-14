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
// Multi-Raft Optimization Implementation
// =============================================================================

#include "cedar/dtx/storage/multi_raft_optimization.h"

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// RaftThreadPool Implementation
// =============================================================================

RaftThreadPool::RaftThreadPool() = default;

RaftThreadPool::~RaftThreadPool() {
  Shutdown();
}

Status RaftThreadPool::Initialize(const Config& config) {
  config_ = config;
  running_.store(true);
  
  // Start minimum threads
  for (uint32_t i = 0; i < config.min_threads; ++i) {
    workers_.push_back(std::make_unique<std::thread>(&RaftThreadPool::WorkerLoop, this));
  }
  
  return Status::OK();
}

void RaftThreadPool::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  queue_cv_.notify_all();
  
  for (auto& worker : workers_) {
    if (worker && worker->joinable()) {
      worker->join();
    }
  }
  workers_.clear();
}

Status RaftThreadPool::Submit(PartitionID pid, TaskPriority priority, 
                               std::function<void()> task) {
  if (!running_.load()) {
    return Status::InvalidArgument("Thread pool not running");
  }
  
  std::unique_lock<std::mutex> lock(queue_mutex_);
  
  if (task_queue_.size() >= config_.queue_capacity) {
    std::unique_lock<std::mutex> stats_lock(stats_mutex_);
    stats_.total_tasks_dropped++;
    return Status::IOError("Task queue full");
  }
  
  Task t;
  t.priority = priority;
  t.partition_id = pid;
  t.callback = std::move(task);
  t.submit_time = std::chrono::steady_clock::now();
  
  task_queue_.push(std::move(t));
  
  {
    std::unique_lock<std::mutex> stats_lock(stats_mutex_);
    stats_.total_tasks_submitted++;
  }
  
  lock.unlock();
  queue_cv_.notify_one();
  
  // Scale up if needed
  if (task_queue_.size() > workers_.size() * 2 && 
      workers_.size() < config_.max_threads) {
    ScaleThreads();
  }
  
  return Status::OK();
}

Status RaftThreadPool::SubmitElectionTask(PartitionID pid, std::function<void()> task) {
  return Submit(pid, TaskPriority::kCritical, std::move(task));
}

Status RaftThreadPool::SubmitReplicationTask(PartitionID pid, std::function<void()> task) {
  return Submit(pid, TaskPriority::kHigh, std::move(task));
}

Status RaftThreadPool::SubmitHeartbeatTask(PartitionID pid, std::function<void()> task) {
  return Submit(pid, TaskPriority::kNormal, std::move(task));
}

void RaftThreadPool::WorkerLoop() {
  while (running_.load()) {
    Task task;
    
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      
      queue_cv_.wait(lock, [this] {
        return !task_queue_.empty() || !running_.load();
      });
      
      if (!running_.load()) {
        break;
      }
      
      if (task_queue_.empty()) {
        continue;
      }
      
      task = std::move(const_cast<Task&>(task_queue_.top()));
      task_queue_.pop();
    }
    
    active_workers_.fetch_add(1);
    
    // Record wait time
    auto wait_time = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - task.submit_time);
    {
      std::unique_lock<std::mutex> stats_lock(stats_mutex_);
      stats_.queue_wait_time_us = (stats_.queue_wait_time_us + wait_time.count()) / 2;
      stats_.total_tasks_executed++;
    }
    
    // Execute task
    if (task.callback) {
      task.callback();
    }
    
    active_workers_.fetch_sub(1);
  }
}

void RaftThreadPool::ScaleThreads() {
  if (workers_.size() < config_.max_threads) {
    workers_.push_back(std::make_unique<std::thread>(&RaftThreadPool::WorkerLoop, this));
  }
}

RaftThreadPool::Stats RaftThreadPool::GetStats() const {
  std::unique_lock<std::mutex> lock(stats_mutex_);
  Stats s = stats_;
  s.current_threads = static_cast<uint32_t>(workers_.size());
  s.active_threads = active_workers_.load();
  return s;
}

// =============================================================================
// BatchHeartbeatManager Implementation
// =============================================================================

BatchHeartbeatManager::BatchHeartbeatManager() = default;

BatchHeartbeatManager::~BatchHeartbeatManager() {
  Shutdown();
}

Status BatchHeartbeatManager::Initialize(const Config& config, SendCallback callback) {
  config_ = config;
  send_callback_ = callback;
  
  if (config.enabled) {
    running_.store(true);
    batcher_thread_ = std::make_unique<std::thread>(&BatchHeartbeatManager::BatcherLoop, this);
  }
  
  return Status::OK();
}

void BatchHeartbeatManager::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  pending_cv_.notify_all();
  
  if (batcher_thread_ && batcher_thread_->joinable()) {
    batcher_thread_->join();
  }
  
  // Flush remaining
  FlushAll();
}

Status BatchHeartbeatManager::QueueHeartbeat(const HeartbeatEntry& entry) {
  if (!config_.enabled) {
    // Send immediately if batching disabled
    BatchedHeartbeat batch;
    batch.to_node = entry.to_node;
    batch.entries.push_back(entry);
    batch.batch_id = next_batch_id_.fetch_add(1);
    return SendBatch(batch);
  }
  
  std::unique_lock<std::mutex> lock(pending_mutex_);
  pending_heartbeats_[entry.to_node].push_back(entry);
  
  std::unique_lock<std::mutex> stats_lock(stats_mutex_);
  stats_.total_heartbeats_batched++;
  
  // Notify if we have enough for a batch
  if (pending_heartbeats_[entry.to_node].size() >= config_.max_batch_size) {
    pending_cv_.notify_one();
  }
  
  return Status::OK();
}

Status BatchHeartbeatManager::FlushAll() {
  std::unique_lock<std::mutex> lock(pending_mutex_);
  
  for (const auto& [node_id, entries] : pending_heartbeats_) {
    if (!entries.empty()) {
      BatchedHeartbeat batch;
      batch.to_node = node_id;
      batch.entries = entries;
      batch.batch_id = next_batch_id_.fetch_add(1);
      
      lock.unlock();
      SendBatch(batch);
      lock.lock();
    }
  }
  
  pending_heartbeats_.clear();
  return Status::OK();
}

void BatchHeartbeatManager::BatcherLoop() {
  while (running_.load()) {
    std::unique_lock<std::mutex> lock(pending_mutex_);
    
    pending_cv_.wait_for(lock, config_.batch_interval, [this] {
      // Check if any node has enough heartbeats for a batch
      for (const auto& [node_id, entries] : pending_heartbeats_) {
        if (entries.size() >= config_.max_batch_size) {
          return true;
        }
      }
      return !running_.load();
    });
    
    // Send batches for all nodes with pending heartbeats
    std::vector<NodeID> nodes_to_clear;
    
    for (auto& [node_id, entries] : pending_heartbeats_) {
      if (!entries.empty()) {
        BatchedHeartbeat batch;
        batch.to_node = node_id;
        batch.entries = std::move(entries);
        batch.batch_id = next_batch_id_.fetch_add(1);
        
        lock.unlock();
        SendBatch(batch);
        lock.lock();
        
        nodes_to_clear.push_back(node_id);
        
        std::unique_lock<std::mutex> stats_lock(stats_mutex_);
        stats_.total_batches_sent++;
        stats_.avg_batch_size = (stats_.avg_batch_size + batch.entries.size()) / 2;
        // Estimate: 100 bytes per heartbeat header saved
        stats_.network_bytes_saved += batch.entries.size() * 100;
      }
    }
    
    for (const auto& node_id : nodes_to_clear) {
      pending_heartbeats_[node_id].clear();
    }
  }
}

Status BatchHeartbeatManager::SendBatch(const BatchedHeartbeat& batch) {
  if (send_callback_) {
    return send_callback_(batch);
  }
  return Status::OK();
}

BatchHeartbeatManager::Stats BatchHeartbeatManager::GetStats() const {
  std::unique_lock<std::mutex> lock(stats_mutex_);
  return stats_;
}

// =============================================================================
// LeaderRebalancer Implementation
// =============================================================================

LeaderRebalancer::LeaderRebalancer() = default;

LeaderRebalancer::~LeaderRebalancer() {
  Shutdown();
}

Status LeaderRebalancer::Initialize(const Config& config) {
  config_ = config;
  running_.store(true);
  
  if (config.auto_rebalance) {
    rebalancer_thread_ = std::make_unique<std::thread>(&LeaderRebalancer::RebalancerLoop, this);
  }
  
  return Status::OK();
}

void LeaderRebalancer::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  if (rebalancer_thread_ && rebalancer_thread_->joinable()) {
    rebalancer_thread_->join();
  }
}

void LeaderRebalancer::RegisterRaftGroup(PartitionID pid, StorageRaftGroup* group) {
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  raft_groups_[pid] = group;
}

void LeaderRebalancer::UnregisterRaftGroup(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  raft_groups_.erase(pid);
}

void LeaderRebalancer::RebalancerLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(config_.check_interval);
    
    if (!config_.auto_rebalance) {
      continue;
    }
    
    // Check if we can transfer (cooldown)
    {
      std::unique_lock<std::mutex> lock(transfer_mutex_);
      auto now = std::chrono::system_clock::now();
      if (now - last_transfer_time_ < config_.cooldown_period) {
        continue;
      }
    }
    
    if (IsRebalancingNeeded()) {
      auto actions = GenerateRebalancePlan();
      
      uint32_t transfers = 0;
      for (const auto& action : actions) {
        if (transfers >= config_.max_concurrent_transfers) {
          break;
        }
        
        if (ExecuteTransfer(action).ok()) {
          transfers++;
          
          std::unique_lock<std::mutex> lock(transfer_mutex_);
          last_transfer_time_ = std::chrono::system_clock::now();
        }
      }
    }
  }
}

bool LeaderRebalancer::IsRebalancingNeeded() const {
  return CalculateImbalanceScore() > config_.imbalance_threshold;
}

double LeaderRebalancer::CalculateImbalanceScore() const {
  auto distribution = GetCurrentDistribution();
  
  if (distribution.size() <= 1) {
    return 0.0;
  }
  
  // Calculate variance in leader counts
  double total_leaders = 0;
  for (const auto& d : distribution) {
    total_leaders += d.leader_count;
  }
  
  double mean = total_leaders / distribution.size();
  if (mean == 0) {
    return 0.0;
  }
  
  double variance = 0;
  for (const auto& d : distribution) {
    double diff = d.leader_count - mean;
    variance += diff * diff;
  }
  variance /= distribution.size();
  
  // Coefficient of variation normalized to 0-1
  double cv = std::sqrt(variance) / mean;
  return std::min(cv, 1.0);
}

std::vector<LeaderRebalancer::LeaderDistribution> 
LeaderRebalancer::GetCurrentDistribution() const {
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  
  std::unordered_map<NodeID, LeaderDistribution> distribution;
  
  for (const auto& [pid, group] : raft_groups_) {
    if (!group) continue;
    
    // In real implementation, we would get the actual leader node
    // For now, just count leaders
    NodeID leader_node = 0;  // TODO: Get from group
    
    if (distribution.count(leader_node) == 0) {
      distribution[leader_node].node_id = leader_node;
    }
    
    if (group->IsLeader()) {
      distribution[leader_node].leader_count++;
    } else {
      distribution[leader_node].follower_count++;
    }
  }
  
  std::vector<LeaderDistribution> result;
  for (const auto& [node_id, dist] : distribution) {
    result.push_back(dist);
  }
  
  return result;
}

std::vector<LeaderRebalancer::RebalanceAction> 
LeaderRebalancer::GenerateRebalancePlan() {
  std::vector<RebalanceAction> actions;
  
  auto distribution = GetCurrentDistribution();
  if (distribution.size() < 2) {
    return actions;
  }
  
  // Sort by leader count
  std::sort(distribution.begin(), distribution.end(),
            [](const LeaderDistribution& a, const LeaderDistribution& b) {
              return a.leader_count > b.leader_count;
            });
  
  // Move leaders from overloaded to underloaded nodes
  size_t avg_leaders = 0;
  for (const auto& d : distribution) {
    avg_leaders += d.leader_count;
  }
  avg_leaders /= distribution.size();
  
  for (size_t i = 0; i < distribution.size(); ++i) {
    if (distribution[i].leader_count <= avg_leaders) {
      break;
    }
    
    // Find underloaded target
    for (size_t j = distribution.size() - 1; j > i; --j) {
      if (distribution[j].leader_count < avg_leaders) {
        // Find a partition to transfer
        std::shared_lock<std::shared_mutex> lock(groups_mutex_);
        for (const auto& [pid, group] : raft_groups_) {
          if (group && group->IsLeader()) {
            RebalanceAction action;
            action.partition_id = pid;
            action.from_node = distribution[i].node_id;
            action.to_node = distribution[j].node_id;
            action.expected_improvement = 1.0;
            actions.push_back(action);
            break;
          }
        }
      }
    }
  }
  
  return actions;
}

Status LeaderRebalancer::ExecuteTransfer(const RebalanceAction& action) {
  // Step 1: Find the Raft group
  StorageRaftGroup* group = nullptr;
  {
    std::shared_lock<std::shared_mutex> lock(groups_mutex_);
    auto it = raft_groups_.find(action.partition_id);
    if (it == raft_groups_.end()) {
      return Status::NotFound("Raft group not found");
    }
    group = it->second;
  }
  
  if (!group) {
    return Status::InvalidArgument("Raft group is null");
  }
  
  // Step 2: Transfer leadership
  // In real implementation, this would call group->TransferLeadership(target_node)
  // For now, just log
  return Status::OK();
}

Status LeaderRebalancer::TriggerRebalance() {
  if (!IsRebalancingNeeded()) {
    return Status::OK();
  }
  
  auto actions = GenerateRebalancePlan();
  
  for (const auto& action : actions) {
    auto status = ExecuteTransfer(action);
    if (!status.ok()) {
      return status;
    }
  }
  
  return Status::OK();
}

// =============================================================================
// OptimizedMultiRaftManager Implementation
// =============================================================================

OptimizedMultiRaftManager::OptimizedMultiRaftManager() = default;

OptimizedMultiRaftManager::~OptimizedMultiRaftManager() = default;

Status OptimizedMultiRaftManager::Initialize(
    const std::string& base_data_dir,
    const RaftThreadPool::Config& thread_pool_config,
    const BatchHeartbeatManager::Config& heartbeat_config,
    const LeaderRebalancer::Config& rebalancer_config) {
  
  // Initialize base manager
  auto status = RaftStorageManager::Initialize(base_data_dir);
  if (!status.ok()) {
    return status;
  }
  
  // Initialize optimizations
  thread_pool_ = std::make_unique<RaftThreadPool>();
  status = thread_pool_->Initialize(thread_pool_config);
  if (!status.ok()) {
    return status;
  }
  
  heartbeat_manager_ = std::make_unique<BatchHeartbeatManager>();
  status = heartbeat_manager_->Initialize(heartbeat_config, nullptr);
  if (!status.ok()) {
    return status;
  }
  
  leader_rebalancer_ = std::make_unique<LeaderRebalancer>();
  status = leader_rebalancer_->Initialize(rebalancer_config);
  if (!status.ok()) {
    return status;
  }
  
  return Status::OK();
}

StatusOr<StorageRaftGroup*> OptimizedMultiRaftManager::CreateRaftGroup(
    const RaftGroupConfig& config,
    StorageRaftGroup::ApplyCallback apply_cb,
    StorageRaftGroup::StateChangeCallback state_cb) {
  
  // Create group using base manager
  auto result = RaftStorageManager::CreateRaftGroup(config, apply_cb, state_cb);
  if (!result.ok()) {
    return result;
  }
  
  auto* group = result.value();
  
  // Register for leader rebalancing
  if (leader_rebalancer_) {
    leader_rebalancer_->RegisterRaftGroup(config.partition_id, group);
  }
  
  return group;
}

OptimizedMultiRaftManager::OptimizationStats 
OptimizedMultiRaftManager::GetOptimizationStats() const {
  OptimizationStats stats;
  
  if (thread_pool_) {
    stats.thread_pool_stats = thread_pool_->GetStats();
  }
  
  if (heartbeat_manager_) {
    stats.heartbeat_stats = heartbeat_manager_->GetStats();
  }
  
  if (leader_rebalancer_) {
    auto distribution = leader_rebalancer_->GetCurrentDistribution();
    if (!distribution.empty()) {
      stats.leader_distribution = distribution[0];
    }
    stats.leader_imbalance_score = leader_rebalancer_->CalculateImbalanceScore();
  }
  
  return stats;
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
