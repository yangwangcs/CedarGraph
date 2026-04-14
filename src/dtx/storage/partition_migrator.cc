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
// Partition Migration Implementation
// =============================================================================

#include "cedar/dtx/storage/partition_migrator.h"

#include <algorithm>
#include <cmath>
#include <map>

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// MigrationTask Implementation
// =============================================================================

bool MigrationTask::CanTransitionTo(MigrationState new_state) const {
  // Define valid state transitions
  switch (state) {
    case MigrationState::kPending:
      return new_state == MigrationState::kPreparing ||
             new_state == MigrationState::kRolledBack;
    case MigrationState::kPreparing:
      return new_state == MigrationState::kCopying ||
             new_state == MigrationState::kFailed;
    case MigrationState::kCopying:
      return new_state == MigrationState::kCatchingUp ||
             new_state == MigrationState::kFailed;
    case MigrationState::kCatchingUp:
      return new_state == MigrationState::kSwitching ||
             new_state == MigrationState::kCopying ||  // More data to copy
             new_state == MigrationState::kFailed;
    case MigrationState::kSwitching:
      return new_state == MigrationState::kVerifying ||
             new_state == MigrationState::kFailed;
    case MigrationState::kVerifying:
      return new_state == MigrationState::kCompleting ||
             new_state == MigrationState::kFailed;
    case MigrationState::kCompleting:
      return new_state == MigrationState::kCompleted ||
             new_state == MigrationState::kFailed;
    case MigrationState::kFailed:
      return new_state == MigrationState::kRolledBack ||
             new_state == MigrationState::kPending;  // Retry
    default:
      return false;
  }
}

Status MigrationTask::TransitionTo(MigrationState new_state) {
  if (!CanTransitionTo(new_state)) {
    return Status::InvalidArgument("Invalid state transition from " +
                                    std::to_string(static_cast<int>(state)) +
                                    " to " +
                                    std::to_string(static_cast<int>(new_state)));
  }
  state = new_state;
  return Status::OK();
}

// =============================================================================
// PartitionMigrator Implementation
// =============================================================================

PartitionMigrator::PartitionMigrator() = default;

PartitionMigrator::~PartitionMigrator() {
  Shutdown();
}

Status PartitionMigrator::Initialize(const MigrationConfig& config) {
  config_ = config;
  running_.store(true);
  
  // Start worker threads
  uint32_t num_workers = std::max(1U, config.max_concurrent_batches);
  for (uint32_t i = 0; i < num_workers; ++i) {
    worker_threads_.push_back(std::make_unique<std::thread>(
        &PartitionMigrator::MigrationWorkerLoop, this));
  }
  
  return Status::OK();
}

void PartitionMigrator::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  for (auto& thread : worker_threads_) {
    if (thread && thread->joinable()) {
      thread->join();
    }
  }
  worker_threads_.clear();
}

void PartitionMigrator::MigrationWorkerLoop() {
  while (running_.load()) {
    uint64_t task_id = 0;
    
    // Find pending task
    {
      std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
      for (const auto& [id, task] : tasks_) {
        if (task->state == MigrationState::kPending) {
          task_id = id;
          break;
        }
      }
    }
    
    if (task_id != 0) {
      ExecuteMigration(task_id);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

StatusOr<uint64_t> PartitionMigrator::SubmitMigration(
    PartitionID pid,
    NodeID source_node,
    NodeID target_node,
    MigrationType type,
    MigrationProgressCallback callback) {
  
  if (!running_.load()) {
    return Status::InvalidArgument("PartitionMigrator not running");
  }
  
  uint64_t id = next_migration_id_.fetch_add(1);
  
  auto task = std::make_unique<MigrationTask>();
  task->migration_id = id;
  task->partition_id = pid;
  task->source_node = source_node;
  task->target_node = target_node;
  task->type = type;
  task->state = MigrationState::kPending;
  task->created_at = std::chrono::system_clock::now();
  
  std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
  tasks_[id] = std::move(task);
  if (callback) {
    callbacks_[id] = callback;
  }
  lock.unlock();
  
  return id;
}

void PartitionMigrator::ExecuteMigration(uint64_t migration_id) {
  MigrationTask* task = nullptr;
  MigrationProgressCallback callback = nullptr;
  
  {
    std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
    auto it = tasks_.find(migration_id);
    if (it == tasks_.end()) {
      return;
    }
    task = it->second.get();
    
    auto cb_it = callbacks_.find(migration_id);
    if (cb_it != callbacks_.end()) {
      callback = cb_it->second;
    }
  }
  
  auto notify_progress = [&](const std::string& message) {
    if (callback) {
      callback(migration_id, task->state, task->GetProgressPercent(), message);
    }
  };
  
  // Execute migration phases
  auto status = task->TransitionTo(MigrationState::kPreparing);
  if (!status.ok()) goto migration_failed;
  notify_progress("Preparing source partition");
  
  status = PrepareSource(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kCopying);
  if (!status.ok()) goto migration_failed;
  notify_progress("Starting data copy");
  
  status = CopyData(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kCatchingUp);
  if (!status.ok()) goto migration_failed;
  notify_progress("Catching up with new writes");
  
  status = CatchUp(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kSwitching);
  if (!status.ok()) goto migration_failed;
  notify_progress("Switching traffic to target");
  
  status = SwitchTraffic(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kVerifying);
  if (!status.ok()) goto migration_failed;
  notify_progress("Verifying consistency");
  
  status = VerifyConsistency(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kCompleting);
  if (!status.ok()) goto migration_failed;
  notify_progress("Completing migration");
  
  status = CompleteMigration(*task);
  if (!status.ok()) goto migration_failed;
  
  task->TransitionTo(MigrationState::kCompleted);
  task->completed_at = std::chrono::system_clock::now();
  notify_progress("Migration completed");
  
  {
    std::unique_lock<std::mutex> lock(stats_mutex_);
    stats_.successful_migrations++;
    stats_.total_migrations++;
    stats_.total_bytes_migrated += task->migrated_bytes;
    stats_.total_keys_migrated += task->migrated_keys;
  }
  
  return;
  
migration_failed:
  task->last_error = status.ToString();
  task->TransitionTo(MigrationState::kFailed);
  notify_progress("Migration failed: " + status.ToString());
  
  {
    std::unique_lock<std::mutex> lock(stats_mutex_);
    stats_.failed_migrations++;
    stats_.total_migrations++;
  }
  
  if (config_.max_retries > 0 && task->retry_count < config_.max_retries) {
    task->retry_count++;
    RetryMigration(migration_id);
  } else {
    RollbackMigration(*task);
  }
}

Status PartitionMigrator::PrepareSource(MigrationTask& task) {
  // TODO: Prepare source partition for migration
  // - Pause compactions temporarily
  // - Create snapshot point
  // - Get partition stats
  return Status::OK();
}

Status PartitionMigrator::CopyData(MigrationTask& task) {
  // TODO: Implement actual data copy
  // - Transfer SST files
  // - Transfer WAL if needed
  // - Update progress
  return Status::OK();
}

Status PartitionMigrator::CatchUp(MigrationTask& task) {
  // TODO: Implement catch-up logic
  // - Copy new writes since snapshot
  // - Repeat until lag is small
  return Status::OK();
}

Status PartitionMigrator::SwitchTraffic(MigrationTask& task) {
  // TODO: Implement traffic switching
  // - Update partition assignment in MetaD
  // - Redirect reads to new location
  // - Wait for in-flight writes
  return Status::OK();
}

Status PartitionMigrator::VerifyConsistency(MigrationTask& task) {
  if (!config_.verify_checksum) {
    return Status::OK();
  }
  
  std::string source_checksum, target_checksum;
  auto status = CalculateChecksum(task.partition_id, &source_checksum);
  if (!status.ok()) return status;
  
  // TODO: Get target checksum
  
  if (!VerifyChecksum(source_checksum, target_checksum)) {
    return Status::Corruption("Checksum mismatch after migration");
  }
  
  return Status::OK();
}

Status PartitionMigrator::CompleteMigration(MigrationTask& task) {
  // TODO: Complete migration
  // - Clean up source data (optional)
  // - Update metadata
  // - Resume normal operations
  return Status::OK();
}

Status PartitionMigrator::RollbackMigration(MigrationTask& task) {
  // TODO: Implement rollback
  // - Revert traffic to source
  // - Clean up partial data on target
  task.state = MigrationState::kRolledBack;
  return Status::OK();
}

Status PartitionMigrator::CalculateChecksum(PartitionID pid, 
                                            std::string* checksum) {
  // TODO: Calculate partition checksum
  *checksum = "dummy_checksum";
  return Status::OK();
}

bool PartitionMigrator::VerifyChecksum(const std::string& source_checksum,
                                        const std::string& target_checksum) {
  return source_checksum == target_checksum;
}

StatusOr<MigrationTask> PartitionMigrator::GetMigrationStatus(
    uint64_t migration_id) const {
  std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }
  return *it->second;
}

std::vector<MigrationTask> PartitionMigrator::GetActiveMigrations() const {
  std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
  std::vector<MigrationTask> active;
  
  for (const auto& [id, task] : tasks_) {
    if (task->state != MigrationState::kCompleted &&
        task->state != MigrationState::kFailed &&
        task->state != MigrationState::kRolledBack) {
      active.push_back(*task);
    }
  }
  
  return active;
}

Status PartitionMigrator::CancelMigration(uint64_t migration_id, bool rollback) {
  std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }
  
  auto& task = it->second;
  
  if (task->state == MigrationState::kCompleted ||
      task->state == MigrationState::kFailed ||
      task->state == MigrationState::kRolledBack) {
    return Status::InvalidArgument("Cannot cancel finished migration");
  }
  
  if (rollback) {
    lock.unlock();
    RollbackMigration(*task);
  }
  
  task->state = MigrationState::kRolledBack;
  
  std::unique_lock<std::mutex> stats_lock(stats_mutex_);
  stats_.cancelled_migrations++;
  
  return Status::OK();
}

Status PartitionMigrator::CommitMigration(uint64_t migration_id) {
  std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }
  
  auto& task = it->second;
  
  if (task->state == MigrationState::kCompleted) {
    return Status::OK();
  }
  if (task->state == MigrationState::kFailed ||
      task->state == MigrationState::kRolledBack) {
    return Status::InvalidArgument("Cannot commit failed/rolled-back migration");
  }
  
  lock.unlock();
  
  auto status = CompleteMigration(*task);
  if (!status.ok()) {
    return status;
  }
  
  task->state = MigrationState::kCompleted;
  task->completed_at = std::chrono::system_clock::now();
  
  {
    std::unique_lock<std::mutex> stats_lock(stats_mutex_);
    stats_.successful_migrations++;
    stats_.total_migrations++;
    stats_.total_bytes_migrated += task->migrated_bytes;
    stats_.total_keys_migrated += task->migrated_keys;
  }
  
  return Status::OK();
}

Status PartitionMigrator::RetryMigration(uint64_t migration_id) {
  std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }
  
  auto& task = it->second;
  
  if (task->state != MigrationState::kFailed &&
      task->state != MigrationState::kRolledBack) {
    return Status::InvalidArgument("Can only retry failed migrations");
  }
  
  task->state = MigrationState::kPending;
  task->last_error.clear();
  
  return Status::OK();
}

PartitionMigrator::Stats PartitionMigrator::GetStats() const {
  std::unique_lock<std::mutex> lock(stats_mutex_);
  return stats_;
}

// =============================================================================
// RebalancePlanner Implementation
// =============================================================================

RebalancePlanner::RebalancePlanner() = default;

void RebalancePlanner::AnalyzeLoad(
    const std::vector<NodeLoad>& node_loads,
    const std::map<PartitionID, NodeID>& partition_distribution) {
  node_loads_ = node_loads;
  partition_distribution_ = partition_distribution;
}

std::vector<RebalancePlanner::RebalanceAction> RebalancePlanner::GeneratePlan(
    uint32_t max_moves, double imbalance_threshold) {
  std::vector<RebalanceAction> plan;
  
  if (!IsRebalancingNeeded(imbalance_threshold)) {
    return plan;
  }
  
  // Calculate target load per node
  uint64_t total_keys = 0;
  for (const auto& load : node_loads_) {
    total_keys += load.total_keys;
  }
  
  if (node_loads_.empty() || total_keys == 0) {
    return plan;
  }
  
  double target_keys_per_node = static_cast<double>(total_keys) / node_loads_.size();
  
  // Find overloaded and underloaded nodes
  std::vector<NodeID> overloaded;
  std::vector<NodeID> underloaded;
  
  for (const auto& load : node_loads_) {
    double ratio = load.total_keys / target_keys_per_node;
    if (ratio > 1.0 + imbalance_threshold) {
      overloaded.push_back(load.node_id);
    } else if (ratio < 1.0 - imbalance_threshold) {
      underloaded.push_back(load.node_id);
    }
  }
  
  // Generate moves from overloaded to underloaded
  for (const auto& from_node : overloaded) {
    if (plan.size() >= max_moves) break;
    
    for (const auto& to_node : underloaded) {
      if (plan.size() >= max_moves) break;
      
      // Find partitions on from_node that can move to to_node
      for (const auto& [pid, current_node] : partition_distribution_) {
        if (current_node == from_node) {
          RebalanceAction action;
          action.partition_id = pid;
          action.from_node = from_node;
          action.to_node = to_node;
          action.score = CalculateNodeScore(
              *std::find_if(node_loads_.begin(), node_loads_.end(),
                           [to_node](const NodeLoad& l) { 
                             return l.node_id == to_node; 
                           }));
          plan.push_back(action);
          break;
        }
      }
    }
  }
  
  return plan;
}

bool RebalancePlanner::IsRebalancingNeeded(double threshold) const {
  return CalculateLoadVariance() > threshold;
}

double RebalancePlanner::CalculateLoadVariance() const {
  if (node_loads_.size() <= 1) {
    return 0.0;
  }
  
  double total_keys = 0;
  for (const auto& load : node_loads_) {
    total_keys += load.total_keys;
  }
  
  double mean = total_keys / node_loads_.size();
  if (mean == 0) {
    return 0.0;
  }
  
  double variance = 0;
  for (const auto& load : node_loads_) {
    double diff = load.total_keys - mean;
    variance += diff * diff;
  }
  variance /= node_loads_.size();
  
  // Coefficient of variation
  return std::sqrt(variance) / mean;
}

double RebalancePlanner::CalculateNodeScore(const NodeLoad& load) const {
  // Higher score = better candidate for receiving partitions
  // Consider CPU, memory, disk usage
  double score = 1.0;
  
  // Penalize high resource usage
  score -= load.cpu_usage * 0.3;
  score -= load.disk_usage * 0.3;
  
  return score;
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
