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
// Data Migration Executor - Production Implementation
// =============================================================================
// Handles actual data movement between StorageD nodes for load balancing
// =============================================================================

#ifndef CEDAR_DTX_MIGRATION_EXECUTOR_H_
#define CEDAR_DTX_MIGRATION_EXECUTOR_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {
namespace dtx {

// Migration Plan Definition
struct MigrationPlan {
  struct Action {
    PartitionID partition_id;
    NodeID from_node;
    NodeID to_node;
    uint64_t estimated_bytes = 0;
    double priority = 1.0;
  };
  
  std::vector<Action> actions;
  std::chrono::steady_clock::time_point plan_time;
};

// Forward declarations
class DTxRpcClient;
class PartitionManager;

// =============================================================================
// Migration State Machine
// =============================================================================

enum class MigrationState : uint8_t {
  kPending = 0,       // Migration planned but not started
  kPreparing = 1,     // Preparing source and target nodes
  kSnapshotSync = 2,  // Transferring initial snapshot
  kDualWrite = 3,     // Both old and new nodes accepting writes
  kCutover = 4,       // Switching primary to new node
  kVerifying = 5,     // Verifying data consistency
  kCompleting = 6,    // Finalizing and cleanup
  kCompleted = 7,     // Migration successful
  kFailed = 8,        // Migration failed, rolled back
  kCancelled = 9,     // Migration cancelled by operator
};

inline std::string MigrationStateToString(MigrationState state) {
  switch (state) {
    case MigrationState::kPending: return "Pending";
    case MigrationState::kPreparing: return "Preparing";
    case MigrationState::kSnapshotSync: return "SnapshotSync";
    case MigrationState::kDualWrite: return "DualWrite";
    case MigrationState::kCutover: return "Cutover";
    case MigrationState::kVerifying: return "Verifying";
    case MigrationState::kCompleting: return "Completing";
    case MigrationState::kCompleted: return "Completed";
    case MigrationState::kFailed: return "Failed";
    case MigrationState::kCancelled: return "Cancelled";
    default: return "Unknown";
  }
}

// =============================================================================
// Migration Progress
// =============================================================================

struct MigrationProgress {
  uint64_t migration_id;
  PartitionID partition_id;
  NodeID source_node;
  NodeID target_node;
  
  MigrationState state = MigrationState::kPending;
  
  // Progress metrics
  uint64_t total_keys = 0;
  uint64_t transferred_keys = 0;
  uint64_t total_bytes = 0;
  uint64_t transferred_bytes = 0;
  
  // Timing
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point estimated_end_time;
  
  // Error info
  std::string error_message;
  int retry_count = 0;
  
  double GetProgressPercentage() const {
    if (total_keys == 0) return 0.0;
    return 100.0 * transferred_keys / total_keys;
  }
  
  bool IsTerminal() const {
    return state == MigrationState::kCompleted ||
           state == MigrationState::kFailed ||
           state == MigrationState::kCancelled;
  }
};

// =============================================================================
// Migration Configuration
// =============================================================================

struct MigrationConfig {
  // Performance tuning
  size_t batch_size = 1000;              // Keys per batch
  size_t max_concurrent_migrations = 3;  // Max parallel migrations
  
  // Rate limiting
  size_t max_bandwidth_mbps = 100;       // Max migration bandwidth
  size_t max_iops = 1000;                // Max IOPS during migration
  
  // Timeouts
  std::chrono::seconds snapshot_timeout{300};     // 5 min for snapshot
  std::chrono::seconds dual_write_timeout{600};   // 10 min for dual write
  std::chrono::seconds cutover_timeout{30};       // 30 sec for cutover
  
  // Retry policy
  uint32_t max_retries = 3;
  std::chrono::seconds retry_base_delay{10};
  
  // Consistency
  bool verify_checksum = true;
  bool enable_dual_write = true;         // Enable dual write phase
  
  // Throttling
  bool throttle_during_migration = true; // Throttle normal traffic
  double throttle_factor = 0.5;          // 50% throttle during migration
};

// =============================================================================
// Migration Task
// =============================================================================

class MigrationTask {
 public:
  MigrationTask(uint64_t id, const MigrationPlan::Action& action,
                const MigrationConfig& config);
  
  uint64_t GetId() const { return migration_id_; }
  MigrationProgress GetProgress() const;
  
  // Execute migration (blocking)
  Status Execute(DTxRpcClient* rpc_client, PartitionManager* partition_mgr);
  
  // Cancel migration
  void Cancel();
  
  // Pause/Resume
  void Pause();
  void Resume();

 private:
  // Phase implementations
  Status Phase_Prepare();
  Status Phase_SnapshotSync();
  Status Phase_DualWrite();
  Status Phase_Cutover();
  Status Phase_Verify();
  Status Phase_Complete();
  
  // Rollback on failure
  Status Rollback();
  
  // Utility
  Status TransferBatch(const std::vector<std::pair<CedarKey, Descriptor>>& batch);
  bool ShouldThrottle() const;
  void UpdateState(MigrationState new_state);
  
  uint64_t migration_id_;
  PartitionID partition_id_;
  NodeID source_node_;
  NodeID target_node_;
  MigrationConfig config_;
  
  std::atomic<MigrationState> state_{MigrationState::kPending};
  std::atomic<bool> cancelled_{false};
  std::atomic<bool> paused_{false};
  
  MigrationProgress progress_;
  mutable std::mutex progress_mutex_;
  
  DTxRpcClient* rpc_client_ = nullptr;
  PartitionManager* partition_mgr_ = nullptr;
  
  // Migration ID returned by the target node's PartitionMigrationService
  std::string external_migration_id_;
};

// =============================================================================
// Migration Executor
// =============================================================================

class MigrationExecutor {
 public:
  using ProgressCallback = std::function<void(const MigrationProgress&)>;
  using CompletionCallback = std::function<void(uint64_t migration_id, bool success)>;
  
  MigrationExecutor(const MigrationConfig& config = MigrationConfig{});
  ~MigrationExecutor();
  
  // Initialize with dependencies
  Status Initialize(DTxRpcClient* rpc_client, PartitionManager* partition_mgr);
  
  // Submit migration plan for execution
  std::vector<uint64_t> SubmitPlan(const MigrationPlan& plan);
  
  // Submit single migration
  uint64_t SubmitMigration(const MigrationPlan::Action& action);
  
  // Query migration status
  MigrationProgress GetProgress(uint64_t migration_id) const;
  std::vector<MigrationProgress> GetAllProgress() const;
  
  // Control operations
  Status CancelMigration(uint64_t migration_id);
  Status PauseMigration(uint64_t migration_id);
  Status ResumeMigration(uint64_t migration_id);
  
  // Set callbacks
  void SetProgressCallback(ProgressCallback callback);
  void SetCompletionCallback(CompletionCallback callback);
  
  // Statistics
  struct Stats {
    uint64_t total_migrations = 0;
    uint64_t successful_migrations = 0;
    uint64_t failed_migrations = 0;
    uint64_t total_keys_transferred = 0;
    uint64_t total_bytes_transferred = 0;
    std::chrono::seconds total_migration_time{0};
  };
  Stats GetStats() const;
  
  // Shutdown
  void Shutdown();

 private:
  void WorkerLoop();
  void ExecuteMigration(std::shared_ptr<MigrationTask> task);
  void NotifyProgress(const MigrationProgress& progress);
  void NotifyCompletion(uint64_t id, bool success);
  
  MigrationConfig config_;
  
  DTxRpcClient* rpc_client_ = nullptr;
  PartitionManager* partition_mgr_ = nullptr;
  
  std::atomic<uint64_t> next_migration_id_{1};
  std::atomic<bool> shutdown_{false};
  
  mutable std::mutex tasks_mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<MigrationTask>> tasks_;
  std::queue<std::shared_ptr<MigrationTask>> pending_queue_;
  std::unordered_set<uint64_t> active_migrations_;
  
  std::vector<std::thread> worker_threads_;
  std::condition_variable worker_cv_;
  
  ProgressCallback progress_callback_;
  CompletionCallback completion_callback_;
  
  mutable std::mutex stats_mutex_;
  Stats stats_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_MIGRATION_EXECUTOR_H_
