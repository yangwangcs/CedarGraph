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
// Partition Migration Tool - Data Rebalancing
// =============================================================================
// Supports online partition migration for load balancing and recovery

#ifndef CEDAR_DTX_STORAGE_PARTITION_MIGRATOR_H_
#define CEDAR_DTX_STORAGE_PARTITION_MIGRATOR_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "migration_service.grpc.pb.h"

namespace cedar {
namespace dtx {

class StoragePartitionManager;
class MetaServiceNodeClient;

namespace storage {

// =============================================================================
// Migration Types
// =============================================================================

enum class MigrationState : uint8_t {
  kPending = 0,      // Waiting to start
  kPreparing = 1,    // Preparing source partition
  kCopying = 2,      // Copying data
  kCatchingUp = 3,   // Catching up with new writes
  kSwitching = 4,    // Switching traffic
  kVerifying = 5,    // Verifying consistency
  kCompleting = 6,   // Completing migration
  kCompleted = 7,    // Migration completed
  kFailed = 8,       // Migration failed
  kRolledBack = 9,   // Rolled back
};

enum class MigrationType : uint8_t {
  kRebalance = 0,    // Rebalance load
  kRecovery = 1,     // Recover from failed node
  kUpgrade = 2,      // Hardware upgrade
  kMaintenance = 3,  // Scheduled maintenance
};

inline std::string MigrationStateToString(MigrationState state) {
  switch (state) {
    case MigrationState::kPending: return "Pending";
    case MigrationState::kPreparing: return "Preparing";
    case MigrationState::kCopying: return "Copying";
    case MigrationState::kCatchingUp: return "CatchingUp";
    case MigrationState::kSwitching: return "Switching";
    case MigrationState::kVerifying: return "Verifying";
    case MigrationState::kCompleting: return "Completing";
    case MigrationState::kCompleted: return "Completed";
    case MigrationState::kFailed: return "Failed";
    case MigrationState::kRolledBack: return "RolledBack";
    default: return "Unknown";
  }
}

// =============================================================================
// Migration Configuration
// =============================================================================

struct MigrationConfig {
  // Performance settings
  uint64_t max_bandwidth_mbps = 100;           // Max migration bandwidth
  uint32_t max_concurrent_batches = 10;        // Parallel batch transfers
  uint64_t batch_size_bytes = 64 * 1024 * 1024; // 64MB batches
  
  // Timing settings
  std::chrono::milliseconds catchup_interval{1000};  // 1 second
  uint32_t max_catchup_iterations = 10;
  std::chrono::milliseconds verify_timeout{300000};  // 5 minutes
  
  // Safety settings
  bool verify_checksum = true;
  bool enable_compression = true;
  uint32_t max_retries = 3;
  bool allow_write_during_migration = true;
};

// =============================================================================
// Migration Task
// =============================================================================

struct MigrationTask {
  uint64_t migration_id = 0;
  PartitionID partition_id;
  NodeID source_node;
  NodeID target_node;
  MigrationType type;
  MigrationState state = MigrationState::kPending;
  
  // Progress tracking
  uint64_t total_keys = 0;
  uint64_t migrated_keys = 0;
  uint64_t total_bytes = 0;
  uint64_t migrated_bytes = 0;
  
  // Timing
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point started_at;
  std::chrono::system_clock::time_point completed_at;
  
  // Error handling
  uint32_t retry_count = 0;
  std::string last_error;
  
  // State transition helpers
  bool CanTransitionTo(MigrationState new_state) const;
  Status TransitionTo(MigrationState new_state);
  double GetProgressPercent() const {
    return total_keys > 0 ? (100.0 * migrated_keys / total_keys) : 0.0;
  }
};

// =============================================================================
// Migration Progress Callback
// =============================================================================

using MigrationProgressCallback = std::function<void(
    uint64_t migration_id,
    MigrationState state,
    double progress_percent,
    const std::string& message)>;

// =============================================================================
// Partition Migrator
// =============================================================================

class PartitionMigrator {
 public:
  PartitionMigrator();
  ~PartitionMigrator();
  
  // Lifecycle
  Status Initialize(const MigrationConfig& config);
  void Shutdown();
  bool IsRunning() const { return running_.load(); }
  
  // Submit migration task
  StatusOr<uint64_t> SubmitMigration(
      PartitionID pid,
      NodeID source_node,
      NodeID target_node,
      MigrationType type,
      MigrationProgressCallback callback = nullptr);
  
  // Get migration status
  StatusOr<MigrationTask> GetMigrationStatus(uint64_t migration_id) const;
  std::vector<MigrationTask> GetActiveMigrations() const;
  std::vector<MigrationTask> GetMigrationHistory(PartitionID pid) const;
  
  // Control operations
  Status PauseMigration(uint64_t migration_id);
  Status ResumeMigration(uint64_t migration_id);
  Status CancelMigration(uint64_t migration_id, bool rollback = true);
  Status CommitMigration(uint64_t migration_id);
  
  // Retry failed migration
  Status RetryMigration(uint64_t migration_id);
  
  // Bulk operations
  Status SubmitRebalancePlan(const std::vector<std::tuple<PartitionID, NodeID, NodeID>>& plan) {
    (void)plan;
    return Status::NotSupported(
        "Rebalance plan submission is not yet production-ready.");
  }
  
  // Statistics
  struct Stats {
    uint64_t total_migrations = 0;
    uint64_t successful_migrations = 0;
    uint64_t failed_migrations = 0;
    uint64_t cancelled_migrations = 0;
    uint64_t total_bytes_migrated = 0;
    uint64_t total_keys_migrated = 0;
    std::chrono::milliseconds avg_migration_time{0};
  };
  Stats GetStats() const;

  // Dependency injection for data movement
  void SetStoragePartitionManager(StoragePartitionManager* manager);
  void SetMetaServiceClient(MetaServiceNodeClient* meta_client);
  void SetMigrationServiceStub(
      std::shared_ptr<cedar::migration::PartitionMigrationService::Stub> stub);

 private:
  void MigrationWorkerLoop();
  void ExecuteMigration(uint64_t migration_id);
  
  // Migration phases
  Status PrepareSource(MigrationTask& task);
  Status CopyData(MigrationTask& task);
  Status CatchUp(MigrationTask& task);
  Status SwitchTraffic(MigrationTask& task);
  Status VerifyConsistency(MigrationTask& task);
  Status CompleteMigration(MigrationTask& task);
  Status RollbackMigration(MigrationTask& task);
  
  // Helper methods
  Status TransferBatch(MigrationTask& task, uint64_t start_key, uint64_t batch_size);
  Status CalculateChecksum(PartitionID pid, std::string* checksum);
  bool VerifyChecksum(const std::string& source_checksum, 
                      const std::string& target_checksum);
  Status StreamSnapshotToTarget(
      const MigrationTask& task, const std::string& snapshot_path);
  
  std::atomic<bool> running_{false};
  MigrationConfig config_;
  
  std::atomic<uint64_t> next_migration_id_{1};
  
  mutable std::shared_mutex tasks_mutex_;
  std::unordered_map<uint64_t, std::unique_ptr<MigrationTask>> tasks_;
  std::unordered_map<uint64_t, MigrationProgressCallback> callbacks_;
  
  std::vector<std::unique_ptr<std::thread>> worker_threads_;
  
  mutable std::mutex stats_mutex_;
  Stats stats_;

  StoragePartitionManager* partition_manager_ = nullptr;
  MetaServiceNodeClient* meta_client_ = nullptr;
  std::shared_ptr<cedar::migration::PartitionMigrationService::Stub> migration_stub_;
};

// =============================================================================
// Rebalance Planner
// =============================================================================

class RebalancePlanner {
 public:
  struct NodeLoad {
    NodeID node_id;
    uint64_t partition_count = 0;
    uint64_t total_keys = 0;
    uint64_t total_bytes = 0;
    double cpu_usage = 0.0;
    double disk_usage = 0.0;
  };
  
  struct RebalanceAction {
    PartitionID partition_id;
    NodeID from_node;
    NodeID to_node;
    double score = 0.0;  // Higher is better
  };
  
  RebalancePlanner();
  
  // Analyze current load distribution
  void AnalyzeLoad(const std::vector<NodeLoad>& node_loads,
                   const std::map<PartitionID, NodeID>& partition_dist);
  
  // Generate rebalance plan
  std::vector<RebalanceAction> GeneratePlan(
      uint32_t max_moves = 10,
      double imbalance_threshold = 0.2);  // 20% threshold
  
  // Check if rebalancing is needed
  bool IsRebalancingNeeded(double threshold = 0.2) const;
  
  // Calculate load variance
  double CalculateLoadVariance() const;

 private:
  mutable std::mutex mutex_;
  std::vector<NodeLoad> node_loads_;
  std::map<PartitionID, NodeID> partition_distribution_;
  
  double CalculateNodeScore(const NodeLoad& load) const;
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_PARTITION_MIGRATOR_H_
