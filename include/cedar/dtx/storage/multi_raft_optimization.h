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
// Multi-Raft Optimization
// =============================================================================
// Multi-Raft architecture allows each partition to have its own Raft group,
// enabling horizontal scalability and load distribution.
//
// Key optimizations:
// 1. Shared Thread Pool - Avoid one thread per Raft group
// 2. Batch Heartbeat - Reduce network overhead
// 3. Leader Rebalancing - Distribute leaders across nodes
// =============================================================================

#ifndef CEDAR_DTX_STORAGE_MULTI_RAFT_OPTIMIZATION_H_
#define CEDAR_DTX_STORAGE_MULTI_RAFT_OPTIMIZATION_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/storage/raft_replication.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// Multi-Raft Architecture Overview
// =============================================================================
//
// Traditional Single-Raft:              Multi-Raft (Current Implementation):
// ┌─────────────────┐                   ┌─────────────────────────────────────┐
// │   Single Raft   │                   │           Node-A (StorageD)         │
// │   Group         │                   │  ┌─────────┐ ┌─────────┐ ┌────────┐│
// │  ┌───────────┐  │                   │  │Raft-P0  │ │Raft-P1  │ │Raft-P2 ││
// │  │  Leader   │  │                   │  │ (Leader)│ │(Follower)│ │(Leader)││
// │  │  P0-P99   │  │                   │  └─────────┘ └─────────┘ └────────┘│
// │  └───────────┘  │                   └─────────────────────────────────────┘
// └─────────────────┘                                        │
//                                                            │
//                   ┌─────────────────────────────────────┐  │
//                   │           Node-B (StorageD)         │  │
//                   │  ┌─────────┐ ┌─────────┐ ┌────────┐│  │
//                   │  │Raft-P0  │ │Raft-P1  │ │Raft-P2 ││  │ Replication
//                   │  │(Follower)│ │ (Leader)│ │(Follower)│◄─┘
//                   │  └─────────┘ └─────────┘ └────────┘│
//                   └─────────────────────────────────────┘
//
// Benefits:
// - Horizontal scalability (add more partitions)
// - Load distribution (leaders spread across nodes)
// - Fault isolation (failure of one partition doesn't affect others)
//
// =============================================================================

// =============================================================================
// Shared Raft Thread Pool
// =============================================================================
// Instead of creating one thread per Raft group, we use a shared thread pool
// to handle all Raft operations. This reduces context switching overhead.

class RaftThreadPool {
 public:
  struct Config {
    uint32_t min_threads = 4;
    uint32_t max_threads = 32;
    uint32_t queue_capacity = 10000;
    std::chrono::milliseconds idle_timeout{60000};  // 1 minute
  };
  
  enum class TaskPriority : uint8_t {
    kCritical = 0,  // Leader election, commit
    kHigh = 1,      // Log replication
    kNormal = 2,    // Heartbeat
    kLow = 3,       // Compaction, snapshot
  };
  
  struct Task {
    TaskPriority priority;
    PartitionID partition_id;
    std::function<void()> callback;
    std::chrono::steady_clock::time_point submit_time;
    
    bool operator<(const Task& other) const {
      return priority > other.priority;  // Lower value = higher priority
    }
  };
  
  RaftThreadPool();
  ~RaftThreadPool();
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // Submit task for specific partition
  Status Submit(PartitionID pid, TaskPriority priority, std::function<void()> task);
  
  // Submit with automatic priority based on operation type
  Status SubmitElectionTask(PartitionID pid, std::function<void()> task);
  Status SubmitReplicationTask(PartitionID pid, std::function<void()> task);
  Status SubmitHeartbeatTask(PartitionID pid, std::function<void()> task);
  
  // Statistics
  struct Stats {
    uint64_t total_tasks_submitted = 0;
    uint64_t total_tasks_executed = 0;
    uint64_t total_tasks_dropped = 0;
    uint64_t queue_wait_time_us = 0;  // Average wait time
    uint32_t current_threads = 0;
    uint32_t active_threads = 0;
  };
  Stats GetStats() const;

 private:
  void WorkerLoop();
  void ScaleThreads();
  
  Config config_;
  std::atomic<bool> running_{false};
  
  std::priority_queue<Task> task_queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  
  std::vector<std::unique_ptr<std::thread>> workers_;
  std::atomic<uint32_t> active_workers_{0};
  
  Stats stats_;
  mutable std::mutex stats_mutex_;
};

// =============================================================================
// Batch Heartbeat Manager
// =============================================================================
// Batches heartbeats from multiple Raft groups to the same peer,
// reducing network overhead significantly.

class BatchHeartbeatManager {
 public:
  struct Config {
    std::chrono::milliseconds batch_interval{10};  // 10ms batching window
    uint32_t max_batch_size = 100;  // Max heartbeats per batch
    bool enabled = true;
  };
  
  struct HeartbeatEntry {
    PartitionID partition_id;
    uint64_t term;
    uint64_t commit_index;
    NodeID to_node;
    std::chrono::steady_clock::time_point deadline;
  };
  
  struct BatchedHeartbeat {
    NodeID to_node;
    std::vector<HeartbeatEntry> entries;
    uint64_t batch_id;
  };
  
  using SendCallback = std::function<Status(const BatchedHeartbeat&)>;
  
  BatchHeartbeatManager();
  ~BatchHeartbeatManager();
  
  Status Initialize(const Config& config, SendCallback callback);
  void Shutdown();
  
  // Queue a heartbeat to be sent
  Status QueueHeartbeat(const HeartbeatEntry& entry);
  
  // Flush all pending heartbeats immediately
  Status FlushAll();
  
  // Statistics
  struct Stats {
    uint64_t total_heartbeats_batched = 0;
    uint64_t total_batches_sent = 0;
    uint64_t avg_batch_size = 0;
    uint64_t network_bytes_saved = 0;  // Compared to individual sends
  };
  Stats GetStats() const;

 private:
  void BatcherLoop();
  Status SendBatch(const BatchedHeartbeat& batch);
  
  Config config_;
  std::atomic<bool> running_{false};
  SendCallback send_callback_;
  
  std::unordered_map<NodeID, std::vector<HeartbeatEntry>> pending_heartbeats_;
  mutable std::mutex pending_mutex_;
  std::condition_variable pending_cv_;
  
  std::unique_ptr<std::thread> batcher_thread_;
  
  std::atomic<uint64_t> next_batch_id_{1};
  
  Stats stats_;
  mutable std::mutex stats_mutex_;
};

// =============================================================================
// Leader Rebalancing
// =============================================================================
// Distributes Raft leaders evenly across nodes to balance load.
// This prevents scenarios where one node is leader for all partitions.

class LeaderRebalancer {
 public:
  struct Config {
    std::chrono::seconds check_interval{60};  // Check every minute
    double imbalance_threshold = 0.2;  // 20% threshold
    bool auto_rebalance = true;
    uint32_t max_concurrent_transfers = 1;
    std::chrono::seconds cooldown_period{300};  // 5 minutes between transfers
  };
  
  struct LeaderDistribution {
    NodeID node_id;
    uint32_t leader_count = 0;
    uint32_t follower_count = 0;
    double cpu_usage = 0.0;
    double memory_usage = 0.0;
  };
  
  struct RebalanceAction {
    PartitionID partition_id;
    NodeID from_node;
    NodeID to_node;
    double expected_improvement = 0.0;
  };
  
  LeaderRebalancer();
  ~LeaderRebalancer();
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // Register a Raft group for monitoring
  void RegisterRaftGroup(PartitionID pid, StorageRaftGroup* group);
  void UnregisterRaftGroup(PartitionID pid);
  
  // Manual trigger
  Status TriggerRebalance();
  
  // Get current distribution
  std::vector<LeaderDistribution> GetCurrentDistribution() const;
  
  // Check if rebalancing is needed
  bool IsRebalancingNeeded() const;
  
  // Calculate imbalance score (0.0 = perfect balance, 1.0 = worst)
  double CalculateImbalanceScore() const;

 private:
  void RebalancerLoop();
  std::vector<RebalanceAction> GenerateRebalancePlan();
  Status ExecuteTransfer(const RebalanceAction& action);
  
  Config config_;
  std::atomic<bool> running_{false};
  
  mutable std::shared_mutex groups_mutex_;
  std::unordered_map<PartitionID, StorageRaftGroup*> raft_groups_;
  
  std::unique_ptr<std::thread> rebalancer_thread_;
  
  std::chrono::system_clock::time_point last_transfer_time_;
  mutable std::mutex transfer_mutex_;
};

// =============================================================================
// Optimized Multi-Raft Manager
// =============================================================================
// Combines all Multi-Raft optimizations into a single manager.

class OptimizedMultiRaftManager : public RaftStorageManager {
 public:
  OptimizedMultiRaftManager();
  ~OptimizedMultiRaftManager();
  
  Status Initialize(const std::string& base_data_dir,
                    const RaftThreadPool::Config& thread_pool_config = {},
                    const BatchHeartbeatManager::Config& heartbeat_config = {},
                    const LeaderRebalancer::Config& rebalancer_config = {});
  
  // Override to use optimized thread pool
  StatusOr<StorageRaftGroup*> CreateRaftGroup(
      const RaftGroupConfig& config,
      StorageRaftGroup::ApplyCallback apply_cb,
      StorageRaftGroup::StateChangeCallback state_cb);
  
  // Get optimization statistics
  struct OptimizationStats {
    RaftThreadPool::Stats thread_pool_stats;
    BatchHeartbeatManager::Stats heartbeat_stats;
    LeaderRebalancer::LeaderDistribution leader_distribution;
    double leader_imbalance_score = 0.0;
  };
  OptimizationStats GetOptimizationStats() const;

 private:
  std::unique_ptr<RaftThreadPool> thread_pool_;
  std::unique_ptr<BatchHeartbeatManager> heartbeat_manager_;
  std::unique_ptr<LeaderRebalancer> leader_rebalancer_;
};

// =============================================================================
// Multi-Raft Performance Tuning Guide
// =============================================================================

/*
 * Recommended configurations based on cluster size:
 *
 * Small Cluster (3 nodes, 10-100 partitions):
 * - Thread Pool: 4-8 threads
 * - Batch Heartbeat: Enabled, 10ms interval
 * - Leader Rebalancing: Enabled
 *
 * Medium Cluster (5-10 nodes, 100-1000 partitions):
 * - Thread Pool: 8-16 threads
 * - Batch Heartbeat: Enabled, 5ms interval
 * - Leader Rebalancing: Enabled, 30s check interval
 *
 * Large Cluster (10+ nodes, 1000+ partitions):
 * - Thread Pool: 16-32 threads
 * - Batch Heartbeat: Enabled, 1ms interval
 * - Leader Rebalancing: Enabled, 10s check interval
 * - Consider: Partition splitting for hot shards
 *
 * Key Metrics to Monitor:
 * 1. Average batch size (heartbeat efficiency)
 * 2. Thread pool queue depth (backpressure)
 * 3. Leader distribution stddev (balance)
 * 4. Rebalance frequency (stability)
 */

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_MULTI_RAFT_OPTIMIZATION_H_
