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

#ifndef CEDAR_RAFT_PARTITION_ROUTER_H_
#define CEDAR_RAFT_PARTITION_ROUTER_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/raft/partition_raft_manager.h"
#include "cedar/storage/storage_health_monitor.h"

namespace cedar {
namespace raft {

// =============================================================================
// Partition State
// =============================================================================

enum class PartitionState {
  kUnknown,
  kCreating,
  kActive,      // Leader elected and healthy
  kDegraded,    // Some replicas unhealthy
  kUnavailable  // No leader or majority unhealthy
};

// =============================================================================
// Router Configuration
// =============================================================================

struct PartitionRouterConfig {
  // 默认副本数
  uint32_t default_replica_count = 3;
  
  // 是否启用 follower 读
  bool enable_read_from_follower = true;
  
  // 是否启用粘性会话
  bool enable_sticky_session = true;
  
  // 分区放置策略
  PartitionPlacementStrategy placement_strategy = 
      PartitionPlacementStrategy::kConsistentHash;
  
  // 本地数据中心优先级
  bool local_dc_priority = true;
  std::string local_dc_id = "";
};

// =============================================================================
// Routing Target
// =============================================================================

struct RoutingTarget {
  std::string node_id;
  std::string address;
  uint16_t port = 0;
  PartitionID partition_id = 0;
  bool is_leader = false;
  std::string dc_id;
};

// =============================================================================
// Batch Routing Result
// =============================================================================

struct BatchRouteResult {
  PartitionID partition_id;
  RoutingTarget target;
  std::vector<std::string> keys;
};

// =============================================================================
// Partition Router
// =============================================================================
//
// PartitionRouter is the main entry point for partition-level routing.
// It wraps PartitionRaftManager and provides a simplified API for:
// - Routing single operations to appropriate partitions/nodes
// - Routing batch operations with automatic partitioning
// - Managing partition topology and replica placement

class PartitionRouter {
 public:
  using RouteChangeCallback = std::function<void(
      PartitionID partition_id,
      const std::string& old_node,
      const std::string& new_node)>;
  
  using PartitionChangeCallback = std::function<void(
      PartitionID partition_id,
      PartitionState old_state,
      PartitionState new_state)>;

  PartitionRouter();
  ~PartitionRouter();

  PartitionRouter(const PartitionRouter&) = delete;
  PartitionRouter& operator=(const PartitionRouter&) = delete;
  PartitionRouter(PartitionRouter&&) = delete;
  PartitionRouter& operator=(PartitionRouter&&) = delete;

  // ---------------------------------------------------------------------------
  // Initialization
  // ---------------------------------------------------------------------------

  Status Initialize(
      const PartitionRouterConfig& config,
      std::shared_ptr<storage::StorageHealthMonitor> health_monitor = nullptr);

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  Status Start();
  void Stop();

  // ---------------------------------------------------------------------------
  // Node Management
  // ---------------------------------------------------------------------------

  Status RegisterNode(const std::string& node_id,
                      const std::string& address,
                      uint16_t port,
                      const std::string& dc_id = "");

  Status DeregisterNode(const std::string& node_id);

  // ---------------------------------------------------------------------------
  // Single Operation Routing
  // ---------------------------------------------------------------------------

  // Route a write operation (always goes to leader)
  StatusOr<RoutingTarget> RouteWrite(uint64_t entity_id);
  StatusOr<RoutingTarget> RouteWrite(const std::string& key);

  // Route a read operation (may go to follower)
  StatusOr<RoutingTarget> RouteRead(uint64_t entity_id, 
                                     bool require_leader = false);
  StatusOr<RoutingTarget> RouteRead(const std::string& key,
                                     bool require_leader = false);

  // Route by partition ID directly
  StatusOr<RoutingTarget> RouteWriteByPartition(PartitionID part_id);
  StatusOr<RoutingTarget> RouteReadByPartition(PartitionID part_id,
                                                bool require_leader = false);

  // ---------------------------------------------------------------------------
  // Batch Operation Routing
  // ---------------------------------------------------------------------------

  // Route batch write operations, grouped by partition
  std::vector<BatchRouteResult> RouteBatchWrite(
      const std::vector<std::string>& keys);

  // Route batch read operations, grouped by partition
  std::vector<BatchRouteResult> RouteBatchRead(
      const std::vector<std::string>& keys,
      bool require_leader = false);

  // ---------------------------------------------------------------------------
  // Partition Management
  // ---------------------------------------------------------------------------

  Status CreatePartition(PartitionID part_id,
                         const std::vector<std::string>& replica_nodes);

  Status RemovePartition(PartitionID part_id);

  StatusOr<PartitionInfo> GetPartitionInfo(PartitionID part_id);

  std::vector<PartitionInfo> GetAllPartitions() const;

  // ---------------------------------------------------------------------------
  // Statistics
  // ---------------------------------------------------------------------------

  struct Stats {
    size_t total_partitions = 0;
    size_t healthy_partitions = 0;
    size_t active_partitions = 0;  // With elected leader
    size_t total_nodes = 0;
    size_t healthy_nodes = 0;
  };

  Stats GetStats() const;

  // ---------------------------------------------------------------------------
  // Callbacks
  // ---------------------------------------------------------------------------

  void SetRouteChangeCallback(RouteChangeCallback callback);
  void SetPartitionChangeCallback(PartitionChangeCallback callback);

  // ---------------------------------------------------------------------------
  // Compatibility with FailoverManager API
  // ---------------------------------------------------------------------------

  // Get current leader for the default partition (backward compatibility)
  StatusOr<RoutingTarget> GetLeader() const;

  // Get healthy followers (backward compatibility)
  std::vector<RoutingTarget> GetHealthyFollowers() const;

  // Get any healthy node for read (backward compatibility)
  StatusOr<RoutingTarget> GetNodeForRead();

  // Get leader node for write (backward compatibility)
  StatusOr<RoutingTarget> GetNodeForWrite();

 private:
  void OnPartitionLeaderChanged(PartitionID part_id,
                                const std::string& old_leader,
                                const std::string& new_leader);

  RoutingTarget BuildRoutingTarget(const std::string& node_id,
                                   PartitionID part_id,
                                   bool is_leader) const;

  PartitionRouterConfig config_;
  std::unique_ptr<PartitionRaftManager> raft_manager_;
  std::shared_ptr<storage::StorageHealthMonitor> health_monitor_;

  std::atomic<bool> running_{false};
  
  mutable std::mutex callback_mutex_;
  RouteChangeCallback route_change_callback_;
  PartitionChangeCallback partition_change_callback_;

  // Node address cache
  mutable std::mutex address_mutex_;
  std::unordered_map<std::string, std::pair<std::string, uint16_t>> node_addresses_;
  std::unordered_map<std::string, std::string> node_dc_ids_;
};

}  // namespace raft
}  // namespace cedar

#endif  // CEDAR_RAFT_PARTITION_ROUTER_H_
