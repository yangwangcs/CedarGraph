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
// Storage Service - Distributed Storage Node (StorageD)
// =============================================================================

#ifndef CEDAR_DTX_STORAGE_STORAGE_SERVICE_H_
#define CEDAR_DTX_STORAGE_STORAGE_SERVICE_H_

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <shared_mutex>
#include <thread>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/chaos_testing.h"
#include "cedar/storage/cedar_graph_storage.h"

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// Storage Node Configuration
// =============================================================================

struct StorageNodeConfig {
  NodeID node_id{0};
  std::string bind_address{"0.0.0.0:7000"};
  std::string data_dir{"/data/cedar/storage"};
  
  // MetaD connection
  std::vector<std::pair<uint32_t, std::string>> metad_endpoints;
  
  // Capacity limits
  uint64_t max_storage_bytes{100ULL * 1024 * 1024 * 1024};  // 100GB
  uint64_t max_partitions{1000};
  
  // Performance tuning
  uint32_t io_threads{4};
  uint32_t worker_threads{8};
  
  // Automated recovery settings
  bool enable_auto_recovery{true};
  std::chrono::seconds health_check_interval{30};
  std::chrono::seconds recovery_timeout{300};
  uint32_t max_recovery_attempts{3};
};

// =============================================================================
// Partition Storage Handle
// =============================================================================

class PartitionStorage {
 public:
  PartitionStorage(PartitionID id, const std::string& space_name);
  ~PartitionStorage();
  
  // Initialize storage for this partition
  Status Initialize(const std::string& data_dir);
  
  // Shutdown
  Status Shutdown();
  
  // Get underlying CedarGraphStorage
  CedarGraphStorage* GetStorage() { return storage_.get(); }
  
  // Partition info
  PartitionID GetPartitionId() const { return partition_id_; }
  std::string GetSpaceName() const { return space_name_; }
  bool IsLeader() const { return is_leader_.load(); }
  void SetLeader(bool leader) { is_leader_.store(leader); }
  
  // Statistics
  uint64_t GetDataSize() const;
  uint64_t GetKeyCount() const;

 private:
  PartitionID partition_id_;
  std::string space_name_;
  std::string data_dir_;
  
  std::unique_ptr<CedarGraphStorage> storage_;
  std::atomic<bool> is_leader_{false};
  std::atomic<bool> running_{false};
};

// =============================================================================
// Storage Service - manages multiple partitions on this node
// =============================================================================

class StorageService {
 public:
  explicit StorageService(const StorageNodeConfig& config);
  ~StorageService();
  
  // Lifecycle
  Status Initialize();
  Status Start();
  Status Shutdown();
  
  // Partition management (called by MetaD)
  Status CreatePartition(const std::string& space_name, 
                         PartitionID partition_id,
                         bool as_leader);
  Status DropPartition(const std::string& space_name,
                       PartitionID partition_id);
  Status SetPartitionLeader(const std::string& space_name,
                            PartitionID partition_id,
                            bool is_leader);
  
  // Get partition handle
  StatusOr<PartitionStorage*> GetPartition(const std::string& space_name,
                                           PartitionID partition_id);
  
  // List all partitions on this node
  std::vector<std::pair<std::string, PartitionID>> ListPartitions() const;
  
  // Node info
  NodeID GetNodeId() const { return config_.node_id; }
  std::string GetNodeAddress() const { return config_.bind_address; }
  
  // Heartbeat to MetaD
  Status SendHeartbeat();
  
  // Statistics
  struct Stats {
    uint64_t total_partitions{0};
    uint64_t leader_partitions{0};
    uint64_t follower_partitions{0};
    uint64_t total_data_size{0};
    uint64_t total_keys{0};
    double cpu_usage{0.0};
    double memory_usage{0.0};
  };
  Stats GetStats() const;
  
  // Automated Recovery
  void EnableAutoRecovery(bool enabled);
  bool IsAutoRecoveryEnabled() const;
  Status TriggerRecovery(AutomatedRecoveryManager::FailureType type, 
                         const std::string& details);
  std::vector<std::pair<AutomatedRecoveryManager::FailureEvent, Status>> 
      GetRecoveryHistory() const;

 private:
  // Connect to MetaD
  Status ConnectToMetaD();
  
  // Background tasks
  void HeartbeatLoop();
  void StatsCollectionLoop();
  void HealthCheckLoop();  // New: automated health checking
  void RecoveryLoop();     // New: automated recovery worker
  
  // Health checks
  Status CheckDiskHealth();
  Status CheckMemoryHealth();
  Status CheckNetworkHealth();
  Status CheckRaftHealth();
  
  StorageNodeConfig config_;
  
  // Partition map: space_name:partition_id -> PartitionStorage
  mutable std::shared_mutex partitions_mutex_;
  std::unordered_map<std::string, std::unique_ptr<PartitionStorage>> partitions_;
  
  // MetaD client
  std::unique_ptr<grpc::Channel> metad_channel_;
  
  // Background threads
  std::atomic<bool> running_{false};
  std::thread heartbeat_thread_;
  std::thread stats_thread_;
  
  // Automated recovery
  std::unique_ptr<AutomatedRecoveryManager> recovery_manager_;
  std::thread health_check_thread_;
  std::thread recovery_worker_thread_;
  std::atomic<bool> recovery_enabled_{true};
  
  // Node addresses for peer monitoring
  std::vector<std::string> peer_addresses_;
  mutable std::mutex peer_mutex_;
};

// =============================================================================
// Data Router - routes requests to correct partition
// =============================================================================

class DataRouter {
 public:
  // Route key to partition
  static PartitionID RouteKey(const CedarKey& key, uint32_t num_partitions);
  
  // Route by partition ID directly
  static StatusOr<NodeID> GetPartitionLeader(
      const std::string& space_name,
      PartitionID partition_id,
      const std::vector<std::pair<uint32_t, std::string>>& metad_endpoints);
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_STORAGE_SERVICE_H_
