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

#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/failover_manager.h"
#include "cedar/dtx/storage/partition_raft_manager.h"
#include "cedar/dtx/monitoring.h"
#include <filesystem>
#include <fstream>

namespace cedar {
namespace dtx {

StoragePartitionManager::StoragePartitionManager() = default;

StoragePartitionManager::~StoragePartitionManager() {
  Shutdown();
}

Status StoragePartitionManager::Initialize(const PartitionConfig& config) {
  std::unique_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (initialized_.load()) {
    return Status::InvalidArgument("PartitionManager already initialized");
  }
  
  config_ = config;
  
  // Create data root directory
  std::filesystem::create_directories(config_.data_root);
  
  CedarOptions options;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.memtable_threshold = 256 * 1024;  // 256KB for faster flush during tests
  options.write_buffer_size = 256 * 1024;
  options.enable_wal = true;
  options.enable_accumulated_flush = false;
  
  if (config_.storage_mode == "partitioned") {
    // Partitioned mode: each partition gets its own CedarGraphStorage
    PartitionedStorageBackend::Config backend_config;
    backend_config.max_open_partitions = config_.max_open_partitions;
    backend_config.per_partition_memtable_mb = config_.per_partition_memtable_mb;
    backend_config.enable_lru_eviction = config_.enable_lru_eviction;
    
    auto backend = std::make_unique<PartitionedStorageBackend>(backend_config);
    Status s = backend->Open(options, config_.data_root);
    if (!s.ok()) {
      return Status::IOError("Failed to open partitioned backend: " + s.ToString());
    }
    backend_ = std::move(backend);
    LOG(INFO) << "Storage mode: partitioned (max_open=" << config_.max_open_partitions << ")";
  } else {
    // Shared mode: all partitions share one CedarGraphStorage (default)
    auto backend = std::make_unique<SharedStorageBackend>();
    Status s = backend->Open(options, config_.data_root);
    if (!s.ok()) {
      return Status::IOError("Failed to open shared backend: " + s.ToString());
    }
    
    // Also set shared_storage_ for backward compatibility
    CedarGraphStorage* storage = nullptr;
    s = CedarGraphStorage::Open(options, config_.data_root, &storage);
    if (!s.ok()) {
      return Status::IOError("Failed to open shared storage: " + s.ToString());
    }
    shared_storage_.reset(storage);
    backend_ = std::move(backend);
    LOG(INFO) << "Storage mode: shared";
  }
  
  // Register failover handlers if both managers are configured
  if (failover_manager_ && raft_manager_) {
    failover_manager_->RegisterSwitchLeaderHandler(
        [this](PartitionID pid, NodeID target) -> Status {
          auto raft_node = raft_manager_->GetRaftGroup(pid);
          if (!raft_node) return Status::NotFound("Raft group not found");
          return raft_node->TransferLeadershipTo(target);
        });
    failover_manager_->RegisterPromoteReplicaHandler(
        [this](PartitionID pid, NodeID target) -> Status {
          auto raft_node = raft_manager_->GetRaftGroup(pid);
          if (!raft_node) return Status::NotFound("Raft group not found");
          return raft_node->TransferLeadershipTo(target);
        });
  }
  
  initialized_ = true;
  return Status::OK();
}

void StoragePartitionManager::Shutdown() {
  std::unique_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (!initialized_.load()) {
    return;
  }
  
  // Clear all logical partition views
  partitions_.clear();
  
  // Close backend
  if (backend_) {
    backend_->Shutdown();
    backend_.reset();
  }
  
  // Close shared storage (backward compat)
  shared_storage_.reset();
  
  initialized_ = false;
}

PartitionStorage* StoragePartitionManager::GetPartition(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  
  auto it = partitions_.find(pid);
  if (it == partitions_.end()) {
    return nullptr;
  }
  
  return it->second.get();
}

Status StoragePartitionManager::AddPartition(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (!initialized_.load()) {
    return Status::IOError("PartitionManager not initialized");
  }
  
  if (partitions_.find(pid) != partitions_.end()) {
    return Status::InvalidArgument("Partition already exists");
  }
  
  if (partitions_.size() >= config_.max_partitions) {
    return Status::InvalidArgument("Max partition limit reached");
  }
  
  // Create logical partition view - shares the same LSM-Tree
  // No physical directory created, partition_id is encoded in keys
  CedarGraphStorage* shared = shared_storage_ ? shared_storage_.get() : nullptr;
  auto storage = std::make_unique<PartitionStorage>(pid, shared, this, backend_.get());
  
  // Recover any prepared transaction state from WAL
  auto recover_status = storage->RecoverFromWAL();
  if (!recover_status.ok()) {
    std::cerr << "[PartitionManager] WAL recovery warning for partition " << pid
              << ": " << recover_status.ToString() << std::endl;
    // Continue anyway - partition is usable even if WAL recovery fails
  }
  
  partitions_[pid] = std::move(storage);
  return Status::OK();
}

Status StoragePartitionManager::RemovePartition(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(partitions_mutex_);
  
  auto it = partitions_.find(pid);
  if (it == partitions_.end()) {
    return Status::NotFound("Partition not found");
  }
  
  // Note: In shared LSM-Tree, we don't delete the data here
  // Data cleanup would be done via:
  // 1. Compaction filter with partition allowlist
  // 2. Or explicit DeleteRange for the partition's key space
  
  partitions_.erase(it);
  
  return Status::OK();
}

bool StoragePartitionManager::HasPartition(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  return partitions_.find(pid) != partitions_.end();
}

std::vector<PartitionID> StoragePartitionManager::GetAllPartitions() const {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  
  std::vector<PartitionID> result;
  result.reserve(partitions_.size());
  
  for (const auto& [pid, _] : partitions_) {
    result.push_back(pid);
  }
  
  return result;
}

std::vector<PartitionID> StoragePartitionManager::GetLoadedPartitions() const {
  // All logical partitions are "loaded" since they share the same storage
  return GetAllPartitions();
}

size_t StoragePartitionManager::GetPartitionCount() const {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  return partitions_.size();
}

size_t StoragePartitionManager::GetTotalDiskUsage() const {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (!shared_storage_) {
    return 0;
  }
  
  // Get total storage size (shared across all partitions)
  // Return total SST size as approximation
  auto stats = shared_storage_->GetStats();
  return stats.sst_size + stats.memtable_size + stats.imm_memtable_size;
}

Status StoragePartitionManager::FlushAll() {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (!backend_) {
    return Status::IOError("Storage not initialized");
  }
  
  return backend_->FlushAll();
}

Status StoragePartitionManager::CompactPartition(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (!backend_) {
    return Status::IOError("Storage not initialized");
  }
  
  if (partitions_.find(pid) == partitions_.end()) {
    return Status::NotFound("Partition not found");
  }
  
  return backend_->CompactPartition(pid);
}

Status StoragePartitionManager::CompactAll() {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (!backend_) {
    return Status::IOError("Storage not initialized");
  }
  
  return backend_->FlushAll();
}

}  // namespace dtx
}  // namespace cedar
