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
#include "cedar/dtx/monitoring.h"
#include "cedar/raft/partition_router.h"

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
  
  // Create ONE shared CedarGraphStorage (LSM-Tree) for ALL partitions
  // This is the key difference from physical isolation design
  CedarOptions options;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.memtable_threshold = 256 * 1024;  // 256KB for faster flush during tests
  options.write_buffer_size = 256 * 1024;
  options.enable_wal = true;
  options.enable_accumulated_flush = false;  // Disable accumulated flush for immediate persistence
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, config_.data_root, &storage);
  if (!s.ok()) {
    return Status::IOError("Failed to open shared storage: " + s.ToString());
  }
  
  shared_storage_.reset(storage);
  
  // Initialize partition router (required for CedarGraphStorage::Put)
  raft::PartitionRouterConfig router_config;
  router_config.default_replica_count = 1;  // Single-node mode for tests / embedded usage
  s = shared_storage_->InitializePartitionRouter(router_config);
  if (!s.ok()) {
    return Status::IOError("Failed to initialize partition router: " + s.ToString());
  }
  
  // Register a default local node so that partition routing can auto-create groups
  s = shared_storage_->RegisterPartitionNode("node1", "127.0.0.1", 0, "");
  if (!s.ok()) {
    return Status::IOError("Failed to register default partition node: " + s.ToString());
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
  
  // Close shared storage
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
  auto storage = std::make_unique<PartitionStorage>(pid, shared_storage_.get(), this);
  
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
  
  if (!shared_storage_) {
    return Status::IOError("Storage not initialized");
  }
  
  // Flush shared storage (affects all partitions)
  auto status = shared_storage_->ForceFlush();
  if (!status.ok()) {
    return Status::IOError("Flush failed: " + status.ToString());
  }
  
  LOG_INFO("PartitionManager", "FlushAll completed for " + std::to_string(partitions_.size()) + " partitions");
  
  return Status::OK();
}

Status StoragePartitionManager::CompactPartition(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (!shared_storage_) {
    return Status::IOError("Storage not initialized");
  }
  
  if (partitions_.find(pid) == partitions_.end()) {
    return Status::NotFound("Partition not found");
  }
  
  // In shared LSM-Tree, we trigger compaction on the entire storage
  // The partition ID is encoded in the key prefix
  auto status = shared_storage_->Compact();
  if (!status.ok()) {
    return Status::IOError("Compact failed: " + status.ToString());
  }
  
  LOG_INFO("PartitionManager", "CompactPartition completed for partition " + std::to_string(pid));
  
  return Status::OK();
}

Status StoragePartitionManager::CompactAll() {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  
  if (!shared_storage_) {
    return Status::IOError("Storage not initialized");
  }
  
  // Compact entire shared storage
  auto status = shared_storage_->Compact();
  if (!status.ok()) {
    return Status::IOError("Compact failed: " + status.ToString());
  }
  
  LOG_INFO("PartitionManager", "CompactAll completed for " + std::to_string(partitions_.size()) + " partitions");
  
  return Status::OK();
}

}  // namespace dtx
}  // namespace cedar
