// Copyright 2025 The Cedar Authors
//
// StorageBackend implementations: SharedStorageBackend + PartitionedStorageBackend

#include "cedar/storage/storage_backend.h"

#include <filesystem>
#include <chrono>

#include "cedar/common/logging.h"

namespace cedar {
namespace dtx {

// =============================================================================
// SharedStorageBackend
// =============================================================================

Status SharedStorageBackend::Open(const CedarOptions& options, const std::string& data_root) {
  if (initialized_.load()) return Status::OK();

  data_root_ = data_root;
  CedarGraphStorage* storage_ptr = nullptr;
  Status s = CedarGraphStorage::Open(options, data_root, &storage_ptr);
  if (!s.ok()) return s;
  storage_.reset(storage_ptr);
  initialized_.store(true);
  return Status::OK();
}

void SharedStorageBackend::Shutdown() {
  if (!initialized_.load()) return;
  initialized_.store(false);
  storage_.reset();
}

CedarGraphStorage* SharedStorageBackend::GetStorageForPartition(PartitionID /*pid*/) {
  return storage_.get();
}

void SharedStorageBackend::ReleasePartition(PartitionID /*pid*/) {
  // No-op for shared mode
}

Status SharedStorageBackend::FlushAll() {
  if (!storage_) return Status::InvalidArgument("SharedStorageBackend", "not opened");
  return storage_->ForceFlush();
}

Status SharedStorageBackend::CompactPartition(PartitionID /*pid*/) {
  if (!storage_) return Status::InvalidArgument("SharedStorageBackend", "not opened");
  return storage_->Compact();
}

void SharedStorageBackend::PauseCompaction(PartitionID /*pid*/) {
  if (storage_) storage_->PauseCompaction();
}

void SharedStorageBackend::ResumeCompaction(PartitionID /*pid*/) {
  if (storage_) storage_->ResumeCompaction();
}

Status SharedStorageBackend::SaveSnapshot(PartitionID /*pid*/, const std::string& snapshot_path) {
  if (!storage_) return Status::InvalidArgument("SharedStorageBackend", "not opened");

  std::filesystem::create_directories(snapshot_path);
  std::string data_root = storage_->GetDbPath();

  storage_->PauseCompaction();
  storage_->ForceFlush();

  Status result = Status::OK();
  try {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(data_root)) {
      if (entry.is_regular_file()) {
        std::string relative = std::filesystem::relative(entry.path(), data_root).string();
        std::string dst = snapshot_path + "/" + relative;
        std::filesystem::create_directories(std::filesystem::path(dst).parent_path());
        std::filesystem::copy_file(entry.path(), dst,
                                   std::filesystem::copy_options::overwrite_existing);
      }
    }
  } catch (const std::exception& e) {
    result = Status::IOError("SharedStorageBackend", std::string("Snapshot save failed: ") + e.what());
  }

  storage_->ResumeCompaction();
  return result;
}

Status SharedStorageBackend::LoadSnapshot(PartitionID /*pid*/, const std::string& snapshot_path) {
  if (!storage_) return Status::InvalidArgument("SharedStorageBackend", "not opened");
  return storage_->RestoreFromSnapshot(snapshot_path);
}

std::string SharedStorageBackend::GetDataRoot(PartitionID /*pid*/) const {
  return data_root_;
}

size_t SharedStorageBackend::GetTotalDiskUsage() const {
  if (!storage_) return 0;
  size_t total = 0;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(data_root_)) {
    if (entry.is_regular_file()) {
      total += entry.file_size();
    }
  }
  return total;
}

bool SharedStorageBackend::IsInitialized() const {
  return initialized_.load();
}

// =============================================================================
// PartitionedStorageBackend
// =============================================================================

PartitionedStorageBackend::PartitionedStorageBackend(const Config& config)
    : config_(config) {}

Status PartitionedStorageBackend::Open(const CedarOptions& options, const std::string& data_root) {
  if (initialized_.load()) return Status::OK();

  base_data_root_ = data_root;
  base_options_ = options;

  // Override per-partition settings
  base_options_.memtable_threshold = config_.per_partition_memtable_mb * 1024 * 1024;

  // Create base directory
  std::filesystem::create_directories(data_root);

  initialized_.store(true);
  return Status::OK();
}

void PartitionedStorageBackend::Shutdown() {
  if (!initialized_.load()) return;
  initialized_.store(false);

  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [pid, entry] : partitions_) {
    if (entry->storage) {
      entry->storage.reset();
    }
  }
  partitions_.clear();
  lru_list_.clear();
}

CedarGraphStorage* PartitionedStorageBackend::GetStorageForPartition(PartitionID pid) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = partitions_.find(pid);
  if (it != partitions_.end()) {
    // Update LRU
    it->second->last_access_time.store(
        std::chrono::steady_clock::now().time_since_epoch().count());
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second->lru_it);
    return it->second->storage.get();
  }

  // Need to open this partition
  // Check capacity and evict if needed
  if (partitions_.size() >= config_.max_open_partitions && config_.enable_lru_eviction) {
    auto status = EvictLRUPartition();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to evict LRU partition: " << status.ToString();
      return nullptr;
    }
  }

  // Open the partition
  auto entry = std::make_unique<PartitionEntry>();
  std::string part_dir = GetPartitionDataDir(pid);
  std::filesystem::create_directories(part_dir);

  CedarGraphStorage* storage_ptr = nullptr;
  Status s = CedarGraphStorage::Open(base_options_, part_dir, &storage_ptr);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to open partition " << pid << ": " << s.ToString();
    return nullptr;
  }
  entry->storage.reset(storage_ptr);
  entry->last_access_time.store(
      std::chrono::steady_clock::now().time_since_epoch().count());

  lru_list_.push_front(pid);
  entry->lru_it = lru_list_.begin();

  CedarGraphStorage* result = entry->storage.get();
  partitions_[pid] = std::move(entry);
  return result;
}

void PartitionedStorageBackend::ReleasePartition(PartitionID pid) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = partitions_.find(pid);
  if (it != partitions_.end()) {
    lru_list_.erase(it->second->lru_it);
    it->second->storage.reset();
    partitions_.erase(it);
  }
}

Status PartitionedStorageBackend::FlushAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [pid, entry] : partitions_) {
    if (entry->storage) {
      entry->storage->ForceFlush();
    }
  }
  return Status::OK();
}

Status PartitionedStorageBackend::CompactPartition(PartitionID pid) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = partitions_.find(pid);
  if (it == partitions_.end() || !it->second->storage) {
    return Status::NotFound("PartitionedStorageBackend", "partition not open");
  }
  return it->second->storage->Compact();
}

void PartitionedStorageBackend::PauseCompaction(PartitionID pid) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = partitions_.find(pid);
  if (it != partitions_.end() && it->second->storage) {
    it->second->storage->PauseCompaction();
  }
}

void PartitionedStorageBackend::ResumeCompaction(PartitionID pid) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = partitions_.find(pid);
  if (it != partitions_.end() && it->second->storage) {
    it->second->storage->ResumeCompaction();
  }
}

Status PartitionedStorageBackend::SaveSnapshot(PartitionID pid, const std::string& snapshot_path) {
  CedarGraphStorage* storage = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = partitions_.find(pid);
    if (it == partitions_.end() || !it->second->storage) {
      return Status::NotFound("PartitionedStorageBackend", "partition not open");
    }
    storage = it->second->storage.get();
  }

  storage->PauseCompaction();
  storage->ForceFlush();

  std::filesystem::create_directories(snapshot_path);
  std::string data_dir = GetPartitionDataDir(pid);

  Status result = Status::OK();
  try {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir)) {
      if (entry.is_regular_file()) {
        std::string relative = std::filesystem::relative(entry.path(), data_dir).string();
        std::string dst = snapshot_path + "/" + relative;
        std::filesystem::create_directories(std::filesystem::path(dst).parent_path());
        std::filesystem::copy_file(entry.path(), dst,
                                   std::filesystem::copy_options::overwrite_existing);
      }
    }
  } catch (const std::exception& e) {
    result = Status::IOError("PartitionedStorageBackend",
                             std::string("Snapshot save failed: ") + e.what());
  }

  storage->ResumeCompaction();
  return result;
}

Status PartitionedStorageBackend::LoadSnapshot(PartitionID pid, const std::string& snapshot_path) {
  CedarGraphStorage* storage = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = partitions_.find(pid);
    if (it == partitions_.end() || !it->second->storage) {
      // Need to open the partition first
      storage = GetStorageForPartition(pid);
      if (!storage) {
        return Status::IOError("PartitionedStorageBackend", "Failed to open partition");
      }
    } else {
      storage = it->second->storage.get();
    }
  }

  return storage->RestoreFromSnapshot(snapshot_path);
}

std::string PartitionedStorageBackend::GetDataRoot(PartitionID pid) const {
  return GetPartitionDataDir(pid);
}

size_t PartitionedStorageBackend::GetTotalDiskUsage() const {
  size_t total = 0;
  try {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(base_data_root_)) {
      if (entry.is_regular_file()) {
        total += entry.file_size();
      }
    }
  } catch (...) {}
  return total;
}

bool PartitionedStorageBackend::IsInitialized() const {
  return initialized_.load();
}

size_t PartitionedStorageBackend::GetOpenPartitionCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return partitions_.size();
}

Status PartitionedStorageBackend::OpenPartition(PartitionID pid) {
  // Caller must hold mutex_
  auto entry = std::make_unique<PartitionEntry>();
  std::string part_dir = GetPartitionDataDir(pid);
  std::filesystem::create_directories(part_dir);

  CedarGraphStorage* storage_ptr = nullptr;
  Status s = CedarGraphStorage::Open(base_options_, part_dir, &storage_ptr);
  if (!s.ok()) return s;

  entry->storage.reset(storage_ptr);
  entry->last_access_time.store(
      std::chrono::steady_clock::now().time_since_epoch().count());
  lru_list_.push_front(pid);
  entry->lru_it = lru_list_.begin();
  partitions_[pid] = std::move(entry);
  return Status::OK();
}

Status PartitionedStorageBackend::EvictLRUPartition() {
  // Caller must hold mutex_
  if (lru_list_.empty()) {
    return Status::IOError("PartitionedStorageBackend", "No partitions to evict");
  }

  PartitionID evict_pid = lru_list_.back();
  auto it = partitions_.find(evict_pid);
  if (it == partitions_.end()) {
    return Status::IOError("PartitionedStorageBackend", "LRU partition not found");
  }

  // Flush before closing
  if (it->second->storage) {
    it->second->storage->ForceFlush();
    it->second->storage.reset();
  }
  lru_list_.pop_back();
  partitions_.erase(it);

  LOG(INFO) << "Evicted partition " << evict_pid << " (LRU)";
  return Status::OK();
}

std::string PartitionedStorageBackend::GetPartitionDataDir(PartitionID pid) const {
  return base_data_root_ + "/partition_" + std::to_string(pid);
}

}  // namespace dtx
}  // namespace cedar
