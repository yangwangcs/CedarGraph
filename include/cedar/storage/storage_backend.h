// Copyright 2025 The Cedar Authors
//
// StorageBackend - Abstract storage interface for shared and partitioned modes
//
// Two implementations:
// 1. SharedStorageBackend: all partitions share one CedarGraphStorage (current default)
// 2. PartitionedStorageBackend: each partition has its own CedarGraphStorage instance
//

#ifndef CEDAR_STORAGE_STORAGE_BACKEND_H_
#define CEDAR_STORAGE_STORAGE_BACKEND_H_

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <list>
#include <atomic>

#include "cedar/core/status.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// Forward declarations
class PartitionStorage;

// =============================================================================
// StorageBackend - Abstract interface
// =============================================================================
class StorageBackend {
 public:
  virtual ~StorageBackend() = default;

  // Initialize the backend
  virtual Status Open(const CedarOptions& options, const std::string& data_root) = 0;

  // Shutdown the backend
  virtual void Shutdown() = 0;

  // Get or create a storage instance for a partition
  // Shared mode: returns the shared CedarGraphStorage
  // Partitioned mode: returns the per-partition CedarGraphStorage
  virtual CedarGraphStorage* GetStorageForPartition(PartitionID pid) = 0;

  // Release a partition's storage (for LRU eviction in partitioned mode)
  virtual void ReleasePartition(PartitionID pid) = 0;

  // Flush all storage instances
  virtual Status FlushAll() = 0;

  // Compact a specific partition's storage
  virtual Status CompactPartition(PartitionID pid) = 0;

  // Pause/resume compaction (for snapshot safety)
  virtual void PauseCompaction(PartitionID pid) = 0;
  virtual void ResumeCompaction(PartitionID pid) = 0;

  // Snapshot support
  virtual Status SaveSnapshot(PartitionID pid, const std::string& snapshot_path) = 0;
  virtual Status LoadSnapshot(PartitionID pid, const std::string& snapshot_path) = 0;

  // Get data root for a partition
  virtual std::string GetDataRoot(PartitionID pid) const = 0;

  // Get total disk usage
  virtual size_t GetTotalDiskUsage() const = 0;

  // Check if backend is initialized
  virtual bool IsInitialized() const = 0;

  // Get the storage mode name
  virtual const char* ModeName() const = 0;
};

// =============================================================================
// SharedStorageBackend - All partitions share one CedarGraphStorage
// =============================================================================
class SharedStorageBackend : public StorageBackend {
 public:
  SharedStorageBackend() = default;
  ~SharedStorageBackend() override { Shutdown(); }

  Status Open(const CedarOptions& options, const std::string& data_root) override;
  void Shutdown() override;
  CedarGraphStorage* GetStorageForPartition(PartitionID pid) override;
  void ReleasePartition(PartitionID pid) override;
  Status FlushAll() override;
  Status CompactPartition(PartitionID pid) override;
  void PauseCompaction(PartitionID pid) override;
  void ResumeCompaction(PartitionID pid) override;
  Status SaveSnapshot(PartitionID pid, const std::string& snapshot_path) override;
  Status LoadSnapshot(PartitionID pid, const std::string& snapshot_path) override;
  std::string GetDataRoot(PartitionID pid) const override;
  size_t GetTotalDiskUsage() const override;
  bool IsInitialized() const override;
  const char* ModeName() const override { return "shared"; }

 private:
  std::unique_ptr<CedarGraphStorage> storage_;
  std::string data_root_;
  std::atomic<bool> initialized_{false};
};

// =============================================================================
// PartitionedStorageBackend - Each partition has its own CedarGraphStorage
// =============================================================================
class PartitionedStorageBackend : public StorageBackend {
 public:
  struct Config {
    size_t max_open_partitions = 256;       // Max simultaneously open partitions
    size_t per_partition_memtable_mb = 16;  // MemTable size per partition
    size_t shared_block_cache_mb = 512;     // Shared block cache across all partitions
    int compaction_threads_per_partition = 1;
    bool enable_lru_eviction = true;        // Evict inactive partitions
  };

  explicit PartitionedStorageBackend(const Config& config);
  ~PartitionedStorageBackend() override { Shutdown(); }

  Status Open(const CedarOptions& options, const std::string& data_root) override;
  void Shutdown() override;
  CedarGraphStorage* GetStorageForPartition(PartitionID pid) override;
  void ReleasePartition(PartitionID pid) override;
  Status FlushAll() override;
  Status CompactPartition(PartitionID pid) override;
  void PauseCompaction(PartitionID pid) override;
  void ResumeCompaction(PartitionID pid) override;
  Status SaveSnapshot(PartitionID pid, const std::string& snapshot_path) override;
  Status LoadSnapshot(PartitionID pid, const std::string& snapshot_path) override;
  std::string GetDataRoot(PartitionID pid) const override;
  size_t GetTotalDiskUsage() const override;
  bool IsInitialized() const override;
  const char* ModeName() const override { return "partitioned"; }

  // Get the number of currently open partitions
  size_t GetOpenPartitionCount() const;

 private:
  struct PartitionEntry {
    std::unique_ptr<CedarGraphStorage> storage;
    std::list<PartitionID>::iterator lru_it;
    std::atomic<uint64_t> last_access_time{0};
  };

  // Open a partition's storage (creates if needed)
  Status OpenPartition(PartitionID pid);

  // Evict the least recently used partition to make room
  Status EvictLRUPartition();

  // Get partition data directory
  std::string GetPartitionDataDir(PartitionID pid) const;

  Config config_;
  std::string base_data_root_;
  CedarOptions base_options_;
  std::atomic<bool> initialized_{false};

  mutable std::mutex mutex_;
  std::unordered_map<PartitionID, std::unique_ptr<PartitionEntry>> partitions_;
  std::list<PartitionID> lru_list_;  // Front = most recently used
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_STORAGE_STORAGE_BACKEND_H_
