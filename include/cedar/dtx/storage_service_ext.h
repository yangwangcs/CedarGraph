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
// StorageService Extensions - Advanced Features for Shared LSM-Tree Architecture
// =============================================================================
// 1. Per-partition WAL for Raft consensus
// 2. Compaction Filter by part_id
// 3. Range Scan with part_id prefix
// 4. Partition statistics via SST metadata
// =============================================================================

#ifndef CEDAR_DTX_STORAGE_SERVICE_EXT_H_
#define CEDAR_DTX_STORAGE_SERVICE_EXT_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/storage_service_impl.h"



// Forward declarations (from cedar namespace, not cedar::dtx)
namespace cedar {
class CedarGraphStorage;
}

namespace cedar {
namespace dtx {

// =============================================================================
// 1. Per-Partition WAL for Raft Consensus
// =============================================================================

// WAL entry types
enum class WalEntryType : uint8_t {
  kData = 0,        // Normal data write
  kPrepare = 1,     // 2PC Prepare
  kCommit = 2,      // 2PC Commit
  kAbort = 3,       // 2PC Abort
  kMetadata = 4,    // Metadata update
};

// WAL entry header (binary format)
struct PartitionWalEntry {
  uint32_t crc32;           // CRC32 checksum
  WalEntryType type;        // Entry type
  uint16_t partition_id;    // Partition ID
  uint64_t sequence;        // Monotonic sequence number
  uint64_t timestamp;       // Wall clock time
  uint32_t data_len;        // Payload length
  // Followed by: data[payload_len]
  
  static constexpr size_t kHeaderSize = 27;  // 4+1+2+8+8+4 = Fixed header size
};

// Per-partition WAL writer
class PartitionWalWriter {
 public:
  explicit PartitionWalWriter(const std::string& wal_dir);
  ~PartitionWalWriter();

  // Initialize WAL for partition
  Status Init(PartitionID pid);
  
  // Append entry to WAL
  Status Append(PartitionID pid, WalEntryType type, 
                const std::string& data, uint64_t* sequence);
  
  // Sync WAL to disk
  Status Sync(PartitionID pid);
  
  // Read entries from WAL (for recovery/replay)
  Status ReadEntries(PartitionID pid, uint64_t start_seq,
                     std::vector<PartitionWalEntry>* entries,
                     std::vector<std::string>* data);
  
  // Get current sequence number
  uint64_t GetCurrentSequence(PartitionID pid) const;
  
  // Truncate WAL before sequence (garbage collection)
  Status TruncateBefore(PartitionID pid, uint64_t sequence);
  
  // Close WAL for partition
  Status Close(PartitionID pid);

 private:
  struct WalFile {
    int fd = -1;
    uint64_t current_offset = 0;
    uint64_t next_sequence = 1;
    std::string filepath;
  };
  
  std::string wal_dir_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<PartitionID, WalFile> wal_files_;
};

// =============================================================================
// 2. Compaction Filter by part_id
// =============================================================================

// Callback for filtering entries during compaction
typedef std::function<bool(const CedarKey& key, const Descriptor& value)> 
    CompactionFilterCallback;

// Partition-based compaction filter
class PartitionCompactionFilter {
 public:
  explicit PartitionCompactionFilter(void* manager);  // Opaque pointer to avoid circular dependency
  
  // Set active partitions (entries not in these partitions will be filtered out)
  void SetActivePartitions(const std::unordered_set<PartitionID>& active_pids);
  
  // Add single active partition
  void AddActivePartition(PartitionID pid);
  
  // Remove partition from active set
  void RemoveActivePartition(PartitionID pid);
  
  // Clear all active partitions
  void ClearActivePartitions();
  
  // Check if partition is active
  bool IsPartitionActive(PartitionID pid) const;
  
  // Filter function (called during compaction)
  bool ShouldKeep(const CedarKey& key, const Descriptor& value) const;
  
  // Get active partition count
  size_t GetActivePartitionCount() const;

 private:
  void* manager_;  // Opaque pointer to PartitionManager
  mutable std::shared_mutex mutex_;
  std::unordered_set<PartitionID> active_partitions_;
};

// =============================================================================
// 3. Range Scan with part_id prefix
// =============================================================================

// Scan options for partition-aware range queries
struct PartitionScanOptions {
  PartitionID partition_id;           // Target partition
  uint64_t start_entity_id = 0;       // Start entity (0 = no lower bound)
  uint64_t end_entity_id = UINT64_MAX; // End entity (max = no upper bound)
  Timestamp start_time;               // Start timestamp
  Timestamp end_time;                 // End timestamp
  uint16_t column_id = 0xFFFF;        // 0xFFFF = all columns
  EntityType entity_type = static_cast<EntityType>(0xFF);  // 0xFF = all types
  size_t limit = SIZE_MAX;            // Max results
};

// Partition-aware scanner
class PartitionRangeScanner {
 public:
  explicit PartitionRangeScanner(::cedar::CedarGraphStorage* storage);
  
  // Scan range within partition
  std::vector<std::pair<CedarKey, Descriptor>> Scan(
      const PartitionScanOptions& options);
  
  // Scan with callback (streaming)
  void ScanWithCallback(const PartitionScanOptions& options,
                        std::function<bool(const CedarKey&, const Descriptor&)> callback);
  
  // Get key bounds for partition (for constructing scan ranges)
  static std::pair<CedarKey, CedarKey> GetPartitionKeyBounds(PartitionID pid);
  
  // Access underlying storage
  ::cedar::CedarGraphStorage* GetStorage() const { return storage_; }
  
  // Check if key belongs to partition
  static bool KeyBelongsToPartition(const CedarKey& key, PartitionID pid);

 private:
  ::cedar::CedarGraphStorage* storage_;
};

// =============================================================================
// 4. Partition Statistics via SST Metadata
// =============================================================================

// Per-partition statistics from SST files
struct PartitionSstStats {
  PartitionID partition_id;
  
  // Key statistics
  uint64_t total_keys = 0;
  uint64_t total_size_bytes = 0;
  
  // SST file statistics
  size_t sst_file_count = 0;
  uint64_t sst_total_size = 0;
  
  // Key range
  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  Timestamp min_timestamp;
  Timestamp max_timestamp;
  
  // Tombstone statistics
  uint64_t tombstone_count = 0;
  uint64_t tombstone_size_bytes = 0;
  
  // Last update time
  uint64_t last_update_time_ms = 0;
};

// Partition statistics collector
class PartitionStatsCollector {
 public:
  explicit PartitionStatsCollector(::cedar::CedarGraphStorage* storage);
  
  // Collect statistics for specific partition
  PartitionSstStats CollectStats(PartitionID pid);
  
  // Collect statistics for all partitions
  std::unordered_map<PartitionID, PartitionSstStats> CollectAllStats();
  
  // Incrementally update stats (call after writes)
  void UpdateStatsIncremental(PartitionID pid, const CedarKey& key, 
                              const Descriptor& value, bool is_delete);
  
  // Get cached stats (fast, may be stale)
  PartitionSstStats GetCachedStats(PartitionID pid) const;
  
  // Refresh all cached stats
  void RefreshAllStats();
  
  // Estimate partition size (for load balancing)
  uint64_t EstimatePartitionSize(PartitionID pid) const;

 private:
  ::cedar::CedarGraphStorage* storage_;
  mutable std::shared_mutex cache_mutex_;
  std::unordered_map<PartitionID, PartitionSstStats> cached_stats_;
  std::atomic<uint64_t> last_full_scan_time_{0};
};

// =============================================================================
// 5. Partition-Aware Compaction Scheduler
// =============================================================================

// Compaction priority for specific partition
struct PartitionCompactionPriority {
  PartitionID partition_id;
  double priority_score;  // Higher = more urgent
  uint64_t estimated_size_bytes;
  uint64_t tombstone_ratio;  // Percentage (0-100)
};

// Scheduler for partition-aware compaction
class PartitionCompactionScheduler {
 public:
  explicit PartitionCompactionScheduler(void* manager);  // Opaque pointer
  
  // Add partition to compaction queue
  void RequestCompaction(PartitionID pid, double priority);
  
  // Get next partition to compact (highest priority)
  std::optional<PartitionID> GetNextCompactionTarget();
  
  // Mark compaction complete
  void MarkCompactionComplete(PartitionID pid);
  
  // Check if partition needs compaction
  bool NeedsCompaction(PartitionID pid) const;
  
  // Get compaction priorities for all partitions
  std::vector<PartitionCompactionPriority> GetCompactionPriorities();

 private:
  void* manager_;  // Opaque pointer to PartitionManager
  mutable std::mutex mutex_;
  std::unordered_map<PartitionID, double> pending_compactions_;
  std::unordered_set<PartitionID> in_progress_;
};

// =============================================================================
// 6. Integration: Extended PartitionManager with Index
// =============================================================================

// Forward declarations from partition_index.h
class PartitionIndex;
class MemTablePartitionTracker;
class PartitionQueryEngine;

// Extended partition manager with all advanced features
class ExtendedPartitionManager {
 public:
  struct ExtendedConfig {
    StoragePartitionManager::PartitionConfig base_config;
    bool enable_per_partition_wal = true;
    bool enable_compaction_filter = true;
    bool enable_partition_stats = true;
    bool enable_partition_index = true;  // Enable secondary index for partition queries
    std::string wal_dir = "/tmp/cedar_wal";
  };

  ExtendedPartitionManager();
  ~ExtendedPartitionManager();

  Status Initialize(const ExtendedConfig& config);
  void Shutdown();

  // WAL operations
  Status WriteWal(PartitionID pid, WalEntryType type, const std::string& data);
  Status SyncWal(PartitionID pid);
  
  // Compaction filter control
  void SetActivePartitionsForCompaction(const std::vector<PartitionID>& active_pids);
  void EnableCompactionFilter(bool enable);
  
  // Partition statistics (uses index when available)
  PartitionSstStats GetPartitionStats(PartitionID pid);
  std::unordered_map<PartitionID, PartitionSstStats> GetAllPartitionStats();
  
  // Range scan using index
  std::vector<std::pair<CedarKey, Descriptor>> ScanPartitionRange(
      const PartitionScanOptions& options);
  
  // Get partition size estimate (MemTable + SST)
  uint64_t EstimatePartitionSize(PartitionID pid) const;
  uint64_t EstimatePartitionKeyCount(PartitionID pid) const;

  // Accessors for advanced components
  PartitionWalWriter* GetWalWriter() const { return wal_writer_.get(); }
  PartitionCompactionFilter* GetCompactionFilter() const { return compaction_filter_.get(); }
  PartitionStatsCollector* GetStatsCollector() const { return stats_collector_.get(); }
  PartitionRangeScanner* GetRangeScanner() const { return range_scanner_.get(); }
  PartitionIndex* GetPartitionIndex() const { return partition_index_.get(); }
  MemTablePartitionTracker* GetMemTableTracker() const { return memtable_tracker_.get(); }
  PartitionQueryEngine* GetQueryEngine() const { return query_engine_.get(); }
  
  // Delegation to base PartitionManager
  ::cedar::CedarGraphStorage* GetSharedStorage() const { return base_manager_.GetSharedStorage(); }
  Status AddPartition(PartitionID pid) { return base_manager_.AddPartition(pid); }
  Status RemovePartition(PartitionID pid) { return base_manager_.RemovePartition(pid); }
  bool HasPartition(PartitionID pid) const { return base_manager_.HasPartition(pid); }
  std::vector<PartitionID> GetAllPartitions() const { return base_manager_.GetAllPartitions(); }

 private:
  StoragePartitionManager base_manager_;  // Composition instead of inheritance
  ExtendedConfig ext_config_;
  
  // Advanced components
  std::unique_ptr<PartitionWalWriter> wal_writer_;
  std::unique_ptr<PartitionCompactionFilter> compaction_filter_;
  std::unique_ptr<PartitionStatsCollector> stats_collector_;
  std::unique_ptr<PartitionRangeScanner> range_scanner_;
  std::unique_ptr<PartitionCompactionScheduler> compaction_scheduler_;
  
  // Index components for efficient partition queries
  std::unique_ptr<PartitionIndex> partition_index_;
  std::unique_ptr<MemTablePartitionTracker> memtable_tracker_;
  std::unique_ptr<PartitionQueryEngine> query_engine_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_SERVICE_EXT_H_
