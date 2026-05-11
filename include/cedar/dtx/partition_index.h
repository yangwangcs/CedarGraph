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
// Partition Index - Efficient partition-aware queries without changing key order
// =============================================================================
// Since part_id is at the suffix of CedarKey (offset 30-31), we cannot do
// efficient prefix range scans. Instead, we maintain a secondary index structure
// that tracks entity ranges within each partition.
//
// Key insight: For partition operations (migration, stats), we typically need:
// 1. List all entities in a partition (for migration)
// 2. Get key count/size estimate per partition (for balancing)
//
// Solution: SST-level metadata indexing + MemTable partition tracking
// =============================================================================

#ifndef CEDAR_DTX_PARTITION_INDEX_H_
#define CEDAR_DTX_PARTITION_INDEX_H_

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/dtx/types.h"



// Forward declaration (global cedar namespace)
namespace cedar {
class CedarGraphStorage;
struct SSTFileMeta;
}

namespace cedar {
namespace dtx {

// =============================================================================
// Partition Entity Range - Tracks entity ID ranges per partition
// =============================================================================
struct PartitionEntityRange {
  PartitionID partition_id;
  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  uint64_t estimated_key_count = 0;
  
  // Check if entity belongs to this range
  bool Contains(uint64_t entity_id) const {
    return entity_id >= min_entity_id && entity_id <= max_entity_id;
  }
};

// =============================================================================
// SST Partition Metadata - Tracks partition distribution in SST files
// =============================================================================
struct SSTPartitionMetadata {
  uint64_t file_number;
  uint64_t file_size;
  
  // Partition -> entity range mapping for this SST
  std::unordered_map<PartitionID, PartitionEntityRange> partition_ranges;
  
  // Quick lookup: does this SST contain data for partition?
  bool ContainsPartition(PartitionID pid) const {
    return partition_ranges.count(pid) > 0;
  }
  
  // Get entity range for partition in this SST
  const PartitionEntityRange* GetRange(PartitionID pid) const {
    auto it = partition_ranges.find(pid);
    if (it != partition_ranges.end()) {
      return &it->second;
    }
    return nullptr;
  }
};

// =============================================================================
// Partition Index - Secondary index for partition-aware operations
// =============================================================================
class PartitionIndex {
 public:
  explicit PartitionIndex(::cedar::CedarGraphStorage* storage);
  ~PartitionIndex();

  // Build index from existing SST files (called on startup)
  Status BuildIndex();
  
  // Update index when new SST is created (Flush/Compaction)
  Status AddSST(uint64_t file_number, const SSTPartitionMetadata& metadata);
  
  // Update index when SST is deleted (Compaction)
  Status RemoveSST(uint64_t file_number);
  
  // Convenience: scan a single SST file and add it to the index
  Status IndexSSTFile(uint64_t file_number);
  
  // Get all SST files that may contain data for partition
  std::vector<uint64_t> GetSSTFilesForPartition(PartitionID pid) const;
  
  // Get entity range for partition (across all SSTs)
  PartitionEntityRange GetPartitionRange(PartitionID pid) const;
  
  // Get all partitions tracked in index
  std::vector<PartitionID> GetAllPartitions() const;
  
  // Get partition statistics
  struct PartitionStats {
    uint64_t estimated_key_count = 0;
    uint64_t total_sst_size = 0;
    size_t sst_file_count = 0;
  };
  PartitionStats GetPartitionStats(PartitionID pid) const;

  // Check if index needs rebuild
  bool NeedsRebuild() const;
  
  // Clear index
  void Clear();

 private:
  ::cedar::CedarGraphStorage* storage_;
  
  mutable std::shared_mutex mutex_;
  
  // SST file metadata
  std::unordered_map<uint64_t, SSTPartitionMetadata> sst_metadata_;
  
  // Partition -> SST files mapping (inverted index)
  std::unordered_map<PartitionID, std::unordered_set<uint64_t>> partition_sst_map_;
  
  // Cached partition ranges (merged across SSTs)
  mutable std::unordered_map<PartitionID, PartitionEntityRange> cached_ranges_;
  mutable std::atomic<bool> cache_dirty_{true};
  
  void InvalidateCache();
  void RebuildCache() const;
  
  StatusOr<SSTPartitionMetadata> IndexSingleSST(const cedar::SSTFileMeta& sst_meta);
};

// =============================================================================
// Partition Scanner - Efficient scan using partition index
// =============================================================================
class PartitionScanner {
 public:
  struct ScanOptions {
    PartitionID partition_id;
    uint64_t start_entity_id = 0;
    uint64_t end_entity_id = UINT64_MAX;
    size_t batch_size = 1000;
  };
  
  explicit PartitionScanner(CedarGraphStorage* storage, 
                            PartitionIndex* index);
  
  // Scan entities in partition
  std::vector<uint64_t> ScanEntities(const ScanOptions& options);
  
  // Scan with callback (streaming)
  void ScanWithCallback(const ScanOptions& options,
                        std::function<bool(uint64_t entity_id)> callback);
  
  // Get all keys for specific entity in partition
  std::vector<CedarKey> GetEntityKeys(PartitionID pid, uint64_t entity_id);

 private:
  CedarGraphStorage* storage_;
  PartitionIndex* index_;
};

// =============================================================================
// MemTable Partition Tracker - Track writes in real-time
// =============================================================================
class MemTablePartitionTracker {
 public:
  void TrackWrite(PartitionID pid, uint64_t entity_id, size_t key_size);
  
  void TrackDelete(PartitionID pid);
  
  // Get partition stats from MemTable
  struct MemTableStats {
    uint64_t key_count = 0;
    uint64_t total_size = 0;
    uint64_t min_entity_id = UINT64_MAX;
    uint64_t max_entity_id = 0;
  };
  MemTableStats GetStats(PartitionID pid) const;
  
  // Get all partitions with data in MemTable
  std::vector<PartitionID> GetActivePartitions() const;
  
  // Clear stats (called after Flush)
  void Clear();
  
  // Clear specific partition stats
  void ClearPartition(PartitionID pid);

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<PartitionID, MemTableStats> stats_;
};

// =============================================================================
// Integrated Partition Query Engine
// =============================================================================
class PartitionQueryEngine {
 public:
  PartitionQueryEngine(::cedar::CedarGraphStorage* storage,
                       PartitionIndex* index,
                       MemTablePartitionTracker* tracker);
  
  // Full partition scan (MemTable + SST)
  std::vector<CedarKey> ScanPartitionKeys(PartitionID pid, 
                                           uint64_t start_entity = 0,
                                           uint64_t end_entity = UINT64_MAX);
  
  // Estimate partition size (MemTable + SST)
  uint64_t EstimatePartitionSize(PartitionID pid) const;
  
  // Check if partition is empty
  bool IsPartitionEmpty(PartitionID pid) const;
  
  // Get partition key count estimate
  uint64_t EstimateKeyCount(PartitionID pid) const;

 private:
  ::cedar::CedarGraphStorage* storage_;
  PartitionIndex* index_;
  MemTablePartitionTracker* tracker_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_PARTITION_INDEX_H_
