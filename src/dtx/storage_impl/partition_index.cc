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

#include "cedar/dtx/partition_index.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <unordered_map>

#include "cedar/common/logging.h"

#include "cedar/storage/lsm_engine.h"
#include "cedar/sst/zone_columnar_reader.h"

namespace cedar {
namespace dtx {

// =============================================================================
// PartitionIndex Implementation
// =============================================================================

PartitionIndex::PartitionIndex(::cedar::CedarGraphStorage* storage)
    : storage_(storage) {}

PartitionIndex::~PartitionIndex() = default;

Status PartitionIndex::BuildIndex() {
  if (!storage_) {
    return Status::IOError("Storage not available for index build");
  }

  auto* lsm_engine = storage_->GetLsmEngine();
  if (!lsm_engine) {
    return Status::IOError("LSM engine not available");
  }

  // Get all SST files across all levels
  const auto& all_levels = lsm_engine->GetSstFiles();
  if (all_levels.empty()) {
    return Status::OK();
  }

  // Collect metadata outside the lock
  std::vector<SSTPartitionMetadata> all_metadata;
  all_metadata.reserve(64);  // rough guess

  for (const auto& level : all_levels) {
    for (const auto& sst_meta : level) {
      auto result = IndexSingleSST(sst_meta);
      if (result.ok()) {
        all_metadata.push_back(std::move(result.ValueOrDie()));
      } else {
        LOG(WARNING) << "[PartitionIndex] Warning: failed to index SST "
                     << sst_meta.file_number << ": " << result.status().ToString();
      }
    }
  }

  // Brief locked section: clear and insert
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    sst_metadata_.clear();
    partition_sst_map_.clear();
    cached_ranges_.clear();
    cache_dirty_ = false;

    for (auto& metadata : all_metadata) {
      uint64_t file_number = metadata.file_number;
      sst_metadata_.try_emplace(file_number, std::move(metadata));
      for (const auto& [pid, range] : sst_metadata_[file_number].partition_ranges) {
        (void)range;
        partition_sst_map_[pid].insert(file_number);
      }
    }
  }

  cache_dirty_.store(true);
  return Status::OK();
}

StatusOr<SSTPartitionMetadata> PartitionIndex::IndexSingleSST(
    const cedar::SSTFileMeta& sst_meta) {
  std::string file_path = sst_meta.file_name();
  try {
    if (!std::filesystem::exists(file_path)) {
      auto* lsm_engine = storage_->GetLsmEngine();
      std::string db_path = lsm_engine ? lsm_engine->GetDbPath() : ".";
      file_path = (std::filesystem::path(db_path) / sst_meta.file_name()).string();
    }
  } catch (const std::filesystem::filesystem_error& e) {
    return Status::IOError("Filesystem error checking SST path: " + std::string(e.what()));
  }

  cedar::SstReader reader(file_path);
  auto s = reader.Open();
  if (!s.ok()) {
    return Status::IOError("Failed to open SST: " + file_path + " - " + s.ToString());
  }

  std::unordered_map<PartitionID, PartitionEntityRange> local_ranges;

  cedar::ReadPredicate predicate;
  reader.Scan(predicate,
              [&](const cedar::CedarKey& key, const cedar::Descriptor& desc) {
                (void)desc;
                PartitionID pid = key.part_id();
                uint64_t entity_id = key.entity_id();

                auto it = local_ranges.find(pid);
                if (it == local_ranges.end()) {
                  PartitionEntityRange range;
                  range.partition_id = pid;
                  range.min_entity_id = entity_id;
                  range.max_entity_id = entity_id;
                  range.estimated_key_count = 1;
                  local_ranges.emplace(pid, range);
                } else {
                  it->second.min_entity_id = std::min(it->second.min_entity_id, entity_id);
                  it->second.max_entity_id = std::max(it->second.max_entity_id, entity_id);
                  it->second.estimated_key_count++;
                }
              });

  reader.Close();

  SSTPartitionMetadata metadata;
  metadata.file_number = sst_meta.file_number;
  metadata.file_size = sst_meta.file_size;
  metadata.partition_ranges = std::move(local_ranges);

  return metadata;
}

Status PartitionIndex::IndexSSTFile(uint64_t file_number) {
  cedar::SSTFileMeta sst_meta;
  sst_meta.file_number = file_number;
  auto result = IndexSingleSST(sst_meta);
  if (result.ok()) {
    return AddSST(file_number, result.ValueOrDie());
  }
  return result.status();
}

Status PartitionIndex::AddSST(uint64_t file_number, 
                              const SSTPartitionMetadata& metadata) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  sst_metadata_[file_number] = metadata;
  
  // Update inverted index
  for (const auto& [pid, range] : metadata.partition_ranges) {
    partition_sst_map_[pid].insert(file_number);
  }
  
  InvalidateCache();
  return Status::OK();
}

Status PartitionIndex::RemoveSST(uint64_t file_number) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  auto it = sst_metadata_.find(file_number);
  if (it == sst_metadata_.end()) {
    return Status::NotFound("SST file not in index");
  }
  
  // Remove from inverted index
  for (const auto& [pid, _] : it->second.partition_ranges) {
    auto pid_it = partition_sst_map_.find(pid);
    if (pid_it != partition_sst_map_.end()) {
      pid_it->second.erase(file_number);
      if (pid_it->second.empty()) {
        partition_sst_map_.erase(pid_it);
      }
    }
  }
  
  sst_metadata_.erase(it);
  InvalidateCache();
  
  return Status::OK();
}

std::vector<uint64_t> PartitionIndex::GetSSTFilesForPartition(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = partition_sst_map_.find(pid);
  if (it != partition_sst_map_.end()) {
    return std::vector<uint64_t>(it->second.begin(), it->second.end());
  }
  
  return {};
}

PartitionEntityRange PartitionIndex::GetPartitionRange(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  if (cache_dirty_.load()) {
    lock.unlock();
    RebuildCache();
    lock.lock();
  }
  
  auto it = cached_ranges_.find(pid);
  if (it != cached_ranges_.end()) {
    return it->second;
  }
  
  return PartitionEntityRange{};
}

std::vector<PartitionID> PartitionIndex::GetAllPartitions() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<PartitionID> partitions;
  partitions.reserve(partition_sst_map_.size());
  
  for (const auto& [pid, _] : partition_sst_map_) {
    partitions.push_back(pid);
  }
  
  return partitions;
}

PartitionIndex::PartitionStats PartitionIndex::GetPartitionStats(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  PartitionStats stats;
  
  auto it = partition_sst_map_.find(pid);
  if (it != partition_sst_map_.end()) {
    stats.sst_file_count = it->second.size();
    
    for (uint64_t file_number : it->second) {
      auto meta_it = sst_metadata_.find(file_number);
      if (meta_it != sst_metadata_.end()) {
        stats.total_sst_size += meta_it->second.file_size;
        
        auto range = meta_it->second.GetRange(pid);
        if (range) {
          stats.estimated_key_count += range->estimated_key_count;
        }
      }
    }
  }
  
  return stats;
}

bool PartitionIndex::NeedsRebuild() const {
  return cache_dirty_.load();
}

void PartitionIndex::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  sst_metadata_.clear();
  partition_sst_map_.clear();
  cached_ranges_.clear();
  cache_dirty_ = false;
}

void PartitionIndex::InvalidateCache() {
  cache_dirty_ = true;
}

void PartitionIndex::RebuildCache() const {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  if (!cache_dirty_.load()) {
    return;
  }
  
  cached_ranges_.clear();
  
  // Merge ranges from all SSTs per partition
  for (const auto& [pid, file_set] : partition_sst_map_) {
    PartitionEntityRange merged_range;
    merged_range.partition_id = pid;
    
    for (uint64_t file_number : file_set) {
      auto meta_it = sst_metadata_.find(file_number);
      if (meta_it != sst_metadata_.end()) {
        auto range = meta_it->second.GetRange(pid);
        if (range) {
          merged_range.min_entity_id = std::min(
              merged_range.min_entity_id, range->min_entity_id);
          merged_range.max_entity_id = std::max(
              merged_range.max_entity_id, range->max_entity_id);
          merged_range.estimated_key_count += range->estimated_key_count;
        }
      }
    }
    
    cached_ranges_[pid] = merged_range;
  }
  
  cache_dirty_ = false;
}

// =============================================================================
// MemTablePartitionTracker Implementation
// =============================================================================

void MemTablePartitionTracker::TrackWrite(PartitionID pid, 
                                          uint64_t entity_id, 
                                          size_t key_size) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  auto& stats = stats_[pid];
  stats.key_count++;
  stats.total_size += key_size;
  stats.min_entity_id = std::min(stats.min_entity_id, entity_id);
  stats.max_entity_id = std::max(stats.max_entity_id, entity_id);
}

void MemTablePartitionTracker::TrackDelete(PartitionID pid) {
  // Deletions are tracked in MemTable, stats updated during flush
  (void)pid;
}

MemTablePartitionTracker::MemTableStats 
MemTablePartitionTracker::GetStats(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = stats_.find(pid);
  if (it != stats_.end()) {
    return it->second;
  }
  
  return MemTableStats{};
}

std::vector<PartitionID> MemTablePartitionTracker::GetActivePartitions() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<PartitionID> partitions;
  partitions.reserve(stats_.size());
  
  for (const auto& [pid, _] : stats_) {
    partitions.push_back(pid);
  }
  
  return partitions;
}

void MemTablePartitionTracker::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  stats_.clear();
}

void MemTablePartitionTracker::ClearPartition(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  stats_.erase(pid);
}

// =============================================================================
// PartitionQueryEngine Implementation
// =============================================================================

PartitionQueryEngine::PartitionQueryEngine(::cedar::CedarGraphStorage* storage,
                                           PartitionIndex* index,
                                           MemTablePartitionTracker* tracker)
    : storage_(storage),
      index_(index),
      tracker_(tracker) {}

std::vector<CedarKey> PartitionQueryEngine::ScanPartitionKeys(
    PartitionID pid,
    uint64_t start_entity,
    uint64_t end_entity) {
  std::vector<CedarKey> results;
  
  // 1. Get MemTable data (real-time)
  auto memtable_stats = tracker_->GetStats(pid);
  if (memtable_stats.key_count > 0) {
    // MemTable scan requires MemTable iterator access.
    // This requires iterating MemTable and filtering by part_id
  }
  
  // 2. Get SST data (via index)
  auto sst_files = index_->GetSSTFilesForPartition(pid);
  for (uint64_t file_number : sst_files) {
    auto range = index_->GetPartitionRange(pid);
    if (range.Contains(start_entity) || range.Contains(end_entity) ||
        (start_entity <= range.min_entity_id && end_entity >= range.max_entity_id)) {
      // SST file scan requires SST iterator integration.
      // This requires SST reader to support filtering by part_id
    }
  }
  
  return results;
}

uint64_t PartitionQueryEngine::EstimatePartitionSize(PartitionID pid) const {
  uint64_t total_size = 0;
  
  // SST size
  auto sst_stats = index_->GetPartitionStats(pid);
  total_size += sst_stats.total_sst_size;
  
  // MemTable size
  auto memtable_stats = tracker_->GetStats(pid);
  total_size += memtable_stats.total_size;
  
  return total_size;
}

bool PartitionQueryEngine::IsPartitionEmpty(PartitionID pid) const {
  auto sst_stats = index_->GetPartitionStats(pid);
  auto memtable_stats = tracker_->GetStats(pid);
  
  return sst_stats.estimated_key_count == 0 && memtable_stats.key_count == 0;
}

uint64_t PartitionQueryEngine::EstimateKeyCount(PartitionID pid) const {
  auto sst_stats = index_->GetPartitionStats(pid);
  auto memtable_stats = tracker_->GetStats(pid);
  
  return sst_stats.estimated_key_count + memtable_stats.key_count;
}

}  // namespace dtx
}  // namespace cedar
