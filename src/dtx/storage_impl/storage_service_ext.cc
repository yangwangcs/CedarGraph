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

#include "cedar/dtx/storage_service_ext.h"
#include "cedar/dtx/partition_index.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <unistd.h>
#include <sys/stat.h>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/core/crc32c.h"

namespace cedar {
namespace dtx {

// =============================================================================
// PartitionWalWriter Implementation
// =============================================================================

PartitionWalWriter::PartitionWalWriter(const std::string& wal_dir)
    : wal_dir_(wal_dir) {}

PartitionWalWriter::~PartitionWalWriter() {
  // Close all WAL files
  for (auto& [pid, wal] : wal_files_) {
    if (wal.fd >= 0) {
      ::close(wal.fd);
    }
  }
}

Status PartitionWalWriter::Init(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  if (wal_files_.find(pid) != wal_files_.end()) {
    return Status::OK();  // Already initialized
  }
  
  // Create WAL directory for partition
  std::string pid_wal_dir = wal_dir_ + "/partition_" + std::to_string(pid);
  std::filesystem::create_directories(pid_wal_dir);
  
  WalFile wal;
  wal.filepath = pid_wal_dir + "/wal.log";
  wal.fd = ::open(wal.filepath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (wal.fd < 0) {
    return Status::IOError("Failed to open WAL file: " + wal.filepath);
  }
  
  // Get current file size for offset
  struct stat st;
  if (::fstat(wal.fd, &st) == 0) {
    wal.current_offset = st.st_size;
  }
  
  wal_files_[pid] = std::move(wal);
  return Status::OK();
}

Status PartitionWalWriter::Append(PartitionID pid, WalEntryType type,
                                  const std::string& data, uint64_t* sequence) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  auto it = wal_files_.find(pid);
  if (it == wal_files_.end()) {
    return Status::NotFound("WAL not initialized for partition " + std::to_string(pid));
  }
  
  WalFile& wal = it->second;
  
  // Build entry header
  PartitionWalEntry entry;
  entry.type = type;
  entry.partition_id = pid;
  entry.sequence = wal.next_sequence++;
  entry.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  entry.data_len = data.size();
  
  // Calculate CRC32 over header (without crc32 field) + data
  std::string header_bytes;
  header_bytes.append(reinterpret_cast<const char*>(&entry.type), 1);
  header_bytes.append(reinterpret_cast<const char*>(&entry.partition_id), 2);
  header_bytes.append(reinterpret_cast<const char*>(&entry.sequence), 8);
  header_bytes.append(reinterpret_cast<const char*>(&entry.timestamp), 8);
  header_bytes.append(reinterpret_cast<const char*>(&entry.data_len), 4);
  
  std::string crc_input = header_bytes + data;
  entry.crc32 = crc32c::Value(crc_input.data(), crc_input.size());
  
  // Write to file: [crc32:4][type:1][partition_id:2][sequence:8][timestamp:8][data_len:4][data:N]
  char header_buf[PartitionWalEntry::kHeaderSize];
  size_t offset = 0;
  
  memcpy(header_buf + offset, &entry.crc32, 4); offset += 4;
  memcpy(header_buf + offset, &entry.type, 1); offset += 1;
  memcpy(header_buf + offset, &entry.partition_id, 2); offset += 2;
  memcpy(header_buf + offset, &entry.sequence, 8); offset += 8;
  memcpy(header_buf + offset, &entry.timestamp, 8); offset += 8;
  memcpy(header_buf + offset, &entry.data_len, 4); offset += 4;
  
  if (::write(wal.fd, header_buf, PartitionWalEntry::kHeaderSize) != 
      static_cast<ssize_t>(PartitionWalEntry::kHeaderSize)) {
    return Status::IOError("Failed to write WAL header");
  }
  
  if (!data.empty()) {
    if (::write(wal.fd, data.data(), data.size()) != static_cast<ssize_t>(data.size())) {
      return Status::IOError("Failed to write WAL data");
    }
  }
  
  wal.current_offset += PartitionWalEntry::kHeaderSize + data.size();
  
  if (sequence) {
    *sequence = entry.sequence;
  }
  
  return Status::OK();
}

Status PartitionWalWriter::Sync(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = wal_files_.find(pid);
  if (it == wal_files_.end()) {
    return Status::NotFound("WAL not initialized for partition " + std::to_string(pid));
  }
  
  if (::fsync(it->second.fd) != 0) {
    return Status::IOError("Failed to sync WAL");
  }
  
  return Status::OK();
}

uint64_t PartitionWalWriter::GetCurrentSequence(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = wal_files_.find(pid);
  if (it == wal_files_.end()) {
    return 0;
  }
  
  return it->second.next_sequence - 1;
}

Status PartitionWalWriter::Close(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  auto it = wal_files_.find(pid);
  if (it == wal_files_.end()) {
    return Status::OK();
  }
  
  if (it->second.fd >= 0) {
    ::close(it->second.fd);
  }
  
  wal_files_.erase(it);
  return Status::OK();
}

// =============================================================================
// PartitionCompactionFilter Implementation
// =============================================================================

PartitionCompactionFilter::PartitionCompactionFilter(void* manager)
    : manager_(manager) {}

void PartitionCompactionFilter::SetActivePartitions(
    const std::unordered_set<PartitionID>& active_pids) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  active_partitions_ = active_pids;
}

void PartitionCompactionFilter::AddActivePartition(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  active_partitions_.insert(pid);
}

void PartitionCompactionFilter::RemoveActivePartition(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  active_partitions_.erase(pid);
}

void PartitionCompactionFilter::ClearActivePartitions() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  active_partitions_.clear();
}

bool PartitionCompactionFilter::IsPartitionActive(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return active_partitions_.empty() || active_partitions_.count(pid) > 0;
}

bool PartitionCompactionFilter::ShouldKeep(const CedarKey& key, 
                                           const Descriptor& value) const {
  PartitionID pid = key.part_id();
  return IsPartitionActive(pid);
}

size_t PartitionCompactionFilter::GetActivePartitionCount() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return active_partitions_.size();
}

// =============================================================================
// PartitionRangeScanner Implementation
// =============================================================================

PartitionRangeScanner::PartitionRangeScanner(::cedar::CedarGraphStorage* storage)
    : storage_(storage) {}

std::pair<CedarKey, CedarKey> PartitionRangeScanner::GetPartitionKeyBounds(PartitionID pid) {
  // Construct min and max keys for this partition
  // CedarKey: entity_id(8) | timestamp(8) | target_id(8) | column_id(2) | 
  //           sequence(2) | entity_type(1) | flags(1) | part_id(2)
  
  CedarKey min_key;
  min_key.SetPartId(pid);
  // All other fields are 0 (minimum)
  
  CedarKey max_key;
  max_key.SetPartId(pid);
  max_key.SetEntityId(UINT64_MAX);
  max_key.SetTimestamp(Timestamp::Max());
  max_key.SetTargetId(UINT64_MAX);
  max_key.SetColumnId(UINT16_MAX);
  // sequence, entity_type, flags are at their max
  
  return {min_key, max_key};
}

bool PartitionRangeScanner::KeyBelongsToPartition(const CedarKey& key, PartitionID pid) {
  return key.part_id() == pid;
}

std::vector<std::pair<CedarKey, Descriptor>> PartitionRangeScanner::Scan(
    const PartitionScanOptions& options) {
  std::vector<std::pair<CedarKey, Descriptor>> results;
  
  ScanWithCallback(options, [&](const CedarKey& key, const Descriptor& value) {
    results.emplace_back(key, value);
    return results.size() < options.limit;  // Continue if under limit
  });
  
  return results;
}

void PartitionRangeScanner::ScanWithCallback(
    const PartitionScanOptions& options,
    std::function<bool(const CedarKey&, const Descriptor&)> callback) {
  // Simplified implementation: iterate entity IDs in range and scan each.
  // For production, this should use a low-level storage iterator.
  size_t count = 0;
  for (uint64_t eid = options.start_entity_id;
       eid <= options.end_entity_id && count < options.limit;
       ++eid) {
    auto versions = storage_->Scan(eid, options.start_time, options.end_time);
    for (const auto& [ts, desc] : versions) {
      CedarKey key;
      key.SetPartId(options.partition_id);
      key.SetEntityId(eid);
      key.SetTimestamp(ts);
      key.SetTargetId(0);
      key.SetColumnId(options.column_id == 0xFFFF ? 0 : options.column_id);
      
      if (!callback(key, desc)) {
        return;
      }
      if (++count >= options.limit) {
        return;
      }
    }
  }
}

// =============================================================================
// PartitionStatsCollector Implementation
// =============================================================================

PartitionStatsCollector::PartitionStatsCollector(::cedar::CedarGraphStorage* storage)
    : storage_(storage) {}

PartitionSstStats PartitionStatsCollector::CollectStats(PartitionID pid) {
  PartitionSstStats stats;
  stats.partition_id = pid;
  
  // Scan partition data to collect statistics
  PartitionRangeScanner scanner(storage_);
  PartitionScanOptions options;
  options.partition_id = pid;
  options.start_entity_id = 0;
  options.end_entity_id = UINT64_MAX;
  options.limit = 100000;  // Sample limit for performance
  
  size_t sampled = 0;
  scanner.ScanWithCallback(options, [&](const CedarKey& key, const Descriptor& desc) {
    stats.total_keys++;
    stats.total_size_bytes += sizeof(CedarKey) + sizeof(Descriptor);
    if (desc.GetKind() == EntryKind::Tombstone) {
      stats.tombstone_count++;
      stats.tombstone_size_bytes += sizeof(Descriptor);
    }
    uint64_t eid = key.entity_id();
    if (eid < stats.min_entity_id) stats.min_entity_id = eid;
    if (eid > stats.max_entity_id) stats.max_entity_id = eid;
    Timestamp ts = key.timestamp();
    if (stats.min_timestamp.value() == 0 || ts.value() < stats.min_timestamp.value()) {
      stats.min_timestamp = ts;
    }
    if (ts.value() > stats.max_timestamp.value()) {
      stats.max_timestamp = ts;
    }
    sampled++;
    return true;
  });
  
  // Get global SST stats
  auto global_stats = storage_->GetStats();
  stats.sst_file_count = global_stats.sst_count;
  stats.sst_total_size = global_stats.sst_size;
  
  stats.last_update_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  
  // Cache the result
  {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    cached_stats_[pid] = stats;
  }
  
  return stats;
}

std::unordered_map<PartitionID, PartitionSstStats> PartitionStatsCollector::CollectAllStats() {
  std::unordered_map<PartitionID, PartitionSstStats> all_stats;
  
  // Collect stats for all cached partitions
  {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    for (const auto& [pid, stats] : cached_stats_) {
      all_stats[pid] = stats;
    }
  }
  
  // Note: Without a partition directory, we cannot discover all partitions.
  // The caller should trigger CollectStats for each known partition.
  
  {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    cached_stats_ = all_stats;
    last_full_scan_time_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
  }
  
  return all_stats;
}

void PartitionStatsCollector::UpdateStatsIncremental(PartitionID pid, const CedarKey& key,
                                                     const Descriptor& value, bool is_delete) {
  std::unique_lock<std::shared_mutex> lock(cache_mutex_);
  
  auto& stats = cached_stats_[pid];
  stats.partition_id = pid;
  
  if (is_delete) {
    stats.tombstone_count++;
    stats.tombstone_size_bytes += sizeof(Descriptor);
  } else {
    stats.total_keys++;
    stats.total_size_bytes += sizeof(CedarKey) + sizeof(Descriptor);
  }
  
  // Update key ranges
  uint64_t entity_id = key.entity_id();
  stats.min_entity_id = std::min(stats.min_entity_id, entity_id);
  stats.max_entity_id = std::max(stats.max_entity_id, entity_id);
}

PartitionSstStats PartitionStatsCollector::GetCachedStats(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(cache_mutex_);
  
  auto it = cached_stats_.find(pid);
  if (it != cached_stats_.end()) {
    return it->second;
  }
  
  return PartitionSstStats{};
}

void PartitionStatsCollector::RefreshAllStats() {
  CollectAllStats();
}

uint64_t PartitionStatsCollector::EstimatePartitionSize(PartitionID pid) const {
  std::shared_lock<std::shared_mutex> lock(cache_mutex_);
  
  auto it = cached_stats_.find(pid);
  if (it != cached_stats_.end()) {
    return it->second.total_size_bytes;
  }
  
  return 0;
}

// =============================================================================
// PartitionCompactionScheduler Implementation
// =============================================================================

PartitionCompactionScheduler::PartitionCompactionScheduler(void* manager)
    : manager_(manager) {}

void PartitionCompactionScheduler::RequestCompaction(PartitionID pid, double priority) {
  std::unique_lock<std::mutex> lock(mutex_);
  
  // Higher priority value = more urgent
  auto it = pending_compactions_.find(pid);
  if (it == pending_compactions_.end() || priority > it->second) {
    pending_compactions_[pid] = priority;
  }
}

std::optional<PartitionID> PartitionCompactionScheduler::GetNextCompactionTarget() {
  std::unique_lock<std::mutex> lock(mutex_);
  
  // Find highest priority partition not currently compacting
  PartitionID best_pid = 0;
  double best_priority = -1;
  
  for (const auto& [pid, priority] : pending_compactions_) {
    if (in_progress_.count(pid) == 0 && priority > best_priority) {
      best_pid = pid;
      best_priority = priority;
    }
  }
  
  if (best_priority < 0) {
    return std::nullopt;
  }
  
  in_progress_.insert(best_pid);
  pending_compactions_.erase(best_pid);
  
  return best_pid;
}

void PartitionCompactionScheduler::MarkCompactionComplete(PartitionID pid) {
  std::unique_lock<std::mutex> lock(mutex_);
  in_progress_.erase(pid);
}

bool PartitionCompactionScheduler::NeedsCompaction(PartitionID pid) const {
  std::unique_lock<std::mutex> lock(mutex_);
  return pending_compactions_.count(pid) > 0;
}

std::vector<PartitionCompactionPriority> 
PartitionCompactionScheduler::GetCompactionPriorities() {
  std::unique_lock<std::mutex> lock(mutex_);
  
  std::vector<PartitionCompactionPriority> priorities;
  
  // Try to get stats collector from manager (expected to be ExtendedPartitionManager)
  PartitionStatsCollector* stats_collector = nullptr;
  if (manager_) {
    auto* ext_mgr = reinterpret_cast<ExtendedPartitionManager*>(manager_);
    stats_collector = ext_mgr->GetStatsCollector();
  }
  
  for (const auto& [pid, priority] : pending_compactions_) {
    PartitionCompactionPriority p;
    p.partition_id = pid;
    p.priority_score = priority;
    if (stats_collector) {
      auto stats = stats_collector->GetCachedStats(pid);
      p.estimated_size_bytes = stats.total_size_bytes;
      if (stats.total_keys > 0) {
        p.tombstone_ratio = (stats.tombstone_count * 100) / stats.total_keys;
      }
    }
    priorities.push_back(p);
  }
  
  // Sort by priority (descending)
  std::sort(priorities.begin(), priorities.end(),
            [](const auto& a, const auto& b) {
              return a.priority_score > b.priority_score;
            });
  
  return priorities;
}

// =============================================================================
// ExtendedPartitionManager Implementation
// =============================================================================

ExtendedPartitionManager::ExtendedPartitionManager() = default;

ExtendedPartitionManager::~ExtendedPartitionManager() {
  Shutdown();
}

Status ExtendedPartitionManager::Initialize(const ExtendedConfig& config) {
  // Initialize base PartitionManager
  Status s = base_manager_.Initialize(config.base_config);
  if (!s.ok()) {
    return s;
  }
  
  ext_config_ = config;
  
  // Initialize WAL writer
  if (config.enable_per_partition_wal) {
    wal_writer_ = std::make_unique<PartitionWalWriter>(config.wal_dir);
  }
  
  // Initialize compaction filter
  if (config.enable_compaction_filter) {
    compaction_filter_ = std::make_unique<PartitionCompactionFilter>(&base_manager_);
  }
  
  // Initialize stats collector
  if (config.enable_partition_stats) {
    stats_collector_ = std::make_unique<PartitionStatsCollector>(base_manager_.GetSharedStorage());
  }
  
  // Initialize range scanner
  range_scanner_ = std::make_unique<PartitionRangeScanner>(base_manager_.GetSharedStorage());
  
  // Initialize compaction scheduler
  compaction_scheduler_ = std::make_unique<PartitionCompactionScheduler>(&base_manager_);
  
  // Initialize partition index components
  if (config.enable_partition_index) {
    partition_index_ = std::make_unique<PartitionIndex>(base_manager_.GetSharedStorage());
    memtable_tracker_ = std::make_unique<MemTablePartitionTracker>();
    query_engine_ = std::make_unique<PartitionQueryEngine>(
        base_manager_.GetSharedStorage(),
        partition_index_.get(),
        memtable_tracker_.get());
    
    // Build initial index
    partition_index_->BuildIndex();
    
    // Inject partition index into LSM engine for incremental updates
    auto* lsm = base_manager_.GetSharedStorage()->GetLsmEngine();
    if (lsm && partition_index_) {
      lsm->SetPartitionIndex(partition_index_.get());
    }
  }
  
  return Status::OK();
}

void ExtendedPartitionManager::Shutdown() {
  // Clean up index components
  query_engine_.reset();
  memtable_tracker_.reset();
  partition_index_.reset();
  
  // Clean up extended components
  compaction_scheduler_.reset();
  range_scanner_.reset();
  stats_collector_.reset();
  compaction_filter_.reset();
  wal_writer_.reset();
  
  // Shutdown base
  base_manager_.Shutdown();
}

Status ExtendedPartitionManager::WriteWal(PartitionID pid, WalEntryType type, 
                                          const std::string& data) {
  if (!wal_writer_) {
    return Status::NotSupported("Per-partition WAL not enabled");
  }
  
  // Ensure WAL is initialized for this partition
  Status s = wal_writer_->Init(pid);
  if (!s.ok()) {
    return s;
  }
  
  return wal_writer_->Append(pid, type, data, nullptr);
}

Status ExtendedPartitionManager::SyncWal(PartitionID pid) {
  if (!wal_writer_) {
    return Status::NotSupported("Per-partition WAL not enabled");
  }
  
  return wal_writer_->Sync(pid);
}

void ExtendedPartitionManager::SetActivePartitionsForCompaction(
    const std::vector<PartitionID>& active_pids) {
  if (!compaction_filter_) {
    return;
  }
  
  std::unordered_set<PartitionID> pid_set;
  for (auto pid : active_pids) {
    pid_set.insert(pid);
  }
  compaction_filter_->SetActivePartitions(pid_set);
}

void ExtendedPartitionManager::EnableCompactionFilter(bool enable) {
  if (!compaction_filter_) {
    return;
  }
  
  if (enable) {
    // By default, allow all partitions if none specified
    compaction_filter_->ClearActivePartitions();
  } else {
    // Disable by setting empty active set (filters everything)
    compaction_filter_->SetActivePartitions({});
  }
}

PartitionSstStats ExtendedPartitionManager::GetPartitionStats(PartitionID pid) {
  if (!stats_collector_) {
    return PartitionSstStats{};
  }
  
  return stats_collector_->CollectStats(pid);
}

std::unordered_map<PartitionID, PartitionSstStats> 
ExtendedPartitionManager::GetAllPartitionStats() {
  if (!stats_collector_) {
    return {};
  }
  
  return stats_collector_->CollectAllStats();
}

std::vector<std::pair<CedarKey, Descriptor>> 
ExtendedPartitionManager::ScanPartitionRange(const PartitionScanOptions& options) {
  // Use query engine if available (more efficient via index)
  if (query_engine_) {
    auto keys = query_engine_->ScanPartitionKeys(
        options.partition_id, options.start_entity_id, options.end_entity_id);
    
    std::vector<std::pair<CedarKey, Descriptor>> results;
    results.reserve(keys.size());
    
    for (const auto& key : keys) {
      // Fetch actual values from storage
      auto desc = range_scanner_->GetStorage()->Get(
          key.entity_id(), key.timestamp().value());
      if (desc.has_value()) {
        results.emplace_back(key, desc.value());
      }
    }
    
    return results;
  }
  
  // Fallback to basic range scanner
  if (range_scanner_) {
    return range_scanner_->Scan(options);
  }
  
  return {};
}

uint64_t ExtendedPartitionManager::EstimatePartitionSize(PartitionID pid) const {
  if (query_engine_) {
    return query_engine_->EstimatePartitionSize(pid);
  }
  return 0;
}

uint64_t ExtendedPartitionManager::EstimatePartitionKeyCount(PartitionID pid) const {
  if (query_engine_) {
    return query_engine_->EstimateKeyCount(pid);
  }
  return 0;
}

}  // namespace dtx
}  // namespace cedar
