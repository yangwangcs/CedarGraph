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

#ifndef CEDAR_LSM_ENGINE_H_
#define CEDAR_LSM_ENGINE_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/storage/cedar_memtable.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/core/status.h"
#include "cedar/types/edge_scan_entry.h"
#include "cedar/transaction/wal.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/storage/vsl_memtable.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/storage/sst_reader_cache.h"
#include "cedar/common/roaring_bitmap.h"
#include "cedar/storage/active_entity_bitmap.h"
#include "cedar/storage/skeleton_cache.h"
#include "cedar/storage/parallel_query_engine.h"

#include <set>
#include <unordered_set>

#include "cedar/core/env.h"

namespace cedar {
namespace dtx {
class PartitionIndex;
}

// 前向声明
class BatchExecutor;
class BlobFileManager;
class AutoBlobStorage;
class ParallelQueryEngine;

// SST file metadata
struct SSTFileMeta {
  uint64_t file_number;
  uint64_t file_size;
  uint64_t num_entries;
  uint64_t min_entity_id;
  uint64_t max_entity_id;
  uint64_t min_tx_time;
  uint64_t max_tx_time;
  int level;
  
  // Column and type info for filtering
  uint16_t column_id = 0;
  uint8_t entity_type = 0;
  
  // Version chain for historical queries
  uint64_t prev_file_number = 0;
  
  // OPTIMIZATION: Bloom Filter for fast negative check
  // This allows excluding files without opening them
  uint64_t bloom_filter_hash = 0;
  bool has_bloom_filter = false;
  
  // Temporal Bloom Filter 序列化数据（用于时间范围查询过滤）
  std::string temporal_filter_metadata;
  
  std::string file_name() const;
};

// LSM Storage Engine
class LsmEngine {
 public:
  LsmEngine(const std::string& db_path,
            const CedarOptions& options,
            cedar::Env* env);
  virtual ~LsmEngine();

  LsmEngine(const LsmEngine&) = delete;
  LsmEngine& operator=(const LsmEngine&) = delete;

  // Open the database
  virtual Status Open();

  // Close the database
  virtual Status Close();

  // Put a key-value pair (legacy interface)
  Status Put(uint64_t entity_id, uint64_t tx_time, const Slice& value, Timestamp txn_version);

  // Put with new types
  Status Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version);

  // Batch write - single WAL write for multiple entries (much faster for bulk operations)
  struct WriteBatchEntry {
    CedarKey key;
    Descriptor descriptor;
    Timestamp txn_version;
  };
  Status WriteBatch(const std::vector<WriteBatchEntry>& entries);

  // Delete a key (legacy interface)
  Status Delete(uint64_t entity_id, uint64_t tx_time, Timestamp txn_version);

  // Delete with new types
  Status Delete(const CedarKey& key, Timestamp txn_version);

  // Get a value (legacy interface)
  Status Get(uint64_t entity_id, uint64_t tx_time, std::string* value);
  
  // ========== Blob Storage API ==========
  // 设置 BlobFileManager（由上层 CedarGraphStorage 传入）
  void SetBlobFileManager(BlobFileManager* blob_mgr);
  
  // 设置 AutoBlobStorage（由上层 CedarGraphStorage 传入）
  void SetAutoBlobStorage(AutoBlobStorage* auto_blob);
  
  // 设置 PartitionIndex（用于增量索引更新）
  void SetPartitionIndex(cedar::dtx::PartitionIndex* index);
  
  // 存储字符串（自动选择内联或 Blob）
  Status PutString(uint64_t entity_id, uint16_t col_id, const std::string& value);
  
  // 读取字符串（自动解析 Blob 引用）
  std::optional<std::string> GetString(uint64_t entity_id, uint16_t col_id);

  // Get with new types
  std::optional<Descriptor> Get(const CedarKey& key);

  // Get all versions
  std::vector<MemTableEntry> GetAll(uint64_t entity_id,
                                     EntityType entity_type,
                                     uint16_t column_id);

  // Get at specific time
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp);
  
  // Get at specific time with full CedarKey metadata
  std::optional<std::pair<CedarKey, Descriptor>> GetRecordAtTime(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      Timestamp timestamp);

  // Get time range
  std::vector<MemTableEntry> GetRange(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp start,
                                       Timestamp end);

  // Get time range with limit - stops after max_results entries
  std::vector<MemTableEntry> GetRangeLimit(uint64_t entity_id,
                                            EntityType entity_type,
                                            uint16_t column_id,
                                            Timestamp start,
                                            Timestamp end,
                                            size_t max_results);
  
  // ========== CedarScan 边扫描接口 ==========
  
  // Scan edges for a vertex (OutEdges or InEdges)
  // Returns list of (target_id, timestamp, descriptor, edge_type) tuples
  // If edge_type is 0xFFFF (kAllLabels), scans all edge types
  std::vector<std::tuple<uint64_t, Timestamp, Descriptor, uint16_t>> ScanEdges(
      uint64_t vertex_id,
      EntityType edge_direction,  // EdgeOut or EdgeIn
      uint16_t edge_type,         // 0xFFFF for all types
      Timestamp snapshot_ts);
  
  // Get all column IDs for an entity (used for edge scanning)
  std::vector<uint16_t> GetEntityColumnIds(uint64_t entity_id,
                                            EntityType entity_type) const;
  
  // ========== Iterator 接口 ==========
  
  // Scan edges with version folding (returns latest version of each unique edge)
  // Optimized for zero-allocation version folding using sorted results
  std::vector<EdgeScanEntry> ScanEdgesWithFolding(
      uint64_t vertex_id,
      EntityType edge_direction,
      uint16_t edge_type,
      Timestamp snapshot_ts) const;
  
  // ========== 批量查询接口 (优化) ==========
  // Batch query multiple entity IDs at once - reduces lock overhead
  struct BatchQueryItem {
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Timestamp timestamp;
    std::optional<Descriptor> result;
  };
  
  // Batch GetAtTime - query multiple entities with single lock acquisition
  void BatchGetAtTime(std::vector<BatchQueryItem>& items);
  
  // Batch GetRange - query multiple entities with single lock acquisition
  struct BatchRangeItem {
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Timestamp start;
    Timestamp end;
    size_t max_results;
    std::vector<MemTableEntry> results;
  };
  void BatchGetRange(std::vector<BatchRangeItem>& items);
  
  // OPTIMIZATION: 并行 BatchGetRange - 使用线程池加速多 entity 查询
  void ParallelBatchGetRange(std::vector<BatchRangeItem>& items, size_t num_threads = 4);
  
  // OPTIMIZATION: P0 - 优化的批量时间范围查询
  // 使用 SST 层的 BatchGetRange 减少文件扫描次数
  // 返回：entity_id -> MemTableEntry 列表
  std::unordered_map<uint64_t, std::vector<MemTableEntry>> 
  BatchGetRangeOptimized(const std::vector<uint64_t>& entity_ids,
                         EntityType entity_type,
                         uint16_t column_id,
                         Timestamp start,
                         Timestamp end,
                         size_t max_results_per_entity = 200);
  
  // OPTIMIZATION: P2 - 并行查询多个 SST 文件
  // 使用线程池并行查询多个文件，加速大数据量查询
  std::unordered_map<uint64_t, std::vector<MemTableEntry>>
  ParallelGetRangeFromSST(const std::vector<uint64_t>& entity_ids,
                           EntityType entity_type,
                           uint16_t column_id,
                           Timestamp start,
                           Timestamp end,
                           size_t max_results_per_entity = 200,
                           size_t num_threads = 4);

  // ========== MVCC 时态版本链查询 (融合方案) ==========
  // 获取实体的完整时间历史 (跨 MemTable + SST)
  // 融合 MemTable 版本链 + SST Full-History，支持 O(k) 遍历版本链
  struct TemporalVersion {
    Timestamp timestamp;
    Descriptor descriptor;
    int source_level;  // -1=MemTable, 0+=SST Level
  };
  
  // 获取完整版本链 (MemTable + 所有 SST 层级)
  // 按时间戳降序返回 (最新 -> 最旧)
  std::vector<TemporalVersion> GetTemporalChain(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 uint16_t column_id);
  
  // 遍历版本链 (回调方式，支持提前终止)
  // 先遍历 MemTable (热数据)，再遍历 SST (冷数据)
  void TraverseTemporalChain(uint64_t entity_id,
                              EntityType entity_type,
                              uint16_t column_id,
                              std::function<bool(const TemporalVersion&)> callback);

  // Force flush memtable to disk
  Status ForceFlush();
  
  // ========== 超大 SST 累积 Flush（V2 优化）==========
  
  // 启用累积 Flush 模式（累积多个 MemTable 到一个大 SST）
  void EnableAccumulatedFlush(size_t target_size_bytes = 64 * 1024 * 1024);
  
  // 禁用累积 Flush（恢复即时 Flush）
  void DisableAccumulatedFlush();
  
  // 手动触发累积 Flush（将累积的数据刷盘）
  Status FlushAccumulated();
  
  // 获取当前累积数据量（字节）
  size_t GetAccumulatedSize() const;
  
  // 检查是否启用累积 Flush
  bool IsAccumulatedFlushEnabled() const { return accumulated_flush_enabled_; }

  // Trigger compaction manually
  Status Compact();

  // Pause/resume background compaction (for snapshot safety)
  void PauseCompaction();
  void ResumeCompaction();

  // Get SST file list for a level
  std::vector<SSTFileMeta> GetSSTFiles(int level) const;

  // Get statistics
  struct Stats {
    size_t memtable_size = 0;
    size_t imm_memtable_size = 0;
    size_t sst_count = 0;
    size_t sst_size = 0;
    int num_levels = 0;
    // Disk usage tracking
    uint64_t total_disk_bytes = 0;      // Total disk space
    uint64_t used_disk_bytes = 0;       // Used disk space
    uint64_t db_size_bytes = 0;         // Database directory size
    double disk_usage_percent = 0.0;    // Disk usage percentage
  };
  Stats GetStats() const;
  
  // Storage capacity monitoring
  struct CapacityInfo {
    uint64_t total_bytes = 0;
    uint64_t used_bytes = 0;
    uint64_t available_bytes = 0;
    uint64_t db_size_bytes = 0;
    double usage_percent = 0.0;
    bool is_critical = false;     // > 90% usage
    bool is_warning = false;      // > 80% usage
  };
  CapacityInfo GetCapacityInfo() const;

  // ========== WAL 和事务支持 ==========
  
  // 获取 WAL 写入器
  WalWriter* GetWalWriter() const { return wal_writer_.get(); }
  
  // 获取事务管理器
  TransactionManager* GetTransactionManager() const { 
    return txn_manager_.get(); 
  }
  
  // 获取 MemTable
  VSLMemTable* GetMemTable() const { return mem_.get(); }
  
  // Acquire a shared lock on the engine's memtable mutex.
  // Use this when you need to prevent memtable swap during a batch operation.
  std::shared_lock<std::shared_mutex> AcquireSharedLock() const {
    return std::shared_lock<std::shared_mutex>(mutex_);
  }
  
  // 获取 Batch 执行器
  BatchExecutor* GetBatchExecutor() const { return batch_executor_.get(); }
  
  // 创建新的事务
  std::unique_ptr<OCCTransaction> BeginTransaction(
      const TransactionOptions& options = TransactionOptions());
  
  // 同步 WAL
  Status SyncWAL();
  
  // Get database path
  std::string GetDbPath() const { return db_path_; }
  
  // Get SST files for checkpoint
  const std::vector<std::vector<SSTFileMeta>>& GetSstFiles() const { return levels_; }
  
  // ============================================================================
  // Secondary Index (Label / Property) — MVP in-memory only
  // ============================================================================
  static constexpr uint16_t kLabelColumnId = 0x0FFF;

  // Add a label → entity_id mapping (called by CreateOperator)
  void IndexLabel(uint64_t entity_id, const std::string& label);

  // Remove an entity from all indexes (called by Delete)
  void RemoveFromIndexes(uint64_t entity_id);

  // Lookup entity IDs by exact label match
  const std::vector<uint64_t>& LookupLabelIndex(const std::string& label) const;

  // Lookup entity IDs by exact (column_id, value_string) match
  std::vector<uint64_t> LookupPropertyIndex(uint16_t column_id,
                                             const std::string& value) const;

  // Helper: convert a Descriptor to its canonical index string
  static std::string DescriptorToIndexString(const Descriptor& desc);

  // ========== Size-Tiered Compaction 引擎 ==========
  
  // 获取 Compaction 引擎
  SizeTieredCompactionEngine* GetCompactionEngine() const { return compaction_engine_.get(); }
  
  // 手动触发全量合并
  Status CompactAll();
  
  // 等待所有后台合并完成
  void WaitForCompactions();

 private:
  // 初始化 WAL
  Status InitWAL();

  // Replay all WAL files from the given starting sequence.
  Status ReplayWAL(uint64_t start_sequence);
  // Background compaction worker
  void BackgroundCompaction();

  // Check if compaction is needed and trigger it
  void MaybeScheduleCompaction();

  // Check if memtable needs flush and trigger it (async, non-blocking)
  void MaybeScheduleFlush();

  // Flush memtable to SST file
  Status FlushMemTable(VSLMemTable* mem);

  // Flush memtable to SST file with retry (max 3 attempts, exponential backoff)
  Status FlushMemTableWithRetry(VSLMemTable* mem);
  
  // Flush entries directly to SST file (unified flush, cross Column ID)
  Status FlushEntriesToSST(std::vector<std::tuple<CedarKey, Descriptor, Timestamp>> entries);
  
  // Flush a group of entries (same entity_type and column_id) to SST file
  // Used by FlushMemTable to support mixed entity types
  Status FlushEntityGroup(uint8_t entity_type, uint16_t column_id,
                          const std::vector<std::tuple<CedarKey, Descriptor, Timestamp>>& entries);

  // Do compaction on specified level
  Status DoCompaction(int level, const std::vector<SSTFileMeta>& inputs);

  // Select files for compaction
  std::vector<SSTFileMeta> SelectCompactionFiles(int level);
  
  // Query SST files for entity
  void QuerySSTFiles(uint64_t entity_id, EntityType entity_type,
                     uint16_t column_id, std::vector<MemTableEntry>* results);
  
  // Helper: Get entries from SST files for specific entity (replaces GetVersionChain)
  void GetEntriesFromSst(uint64_t entity_id, EntityType entity_type,
                         uint16_t column_id, std::vector<std::tuple<CedarKey, Descriptor, Timestamp>>* results);
  
  // Check if compaction is needed
  bool NeedsCompaction(int level) const;

  // Load existing SST files
  Status LoadSstFiles();

  // Generate next file number
  uint64_t NewFileNumber();

  // SST file path
  std::string SstFilePath(uint64_t file_number);

  std::string db_path_;
  CedarOptions options_;
  cedar::Env* env_;

  // MemTables (使用 Lock-Free VSL MemTable)
  std::unique_ptr<VSLMemTable> mem_;
  std::unique_ptr<VSLMemTable> imm_;  // Immutable memtable being flushing
  mutable std::shared_mutex mutex_;  // 使用 shared_mutex 支持并发读
  std::atomic<bool> has_work_{false};

  // SST files by level
  std::vector<std::vector<SSTFileMeta>> levels_;

  // Background thread
  std::atomic<bool> shutdown_;
  std::atomic<bool> disable_auto_flush_{false};
  std::unique_ptr<std::thread> bg_thread_;
  std::atomic<bool> compaction_scheduled_;

  // File number generator (shared with compaction engine)
  std::shared_ptr<std::atomic<uint64_t>> next_file_number_;

  // State
  bool opened_ = false;
  
  // WAL 和事务支持
  std::unique_ptr<WalWriter> wal_writer_;
  std::unique_ptr<TransactionManager> txn_manager_;
  std::unique_ptr<BatchExecutor> batch_executor_;
  
  // OPTIMIZATION: Hot data preload flag
  bool hot_data_preloaded_ = false;
  
  // OPTIMIZATION: Preload hot entities on startup
  void PreloadHotEntities();
  
  // OPTIMIZATION: 热数据预读取 - 根据查询模式预读取热数据
  // 在查询 SST 文件之前，先检查是否需要预读取热数据
  void PrefetchHotData(uint64_t entity_id, EntityType entity_type, uint16_t column_id);
  
  // OPTIMIZATION: 查询模式追踪 - 用于识别热数据
  void TrackQueryPattern(uint64_t entity_id, EntityType entity_type, uint16_t column_id);

  
  // OPTIMIZATION: 跨查询缓存 - 缓存热数据的查询结果
  // 格式: (entity_id, column_id) -> vector<MemTableEntry>
  struct CacheKey {
    uint64_t entity_id;
    uint16_t column_id;
    
    bool operator==(const CacheKey& other) const {
      return entity_id == other.entity_id && column_id == other.column_id;
    }
  };
  
  struct CacheKeyHash {
    size_t operator()(const CacheKey& key) const {
      return std::hash<uint64_t>()(key.entity_id) ^ 
             (std::hash<uint16_t>()(key.column_id) << 1);
    }
  };
  
  // 缓存条目
  struct CacheEntry {
    std::vector<MemTableEntry> data;
    std::chrono::steady_clock::time_point timestamp;
    uint32_t hit_count = 0;
  };
  
  mutable std::unordered_map<CacheKey, CacheEntry, CacheKeyHash> cross_query_cache_;
  mutable std::shared_mutex cross_query_cache_mutex_;
  static constexpr size_t kMaxCrossQueryCacheSize = 1000;  // 最大缓存条目数
  static constexpr auto kCrossQueryCacheTTL = std::chrono::seconds(60);  // 缓存过期时间
  
  // 查询模式追踪
  struct QueryPattern {
    uint64_t count = 0;
    std::chrono::steady_clock::time_point last_query;
  };
  mutable std::unordered_map<CacheKey, QueryPattern, CacheKeyHash> query_patterns_;
  mutable std::shared_mutex query_pattern_mutex_;
  static constexpr uint64_t kHotDataThreshold = 10;  // 热数据阈值：查询次数超过此值
  
  // 跨查询缓存操作
  std::optional<std::vector<MemTableEntry>> GetFromCrossQueryCache(
      uint64_t entity_id, uint16_t column_id) const;
  void AddToCrossQueryCache(uint64_t entity_id, uint16_t column_id, 
                            const std::vector<MemTableEntry>& data);
  void InvalidateCrossQueryCache(uint64_t entity_id, uint16_t column_id);
  void CleanupCrossQueryCache();
  
  // OPTIMIZATION: P1 - 时间范围预计算缓存
  // 缓存常见时间范围的查询结果
  struct TimeRangeCacheKey {
    uint64_t entity_id;
    uint16_t column_id;
    uint64_t start_ts;
    uint64_t end_ts;
    
    bool operator==(const TimeRangeCacheKey& other) const {
      return entity_id == other.entity_id && 
             column_id == other.column_id &&
             start_ts == other.start_ts && 
             end_ts == other.end_ts;
    }
  };
  
  struct TimeRangeCacheKeyHash {
    size_t operator()(const TimeRangeCacheKey& key) const {
      return std::hash<uint64_t>()(key.entity_id) ^ 
             (std::hash<uint16_t>()(key.column_id) << 1) ^
             (std::hash<uint64_t>()(key.start_ts) << 2) ^
             (std::hash<uint64_t>()(key.end_ts) << 3);
    }
  };
  
  mutable std::unordered_map<TimeRangeCacheKey, 
                             std::pair<std::vector<MemTableEntry>, 
                                       std::chrono::steady_clock::time_point>,
                             TimeRangeCacheKeyHash> time_range_cache_;
  mutable std::shared_mutex time_range_cache_mutex_;
  static constexpr size_t kMaxTimeRangeCacheSize = 10000;
  static constexpr auto kTimeRangeCacheTTL = std::chrono::seconds(30);
  
  std::optional<std::vector<MemTableEntry>> GetFromTimeRangeCache(
      uint64_t entity_id, uint16_t column_id, uint64_t start_ts, uint64_t end_ts) const;
  void AddToTimeRangeCache(uint64_t entity_id, uint16_t column_id, 
                           uint64_t start_ts, uint64_t end_ts,
                           const std::vector<MemTableEntry>& data);
  void CleanupTimeRangeCache();
  
  // TTL Data Expiration
  // Configures automatic data expiration based on timestamp
  void SetTTLConfig(uint64_t retention_period_us, bool enable_auto_cleanup = true,
                    const std::string& archive_dir = "");
  uint64_t GetTTLConfig() const;
  bool IsTTLEnabled() const;
  
  // 归档过期数据（不物理删除，移到归档目录）
  int64_t ExpireOldData();
  int64_t GetArchivedDataCount() const;
  
  // Background cleanup thread
  void StartTTLCleanupThread(int interval_seconds = 3600);
  void StopTTLCleanupThread();
  
  // Get expired data count
  int64_t GetExpiredDataCount() const;
  
  // SST Reader cache for better read performance
  std::unique_ptr<SstReaderCache> sst_reader_cache_;
  
  // OPTIMIZATION: P1 - SST 文件预加载管理
  // 预加载热数据文件到缓存
  void PreloadHotSSTFiles();
  
  // 获取查询涉及的热点文件列表
  std::vector<std::string> GetHotSSTFilesForQuery(
      const std::vector<uint64_t>& entity_ids,
      uint16_t column_id,
      uint8_t entity_type) const;
  
  // Column ID tracking for efficient Scan queries
  // Maps entity_id -> roaring bitmap of column_ids that have data
  std::unordered_map<uint64_t, RoaringBitmap> entity_column_map_;
  mutable std::shared_mutex column_map_mutex_;

  // Secondary indexes (in-memory only — rebuilt on Open or populated lazily)
  std::map<std::string, std::vector<uint64_t>> label_index_;
  std::map<std::pair<uint16_t, std::string>, std::vector<uint64_t>> property_index_;
  mutable std::shared_mutex index_mutex_;

  // TTL Configuration — 归档模式，不物理删除
  uint64_t ttl_retention_period_us_ = 0;  // 0 = disabled
  bool ttl_auto_cleanup_enabled_ = false;
  std::string archive_dir_;  // 归档目录路径
  std::thread ttl_cleanup_thread_;
  std::atomic<bool> ttl_running_{false};
  std::atomic<int64_t> expired_data_count_{0};
  std::atomic<int64_t> archived_data_count_{0};
  mutable std::mutex ttl_mutex_;
  std::mutex ttl_cleanup_mutex_;
  std::condition_variable ttl_cleanup_cv_;

  // Internal helpers
  void UpdatePropertyIndex(uint64_t entity_id, uint16_t column_id,
                           const std::string& value);
  void RemoveEntityFromLabelIndex(uint64_t entity_id);
  void RemoveEntityFromPropertyIndex(uint64_t entity_id);

  // ========== Phase 4: 锚点机制内存加速 ==========
  // 活跃实体位图（L1 缓存）- O(1) 存在性检查
  ActiveEntityBitmap active_entity_bitmap_;
  
  // ========== Phase 5: SkeletonCache 内存拓扑缓存 ==========
  // 分片 LRU 缓存，12B 极致压缩边
  mutable std::unique_ptr<ShardedSkeletonCache> skeleton_cache_;
  
  // ========== Phase 4c: ParallelQueryEngine ==========
  // 跨列并行查询引擎
  std::unique_ptr<ParallelQueryEngine> parallel_engine_;
  
  // ========== Phase 4c: BlobFileManager ==========
  // Blob 存储（由上层 CedarGraphStorage 设置）
  BlobFileManager* blob_manager_ = nullptr;
  AutoBlobStorage* auto_blob_storage_ = nullptr;
  
  // ========== Partition Index ==========
  cedar::dtx::PartitionIndex* partition_index_ = nullptr;
  
  // 公开访问接口
 public:
  // 标记实体为活跃（供写入路径调用）
  void MarkEntityActive(uint64_t entity_id);
  // 标记实体为删除（供写入路径调用）
  void MarkEntityDeleted(uint64_t entity_id);
  // 快速检查实体是否活跃
  bool IsEntityActive(uint64_t entity_id) const;
  // 获取活跃实体位图引用
  const ActiveEntityBitmap& GetActiveEntityBitmap() const { return active_entity_bitmap_; }
  
  // Hot spot detection: returns top-N most queried entities
  struct HotSpot {
    uint64_t entity_id;
    uint16_t column_id;
    uint64_t query_count;
    std::chrono::steady_clock::time_point last_query;
  };
  std::vector<HotSpot> GetHotSpots(size_t top_n = 20) const;
  void ResetQueryPatterns();
  
  // SkeletonCache 接口
  // 启用 SkeletonCache（默认自动启用）
  void EnableSkeletonCache(
      size_t num_shards = ShardedSkeletonCache::kDefaultNumShards,
      size_t max_entries_per_shard = ShardedSkeletonCache::kDefaultMaxEntriesPerShard);
  
  // 获取节点骨架（可能触发 Hydrate）
  std::pair<VertexSkeleton*, bool> GetVertexSkeleton(uint64_t vertex_id);
  
  // 扫描出边（使用 SkeletonCache 优化）
  std::vector<std::tuple<uint64_t, Timestamp, Descriptor, uint16_t>>
  ScanOutEdgesCached(uint64_t src_id, uint16_t edge_type, Timestamp snapshot_ts);
  
  // 获取 SkeletonCache 统计
  ShardedSkeletonCache::Stats GetSkeletonCacheStats() const;
  
  // 获取 SkeletonCache 内存占用
  size_t GetSkeletonCacheMemoryUsage() const;
  
  // 获取 SkeletonCache 原始指针（用于测试）
  ShardedSkeletonCache* GetSkeletonCache() { return skeleton_cache_.get(); }
  
  // ============================================================================
  // Phase 4c: ParallelQueryEngine API
  // ============================================================================
  
  // 并行多列查询（同时查询一个实体的多个属性列）
  // 当 column_ids 数量 >= parallel_threshold 时启用线程池并行
  std::vector<ColumnQueryResult> QueryColumnsParallel(
      uint64_t entity_id,
      const std::vector<uint16_t>& column_ids,
      EntityType entity_type = EntityType::Vertex,
      Timestamp timestamp = Timestamp::Static());
  
  // 获取 ParallelQueryEngine 统计
  ThreadPoolQueryExecutor::Stats GetParallelQueryStats() const;
  
  // ============================================================================
  // QUERY OPTIMIZATION: Fast File Lookup by Entity Range
  // ============================================================================
  // Organize files by column_id for faster lookup
  // column_id -> list of lightweight index entries (sorted by min_entity_id)
  // Stores copies of filtering fields so reallocation of levels_ does not
  // invalidate pointers.
  struct ColumnIndexEntry {
    uint64_t file_number;
    uint64_t min_entity_id;
    uint64_t max_entity_id;
    uint8_t entity_type;
  };
  std::unordered_map<uint16_t, std::vector<ColumnIndexEntry>> column_file_index_;
  mutable std::shared_mutex column_index_mutex_;
  
  // Build column-based file index
  void BuildColumnFileIndex();
  
  // Get candidate files for an entity (using range filtering + Bloom Filter)
  std::vector<uint64_t> GetCandidateFiles(uint64_t entity_id,
                                             EntityType entity_type,
                                             uint16_t column_id) const;
  
  // Track column IDs during write for quick lookup
  void TrackColumnId(uint64_t entity_id, uint16_t column_id);
  
  // ============================================================================
  // 超大 SST 累积 Flush（V2 优化）
  // ============================================================================
 private:
  // 累积 Flush 开关
  bool accumulated_flush_enabled_ = false;
  
  // 累积目标大小（默认 64MB）
  size_t accumulated_flush_target_size_ = 64 * 1024 * 1024;
  
  // 累积的数据缓冲区
  std::vector<std::tuple<CedarKey, Descriptor, Timestamp>> accumulated_entries_;
  
  // 累积缓冲区互斥锁
  mutable std::shared_mutex accumulated_mutex_;  // Read-write lock for concurrent reads
  
  // 累积统计
  size_t accumulated_bytes_ = 0;
  
  // 查询累积缓冲区中匹配条件的最新记录
  std::optional<std::pair<CedarKey, Descriptor>> QueryAccumulatedBuffer(
      uint64_t entity_id, EntityType entity_type, uint16_t column_id,
      Timestamp timestamp) const;

  // ============================================================================
  // BULK IMPORT OPTIMIZATIONS
  // ============================================================================
 public:
  // Enable/disable optimizations for bulk import
  void SetBulkImportMode(bool enabled) {
    disable_column_tracking_ = enabled;
  }
  
 private:
  bool disable_column_tracking_ = false;
  
  // ========== Size-Tiered Compaction 引擎 ==========
  std::unique_ptr<SizeTieredCompactionEngine> compaction_engine_;
  SizeTieredConfig compaction_config_;
  
  // 后台自动触发 Compaction 线程
  std::atomic<bool> auto_compaction_enabled_{true};
  std::unique_ptr<std::thread> auto_compaction_thread_;
  std::mutex auto_compaction_mutex_;
  std::condition_variable auto_compaction_cv_;
  void AutoCompactionThread();

  // Compaction pause support (for snapshot safety)
  std::mutex compaction_pause_mutex_;
  std::condition_variable compaction_pause_cv_;
  std::atomic<bool> compaction_paused_{false};
  
  // 跟踪后台 Flush 操作
  std::atomic<int> active_flush_count_{0};
  std::mutex flush_completion_mutex_;
  std::condition_variable flush_completion_cv_;

  // ============================================================================
  // Commit serialization (P0-6) - Striped locks for better concurrency
  // ============================================================================
  // Instead of a single global mutex, use striped locks based on entity_id.
  // This allows transactions on different entities to commit concurrently.
  friend class OCCTransaction;
  
  static constexpr size_t kCommitLockStripes = 64;  // Power of 2 for fast modulo
  mutable std::array<std::mutex, kCommitLockStripes> commit_lock_stripes_;
  
  // Get the lock stripe for a given entity_id
  std::mutex& GetCommitLock(uint64_t entity_id) const {
    return commit_lock_stripes_[entity_id & (kCommitLockStripes - 1)];
  }
  
  // Legacy global lock for backward compatibility (deprecated)
  mutable std::mutex global_commit_mutex_;
  
  // 迁移现有的 SST 文件到新的 Compaction 引擎
  void MigrateExistingSstFiles();
};

}  // namespace cedar

#endif  // CEDAR_LSM_ENGINE_H_
