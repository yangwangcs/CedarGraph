// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/lsm_engine.h"
#include "cedar/query/cedar_scan.h"
#include "cedar/storage/compaction_merger.h"
#include "cedar/storage/temporal_bloom_filter.h"
#include "cedar/storage/auto_blob_storage.h"
#include "cedar/sst/zone_columnar_format.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/sst/zone_columnar_builder.h"
#include "cedar/sst/sst_builder_factory.h"
#include "cedar/transaction/batch_api.h"
#include "cedar/transaction/wal.h"
#include "cedar/dtx/partition_index.h"

#include <filesystem>
#include <thread>
#include <future>
#include <chrono>
#include <iostream>
#include <unordered_set>

#include "cedar/common/logging.h"

namespace cedar {

LsmEngine::LsmEngine(const std::string& db_path,
                     const CedarOptions& options,
                     cedar::Env* env)
    : db_path_(db_path),
      options_(options),
      env_(env ? env : cedar::Env::Default()),
      mem_(std::make_unique<VSLMemTable>()),
      opened_(false),
      next_file_number_(1),
      shutdown_(false),
      bg_thread_(nullptr),
      query_cache_(std::make_unique<QueryCache>(10000)),
      compaction_scheduled_(false),
      has_work_(false),
      disable_column_tracking_(false),
      disable_query_cache_invalidate_(false) {
  // Initialize Size-Tiered Compaction config from CedarOptions
  if (options_.use_zone_columnar_format) {
    compaction_config_.l0_max_size = options_.size_tiered_config.l0_max_size;
    compaction_config_.l0_file_size = options_.size_tiered_config.l0_file_size;
    compaction_config_.l0_max_files = options_.size_tiered_config.l0_max_files;
    compaction_config_.size_ratio = options_.size_tiered_config.size_ratio;
    compaction_config_.max_levels = options_.size_tiered_config.max_levels;
    compaction_config_.level_size_trigger_ratio = options_.size_tiered_config.level_size_trigger_ratio;
    compaction_config_.max_merge_width = options_.size_tiered_config.max_merge_width;
    compaction_config_.compaction_threads = options_.size_tiered_config.compaction_threads;
    compaction_config_.enable_background_compaction = options_.size_tiered_config.enable_background_compaction;
    compaction_config_.tombstone_cleanup_level = options_.size_tiered_config.tombstone_cleanup_level;
    compaction_config_.blob_rewrite_threshold = options_.size_tiered_config.blob_rewrite_threshold;
  }
  
  // ========== SST 配置 ==========
  // 默认使用 Zone-Columnar 格式（256KB Block，稀疏索引）
  // 默认使用更大的 SST 文件
  if (compaction_config_.l0_file_size < 64 * 1024 * 1024) {
    compaction_config_.l0_file_size = 64 * 1024 * 1024;  // 64MB
  }
  if (compaction_config_.l0_max_files < 8) {
    compaction_config_.l0_max_files = 8;  // L0 最多 8 个文件
  }
}

LsmEngine::~LsmEngine() {
  try {
    Close();
  } catch (...) {
    std::cerr << "[LsmEngine] Exception during Close() in destructor — swallowed" << std::endl;
  }
  // Wait for any pending async flush to prevent UAF
  if (flush_future_.valid()) {
    flush_future_.wait();
  }
}

Status LsmEngine::Open() {
  if (options_.create_if_missing) {
    if (!std::filesystem::exists(db_path_)) {
      std::filesystem::create_directories(db_path_);
    }
  }

  if (!std::filesystem::exists(db_path_)) {
    return Status::InvalidArgument("LsmEngine", "database path does not exist");
  }

  // Initialize levels vector
  levels_.resize(options_.compaction_config.max_levels);

  Status s = LoadSstFiles();
  CEDAR_RETURN_IF_ERROR(s);
  
  // Initialize SST reader cache
  sst_reader_cache_ = std::make_unique<SstReaderCache>(options_.file_cache_size > 0 ? options_.file_cache_size : 16);
  
  // Build column-based file index for fast query
  BuildColumnFileIndex();
  
  // Initialize WAL and TransactionManager
  s = InitWAL();
  CEDAR_RETURN_IF_ERROR(s);
  
  // Recover any unflushed data from WAL
  auto replay_status = ReplayWAL(1);
  if (!replay_status.ok()) {
    return replay_status;
  }
  
  // ========== Initialize Size-Tiered Compaction Engine ==========
  // 从 options 中读取配置或使用默认配置
  compaction_engine_ = std::make_unique<SizeTieredCompactionEngine>(
      db_path_, compaction_config_, env_);
  
  s = compaction_engine_->Open();
  if (!s.ok()) {
    return Status::IOError("LsmEngine", "Failed to open compaction engine: " + s.ToString());
  }
  
  // 启动自动 Compaction 后台线程
  auto_compaction_enabled_.store(true);
  auto_compaction_thread_ = new std::thread(&LsmEngine::AutoCompactionThread, this);
  
  // 如果存在现有的 SST 文件，迁移到新的 Compaction 引擎
  MigrateExistingSstFiles();
  
  // ========== Initialize Phase 5: SkeletonCache ==========
  if (options_.enable_skeleton_cache) {
    EnableSkeletonCache(options_.skeleton_cache_shards, 
                        options_.skeleton_cache_entries_per_shard);
  }
  
  // ========== Initialize Phase 4c: ParallelQueryEngine ==========
  if (options_.enable_parallel_query) {
    ParallelQueryConfig pq_config;
    pq_config.num_threads = options_.parallel_query_threads;
    pq_config.max_concurrent_columns = options_.parallel_query_max_columns;
    pq_config.parallel_threshold = options_.parallel_query_threshold;
    pq_config.timeout_ms = options_.parallel_query_timeout_ms;
    parallel_engine_ = std::make_unique<ParallelQueryEngine>(this, pq_config);
  }
  
  opened_ = true;
  
  return Status::OK();
}

Status LsmEngine::Close() {
  if (!opened_) return Status::OK();

  shutdown_ = true;
  disable_auto_flush_ = true;

  // 关闭自动 Compaction 线程
  auto_compaction_enabled_.store(false);
  if (auto_compaction_thread_ && auto_compaction_thread_->joinable()) {
    auto_compaction_thread_->join();
    delete auto_compaction_thread_;
    auto_compaction_thread_ = nullptr;
  }

  if (bg_thread_ && bg_thread_->joinable()) {
    bg_thread_->join();
    delete bg_thread_;
    bg_thread_ = nullptr;
  }
  
  // 首先关闭 Compaction 引擎，防止在 Flush 期间触发新的 Compaction
  if (compaction_engine_) {
    std::cerr << "[LsmEngine::Close] Closing compaction engine" << std::endl;
    compaction_engine_->WaitForCompactions();
    compaction_engine_->Close();
    compaction_engine_.reset();
  }
  
  // 等待所有后台 Flush 完成
  {
    std::cerr << "[LsmEngine::Close] Waiting for " << active_flush_count_.load() << " active flushes" << std::endl;
    std::unique_lock<std::mutex> lock(flush_completion_mutex_);
    flush_completion_cv_.wait(lock, [this] { return active_flush_count_.load() == 0; });
    std::cerr << "[LsmEngine::Close] All flushes completed" << std::endl;
  }

  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (imm_) {
      std::cerr << "[LsmEngine::Close] Flushing imm_" << std::endl;
      auto* imm = imm_.get();
      lock.unlock();
      FlushMemTable(imm);
      lock.lock();
      imm_.reset();
    }

    if (mem_ && !mem_->IsEmpty()) {
      std::cerr << "[LsmEngine::Close] Flushing mem_" << std::endl;
      auto* mem = mem_.get();
      lock.unlock();
      FlushMemTable(mem);
      lock.lock();
    } else {
      std::cerr << "[LsmEngine::Close] mem_ is empty or null" << std::endl;
    }
  }

  // Flush any accumulated entries (accumulated flush mode)
  if (options_.enable_accumulated_flush && !accumulated_entries_.empty()) {
    std::cerr << "[LsmEngine::Close] Flushing accumulated entries" << std::endl;
    FlushAccumulated();
  }

  // 同步并关闭 WAL，确保所有已提交数据持久化
  if (wal_writer_) {
    std::cerr << "[LsmEngine::Close] Closing WAL" << std::endl;
    wal_writer_->Close();
    wal_writer_.reset();
  }

  opened_ = false;
  return Status::OK();
}

Status LsmEngine::Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version) {
  if (!opened_) {
    return Status::InvalidArgument("LsmEngine", "not opened");
  }

  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 先写 WAL，确保持久化成功后再修改 memtable
    if (wal_writer_) {
      Status wal_status = wal_writer_->WritePut(key, descriptor, txn_version);
      if (!wal_status.ok()) {
        return wal_status;
      }
    }
    
    mem_->Put(key, descriptor, txn_version);
    
    InvalidateQueryCache(key.entity_id());
    
    if (!disable_column_tracking_) {
      TrackColumnId(key.entity_id(), key.column_id());
    }
    
    // 自动 Flush 检查：当 memtable 达到阈值时触发
    if (mem_->ApproximateMemoryUsage() >= options_.memtable_threshold) {
      lock.unlock();
      MaybeScheduleFlush();
    }
  }
  
  return Status::OK();
}

Status LsmEngine::Put(uint64_t entity_id, uint64_t tx_time, const Slice& value, Timestamp txn_version) {
  CedarKey key(entity_id, EntityType::Vertex, 0, Timestamp(tx_time));
  auto desc_opt = Descriptor::InlineShortStr(0, value);
  if (!desc_opt.has_value()) {
    return Status::InvalidArgument("LsmEngine", "value too long for InlineShortStr, use ExternalRef");
  }
  return Put(key, *desc_opt, txn_version);
}

Status LsmEngine::Delete(const CedarKey& key, Timestamp txn_version) {
  if (!opened_) {
    return Status::InvalidArgument("LsmEngine", "not opened");
  }
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 先写 WAL，确保持久化成功后再修改 memtable
    if (wal_writer_) {
      Status wal_status = wal_writer_->WriteDelete(key, txn_version);
      if (!wal_status.ok()) {
        return wal_status;
      }
    }
    
    mem_->Put(key, Descriptor(), txn_version);
    
    InvalidateQueryCache(key.entity_id());
    if (!disable_column_tracking_) {
      TrackColumnId(key.entity_id(), key.column_id());
    }
    if (mem_->ApproximateMemoryUsage() >= options_.memtable_threshold) {
      lock.unlock();
      MaybeScheduleFlush();
    }
  }
  return Status::OK();
}

Status LsmEngine::Delete(uint64_t entity_id, uint64_t tx_time, Timestamp txn_version) {
  CedarKey key(entity_id, EntityType::Vertex, 0, Timestamp(tx_time));
  return Delete(key, txn_version);
}

Status LsmEngine::Get(uint64_t entity_id, uint64_t tx_time, std::string* value) {
  return Status::NotSupported("LsmEngine", "legacy Get not supported");
}

std::optional<Descriptor> LsmEngine::Get(const CedarKey& key) {
  if (!opened_) {
    return std::nullopt;
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 1. Query MemTable (hot data)
  Descriptor desc;
  Status s = mem_->Get(key.entity_id(), key.entity_type(), key.column_id(), 
                       key.timestamp(), &desc);
  if (s.ok()) {
    return desc;
  }

  // 2. Query Immutable MemTable
  if (imm_) {
    s = imm_->Get(key.entity_id(), key.entity_type(), key.column_id(), 
                  key.timestamp(), &desc);
    if (s.ok()) {
      return desc;
    }
  }
  
  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    // 获取覆盖该 Entity 的所有文件
    auto files = compaction_engine_->GetFilesForEntity(
        key.entity_id(), key.column_id(), static_cast<uint8_t>(key.entity_type()));
    
    // 按时间从新到旧排序（文件号大的通常更新）
    std::sort(files.begin(), files.end(), 
              [](const ZoneSstMeta& a, const ZoneSstMeta& b) {
                return a.file_number > b.file_number;
              });
    
    // 查询每个文件
    for (const auto& file_meta : files) {
      // 快速范围检查
      if (key.entity_id() < file_meta.min_entity_id || 
          key.entity_id() > file_meta.max_entity_id) {
        continue;
      }
      
      // 使用 SstReader 查询
      SstReader reader(file_meta.path);
      Status open_status = reader.Open();
      if (!open_status.ok()) {
        continue;
      }
      
      auto result = reader.Get(key);
      if (result.has_value()) {
        return result.value();
      }
    }
  }
  
  return std::nullopt;
}

std::vector<MemTableEntry> LsmEngine::GetAll(uint64_t entity_id,
                                              EntityType entity_type,
                                              uint16_t column_id) {
  std::vector<MemTableEntry> results;
  if (!opened_) {
    return results;
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 1. Query MemTable (hot data)
  auto mem_results = mem_->GetAll(entity_id, entity_type, column_id);
  results.insert(results.end(), mem_results.begin(), mem_results.end());

  // 2. Query Immutable MemTable
  if (imm_) {
    auto imm_results = imm_->GetAll(entity_id, entity_type, column_id);
    results.insert(results.end(), imm_results.begin(), imm_results.end());
  }
  
  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    for (const auto& file_meta : files) {
      // 快速范围检查
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      SstReader reader(file_meta.path);
      if (!reader.Open().ok()) {
        continue;
      }
      
      // 获取该 Entity 的所有版本
      auto range_results = reader.GetRange(entity_id, entity_type, column_id,
                                           Timestamp(0), Timestamp(UINT64_MAX));
      
      for (const auto& [key, descriptor] : range_results) {
        std::optional<uint64_t> dst_id = (key.target_id() != 0) 
            ? std::optional<uint64_t>(key.target_id()) 
            : std::nullopt;
        results.emplace_back(key.timestamp(), descriptor, dst_id, Timestamp(0));
      }
    }
  }
  
  // Sort by timestamp descending, then by dst_id for stable ordering
  std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
    if (a.timestamp.value() != b.timestamp.value()) {
      return a.timestamp.value() > b.timestamp.value();
    }
    // For edges, sort by dst_id to ensure stable ordering
    if (a.dst_id.has_value() && b.dst_id.has_value()) {
      return a.dst_id.value() < b.dst_id.value();
    }
    return a.dst_id.has_value() > b.dst_id.has_value();
  });
  
  // Remove duplicates - only if both timestamp AND dst_id are the same
  auto last = std::unique(results.begin(), results.end(), [](const auto& a, const auto& b) {
    if (a.timestamp.value() != b.timestamp.value()) return false;
    // For edges, also check dst_id
    if (a.dst_id.has_value() && b.dst_id.has_value()) {
      return a.dst_id.value() == b.dst_id.value();
    }
    return a.dst_id.has_value() == b.dst_id.has_value();
  });
  results.erase(last, results.end());
  
  return results;
}

std::optional<Descriptor> LsmEngine::GetAtTime(uint64_t entity_id,
                                               EntityType entity_type,
                                               uint16_t column_id,
                                               Timestamp timestamp) {
  if (!opened_) {
    return std::nullopt;
  }

  // Check query cache first
  if (query_cache_) {
    auto cached = query_cache_->Get(entity_id, column_id, timestamp.value());
    if (cached.has_value()) {
      return cached.value();
    }
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 1. Query MemTable (hot data)
  auto desc = mem_->GetAtTime(entity_id, entity_type, column_id, timestamp);
  if (desc.has_value()) {
    if (query_cache_) {
      query_cache_->Put(entity_id, column_id, timestamp.value(), desc);
    }
    return desc;
  }

  // 2. Query Immutable MemTable
  if (imm_) {
    desc = imm_->GetAtTime(entity_id, entity_type, column_id, timestamp);
    if (desc.has_value()) {
      if (query_cache_) {
        query_cache_->Put(entity_id, column_id, timestamp.value(), desc);
      }
      return desc;
    }
  }
  
  // 3. Query Accumulated Buffer (for accumulated flush mode)
  auto accumulated = QueryAccumulatedBuffer(entity_id, entity_type, column_id, timestamp);
  if (accumulated.has_value()) {
    if (query_cache_) {
      query_cache_->Put(entity_id, column_id, timestamp.value(), accumulated->second);
    }
    return accumulated->second;
  }
  
  // 4. Query SST files
  // 收集所有匹配的条目
  std::vector<std::pair<CedarKey, Descriptor>> all_entries;
  
  // 从 Compaction Engine 获取文件列表（如果可用）
  if (compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    for (const auto& file_meta : files) {
      // 快速范围检查
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      // Temporal Bloom Filter 检查
      if (!file_meta.temporal_filter_metadata.empty()) {
        auto filter_opt = TemporalBloomFilter::Deserialize(file_meta.temporal_filter_metadata);
        if (filter_opt.has_value() && 
            !filter_opt.value().MayContain(entity_id)) {
          continue;
        }
      }
      
      // 使用缓存的 Reader
      std::shared_ptr<SstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        // 缓存未命中，创建新的
        reader = std::make_shared<SstReader>(file_meta.path);
        if (!reader->Open().ok()) {
          continue;
        }
      }
      
      // 使用 GetRange 获取该 Entity 的所有版本
      auto range_results = reader->GetRange(entity_id, entity_type, column_id,
                                           Timestamp(0), Timestamp(UINT64_MAX));
      all_entries.insert(all_entries.end(), range_results.begin(), range_results.end());
      
      // 释放 Reader（减少引用计数）
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
    }
  }
  
  // 从本地 levels_ 获取文件列表（当 compaction_engine_ 不可用时）
  if (all_entries.empty()) {
    // mutex_ is already locked by the outer shared_lock at line 457
    for (const auto& level : levels_) {
      for (const auto& meta : level) {
        // 支持跨列存储的文件
        bool column_match = (meta.column_id == column_id) || (meta.column_id == UINT16_MAX);
        bool type_match = (meta.entity_type == static_cast<uint8_t>(entity_type)) || 
                         (meta.entity_type == 0);
        
        if (!column_match || !type_match) {
          continue;
        }
        
        // 范围检查
        if (entity_id < meta.min_entity_id || entity_id > meta.max_entity_id) {
          continue;
        }
        
        // 构建文件路径
        std::string filepath = SstFilePath(meta.file_number);
        
        // 使用 SstReader 查询
        SstReader reader(filepath);
        Status open_status = reader.Open();
        if (!open_status.ok()) {
          continue;
        }
        
        auto range_results = reader.GetRange(entity_id, entity_type, column_id,
                                             Timestamp(0), Timestamp(UINT64_MAX));
        all_entries.insert(all_entries.end(), range_results.begin(), range_results.end());
      }
    }
  }
  
  if (!all_entries.empty()) {
    // 增量处理：直接遍历找最佳匹配，避免全量排序
    std::optional<Descriptor> best_descriptor;
    uint64_t best_ts = 0;
    
    for (const auto& [key, descriptor] : all_entries) {
      uint64_t ts = key.timestamp().value();
      if (ts <= timestamp.value() && ts >= best_ts) {
        best_ts = ts;
        best_descriptor = descriptor;
      }
    }
    
    if (best_descriptor.has_value()) {
      if (query_cache_) {
        query_cache_->Put(entity_id, column_id, timestamp.value(), best_descriptor);
      }
      return best_descriptor;
    }
  }
  
  // Cache the negative result to avoid repeated SST scans
  if (query_cache_) {
    query_cache_->Put(entity_id, column_id, timestamp.value(), std::nullopt);
  }
  return std::nullopt;
}

std::optional<std::pair<CedarKey, Descriptor>> LsmEngine::GetRecordAtTime(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    Timestamp timestamp) {
  if (!opened_) {
    return std::nullopt;
  }

  // Helper lambda to build CedarKey based on entity type
  // 使用 MemTableEntry 中存储的完整 metadata
  auto BuildKey = [entity_id, entity_type, column_id](const MemTableEntry& entry) -> CedarKey {
    if (entity_type == EntityType::Vertex) {
      return CedarKey::Vertex(entity_id, column_id, entry.timestamp, 
                              entry.sequence, entry.part_id, 
                              entry.dst_id.value_or(0), entry.flags);
    } else if (entity_type == EntityType::EdgeOut) {
      return CedarKey::EdgeOut(entity_id, entry.dst_id.value_or(0), 
                               EdgeTypeId(column_id), entry.timestamp, 
                               entry.sequence, entry.part_id, entry.flags);
    } else {  // EdgeIn
      return CedarKey::EdgeIn(entity_id, entry.dst_id.value_or(0),
                              EdgeTypeId(column_id), entry.timestamp, 
                              entry.sequence, entry.part_id, entry.flags);
    }
  };

  // 1. Query MemTable (hot data)
  auto mem_results = mem_->GetRange(entity_id, entity_type, column_id, 
                                     Timestamp(0), timestamp);
  if (!mem_results.empty()) {
    const auto& entry = mem_results.front();  // front() = latest version (descending order)
    return std::make_pair(BuildKey(entry), entry.descriptor);
  }

  // 2. Query Immutable MemTable
  if (imm_) {
    auto imm_results = imm_->GetRange(entity_id, entity_type, column_id,
                                       Timestamp(0), timestamp);
    if (!imm_results.empty()) {
      const auto& entry = imm_results.front();  // front() = latest version
      return std::make_pair(BuildKey(entry), entry.descriptor);
    }
  }
  
  // 3. Query Accumulated Buffer (for accumulated flush mode)
  auto accumulated = QueryAccumulatedBuffer(entity_id, entity_type, column_id, timestamp);
  if (accumulated.has_value()) {
    return accumulated;
  }
  
  // 4. Query SST files via Size-Tiered Compaction Engine
  // SST files store complete CedarKey with all metadata
  if (compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    // Collect all matching entries
    std::vector<std::pair<CedarKey, Descriptor>> all_entries;
    
    for (const auto& file_meta : files) {
      // Quick range check
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      // Use cached Reader
      std::shared_ptr<SstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        reader = std::make_shared<SstReader>(file_meta.path);
        if (!reader->Open().ok()) {
          continue;
        }
      }
      
      // Use GetRange to get all versions of this Entity
      auto range_results = reader->GetRange(entity_id, entity_type, column_id,
                                           Timestamp(0), timestamp);
      all_entries.insert(all_entries.end(), range_results.begin(), range_results.end());
      
      // Release Reader
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
    }
    
    // Sort by timestamp descending
    std::sort(all_entries.begin(), all_entries.end(),
              [](const auto& a, const auto& b) {
                return a.first.timestamp().value() > b.first.timestamp().value();
              });
    
    // Find the version at specified time (latest version <= timestamp)
    for (const auto& [key, descriptor] : all_entries) {
      if (key.timestamp().value() <= timestamp.value()) {
        return std::make_pair(key, descriptor);
      }
    }
  }
  
  return std::nullopt;
}

std::vector<MemTableEntry> LsmEngine::GetRange(uint64_t entity_id,
                                                EntityType entity_type,
                                                uint16_t column_id,
                                                Timestamp start,
                                                Timestamp end) {
  std::vector<MemTableEntry> results;
  if (!opened_) {
    return results;
  }

  // 1. Query MemTable (hot data)
  auto mem_results = mem_->GetRange(entity_id, entity_type, column_id, start, end);
  results.insert(results.end(), mem_results.begin(), mem_results.end());

  // 2. Query Immutable MemTable
  if (imm_) {
    auto imm_results = imm_->GetRange(entity_id, entity_type, column_id, start, end);
    results.insert(results.end(), imm_results.begin(), imm_results.end());
  }
  
  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    for (const auto& file_meta : files) {
      // 快速范围检查
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      // 时间范围快速检查
      if (file_meta.max_timestamp < start.value() || 
          file_meta.min_timestamp > end.value()) {
        continue;
      }
      
      // 使用缓存的 Reader
      std::shared_ptr<SstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        reader = std::make_shared<SstReader>(file_meta.path);
        if (!reader->Open().ok()) {
          continue;
        }
      }
      
      // 使用 GetRange 获取时间范围内的版本
      auto range_results = reader->GetRange(entity_id, entity_type, column_id, start, end);
      
      // 释放 Reader
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
      
      for (const auto& [key, descriptor] : range_results) {
        std::optional<uint64_t> dst_id = (key.target_id() != 0) 
            ? std::optional<uint64_t>(key.target_id()) 
            : std::nullopt;
        results.emplace_back(key.timestamp(), descriptor, dst_id, Timestamp(0));
      }
    }
  }
  
  return results;
}

std::vector<MemTableEntry> LsmEngine::GetRangeLimit(uint64_t entity_id,
                                                     EntityType entity_type,
                                                     uint16_t column_id,
                                                     Timestamp start,
                                                     Timestamp end,
                                                     size_t max_results) {
  if (!opened_) {
    return {};
  }

  std::shared_lock<std::shared_mutex> lock(mutex_);

  // OPTIMIZATION: 追踪查询模式（用于识别热数据）
  TrackQueryPattern(entity_id, entity_type, column_id);
  
  // OPTIMIZATION: 尝试从跨查询缓存获取（如果是全范围查询）
  if (start.value() == 0 && end.value() == UINT64_MAX) {
    auto cached = GetFromCrossQueryCache(entity_id, column_id);
    if (cached.has_value()) {
      return cached.value();
    }
  }
  
  // OPTIMIZATION: 预读取热数据到缓存
  PrefetchHotData(entity_id, entity_type, column_id);

  std::vector<MemTableEntry> result;
  // 修复: 只有当 max_results 是合理值时才预分配，避免 SIZE_MAX 导致内存分配失败
  if (max_results > 0 && max_results < 1000000) {
    result.reserve(max_results);
  } else if (max_results > 0) {
    result.reserve(1024);  // 使用合理的默认值
  }
  
  // 1. Query MemTable (hot data)
  result = mem_->GetRange(entity_id, entity_type, column_id, start, end);
  if (result.size() >= max_results) {
    result.resize(max_results);
    return result;
  }

  // 2. Query Immutable MemTable
  if (imm_) {
    auto imm_result = imm_->GetRange(entity_id, entity_type, column_id, start, end);
    for (const auto& entry : imm_result) {
      if (result.size() >= max_results) {
        return result;
      }
      result.push_back(entry);
    }
  }
  
  // 3. Query Accumulated Buffer (for accumulated flush mode)
  if (result.size() < max_results && !accumulated_entries_.empty()) {
    std::lock_guard<std::mutex> lock(accumulated_mutex_);
    for (const auto& [key, descriptor] : accumulated_entries_) {
      if (result.size() >= max_results) break;
      if (key.entity_id() != entity_id) continue;
      if (static_cast<uint8_t>(key.entity_type()) != static_cast<uint8_t>(entity_type)) continue;
      if (key.column_id() != column_id) continue;
      if (key.timestamp().value() < start.value() || key.timestamp().value() > end.value()) continue;
      result.emplace_back(key.timestamp(), descriptor, std::nullopt, Timestamp(0));
    }
  }

  // 4. Query SST files via Size-Tiered Compaction Engine
  if (result.size() < max_results && compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    // OPTIMIZATION: 按时间戳范围排序文件，优先查询时间范围匹配度高的文件
    // 同时限制最大查询文件数，避免扫描过多文件
    std::vector<std::pair<ZoneSstMeta, uint64_t>> files_with_overlap;
    files_with_overlap.reserve(files.size());
    
    for (const auto& file_meta : files) {
      // 快速范围检查: entity_id
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      // 快速范围检查: 时间戳范围
      if (file_meta.max_timestamp < start.value() || 
          file_meta.min_timestamp > end.value()) {
        continue;
      }
      
      // Temporal Bloom Filter 检查: 快速排除不包含该 entity 时间范围的文件
      if (!file_meta.temporal_filter_metadata.empty()) {
        auto filter_opt = TemporalBloomFilter::Deserialize(file_meta.temporal_filter_metadata);
        if (filter_opt.has_value() && 
            !filter_opt.value().MayExistInRange(entity_id, start, end)) {
          continue;  // 该文件肯定不包含目标数据
        }
      }
      
      // 计算时间范围重叠度（用于排序）
      uint64_t overlap_start = std::max(file_meta.min_timestamp, start.value());
      uint64_t overlap_end = std::min(file_meta.max_timestamp, end.value());
      uint64_t overlap = (overlap_end > overlap_start) ? (overlap_end - overlap_start) : 0;
      
      files_with_overlap.push_back({file_meta, overlap});
    }
    
    // 按时间范围重叠度降序排序（优先查询重叠度高的文件）
    std::sort(files_with_overlap.begin(), files_with_overlap.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    
    // OPTIMIZATION: 限制最大查询文件数
    // 对于时间范围查询，通常只需要检查最近的几个文件
    const size_t kMaxFilesToQuery = 10;
    size_t files_queried = 0;
    
    for (const auto& [file_meta, overlap] : files_with_overlap) {
      if (result.size() >= max_results) {
        break;
      }
      
      if (++files_queried > kMaxFilesToQuery) {
        break;  // 已达到最大文件查询限制
      }
      
      // OPTIMIZATION: 使用缓存的 Reader（如果可用）
      std::shared_ptr<SstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        reader = std::make_shared<SstReader>(file_meta.path);
        if (!reader->Open().ok()) {
          continue;
        }
      }
      
      auto range_results = reader->GetRange(entity_id, entity_type, column_id, start, end);
      
      // 释放 Reader
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
      
      for (const auto& [key, descriptor] : range_results) {
        if (result.size() >= max_results) {
          break;
        }
        std::optional<uint64_t> dst_id = (key.target_id() != 0) 
            ? std::optional<uint64_t>(key.target_id()) 
            : std::nullopt;
        result.emplace_back(key.timestamp(), descriptor, dst_id, Timestamp(0));
      }
    }
  }
  
  // OPTIMIZATION: 将全范围查询结果缓存
  if (start.value() == 0 && end.value() == UINT64_MAX && !result.empty()) {
    AddToCrossQueryCache(entity_id, column_id, result);
  }
  
  return result;
}

void LsmEngine::BatchGetAtTime(std::vector<BatchQueryItem>& items) {
  for (auto& item : items) {
    item.result = GetAtTime(item.entity_id, item.entity_type, item.column_id, item.timestamp);
  }
}

void LsmEngine::BatchGetRange(std::vector<BatchRangeItem>& items) {
  for (auto& item : items) {
    item.results = GetRangeLimit(item.entity_id, item.entity_type, item.column_id, 
                                  item.start, item.end, item.max_results);
  }
}

// OPTIMIZATION: 并行 BatchGetRange - 使用线程池加速多 entity 查询
void LsmEngine::ParallelBatchGetRange(std::vector<BatchRangeItem>& items, 
                                       size_t num_threads) {
  if (items.empty()) return;
  
  // 如果只有少量查询，使用串行版本
  if (items.size() < num_threads * 2) {
    BatchGetRange(items);
    return;
  }
  
  // 限制线程数
  num_threads = std::min(num_threads, items.size());
  
  // 计算每个线程处理的查询数量
  size_t items_per_thread = items.size() / num_threads;
  size_t remainder = items.size() % num_threads;
  
  std::vector<std::thread> threads;
  size_t start_idx = 0;
  
  for (size_t t = 0; t < num_threads; ++t) {
    size_t count = items_per_thread + (t < remainder ? 1 : 0);
    if (count == 0) break;
    
    threads.emplace_back([this, &items, start_idx, count]() {
      for (size_t i = start_idx; i < start_idx + count; ++i) {
        auto& item = items[i];
        item.results = GetRangeLimit(item.entity_id, item.entity_type, item.column_id,
                                      item.start, item.end, item.max_results);
      }
    });
    
    start_idx += count;
  }
  
  // 等待所有线程完成
  for (auto& t : threads) {
    t.join();
  }
}

// OPTIMIZATION: P0 - 优化的批量时间范围查询
// 使用 SST 层的 BatchGetRange 减少文件扫描次数
std::unordered_map<uint64_t, std::vector<MemTableEntry>> 
LsmEngine::BatchGetRangeOptimized(const std::vector<uint64_t>& entity_ids,
                                   EntityType entity_type,
                                   uint16_t column_id,
                                   Timestamp start,
                                   Timestamp end,
                                   size_t max_results_per_entity) {
  std::unordered_map<uint64_t, std::vector<MemTableEntry>> results;
  
  if (!opened_ || entity_ids.empty()) return results;
  
  // 预分配结果空间
  for (uint64_t eid : entity_ids) {
    results[eid].reserve(max_results_per_entity);
  }
  
  // 1. 从 MemTable 查询（热数据）
  for (uint64_t eid : entity_ids) {
    auto mem_results = mem_->GetRange(eid, entity_type, column_id, start, end);
    for (const auto& entry : mem_results) {
      if (results[eid].size() >= max_results_per_entity) break;
      results[eid].push_back(entry);
    }
  }
  
  // 2. 从 Immutable MemTable 查询
  if (imm_) {
    for (uint64_t eid : entity_ids) {
      if (results[eid].size() >= max_results_per_entity) continue;
      
      auto imm_results = imm_->GetRange(eid, entity_type, column_id, start, end);
      for (const auto& entry : imm_results) {
        if (results[eid].size() >= max_results_per_entity) break;
        results[eid].push_back(entry);
      }
    }
  }
  
  // 3. 从 SST 文件批量查询
  if (compaction_engine_) {
    // 获取所有 entity 相关的 SST 文件并合并去重
    std::unordered_set<std::string> file_paths_seen;
    std::vector<ZoneSstMeta> files;
    for (uint64_t eid : entity_ids) {
      auto entity_files = compaction_engine_->GetFilesForEntity(
          eid, column_id, static_cast<uint8_t>(entity_type));
      for (const auto& meta : entity_files) {
        if (file_paths_seen.insert(meta.path).second) {
          files.push_back(meta);
        }
      }
    }
    
    // 过滤时间范围相关的文件
    std::vector<ZoneSstMeta> relevant_files;
    relevant_files.reserve(files.size());
    for (const auto& file_meta : files) {
      if (file_meta.max_timestamp < start.value() || 
          file_meta.min_timestamp > end.value()) {
        continue;  // 时间范围不重叠
      }
      relevant_files.push_back(file_meta);
    }
    
    // 限制文件数量
    const size_t kMaxFilesToQuery = 10;
    if (relevant_files.size() > kMaxFilesToQuery) {
      relevant_files.resize(kMaxFilesToQuery);
    }
    
    // 对每个 SST 文件使用批量查询
    for (const auto& file_meta : relevant_files) {
      // 检查是否所有 entity 都已经收集够数据
      bool all_full = true;
      for (uint64_t eid : entity_ids) {
        if (results[eid].size() < max_results_per_entity) {
          all_full = false;
          break;
        }
      }
      if (all_full) break;
      
      // 使用缓存的 Reader
      std::shared_ptr<SstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        reader = std::make_shared<SstReader>(file_meta.path);
        if (!reader->Open().ok()) continue;
      }
      
      // 批量查询该文件中的所有 entity
      auto file_results = reader->BatchGetRange(entity_ids, entity_type, column_id, start, end);
      
      // 合并结果
      for (const auto& [eid, entries] : file_results) {
        if (results[eid].size() >= max_results_per_entity) continue;
        
        for (const auto& [key, desc] : entries) {
          if (results[eid].size() >= max_results_per_entity) break;
          
          std::optional<uint64_t> dst_id = (key.target_id() != 0) 
              ? std::optional<uint64_t>(key.target_id()) 
              : std::nullopt;
          results[eid].emplace_back(key.timestamp(), desc, dst_id, Timestamp(0));
        }
      }
      
      // 释放 Reader
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
    }
  }
  
  return results;
}

// OPTIMIZATION: P2 - 并行查询多个 SST 文件
std::unordered_map<uint64_t, std::vector<MemTableEntry>>
LsmEngine::ParallelGetRangeFromSST(const std::vector<uint64_t>& entity_ids,
                                     EntityType entity_type,
                                     uint16_t column_id,
                                     Timestamp start,
                                     Timestamp end,
                                     size_t max_results_per_entity,
                                     size_t num_threads) {
  std::unordered_map<uint64_t, std::vector<MemTableEntry>> results;
  
  if (!opened_ || !compaction_engine_ || entity_ids.empty()) return results;
  
  // 预分配结果空间
  for (uint64_t eid : entity_ids) {
    results[eid].reserve(max_results_per_entity);
  }
  
  // 获取相关 SST 文件
  auto files = compaction_engine_->GetFilesForEntity(
      entity_ids[0], column_id, static_cast<uint8_t>(entity_type));
  
  // 过滤时间范围相关的文件
  std::vector<ZoneSstMeta> relevant_files;
  for (const auto& file_meta : files) {
    if (file_meta.max_timestamp < start.value() || 
        file_meta.min_timestamp > end.value()) {
      continue;
    }
    relevant_files.push_back(file_meta);
  }
  
  if (relevant_files.empty()) return results;
  
  // 限制文件数量
  const size_t kMaxFilesToQuery = 20;
  if (relevant_files.size() > kMaxFilesToQuery) {
    relevant_files.resize(kMaxFilesToQuery);
  }
  
  // 如果文件数量少，使用串行查询
  if (relevant_files.size() <= 2) {
    return BatchGetRangeOptimized(entity_ids, entity_type, column_id, 
                                   start, end, max_results_per_entity);
  }
  
  // 并行查询多个文件
  num_threads = std::min(num_threads, relevant_files.size());
  size_t files_per_thread = relevant_files.size() / num_threads;
  size_t remainder = relevant_files.size() % num_threads;
  
  // 每个线程的结果
  std::vector<std::unordered_map<uint64_t, std::vector<std::pair<CedarKey, Descriptor>>>>
      thread_results(num_threads);
  
  std::vector<std::thread> threads;
  size_t start_idx = 0;
  
  // 构建 entity_set 用于快速查找
  std::unordered_set<uint64_t> entity_set(entity_ids.begin(), entity_ids.end());
  
  for (size_t t = 0; t < num_threads; ++t) {
    size_t count = files_per_thread + (t < remainder ? 1 : 0);
    if (count == 0) continue;
    
    threads.emplace_back([this, &relevant_files, &entity_set, &thread_results, 
                          t, start_idx, count, entity_type, column_id, start, end]() {
      for (size_t i = start_idx; i < start_idx + count; ++i) {
        const auto& file_meta = relevant_files[i];
        
        // 使用缓存的 Reader
        std::shared_ptr<SstReader> reader;
        if (sst_reader_cache_) {
          reader = sst_reader_cache_->Get(file_meta.path);
        }
        if (!reader) {
          reader = std::make_shared<SstReader>(file_meta.path);
          if (!reader->Open().ok()) continue;
        }
        
        // 获取时间范围内的所有数据
        auto positions = reader->GetRange(0, entity_type, column_id, start, end);
        
        // 过滤 entity_ids 中的数据
        for (const auto& [key, desc] : positions) {
          if (entity_set.find(key.entity_id()) != entity_set.end()) {
            thread_results[t][key.entity_id()].emplace_back(key, desc);
          }
        }
        
        // 释放 Reader
        if (sst_reader_cache_) {
          sst_reader_cache_->Release(file_meta.path);
        }
      }
    });
    
    start_idx += count;
  }
  
  // 等待所有线程完成
  for (auto& t : threads) {
    t.join();
  }
  
  // 合并结果
  for (size_t t = 0; t < num_threads; ++t) {
    for (const auto& [eid, entries] : thread_results[t]) {
      if (results[eid].size() >= max_results_per_entity) continue;
      
      for (const auto& [key, desc] : entries) {
        if (results[eid].size() >= max_results_per_entity) break;
        
        std::optional<uint64_t> dst_id = (key.target_id() != 0) 
            ? std::optional<uint64_t>(key.target_id()) 
            : std::nullopt;
        results[eid].emplace_back(key.timestamp(), desc, dst_id, Timestamp(0));
      }
    }
  }
  
  return results;
}

std::vector<LsmEngine::TemporalVersion> LsmEngine::GetTemporalChain(uint64_t entity_id,
                                                                     EntityType entity_type,
                                                                     uint16_t column_id) {
  std::vector<TemporalVersion> result;
  
  // First, get from MemTable (hot data)
  auto mem_entries = mem_->GetAll(entity_id, entity_type, column_id);
  result.reserve(mem_entries.size());
  for (const auto& entry : mem_entries) {
    result.push_back({entry.timestamp, entry.descriptor, -1});
  }
  
  // Sort by timestamp descending
  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
    return a.timestamp.value() > b.timestamp.value();
  });
  
  return result;
}

void LsmEngine::TraverseTemporalChain(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       std::function<bool(const TemporalVersion&)> callback) {
  auto chain = GetTemporalChain(entity_id, entity_type, column_id);
  for (const auto& version : chain) {
    if (!callback(version)) {
      break;
    }
  }
}

Status LsmEngine::ForceFlush() {
  // 等待任何后台 Flush 完成
  while (true) {
    {
      std::unique_lock<std::mutex> flush_lock(flush_completion_mutex_);
      flush_completion_cv_.wait(flush_lock, [this]() {
        return active_flush_count_.load() == 0;
      });
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!imm_) {
      break;  // 安全，没有后台 flush 在进行
    }
    // 后台线程可能刚设置 imm_ 但还没增加计数器，释放锁再等待一轮
    lock.unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);

  if (mem_->IsEmpty()) {
    return Status::OK();
  }

  imm_ = std::move(mem_);
  mem_ = std::make_unique<VSLMemTable>();

  VSLMemTable* imm = imm_.get();
  lock.unlock();
  
  // 增加计数器，让 Close 知道有 Flush 在进行
  active_flush_count_.fetch_add(1);
  Status s = FlushMemTable(imm);
  // 减少计数器并通知等待者（使用 RAII 确保即使异常也能执行）
  active_flush_count_.fetch_sub(1);
  flush_completion_cv_.notify_all();

  lock.lock();
  imm_.reset();

  return s;
}

void LsmEngine::MaybeScheduleFlush() {
  if (shutdown_.load() || disable_auto_flush_.load()) {
    return;
  }
  
  // 尝试获取锁检查是否可以 Flush
  std::unique_lock<std::shared_mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    // 其他操作正在进行，跳过本次 Flush
    return;
  }
  
  if (imm_ || mem_->IsEmpty()) {
    // 已有 Flush 在进行中，或 memtable 为空
    return;
  }
  
  // 将 memtable 转为 immutable
  imm_ = std::move(mem_);
  mem_ = std::make_unique<VSLMemTable>();
  
  VSLMemTable* imm = imm_.get();
  
  // 先增加计数再释放锁，避免 Close() 在计数为 0 时看到 imm_ 非空
  active_flush_count_.fetch_add(1);
  lock.unlock();
  auto flush_task = [this, imm]() noexcept {
    try {
      Status s = FlushMemTable(imm);
      if (!s.ok()) {
        std::cerr << "[LsmEngine] FlushMemTable failed: " << s.ToString() << std::endl;
      }
    } catch (const std::exception& e) {
      std::cerr << "[LsmEngine] FlushMemTable exception: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "[LsmEngine] FlushMemTable unknown exception" << std::endl;
    }

    {
      std::unique_lock<std::shared_mutex> cleanup_lock(mutex_);
      imm_.reset();
      if (compaction_engine_) {
        compaction_engine_->ScheduleCompaction();
      }
    }
    active_flush_count_.fetch_sub(1);
    flush_completion_cv_.notify_all();
  };

  try {
    flush_future_ = std::async(std::launch::async, flush_task);
  } catch (const std::exception& e) {
    std::cerr << "[MaybeScheduleFlush] std::async failed: " << e.what()
              << " — falling back to sync flush" << std::endl;
    flush_task();
  }
}

Status LsmEngine::Compact() {
  // Stub implementation
  return Status::OK();
}

// ============================================================================
// 超大 SST 累积 Flush（生成大文件优化）
// ============================================================================

void LsmEngine::EnableAccumulatedFlush(size_t target_size_bytes) {
  std::lock_guard<std::mutex> lock(accumulated_mutex_);
  accumulated_flush_enabled_ = true;
  accumulated_flush_target_size_ = target_size_bytes;
  // Accumulated flush enabled
}

void LsmEngine::DisableAccumulatedFlush() {
  std::lock_guard<std::mutex> lock(accumulated_mutex_);
  accumulated_flush_enabled_ = false;
  // Accumulated flush disabled
}

size_t LsmEngine::GetAccumulatedSize() const {
  std::lock_guard<std::mutex> lock(accumulated_mutex_);
  return accumulated_bytes_;
}

std::optional<std::pair<CedarKey, Descriptor>> LsmEngine::QueryAccumulatedBuffer(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    Timestamp timestamp) const {
  std::lock_guard<std::mutex> lock(accumulated_mutex_);

  if (accumulated_entries_.empty()) {
    return std::nullopt;
  }

  std::optional<std::pair<CedarKey, Descriptor>> best_match;
  for (const auto& [key, descriptor] : accumulated_entries_) {
    if (key.entity_id() != entity_id) continue;
    if (key.entity_type() != entity_type) continue;
    if (key.column_id() != column_id) continue;
    if (key.timestamp().value() > timestamp.value()) continue;
    
    // Keep the latest version (largest timestamp <= query timestamp)
    if (!best_match.has_value() ||
        key.timestamp().value() > best_match->first.timestamp().value()) {
      best_match = std::make_pair(key, descriptor);
    }
  }
  
  return best_match;
}

Status LsmEngine::FlushAccumulated() {
  std::unique_lock<std::mutex> lock(accumulated_mutex_);
  
  if (accumulated_entries_.empty()) {
    return Status::OK();
  }
  
  // 复制数据并清空缓冲区
  auto entries_to_flush = std::move(accumulated_entries_);
  accumulated_bytes_ = 0;
  lock.unlock();
  
  // Flushing accumulated entries
  
  // 创建单个 SST 文件
  uint64_t file_number = next_file_number_.fetch_add(1);
  std::string filepath = SstFilePath(file_number);
  
  WritableFile* file = nullptr;
  Status ls = env_->NewWritableFile(filepath, &file);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }
  
  // 排序
  std::sort(entries_to_flush.begin(), entries_to_flush.end(),
    [](const auto& a, const auto& b) { 
      return a.first.LessForSorting(b.first); 
    });
  
  // 使用 SST Builder
  auto builder = SstBuilderFactory::Create(file, db_path_);
  
  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  uint64_t min_ts = UINT64_MAX;
  uint64_t max_ts = 0;
  
  for (const auto& [key, desc] : entries_to_flush) {
    builder->Add(key, desc);
    min_entity_id = std::min(min_entity_id, key.entity_id());
    max_entity_id = std::max(max_entity_id, key.entity_id());
    min_ts = std::min(min_ts, key.timestamp().value());
    max_ts = std::max(max_ts, key.timestamp().value());
  }
  
  Status s = builder->Finish();
  delete file;
  
  if (!s.ok()) {
    env_->RemoveFile(filepath);
    return s;
  }
  
  // 获取文件大小
  uint64_t file_size = 0;
  ls = env_->GetFileSize(filepath, &file_size);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }
  
  // 创建 Meta
  SSTFileMeta meta;
  meta.file_number = file_number;
  meta.file_size = file_size;
  meta.num_entries = builder->NumEntries();
  meta.min_entity_id = min_entity_id;
  meta.max_entity_id = max_entity_id;
  meta.min_tx_time = min_ts;
  meta.max_tx_time = max_ts;
  meta.level = 0;
  meta.column_id = UINT16_MAX;  // 跨 Column
  meta.entity_type = 0;
  
  {
    std::lock_guard<std::shared_mutex> level_lock(mutex_);
    levels_[0].push_back(meta);
  }
  BuildColumnFileIndex();
  
  // 注册到 Compaction Engine
  if (compaction_engine_) {
    ZoneSstMeta zone_meta;
    zone_meta.file_number = file_number;
    zone_meta.file_size = file_size;
    zone_meta.num_entries = builder->NumEntries();
    zone_meta.level = 0;
    zone_meta.min_entity_id = min_entity_id;
    zone_meta.max_entity_id = max_entity_id;
    zone_meta.min_timestamp = min_ts;
    zone_meta.max_timestamp = max_ts;
    zone_meta.column_id = UINT16_MAX;
    zone_meta.entity_type = 0;
    zone_meta.path = filepath;
    zone_meta.blob_path = db_path_ + "/sst_" + std::to_string(file_number) + ".blob";
    zone_meta.temporal_filter_metadata = builder->GetTemporalFilterData();
    
    compaction_engine_->AddSSTFile(zone_meta);
    compaction_engine_->ScheduleCompaction();
  }
  
  // 增量更新 Partition Index
  if (partition_index_) {
    auto s = partition_index_->IndexSSTFile(file_number);
    if (!s.ok()) {
      LOG(WARNING) << "PartitionIndex::IndexSSTFile failed for file " << file_number
                   << ": " << s.ToString();
    }
  }
  
  // Accumulated flush complete
  
  return Status::OK();
}

// ========== 立即 Flush 模式：将所有条目写入单个 SST 文件 ==========
Status LsmEngine::FlushEntriesToSST(std::vector<std::pair<CedarKey, Descriptor>> entries) {
  if (entries.empty()) {
    return Status::OK();
  }

  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  uint64_t min_tx_time = UINT64_MAX;
  uint64_t max_tx_time = 0;
  
  for (const auto& [key, desc] : entries) {
    min_entity_id = std::min(min_entity_id, key.entity_id());
    max_entity_id = std::max(max_entity_id, key.entity_id());
    min_tx_time = std::min(min_tx_time, key.timestamp().value());
    max_tx_time = std::max(max_tx_time, key.timestamp().value());
  }
  
  // 排序（全局有序）
  std::sort(entries.begin(), entries.end(),
    [](const auto& a, const auto& b) { return a.first.LessForSorting(b.first); });
  
  // 创建单个 SST 文件
  uint64_t file_number = next_file_number_.fetch_add(1);
  std::string filepath = SstFilePath(file_number);
  std::cerr << "[FlushEntriesToSST] Creating SST file: " << filepath << " with " << entries.size() << " entries" << std::endl;
  
  cedar::WritableFile* file = nullptr;
  cedar::Status ls = env_->NewWritableFile(filepath, &file);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }
  
  // 使用 Builder
  auto builder = SstBuilderFactory::Create(file, db_path_);
  
  for (const auto& [key, descriptor] : entries) {
    builder->Add(key, descriptor);
  }
  
  Status fs = builder->Finish();
  delete file;
  
  if (!fs.ok()) {
    env_->RemoveFile(filepath);
    return fs;
  }
  
  uint64_t file_size = 0;
  ls = env_->GetFileSize(filepath, &file_size);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }
  
  // 创建 Meta（跨 Column ID）
  SSTFileMeta meta;
  meta.file_number = file_number;
  meta.file_size = file_size;
  meta.num_entries = builder->NumEntries();
  meta.min_entity_id = min_entity_id;
  meta.max_entity_id = max_entity_id;
  meta.min_tx_time = min_tx_time;
  meta.max_tx_time = max_tx_time;
  meta.level = 0;
  meta.column_id = UINT16_MAX;  // 跨 Column ID
  meta.entity_type = 0;         // 混合 Entity Type
  meta.temporal_filter_metadata = builder->GetTemporalFilterData();
  
  {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    levels_[0].push_back(meta);
  }
  BuildColumnFileIndex();
  
  // 注册到 Compaction Engine
  if (compaction_engine_) {
    ZoneSstMeta zone_meta;
    zone_meta.file_number = file_number;
    zone_meta.file_size = file_size;
    zone_meta.num_entries = builder->NumEntries();
    zone_meta.level = 0;
    zone_meta.min_entity_id = min_entity_id;
    zone_meta.max_entity_id = max_entity_id;
    zone_meta.min_timestamp = min_tx_time;
    zone_meta.max_timestamp = max_tx_time;
    zone_meta.column_id = UINT16_MAX;
    zone_meta.entity_type = 0;
    zone_meta.path = filepath;
    zone_meta.blob_path = db_path_ + "/sst_" + std::to_string(file_number) + ".blob";
    
    Status cs = compaction_engine_->AddSSTFile(zone_meta);
    if (!cs.ok()) {
      // 日志记录
    }
    
    compaction_engine_->ScheduleCompaction();
  }
  
  // 增量更新 Partition Index
  if (partition_index_) {
    auto s = partition_index_->IndexSSTFile(file_number);
    if (!s.ok()) {
      LOG(WARNING) << "PartitionIndex::IndexSSTFile failed for file " << file_number
                   << ": " << s.ToString();
    }
  }
  
  return Status::OK();
}

LsmEngine::Stats LsmEngine::GetStats() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  Stats stats;
  stats.memtable_size = mem_->size();
  if (imm_) {
    stats.imm_memtable_size = imm_->size();
  }
  stats.num_levels = levels_.size();
  for (const auto& level : levels_) {
    stats.sst_count += level.size();
    for (const auto& meta : level) {
      stats.sst_size += meta.file_size;
    }
  }
  return stats;
}

void LsmEngine::TrackColumnId(uint64_t entity_id, uint16_t column_id) {
  if (batch_tracking_enabled_) {
    size_t idx = batch_buffer_index_.fetch_add(1);
    if (idx < kTrackBatchSize) {
      batch_buffer_[idx] = {entity_id, column_id};
    } else {
      // Buffer overflow guard: prevent unbounded index growth
      batch_buffer_index_.fetch_sub(1);
    }
    return;
  }
  
  std::unique_lock<std::shared_mutex> lock(column_map_mutex_);
  entity_column_map_[entity_id].Add(column_id);
}

void LsmEngine::FlushColumnIdBatch() {
  if (!batch_tracking_enabled_) return;
  
  size_t count = batch_buffer_index_.load();
  if (count == 0) return;
  
  std::unique_lock<std::shared_mutex> lock(column_map_mutex_);
  for (size_t i = 0; i < count && i < kTrackBatchSize; i++) {
    entity_column_map_[batch_buffer_[i].first].Add(batch_buffer_[i].second);
  }
  batch_buffer_index_.store(0);
}

Status LsmEngine::InitWAL() {
  // 初始化 TransactionManager
  txn_manager_ = std::make_unique<TransactionManager>();
  
  // 初始化 WAL Writer（如果启用）
  if (options_.enable_wal) {
    std::string wal_path = db_path_ + "/wal";
    WalOptions wal_options;
    wal_writer_ = std::make_unique<WalWriter>(wal_path, env_, wal_options);
    Status s = wal_writer_->Open();
    if (!s.ok()) {
      return s;
    }
  }
  
  return Status::OK();
}

Status LsmEngine::ReplayWAL(uint64_t start_sequence) {
  if (!options_.enable_wal) {
    return Status::OK();
  }

  std::string wal_dir = db_path_ + "/wal";
  std::vector<std::string> wal_files;
  Status s = env_->GetChildren(wal_dir, &wal_files);
  if (!s.ok()) {
    // WAL dir may not exist yet, which is fine.
    return Status::OK();
  }

  std::vector<uint64_t> file_numbers;
  file_numbers.reserve(wal_files.size());
  for (const auto& f : wal_files) {
    uint64_t num = 0;
    if (sscanf(f.c_str(), "%llu.wal", &num) == 1) {
      if (num >= start_sequence) {
        file_numbers.push_back(num);
      }
    }
  }
  std::sort(file_numbers.begin(), file_numbers.end());

  for (uint64_t file_num : file_numbers) {
    std::string path = wal_dir + "/" + std::to_string(file_num) + ".wal";
    WalReader reader(path, env_);
    s = reader.Open();
    if (!s.ok()) {
      return s;
    }

    WalBatch batch;
    uint64_t sequence = 0;
    while (true) {
      auto status = reader.ReadNextRecord(&batch, &sequence);
      if (status.ok()) {
        for (const auto& op : batch.ops()) {
          if (op.type == WalRecordType::kPut) {
            mem_->Put(op.key, op.descriptor, op.txn_version);
          } else if (op.type == WalRecordType::kDelete) {
            mem_->Put(op.key, Descriptor(), op.txn_version);
          }
        }
      } else if (status.IsCorruption()) {
        // Log corruption and attempt to continue with next record
        std::cerr << "[WAL WARNING] corruption in " << path
                  << " at sequence " << sequence
                  << ": " << status.ToString()
                  << ". Skipping record and attempting to continue." << std::endl;
        continue;
      } else if (status.IsNotFound()) {
        // End of file
        break;
      } else {
        // IO error or other fatal error - stop replay but don't fail startup
        std::cerr << "[WAL ERROR] replay error in " << path
                  << ": " << status.ToString()
                  << ". Stopping WAL replay for this file." << std::endl;
        break;
      }
    }
  }

  return Status::OK();
}

std::unique_ptr<OCCTransaction> LsmEngine::BeginTransaction(const TransactionOptions& options) {
  if (!txn_manager_ || !mem_) {
    return nullptr;
  }
  
  // 创建 OCC 事务
  auto txn = std::make_unique<OCCTransaction>(
      txn_manager_.get(),      // TransactionManager
      mem_.get(),              // VSLMemTable
      this,                    // LsmEngine (用于查询 SST)
      wal_writer_.get(),       // WalWriter
      options                  // TransactionOptions
  );
  
  // 开始事务
  Status s = txn->Begin();
  if (!s.ok()) {
    return nullptr;
  }
  
  return txn;
}

Status LsmEngine::SyncWAL() {
  if (wal_writer_) {
    return wal_writer_->Sync();
  }
  return Status::OK();
}

// ========== CedarScan 边扫描接口 ==========

std::vector<std::tuple<uint64_t, Timestamp, Descriptor, uint16_t>> 
LsmEngine::ScanEdges(uint64_t vertex_id,
                     EntityType edge_direction,
                     uint16_t edge_type,
                     Timestamp snapshot_ts) {
  std::vector<std::tuple<uint64_t, Timestamp, Descriptor, uint16_t>> results;
  
  if (!opened_) {
    return results;
  }
  
  // Ensure direction is either EdgeOut or EdgeIn
  if (edge_direction != EntityType::EdgeOut && edge_direction != EntityType::EdgeIn) {
    return results;
  }
  
  // Get all column IDs (edge types) for this entity
  std::vector<uint16_t> column_ids;
  if (edge_type == 0xFFFF) {  // kAllLabels
    column_ids = GetEntityColumnIds(vertex_id, edge_direction);
  } else {
    column_ids.push_back(edge_type);
  }
  
  // Query each edge type
  for (uint16_t col_id : column_ids) {
    // Get all versions up to snapshot time
    auto entries = GetRangeLimit(vertex_id, edge_direction, col_id,
                                  Timestamp(0), snapshot_ts, 1000);
    
    // Track seen targets for version folding (only return latest version)
    std::unordered_set<uint64_t> seen_targets;
    
    for (const auto& entry : entries) {
      if (!entry.dst_id.has_value()) continue;
      
      uint64_t target_id = entry.dst_id.value();
      
      // Version folding: skip if we've seen this target
      if (seen_targets.count(target_id)) continue;
      seen_targets.insert(target_id);
      
      // Skip deleted entries (tombstones).
      // MemTableEntry currently lacks flags for tombstone detection;
      // compaction filters handle physical deletion.
      
      results.emplace_back(target_id, entry.timestamp, entry.descriptor, col_id);
    }
  }
  
  return results;
}

std::vector<uint16_t> LsmEngine::GetEntityColumnIds(uint64_t entity_id,
                                                     EntityType entity_type) const {
  (void)entity_type;  // Currently not filtering by entity_type
  std::vector<uint16_t> result;
  
  // Query entity_column_map_ for persistent column IDs
  // Note: This map is updated during writes, so it contains column IDs
  // from both MemTable and SST files
  {
    std::shared_lock<std::shared_mutex> lock(column_map_mutex_);
    auto it = entity_column_map_.find(entity_id);
    if (it != entity_column_map_.end()) {
      result = it->second.ToVector();
    }
  }
  
  // entity_column_map_ is updated during writes, so it covers both
  // MemTable and SST columns without requiring a separate scan.
  
  // Remove duplicates and sort
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  
  return result;
}

std::vector<EdgeScanEntry> LsmEngine::ScanEdgesWithFolding(
    uint64_t vertex_id,
    EntityType edge_direction,
    uint16_t edge_type,
    Timestamp snapshot_ts) const {
  std::vector<EdgeScanEntry> results;
  
  if (!opened_) {
    return results;
  }
  
  // Ensure direction is either EdgeOut or EdgeIn
  if (edge_direction != EntityType::EdgeOut && edge_direction != EntityType::EdgeIn) {
    return results;
  }
  
  // Fast path: SkeletonCache for EdgeOut (structure-only, no hydration trigger)
  // NOTE: We do NOT call HydrateVertex here to avoid infinite recursion,
  // since HydrateVertex calls ScanEdgesWithFolding.
  if (edge_direction == EntityType::EdgeOut && skeleton_cache_) {
    auto [cached_edges, base_ts] = skeleton_cache_->ScanOutEdgesSafe(vertex_id);
    if (cached_edges.has_value()) {
      for (const auto& edge : cached_edges.value()) {
        // Edge type filter
        if (edge_type != 0xFFFF && edge.bits.label_id != (edge_type & 0x7F)) {
          continue;
        }
        // Snapshot timestamp filter
        uint64_t edge_ts = base_ts + edge.bits.timestamp_offset;
        if (edge_ts > snapshot_ts.value()) {
          continue;
        }
        // Skip deleted edges
        if (edge.IsDeleted()) {
          continue;
        }
        // Reconstruct CedarKey; Descriptor is empty (SkeletonCache stores no properties)
        CedarKey key = edge.ToCedarKey(vertex_id, base_ts);
        results.push_back({edge.dst_id, key.timestamp(), Descriptor(),
                          static_cast<uint16_t>(edge.bits.label_id), key});
      }
      return results;
    }
  }
  
  // Get all column IDs (edge types) for this entity
  std::vector<uint16_t> column_ids;
  if (edge_type == 0xFFFF) {  // kAllLabels
    column_ids = GetEntityColumnIds(vertex_id, edge_direction);
  } else {
    column_ids.push_back(edge_type);
  }
  
  // Temporary storage for all entries before version folding
  struct TempEntry {
    uint64_t target_id;
    Timestamp timestamp;
    Descriptor descriptor;
    uint16_t edge_type;
    CedarKey key;
  };
  std::vector<TempEntry> all_entries;
  all_entries.reserve(column_ids.size() * 8);  // heuristic pre-allocation
  
  // Query each edge type
  for (uint16_t col_id : column_ids) {
    // Query MemTable
    auto mem_entries = mem_->GetRange(vertex_id, edge_direction, col_id, 
                                       Timestamp(0), snapshot_ts);
    for (const auto& entry : mem_entries) {
      if (!entry.dst_id.has_value()) continue;
      
      // Build CedarKey from MemTable entry
      CedarKey key;
      if (edge_direction == EntityType::EdgeOut) {
        key = CedarKey::EdgeOut(vertex_id, entry.dst_id.value(), 
                                EdgeTypeId(col_id), entry.timestamp, 0, 0, 0);
      } else {
        key = CedarKey::EdgeIn(vertex_id, entry.dst_id.value(),
                               EdgeTypeId(col_id), entry.timestamp, 0, 0, 0);
      }
      
      all_entries.push_back({entry.dst_id.value(), entry.timestamp, 
                            entry.descriptor, col_id, key});
    }
    
    // Query Immutable MemTable
    if (imm_) {
      auto imm_entries = imm_->GetRange(vertex_id, edge_direction, col_id,
                                         Timestamp(0), snapshot_ts);
      for (const auto& entry : imm_entries) {
        if (!entry.dst_id.has_value()) continue;
        
        CedarKey key;
        if (edge_direction == EntityType::EdgeOut) {
          key = CedarKey::EdgeOut(vertex_id, entry.dst_id.value(),
                                  EdgeTypeId(col_id), entry.timestamp, 0, 0, 0);
        } else {
          key = CedarKey::EdgeIn(vertex_id, entry.dst_id.value(),
                                 EdgeTypeId(col_id), entry.timestamp, 0, 0, 0);
        }
        
        all_entries.push_back({entry.dst_id.value(), entry.timestamp,
                              entry.descriptor, col_id, key});
      }
    }
    
    // Query Accumulated Buffer (for accumulated flush mode)
    if (!accumulated_entries_.empty()) {
      std::lock_guard<std::mutex> lock(accumulated_mutex_);
      for (const auto& [key, descriptor] : accumulated_entries_) {
        if (key.entity_id() != vertex_id) continue;
        if (key.entity_type() != edge_direction) continue;
        if (key.column_id() != col_id) continue;
        if (key.timestamp().value() > snapshot_ts.value()) continue;
        if (key.IsDelete()) continue;
        
        all_entries.push_back({key.target_id(), key.timestamp(),
                              descriptor, col_id, key});
      }
    }
    
    // Query SST files
    if (compaction_engine_) {
      auto files = compaction_engine_->GetFilesForEntity(
          vertex_id, col_id, static_cast<uint8_t>(edge_direction));
      
      for (const auto& file_meta : files) {
        // Quick range check
        if (vertex_id < file_meta.min_entity_id || 
            vertex_id > file_meta.max_entity_id) {
          continue;
        }
        
        // Use cached Reader
        std::shared_ptr<SstReader> reader;
        if (sst_reader_cache_) {
          reader = sst_reader_cache_->Get(file_meta.path);
        }
        if (!reader) {
          reader = std::make_shared<SstReader>(file_meta.path);
          if (!reader->Open().ok()) {
            continue;
          }
        }
        
        // GetRange returns CedarKey with full metadata
        auto range_results = reader->GetRange(vertex_id, edge_direction, col_id,
                                             Timestamp(0), snapshot_ts);
        for (const auto& [key, descriptor] : range_results) {
          // Cross-column SST files may contain mixed entity types;
          // filter to ensure we only process edges in the requested direction.
          if (key.entity_type() != edge_direction) {
            continue;
          }
          all_entries.push_back({key.target_id(), key.timestamp(),
                                descriptor, col_id, key});
        }
        
        // Release Reader
        if (sst_reader_cache_) {
          sst_reader_cache_->Release(file_meta.path);
        }
      }
    }
  }
  
  // Sort by (target_id, timestamp_desc) for zero-allocation version folding
  std::sort(all_entries.begin(), all_entries.end(),
    [](const TempEntry& a, const TempEntry& b) {
      if (a.target_id != b.target_id) {
        return a.target_id < b.target_id;
      }
      // Higher timestamp first (descending)
      return a.timestamp.value() > b.timestamp.value();
    });
  
  // Zero-allocation version folding: keep first entry of each target_id group
  uint64_t last_target_id = 0;
  bool has_last = false;
  
  for (const auto& entry : all_entries) {
    // Skip deleted entries
    if (entry.key.IsDelete()) {
      continue;
    }
    
    // Version folding: skip if we've seen this target
    if (has_last && entry.target_id == last_target_id) {
      continue;
    }
    
    last_target_id = entry.target_id;
    has_last = true;
    
    results.push_back({entry.target_id, entry.timestamp, 
                      entry.descriptor, entry.edge_type, entry.key});
  }
  
  return results;
}

void LsmEngine::InvalidateQueryCache(uint64_t entity_id) {
  if (query_cache_ && !disable_query_cache_invalidate_) {
    query_cache_->Invalidate(entity_id);
  }
}

// 新的 FlushMemTable 实现 - 统一大 SST 文件（跨 Column ID 合并）

// ========== 统一 Flush：所有数据写入一个 SST 文件（生产级设计）==========
Status LsmEngine::FlushMemTable(VSLMemTable* mem) {
  if (mem->IsEmpty()) {
    return Status::OK();
  }

  // 收集所有条目（不区分 column_id，统一处理）
  std::vector<std::pair<CedarKey, Descriptor>> all_entries;
  all_entries.reserve(mem->size());
  
  mem->Traverse([&](const CedarKey& key, const Descriptor& descriptor) -> bool {
    all_entries.emplace_back(key, descriptor);
    return true;
  });
  
  if (all_entries.empty()) {
    return Status::OK();
  }
  
  // ========== 累积 Flush 模式（生成大 SST 文件）==========
  if (options_.enable_accumulated_flush) {
    std::unique_lock<std::mutex> lock(accumulated_mutex_);
    
    // 添加到累积缓冲区
    size_t new_bytes = all_entries.size() * 40;  // 估算 40 bytes/entry
    accumulated_entries_.insert(
      accumulated_entries_.end(),
      std::make_move_iterator(all_entries.begin()),
      std::make_move_iterator(all_entries.end())
    );
    accumulated_bytes_ += new_bytes;
    
    // 如果达到目标大小，触发 Flush
    size_t target_bytes = options_.accumulated_flush_size_mb * 1024 * 1024;
    if (accumulated_bytes_ >= target_bytes) {
      lock.unlock();
      return FlushAccumulated();
    }
    
    return Status::OK();  // 已累积，不立即刷盘
  }
  
  // ========== 立即 Flush 模式 ==========
  return FlushEntriesToSST(std::move(all_entries));
}

Status LsmEngine::FlushEntityGroup(uint8_t entity_type, uint16_t column_id,
                                   const std::vector<std::pair<CedarKey, Descriptor>>& entries) {
  if (entries.empty()) {
    return Status::OK();
  }

  uint64_t file_number = next_file_number_.fetch_add(1);
  std::string filepath = SstFilePath(file_number);

  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  uint64_t min_tx_time = UINT64_MAX;
  uint64_t max_tx_time = 0;
  
  // 复制条目并排序（使用 CompareForSorting，timestamp 降序）
  auto sorted_entries = entries;
  std::sort(sorted_entries.begin(), sorted_entries.end(),
    [](const auto& a, const auto& b) { return a.first.LessForSorting(b.first); });
  
  // ========== 使用 SST Builder 工厂 ==========
  cedar::WritableFile* file = nullptr;
  cedar::Status ls = env_->NewWritableFile(filepath, &file);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }
  
  // 使用 Zone-Columnar Builder（默认格式）
  auto builder = SstBuilderFactory::Create(file, db_path_);
  
  for (const auto& [key, descriptor] : sorted_entries) {
    builder->Add(key, descriptor);
    
    min_entity_id = std::min(min_entity_id, key.entity_id());
    max_entity_id = std::max(max_entity_id, key.entity_id());
    min_tx_time = std::min(min_tx_time, key.timestamp().value());
    max_tx_time = std::max(max_tx_time, key.timestamp().value());
  }

  Status fs = builder->Finish();
  delete file;

  if (!fs.ok()) {
    env_->RemoveFile(filepath);
    return fs;
  }

  uint64_t file_size = 0;
  ls = env_->GetFileSize(filepath, &file_size);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }

  SSTFileMeta meta;
  meta.file_number = file_number;
  meta.file_size = file_size;
  meta.num_entries = builder->NumEntries();
  meta.min_entity_id = min_entity_id;
  meta.max_entity_id = max_entity_id;
  meta.min_tx_time = min_tx_time;
  meta.max_tx_time = max_tx_time;
  meta.level = 0;
  meta.column_id = column_id;
  meta.entity_type = entity_type;
  meta.temporal_filter_metadata = builder->GetTemporalFilterData();

  {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    levels_[0].push_back(meta);
  }
  BuildColumnFileIndex();
  
  if (compaction_engine_) {
    ZoneSstMeta zone_meta;
    zone_meta.file_number = file_number;
    zone_meta.file_size = file_size;
    zone_meta.num_entries = builder->NumEntries();
    zone_meta.level = 0;
    zone_meta.min_entity_id = min_entity_id;
    zone_meta.max_entity_id = max_entity_id;
    zone_meta.min_timestamp = min_tx_time;
    zone_meta.max_timestamp = max_tx_time;
    zone_meta.column_id = column_id;
    zone_meta.entity_type = entity_type;
    zone_meta.path = filepath;
    zone_meta.blob_path = db_path_ + "/sst_" + std::to_string(file_number) + ".blob";
    zone_meta.temporal_filter_metadata = builder->GetTemporalFilterData();
    
    Status cs = compaction_engine_->AddSSTFile(zone_meta);
    if (!cs.ok()) {
      // Compaction engine AddSSTFile error is non-fatal.
    }
    
    compaction_engine_->ScheduleCompaction();
  }
  
  // 增量更新 Partition Index
  if (partition_index_) {
    auto s = partition_index_->IndexSSTFile(file_number);
    if (!s.ok()) {
      LOG(WARNING) << "PartitionIndex::IndexSSTFile failed for file " << file_number
                   << ": " << s.ToString();
    }
  }

  return Status::OK();
}

Status LsmEngine::DoCompaction(int level, const std::vector<SSTFileMeta>& inputs) {
  if (inputs.empty()) {
    return Status::OK();
  }

  // 提取 entity_type 和 column_id（应该都相同）
  uint8_t entity_type = inputs[0].entity_type;
  uint16_t column_id = inputs[0].column_id;
  
  // 验证所有输入文件类型一致
  for (const auto& input : inputs) {
    if (input.entity_type != entity_type || input.column_id != column_id) {
      return Status::InvalidArgument("DoCompaction", 
          "input files have different entity_type or column_id");
    }
  }
  
  // 打开所有输入 SST
  std::vector<std::shared_ptr<SstReader>> readers;
  readers.reserve(inputs.size());
  for (const auto& input : inputs) {
    std::string input_path = SstFilePath(input.file_number);
    auto reader = std::make_shared<SstReader>(input_path);
    Status s = reader->Open();
    if (!s.ok()) {
      return s;
    }
    readers.push_back(reader);
  }
  
  // 准备 reader 指针向量
  std::vector<SstReader*> reader_ptrs;
  reader_ptrs.reserve(readers.size());
  for (auto& r : readers) {
    reader_ptrs.push_back(r.get());
  }
  
  // 创建输出文件
  uint64_t file_number = next_file_number_.fetch_add(1);
  std::string output_path = SstFilePath(file_number);
  int output_level = level + 1;
  
  // 执行归并
  CompactionMerger merger(reader_ptrs, entity_type, column_id);
  auto output_meta = merger.Run(output_path, db_path_);
  
  if (!output_meta) {
    return Status::IOError("DoCompaction", "merger failed");
  }
  
  // 设置文件号
  output_meta->file_number = file_number;
  output_meta->level = output_level;
  output_meta->min_entity_id = inputs[0].min_entity_id;
  output_meta->max_entity_id = inputs[0].max_entity_id;
  for (const auto& input : inputs) {
    output_meta->min_entity_id = std::min(output_meta->min_entity_id, input.min_entity_id);
    output_meta->max_entity_id = std::max(output_meta->max_entity_id, input.max_entity_id);
    output_meta->min_timestamp = std::min(output_meta->min_timestamp, input.min_tx_time);
    output_meta->max_timestamp = std::max(output_meta->max_timestamp, input.max_tx_time);
  }
  
  // 获取文件大小
  uint64_t file_size = 0;
  Status ls = env_->GetFileSize(output_path, &file_size);
  if (!ls.ok()) {
    return Status::IOError("DoCompaction", ls.ToString());
  }
  
  // 创建 SSTFileMeta
  SSTFileMeta new_meta;
  new_meta.file_number = file_number;
  new_meta.file_size = file_size;
  new_meta.num_entries = output_meta->num_entries;
  new_meta.min_entity_id = output_meta->min_entity_id;
  new_meta.max_entity_id = output_meta->max_entity_id;
  new_meta.min_tx_time = output_meta->min_timestamp;
  new_meta.max_tx_time = output_meta->max_timestamp;
  new_meta.level = output_level;
  new_meta.column_id = column_id;
  new_meta.entity_type = entity_type;
  
  // 原子更新 levels_
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 从当前层级删除输入文件
    auto& current_level = levels_[level];
    for (const auto& input : inputs) {
      auto it = std::remove_if(current_level.begin(), current_level.end(),
                               [&input](const SSTFileMeta& m) {
                                 return m.file_number == input.file_number;
                               });
      current_level.erase(it, current_level.end());
    }
    
    // 添加新文件到下一层级
    if (output_level >= static_cast<int>(levels_.size())) {
      levels_.resize(output_level + 1);
    }
    levels_[output_level].push_back(new_meta);
  }
  BuildColumnFileIndex();
  
  // 删除旧文件
  for (const auto& input : inputs) {
    std::string old_path = SstFilePath(input.file_number);
    env_->RemoveFile(old_path);
  }
  
  // 更新 Compaction Engine: remove input files, add output file
  if (compaction_engine_) {
    for (const auto& input : inputs) {
      compaction_engine_->RemoveSSTFile(input.file_number);
    }
    compaction_engine_->AddSSTFile(*output_meta);
  }
  
  // 增量更新 Partition Index
  if (partition_index_) {
    for (const auto& input : inputs) {
      auto rs = partition_index_->RemoveSST(input.file_number);
      if (!rs.ok()) {
        LOG(WARNING) << "PartitionIndex::RemoveSST failed for file " << input.file_number
                     << ": " << rs.ToString();
      }
    }
    auto s = partition_index_->IndexSSTFile(file_number);
    if (!s.ok()) {
      LOG(WARNING) << "PartitionIndex::IndexSSTFile failed for file " << file_number
                   << ": " << s.ToString();
    }
  }
  
  return Status::OK();
}

std::vector<SSTFileMeta> LsmEngine::SelectCompactionFiles(int level) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  if (level < 0 || level >= static_cast<int>(levels_.size())) {
    return {};
  }
  
  const auto& level_files = levels_[level];
  if (level_files.size() < options_.compaction_config.min_files) {
    return {};
  }
  
  // Simple strategy: select oldest files (smallest file numbers)
  std::vector<SSTFileMeta> candidates = level_files;
  std::sort(candidates.begin(), candidates.end(),
            [](const auto& a, const auto& b) {
              return a.file_number < b.file_number;
            });
  
  // Select up to 4 files for compaction
  size_t num_to_select = std::min(candidates.size(), size_t(4));
  return std::vector<SSTFileMeta>(candidates.begin(), candidates.begin() + num_to_select);
}

bool LsmEngine::NeedsCompaction(int level) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  if (level < 0 || level >= static_cast<int>(levels_.size())) {
    return false;
  }
  
  const auto& level_files = levels_[level];
  
  // Check file count
  if (level_files.size() >= options_.compaction_config.min_files) {
    return true;
  }
  
  // Check total size
  uint64_t total_size = 0;
  for (const auto& meta : level_files) {
    total_size += meta.file_size;
  }
  
  if (total_size >= options_.compaction_config.min_size) {
    return true;
  }
  
  return false;
}

void LsmEngine::QuerySSTFiles(uint64_t entity_id, EntityType entity_type,
                                uint16_t column_id, std::vector<MemTableEntry>* results) {
  // Stub
}

void LsmEngine::GetEntriesFromSst(uint64_t entity_id, EntityType entity_type,
                                  uint16_t column_id,
                                  std::vector<std::pair<CedarKey, Descriptor>>* results) {
  if (!results || levels_.empty()) {
    return;
  }
  
  // OPTIMIZATION: Use column-based file index to quickly find candidate files
  std::vector<uint64_t> candidates = GetCandidateFiles(entity_id, entity_type, column_id);
  
  // If index returns empty, fall back to building from levels
  if (candidates.empty()) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (int level = 0; level < static_cast<int>(levels_.size()); ++level) {
      for (const auto& meta : levels_[level]) {
        if (entity_id >= meta.min_entity_id && entity_id <= meta.max_entity_id &&
            column_id == meta.column_id &&
            static_cast<uint8_t>(entity_type) == meta.entity_type) {
          candidates.push_back(meta.file_number);
        }
      }
    }
  }
  
  // OPTIMIZATION: If too many candidates, limit to first 50 files
  size_t max_files_to_check = 50;
  if (candidates.size() > max_files_to_check) {
    // Sort by file_number (newest first)
    std::sort(candidates.begin(), candidates.end(),
      [](uint64_t a, uint64_t b) {
        return a > b;
      });
    candidates.resize(max_files_to_check);
  }
  
  // Query candidate files
  for (uint64_t file_number : candidates) {
    std::string filepath = SstFilePath(file_number);
    
    // ========== 使用 SstReader（新格式）==========
    SstReader reader(filepath);
    if (!reader.Open().ok()) {
      continue;
    }
    
    // Bloom Filter optimization: skip files that definitely don't contain the entity
    if (!reader.MayContainEntity(entity_id)) {
      continue;
    }
    
    // Get all versions for this entity using GetRange
    // Use full time range to get all versions
    auto versions = reader.GetRange(entity_id, entity_type, column_id, 
                                     Timestamp(0), 
                                     Timestamp(UINT64_MAX));
    
    // Add results
    for (auto& [key, desc] : versions) {
      results->emplace_back(key, desc);
    }
  }
  
  // Sort results by timestamp descending (newest first)
  std::sort(results->begin(), results->end(), 
            [](const auto& a, const auto& b) {
              return a.first.timestamp().value() > b.first.timestamp().value();
            });
}

void LsmEngine::PreloadHotEntities() {
  // Stub
}

Status LsmEngine::LoadSstFiles() {
  if (!std::filesystem::exists(db_path_)) {
    std::cerr << "[LoadSstFiles] DB path does not exist: " << db_path_ << std::endl;
    return Status::OK();
  }

  std::cerr << "[LoadSstFiles] Loading from: " << db_path_ << std::endl;
  int loaded_count = 0;

  for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
    if (entry.is_regular_file() && entry.path().extension() == ".sst") {
      std::string filename = entry.path().filename().string();
      std::cerr << "[LoadSstFiles] Found SST: " << filename << std::endl;
      
      size_t dot_pos = filename.find('.');
      if (dot_pos == std::string::npos) continue;
      std::string number_str = filename.substr(0, dot_pos);
      if (number_str.empty() || !std::all_of(number_str.begin(), number_str.end(), ::isdigit)) {
        continue;
      }
      uint64_t file_number = 0;
      try {
        file_number = std::stoull(number_str);
        next_file_number_ = std::max(next_file_number_.load(), file_number + 1);
      } catch (...) {
        std::cerr << "[LSMEngine] Failed to parse file number from: " << number_str << std::endl;
        continue;
      }

      uint64_t file_size = entry.file_size();

      std::string filepath = entry.path().string();
      // ========== 使用 SstReader（新格式）==========
      SstReader reader(filepath);
      Status open_status = reader.Open();
      if (!open_status.ok()) {
        std::cerr << "[LoadSstFiles] Failed to open " << filename << ": " << open_status.ToString() << std::endl;
        continue;
      }
      std::cerr << "[LoadSstFiles] Opened " << filename << " successfully" << std::endl;

      SSTFileMeta meta;
      meta.file_number = file_number;
      meta.file_size = file_size;
      meta.num_entries = reader.NumEntries();
      meta.min_entity_id = reader.MinEntityId();
      meta.max_entity_id = reader.MaxEntityId();
      meta.min_tx_time = reader.MinTimestamp();
      meta.max_tx_time = reader.MaxTimestamp();
      meta.level = 0;
      // Get column_id from header
      meta.column_id = reader.ColumnId();
      // entity_type from header
      meta.entity_type = reader.GetEntityType();
      meta.temporal_filter_metadata = reader.GetTemporalFilterData();

      levels_[0].push_back(meta);
    }
  }

  return Status::OK();
}

uint64_t LsmEngine::NewFileNumber() {
  return next_file_number_.fetch_add(1);
}

std::string LsmEngine::SstFilePath(uint64_t file_number) {
  return db_path_ + "/" + std::to_string(file_number) + ".sst";
}

std::string SSTFileMeta::file_name() const {
  return std::to_string(file_number) + ".sst";
}

std::vector<SSTFileMeta> LsmEngine::GetSSTFiles(int level) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (level >= 0 && level < static_cast<int>(levels_.size())) {
    return levels_[level];
  }
  return {};
}

// ============================================================================
// QUERY OPTIMIZATION: Fast File Lookup by Entity Range
// ============================================================================

void LsmEngine::BuildColumnFileIndex() {
  std::unique_lock<std::shared_mutex> lock(column_index_mutex_);
  column_file_index_.clear();
  
  // Iterate through all levels and files
  for (int level = 0; level < static_cast<int>(levels_.size()); ++level) {
    for (auto& meta : levels_[level]) {
      // Group by column_id, store lightweight copies
      ColumnIndexEntry entry;
      entry.file_number = meta.file_number;
      entry.min_entity_id = meta.min_entity_id;
      entry.max_entity_id = meta.max_entity_id;
      entry.entity_type = meta.entity_type;
      column_file_index_[meta.column_id].push_back(entry);
    }
  }
  
  // Sort each column's files by min_entity_id for binary search
  for (auto& [col_id, files] : column_file_index_) {
    std::sort(files.begin(), files.end(), [](const ColumnIndexEntry& a, const ColumnIndexEntry& b) {
      return a.min_entity_id < b.min_entity_id;
    });
  }
}

std::vector<uint64_t> LsmEngine::GetCandidateFiles(uint64_t entity_id,
                                                           EntityType entity_type,
                                                           uint16_t column_id) const {
  std::shared_lock<std::shared_mutex> lock(column_index_mutex_);
  
  auto it = column_file_index_.find(column_id);
  if (it == column_file_index_.end()) {
    return {};
  }
  
  const auto& files = it->second;
  std::vector<uint64_t> candidates;
  
  // Use binary search to find files that might contain entity_id
  // Files are sorted by min_entity_id
  for (const ColumnIndexEntry& entry : files) {
    // Quick range check: entity_id must be within [min_entity_id, max_entity_id]
    if (entity_id >= entry.min_entity_id && entity_id <= entry.max_entity_id) {
      // Additional check: entity_type must match
      if (static_cast<uint8_t>(entity_type) == entry.entity_type) {
        candidates.push_back(entry.file_number);
      }
    }
    // Early termination: if file's min_entity_id > entity_id, no need to check further
    // But we can't break here because files may have overlapping ranges
  }
  
  return candidates;
}

// ========== Size-Tiered Compaction 集成 ==========

void LsmEngine::AutoCompactionThread() {
  while (auto_compaction_enabled_.load()) {
    // 如果后台合并被禁用，跳过执行但保持线程运行
    if (!options_.size_tiered_config.enable_background_compaction) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    if (compaction_engine_ && compaction_engine_->NeedsCompaction()) {
      // 尝试获取下一个 Compaction 任务
      auto task = compaction_engine_->PickNextCompaction();
      if (task.has_value()) {
        // 执行 Compaction
        Status s = compaction_engine_->ExecuteCompaction(task.value());
        if (!s.ok()) {
          // Compaction execution error logged; continue monitoring.
        }
      }
    }
    
    // 休眠一段时间再检查
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void LsmEngine::MigrateExistingSstFiles() {
  // 将现有的 SST 文件从旧格式迁移到新的 Compaction 引擎
  // 注意：这是一个一次性迁移，在首次启用新引擎时调用
  
  if (!compaction_engine_) return;
  
  for (int level = 0; level < static_cast<int>(levels_.size()); ++level) {
    for (const auto& old_meta : levels_[level]) {
      ZoneSstMeta zone_meta;
      zone_meta.file_number = old_meta.file_number;
      zone_meta.file_size = old_meta.file_size;
      zone_meta.num_entries = old_meta.num_entries;
      zone_meta.level = level;
      zone_meta.min_entity_id = old_meta.min_entity_id;
      zone_meta.max_entity_id = old_meta.max_entity_id;
      zone_meta.min_timestamp = old_meta.min_tx_time;
      zone_meta.max_timestamp = old_meta.max_tx_time;
      zone_meta.column_id = old_meta.column_id;
      zone_meta.entity_type = old_meta.entity_type;
      zone_meta.path = SstFilePath(old_meta.file_number);
      zone_meta.blob_path = db_path_ + "/sst_" + std::to_string(old_meta.file_number) + ".blob";
      zone_meta.temporal_filter_metadata = old_meta.temporal_filter_metadata;
      
      // 静默添加，不触发 Compaction
      compaction_engine_->AddSSTFile(zone_meta);
    }
  }
}

Status LsmEngine::CompactAll() {
  if (!compaction_engine_) {
    return Status::InvalidArgument("LsmEngine", "Compaction engine not initialized");
  }
  
  // 等待所有自动 Compaction 完成
  auto_compaction_enabled_.store(false);
  if (auto_compaction_thread_ && auto_compaction_thread_->joinable()) {
    auto_compaction_thread_->join();
    delete auto_compaction_thread_;
    auto_compaction_thread_ = nullptr;
  }
  
  // 执行全量合并
  Status s = compaction_engine_->CompactAll();
  
  // 重新启动自动 Compaction 线程
  auto_compaction_enabled_.store(true);
  auto_compaction_thread_ = new std::thread(&LsmEngine::AutoCompactionThread, this);
  
  return s;
}

void LsmEngine::WaitForCompactions() {
  if (compaction_engine_) {
    compaction_engine_->WaitForCompactions();
  }
}

// =============================================================================
// OPTIMIZATION: 跨查询缓存实现
// =============================================================================

std::optional<std::vector<MemTableEntry>> LsmEngine::GetFromCrossQueryCache(
    uint64_t entity_id, uint16_t column_id) const {
  CacheKey key{entity_id, column_id};
  
  std::shared_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
  auto it = cross_query_cache_.find(key);
  if (it == cross_query_cache_.end()) {
    return std::nullopt;
  }
  
  // 检查是否过期
  auto now = std::chrono::steady_clock::now();
  if (now - it->second.timestamp > kCrossQueryCacheTTL) {
    lock.unlock();
    // 过期，移除缓存
    std::unique_lock<std::shared_mutex> write_lock(cross_query_cache_mutex_);
    cross_query_cache_.erase(it);
    return std::nullopt;
  }
  
  // 缓存命中
  it->second.hit_count++;
  return it->second.data;
}

void LsmEngine::AddToCrossQueryCache(uint64_t entity_id, uint16_t column_id,
                                     const std::vector<MemTableEntry>& data) {
  // 清理过期缓存
  CleanupCrossQueryCache();
  
  // 如果缓存已满，不添加
  {
    std::shared_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
    if (cross_query_cache_.size() >= kMaxCrossQueryCacheSize) {
      return;
    }
  }
  
  CacheKey key{entity_id, column_id};
  CacheEntry entry;
  entry.data = data;
  entry.timestamp = std::chrono::steady_clock::now();
  entry.hit_count = 1;
  
  std::unique_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
  cross_query_cache_[key] = std::move(entry);
}

void LsmEngine::InvalidateCrossQueryCache(uint64_t entity_id, uint16_t column_id) {
  CacheKey key{entity_id, column_id};
  std::unique_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
  cross_query_cache_.erase(key);
}

void LsmEngine::CleanupCrossQueryCache() {
  auto now = std::chrono::steady_clock::now();
  std::unique_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
  
  for (auto it = cross_query_cache_.begin(); it != cross_query_cache_.end();) {
    if (now - it->second.timestamp > kCrossQueryCacheTTL) {
      it = cross_query_cache_.erase(it);
    } else {
      ++it;
    }
  }
}

// =============================================================================
// OPTIMIZATION: 热数据预读取和查询模式追踪
// =============================================================================

void LsmEngine::TrackQueryPattern(uint64_t entity_id, EntityType entity_type, 
                                   uint16_t column_id) {
  CacheKey key{entity_id, column_id};
  
  std::unique_lock<std::shared_mutex> lock(query_pattern_mutex_);
  auto& pattern = query_patterns_[key];
  pattern.count++;
  pattern.last_query = std::chrono::steady_clock::now();
}

void LsmEngine::PrefetchHotData(uint64_t entity_id, EntityType entity_type, 
                                 uint16_t column_id) {
  // 检查是否是热数据
  CacheKey key{entity_id, column_id};
  
  {
    std::shared_lock<std::shared_mutex> lock(query_pattern_mutex_);
    auto it = query_patterns_.find(key);
    if (it == query_patterns_.end() || it->second.count < kHotDataThreshold) {
      return;  // 不是热数据，不预读取
    }
  }
  
  // 检查是否已在缓存中
  if (GetFromCrossQueryCache(entity_id, column_id).has_value()) {
    return;  // 已在缓存中
  }
  
  // 预读取数据到缓存（使用全时间范围）
  auto data = GetRange(entity_id, entity_type, column_id, 
                       Timestamp(0), Timestamp(UINT64_MAX));
  
  if (!data.empty()) {
    AddToCrossQueryCache(entity_id, column_id, data);
  }
}

// =============================================================================
// OPTIMIZATION: P1 - 时间范围预计算缓存实现
// =============================================================================

std::optional<std::vector<MemTableEntry>> LsmEngine::GetFromTimeRangeCache(
    uint64_t entity_id, uint16_t column_id, uint64_t start_ts, uint64_t end_ts) const {
  TimeRangeCacheKey key{entity_id, column_id, start_ts, end_ts};
  
  std::shared_lock<std::shared_mutex> lock(time_range_cache_mutex_);
  auto it = time_range_cache_.find(key);
  if (it == time_range_cache_.end()) {
    return std::nullopt;
  }
  
  // 检查是否过期
  auto now = std::chrono::steady_clock::now();
  if (now - it->second.second > kTimeRangeCacheTTL) {
    lock.unlock();
    std::unique_lock<std::shared_mutex> write_lock(time_range_cache_mutex_);
    time_range_cache_.erase(it);
    return std::nullopt;
  }
  
  return it->second.first;
}

void LsmEngine::AddToTimeRangeCache(uint64_t entity_id, uint16_t column_id, 
                                     uint64_t start_ts, uint64_t end_ts,
                                     const std::vector<MemTableEntry>& data) {
  // 清理过期缓存
  CleanupTimeRangeCache();
  
  // 如果缓存已满，不添加
  {
    std::shared_lock<std::shared_mutex> lock(time_range_cache_mutex_);
    if (time_range_cache_.size() >= kMaxTimeRangeCacheSize) {
      return;
    }
  }
  
  TimeRangeCacheKey key{entity_id, column_id, start_ts, end_ts};
  std::unique_lock<std::shared_mutex> lock(time_range_cache_mutex_);
  time_range_cache_[key] = {data, std::chrono::steady_clock::now()};
}

void LsmEngine::CleanupTimeRangeCache() {
  auto now = std::chrono::steady_clock::now();
  std::unique_lock<std::shared_mutex> lock(time_range_cache_mutex_);
  
  for (auto it = time_range_cache_.begin(); it != time_range_cache_.end();) {
    if (now - it->second.second > kTimeRangeCacheTTL) {
      it = time_range_cache_.erase(it);
    } else {
      ++it;
    }
  }
}

// =============================================================================
// OPTIMIZATION: P1 - SST 文件预加载管理
// =============================================================================

void LsmEngine::PreloadHotSSTFiles() {
  if (!sst_reader_cache_ || !compaction_engine_) return;
  
  // 获取最近频繁访问的文件
  auto stats = sst_reader_cache_->GetStats();
  if (stats.hits < 100) return;  // 访问次数不够，不预加载
  
  // 获取热数据文件列表
  auto hot_files = sst_reader_cache_->GetHotFiles();
  
  // 预加载热数据文件（最多预加载 5 个）
  size_t preload_count = 0;
  for (const auto& file_path : hot_files) {
    if (preload_count >= 5) break;
    
    if (sst_reader_cache_->Preload(file_path)) {
      preload_count++;
    }
  }
}

std::vector<std::string> LsmEngine::GetHotSSTFilesForQuery(
    const std::vector<uint64_t>& entity_ids,
    uint16_t column_id,
    uint8_t entity_type) const {
  std::vector<std::string> hot_files;
  
  if (!compaction_engine_) return hot_files;
  
  // 找出包含最多查询 entity 的文件
  std::unordered_map<std::string, size_t> file_entity_count;
  
  for (uint64_t entity_id : entity_ids) {
    auto files = compaction_engine_->GetFilesForEntity(entity_id, column_id, entity_type);
    for (const auto& file : files) {
      file_entity_count[file.path]++;
    }
  }
  
  // 按包含 entity 数量排序，返回前 10 个
  std::vector<std::pair<std::string, size_t>> sorted_files(
      file_entity_count.begin(), file_entity_count.end());
  std::sort(sorted_files.begin(), sorted_files.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });
  
  for (size_t i = 0; i < std::min(size_t(10), sorted_files.size()); ++i) {
    hot_files.push_back(sorted_files[i].first);
  }
  
  return hot_files;
}

// ========== Phase 4: 锚点机制内存加速实现 ==========

void LsmEngine::MarkEntityActive(uint64_t entity_id) {
  active_entity_bitmap_.MarkActive(entity_id);
  // 同时使 SkeletonCache 中的条目失效（如果存在）
  if (skeleton_cache_) {
    skeleton_cache_->Invalidate(entity_id);
  }
}

void LsmEngine::MarkEntityDeleted(uint64_t entity_id) {
  active_entity_bitmap_.MarkDeleted(entity_id);
  // 标记 SkeletonCache 中的条目为删除
  if (skeleton_cache_) {
    skeleton_cache_->MarkDeleted(entity_id);
  }
}

bool LsmEngine::IsEntityActive(uint64_t entity_id) const {
  return active_entity_bitmap_.IsActive(entity_id);
}

// ========== Phase 5: SkeletonCache Implementation ==========

void LsmEngine::EnableSkeletonCache(size_t num_shards, size_t max_entries_per_shard) {
  if (!skeleton_cache_) {
    skeleton_cache_ = std::make_unique<ShardedSkeletonCache>(num_shards, max_entries_per_shard);
  }
}

std::pair<VertexSkeleton*, bool> LsmEngine::GetVertexSkeleton(uint64_t vertex_id) {
  if (!skeleton_cache_) {
    // 自动启用默认配置
    EnableSkeletonCache();
  }
  
  // 使用 GetOrCreate 确保线程安全（只有一个线程执行 Hydrate）
  auto [skeleton, created] = skeleton_cache_->GetOrCreate(vertex_id, [this, vertex_id]() {
    VertexSkeleton skeleton;
    // Hydrate 可能失败，但我们需要返回一个有效的 skeleton
    // 即使失败也返回空 skeleton，调用方需要检查 IsDeleted()
    Status s = SkeletonHydrator::HydrateVertex(vertex_id, this, &skeleton);
    if (!s.ok()) {
      skeleton.SetStatus(2);  // Mark as deleted (not found)
    }
    return skeleton;
  });
  
  if (!skeleton || skeleton->IsDeleted()) {
    return {nullptr, false};
  }
  
  return {skeleton, !created};  // created==true 表示 miss，created==false 表示 hit
}

std::vector<std::tuple<uint64_t, Timestamp, Descriptor, uint16_t>>
LsmEngine::ScanOutEdgesCached(uint64_t src_id, uint16_t edge_type, Timestamp snapshot_ts) {
  std::vector<std::tuple<uint64_t, Timestamp, Descriptor, uint16_t>> results;
  
  if (!skeleton_cache_) {
    return results;  // SkeletonCache 未启用
  }
  
  // 使用线程安全的边扫描（内部加锁，拷贝数据后释放锁）
  auto edges_info = skeleton_cache_->ScanOutEdgesSafe(src_id);
  
  // 未命中，尝试 Hydrate
  if (!edges_info.first.has_value()) {
    auto [new_sk, new_hit] = GetVertexSkeleton(src_id);
    if (!new_sk || new_sk->IsDeleted()) {
      return results;
    }
    // 重新获取（这次应该命中）
    edges_info = skeleton_cache_->ScanOutEdgesSafe(src_id);
    if (!edges_info.first.has_value()) {
      return results;
    }
  }
  
  uint32_t base_ts = edges_info.second;  // 基础时间戳
  
  // 遍历边（数据已拷贝，无需锁，线程安全）
  for (const auto& edge : edges_info.first.value()) {
    // 边类型过滤
    if (edge_type != 0xFFFF && edge.bits.label_id != (edge_type & 0x7F)) {
      continue;
    }
    
    // 时间戳过滤
    uint64_t edge_ts = base_ts + edge.bits.timestamp_offset;
    if (edge_ts > snapshot_ts.value()) {
      continue;  // 边时间晚于查询时间
    }
    
    // 跳过已删除的边
    if (edge.IsDeleted()) {
      continue;
    }
    
    // 构造返回值
    Timestamp ts(edge_ts);
    Descriptor desc;  // 简化：SkeletonCache 不存储属性，需要时从磁盘加载
    results.emplace_back(edge.dst_id, ts, desc, edge.bits.label_id);
  }
  
  return results;
}

ShardedSkeletonCache::Stats LsmEngine::GetSkeletonCacheStats() const {
  if (!skeleton_cache_) {
    return ShardedSkeletonCache::Stats();
  }
  return skeleton_cache_->GetStats();
}

size_t LsmEngine::GetSkeletonCacheMemoryUsage() const {
  if (!skeleton_cache_) {
    return 0;
  }
  return skeleton_cache_->MemoryUsage();
}

// ========== Phase 4c: BlobFileManager Implementation ==========

void LsmEngine::SetBlobFileManager(BlobFileManager* blob_mgr) {
  blob_manager_ = blob_mgr;
}

void LsmEngine::SetAutoBlobStorage(AutoBlobStorage* auto_blob) {
  auto_blob_storage_ = auto_blob;
}

void LsmEngine::SetPartitionIndex(cedar::dtx::PartitionIndex* index) {
  partition_index_ = index;
  if (compaction_engine_ && partition_index_) {
    compaction_engine_->SetCompactionObserver(
        [this](const std::vector<uint64_t>& removed_files, uint64_t added_file) {
          if (!partition_index_) return;
          for (uint64_t fn : removed_files) {
            partition_index_->RemoveSST(fn);
          }
          auto s = partition_index_->IndexSSTFile(added_file);
          if (!s.ok()) {
            LOG(WARNING) << "PartitionIndex::IndexSSTFile failed for file " << added_file
                         << ": " << s.ToString();
          }
        });
  }
}

Status LsmEngine::PutString(uint64_t entity_id, uint16_t col_id, const std::string& value) {
  if (auto_blob_storage_) {
    return auto_blob_storage_->PutString(entity_id, col_id, value);
  }
  // Fallback: try inline short string
  auto desc_opt = Descriptor::InlineShortStr(col_id, Slice(value));
  if (!desc_opt.has_value()) {
    return Status::InvalidArgument("LsmEngine", 
        "value too long for InlineShortStr and blob storage not enabled");
  }
  CedarKey key = CedarKey::Vertex(entity_id, col_id, Timestamp(0));
  return Put(key, *desc_opt, Timestamp(0));
}

std::optional<std::string> LsmEngine::GetString(uint64_t entity_id, uint16_t col_id) {
  if (auto_blob_storage_) {
    return auto_blob_storage_->GetString(entity_id, col_id);
  }
  // Fallback: try to read as inline short string
  CedarKey key = CedarKey::Vertex(entity_id, col_id, Timestamp(0));
  auto desc = Get(key);
  if (!desc.has_value()) {
    return std::nullopt;
  }
  if (desc->GetKind() == EntryKind::InlineShortStr) {
    return desc->AsInlineShortStr();
  }
  if (desc->GetKind() == EntryKind::InlineInt) {
    auto val = desc->AsInlineInt();
    if (val.has_value()) {
      return std::to_string(*val);
    }
  }
  return std::nullopt;
}

// ========== Phase 4c: ParallelQueryEngine Implementation ==========

std::vector<ColumnQueryResult> LsmEngine::QueryColumnsParallel(
    uint64_t entity_id,
    const std::vector<uint16_t>& column_ids,
    EntityType entity_type,
    Timestamp timestamp) {
  if (!parallel_engine_) {
    // Fallback to serial execution
    std::vector<ColumnQueryResult> results;
    results.reserve(column_ids.size());
    for (uint16_t col_id : column_ids) {
      CedarKey key;
      if (entity_type == EntityType::Vertex) {
        key = CedarKey::Vertex(entity_id, col_id, timestamp);
      } else if (entity_type == EntityType::EdgeOut) {
        key = CedarKey::EdgeOut(entity_id, col_id, EdgeTypeId(col_id), timestamp);
      } else {
        key = CedarKey::EdgeIn(entity_id, col_id, EdgeTypeId(col_id), timestamp);
      }
      auto value = Get(key);
      ColumnQueryResult result;
      result.column_id = col_id;
      result.value = value;
      result.status = Status::OK();
      results.push_back(std::move(result));
    }
    return results;
  }
  
  return parallel_engine_->QueryColumns(entity_id, column_ids,
                                        static_cast<uint8_t>(entity_type),
                                        timestamp);
}

ThreadPoolQueryExecutor::Stats LsmEngine::GetParallelQueryStats() const {
  if (!parallel_engine_) {
    return ThreadPoolQueryExecutor::Stats();
  }
  return parallel_engine_->GetStats();
}

}  // namespace cedar
