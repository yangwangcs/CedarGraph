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

#include "cedar/db/graph_db_impl.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <queue>

#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/sst/sst_builder_factory.h"

namespace {

// RAII guard that pauses background work on construction and resumes on destruction.
struct BgWorkPauseGuard {
  explicit BgWorkPauseGuard(std::atomic<bool>* flag, std::condition_variable* cv)
      : flag_(flag), cv_(cv), released_(false) {
    flag_->store(true);
  }
  void Release() {
    if (!released_) {
      flag_->store(false);
      if (cv_) cv_->notify_all();
      released_ = true;
    }
  }
  ~BgWorkPauseGuard() {
    if (!released_) {
      flag_->store(false);
      if (cv_) cv_->notify_all();
    }
  }
 private:
  std::atomic<bool>* flag_;
  std::condition_variable* cv_;
  bool released_;
};

}  // namespace

namespace cedar {

// ==================== CedarGraphDBImpl 构造函数/析构函数 ====================

CedarGraphDBImpl::CedarGraphDBImpl(const std::string& db_path, 
                                 const CedarGraphOptions& options)
    : db_path_(db_path),
      options_(options),
      env_(options.env ? options.env : Env::Default()),
      manifest_manager_(db_path, env_) {}

CedarGraphDBImpl::~CedarGraphDBImpl() {
  if (opened_.load()) {
    Close().IgnoreError();
  }
}

// ==================== CedarGraphDBImpl 打开/关闭 ====================

Status CedarGraphDBImpl::Open() {
  if (opened_.load()) {
    return Status::InvalidArgument("CedarGraphDB", "already opened");
  }
  
  // 创建数据库目录
  if (!std::filesystem::exists(db_path_)) {
    if (options_.create_if_missing) {
      try {
        std::filesystem::create_directories(db_path_);
      } catch (const std::exception& e) {
        return Status::IOError("CedarGraphDB", 
                               std::string("Failed to create directory: ") + e.what());
      }
    } else {
      return Status::NotFound("CedarGraphDB", 
                              "Database not found: " + db_path_);
    }
  }
  
  // 检查数据库是否已存在
  std::string current_file = db_path_ + "/CURRENT";
  bool db_exists = std::filesystem::exists(current_file);
  
  if (db_exists && options_.error_if_exists) {
    return Status::InvalidArgument("CedarGraphDB", 
                                   "Database already exists: " + db_path_);
  }
  
  // 初始化 Manifest
  Status s = manifest_manager_.Initialize(options_.create_if_missing);
  if (!s.ok()) {
    return s;
  }
  
  // 加载当前版本
  uint64_t next_file_number = 1;
  uint64_t last_sequence = 0;
  std::shared_ptr<Version> current_version;
  
  s = manifest_manager_.LoadCurrentVersion(&current_version,
                                           &next_file_number,
                                           &last_sequence);
  if (!s.ok()) {
    // 如果是新数据库，创建初始版本
    if (!db_exists) {
      current_version = std::make_shared<Version>(1, 0);
    } else {
      return s;
    }
  }
  
  version_set_.ApplyEdits({}, &current_version);
  version_set_.SetLastSequence(last_sequence);
  
  // 创建默认列族
  ColumnFamilyData* cf_data;
  s = CreateColumnFamilyInternal("default", 0, &cf_data);
  if (!s.ok()) {
    return s;
  }
  default_cf_ = cf_data;
  
  // 初始化 WAL
  if (options_.enable_wal) {
    std::string wal_dir = db_path_ + "/wal";
    if (!std::filesystem::exists(wal_dir)) {
      std::filesystem::create_directories(wal_dir);
    }
    
    WalOptions wal_options;
    wal_writer_ = std::make_unique<WalWriter>(wal_dir, env_, wal_options);
    
    // 打开 WAL 文件
    s = wal_writer_->Open();
    if (!s.ok()) {
      return s;
    }
  }
  
  // 启动后台线程
  if (options_.max_background_flushes > 0) {
    bg_flush_thread_ = std::thread(&CedarGraphDBImpl::BackgroundFlushThread, this);
  }
  if (options_.max_background_compactions > 0) {
    bg_compact_thread_ = std::thread(&CedarGraphDBImpl::BackgroundCompactThread, this);
  }
  
  opened_.store(true);
  return Status::OK();
}

Status CedarGraphDBImpl::Close() {
  if (!opened_.load()) {
    return Status::OK();
  }
  
  shutting_down_.store(true);
  
  // 通知后台线程停止
  {
    std::unique_lock<std::mutex> lock(bg_mutex_);
    bg_cv_.notify_all();
  }
  
  // 等待后台线程结束
  if (bg_flush_thread_.joinable()) {
    bg_flush_thread_.join();
  }
  if (bg_compact_thread_.joinable()) {
    bg_compact_thread_.join();
  }
  
  // Flush 所有 MemTable (with retry and status checking)
  for (auto& cf : column_families_) {
    if (cf->engine) {
      Status s;
      for (int attempt = 1; attempt <= 3; ++attempt) {
        s = cf->engine->ForceFlush();
        if (s.ok()) break;
        std::cerr << "[CedarGraphDBImpl::Close] ForceFlush attempt " << attempt
                  << " failed for cf " << cf->name << ": " << s.ToString() << std::endl;
        if (attempt < 3) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
      }
      if (!s.ok()) {
        std::cerr << "[CedarGraphDBImpl::Close] ForceFlush ultimately failed for cf "
                  << cf->name << ": " << s.ToString() << std::endl;
      }
    }
  }
  
  // 关闭 WAL (ensure sync before close)
  if (wal_writer_) {
    Status wal_sync = wal_writer_->Sync();
    if (!wal_sync.ok()) {
      std::cerr << "[CedarGraphDBImpl::Close] WAL sync failed: " << wal_sync.ToString() << std::endl;
    }
    wal_writer_->Close();
    wal_writer_.reset();
  }
  
  // 关闭 Manifest
  manifest_manager_.Close();
  
  // 清理列族
  {
    std::unique_lock<std::mutex> lock(cf_mutex_);
    column_families_.clear();
    default_cf_ = nullptr;
  }
  
  opened_.store(false);
  return Status::OK();
}

// ==================== CedarGraphDBImpl 基本操作 ====================

Status CedarGraphDBImpl::Put(const CedarKey& key, 
                            const Descriptor& descriptor,
                            const WriteOptions& options) {
  if (!opened_.load()) {
    return Status::InvalidArgument("CedarGraphDB", "not opened");
  }
  
  // 分配序列号 (用作 txn_version) — atomic RMW to avoid duplicate seqs
  uint64_t seq = version_set_.FetchAddSequence(1) + 1;
  
  // 写入 WAL
  if (options_.enable_wal) {
    Status s = WriteToWAL(key, descriptor, seq);
    if (!s.ok()) {
      return s;
    }
  }
  
  // 写入 MemTable - 使用 key.timestamp() 作为业务时间戳
  // 使用序列号作为 txn_version (MVCC 用)
  Status s = default_cf_->engine->Put(key, descriptor, key.timestamp());
  if (s.ok()) {
    stats_.puts.fetch_add(1, std::memory_order_relaxed);
  }
  
  // 检查是否需要 Flush
  MaybeScheduleFlush();
  
  return s;
}

Status CedarGraphDBImpl::Delete(const CedarKey& key,
                               const WriteOptions& options) {
  // 删除就是写入 Tombstone
  return Put(key, Descriptor::Tombstone(key.column_id()), options);
}

std::optional<Descriptor> CedarGraphDBImpl::Get(const CedarKey& key,
                                               const ReadOptions& options) {
  if (!opened_.load()) {
    return std::nullopt;
  }
  
  stats_.gets.fetch_add(1, std::memory_order_relaxed);
  
  // 使用 LsmEngine 的 Get
  return default_cf_->engine->Get(key);
}

// ==================== CedarGraphDBImpl 事务支持 ====================

std::unique_ptr<OCCTransaction> CedarGraphDBImpl::BeginTransaction(
    const TransactionOptions& options) {
  if (!opened_.load() || !options_.enable_transaction) {
    return nullptr;
  }
  
  // Create transaction via LsmEngine.
  // A unified TransactionManager can wrap this in the future.
  return default_cf_->engine->BeginTransaction(options);
}

// ==================== CedarGraphDBImpl 管理操作 ====================

Status CedarGraphDBImpl::Flush(const FlushOptions& options) {
  if (!opened_.load()) {
    return Status::InvalidArgument("CedarGraphDB", "not opened");
  }
  
  Status s = default_cf_->engine->ForceFlush();
  if (s.ok()) {
    stats_.flushes.fetch_add(1, std::memory_order_relaxed);
  }
  return s;
}

Status CedarGraphDBImpl::CompactRange(const CompactRangeOptions& options) {
  if (!opened_.load()) {
    return Status::InvalidArgument("CedarGraphDB", "not opened");
  }

  // Pause background compaction/flushing during manual compaction
  BgWorkPauseGuard pause_guard(&bg_work_paused_, &bg_cv_);

  // Flush all memtables so compaction works purely on SSTs
  Status s = Flush(FlushOptions{});
  if (!s.ok()) {
    return s;
  }

  // Compact each level that has files overlapping the requested range
  for (int level = 0; level < options_.max_levels; ++level) {
    auto version = version_set_.GetCurrentVersion();
    const auto& files = version->GetFiles(level);
    bool has_overlap = false;
    for (const auto& f : files) {
      if (f.Overlaps(options.start_entity_id, options.end_entity_id)) {
        has_overlap = true;
        break;
      }
    }
    if (has_overlap) {
      s = DoCompactionRange(level, options.start_entity_id, options.end_entity_id);
      if (!s.ok()) {
        return s;
      }
    }
  }

  stats_.compactions.fetch_add(1, std::memory_order_relaxed);
  return Status::OK();
}

// ==================== CedarGraphDBImpl 快照支持 ====================

const Snapshot* CedarGraphDBImpl::GetSnapshot() {
  std::unique_lock<std::mutex> lock(snapshot_mutex_);
  
  uint64_t id = next_snapshot_id_.fetch_add(1);
  uint64_t seq = version_set_.GetLastSequence();
  uint64_t ts = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  
  auto* snapshot = new SnapshotImpl(id, seq, ts);
  snapshots_.push_back(snapshot);
  
  return snapshot;
}

void CedarGraphDBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  std::unique_lock<std::mutex> lock(snapshot_mutex_);
  
  auto it = std::find(snapshots_.begin(), snapshots_.end(), snapshot);
  if (it != snapshots_.end()) {
    snapshots_.erase(it);
    delete snapshot;
  }
}

// ==================== CedarGraphDBImpl 列族操作 ====================

Status CedarGraphDBImpl::CreateColumnFamily(const std::string& name,
                                           ColumnFamilyHandle** handle) {
  if (!opened_.load()) {
    return Status::InvalidArgument("CedarGraphDB", "not opened");
  }
  
  std::unique_lock<std::mutex> lock(cf_mutex_);
  
  // 检查是否已存在
  for (const auto& cf : column_families_) {
    if (cf->name == name) {
      return Status::InvalidArgument("CedarGraphDB", 
                                     "Column family already exists: " + name);
    }
  }
  
  // 分配 ID
  uint32_t cf_id = next_cf_id_.fetch_add(1);
  
  // 创建列族数据
  ColumnFamilyData* cf_data;
  Status s = CreateColumnFamilyInternal(name, cf_id, &cf_data);
  if (!s.ok()) {
    return s;
  }
  
  // 记录到 Manifest
  ManifestEdit edit = ManifestEdit::AddColumnFamily(name);
  std::shared_ptr<Version> new_version;
  s = manifest_manager_.ApplyEdit(edit, &new_version);
  if (!s.ok()) {
    return s;
  }
  
  if (handle) {
    *handle = new ColumnFamilyHandleImpl(cf_data);
  }
  
  return Status::OK();
}

Status CedarGraphDBImpl::CreateColumnFamilyInternal(const std::string& name, 
                                                    uint32_t cf_id,
                                                    ColumnFamilyData** cf_data) {
  auto cf = std::make_unique<ColumnFamilyData>(cf_id, name);
  
  // 创建 LsmEngine
  std::string cf_path = db_path_ + "/" + name;
  if (!std::filesystem::exists(cf_path)) {
    std::filesystem::create_directories(cf_path);
  }
  
  // Create an independent LsmEngine for this column family.
  CedarOptions legacy_options;
  legacy_options.create_if_missing = true;
  cf->engine = std::make_unique<LsmEngine>(cf_path, legacy_options, env_);
  
  Status s = cf->engine->Open();
  if (!s.ok()) {
    return s;
  }
  
  *cf_data = cf.get();
  column_families_.push_back(std::move(cf));
  
  return Status::OK();
}

Status CedarGraphDBImpl::DropColumnFamily(ColumnFamilyHandle* handle) {
  if (!handle) {
    return Status::InvalidArgument("DropColumnFamily", "null handle");
  }
  
  std::lock_guard<std::mutex> lock(cf_mutex_);
  auto it = std::find_if(column_families_.begin(), column_families_.end(),
    [handle](const auto& cf) { return cf.get() == static_cast<ColumnFamilyHandleImpl*>(handle)->GetCFD(); });
  
  if (it == column_families_.end()) {
    return Status::NotFound("Column family not found");
  }
  
  // Close the engine before removing
  if ((*it)->engine) {
    (*it)->engine->Close();
  }

  delete handle;
  column_families_.erase(it);
  return Status::OK();
}

ColumnFamilyHandle* CedarGraphDBImpl::DefaultColumnFamily() {
  return new ColumnFamilyHandleImpl(default_cf_);
}

// ==================== CedarGraphDBImpl 属性与统计 ====================

Status CedarGraphDBImpl::GetProperty(const std::string& property, 
                                    std::string* value) {
  if (property == "cedar.num-files-at-level0") {
    auto stats = default_cf_->engine->GetStats();
    *value = std::to_string(stats.sst_count);
    return Status::OK();
  }
  if (property == "cedar.stats") {
    *value = GetStatsString();
    return Status::OK();
  }
  return Status::NotFound("Property", property);
}

std::string CedarGraphDBImpl::GetStatsString() {
  std::ostringstream oss;
  oss << "CedarGraphDB Stats:\n"
      << "  puts: " << stats_.puts.load() << "\n"
      << "  gets: " << stats_.gets.load() << "\n"
      << "  deletes: " << stats_.deletes.load() << "\n"
      << "  flushes: " << stats_.flushes.load() << "\n"
      << "  compactions: " << stats_.compactions.load() << "\n"
      << "  transactions: " << stats_.transactions.load() << "\n"
      << "  last_sequence: " << version_set_.GetLastSequence() << "\n";
  return oss.str();
}

uint64_t CedarGraphDBImpl::GetLatestSequenceNumber() const {
  return version_set_.GetLastSequence();
}

// ==================== CedarGraphDBImpl 备份 ====================

Status CedarGraphDBImpl::CreateCheckpoint(const std::string& checkpoint_dir) {
  // 1. 暂停后台操作 (RAII guard ensures resume on all exit paths)
  BgWorkPauseGuard pause_guard(&bg_work_paused_, &bg_cv_);
  
  // 2. Flush 所有 MemTable
  Status s = Flush(FlushOptions{});
  if (!s.ok()) {
    return s;
  }
  
  // 3. 同步 WAL
  if (wal_writer_) {
    s = wal_writer_->Sync();
    if (!s.ok()) {
      return s;
    }
  }
  
  // 4. 创建检查点目录
  try {
    if (std::filesystem::exists(checkpoint_dir)) {
      std::filesystem::remove_all(checkpoint_dir);
    }
    std::filesystem::create_directories(checkpoint_dir);
  } catch (const std::exception& e) {
    return Status::IOError("CreateCheckpoint", e.what());
  }
  
  // 5. 复制 Manifest 文件
  std::string current_manifest = manifest_manager_.GetManifestFileNumber() > 0 
      ? db_path_ + "/MANIFEST-" + std::to_string(manifest_manager_.GetManifestFileNumber())
      : "";
  
  if (!current_manifest.empty() && std::filesystem::exists(current_manifest)) {
    try {
      std::filesystem::copy_file(
          current_manifest,
          checkpoint_dir + "/MANIFEST",
          std::filesystem::copy_options::overwrite_existing);
    } catch (const std::exception& e) {
      return Status::IOError("CreateCheckpoint", 
                             std::string("Failed to copy manifest: ") + e.what());
    }
  }
  
  // 6. 硬链接所有 SST 文件（从 LsmEngine 获取，因为 Flush 更新的是 LsmEngine 的 levels_ 而不是 VersionSet）
  // 注：SST 文件存储在列族目录下（如 default/）
  const auto& levels = default_cf_->engine->GetSstFiles();
  std::string cf_path = db_path_ + "/" + default_cf_->name;
  for (int level = 0; level < static_cast<int>(levels.size()); level++) {
    const auto& files = levels[level];
    for (const auto& file : files) {
      std::string src = cf_path + "/" + std::to_string(file.file_number) + ".sst";
      std::string dst = checkpoint_dir + "/" + std::to_string(file.file_number) + ".sst";
      
      if (std::filesystem::exists(src)) {
        try {
          // 尝试硬链接
          std::filesystem::create_hard_link(src, dst);
        } catch (...) {
          // 硬链接失败，使用复制
          try {
            std::filesystem::copy_file(src, dst);
          } catch (const std::exception& e) {
            return Status::IOError("CreateCheckpoint",
                                   std::string("Failed to copy SST file: ") + e.what());
          }
        }
      }
    }
  }
  
  // 7. 创建 CURRENT 文件
  std::ofstream current_file(checkpoint_dir + "/CURRENT");
  if (current_file.is_open()) {
    current_file << "MANIFEST" << std::endl;
    current_file.close();
  }
  
  // 8. 复制选项文件
  std::string options_file = db_path_ + "/OPTIONS";
  if (std::filesystem::exists(options_file)) {
    try {
      std::filesystem::copy_file(
          options_file,
          checkpoint_dir + "/OPTIONS",
          std::filesystem::copy_options::overwrite_existing);
    } catch (...) {
      // 选项文件复制失败不影响检查点功能
    }
  }
  
  // 9. 创建检查点元数据文件
  {
    std::ofstream meta(checkpoint_dir + "/CHECKPOINT_META");
    if (meta.is_open()) {
      auto now = std::chrono::system_clock::now();
      auto time_t = std::chrono::system_clock::to_time_t(now);
      
      meta << "timestamp: " << time_t << std::endl;
      meta << "sequence: " << version_set_.GetLastSequence() << std::endl;
      meta << "manifest_size: " << manifest_manager_.GetManifestSize() << std::endl;
      meta.close();
    }
  }
  
  // 10. 恢复后台操作
  pause_guard.Release();
  
  return Status::OK();
}

// ==================== CedarGraphDBImpl 内部方法 ====================

Status CedarGraphDBImpl::WriteToWAL(const CedarKey& key, 
                                    const Descriptor& desc,
                                    uint64_t sequence) {
  if (!wal_writer_) {
    return Status::OK();
  }
  
  std::lock_guard<std::mutex> lock(wal_mutex_);
  
  WalBatch batch;
  batch.Put(key, desc, Timestamp(sequence));
  
  return wal_writer_->WriteBatch(batch);
}

void CedarGraphDBImpl::BackgroundFlushThread() {
  while (!shutting_down_.load()) {
    std::unique_lock<std::mutex> lock(bg_mutex_);
    bg_cv_.wait_for(lock, std::chrono::seconds(1),
                    [this] { return shutting_down_.load() || bg_work_paused_.load(); });
    
    if (shutting_down_.load()) break;
    if (bg_work_paused_.load()) continue;
    
    // 检查是否需要 Flush
    MaybeScheduleFlush();
  }
}

void CedarGraphDBImpl::BackgroundCompactThread() {
  while (!shutting_down_.load()) {
    std::unique_lock<std::mutex> lock(bg_mutex_);
    bg_cv_.wait_for(lock, std::chrono::seconds(5),
                    [this] { return shutting_down_.load() || bg_work_paused_.load(); });
    
    if (shutting_down_.load()) break;
    if (bg_work_paused_.load()) continue;
    
    // 检查是否需要压缩
    MaybeScheduleCompaction();
  }
}

Status CedarGraphDBImpl::MaybeScheduleFlush() {
  // 检查默认列族的 MemTable 大小
  if (!default_cf_ || !default_cf_->engine) {
    return Status::OK();
  }
  
  // 获取 MemTable 大小（近似值）
  size_t memtable_size = default_cf_->engine->GetMemTable()->ApproximateMemoryUsage();
  
  // 如果超过阈值，触发 Flush
  if (memtable_size >= options_.write_buffer_size) {
    Status s = default_cf_->engine->ForceFlush();
    if (s.ok()) {
      stats_.flushes.fetch_add(1, std::memory_order_relaxed);
      
      // Flush 后检查是否需要压缩
      MaybeScheduleCompaction();
    }
    return s;
  }
  
  return Status::OK();
}

Status CedarGraphDBImpl::MaybeScheduleCompaction() {
  // 获取当前版本
  auto version = version_set_.GetCurrentVersion();
  
  // 检查每层文件数量和大小
  for (int level = 0; level < options_.max_levels; level++) {
    const auto& files = version->GetFiles(level);
    
    // 计算该层总大小
    uint64_t total_size = 0;
    for (const auto& file : files) {
      total_size += file.file_size;
    }
    
    // 检查是否需要压缩
    bool need_compaction = false;
    
    if (level == 0) {
      // L0: 文件数量超过阈值 (使用写缓冲区数量作为参考)
      if (files.size() >= static_cast<size_t>(options_.min_write_buffer_number_to_merge)) {
        need_compaction = true;
      }
    } else {
      // 其他层：大小超过阈值
      uint64_t level_size_limit = options_.target_file_size_base * 
          static_cast<uint64_t>(std::pow(options_.max_bytes_for_level_multiplier, level));
      if (total_size >= level_size_limit) {
        need_compaction = true;
      }
    }
    
    if (need_compaction) {
      // 执行压缩
      Status s = DoCompaction(level);
      if (!s.ok()) {
        return s;
      }
    }
  }
  
  return Status::OK();
}

Status CedarGraphDBImpl::DoCompaction(int level) {
  // 获取当前版本
  auto version = version_set_.GetCurrentVersion();
  
  // 获取该层的文件
  const auto& input_files = version->GetFiles(level);
  if (input_files.empty()) {
    return Status::OK();
  }
  
  // 选择要压缩的文件（简单策略：选择最老的文件）
  std::vector<FileMetaData> inputs;
  size_t max_files = (level == 0) ? 4 : 1;
  for (size_t i = 0; i < std::min(input_files.size(), max_files); i++) {
    inputs.push_back(input_files[i]);
  }
  
  // 获取下一层的文件（用于合并）
  std::vector<FileMetaData> level_files;
  if (level + 1 < options_.max_levels) {
    level_files = version->GetFiles(level + 1);
  }
  
  // 计算输出文件的 key 范围
  uint64_t smallest_entity = UINT64_MAX;
  uint64_t largest_entity = 0;
  for (const auto& file : inputs) {
    smallest_entity = std::min(smallest_entity, file.smallest_entity_id);
    largest_entity = std::max(largest_entity, file.largest_entity_id);
  }
  
  // 检查与下一层文件的重叠
  std::vector<FileMetaData> overlapping_files;
  for (const auto& file : level_files) {
    if (file.Overlaps(smallest_entity, largest_entity)) {
      overlapping_files.push_back(file);
    }
  }
  
  // 收集所有需要合并的文件
  std::vector<FileMetaData> all_inputs = inputs;
  all_inputs.insert(all_inputs.end(), overlapping_files.begin(),
                    overlapping_files.end());
  
  // 流式合并：使用最小堆避免将所有数据载入内存
  struct Source {
    std::unique_ptr<ZoneColumnarSstReader> reader;
    std::unique_ptr<ZoneColumnarSstReader::Iterator> iter;
  };
  std::vector<Source> sources;
  sources.reserve(all_inputs.size());

  for (const auto& file : all_inputs) {
    std::string filepath = db_path_ + "/" + std::to_string(file.file_number) + ".sst";
    auto reader = std::make_unique<ZoneColumnarSstReader>(filepath);
    Status s = reader->Open();
    if (!s.ok()) {
      return s;
    }
    auto* iter = reader->NewIterator();
    iter->SeekToFirst();
    if (iter->Valid()) {
      sources.push_back({std::move(reader),
                         std::unique_ptr<ZoneColumnarSstReader::Iterator>(iter)});
    } else {
      delete iter;
    }
  }

  if (sources.empty()) {
    // 无数据可合并，仅删除输入文件
    std::vector<ManifestEdit> edits;
    for (const auto& file : all_inputs) {
      edits.push_back(ManifestEdit::DeleteFile(file.level, file.file_number));
    }
    std::shared_ptr<Version> new_version;
    Status s = version_set_.ApplyEdits(edits, &new_version);
    if (!s.ok()) {
      return s;
    }
    for (const auto& edit : edits) {
      s = manifest_manager_.LogEdit(edit);
      if (!s.ok()) {
        return s;
      }
    }
    for (const auto& file : all_inputs) {
      std::string old_path = db_path_ + "/" + std::to_string(file.file_number) + ".sst";
      env_->RemoveFile(old_path).IgnoreError();
    }
    stats_.compactions.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
  }

  // 按全局排序契约排序（entity_id ASC, type ASC, col_id ASC, target_id ASC,
  // timestamp DESC, sequence ASC）
  struct HeapEntry {
    CedarKey key;
    Descriptor descriptor;
    Timestamp txn_version;
    size_t source_idx;
    bool operator>(const HeapEntry& o) const {
      return o.key.LessForSorting(key);
    }
  };
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> min_heap;

  // 初始化堆：每个源的第一个条目
  for (size_t i = 0; i < sources.size(); ++i) {
    min_heap.push({sources[i].iter->Key(), sources[i].iter->Value(), sources[i].iter->TxnVersion(), i});
  }

  // 确定输出层级
  int output_level = (level + 1 < options_.max_levels) ? level + 1 : level;

  // 创建新的 SST 文件
  uint64_t new_file_number = version_set_.GetNextFileNumber();
  std::string output_path = db_path_ + "/" + std::to_string(new_file_number) + ".sst";

  WritableFile* file = nullptr;
  Status s = env_->NewWritableFile(output_path, &file);
  if (!s.ok()) {
    return s;
  }

  auto builder = SstBuilderFactory::Create(file, db_path_);
  uint64_t out_min_entity = UINT64_MAX;
  uint64_t out_max_entity = 0;
  uint64_t out_min_ts = UINT64_MAX;
  uint64_t out_max_ts = 0;

  // K路流式合并
  CedarKey last_key;
  bool has_last = false;
  while (!min_heap.empty()) {
    auto entry = min_heap.top();
    min_heap.pop();

    // Skip duplicate keys (same entity, type, column, target, timestamp, sequence)
    if (has_last && entry.key.CompareForSorting(last_key) == 0) {
      size_t idx = entry.source_idx;
      sources[idx].iter->Next();
      if (sources[idx].iter->Valid()) {
        min_heap.push({sources[idx].iter->Key(), sources[idx].iter->Value(), sources[idx].iter->TxnVersion(), idx});
      }
      continue;
    }
    last_key = entry.key;
    has_last = true;

    builder->Add(entry.key, entry.descriptor, entry.txn_version);
    out_min_entity = std::min(out_min_entity, entry.key.entity_id());
    out_max_entity = std::max(out_max_entity, entry.key.entity_id());
    out_min_ts = std::min(out_min_ts, entry.key.timestamp().value());
    out_max_ts = std::max(out_max_ts, entry.key.timestamp().value());

    size_t idx = entry.source_idx;
    sources[idx].iter->Next();
    if (sources[idx].iter->Valid()) {
      min_heap.push({sources[idx].iter->Key(), sources[idx].iter->Value(), sources[idx].iter->TxnVersion(), idx});
    }
  }
  
  s = builder->Finish();
  delete file;
  if (!s.ok()) {
    env_->RemoveFile(output_path);
    return s;
  }
  
  uint64_t file_size = 0;
  s = env_->GetFileSize(output_path, &file_size);
  if (!s.ok()) {
    env_->RemoveFile(output_path);
    return s;
  }
  
  // 构造新文件的元数据
  FileMetaData new_meta;
  new_meta.file_number = new_file_number;
  new_meta.level = output_level;
  new_meta.file_size = file_size;
  new_meta.smallest_entity_id = out_min_entity;
  new_meta.largest_entity_id = out_max_entity;
  new_meta.smallest_timestamp = out_min_ts;
  new_meta.largest_timestamp = out_max_ts;
  new_meta.num_entries = builder->NumEntries();
  new_meta.num_deletions = 0;
  
  // 构造 Manifest 编辑：删除旧文件，添加新文件
  std::vector<ManifestEdit> edits;
  edits.reserve(all_inputs.size() + 1);
  for (const auto& f : all_inputs) {
    edits.push_back(ManifestEdit::DeleteFile(f.level, f.file_number));
  }
  edits.push_back(ManifestEdit::AddFile(output_level, new_meta));
  
  // 应用到 VersionSet
  std::shared_ptr<Version> new_version;
  s = version_set_.ApplyEdits(edits, &new_version);
  if (!s.ok()) {
    env_->RemoveFile(output_path);
    return s;
  }
  
  // 记录到 Manifest
  for (const auto& edit : edits) {
    s = manifest_manager_.LogEdit(edit);
    if (!s.ok()) {
      return s;
    }
  }
  
  // 删除旧的 SST 文件
  for (const auto& f : all_inputs) {
    std::string old_path = db_path_ + "/" + std::to_string(f.file_number) + ".sst";
    env_->RemoveFile(old_path).IgnoreError();
  }
  
  // 更新统计
  stats_.compactions.fetch_add(1, std::memory_order_relaxed);
  
  return Status::OK();
}

Status CedarGraphDBImpl::DoCompactionRange(int level,
                                            uint64_t start_entity_id,
                                            uint64_t end_entity_id) {
  auto version = version_set_.GetCurrentVersion();

  // Select only files that overlap the requested entity range
  const auto& all_files = version->GetFiles(level);
  std::vector<FileMetaData> inputs;
  for (const auto& f : all_files) {
    if (f.Overlaps(start_entity_id, end_entity_id)) {
      inputs.push_back(f);
    }
  }
  if (inputs.empty()) {
    return Status::OK();
  }

  // Gather overlapping files from the next level (same as DoCompaction)
  std::vector<FileMetaData> level_files;
  if (level + 1 < options_.max_levels) {
    level_files = version->GetFiles(level + 1);
  }

  uint64_t smallest_entity = UINT64_MAX;
  uint64_t largest_entity = 0;
  for (const auto& file : inputs) {
    smallest_entity = std::min(smallest_entity, file.smallest_entity_id);
    largest_entity = std::max(largest_entity, file.largest_entity_id);
  }

  std::vector<FileMetaData> overlapping_files;
  for (const auto& file : level_files) {
    if (file.Overlaps(smallest_entity, largest_entity)) {
      overlapping_files.push_back(file);
    }
  }

  std::vector<FileMetaData> all_inputs = inputs;
  all_inputs.insert(all_inputs.end(), overlapping_files.begin(),
                    overlapping_files.end());

  // K-way streaming merge — identical logic to DoCompaction
  struct Source {
    std::unique_ptr<ZoneColumnarSstReader> reader;
    std::unique_ptr<ZoneColumnarSstReader::Iterator> iter;
  };
  std::vector<Source> sources;
  sources.reserve(all_inputs.size());

  for (const auto& file : all_inputs) {
    std::string filepath = db_path_ + "/" + std::to_string(file.file_number) + ".sst";
    auto reader = std::make_unique<ZoneColumnarSstReader>(filepath);
    Status s = reader->Open();
    if (!s.ok()) {
      return s;
    }
    auto* iter = reader->NewIterator();
    iter->SeekToFirst();
    if (iter->Valid()) {
      sources.push_back({std::move(reader),
                         std::unique_ptr<ZoneColumnarSstReader::Iterator>(iter)});
    } else {
      delete iter;
    }
  }

  if (sources.empty()) {
    std::vector<ManifestEdit> edits;
    for (const auto& file : all_inputs) {
      edits.push_back(ManifestEdit::DeleteFile(file.level, file.file_number));
    }
    std::shared_ptr<Version> new_version;
    Status s = version_set_.ApplyEdits(edits, &new_version);
    if (!s.ok()) {
      return s;
    }
    for (const auto& edit : edits) {
      s = manifest_manager_.LogEdit(edit);
      if (!s.ok()) {
        return s;
      }
    }
    for (const auto& file : all_inputs) {
      std::string old_path = db_path_ + "/" + std::to_string(file.file_number) + ".sst";
      env_->RemoveFile(old_path).IgnoreError();
    }
    return Status::OK();
  }

  struct HeapEntry {
    CedarKey key;
    Descriptor descriptor;
    Timestamp txn_version;
    size_t source_idx;
    bool operator>(const HeapEntry& o) const {
      return o.key.LessForSorting(key);
    }
  };
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> min_heap;

  for (size_t i = 0; i < sources.size(); ++i) {
    min_heap.push({sources[i].iter->Key(), sources[i].iter->Value(),
                   sources[i].iter->TxnVersion(), i});
  }

  int output_level = (level + 1 < options_.max_levels) ? level + 1 : level;
  uint64_t new_file_number = version_set_.GetNextFileNumber();
  std::string output_path = db_path_ + "/" + std::to_string(new_file_number) + ".sst";

  WritableFile* file = nullptr;
  Status s = env_->NewWritableFile(output_path, &file);
  if (!s.ok()) {
    return s;
  }

  auto builder = SstBuilderFactory::Create(file, db_path_);
  uint64_t out_min_entity = UINT64_MAX;
  uint64_t out_max_entity = 0;
  uint64_t out_min_ts = UINT64_MAX;
  uint64_t out_max_ts = 0;

  CedarKey last_key;
  bool has_last = false;
  while (!min_heap.empty()) {
    auto entry = min_heap.top();
    min_heap.pop();

    if (has_last && entry.key.CompareForSorting(last_key) == 0) {
      size_t idx = entry.source_idx;
      sources[idx].iter->Next();
      if (sources[idx].iter->Valid()) {
        min_heap.push({sources[idx].iter->Key(), sources[idx].iter->Value(),
                       sources[idx].iter->TxnVersion(), idx});
      }
      continue;
    }
    last_key = entry.key;
    has_last = true;

    builder->Add(entry.key, entry.descriptor, entry.txn_version);
    out_min_entity = std::min(out_min_entity, entry.key.entity_id());
    out_max_entity = std::max(out_max_entity, entry.key.entity_id());
    out_min_ts = std::min(out_min_ts, entry.key.timestamp().value());
    out_max_ts = std::max(out_max_ts, entry.key.timestamp().value());

    size_t idx = entry.source_idx;
    sources[idx].iter->Next();
    if (sources[idx].iter->Valid()) {
      min_heap.push({sources[idx].iter->Key(), sources[idx].iter->Value(),
                     sources[idx].iter->TxnVersion(), idx});
    }
  }

  s = builder->Finish();
  delete file;
  if (!s.ok()) {
    env_->RemoveFile(output_path);
    return s;
  }

  uint64_t file_size = 0;
  s = env_->GetFileSize(output_path, &file_size);
  if (!s.ok()) {
    env_->RemoveFile(output_path);
    return s;
  }

  FileMetaData new_meta;
  new_meta.file_number = new_file_number;
  new_meta.level = output_level;
  new_meta.file_size = file_size;
  new_meta.smallest_entity_id = out_min_entity;
  new_meta.largest_entity_id = out_max_entity;
  new_meta.smallest_timestamp = out_min_ts;
  new_meta.largest_timestamp = out_max_ts;
  new_meta.num_entries = builder->NumEntries();
  new_meta.num_deletions = 0;

  std::vector<ManifestEdit> edits;
  edits.reserve(all_inputs.size() + 1);
  for (const auto& f : all_inputs) {
    edits.push_back(ManifestEdit::DeleteFile(f.level, f.file_number));
  }
  edits.push_back(ManifestEdit::AddFile(output_level, new_meta));

  std::shared_ptr<Version> new_version;
  s = version_set_.ApplyEdits(edits, &new_version);
  if (!s.ok()) {
    env_->RemoveFile(output_path);
    return s;
  }

  for (const auto& edit : edits) {
    s = manifest_manager_.LogEdit(edit);
    if (!s.ok()) {
      return s;
    }
  }

  for (const auto& f : all_inputs) {
    std::string old_path = db_path_ + "/" + std::to_string(f.file_number) + ".sst";
    env_->RemoveFile(old_path).IgnoreError();
  }

  return Status::OK();
}

ColumnFamilyData* CedarGraphDBImpl::FindColumnFamily(uint32_t id) {
  std::lock_guard<std::mutex> lock(cf_mutex_);
  for (const auto& cf : column_families_) {
    if (cf->id == id) {
      return cf.get();
    }
  }
  return nullptr;
}

ColumnFamilyData* CedarGraphDBImpl::FindColumnFamily(const std::string& name) {
  std::lock_guard<std::mutex> lock(cf_mutex_);
  for (const auto& cf : column_families_) {
    if (cf->name == name) {
      return cf.get();
    }
  }
  return nullptr;
}

// ==================== ColumnFamilyHandleImpl ====================

std::string ColumnFamilyHandleImpl::GetStats() const {
  return "ColumnFamily: " + cfd_->name;
}

}  // namespace cedar
