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

#include "cedar/db/graph_db.h"
#include "cedar/db/graph_db_impl.h"

#include <filesystem>
#include <fstream>
#include <iostream>

namespace cedar {

// ==================== CedarGraphDB 静态方法 ====================

Status CedarGraphDB::Open(const std::string& db_path,
                         const CedarGraphOptions& options,
                         CedarGraphDB** db_ptr) {
  if (db_ptr == nullptr) {
    return Status::InvalidArgument("CedarGraphDB", "db_ptr is null");
  }
  
  // 创建实现
  auto impl = std::make_shared<CedarGraphDBImpl>(db_path, options);
  
  // 打开数据库
  Status s = impl->Open();
  if (!s.ok()) {
    return s;
  }
  
  *db_ptr = new CedarGraphDB(impl);
  return Status::OK();
}

Status CedarGraphDB::Open(const std::string& db_path,
                         const CedarGraphOptions& options,
                         const std::vector<std::string>& column_families,
                         std::vector<CedarGraphDB*>* db_ptrs,
                         std::vector<ColumnFamilyHandle*>* handles) {
  if (db_ptrs == nullptr) {
    return Status::InvalidArgument("CedarGraphDB", "db_ptrs is null");
  }
  
  // 先打开默认数据库
  CedarGraphDB* default_db;
  Status s = Open(db_path, options, &default_db);
  if (!s.ok()) {
    return s;
  }
  
  db_ptrs->clear();
  db_ptrs->push_back(default_db);
  
  if (handles) {
    handles->clear();
    handles->push_back(default_db->DefaultColumnFamily());
  }
  
  // 清理（handles 中存储的指针在 db 关闭时会失效）
  (void)handles;
  
  // 创建其他列族
  for (size_t i = 1; i < column_families.size(); i++) {
    CedarGraphDB* cf_handle;
    s = default_db->CreateColumnFamily(column_families[i], &cf_handle);
    if (!s.ok()) {
      // 清理已创建的
      for (auto* db : *db_ptrs) {
        delete db;
      }
      db_ptrs->clear();
      return s;
    }
    db_ptrs->push_back(cf_handle);
    if (handles) {
      handles->push_back(cf_handle->DefaultColumnFamily());
    }
    
    // 清理
    (void)handles;
  }
  
  return Status::OK();
}

Status CedarGraphDB::DestroyDB(const std::string& db_path,
                              const CedarGraphOptions& options) {
  // 检查目录是否存在
  if (!std::filesystem::exists(db_path)) {
    return Status::OK();  // 目录不存在，视为成功
  }
  
  // 尝试删除所有文件
  try {
    std::filesystem::remove_all(db_path);
    return Status::OK();
  } catch (const std::exception& e) {
    return Status::IOError("DestroyDB", e.what());
  }
}

Status CedarGraphDB::RepairDB(const std::string& db_path,
                              const CedarGraphOptions& options) {
  if (!std::filesystem::exists(db_path)) {
    return Status::IOError("RepairDB: path does not exist: " + db_path);
  }

  Env* env = options.env ? options.env : Env::Default();

  // Phase 1: Scan for SST files and validate them
  std::vector<FileMetaData> valid_files;
  std::vector<std::string> sst_paths;

  // Scan root db_path and all subdirectories for .sst files
  try {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(db_path)) {
      if (entry.is_regular_file()) {
        std::string ext = entry.path().extension().string();
        if (ext == ".sst") {
          sst_paths.push_back(entry.path().string());
        }
      }
    }
  } catch (const std::exception& e) {
    return Status::IOError("RepairDB", std::string("Scan failed: ") + e.what());
  }

  for (const auto& path : sst_paths) {
    // Validate by opening the SST file
    ZoneColumnarSstReader reader(path);
    Status s = reader.Open();
    if (!s.ok()) {
      std::cerr << "[RepairDB] Skipping corrupt SST: " << path
                << " (" << s.ToString() << ")" << std::endl;
      continue;
    }

    // Extract metadata from the file
    FileMetaData meta;
    std::string filename = std::filesystem::path(path).filename().string();
    // File number from name: "{number}.sst"
    size_t dot_pos = filename.rfind('.');
    if (dot_pos != std::string::npos) {
      try {
        meta.file_number = std::stoull(filename.substr(0, dot_pos));
      } catch (...) {
        meta.file_number = 0;
      }
    }

    meta.level = 0;  // Safe default: place all recovered files at L0
    uint64_t fsize = 0;
    env->GetFileSize(path, &fsize);
    meta.file_size = fsize;
    meta.num_entries = reader.NumEntries();

    // Try to extract key range from the reader
    auto* iter = reader.NewIterator();
    iter->SeekToFirst();
    if (iter->Valid()) {
      meta.smallest_entity_id = iter->Key().entity_id();
      meta.smallest_timestamp = iter->Key().timestamp().value();
    }
    // Advance to the last entry
    while (iter->Valid()) {
      meta.largest_entity_id = iter->Key().entity_id();
      meta.largest_timestamp = iter->Key().timestamp().value();
      iter->Next();
    }
    delete iter;

    // If we couldn't determine range, use safe defaults
    if (meta.smallest_entity_id == 0 && meta.largest_entity_id == 0) {
      meta.smallest_entity_id = 0;
      meta.largest_entity_id = UINT64_MAX;
      meta.smallest_timestamp = 0;
      meta.largest_timestamp = UINT64_MAX;
    }

    valid_files.push_back(meta);
  }

  std::cout << "[RepairDB] Scanned " << sst_paths.size()
            << " SST files, " << valid_files.size() << " valid." << std::endl;

  // Phase 2: Backup old manifest / CURRENT
  std::string current_path = db_path + "/CURRENT";
  if (std::filesystem::exists(current_path)) {
    try {
      std::filesystem::rename(current_path, db_path + "/CURRENT.bak");
    } catch (const std::exception& e) {
      return Status::IOError("RepairDB",
                             std::string("Failed to backup CURRENT: ") + e.what());
    }
  }

  // Backup any existing MANIFEST files
  try {
    for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
      if (entry.is_regular_file()) {
        std::string name = entry.path().filename().string();
        if (name.find("MANIFEST-") == 0) {
          std::string bak = entry.path().string() + ".bak";
          std::filesystem::rename(entry.path().string(), bak);
        }
      }
    }
  } catch (const std::exception& e) {
    // Non-fatal: continue even if backup of old manifests fails
    (void)e;
  }

  // Phase 3: Create fresh manifest with valid files
  ManifestManager manifest(db_path, env);
  Status s = manifest.Initialize(true);  // create new manifest
  if (!s.ok()) {
    return Status::IOError("RepairDB",
                           std::string("Failed to create new manifest: ") + s.ToString());
  }

  for (const auto& meta : valid_files) {
    ManifestEdit edit = ManifestEdit::AddFile(0, meta);
    s = manifest.LogEdit(edit);
    if (!s.ok()) {
      manifest.Close();
      return Status::IOError("RepairDB",
                             std::string("Failed to log edit: ") + s.ToString());
    }
  }

  s = manifest.Sync();
  if (!s.ok()) {
    manifest.Close();
    return s;
  }
  manifest.Close();

  std::cout << "[RepairDB] Rebuilt manifest with " << valid_files.size()
            << " files." << std::endl;

  // Phase 4: Replay WALs by opening a transient LsmEngine
  std::string wal_dir = db_path + "/wal";
  if (std::filesystem::exists(wal_dir)) {
    CedarOptions legacy_options;
    legacy_options.create_if_missing = false;
    legacy_options.enable_wal = true;

    // We open each column family directory that contains SSTs
    // For simplicity, always repair the default column family
    std::string default_cf_path = db_path + "/default";
    if (!std::filesystem::exists(default_cf_path)) {
      default_cf_path = db_path;
    }

    LsmEngine engine(default_cf_path, legacy_options, env);
    s = engine.Open();
    if (!s.ok()) {
      std::cerr << "[RepairDB] Warning: LsmEngine open for WAL replay failed: "
                << s.ToString() << std::endl;
      // Continue: manifest is already repaired; WAL replay is best-effort
    } else {
      // Force flush any WAL-recovered data to SST
      s = engine.ForceFlush();
      if (!s.ok()) {
        std::cerr << "[RepairDB] Warning: ForceFlush after WAL replay failed: "
                  << s.ToString() << std::endl;
      }
      engine.Close();
      std::cout << "[RepairDB] WAL replay and flush completed." << std::endl;
    }
  }

  return Status::OK();
}

// ==================== CedarGraphDB 构造函数/析构函数 ====================

CedarGraphDB::CedarGraphDB(std::shared_ptr<CedarGraphDBImpl> impl,
                           ColumnFamilyHandle* cf_handle)
    : impl_(std::move(impl)), cf_handle_(cf_handle) {}

CedarGraphDB::~CedarGraphDB() {
  // 实现会自动清理
}

// ==================== CedarGraphDB 基本操作 ====================

Status CedarGraphDB::Put(const CedarKey& key, 
                        const Descriptor& descriptor,
                        const WriteOptions& options) {
  return impl_->Put(key, descriptor, options);
}

Status CedarGraphDB::Delete(const CedarKey& key,
                           const WriteOptions& options) {
  return impl_->Delete(key, options);
}

std::optional<Descriptor> CedarGraphDB::Get(const CedarKey& key,
                                           const ReadOptions& options) {
  return impl_->Get(key, options);
}

// ==================== CedarGraphDB 事务支持 ====================

std::unique_ptr<OCCTransaction> CedarGraphDB::BeginTransaction(
    const TransactionOptions& options) {
  return impl_->BeginTransaction(options);
}

// ==================== CedarGraphDB 管理操作 ====================

Status CedarGraphDB::Flush(const FlushOptions& options) {
  return impl_->Flush(options);
}

Status CedarGraphDB::CompactRange(const CompactRangeOptions& options) {
  return impl_->CompactRange(options);
}

// ==================== CedarGraphDB 快照支持 ====================

const Snapshot* CedarGraphDB::GetSnapshot() {
  return impl_->GetSnapshot();
}

void CedarGraphDB::ReleaseSnapshot(const Snapshot* snapshot) {
  impl_->ReleaseSnapshot(snapshot);
}

// ==================== CedarGraphDB 列族操作 ====================

Status CedarGraphDB::CreateColumnFamily(const std::string& name,
                                       CedarGraphDB** cf_handle) {
  ColumnFamilyHandle* handle;
  Status s = impl_->CreateColumnFamily(name, &handle);
  if (s.ok() && cf_handle) {
    // Return a new CedarGraphDB instance bound to this column family.
    // The new instance shares the underlying impl (reference-counted).
    *cf_handle = new CedarGraphDB(impl_, handle);
  }
  return s;
}

Status CedarGraphDB::DropColumnFamily(CedarGraphDB* cf_handle) {
  if (!cf_handle || !cf_handle->impl_) {
    return Status::InvalidArgument("DropColumnFamily", "null handle");
  }
  return cf_handle->impl_->DropColumnFamily(cf_handle->cf_handle_);
}

ColumnFamilyHandle* CedarGraphDB::DefaultColumnFamily() {
  return impl_->DefaultColumnFamily();
}

// ==================== CedarGraphDB 属性与统计 ====================

Status CedarGraphDB::GetProperty(const std::string& property, 
                                std::string* value) {
  return impl_->GetProperty(property, value);
}

std::string CedarGraphDB::GetStatsString() {
  return impl_->GetStatsString();
}

uint64_t CedarGraphDB::GetLatestSequenceNumber() const {
  return impl_->GetLatestSequenceNumber();
}

std::string CedarGraphDB::GetName() const {
  return impl_ ? impl_->GetLsmEngine()->GetDbPath() : "";
}

std::string CedarGraphDB::GetColumnFamilyName() const {
  return cf_handle_ ? cf_handle_->GetName() : "default";
}

// ==================== CedarGraphDB 备份 ====================

Status CedarGraphDB::CreateCheckpoint(const std::string& checkpoint_dir) {
  return impl_->CreateCheckpoint(checkpoint_dir);
}

Status CedarGraphDB::RestoreFromCheckpoint(const std::string& checkpoint_dir,
                                          const std::string& db_dir) {
  // 1. 检查检查点目录是否存在
  if (!std::filesystem::exists(checkpoint_dir)) {
    return Status::NotFound("RestoreFromCheckpoint", 
                            "Checkpoint directory not found: " + checkpoint_dir);
  }
  
  // 2. 检查检查点元数据
  std::string meta_file = checkpoint_dir + "/CHECKPOINT_META";
  if (!std::filesystem::exists(meta_file)) {
    return Status::Corruption("RestoreFromCheckpoint", 
                              "Checkpoint metadata not found");
  }
  
  // 3. 如果目标目录存在，先删除
  if (std::filesystem::exists(db_dir)) {
    try {
      std::filesystem::remove_all(db_dir);
    } catch (const std::exception& e) {
      return Status::IOError("RestoreFromCheckpoint", 
                             std::string("Failed to remove existing db: ") + e.what());
    }
  }
  
  // 4. 创建目标目录
  try {
    std::filesystem::create_directories(db_dir);
  } catch (const std::exception& e) {
    return Status::IOError("RestoreFromCheckpoint", e.what());
  }
  
  // 5. 复制所有文件从检查点到目标目录
  // 创建默认列族目录
  std::string default_cf_dir = db_dir + "/default";
  try {
    std::filesystem::create_directories(default_cf_dir);
    
    for (const auto& entry : std::filesystem::directory_iterator(checkpoint_dir)) {
      if (entry.is_regular_file()) {
        std::string filename = entry.path().filename().string();
        std::string src = entry.path().string();
        std::string dst;
        
        // 处理 MANIFEST 文件
        if (filename == "MANIFEST") {
          dst = db_dir + "/MANIFEST-1";
        } else if (filename.size() >= 6 && filename.substr(filename.size() - 6) == ".sst") {
          // SST 文件放在列族目录下
          dst = default_cf_dir + "/" + filename;
        } else {
          dst = db_dir + "/" + filename;
        }
        
        std::filesystem::copy_file(src, dst);
      }
    }
  } catch (const std::exception& e) {
    return Status::IOError("RestoreFromCheckpoint", 
                           std::string("Failed to copy files: ") + e.what());
  }
  
  // 6. 更新 CURRENT 文件
  std::ofstream current(db_dir + "/CURRENT");
  if (current.is_open()) {
    current << "MANIFEST-1" << std::endl;
    current.close();
  }
  
  return Status::OK();
}

// ==================== CedarGraphDB 内部接口 ====================

LsmEngine* CedarGraphDB::GetLsmEngine() {
  return impl_->GetLsmEngine();
}

}  // namespace cedar
