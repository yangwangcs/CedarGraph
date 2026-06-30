// Copyright 2025 The Cedar Authors. All rights reserved.
// Backup and Restore Manager implementation

#include "cedar/backup/backup_manager.h"
#include "cedar/core/crc32c.h"

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

namespace cedar {
namespace {

namespace fs = std::filesystem;

Status FilesystemError(const std::string& action,
                       const fs::filesystem_error& e) {
  return Status::IOError(action, e.what());
}

Status FilesystemError(const std::string& action,
                       const std::exception& e) {
  return Status::IOError(action, e.what());
}

std::string ToHex(uint32_t value) {
  std::ostringstream ss;
  ss << std::hex << std::setw(8) << std::setfill('0') << value;
  return ss.str();
}

bool IsManifestFile(const fs::path& path) {
  return path.filename() == "backup.meta";
}

}  // namespace

BackupManager::BackupManager(const BackupConfig& config) 
    : config_(config), backup_dir_(config.backup_dir) {
  try {
    fs::create_directories(backup_dir_);
  } catch (const fs::filesystem_error&) {
    // Constructors cannot return Status; CreateFullBackup/ListBackups will
    // surface filesystem failures on first operational use.
  }
}

Status BackupManager::CreateFullBackup(const std::string& storage_path,
                                       BackupMetadata* metadata) {
  try {
    if (!fs::exists(storage_path)) {
      return Status::InvalidArgument("Storage path does not exist");
    }
    if (!fs::is_directory(storage_path)) {
      return Status::InvalidArgument("Storage path is not a directory");
    }

    std::string backup_id;
    std::string backup_path;
    Status s;
    bool allocated = false;
    for (int i = 0; i < 1024; ++i) {
      backup_id = GenerateBackupId();
      backup_path = (fs::path(backup_dir_) / backup_id).string();
      if (!fs::exists(backup_path)) {
        s = CreateBackupDirectory(backup_id);
        allocated = true;
        break;
      }
    }
    if (!allocated) {
      return Status::IOError("Failed to allocate a unique backup id");
    }
    if (!s.ok()) return s;

    if (config_.include_sst) {
      s = CopySSTFiles(storage_path, backup_path);
      if (!s.ok()) return s;
    }

    if (config_.include_wal) {
      s = CopyWALFiles(storage_path, backup_path);
      if (!s.ok()) return s;
    }

    if (config_.include_metadata) {
      s = CopyMetadata(storage_path, backup_path);
      if (!s.ok()) return s;
    }

    if (config_.compress) {
      s = CompressBackup(backup_path);
      if (!s.ok()) return s;
    }

    BackupMetadata current;
    current.backup_id = backup_id;
    current.timestamp = std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    current.total_size_bytes = 0;
    current.sst_count = 0;
    current.wal_count = 0;
    current.is_incremental = false;

    for (auto& entry : fs::recursive_directory_iterator(backup_path)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      if (IsManifestFile(entry.path())) {
        continue;
      }
      current.total_size_bytes += entry.file_size();
      if (entry.path().extension() == ".sst") {
        current.sst_count++;
      } else if (entry.path().extension() == ".wal") {
        current.wal_count++;
      }
    }

    s = CalculateChecksum(backup_path, &current.checksum);
    if (!s.ok()) return s;

    s = WriteManifest(backup_path, current);
    if (!s.ok()) return s;

    if (metadata) {
      *metadata = current;
    }

    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Create full backup failed", e);
  } catch (const std::exception& e) {
    return FilesystemError("Create full backup failed", e);
  }
}

Status BackupManager::CreateIncrementalBackup(const std::string& storage_path,
                                               const std::string& parent_backup_id,
                                               BackupMetadata* metadata) {
  // For now, just create a full backup
  return CreateFullBackup(storage_path, metadata);
}

Status BackupManager::RestoreFromBackup(const RestoreConfig& config,
                                         const std::string& target_path) {
  try {
    if (!fs::exists(config.backup_path)) {
      return Status::InvalidArgument("Backup path does not exist");
    }

    if (config.verify_checksums) {
      auto backup_id = fs::path(config.backup_path).filename().string();
      Status s = VerifyBackup(backup_id);
      if (!s.ok()) return s;
    }

    // Decompress if needed
    if (config_.compress) {
      Status s = DecompressBackup(config.backup_path);
      if (!s.ok()) return s;
    }

    // Copy files to target
    // This is a simplified implementation
    // In production, you would restore files individually

    return Status::NotSupported("BackupManager", "Restore not yet implemented");
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Restore backup failed", e);
  } catch (const std::exception& e) {
    return FilesystemError("Restore backup failed", e);
  }
}

std::vector<BackupMetadata> BackupManager::ListBackups() const {
  std::vector<BackupMetadata> backups;

  try {
    if (!fs::exists(backup_dir_)) {
      return backups;
    }

    for (const auto& entry : fs::directory_iterator(backup_dir_)) {
      if (entry.is_directory()) {
        BackupMetadata metadata;
        if (!ReadManifest(entry.path().string(), &metadata).ok()) {
          metadata.backup_id = entry.path().filename().string();
          metadata.timestamp = "";
          uint64_t total_size = 0;
          uint64_t sst_count = 0;
          uint64_t wal_count = 0;
          for (auto& file_entry : fs::recursive_directory_iterator(entry.path())) {
            if (file_entry.is_regular_file()) {
              if (IsManifestFile(file_entry.path())) {
                continue;
              }
              total_size += file_entry.file_size();
              if (file_entry.path().extension() == ".sst") {
                sst_count++;
              } else if (file_entry.path().extension() == ".wal") {
                wal_count++;
              }
            }
          }
          metadata.total_size_bytes = total_size;
          metadata.sst_count = sst_count;
          metadata.wal_count = wal_count;
        }
        backups.push_back(metadata);
      }
    }
  } catch (const fs::filesystem_error&) {
    return backups;
  }
  
  return backups;
}

Status BackupManager::DeleteBackup(const std::string& backup_id) {
  if (!IsSafeBackupId(backup_id)) {
    return Status::InvalidArgument("Invalid backup id");
  }

  try {
    fs::path backup_path = fs::path(backup_dir_) / backup_id;
    if (!fs::exists(backup_path)) {
      return Status::NotFound("Backup not found");
    }

    fs::remove_all(backup_path);
    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Delete backup failed", e);
  }
}

Status BackupManager::VerifyBackup(const std::string& backup_id) const {
  if (!IsSafeBackupId(backup_id)) {
    return Status::InvalidArgument("Invalid backup id");
  }

  try {
    fs::path backup_path = fs::path(backup_dir_) / backup_id;
    if (!fs::exists(backup_path)) {
      return Status::NotFound("Backup not found");
    }

    BackupMetadata metadata;
    Status s = ReadManifest(backup_path.string(), &metadata);
    if (!s.ok()) return s;

    std::string checksum;
    s = CalculateChecksum(backup_path.string(), &checksum);
    if (!s.ok()) return s;

    if (checksum != metadata.checksum) {
      return Status::Corruption("Backup checksum mismatch");
    }

    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Verify backup failed", e);
  }
}

Status BackupManager::CreateBackupDirectory(const std::string& backup_id) {
  if (!IsSafeBackupId(backup_id)) {
    return Status::InvalidArgument("Invalid backup id");
  }

  try {
    fs::path backup_path = fs::path(backup_dir_) / backup_id;
    fs::create_directories(backup_path);
    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Create backup directory failed", e);
  }
}

Status BackupManager::CopySSTFiles(const std::string& source_dir, 
                                    const std::string& backup_dir) {
  try {
    fs::path sst_dir = fs::path(backup_dir) / "sst";
    fs::create_directories(sst_dir);

    for (const auto& entry : fs::directory_iterator(source_dir)) {
      if (entry.is_regular_file() && entry.path().extension() == ".sst") {
        fs::path dest = sst_dir / entry.path().filename();
        fs::copy_file(entry.path(), dest, fs::copy_options::overwrite_existing);
      }
    }

    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Copy SST files failed", e);
  }
}

Status BackupManager::CopyWALFiles(const std::string& source_dir,
                                    const std::string& backup_dir) {
  try {
    fs::path wal_dir = fs::path(backup_dir) / "wal";
    fs::create_directories(wal_dir);

    fs::path wal_source = fs::path(source_dir) / "wal";
    if (fs::exists(wal_source)) {
      for (const auto& entry : fs::directory_iterator(wal_source)) {
        if (entry.is_regular_file() && entry.path().extension() == ".wal") {
          fs::path dest = wal_dir / entry.path().filename();
          fs::copy_file(entry.path(), dest, fs::copy_options::overwrite_existing);
        }
      }
    }

    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Copy WAL files failed", e);
  }
}

Status BackupManager::CopyMetadata(const std::string& source_dir,
                                    const std::string& backup_dir) {
  try {
    // Copy property_names.meta
    fs::path meta_file = fs::path(source_dir) / "property_names.meta";
    if (fs::exists(meta_file) && fs::is_regular_file(meta_file)) {
      fs::copy_file(meta_file, fs::path(backup_dir) / "property_names.meta",
                    fs::copy_options::overwrite_existing);
    }

    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Copy metadata failed", e);
  }
}

Status BackupManager::CompressBackup(const std::string& backup_dir) {
  return Status::NotSupported("BackupManager", "Compression not yet implemented");
}

Status BackupManager::DecompressBackup(const std::string& backup_dir) {
  return Status::NotSupported("BackupManager", "Decompression not yet implemented");
}

Status BackupManager::CalculateChecksum(const std::string& backup_dir,
                                        std::string* checksum) const {
  if (checksum == nullptr) {
    return Status::InvalidArgument("Checksum output is null");
  }

  try {
    std::vector<fs::path> files;
    for (const auto& entry : fs::recursive_directory_iterator(backup_dir)) {
      if (entry.is_regular_file() && !IsManifestFile(entry.path())) {
        files.push_back(entry.path());
      }
    }
    std::sort(files.begin(), files.end());

    uint32_t crc = 0;
    std::vector<char> buffer(64 * 1024);
    fs::path root = fs::path(backup_dir);

    for (const auto& path : files) {
      std::string relative = fs::relative(path, root).generic_string();
      crc = crc32c::Extend(crc, relative.data(), relative.size());
      const char separator = '\0';
      crc = crc32c::Extend(crc, &separator, 1);

      std::ifstream file(path, std::ios::binary);
      if (!file.is_open()) {
        return Status::IOError("Failed to open backup file", path.string());
      }

      while (file.good()) {
        file.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        std::streamsize bytes = file.gcount();
        if (bytes > 0) {
          crc = crc32c::Extend(crc, buffer.data(), static_cast<size_t>(bytes));
        }
      }

      if (file.bad()) {
        return Status::IOError("Failed to read backup file", path.string());
      }
    }

    *checksum = ToHex(crc);
    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Calculate backup checksum failed", e);
  }
}

Status BackupManager::WriteManifest(const std::string& backup_dir,
                                    const BackupMetadata& metadata) const {
  try {
    fs::path manifest = fs::path(backup_dir) / "backup.meta";
    std::ofstream out(manifest, std::ios::trunc);
    if (!out.is_open()) {
      return Status::IOError("Failed to create backup manifest", manifest.string());
    }

    out << "backup_id=" << metadata.backup_id << "\n"
        << "timestamp=" << metadata.timestamp << "\n"
        << "total_size_bytes=" << metadata.total_size_bytes << "\n"
        << "sst_count=" << metadata.sst_count << "\n"
        << "wal_count=" << metadata.wal_count << "\n"
        << "checksum=" << metadata.checksum << "\n"
        << "is_incremental=" << (metadata.is_incremental ? "1" : "0") << "\n"
        << "parent_backup_id=" << metadata.parent_backup_id << "\n";

    if (!out.good()) {
      return Status::IOError("Failed to write backup manifest", manifest.string());
    }
    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Write backup manifest failed", e);
  }
}

Status BackupManager::ReadManifest(const std::string& backup_dir,
                                   BackupMetadata* metadata) const {
  if (metadata == nullptr) {
    return Status::InvalidArgument("Metadata output is null");
  }

  try {
    fs::path manifest = fs::path(backup_dir) / "backup.meta";
    std::ifstream in(manifest);
    if (!in.is_open()) {
      return Status::NotFound("Backup manifest not found");
    }

    BackupMetadata parsed;
    std::string line;
    while (std::getline(in, line)) {
      auto pos = line.find('=');
      if (pos == std::string::npos) {
        continue;
      }
      std::string key = line.substr(0, pos);
      std::string value = line.substr(pos + 1);

      try {
        if (key == "backup_id") {
          parsed.backup_id = value;
        } else if (key == "timestamp") {
          parsed.timestamp = value;
        } else if (key == "total_size_bytes") {
          parsed.total_size_bytes = std::stoull(value);
        } else if (key == "sst_count") {
          parsed.sst_count = std::stoull(value);
        } else if (key == "wal_count") {
          parsed.wal_count = std::stoull(value);
        } else if (key == "checksum") {
          parsed.checksum = value;
        } else if (key == "is_incremental") {
          parsed.is_incremental = (value == "1" || value == "true");
        } else if (key == "parent_backup_id") {
          parsed.parent_backup_id = value;
        }
      } catch (const std::exception&) {
        return Status::Corruption("Invalid backup manifest field", key);
      }
    }

    if (parsed.backup_id.empty() || parsed.checksum.empty()) {
      return Status::Corruption("Backup manifest missing required fields");
    }

    *metadata = parsed;
    return Status::OK();
  } catch (const fs::filesystem_error& e) {
    return FilesystemError("Read backup manifest failed", e);
  }
}

bool BackupManager::IsSafeBackupId(const std::string& backup_id) const {
  if (backup_id.empty()) {
    return false;
  }
  if (backup_id == "." || backup_id == "..") {
    return false;
  }
  return backup_id.find('/') == std::string::npos &&
         backup_id.find('\\') == std::string::npos;
}

std::string BackupManager::GenerateBackupId() const {
  static std::atomic<uint64_t> sequence{0};
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      now.time_since_epoch()).count() % 1000000;
  struct tm tm_buf;
  localtime_r(&time, &tm_buf);
  std::stringstream ss;
  ss << "backup_" << std::put_time(&tm_buf, "%Y%m%d_%H%M%S")
     << "_" << std::setw(6) << std::setfill('0') << micros
     << "_" << sequence.fetch_add(1, std::memory_order_relaxed);
  return ss.str();
}

}  // namespace cedar
