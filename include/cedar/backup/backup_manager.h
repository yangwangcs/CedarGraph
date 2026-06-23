// Copyright 2025 The Cedar Authors. All rights reserved.
// Backup and Restore Manager for CedarGraph

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <functional>

#include "cedar/core/status.h"

namespace cedar {

// Backup configuration
struct BackupConfig {
  std::string backup_dir = "/tmp/cedar_backups";
  bool compress = false;
  bool include_wal = true;
  bool include_sst = true;
  bool include_metadata = true;
  uint64_t max_backup_size_bytes = 0;  // 0 = unlimited
};

// Restore configuration
struct RestoreConfig {
  std::string backup_path;
  bool restore_wal = true;
  bool restore_sst = true;
  bool restore_metadata = true;
  bool verify_checksums = true;
};

// Backup metadata
struct BackupMetadata {
  std::string backup_id;
  std::string timestamp;
  uint64_t total_size_bytes;
  uint64_t sst_count;
  uint64_t wal_count;
  std::string checksum;
  bool is_incremental;
  std::string parent_backup_id;
};

// Backup and Restore Manager
class BackupManager {
 public:
  explicit BackupManager(const BackupConfig& config = BackupConfig());
  ~BackupManager() = default;

  // Create a full backup
  Status CreateFullBackup(const std::string& storage_path,
                          BackupMetadata* metadata);

  // Create an incremental backup
  Status CreateIncrementalBackup(const std::string& storage_path,
                                 const std::string& parent_backup_id,
                                 BackupMetadata* metadata);

  // Restore from backup
  Status RestoreFromBackup(const RestoreConfig& config,
                           const std::string& target_path);

  // List available backups
  std::vector<BackupMetadata> ListBackups() const;

  // Delete a backup
  Status DeleteBackup(const std::string& backup_id);

  // Verify backup integrity
  Status VerifyBackup(const std::string& backup_id) const;

 private:
  BackupConfig config_;
  std::string backup_dir_;
  
  // Helper methods
  Status CreateBackupDirectory(const std::string& backup_id);
  Status CopySSTFiles(const std::string& source_dir, const std::string& backup_dir);
  Status CopyWALFiles(const std::string& source_dir, const std::string& backup_dir);
  Status CopyMetadata(const std::string& source_dir, const std::string& backup_dir);
  Status CompressBackup(const std::string& backup_dir);
  Status DecompressBackup(const std::string& backup_dir);
  std::string CalculateChecksum(const std::string& backup_dir) const;
  std::string GenerateBackupId() const;
};

}  // namespace cedar
