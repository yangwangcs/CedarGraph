// Copyright 2025 The Cedar Authors
//
// Cluster Backup and Recovery

#ifndef CEDAR_CLIENT_CLUSTER_BACKUP_H_
#define CEDAR_CLIENT_CLUSTER_BACKUP_H_

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace cedar {
namespace client {

// Backup type
enum class BackupType {
  FULL,           // Full backup
  INCREMENTAL,    // Incremental backup
  SNAPSHOT        // Snapshot backup
};

// Backup status
enum class BackupStatus {
  PENDING,
  IN_PROGRESS,
  COMPLETED,
  FAILED,
  RESTORING,
  RESTORED
};

// Backup configuration
struct BackupConfig {
  std::string backup_dir = "./backups";
  std::string s3_bucket;           // S3 bucket for remote backups
  std::string s3_prefix;           // S3 prefix
  int retention_days = 30;         // Keep backups for 30 days
  bool compress = true;            // Compress backups
  bool encrypt = false;            // Encrypt backups
  std::string encryption_key;      // Encryption key
};

// Backup information
struct BackupInfo {
  std::string backup_id;
  BackupType type;
  BackupStatus status;
  std::string component;           // metad, storaged, graphd, all
  std::string backup_path;
  int64_t start_time;
  int64_t end_time;
  int64_t size_bytes;
  std::string error_message;
  std::unordered_map<std::string, std::string> metadata;
};

// Restore options
struct RestoreOptions {
  std::string backup_id;
  std::string target_component;    // Restore to specific component
  bool force = false;              // Force restore even if component is running
  bool verify = true;              // Verify backup before restore
};

// Backup callback
using BackupCallback = std::function<void(const BackupInfo&)>;

// Cluster Backup Manager
class ClusterBackup {
 public:
  ClusterBackup();
  ~ClusterBackup();

  // Initialize backup manager
  bool Initialize(const BackupConfig& config, class ClusterManager* cluster_manager);

  // Backup operations
  BackupInfo CreateBackup(const std::string& component, BackupType type = BackupType::FULL);
  bool DeleteBackup(const std::string& backup_id);
  bool VerifyBackup(const std::string& backup_id);

  // Restore operations
  bool RestoreBackup(const RestoreOptions& options);
  bool RestoreLatest(const std::string& component);

  // Backup management
  std::vector<BackupInfo> ListBackups();
  std::vector<BackupInfo> ListBackupsByComponent(const std::string& component);
  BackupInfo GetBackupInfo(const std::string& backup_id);

  // Remote backup (S3)
  bool UploadToS3(const std::string& backup_id);
  bool DownloadFromS3(const std::string& backup_id);

  // Cleanup
  bool CleanupOldBackups();
  bool CleanupByRetention(int retention_days);

  // Callback
  void SetCallback(BackupCallback callback);

  // Statistics
  int GetTotalBackups();
  int64_t GetTotalBackupSize();

 private:
  BackupConfig config_;
  class ClusterManager* cluster_manager_;
  mutable std::mutex mutex_;
  std::vector<BackupInfo> backups_;
  BackupCallback callback_;

  // Generate backup ID
  std::string GenerateBackupId();

  // Create backup directory
  bool CreateBackupDirectory(const std::string& path);

  // Execute backup command
  std::string ExecuteBackupCommand(const std::string& command);

  // Compress backup
  bool CompressBackup(const std::string& backup_path);

  // Decompress backup
  bool DecompressBackup(const std::string& backup_path);

  // Encrypt backup
  bool EncryptBackup(const std::string& backup_path);

  // Decrypt backup
  bool DecryptBackup(const std::string& backup_path);

  // Verify backup integrity
  bool VerifyBackupIntegrity(const std::string& backup_path);

  // Get backup size
  int64_t GetBackupSize(const std::string& backup_path);

  // Update backup status
  void UpdateBackupStatus(const std::string& backup_id, BackupStatus status,
                          const std::string& error_message = "");

  // Notify callback
  void NotifyCallback(const BackupInfo& info);
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CLUSTER_BACKUP_H_
