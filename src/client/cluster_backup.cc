// Copyright 2025 The Cedar Authors
//
// Cluster Backup and Recovery implementation

#include "cedar/client/cluster_backup.h"
#include "cedar/client/cluster_manager.h"

#include <chrono>
#include <cctype>
#include <filesystem>
#include <iostream>
#include <random>
#include <sstream>
#include <system_error>
#include <sys/wait.h>

namespace cedar {
namespace client {

ClusterBackup::ClusterBackup() = default;

ClusterBackup::~ClusterBackup() = default;

bool ClusterBackup::Initialize(const BackupConfig& config, ClusterManager* cluster_manager) {
  initialized_ = false;
  if (config.encrypt) {
    return false;
  }

  config_ = config;
  cluster_manager_ = cluster_manager;

  // Create backup directory
  initialized_ = CreateBackupDirectory(config_.backup_dir);
  return initialized_;
}

BackupInfo ClusterBackup::CreateBackup(const std::string& component, BackupType type) {
  BackupInfo info;
  info.backup_id = GenerateBackupId();
  info.type = type;
  info.status = BackupStatus::IN_PROGRESS;
  info.component = component;
  info.start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();

  if (!initialized_) {
    info.status = BackupStatus::FAILED;
    info.error_message = "ClusterBackup is not initialized";
    return info;
  }

  // Create backup path
  info.backup_path = config_.backup_dir + "/" + info.backup_id;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    backups_.push_back(info);
  }

  NotifyCallback(info);

  auto fail_backup = [this, &info](const std::string& error_message) {
    info = UpdateBackupStatus(info.backup_id, BackupStatus::FAILED, error_message);
    NotifyCallback(info);
    return info;
  };

  if (component != "metad" && component != "storaged" && component != "all" &&
      component != "graphd") {
    return fail_backup("Unknown backup component: " + component);
  }

  return fail_backup(
      "ClusterBackup client backup is not implemented in this build; use "
      "scripts/deploy.sh backup or the storage BackupManager API instead");
}

bool ClusterBackup::DeleteBackup(const std::string& backup_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  for (auto it = backups_.begin(); it != backups_.end(); ++it) {
    if (it->backup_id == backup_id) {
      if (!IsPathWithinBackupDir(it->backup_path)) {
        it->status = BackupStatus::FAILED;
        it->error_message = "Backup path is outside configured backup directory";
        return false;
      }

      std::error_code ec;
      std::filesystem::remove_all(it->backup_path, ec);
      if (ec) {
        it->status = BackupStatus::FAILED;
        it->error_message = ec.message();
        return false;
      }

      backups_.erase(it);
      return true;
    }
  }

  return false;
}

bool ClusterBackup::VerifyBackup(const std::string& backup_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& backup : backups_) {
    if (backup.backup_id == backup_id) {
      return VerifyBackupIntegrity(backup.backup_path);
    }
  }

  return false;
}

bool ClusterBackup::RestoreBackup(const RestoreOptions& options) {
  if (!initialized_) {
    return false;
  }

  BackupInfo backup;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find_if(backups_.begin(), backups_.end(),
                           [&options](const BackupInfo& b) {
                             return b.backup_id == options.backup_id;
                           });
    if (it == backups_.end()) {
      return false;
    }
    backup = *it;
  }

  auto fail_restore = [this, &backup](const std::string& error_message) {
    backup = UpdateBackupStatus(backup.backup_id, BackupStatus::FAILED,
                                error_message);
    NotifyCallback(backup);
    return false;
  };

  if (!IsPathWithinBackupDir(backup.backup_path)) {
    return fail_restore("Backup path is outside configured backup directory");
  }

  // Verify backup if requested
  if (options.verify) {
    if (!VerifyBackupIntegrity(backup.backup_path)) {
      return fail_restore("Backup integrity verification failed");
    }
  }

  // Update status
  backup = UpdateBackupStatus(backup.backup_id, BackupStatus::RESTORING);
  NotifyCallback(backup);

  // Decompress if needed
  if (config_.compress) {
    if (!DecompressBackup(backup.backup_path)) {
      return fail_restore("Backup decompression failed");
    }
  }

  // Decrypt if needed
  if (config_.encrypt) {
    if (!DecryptBackup(backup.backup_path)) {
      return fail_restore("Backup decryption failed");
    }
  }

  std::string component = options.target_component.empty() ? backup.component : options.target_component;

  if (component != "metad" && component != "storaged" && component != "all" &&
      component != "graphd") {
    return fail_restore("Unknown restore component: " + component);
  }

  return fail_restore(
      "ClusterBackup client restore is not implemented in this build; use "
      "scripts/deploy.sh restore or the storage BackupManager API instead");
}

bool ClusterBackup::RestoreLatest(const std::string& component) {
  auto backups = ListBackupsByComponent(component);

  if (backups.empty()) {
    return false;
  }

  // Find latest completed backup
  BackupInfo* latest = nullptr;
  for (auto& backup : backups) {
    if (backup.status == BackupStatus::COMPLETED) {
      if (!latest || backup.start_time > latest->start_time) {
        latest = &backup;
      }
    }
  }

  if (!latest) {
    return false;
  }

  RestoreOptions options;
  options.backup_id = latest->backup_id;
  options.target_component = component;

  return RestoreBackup(options);
}

std::vector<BackupInfo> ClusterBackup::ListBackups() {
  std::lock_guard<std::mutex> lock(mutex_);
  return backups_;
}

std::vector<BackupInfo> ClusterBackup::ListBackupsByComponent(const std::string& component) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<BackupInfo> result;
  for (const auto& backup : backups_) {
    if (backup.component == component) {
      result.push_back(backup);
    }
  }

  return result;
}

BackupInfo ClusterBackup::GetBackupInfo(const std::string& backup_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& backup : backups_) {
    if (backup.backup_id == backup_id) {
      return backup;
    }
  }

  return {};
}

bool ClusterBackup::UploadToS3(const std::string& backup_id) {
  if (!initialized_) {
    return false;
  }

  BackupInfo backup;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find_if(backups_.begin(), backups_.end(),
                           [&backup_id](const BackupInfo& b) {
                             return b.backup_id == backup_id;
                           });
    if (it == backups_.end()) {
      return false;
    }
    backup = *it;
  }

  if (!IsPathWithinBackupDir(backup.backup_path) ||
      backup.status != BackupStatus::COMPLETED ||
      !VerifyBackupIntegrity(backup.backup_path) ||
      !IsSafeS3Part(config_.s3_bucket, false) ||
      !IsSafeS3Part(config_.s3_prefix, true) ||
      !IsSafeS3Part(backup_id, false)) {
    return false;
  }

  // Upload to S3
  std::string s3_uri = "s3://" + config_.s3_bucket + "/" +
                       config_.s3_prefix + "/" + backup_id;
  std::string command = "aws s3 cp " + ShellQuote(backup.backup_path) + " " +
                        ShellQuote(s3_uri);
  std::string output = ExecuteBackupCommand(command);

  return output.find("Error") == std::string::npos;
}

bool ClusterBackup::DownloadFromS3(const std::string& backup_id) {
  if (!initialized_) {
    return false;
  }

  if (!IsSafeS3Part(config_.s3_bucket, false) ||
      !IsSafeS3Part(config_.s3_prefix, true) ||
      !IsSafeS3Part(backup_id, false)) {
    return false;
  }

  // Remote backup metadata registration is not implemented. Downloading a
  // payload without a verified manifest would create an unusable local backup.
  return false;
}

bool ClusterBackup::CleanupOldBackups() {
  if (!initialized_) {
    return false;
  }
  return CleanupByRetention(config_.retention_days);
}

bool ClusterBackup::CleanupByRetention(int retention_days) {
  if (!initialized_) {
    return false;
  }
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  auto cutoff = now - (static_cast<int64_t>(retention_days) * 24 * 60 * 60 * 1000);

  std::vector<std::string> to_delete;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& backup : backups_) {
      if (backup.start_time < cutoff) {
        to_delete.push_back(backup.backup_id);
      }
    }
  }

  bool ok = true;
  for (const auto& id : to_delete) {
    ok = DeleteBackup(id) && ok;
  }

  return ok;
}

void ClusterBackup::SetCallback(BackupCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  callback_ = callback;
}

int ClusterBackup::GetTotalBackups() {
  std::lock_guard<std::mutex> lock(mutex_);
  return backups_.size();
}

int64_t ClusterBackup::GetTotalBackupSize() {
  std::lock_guard<std::mutex> lock(mutex_);

  int64_t total = 0;
  for (const auto& backup : backups_) {
    total += backup.size_bytes;
  }

  return total;
}

// ============================================================================
// Private methods
// ============================================================================

std::string ClusterBackup::GenerateBackupId() {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  
  char buffer[100];
  std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", std::localtime(&time));
  
  // Add random suffix
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 9999);
  
  std::stringstream ss;
  ss << buffer << "_" << std::setfill('0') << std::setw(4) << dis(gen);
  
  return ss.str();
}

bool ClusterBackup::CreateBackupDirectory(const std::string& path) {
  try {
    std::filesystem::create_directories(path);
    return true;
  } catch (const std::exception& e) {
    return false;
  }
}

std::string ClusterBackup::ExecuteBackupCommand(const std::string& command) {
  std::array<char, 128> buffer;
  std::string result;

  std::string redirected_command = command + " 2>&1";
  FILE* raw_pipe = popen(redirected_command.c_str(), "r");

  if (!raw_pipe) {
    return "Error: Failed to execute command";
  }

  while (fgets(buffer.data(), buffer.size(), raw_pipe) != nullptr) {
    result += buffer.data();
  }

  int exit_status = pclose(raw_pipe);
  if (exit_status == -1) {
    result += "Error: Failed to close command pipe";
  } else if (WIFEXITED(exit_status) && WEXITSTATUS(exit_status) != 0) {
    result += "Error: command exited with status " +
              std::to_string(WEXITSTATUS(exit_status));
  } else if (WIFSIGNALED(exit_status)) {
    result += "Error: command terminated by signal " +
              std::to_string(WTERMSIG(exit_status));
  }

  return result;
}

bool ClusterBackup::CompressBackup(const std::string& backup_path) {
  if (!IsPathWithinBackupDir(backup_path)) {
    return false;
  }
  auto member = std::filesystem::path(backup_path).filename().string();
  std::string command = "tar -czf " + ShellQuote(backup_path + ".tar.gz") +
                        " -C " + ShellQuote(config_.backup_dir) + " " +
                        ShellQuote(member);
  std::string output = ExecuteBackupCommand(command);
  return output.find("Error") == std::string::npos;
}

bool ClusterBackup::DecompressBackup(const std::string& backup_path) {
  if (!IsPathWithinBackupDir(backup_path)) {
    return false;
  }
  std::string command = "tar -xzf " + ShellQuote(backup_path + ".tar.gz") +
                        " -C " + ShellQuote(config_.backup_dir);
  std::string output = ExecuteBackupCommand(command);
  return output.find("Error") == std::string::npos;
}

bool ClusterBackup::EncryptBackup(const std::string& backup_path) {
  return false;
}

bool ClusterBackup::DecryptBackup(const std::string& backup_path) {
  return false;
}

bool ClusterBackup::VerifyBackupIntegrity(const std::string& backup_path) {
  std::error_code ec;
  return IsPathWithinBackupDir(backup_path) &&
         std::filesystem::exists(backup_path, ec) &&
         std::filesystem::is_directory(backup_path, ec);
}

int64_t ClusterBackup::GetBackupSize(const std::string& backup_path) {
  if (!IsPathWithinBackupDir(backup_path)) {
    return 0;
  }

  std::error_code ec;
  int64_t total = 0;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(backup_path, ec)) {
    if (ec) {
      return 0;
    }
    if (entry.is_regular_file(ec)) {
      total += static_cast<int64_t>(entry.file_size(ec));
      if (ec) {
        return 0;
      }
    }
  }

  return total;
}

bool ClusterBackup::IsPathWithinBackupDir(const std::string& backup_path) const {
  std::error_code ec;
  auto root = std::filesystem::weakly_canonical(config_.backup_dir, ec);
  if (ec) {
    return false;
  }

  auto path = std::filesystem::weakly_canonical(backup_path, ec);
  if (ec) {
    path = std::filesystem::weakly_canonical(
        std::filesystem::path(backup_path).parent_path(), ec);
    if (ec) {
      return false;
    }
  }

  auto root_it = root.begin();
  auto path_it = path.begin();
  for (; root_it != root.end(); ++root_it, ++path_it) {
    if (path_it == path.end() || *root_it != *path_it) {
      return false;
    }
  }
  return true;
}

bool ClusterBackup::IsSafeS3Part(const std::string& value, bool allow_slash) const {
  if (value.empty()) {
    return allow_slash;
  }
  for (unsigned char c : value) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' ||
        (allow_slash && c == '/')) {
      continue;
    }
    return false;
  }
  return value.find("..") == std::string::npos;
}

std::string ClusterBackup::ShellQuote(const std::string& arg) const {
  std::string quoted = "'";
  for (char c : arg) {
    if (c == '\'') {
      quoted += "'\\''";
    } else {
      quoted += c;
    }
  }
  quoted += "'";
  return quoted;
}

BackupInfo ClusterBackup::UpdateBackupStatus(const std::string& backup_id, BackupStatus status,
                                             const std::string& error_message) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& backup : backups_) {
    if (backup.backup_id == backup_id) {
      backup.status = status;
      backup.error_message = error_message;
      return backup;
    }
  }
  return {};
}

void ClusterBackup::NotifyCallback(const BackupInfo& info) {
  BackupCallback callback;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    callback = callback_;
  }
  if (callback) {
    callback(info);
  }
}

}  // namespace client
}  // namespace cedar
