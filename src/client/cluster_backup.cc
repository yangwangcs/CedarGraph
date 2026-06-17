// Copyright 2025 The Cedar Authors
//
// Cluster Backup and Recovery implementation

#include "cedar/client/cluster_backup.h"
#include "cedar/client/cluster_manager.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <sstream>

namespace cedar {
namespace client {

ClusterBackup::ClusterBackup() = default;

ClusterBackup::~ClusterBackup() = default;

bool ClusterBackup::Initialize(const BackupConfig& config, ClusterManager* cluster_manager) {
  config_ = config;
  cluster_manager_ = cluster_manager;

  // Create backup directory
  return CreateBackupDirectory(config_.backup_dir);
}

BackupInfo ClusterBackup::CreateBackup(const std::string& component, BackupType type) {
  std::lock_guard<std::mutex> lock(mutex_);

  BackupInfo info;
  info.backup_id = GenerateBackupId();
  info.type = type;
  info.status = BackupStatus::IN_PROGRESS;
  info.component = component;
  info.start_time = std::chrono::system_clock::now().time_since_epoch().count();

  // Create backup path
  info.backup_path = config_.backup_dir + "/" + info.backup_id;

  // Add to backups list
  backups_.push_back(info);

  // Notify callback
  NotifyCallback(info);

  // Execute backup based on component
  std::string command;
  if (component == "metad") {
    command = "docker exec cedar-metad-0 cedar-backup --type=" + 
              std::to_string(static_cast<int>(type)) + 
              " --output=" + info.backup_path;
  } else if (component == "storaged") {
    command = "docker exec cedar-storaged-0 cedar-backup --type=" + 
              std::to_string(static_cast<int>(type)) + 
              " --output=" + info.backup_path;
  } else if (component == "all") {
    command = "docker-compose exec metad cedar-backup --type=" + 
              std::to_string(static_cast<int>(type)) + 
              " --output=" + info.backup_path;
  }

  // Execute backup command
  std::string output = ExecuteBackupCommand(command);

  // Update status
  if (output.find("Error") != std::string::npos) {
    UpdateBackupStatus(info.backup_id, BackupStatus::FAILED, output);
  } else {
    UpdateBackupStatus(info.backup_id, BackupStatus::COMPLETED);

    // Compress if configured
    if (config_.compress) {
      CompressBackup(info.backup_path);
    }

    // Encrypt if configured
    if (config_.encrypt) {
      EncryptBackup(info.backup_path);
    }

    // Update size
    auto it = std::find_if(backups_.begin(), backups_.end(),
                           [&info](const BackupInfo& b) { return b.backup_id == info.backup_id; });
    if (it != backups_.end()) {
      it->size_bytes = GetBackupSize(it->backup_path);
      it->end_time = std::chrono::system_clock::now().time_since_epoch().count();
      info = *it;
    }
  }

  return info;
}

bool ClusterBackup::DeleteBackup(const std::string& backup_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  for (auto it = backups_.begin(); it != backups_.end(); ++it) {
    if (it->backup_id == backup_id) {
      // Delete backup files
      std::string command = "rm -rf " + it->backup_path;
      ExecuteBackupCommand(command);

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
  std::lock_guard<std::mutex> lock(mutex_);

  // Find backup
  BackupInfo* backup = nullptr;
  for (auto& b : backups_) {
    if (b.backup_id == options.backup_id) {
      backup = &b;
      break;
    }
  }

  if (!backup) {
    return false;
  }

  // Verify backup if requested
  if (options.verify) {
    if (!VerifyBackupIntegrity(backup->backup_path)) {
      return false;
    }
  }

  // Update status
  backup->status = BackupStatus::RESTORING;
  NotifyCallback(*backup);

  // Decompress if needed
  if (config_.compress) {
    DecompressBackup(backup->backup_path);
  }

  // Decrypt if needed
  if (config_.encrypt) {
    DecryptBackup(backup->backup_path);
  }

  // Execute restore based on component
  std::string component = options.target_component.empty() ? backup->component : options.target_component;
  
  std::string command;
  if (component == "metad") {
    command = "docker exec cedar-metad-0 cedar-restore --input=" + backup->backup_path;
  } else if (component == "storaged") {
    command = "docker exec cedar-storaged-0 cedar-restore --input=" + backup->backup_path;
  } else if (component == "all") {
    command = "docker-compose exec metad cedar-restore --input=" + backup->backup_path;
  }

  // Execute restore command
  std::string output = ExecuteBackupCommand(command);

  // Update status
  if (output.find("Error") != std::string::npos) {
    backup->status = BackupStatus::FAILED;
    backup->error_message = output;
    NotifyCallback(*backup);
    return false;
  }

  backup->status = BackupStatus::RESTORED;
  NotifyCallback(*backup);

  return true;
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
  std::lock_guard<std::mutex> lock(mutex_);

  // Find backup
  BackupInfo* backup = nullptr;
  for (auto& b : backups_) {
    if (b.backup_id == backup_id) {
      backup = &b;
      break;
    }
  }

  if (!backup) {
    return false;
  }

  // Upload to S3
  std::string command = "aws s3 cp " + backup->backup_path + " s3://" + 
                        config_.s3_bucket + "/" + config_.s3_prefix + "/" + backup_id;
  std::string output = ExecuteBackupCommand(command);

  return output.find("Error") == std::string::npos;
}

bool ClusterBackup::DownloadFromS3(const std::string& backup_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Download from S3
  std::string command = "aws s3 cp s3://" + config_.s3_bucket + "/" + 
                        config_.s3_prefix + "/" + backup_id + " " + 
                        config_.backup_dir + "/" + backup_id;
  std::string output = ExecuteBackupCommand(command);

  return output.find("Error") == std::string::npos;
}

bool ClusterBackup::CleanupOldBackups() {
  return CleanupByRetention(config_.retention_days);
}

bool ClusterBackup::CleanupByRetention(int retention_days) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto now = std::chrono::system_clock::now().time_since_epoch().count();
  auto cutoff = now - (retention_days * 24 * 60 * 60 * 1000000000LL);

  std::vector<std::string> to_delete;

  for (const auto& backup : backups_) {
    if (backup.start_time < cutoff) {
      to_delete.push_back(backup.backup_id);
    }
  }

  for (const auto& id : to_delete) {
    DeleteBackup(id);
  }

  return true;
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

  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(command.c_str(), "r"), pclose);

  if (!pipe) {
    return "Error: Failed to execute command";
  }

  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }

  return result;
}

bool ClusterBackup::CompressBackup(const std::string& backup_path) {
  std::string command = "tar -czf " + backup_path + ".tar.gz -C " + 
                        config_.backup_dir + " " + backup_path;
  std::string output = ExecuteBackupCommand(command);
  return output.find("Error") == std::string::npos;
}

bool ClusterBackup::DecompressBackup(const std::string& backup_path) {
  std::string command = "tar -xzf " + backup_path + ".tar.gz -C " + config_.backup_dir;
  std::string output = ExecuteBackupCommand(command);
  return output.find("Error") == std::string::npos;
}

bool ClusterBackup::EncryptBackup(const std::string& backup_path) {
  // TODO: Implement encryption
  return true;
}

bool ClusterBackup::DecryptBackup(const std::string& backup_path) {
  // TODO: Implement decryption
  return true;
}

bool ClusterBackup::VerifyBackupIntegrity(const std::string& backup_path) {
  // Check if backup exists
  std::string command = "test -d " + backup_path + " && echo 'OK' || echo 'FAIL'";
  std::string output = ExecuteBackupCommand(command);
  return output.find("OK") != std::string::npos;
}

int64_t ClusterBackup::GetBackupSize(const std::string& backup_path) {
  std::string command = "du -sb " + backup_path + " | cut -f1";
  std::string output = ExecuteBackupCommand(command);
  
  try {
    return std::stoll(output);
  } catch (...) {
    return 0;
  }
}

void ClusterBackup::UpdateBackupStatus(const std::string& backup_id, BackupStatus status,
                                         const std::string& error_message) {
  for (auto& backup : backups_) {
    if (backup.backup_id == backup_id) {
      backup.status = status;
      backup.error_message = error_message;
      NotifyCallback(backup);
      break;
    }
  }
}

void ClusterBackup::NotifyCallback(const BackupInfo& info) {
  if (callback_) {
    callback_(info);
  }
}

}  // namespace client
}  // namespace cedar
