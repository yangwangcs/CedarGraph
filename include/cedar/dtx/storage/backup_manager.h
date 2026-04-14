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

#ifndef CEDAR_DTX_STORAGE_BACKUP_MANAGER_H_
#define CEDAR_DTX_STORAGE_BACKUP_MANAGER_H_

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {
namespace storage {

enum class BackupType : uint8_t {
  kFull = 0,
  kIncremental = 1,
  kDifferential = 2,
};

enum class BackupState : uint8_t {
  kPending = 0,
  kRunning = 1,
  kCompleted = 2,
  kFailed = 3,
};

struct BackupTask {
  uint64_t backup_id = 0;
  BackupType type;
  BackupState state = BackupState::kPending;
  PartitionID partition_id;
  std::string backup_path;
  uint64_t total_bytes = 0;
  uint64_t processed_bytes = 0;
  std::chrono::system_clock::time_point started_at;
  std::chrono::system_clock::time_point completed_at;
  std::string checksum;
};

class BackupManager {
 public:
  using ProgressCallback = std::function<void(uint64_t backup_id, 
                                               double progress)>;
  
  BackupManager();
  ~BackupManager();
  
  Status Initialize(const std::string& backup_root);
  
  // Create backup
  StatusOr<uint64_t> CreateBackup(PartitionID pid, BackupType type,
                                   ProgressCallback callback = nullptr);
  
  // Restore from backup
  Status RestoreFromBackup(uint64_t backup_id, PartitionID target_pid);
  
  // List backups
  std::vector<BackupTask> ListBackups(PartitionID pid) const;
  
  // Delete backup
  Status DeleteBackup(uint64_t backup_id);
  
  // Schedule automatic backups
  Status ScheduleBackup(PartitionID pid, BackupType type, 
                        std::chrono::hours interval);

 private:
  std::string backup_root_;
  std::atomic<uint64_t> next_backup_id_{1};
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_BACKUP_MANAGER_H_
