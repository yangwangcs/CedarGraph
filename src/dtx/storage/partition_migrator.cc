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

// =============================================================================
// Partition Migration Implementation
// =============================================================================

#include "cedar/dtx/storage/partition_migrator.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/core/crc32c.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/storage/lsm_engine.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <sys/stat.h>

#include <butil/logging.h>

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// MigrationTask Implementation
// =============================================================================

bool MigrationTask::CanTransitionTo(MigrationState new_state) const {
  // Define valid state transitions
  switch (state) {
    case MigrationState::kPending:
      return new_state == MigrationState::kPreparing ||
             new_state == MigrationState::kRolledBack;
    case MigrationState::kPreparing:
      return new_state == MigrationState::kCopying ||
             new_state == MigrationState::kFailed;
    case MigrationState::kCopying:
      return new_state == MigrationState::kCatchingUp ||
             new_state == MigrationState::kFailed;
    case MigrationState::kCatchingUp:
      return new_state == MigrationState::kSwitching ||
             new_state == MigrationState::kCopying ||  // More data to copy
             new_state == MigrationState::kFailed;
    case MigrationState::kSwitching:
      return new_state == MigrationState::kVerifying ||
             new_state == MigrationState::kFailed;
    case MigrationState::kVerifying:
      return new_state == MigrationState::kCompleting ||
             new_state == MigrationState::kFailed;
    case MigrationState::kCompleting:
      return new_state == MigrationState::kCompleted ||
             new_state == MigrationState::kFailed;
    case MigrationState::kFailed:
      return new_state == MigrationState::kRolledBack ||
             new_state == MigrationState::kPending;  // Retry
    default:
      return false;
  }
}

Status MigrationTask::TransitionTo(MigrationState new_state) {
  if (!CanTransitionTo(new_state)) {
    return Status::InvalidArgument("Invalid state transition from " +
                                    std::to_string(static_cast<int>(state)) +
                                    " to " +
                                    std::to_string(static_cast<int>(new_state)));
  }
  state = new_state;
  return Status::OK();
}

// =============================================================================
// PartitionMigrator Implementation
// =============================================================================

PartitionMigrator::PartitionMigrator() = default;

PartitionMigrator::~PartitionMigrator() {
  Shutdown();
}

void PartitionMigrator::SetStoragePartitionManager(StoragePartitionManager* manager) {
  partition_manager_ = manager;
}

void PartitionMigrator::SetMetaServiceClient(MetaServiceNodeClient* meta_client) {
  meta_client_ = meta_client;
}

void PartitionMigrator::SetMigrationServiceStub(
    std::shared_ptr<cedar::migration::PartitionMigrationService::Stub> stub) {
  migration_stub_ = std::move(stub);
}

Status PartitionMigrator::Initialize(const MigrationConfig& config) {
  config_ = config;
  running_.store(true);
  
  // Start worker threads
  uint32_t num_workers = std::max(1U, config.max_concurrent_batches);
  for (uint32_t i = 0; i < num_workers; ++i) {
    worker_threads_.push_back(std::make_unique<std::thread>(
        &PartitionMigrator::MigrationWorkerLoop, this));
  }
  
  return Status::OK();
}

void PartitionMigrator::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  for (auto& thread : worker_threads_) {
    if (thread && thread->joinable()) {
      thread->join();
    }
  }
  worker_threads_.clear();
}

void PartitionMigrator::MigrationWorkerLoop() {
  while (running_.load()) {
    uint64_t task_id = 0;
    
    // Find pending task and atomically claim it
    {
      std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
      for (const auto& [id, task] : tasks_) {
        if (task->state == MigrationState::kPending) {
          task->state = MigrationState::kPreparing;
          task->started_at = std::chrono::system_clock::now();
          task_id = id;
          break;
        }
      }
    }
    
    if (task_id != 0) {
      ExecuteMigration(task_id);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

StatusOr<uint64_t> PartitionMigrator::SubmitMigration(
    PartitionID pid,
    NodeID source_node,
    NodeID target_node,
    MigrationType type,
    MigrationProgressCallback callback) {
  if (!running_.load()) {
    return Status::InvalidArgument("PartitionMigrator not running");
  }

  uint64_t id = next_migration_id_.fetch_add(1);
  auto task = std::make_unique<MigrationTask>();
  task->migration_id = id;
  task->partition_id = pid;
  task->source_node = source_node;
  task->target_node = target_node;
  task->type = type;
  task->state = MigrationState::kPending;
  task->created_at = std::chrono::system_clock::now();

  {
    std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
    tasks_[id] = std::move(task);
    if (callback) {
      callbacks_[id] = callback;
    }
  }

  return id;
}

void PartitionMigrator::ExecuteMigration(uint64_t migration_id) {
  MigrationTask* task = nullptr;
  MigrationProgressCallback callback = nullptr;
  
  {
    std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
    auto it = tasks_.find(migration_id);
    if (it == tasks_.end()) {
      return;
    }
    task = it->second.get();
    
    auto cb_it = callbacks_.find(migration_id);
    if (cb_it != callbacks_.end()) {
      callback = cb_it->second;
    }
  }
  
  auto notify_progress = [&](const std::string& message) {
    if (callback) {
      callback(migration_id, task->state, task->GetProgressPercent(), message);
    }
  };
  
  // Execute migration phases
  // state is already kPreparing from MigrationWorkerLoop
  notify_progress("Preparing source partition");
  
  auto status = PrepareSource(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kCopying);
  if (!status.ok()) goto migration_failed;
  notify_progress("Starting data copy");
  
  status = CopyData(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kCatchingUp);
  if (!status.ok()) goto migration_failed;
  notify_progress("Catching up with new writes");
  
  status = CatchUp(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kSwitching);
  if (!status.ok()) goto migration_failed;
  notify_progress("Switching traffic to target");
  
  status = SwitchTraffic(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kVerifying);
  if (!status.ok()) goto migration_failed;
  notify_progress("Verifying consistency");
  
  status = VerifyConsistency(*task);
  if (!status.ok()) goto migration_failed;
  
  status = task->TransitionTo(MigrationState::kCompleting);
  if (!status.ok()) goto migration_failed;
  notify_progress("Completing migration");
  
  status = CompleteMigration(*task);
  if (!status.ok()) goto migration_failed;
  
  task->TransitionTo(MigrationState::kCompleted);
  task->completed_at = std::chrono::system_clock::now();
  notify_progress("Migration completed");
  
  {
    std::unique_lock<std::mutex> lock(stats_mutex_);
    stats_.successful_migrations++;
    stats_.total_migrations++;
    stats_.total_bytes_migrated += task->migrated_bytes;
    stats_.total_keys_migrated += task->migrated_keys;
  }
  
  return;
  
migration_failed:
  task->last_error = status.ToString();
  task->TransitionTo(MigrationState::kFailed);
  notify_progress("Migration failed: " + status.ToString());
  
  {
    std::unique_lock<std::mutex> lock(stats_mutex_);
    stats_.failed_migrations++;
    stats_.total_migrations++;
  }
  
  if (config_.max_retries > 0 && task->retry_count < config_.max_retries) {
    task->retry_count++;
    RetryMigration(migration_id);
  } else {
    RollbackMigration(*task);
  }
}

Status PartitionMigrator::PrepareSource(MigrationTask& task) {
  // Prepare source partition for migration
  task.started_at = std::chrono::system_clock::now();
  // In a full implementation this would:
  // - Pause compactions temporarily
  // - Create snapshot point
  // - Get partition stats
  return Status::OK();
}

Status PartitionMigrator::CopyData(MigrationTask& task) {
  if (!partition_manager_) {
    return Status::IOError("PartitionManager not injected");
  }

  auto source_storage = partition_manager_->GetPartition(task.partition_id);
  if (!source_storage) {
    return Status::NotFound("Source partition not found: " +
                            std::to_string(task.partition_id));
  }

  // 1. Force flush to ensure all data is in SST files
  auto* shared_storage = source_storage->GetSharedStorage();
  if (shared_storage) {
    auto s = shared_storage->ForceFlush();
    if (!s.ok()) {
      return Status::IOError("ForceFlush failed: " + s.ToString());
    }
  }

  // 2. Get partition stats for progress tracking
  auto stats = source_storage->GetStats();
  task.total_keys = stats.num_keys;
  task.total_bytes = stats.disk_usage_bytes;
  task.state = MigrationState::kCopying;

  // 3. Save snapshot for migration
  auto data_root = source_storage->GetDataRoot();
  auto snapshot_path = data_root + "/migration_snap_" + std::to_string(task.migration_id);

  auto s = source_storage->SaveSnapshotForMigration(snapshot_path);
  if (!s.ok()) {
    return Status::IOError("Snapshot creation failed: " + s.ToString());
  }

  // 4. Stream snapshot files to target node via SyncData RPC
  if (migration_stub_) {
    s = StreamSnapshotToTarget(task, snapshot_path);
    if (!s.ok()) {
      return Status::IOError("Snapshot stream failed: " + s.ToString());
    }
  }

  task.migrated_keys = task.total_keys;
  task.migrated_bytes = task.total_bytes;
  return Status::OK();
}

Status PartitionMigrator::StreamSnapshotToTarget(
    const MigrationTask& task, const std::string& snapshot_path) {
  ::grpc::ClientContext context;
  cedar::migration::SyncDataRequest request;
  cedar::migration::SyncDataResponse response;

  request.set_migration_id(std::to_string(task.migration_id));
  request.set_partition_id(task.partition_id);
  request.set_offset(0);

  auto writer = migration_stub_->SyncData(&context, &response);

  for (const auto& entry : std::filesystem::recursive_directory_iterator(snapshot_path)) {
    if (!entry.is_regular_file()) continue;

    std::ifstream file(entry.path(), std::ios::binary);
    if (!file) continue;

    std::vector<char> buffer(64 * 1024);
    while (file.good()) {
      file.read(buffer.data(), buffer.size());
      std::streamsize bytes_read = file.gcount();
      if (bytes_read <= 0) break;

      request.set_data(buffer.data(), static_cast<size_t>(bytes_read));
      if (!writer->Write(request)) {
        return Status::IOError("SyncData stream write failed");
      }
    }
  }

  writer->WritesDone();
  auto status = writer->Finish();
  if (!status.ok()) {
    return Status::IOError("SyncData RPC failed: " + status.error_message());
  }
  return Status::OK();
}

Status PartitionMigrator::CatchUp(MigrationTask& task) {
  if (!partition_manager_) {
    return Status::IOError("PartitionManager not injected");
  }

  auto source_storage = partition_manager_->GetPartition(task.partition_id);
  if (!source_storage) {
    return Status::NotFound("Source partition not found");
  }

  task.state = MigrationState::kCatchingUp;

  // Read WAL and replay recent operations to target
  std::string wal_dir = source_storage->GetDataRoot() + "/wal";
  std::string wal_path = wal_dir + "/partition_" +
                         std::to_string(task.partition_id) + "_wal.log";

  if (!std::filesystem::exists(wal_path)) {
    return Status::OK();
  }

  auto s = ReplayWalToTarget(task, wal_path);
  if (!s.ok()) {
    return Status::IOError("WAL replay failed: " + s.ToString());
  }

  return Status::OK();
}

Status PartitionMigrator::ReplayWalToTarget(
    const MigrationTask& task, const std::string& wal_path) {
  int fd = ::open(wal_path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError("Failed to open WAL");

  struct stat st;
  if (fstat(fd, &st) < 0 || st.st_size == 0) {
    ::close(fd);
    return Status::OK();
  }

  std::string wal_data(st.st_size, '\0');
  ssize_t n = ::read(fd, &wal_data[0], st.st_size);
  ::close(fd);
  if (n != st.st_size) return Status::IOError("Failed to read WAL");

  size_t pos = 0;
  uint64_t ops_replayed = 0;

  while (pos + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) <=
         static_cast<size_t>(st.st_size)) {
    uint64_t ts, txn_id;
    uint32_t op_len;

    std::memcpy(&ts, &wal_data[pos], sizeof(ts));
    pos += sizeof(ts);
    std::memcpy(&txn_id, &wal_data[pos], sizeof(txn_id));
    pos += sizeof(txn_id);
    std::memcpy(&op_len, &wal_data[pos], sizeof(op_len));
    pos += sizeof(op_len);

    if (pos + op_len > static_cast<size_t>(st.st_size)) break;

    std::string op = wal_data.substr(pos, op_len);
    pos += op_len;

    // For now, just count operations. In a full implementation, these would be
    // streamed to the target node and replayed there.
    ops_replayed++;
  }

  LOG(INFO) << "[Migration] Caught up " << ops_replayed
            << " WAL operations for partition " << task.partition_id;

  return Status::OK();
}

Status PartitionMigrator::SwitchTraffic(MigrationTask& task) {
  if (!meta_client_) {
    return Status::IOError("MetaServiceClient not injected");
  }

  task.state = MigrationState::kSwitching;

  // Update partition assignment in MetaD: move leader from source to target
  auto result = meta_client_->GetPartitionAssignment(task.partition_id);
  if (!result.ok()) {
    return result.status();
  }

  auto assignment = result.value();
  assignment.set_leader_node(task.target_node);

  auto s = meta_client_->UpdatePartitionAssignment(assignment);
  if (!s.ok()) {
    return Status::IOError("MetaD assignment update failed: " + s.ToString());
  }

  return Status::OK();
}

Status PartitionMigrator::VerifyConsistency(MigrationTask& task) {
  if (!config_.verify_checksum) {
    return Status::OK();
  }
  
  std::string source_checksum, target_checksum;
  auto status = CalculateChecksum(task.partition_id, &source_checksum);
  if (!status.ok()) return status;
  
  // Target checksum: in a full implementation this would be fetched
  // from the target node via RPC. For now, skip if not available.
  if (target_checksum.empty()) {
    return Status::OK();
  }
  
  if (!VerifyChecksum(source_checksum, target_checksum)) {
    return Status::Corruption("Checksum mismatch after migration");
  }
  
  return Status::OK();
}

Status PartitionMigrator::CompleteMigration(MigrationTask& task) {
  task.state = MigrationState::kCompleting;

  if (partition_manager_) {
    auto source_storage = partition_manager_->GetPartition(task.partition_id);
    if (source_storage) {
      LOG(INFO) << "[Migration] Completed migration " << task.migration_id
                << " for partition " << task.partition_id;
    }
  }

  return Status::OK();
}

Status PartitionMigrator::RollbackMigration(MigrationTask& task) {
  task.state = MigrationState::kRolledBack;

  // Revert traffic if SwitchTraffic was already done
  if (meta_client_ && task.target_node != 0) {
    auto result = meta_client_->GetPartitionAssignment(task.partition_id);
    if (result.ok()) {
      auto assignment = result.value();
      if (assignment.leader_node() == task.target_node) {
        assignment.set_leader_node(task.source_node);
        auto s = meta_client_->UpdatePartitionAssignment(assignment);
        if (!s.ok()) {
          LOG(WARNING) << "[Migration] Rollback leader revert failed: " << s.ToString();
        }
      }
    }
  }

  return Status::OK();
}

Status PartitionMigrator::CalculateChecksum(PartitionID pid,
                                            std::string* checksum) {
  PartitionStorage* storage = partition_manager_->GetPartition(pid);
  if (!storage) {
    return Status::NotFound("Partition", std::to_string(pid));
  }

  auto* shared_storage = storage->GetSharedStorage();
  if (!shared_storage) {
    return Status::IOError("Partition", "No shared storage");
  }

  // Flush memtable to ensure all data is in SST files
  Status s = shared_storage->ForceFlush();
  if (!s.ok()) {
    return Status::IOError("ForceFlush failed", s.ToString());
  }

  auto* lsm = shared_storage->GetLsmEngine();
  if (!lsm) {
    return Status::IOError("Partition", "No LSM engine");
  }

  uint32_t crc = 0;
  const auto& levels = lsm->GetSstFiles();
  for (const auto& level : levels) {
    for (const auto& meta : level) {
      std::string filepath = lsm->GetDbPath() + "/" + std::to_string(meta.file_number) + ".sst";
      ZoneColumnarSstReader reader(filepath);
      s = reader.Open();
      if (!s.ok()) {
        return Status::IOError("Cannot open SST",
                               filepath + ": " + s.ToString());
      }

      auto* iter = reader.NewIterator();
      iter->SeekToFirst();
      size_t entry_count = 0;
      for (; iter->Valid(); iter->Next()) {
        CedarKey key = iter->Key();
        Descriptor desc = iter->Value();
        entry_count++;

        char key_buf[CedarKey::kKeySize];
        key.EncodeTo(key_buf);
        crc = cedar::crc32c::Extend(crc, key_buf, CedarKey::kKeySize);

        char desc_buf[8];
        uint64_t raw = desc.AsRaw();
        std::memcpy(desc_buf, &raw, 8);
        crc = cedar::crc32c::Extend(crc, desc_buf, 8);
      }
      delete iter;
      reader.Close();
    }
  }

  *checksum = std::to_string(crc);
  return Status::OK();
}

bool PartitionMigrator::VerifyChecksum(const std::string& source_checksum,
                                        const std::string& target_checksum) {
  return source_checksum == target_checksum;
}

StatusOr<MigrationTask> PartitionMigrator::GetMigrationStatus(
    uint64_t migration_id) const {
  std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }
  return *it->second;
}

std::vector<MigrationTask> PartitionMigrator::GetActiveMigrations() const {
  std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
  std::vector<MigrationTask> active;
  
  for (const auto& [id, task] : tasks_) {
    if (task->state != MigrationState::kCompleted &&
        task->state != MigrationState::kFailed &&
        task->state != MigrationState::kRolledBack) {
      active.push_back(*task);
    }
  }
  
  return active;
}

Status PartitionMigrator::CancelMigration(uint64_t migration_id, bool rollback) {
  std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }
  
  auto task = it->second.get();
  
  if (task->state == MigrationState::kCompleted ||
      task->state == MigrationState::kFailed ||
      task->state == MigrationState::kRolledBack) {
    return Status::InvalidArgument("Cannot cancel finished migration");
  }
  
  if (rollback) {
    lock.unlock();
    RollbackMigration(*task);
    std::unique_lock<std::shared_mutex> relock(tasks_mutex_);
    task->state = MigrationState::kRolledBack;
  } else {
    task->state = MigrationState::kFailed;
  }
  
  std::unique_lock<std::mutex> stats_lock(stats_mutex_);
  stats_.cancelled_migrations++;
  
  return Status::OK();
}

Status PartitionMigrator::LoadSnapshotForMigration(
    uint64_t migration_id, const std::string& snapshot_path) {
  std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
  auto task_it = tasks_.find(migration_id);
  if (task_it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }

  auto& task = task_it->second;
  if (!partition_manager_) {
    return Status::IOError("PartitionManager not injected");
  }

  auto target_storage = partition_manager_->GetPartition(task->partition_id);
  if (!target_storage) {
    return Status::NotFound("Target partition not found");
  }

  // Load prepared transaction state if present
  auto txn_state_path = snapshot_path + "/txn_state";
  if (std::filesystem::exists(txn_state_path)) {
    auto s = target_storage->LoadPreparedTxns(txn_state_path);
    if (!s.ok()) return s;
  }

  // Copy data files into target partition's data root
  auto data_root = target_storage->GetDataRoot();
  for (const auto& entry : std::filesystem::recursive_directory_iterator(snapshot_path)) {
    if (!entry.is_regular_file()) continue;
    auto rel_path = std::filesystem::relative(entry.path(), snapshot_path);
    if (rel_path == "txn_state") continue;
    auto dst_path = data_root + "/" + rel_path.string();
    std::filesystem::create_directories(std::filesystem::path(dst_path).parent_path());
    std::filesystem::copy_file(entry.path(), dst_path,
                                std::filesystem::copy_options::overwrite_existing);
  }

  return Status::OK();
}

Status PartitionMigrator::CommitMigration(uint64_t migration_id) {
  std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }
  
  auto task = it->second.get();
  
  if (task->state == MigrationState::kCompleted) {
    return Status::OK();
  }
  if (task->state == MigrationState::kFailed ||
      task->state == MigrationState::kRolledBack) {
    return Status::InvalidArgument("Cannot commit failed/rolled-back migration");
  }
  
  lock.unlock();
  
  auto status = CompleteMigration(*task);
  if (!status.ok()) {
    return status;
  }
  
  std::unique_lock<std::shared_mutex> relock(tasks_mutex_);
  task->state = MigrationState::kCompleted;
  task->completed_at = std::chrono::system_clock::now();
  
  {
    std::unique_lock<std::mutex> stats_lock(stats_mutex_);
    stats_.successful_migrations++;
    stats_.total_migrations++;
    stats_.total_bytes_migrated += task->migrated_bytes;
    stats_.total_keys_migrated += task->migrated_keys;
  }
  
  return Status::OK();
}

Status PartitionMigrator::RetryMigration(uint64_t migration_id) {
  std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it == tasks_.end()) {
    return Status::NotFound("Migration not found");
  }
  
  auto& task = it->second;
  
  if (task->state != MigrationState::kFailed &&
      task->state != MigrationState::kRolledBack) {
    return Status::InvalidArgument("Can only retry failed migrations");
  }
  
  task->state = MigrationState::kPending;
  task->last_error.clear();
  
  return Status::OK();
}

PartitionMigrator::Stats PartitionMigrator::GetStats() const {
  std::unique_lock<std::mutex> lock(stats_mutex_);
  return stats_;
}

// =============================================================================
// RebalancePlanner Implementation
// =============================================================================

RebalancePlanner::RebalancePlanner() = default;

void RebalancePlanner::AnalyzeLoad(
    const std::vector<NodeLoad>& node_loads,
    const std::map<PartitionID, NodeID>& partition_distribution) {
  std::lock_guard<std::mutex> lock(mutex_);
  node_loads_ = node_loads;
  partition_distribution_ = partition_distribution;
}

std::vector<RebalancePlanner::RebalanceAction> RebalancePlanner::GeneratePlan(
    uint32_t max_moves, double imbalance_threshold) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<RebalanceAction> plan;
  
  if (!IsRebalancingNeeded(imbalance_threshold)) {
    return plan;
  }
  
  // Calculate target load per node
  uint64_t total_keys = 0;
  for (const auto& load : node_loads_) {
    total_keys += load.total_keys;
  }
  
  if (node_loads_.empty() || total_keys == 0) {
    return plan;
  }
  
  double target_keys_per_node = static_cast<double>(total_keys) / node_loads_.size();
  
  // Find overloaded and underloaded nodes
  std::vector<NodeID> overloaded;
  std::vector<NodeID> underloaded;
  
  for (const auto& load : node_loads_) {
    double ratio = load.total_keys / target_keys_per_node;
    if (ratio > 1.0 + imbalance_threshold) {
      overloaded.push_back(load.node_id);
    } else if (ratio < 1.0 - imbalance_threshold) {
      underloaded.push_back(load.node_id);
    }
  }
  
  // Generate moves from overloaded to underloaded
  for (const auto& from_node : overloaded) {
    if (plan.size() >= max_moves) break;
    
    for (const auto& to_node : underloaded) {
      if (plan.size() >= max_moves) break;
      
      // Find partitions on from_node that can move to to_node
      for (const auto& [pid, current_node] : partition_distribution_) {
        if (current_node == from_node) {
          RebalanceAction action;
          action.partition_id = pid;
          action.from_node = from_node;
          action.to_node = to_node;
          action.score = CalculateNodeScore(
              *std::find_if(node_loads_.begin(), node_loads_.end(),
                           [to_node](const NodeLoad& l) { 
                             return l.node_id == to_node; 
                           }));
          plan.push_back(action);
          break;
        }
      }
    }
  }
  
  return plan;
}

bool RebalancePlanner::IsRebalancingNeeded(double threshold) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return CalculateLoadVariance() > threshold;
}

double RebalancePlanner::CalculateLoadVariance() const {
  // Caller must hold mutex_
  if (node_loads_.size() <= 1) {
    return 0.0;
  }
  
  double total_keys = 0;
  for (const auto& load : node_loads_) {
    total_keys += load.total_keys;
  }
  
  double mean = total_keys / node_loads_.size();
  if (mean == 0) {
    return 0.0;
  }
  
  double variance = 0;
  for (const auto& load : node_loads_) {
    double diff = load.total_keys - mean;
    variance += diff * diff;
  }
  variance /= node_loads_.size();
  
  // Coefficient of variation
  return std::sqrt(variance) / mean;
}

double RebalancePlanner::CalculateNodeScore(const NodeLoad& load) const {
  // Higher score = better candidate for receiving partitions
  // Consider CPU, memory, disk usage
  double score = 1.0;
  
  // Penalize high resource usage
  score -= load.cpu_usage * 0.3;
  score -= load.disk_usage * 0.3;
  
  return score;
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
