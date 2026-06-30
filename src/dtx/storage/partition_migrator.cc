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
#include "cedar/storage/lsm_engine.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <system_error>
#include <sys/stat.h>

#include <butil/logging.h>

namespace cedar {
namespace dtx {
namespace storage {

namespace {

namespace fs = std::filesystem;

Status FilesystemStatus(const std::string& operation,
                        const fs::path& path,
                        const std::error_code& ec) {
  return Status::IOError(operation + ": " + path.string(), ec.message());
}

Status ValidateReadableDirectory(const fs::path& path,
                                 const std::string& label) {
  std::error_code ec;
  if (!fs::exists(path, ec)) {
    if (ec) return FilesystemStatus("Cannot stat " + label, path, ec);
    return Status::NotFound(label + " not found", path.string());
  }
  if (!fs::is_directory(path, ec)) {
    if (ec) return FilesystemStatus("Cannot inspect " + label, path, ec);
    return Status::InvalidArgument(label + " is not a directory", path.string());
  }
  return Status::OK();
}

bool HasParentTraversal(const fs::path& path) {
  for (const auto& part : path) {
    if (part == "..") return true;
  }
  return false;
}

}  // namespace

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
    worker_threads_.emplace_back(std::make_unique<std::thread>(
        &PartitionMigrator::MigrationWorkerLoop, this));
  }
  
  return Status::OK();
}

void PartitionMigrator::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  worker_cv_.notify_all();
  
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
      std::unique_lock<std::mutex> lock(worker_mutex_);
      worker_cv_.wait_for(lock, std::chrono::milliseconds(100), [this]() {
        if (!running_.load()) {
          return true;
        }
        std::shared_lock<std::shared_mutex> tasks_lock(tasks_mutex_);
        for (const auto& [id, task] : tasks_) {
          (void)id;
          if (task->state == MigrationState::kPending) {
            return true;
          }
        }
        return false;
      });
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
  worker_cv_.notify_one();

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
  auto* effective_storage = source_storage->GetEffectiveStorage();
  if (effective_storage) {
    auto s = effective_storage->ForceFlush();
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
  const fs::path snapshot_dir(snapshot_path);
  auto dir_status = ValidateReadableDirectory(snapshot_dir, "Snapshot path");
  if (!dir_status.ok()) {
    return dir_status;
  }
  if (!migration_stub_) {
    return Status::IOError("MigrationService stub not injected");
  }

  ::grpc::ClientContext context;
  cedar::migration::SyncDataRequest request;
  cedar::migration::SyncDataResponse response;

  request.set_migration_id(std::to_string(task.migration_id));
  request.set_partition_id(task.partition_id);

  auto writer = migration_stub_->SyncData(&context, &response);
  if (!writer) {
    return Status::IOError("Failed to open SyncData stream");
  }

  uint64_t offset = 0;
  std::error_code ec;
  fs::recursive_directory_iterator it(
      snapshot_dir, fs::directory_options::skip_permission_denied, ec);
  if (ec) {
    return FilesystemStatus("Cannot iterate snapshot path", snapshot_dir, ec);
  }
  const fs::recursive_directory_iterator end;
  for (; it != end; it.increment(ec)) {
    if (ec) {
      return FilesystemStatus("Cannot advance snapshot iterator", snapshot_dir, ec);
    }
    const auto& entry = *it;
    if (!entry.is_regular_file(ec)) {
      if (ec) return FilesystemStatus("Cannot inspect snapshot entry", entry.path(), ec);
      continue;
    }

    std::ifstream file(entry.path(), std::ios::binary);
    if (!file) {
      return Status::IOError("Cannot open snapshot file", entry.path().string());
    }

    std::vector<char> buffer(64 * 1024);
    while (file.good()) {
      file.read(buffer.data(), buffer.size());
      std::streamsize bytes_read = file.gcount();
      if (bytes_read <= 0) break;

      request.set_offset(offset);
      request.set_data(buffer.data(), static_cast<size_t>(bytes_read));
      offset += static_cast<uint64_t>(bytes_read);
      if (!writer->Write(request)) {
        return Status::IOError("SyncData stream write failed");
      }
    }
    if (file.bad()) {
      return Status::IOError("Failed reading snapshot file", entry.path().string());
    }
  }

  if (!writer->WritesDone()) {
    return Status::IOError("SyncData stream close failed");
  }
  auto status = writer->Finish();
  if (!status.ok()) {
    return Status::IOError("SyncData RPC failed: " + status.error_message());
  }
  if (!response.success()) {
    std::string error_msg = response.error_msg().empty()
                                ? "target returned unsuccessful SyncData response"
                                : response.error_msg();
    return Status::IOError("SyncData target rejected snapshot: " + error_msg);
  }
  if (response.bytes_received() != offset) {
    return Status::IOError(
        "SyncData byte count mismatch",
        "sent " + std::to_string(offset) + ", target received " +
            std::to_string(response.bytes_received()));
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
  size_t total_read = 0;
  while (total_read < static_cast<size_t>(st.st_size)) {
    ssize_t n = ::read(fd, &wal_data[total_read], st.st_size - total_read);
    if (n < 0) {
      if (errno == EINTR) continue;
      ::close(fd);
      return Status::IOError("Failed to read WAL");
    }
    if (n == 0) {
      ::close(fd);
      return Status::IOError("Unexpected EOF reading WAL");
    }
    total_read += static_cast<size_t>(n);
  }
  ::close(fd);

  size_t pos = 0;
  uint64_t ops_replayed = 0;
  const uint32_t kWalMagic = 0x57414C01;  // "WAL\x01"
  const size_t kWalBatchSize = 100;
  std::vector<cedar::migration::ReplicateWALEntryRequest> batch;

  auto flush_batch = [&]() -> Status {
    if (batch.empty()) return Status::OK();
    if (!migration_stub_) {
      batch.clear();
      return Status::OK();
    }
    ::grpc::ClientContext context;
    cedar::migration::ReplicateWALBatchRequest request;
    cedar::migration::ReplicateWALBatchResponse response;

    request.set_migration_id(std::to_string(task.migration_id));
    request.set_partition_id(task.partition_id);
    for (auto& entry : batch) {
      *request.add_entries() = std::move(entry);
    }

    ::grpc::Status status = migration_stub_->ReplicateWALBatch(
        &context, request, &response);
    if (!status.ok()) {
      return Status::IOError("WAL batch replication RPC failed: " +
                             status.error_message());
    }
    if (!response.success()) {
      return Status::IOError("WAL batch replication rejected: " +
                             response.error_msg());
    }
    batch.clear();
    return Status::OK();
  };

  while (pos + sizeof(uint32_t) <= static_cast<size_t>(st.st_size)) {
    uint32_t magic;
    std::memcpy(&magic, &wal_data[pos], sizeof(magic));
    pos += sizeof(magic);
    if (magic != kWalMagic) {
      return Status::Corruption("Migration WAL", "magic mismatch");
    }

    if (pos + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) >
        static_cast<size_t>(st.st_size)) {
      return Status::Corruption("Migration WAL", "truncated header");
    }

    uint64_t ts, txn_id;
    uint32_t op_len, crc;
    std::memcpy(&ts, &wal_data[pos], sizeof(ts)); pos += sizeof(ts);
    std::memcpy(&txn_id, &wal_data[pos], sizeof(txn_id)); pos += sizeof(txn_id);
    std::memcpy(&op_len, &wal_data[pos], sizeof(op_len)); pos += sizeof(op_len);
    std::memcpy(&crc, &wal_data[pos], sizeof(crc)); pos += sizeof(crc);

    if (pos + op_len > static_cast<size_t>(st.st_size)) {
      return Status::Corruption("Migration WAL", "truncated op data");
    }

    std::string op = wal_data.substr(pos, op_len);
    pos += op_len;

    // Verify CRC32
    uint32_t computed_crc = cedar::crc32c::Value(op.data(), op.size());
    if (computed_crc != crc) {
      return Status::Corruption("Migration WAL", "CRC mismatch");
    }

    // Accumulate WAL entry for batch replay
    if (migration_stub_) {
      cedar::migration::ReplicateWALEntryRequest entry;
      entry.set_migration_id(std::to_string(task.migration_id));
      entry.set_partition_id(task.partition_id);
      entry.set_timestamp(ts);
      entry.set_txn_id(txn_id);
      entry.set_op_data(op);
      batch.push_back(std::move(entry));

      if (batch.size() >= kWalBatchSize) {
        auto s = flush_batch();
        if (!s.ok()) return s;
      }
    }
    ops_replayed++;
  }

  if (!batch.empty()) {
    auto s = flush_batch();
    if (!s.ok()) return s;
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

  // Wait for in-flight requests on the source to drain.
  // Use 2x the configured RPC timeout as a reasonable upper bound.
  const int drain_wait_ms = config_.rpc_timeout_ms * 2;
  if (drain_wait_ms > 0) {
    LOG(INFO) << "[Migration] Draining in-flight requests for partition "
              << task.partition_id << " (max " << drain_wait_ms << "ms)";

    auto drain_start = std::chrono::steady_clock::now();
    bool drained = false;
    while (true) {
      auto elapsed = std::chrono::steady_clock::now() - drain_start;
      if (elapsed >= std::chrono::milliseconds(drain_wait_ms)) break;

      // Active drain: poll for active transaction count on the partition
      if (partition_manager_) {
        auto* storage = partition_manager_->GetPartition(task.partition_id);
        if (storage) {
          auto stats = storage->GetStats();
          if (stats.num_active_txns == 0) {
            drained = true;
            break;
          }
        }
      }
      if (WaitForDrainOrShutdown(std::chrono::milliseconds(10))) {
        break;
      }
    }

    if (!drained) {
      LOG(WARNING) << "[Migration] Drain timeout exceeded for partition "
                   << task.partition_id
                   << ", some requests may still be in flight";
    } else {
      LOG(INFO) << "[Migration] Partition " << task.partition_id
                << " drained successfully";
    }
  }

  return Status::OK();
}

bool PartitionMigrator::WaitForDrainOrShutdown(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(worker_mutex_);
  return worker_cv_.wait_for(lock, timeout, [this]() {
    return !running_.load();
  });
}

Status PartitionMigrator::VerifyConsistency(MigrationTask& task) {
  if (!config_.verify_checksum) {
    return Status::OK();
  }

  std::string source_checksum, target_checksum;
  auto status = CalculateChecksum(task.partition_id, &source_checksum);
  if (!status.ok()) return status;

  if (!migration_stub_) {
    return Status::OK();
  }

  ::grpc::ClientContext context;
  cedar::migration::FetchChecksumRequest request;
  cedar::migration::FetchChecksumResponse response;

  request.set_partition_id(task.partition_id);
  ::grpc::Status grpc_status = migration_stub_->FetchChecksum(
      &context, request, &response);
  if (!grpc_status.ok()) {
    return Status::IOError("FetchChecksum RPC failed: " +
                           grpc_status.error_message());
  }
  if (!response.success()) {
    return Status::IOError("FetchChecksum failed: " + response.error_msg());
  }
  target_checksum = response.checksum();

  if (!VerifyChecksum(source_checksum, target_checksum)) {
    return Status::Corruption("Migration checksum mismatch");
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

  auto* effective_storage = storage->GetEffectiveStorage();
  if (!effective_storage) {
    return Status::IOError("Partition", "No storage available");
  }

  // Flush memtable to ensure all data is in SST files
  Status s = effective_storage->ForceFlush();
  if (!s.ok()) {
    return Status::IOError("ForceFlush failed", s.ToString());
  }

  auto* lsm = effective_storage->GetLsmEngine();
  if (!lsm) {
    return Status::IOError("Partition", "No LSM engine");
  }

  uint32_t crc = 0;
  const auto& levels = lsm->GetSstFiles();
  for (const auto& level : levels) {
    for (const auto& meta : level) {
      // Include SST metadata in checksum for sensitivity to metadata changes
      char meta_buf[40];
      size_t meta_off = 0;
      std::memcpy(meta_buf + meta_off, &meta.file_number, sizeof(meta.file_number));
      meta_off += sizeof(meta.file_number);
      std::memcpy(meta_buf + meta_off, &meta.file_size, sizeof(meta.file_size));
      meta_off += sizeof(meta.file_size);
      std::memcpy(meta_buf + meta_off, &meta.num_entries, sizeof(meta.num_entries));
      meta_off += sizeof(meta.num_entries);
      std::memcpy(meta_buf + meta_off, &meta.level, sizeof(meta.level));
      meta_off += sizeof(meta.level);
      crc = cedar::crc32c::Extend(crc, meta_buf, meta_off);

      // Compute checksum from raw file content instead of parsing every entry
      std::string filepath = lsm->GetDbPath() + "/" + std::to_string(meta.file_number) + ".sst";
      std::ifstream file(filepath, std::ios::binary);
      if (!file) {
        return Status::IOError("Cannot open SST", filepath);
      }

      char buf[64 * 1024];
      while (file.read(buf, sizeof(buf)) || file.gcount() > 0) {
        crc = cedar::crc32c::Extend(crc, buf, file.gcount());
      }
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
  PartitionID partition_id = 0;
  {
    std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
    auto task_it = tasks_.find(migration_id);
    if (task_it == tasks_.end()) {
      return Status::NotFound("Migration not found");
    }
    partition_id = task_it->second->partition_id;
  }

  if (!partition_manager_) {
    return Status::IOError("PartitionManager not injected");
  }

  auto target_storage = partition_manager_->GetPartition(partition_id);
  if (!target_storage) {
    return Status::NotFound("Target partition not found");
  }

  const fs::path snapshot_dir(snapshot_path);
  auto dir_status = ValidateReadableDirectory(snapshot_dir, "Snapshot path");
  if (!dir_status.ok()) {
    return dir_status;
  }

  // Load prepared transaction state if present
  const fs::path txn_state_path = snapshot_dir / "txn_state";
  std::error_code ec;
  if (fs::exists(txn_state_path, ec)) {
    auto s = target_storage->LoadPreparedTxns(txn_state_path.string());
    if (!s.ok()) return s;
  } else if (ec) {
    return FilesystemStatus("Cannot stat transaction state snapshot",
                            txn_state_path, ec);
  }

  // Copy data files into target partition's data root
  const fs::path data_root(target_storage->GetDataRoot());
  if (fs::equivalent(snapshot_dir, data_root, ec) && !ec) {
    return Status::InvalidArgument("Snapshot path cannot be the target data root",
                                   snapshot_dir.string());
  }
  ec.clear();
  fs::recursive_directory_iterator it(
      snapshot_dir, fs::directory_options::skip_permission_denied, ec);
  if (ec) {
    return FilesystemStatus("Cannot iterate snapshot path", snapshot_dir, ec);
  }
  const fs::recursive_directory_iterator end;
  for (; it != end; it.increment(ec)) {
    if (ec) {
      return FilesystemStatus("Cannot advance snapshot iterator", snapshot_dir, ec);
    }
    const auto& entry = *it;
    if (!entry.is_regular_file(ec)) {
      if (ec) return FilesystemStatus("Cannot inspect snapshot entry", entry.path(), ec);
      continue;
    }
    auto rel_path = fs::relative(entry.path(), snapshot_dir, ec);
    if (ec) {
      return FilesystemStatus("Cannot derive snapshot relative path", entry.path(), ec);
    }
    if (rel_path == fs::path("txn_state")) continue;
    if (rel_path.is_absolute() || HasParentTraversal(rel_path)) {
      return Status::InvalidArgument("Unsafe snapshot relative path",
                                     rel_path.string());
    }

    auto dst_path = data_root / rel_path;
    fs::create_directories(dst_path.parent_path(), ec);
    if (ec) {
      return FilesystemStatus("Cannot create snapshot destination directory",
                              dst_path.parent_path(), ec);
    }
    fs::copy_file(entry.path(), dst_path,
                  fs::copy_options::overwrite_existing, ec);
    if (ec) {
      return FilesystemStatus("Cannot copy snapshot file", dst_path, ec);
    }
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
