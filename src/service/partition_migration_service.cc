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
// Partition Migration Service Implementation
// =============================================================================

#include "cedar/service/partition_migration_service.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>
#include <unordered_set>

#include "cedar/dtx/storage/partition_migrator.h"

namespace cedar {
namespace service {

// =============================================================================
// Helper Functions
// =============================================================================

namespace {

// RAII temp directory guard
class TempDirGuard {
 public:
  explicit TempDirGuard(const std::string& path) : path_(path) {
    std::filesystem::create_directories(path_);
  }
  ~TempDirGuard() {
    std::filesystem::remove_all(path_);
  }
  const std::string& path() const { return path_; }
 private:
  std::string path_;
};

// Generate UUID v4 style string
std::string GenerateUuid() {
  static thread_local std::mt19937 rng(std::random_device{}());
  static thread_local std::uniform_int_distribution<int> dist(0, 15);
  static thread_local std::uniform_int_distribution<int> dist2(8, 11);
  
  std::stringstream ss;
  ss << std::hex;
  
  // 8 digits
  for (int i = 0; i < 8; ++i) ss << dist(rng);
  ss << "-";
  // 4 digits
  for (int i = 0; i < 4; ++i) ss << dist(rng);
  ss << "-4";  // Version 4
  // 3 digits
  for (int i = 0; i < 3; ++i) ss << dist(rng);
  ss << "-";
  ss << dist2(rng);  // Variant
  // 3 digits
  for (int i = 0; i < 3; ++i) ss << dist(rng);
  ss << "-";
  // 12 digits
  for (int i = 0; i < 12; ++i) ss << dist(rng);
  
  return ss.str();
}

// Simple CRC64-like checksum for data verification
uint64_t ComputeChecksum(const uint8_t* data, size_t length) {
  static const uint64_t POLY = 0xC96C5795D7870F42ULL;
  uint64_t crc = 0xFFFFFFFFFFFFFFFFULL;
  
  for (size_t i = 0; i < length; ++i) {
    crc ^= data[i];
    for (int j = 0; j < 8; ++j) {
      crc = (crc >> 1) ^ (POLY & -(crc & 1));
    }
  }
  
  return crc ^ 0xFFFFFFFFFFFFFFFFULL;
}

// Check if migration status is terminal (completed, failed, or rolled back)
bool IsTerminalStatus(cedar::migration::MigrationStatus status) {
  return status == cedar::migration::COMPLETED ||
         status == cedar::migration::FAILED ||
         status == cedar::migration::ROLLED_BACK;
}

// Check if migration status allows data sync
bool CanSyncData(cedar::migration::MigrationStatus status) {
  return status == cedar::migration::SYNCING ||
         status == cedar::migration::CATCHING_UP;
}

}  // namespace

// =============================================================================
// Constructor / Destructor
// =============================================================================

PartitionMigrationServiceImpl::PartitionMigrationServiceImpl(const Options& options)
    : options_(options) {}

PartitionMigrationServiceImpl::~PartitionMigrationServiceImpl() {
  // Clean up all tasks
  std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
  tasks_.clear();
}

void PartitionMigrationServiceImpl::SetPartitionMigrator(
    dtx::storage::PartitionMigrator* migrator) {
  partition_migrator_ = migrator;
}

// =============================================================================
// gRPC Service Methods
// =============================================================================

::grpc::Status PartitionMigrationServiceImpl::StartMigration(
    ::grpc::ServerContext* context,
    const ::cedar::migration::StartMigrationRequest* request,
    ::cedar::migration::StartMigrationResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  // Validate request
  std::string error_msg;
  if (!ValidateStartRequest(request, &error_msg)) {
    response->set_success(false);
    response->set_error_msg(error_msg);
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, error_msg);
  }
  
  // Check concurrent migration limit
  {
    std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
    uint32_t active_count = 0;
    for (const auto& [id, task] : tasks_) {
      if (!IsTerminalStatus(task->status)) {
        ++active_count;
      }
    }
    if (active_count >= options_.max_concurrent_migrations) {
      error_msg = "Maximum concurrent migrations reached: " + 
                  std::to_string(options_.max_concurrent_migrations);
      response->set_success(false);
      response->set_error_msg(error_msg);
      return ::grpc::Status(::grpc::StatusCode::RESOURCE_EXHAUSTED, error_msg);
    }
  }
  
  // Generate migration ID
  std::string migration_id = GenerateMigrationId();
  
  // Create new task
  auto task = std::make_unique<ServiceMigrationTask>();
  task->migration_id = migration_id;
  task->internal_id = next_internal_id_.fetch_add(1);
  task->partition_id = request->partition_id();
  task->source_node = request->source_node();
  task->target_node = request->target_node();
  task->target_address = request->target_address();
  task->estimated_data_size = request->estimated_data_size();
  task->bytes_total = request->estimated_data_size();
  task->status = cedar::migration::PREPARING;
  task->started_at = std::chrono::system_clock::now();
  
  // Store task
  {
    std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
    tasks_[migration_id] = std::move(task);
  }
  
  // Update statistics
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    ++stats_.total_migrations_started;
    ++stats_.active_migrations;
  }
  
  // Transition to SYNCING state (ready for data)
  ServiceMigrationTask* task_ptr = FindTask(migration_id);
  if (task_ptr) {
    UpdateTaskStatus(task_ptr, cedar::migration::SYNCING);
    
    // If we have a partition migrator, submit the task there as well
    if (partition_migrator_ != nullptr) {
      auto parse_node_id = [](const std::string& s) -> dtx::NodeID {
        try {
          return static_cast<dtx::NodeID>(std::stoul(s));
        } catch (...) {
          return 0;
        }
      };
      
      auto migrator_result = partition_migrator_->SubmitMigration(
          static_cast<dtx::PartitionID>(task_ptr->partition_id),
          parse_node_id(task_ptr->source_node),
          parse_node_id(task_ptr->target_node),
          dtx::storage::MigrationType::kRebalance);
      
      if (migrator_result.ok()) {
        task_ptr->internal_id = migrator_result.ValueOrDie();
      }
    }
  }
  
  response->set_success(true);
  response->set_migration_id(migration_id);
  
  return ::grpc::Status::OK;
}

::grpc::Status PartitionMigrationServiceImpl::SyncData(
    ::grpc::ServerContext* context,
    ::grpc::ServerReader<::cedar::migration::SyncDataRequest>* reader,
    ::cedar::migration::SyncDataResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  ::cedar::migration::SyncDataRequest request;
  uint64_t total_bytes_received = 0;
  std::string migration_id;
  uint32_t partition_id = 0;
  
  // Read first request to identify the migration
  if (!reader->Read(&request)) {
    response->set_success(false);
    response->set_error_msg("No data received in stream");
    response->set_bytes_received(0);
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Empty data stream");
  }
  
  migration_id = request.migration_id();
  partition_id = request.partition_id();
  
  // Find the task once and hold reference for the entire stream
  ServiceMigrationTask* task = FindTask(migration_id);
  if (!task) {
    response->set_success(false);
    response->set_error_msg("Migration not found: " + migration_id);
    response->set_bytes_received(0);
    return ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                          "Migration not found: " + migration_id);
  }
  
  // Hold task lock across entire streaming loop
  std::lock_guard<std::mutex> lock(task->mutex);
  
  if (!CanSyncData(task->status)) {
    response->set_success(false);
    response->set_error_msg("Migration not in syncable state: " +
                            std::to_string(task->status));
    response->set_bytes_received(0);
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION,
                          "Migration not in syncable state");
  }
  
  // Process first chunk
  if (request.partition_id() != partition_id) {
    response->set_success(false);
    response->set_error_msg("Partition ID mismatch in data stream");
    response->set_bytes_received(0);
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Partition ID mismatch");
  }
  
  if (options_.verify_checksum && request.checksum() != 0) {
    const uint8_t* data = reinterpret_cast<const uint8_t*>(request.data().data());
    uint64_t computed = ComputeChecksum(data, request.data().size());
    if (computed != request.checksum()) {
      response->set_success(false);
      response->set_error_msg("Checksum mismatch at offset 0");
      response->set_bytes_received(0);
      return ::grpc::Status(::grpc::StatusCode::DATA_LOSS, "Checksum mismatch");
    }
  }
  
  if (task->data_buffer.size() + request.data().size() > options_.max_buffer_size) {
    response->set_success(false);
    response->set_error_msg("Buffer size limit exceeded");
    response->set_bytes_received(0);
    return ::grpc::Status(::grpc::StatusCode::RESOURCE_EXHAUSTED,
                          "Buffer size limit exceeded");
  }
  
  size_t current_size = task->data_buffer.size();
  task->data_buffer.resize(current_size + request.data().size());
  std::memcpy(task->data_buffer.data() + current_size,
              request.data().data(),
              request.data().size());
  task->current_offset = request.offset() + request.data().size();
  task->bytes_transferred += request.data().size();
  total_bytes_received += request.data().size();
  if (task->bytes_total > 0) {
    task->progress_percent = static_cast<uint32_t>(
        (task->bytes_transferred * 100) / task->bytes_total);
  }
  
  // Continue reading remaining chunks while holding the lock
  while (reader->Read(&request)) {
    // Verify partition ID consistency
    if (request.partition_id() != partition_id) {
      response->set_success(false);
      response->set_error_msg("Partition ID mismatch in data stream");
      response->set_bytes_received(total_bytes_received);
      return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                            "Partition ID mismatch");
    }
    
    // Check task state before each chunk
    if (!CanSyncData(task->status)) {
      response->set_success(false);
      response->set_error_msg("Migration not in syncable state: " +
                              std::to_string(task->status));
      response->set_bytes_received(total_bytes_received);
      return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION,
                            "Migration not in syncable state");
    }
    
    // Verify checksum if enabled
    if (options_.verify_checksum && request.checksum() != 0) {
      const uint8_t* data = reinterpret_cast<const uint8_t*>(request.data().data());
      uint64_t computed = ComputeChecksum(data, request.data().size());
      if (computed != request.checksum()) {
        response->set_success(false);
        response->set_error_msg("Checksum mismatch at offset " +
                                std::to_string(request.offset()));
        response->set_bytes_received(total_bytes_received);
        return ::grpc::Status(::grpc::StatusCode::DATA_LOSS, "Checksum mismatch");
      }
    }
    
    // Check buffer size limit
    if (task->data_buffer.size() + request.data().size() > options_.max_buffer_size) {
      response->set_success(false);
      response->set_error_msg("Buffer size limit exceeded");
      response->set_bytes_received(total_bytes_received);
      return ::grpc::Status(::grpc::StatusCode::RESOURCE_EXHAUSTED,
                            "Buffer size limit exceeded");
    }
    
    // Append data to buffer
    size_t chunk_size = task->data_buffer.size();
    task->data_buffer.resize(chunk_size + request.data().size());
    std::memcpy(task->data_buffer.data() + chunk_size,
                request.data().data(),
                request.data().size());
    
    // Update offset and byte count
    task->current_offset = request.offset() + request.data().size();
    task->bytes_transferred += request.data().size();
    total_bytes_received += request.data().size();
    
    // Update progress
    if (task->bytes_total > 0) {
      task->progress_percent = static_cast<uint32_t>(
          (task->bytes_transferred * 100) / task->bytes_total);
    }
  }
  
  // Update global stats
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    stats_.total_bytes_transferred += total_bytes_received;
  }
  
  // Update task status to CATCHING_UP after initial sync
  if (task->status == cedar::migration::SYNCING) {
    task->status = cedar::migration::CATCHING_UP;
  }
  
  response->set_success(true);
  response->set_bytes_received(total_bytes_received);
  
  return ::grpc::Status::OK;
}

::grpc::Status PartitionMigrationServiceImpl::ReplicateWALEntry(
    ::grpc::ServerContext* context,
    const ::cedar::migration::ReplicateWALEntryRequest* request,
    ::cedar::migration::ReplicateWALEntryResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  // TODO: Apply the WAL entry to the target partition storage.
  // For now, acknowledge receipt so the source can continue catch-up.
  response->set_success(true);
  return ::grpc::Status::OK;
}

::grpc::Status PartitionMigrationServiceImpl::FinalizeMigration(
    ::grpc::ServerContext* context,
    const ::cedar::migration::FinalizeMigrationRequest* request,
    ::cedar::migration::FinalizeMigrationResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  const std::string& migration_id = request->migration_id();
  
  // Find the task
  ServiceMigrationTask* task = FindTask(migration_id);
  if (!task) {
    response->set_success(false);
    response->set_error_msg("Migration not found: " + migration_id);
    response->set_final_status(cedar::migration::FAILED);
    return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, 
                          "Migration not found: " + migration_id);
  }
  
  // Lock the task for the finalization
  std::lock_guard<std::mutex> lock(task->mutex);
  
  // Check if we can finalize
  if (IsTerminalStatus(task->status)) {
    response->set_success(false);
    response->set_error_msg("Migration already in terminal state: " + 
                            std::to_string(task->status));
    response->set_final_status(task->status);
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION,
                          "Migration already finalized");
  }
  
  if (request->commit()) {
    // Commit the migration
    task->status = cedar::migration::FINALIZING;
    
    // Write buffered data to storage before committing
    if (!task->data_buffer.empty() && partition_migrator_ != nullptr) {
      TempDirGuard temp_guard("/tmp/cedar_migration_" + task->migration_id);
      std::string temp_file = temp_guard.path() + "/snapshot.bin";
      std::ofstream ofs(temp_file, std::ios::binary);
      if (ofs) {
        ofs.write(reinterpret_cast<const char*>(task->data_buffer.data()),
                  task->data_buffer.size());
        ofs.close();

        auto load_status = partition_migrator_->LoadSnapshotForMigration(
            task->internal_id, temp_guard.path());
        if (!load_status.ok()) {
          task->status = cedar::migration::FAILED;
          task->error_msg = "Snapshot load failed: " + load_status.ToString();
          {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            ++stats_.total_migrations_failed;
            if (stats_.active_migrations > 0) --stats_.active_migrations;
          }
          response->set_success(false);
          response->set_error_msg(task->error_msg);
          response->set_final_status(cedar::migration::FAILED);
          return ::grpc::Status(::grpc::StatusCode::INTERNAL, task->error_msg);
        }
      }
    }
    
    // Apply the actual data changes through the partition migrator
    if (partition_migrator_ != nullptr && task->internal_id > 0) {
      auto migrator_status = partition_migrator_->CommitMigration(task->internal_id);
      if (!migrator_status.ok()) {
        task->status = cedar::migration::FAILED;
        task->error_msg = "Migrator commit failed: " + migrator_status.ToString();
        {
          std::lock_guard<std::mutex> stats_lock(stats_mutex_);
          ++stats_.total_migrations_failed;
          if (stats_.active_migrations > 0) --stats_.active_migrations;
        }
        response->set_success(false);
        response->set_error_msg(task->error_msg);
        response->set_final_status(cedar::migration::FAILED);
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, task->error_msg);
      }
    } else {
      task->status = cedar::migration::FAILED;
      task->error_msg = "Partition migrator not available; cannot finalize migration without data movement";
      {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        ++stats_.total_migrations_failed;
        if (stats_.active_migrations > 0) --stats_.active_migrations;
      }
      response->set_success(false);
      response->set_error_msg(task->error_msg);
      response->set_final_status(cedar::migration::FAILED);
      return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, task->error_msg);
    }
    
    // Mark as completed
    task->status = cedar::migration::COMPLETED;
    task->completed_at = std::chrono::system_clock::now();
    task->progress_percent = 100;
    
    // Update statistics
    {
      std::lock_guard<std::mutex> stats_lock(stats_mutex_);
      ++stats_.total_migrations_completed;
      if (stats_.active_migrations > 0) --stats_.active_migrations;
    }
    
    response->set_success(true);
    response->set_final_status(cedar::migration::COMPLETED);
    
  } else {
    // Rollback the migration
    task->status = cedar::migration::ROLLED_BACK;
    task->completed_at = std::chrono::system_clock::now();
    task->error_msg = "Migration rolled back by user request";
    
    // Clear the data buffer
    task->data_buffer.clear();
    task->data_buffer.shrink_to_fit();
    
    // Update statistics
    {
      std::lock_guard<std::mutex> stats_lock(stats_mutex_);
      ++stats_.total_migrations_rolled_back;
      if (stats_.active_migrations > 0) --stats_.active_migrations;
    }
    
    response->set_success(true);
    response->set_final_status(cedar::migration::ROLLED_BACK);
  }
  
  return ::grpc::Status::OK;
}

::grpc::Status PartitionMigrationServiceImpl::GetMigrationStatus(
    ::grpc::ServerContext* context,
    const ::cedar::migration::GetMigrationStatusRequest* request,
    ::cedar::migration::GetMigrationStatusResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  const std::string& migration_id = request->migration_id();
  
  // Find the task
  const ServiceMigrationTask* task = FindTask(migration_id);
  if (!task) {
    response->set_success(false);
    response->set_error_msg("Migration not found: " + migration_id);
    return ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                          "Migration not found: " + migration_id);
  }
  
  // Read task status (thread-safe)
  {
    std::lock_guard<std::mutex> lock(task->mutex);
    response->set_success(true);
    response->set_status(task->status);
    response->set_bytes_transferred(task->bytes_transferred);
    response->set_bytes_total(task->bytes_total);
    response->set_progress_percent(task->progress_percent);
    if (!task->error_msg.empty()) {
      response->set_error_msg(task->error_msg);
    }
  }
  
  return ::grpc::Status::OK;
}

::grpc::Status PartitionMigrationServiceImpl::FetchChecksum(
    ::grpc::ServerContext* context,
    const ::cedar::migration::FetchChecksumRequest* request,
    ::cedar::migration::FetchChecksumResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  if (partition_migrator_ == nullptr) {
    response->set_success(false);
    response->set_error_msg("Partition migrator not available");
    return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE,
                          "Partition migrator not available");
  }

  std::string checksum;
  auto status = partition_migrator_->CalculateChecksum(
      static_cast<dtx::PartitionID>(request->partition_id()), &checksum);
  if (!status.ok()) {
    response->set_success(false);
    response->set_error_msg(status.ToString());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, status.ToString());
  }

  response->set_checksum(checksum);
  response->set_success(true);
  return ::grpc::Status::OK;
}

// =============================================================================
// Administrative Methods
// =============================================================================

bool PartitionMigrationServiceImpl::CancelMigration(
    const std::string& migration_id, 
    const std::string& reason) {
  ServiceMigrationTask* task = FindTask(migration_id);
  if (!task) {
    return false;
  }
  
  std::lock_guard<std::mutex> lock(task->mutex);
  
  // Can only cancel non-terminal migrations
  if (IsTerminalStatus(task->status)) {
    return false;
  }
  
  task->status = cedar::migration::ROLLED_BACK;
  task->completed_at = std::chrono::system_clock::now();
  task->error_msg = "Cancelled: " + reason;
  
  // Clear data buffer
  task->data_buffer.clear();
  task->data_buffer.shrink_to_fit();
  
  // Update statistics
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    ++stats_.total_migrations_rolled_back;
    if (stats_.active_migrations > 0) --stats_.active_migrations;
  }
  
  return true;
}

std::vector<std::string> PartitionMigrationServiceImpl::GetActiveMigrationIds() const {
  std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
  std::vector<std::string> result;
  
  for (const auto& [id, task] : tasks_) {
    if (!IsTerminalStatus(task->status)) {
      result.push_back(id);
    }
  }
  
  return result;
}

size_t PartitionMigrationServiceImpl::CleanupOldMigrations(std::chrono::seconds max_age) {
  auto cutoff = std::chrono::system_clock::now() - max_age;
  std::vector<std::string> to_remove;

  {
    std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
    for (const auto& [id, task] : tasks_) {
      if (IsTerminalStatus(task->status) && task->completed_at < cutoff) {
        to_remove.push_back(id);
      }
    }
  }

  {
    std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
    for (const auto& id : to_remove) {
      auto it = tasks_.find(id);
      if (it != tasks_.end() && IsTerminalStatus(it->second->status) &&
          it->second->completed_at < cutoff) {
        tasks_.erase(it);
      }
    }
  }

  return to_remove.size();
}

PartitionMigrationServiceImpl::Stats 
PartitionMigrationServiceImpl::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

// =============================================================================
// Private Helper Methods
// =============================================================================

std::string PartitionMigrationServiceImpl::GenerateMigrationId() {
  return "mig-" + GenerateUuid();
}

ServiceMigrationTask* PartitionMigrationServiceImpl::FindTask(
    const std::string& migration_id) {
  std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it != tasks_.end()) {
    return it->second.get();
  }
  return nullptr;
}

const ServiceMigrationTask* PartitionMigrationServiceImpl::FindTask(
    const std::string& migration_id) const {
  std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
  auto it = tasks_.find(migration_id);
  if (it != tasks_.end()) {
    return it->second.get();
  }
  return nullptr;
}

bool PartitionMigrationServiceImpl::CanTransition(
    cedar::migration::MigrationStatus from,
    cedar::migration::MigrationStatus to) const {
  // Define valid state transitions
  switch (from) {
    case cedar::migration::PENDING:
      return to == cedar::migration::PREPARING ||
             to == cedar::migration::ROLLED_BACK;
    case cedar::migration::PREPARING:
      return to == cedar::migration::SYNCING ||
             to == cedar::migration::FAILED ||
             to == cedar::migration::ROLLED_BACK;
    case cedar::migration::SYNCING:
      return to == cedar::migration::CATCHING_UP ||
             to == cedar::migration::FAILED ||
             to == cedar::migration::ROLLED_BACK;
    case cedar::migration::CATCHING_UP:
      return to == cedar::migration::FINALIZING ||
             to == cedar::migration::SYNCING ||  // More data to sync
             to == cedar::migration::FAILED ||
             to == cedar::migration::ROLLED_BACK;
    case cedar::migration::FINALIZING:
      return to == cedar::migration::COMPLETED ||
             to == cedar::migration::FAILED;
    case cedar::migration::COMPLETED:
    case cedar::migration::FAILED:
    case cedar::migration::ROLLED_BACK:
      return false;  // Terminal states
    default:
      return false;
  }
}

bool PartitionMigrationServiceImpl::UpdateTaskStatus(
    ServiceMigrationTask* task,
    cedar::migration::MigrationStatus new_status,
    const std::string& error_msg) {
  if (!task) return false;
  
  std::lock_guard<std::mutex> lock(task->mutex);
  
  if (!CanTransition(task->status, new_status)) {
    return false;
  }
  
  task->status = new_status;
  if (!error_msg.empty()) {
    task->error_msg = error_msg;
  }
  
  if (new_status == cedar::migration::FAILED) {
    task->completed_at = std::chrono::system_clock::now();
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    ++stats_.total_migrations_failed;
    if (stats_.active_migrations > 0) --stats_.active_migrations;
  }
  
  return true;
}

cedar::migration::MigrationStatus PartitionMigrationServiceImpl::StateToProtoStatus(
    dtx::storage::MigrationState state) const {
  switch (state) {
    case dtx::storage::MigrationState::kPending:
      return cedar::migration::PENDING;
    case dtx::storage::MigrationState::kPreparing:
      return cedar::migration::PREPARING;
    case dtx::storage::MigrationState::kCopying:
      return cedar::migration::SYNCING;
    case dtx::storage::MigrationState::kCatchingUp:
      return cedar::migration::CATCHING_UP;
    case dtx::storage::MigrationState::kSwitching:
    case dtx::storage::MigrationState::kVerifying:
    case dtx::storage::MigrationState::kCompleting:
      return cedar::migration::FINALIZING;
    case dtx::storage::MigrationState::kCompleted:
      return cedar::migration::COMPLETED;
    case dtx::storage::MigrationState::kFailed:
      return cedar::migration::FAILED;
    case dtx::storage::MigrationState::kRolledBack:
      return cedar::migration::ROLLED_BACK;
    default:
      return cedar::migration::PENDING;
  }
}

bool PartitionMigrationServiceImpl::ValidateStartRequest(
    const ::cedar::migration::StartMigrationRequest* request,
    std::string* error_msg) const {
  if (request->partition_id() == 0) {
    *error_msg = "Partition ID cannot be 0";
    return false;
  }
  
  if (request->source_node().empty()) {
    *error_msg = "Source node cannot be empty";
    return false;
  }
  
  if (request->target_node().empty()) {
    *error_msg = "Target node cannot be empty";
    return false;
  }
  
  if (request->target_address().empty()) {
    *error_msg = "Target address cannot be empty";
    return false;
  }
  
  if (request->source_node() == request->target_node()) {
    *error_msg = "Source and target nodes cannot be the same";
    return false;
  }
  
  return true;
}

bool PartitionMigrationServiceImpl::VerifyChecksum(
    const uint8_t* data, 
    size_t length, 
    uint64_t expected_checksum) const {
  if (!options_.verify_checksum) {
    return true;
  }
  return ComputeChecksum(data, length) == expected_checksum;
}

uint64_t PartitionMigrationServiceImpl::CalculateChecksum(
    const uint8_t* data, 
    size_t length) const {
  return ComputeChecksum(data, length);
}

}  // namespace service
}  // namespace cedar
