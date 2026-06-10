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
// Partition Migration Service - gRPC Service Implementation
// Online partition migration for load balancing and recovery
// =============================================================================

#ifndef CEDAR_SERVICE_PARTITION_MIGRATION_SERVICE_H_
#define CEDAR_SERVICE_PARTITION_MIGRATION_SERVICE_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "migration_service.grpc.pb.h"

namespace cedar {
namespace dtx {
namespace storage {
class PartitionMigrator;
struct MigrationTask;
enum class MigrationState : uint8_t;
}  // namespace storage
}  // namespace dtx

namespace service {

// =============================================================================
// Migration Task Info (Internal Tracking)
// =============================================================================

struct ServiceMigrationTask {
  std::string migration_id;          // UUID string format
  uint64_t internal_id = 0;          // Internal numeric ID
  uint32_t partition_id = 0;
  std::string source_node;
  std::string target_node;
  std::string target_address;
  uint64_t estimated_data_size = 0;
  
  // Status
  cedar::migration::MigrationStatus status = cedar::migration::PENDING;
  
  // Progress tracking
  uint64_t bytes_transferred = 0;
  uint64_t bytes_total = 0;
  uint32_t progress_percent = 0;
  
  // Data buffer for streaming sync
  std::vector<uint8_t> data_buffer;
  uint64_t current_offset = 0;
  
  // Timing
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point started_at;
  std::chrono::system_clock::time_point completed_at;
  
  // Error handling
  std::string error_msg;
  uint32_t retry_count = 0;
  
  // Thread safety
  mutable std::mutex mutex;
  
  ServiceMigrationTask() : created_at(std::chrono::system_clock::now()) {}
};

// =============================================================================
// Partition Migration Service Implementation
// =============================================================================

class PartitionMigrationServiceImpl final 
    : public cedar::migration::PartitionMigrationService::Service {
 public:
  struct Options {
    // Maximum concurrent migrations
    uint32_t max_concurrent_migrations;
    
    // Maximum data buffer size per migration
    uint64_t max_buffer_size;
    
    // Default data sync chunk size
    uint64_t sync_chunk_size;
    
    // Enable checksum verification
    bool verify_checksum;
    
    // Migration timeout
    std::chrono::minutes migration_timeout;
    
    // Constructor with defaults
    Options()
        : max_concurrent_migrations(10),
          max_buffer_size(256ULL * 1024 * 1024),
          sync_chunk_size(64ULL * 1024 * 1024),
          verify_checksum(true),
          migration_timeout(30) {}
  };
  
  explicit PartitionMigrationServiceImpl(const Options& options = Options());
  ~PartitionMigrationServiceImpl() override;
  
  // Disable copy and move
  PartitionMigrationServiceImpl(const PartitionMigrationServiceImpl&) = delete;
  PartitionMigrationServiceImpl& operator=(const PartitionMigrationServiceImpl&) = delete;
  
  // Initialize with partition migrator (optional - for integration with existing migrator)
  void SetPartitionMigrator(dtx::storage::PartitionMigrator* migrator);
  
  // gRPC Service Methods
  
  // Start a new partition migration
  ::grpc::Status StartMigration(
      ::grpc::ServerContext* context,
      const ::cedar::migration::StartMigrationRequest* request,
      ::cedar::migration::StartMigrationResponse* response) override;
  
  // Stream data from source to target
  ::grpc::Status SyncData(
      ::grpc::ServerContext* context,
      ::grpc::ServerReader<::cedar::migration::SyncDataRequest>* reader,
      ::cedar::migration::SyncDataResponse* response) override;
  
  // Replicate a single WAL entry during catch-up phase
  ::grpc::Status ReplicateWALEntry(
      ::grpc::ServerContext* context,
      const ::cedar::migration::ReplicateWALEntryRequest* request,
      ::cedar::migration::ReplicateWALEntryResponse* response) override;
  
  // Replicate a batch of WAL entries during catch-up phase
  ::grpc::Status ReplicateWALBatch(
      ::grpc::ServerContext* context,
      const ::cedar::migration::ReplicateWALBatchRequest* request,
      ::cedar::migration::ReplicateWALBatchResponse* response) override;
  
  // Finalize migration (commit or rollback)
  ::grpc::Status FinalizeMigration(
      ::grpc::ServerContext* context,
      const ::cedar::migration::FinalizeMigrationRequest* request,
      ::cedar::migration::FinalizeMigrationResponse* response) override;
  
  // Get current migration status
  ::grpc::Status GetMigrationStatus(
      ::grpc::ServerContext* context,
      const ::cedar::migration::GetMigrationStatusRequest* request,
      ::cedar::migration::GetMigrationStatusResponse* response) override;
  
  // Fetch partition checksum for verification
  ::grpc::Status FetchChecksum(
      ::grpc::ServerContext* context,
      const ::cedar::migration::FetchChecksumRequest* request,
      ::cedar::migration::FetchChecksumResponse* response) override;
  
  // Administrative methods
  
  // Cancel a migration
  bool CancelMigration(const std::string& migration_id, const std::string& reason);
  
  // Get all active migrations
  std::vector<std::string> GetActiveMigrationIds() const;
  
  // Clean up completed/failed migrations older than specified duration
  size_t CleanupOldMigrations(std::chrono::milliseconds max_age);
  
  // Get service statistics
  struct Stats {
    uint64_t total_migrations_started = 0;
    uint64_t total_migrations_completed = 0;
    uint64_t total_migrations_failed = 0;
    uint64_t total_migrations_rolled_back = 0;
    uint64_t total_bytes_transferred = 0;
    uint64_t active_migrations = 0;
  };
  Stats GetStats() const;
  
 private:
  Options options_;
  
  // Optional reference to existing migrator
  dtx::storage::PartitionMigrator* partition_migrator_ = nullptr;
  
  // Task management
  mutable std::shared_mutex tasks_mutex_;
  std::unordered_map<std::string, std::unique_ptr<ServiceMigrationTask>> tasks_;
  
  // Statistics
  mutable std::mutex stats_mutex_;
  Stats stats_;
  
  // Internal ID counter
  std::atomic<uint64_t> next_internal_id_{1};
  
  // Helper methods
  std::string GenerateMigrationId();
  ServiceMigrationTask* FindTask(const std::string& migration_id);
  const ServiceMigrationTask* FindTask(const std::string& migration_id) const;
  
  // State transition helpers
  bool CanTransition(cedar::migration::MigrationStatus from,
                     cedar::migration::MigrationStatus to) const;
  bool UpdateTaskStatus(ServiceMigrationTask* task,
                        cedar::migration::MigrationStatus new_status,
                        const std::string& error_msg = "");
  
  // Convert between proto status and internal state
  cedar::migration::MigrationStatus StateToProtoStatus(
      dtx::storage::MigrationState state) const;
  
  // Validation helpers
  bool ValidateStartRequest(const ::cedar::migration::StartMigrationRequest* request,
                            std::string* error_msg) const;
  bool VerifyChecksum(const uint8_t* data, size_t length, uint64_t expected_checksum) const;
  uint64_t CalculateChecksum(const uint8_t* data, size_t length) const;
};

}  // namespace service
}  // namespace cedar

#endif  // CEDAR_SERVICE_PARTITION_MIGRATION_SERVICE_H_
