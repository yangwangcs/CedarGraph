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

#include "cedar/dtx/storage/storage_service.h"
#include "cedar/dtx/chaos_testing.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// PartitionStorage Stub
// =============================================================================

PartitionStorage::PartitionStorage(PartitionID id, const std::string& space_name)
    : partition_id_(id), space_name_(space_name) {}

PartitionStorage::~PartitionStorage() = default;

Status PartitionStorage::Initialize(const std::string& data_dir) {
  data_dir_ = data_dir;
  running_.store(true);
  std::filesystem::create_directories(data_dir_);
  return Status::OK();
}

Status PartitionStorage::Shutdown() {
  running_.store(false);
  return Status::OK();
}

uint64_t PartitionStorage::GetDataSize() const { return 0; }
uint64_t PartitionStorage::GetKeyCount() const { return 0; }

// =============================================================================
// StorageService Implementation
// =============================================================================

StorageService::StorageService(const StorageNodeConfig& config)
    : config_(config), recovery_enabled_(config.enable_auto_recovery) {}

StorageService::~StorageService() {
  Shutdown();
}

Status StorageService::Initialize() {
  std::filesystem::create_directories(config_.data_dir);

  recovery_manager_ = std::make_unique<AutomatedRecoveryManager>();
  auto init_status = recovery_manager_->Initialize(peer_addresses_);
  if (!init_status.ok()) {
    std::cerr << "[StorageService] AutomatedRecoveryManager init failed: "
              << init_status.ToString() << std::endl;
  }

  return Status::OK();
}

Status StorageService::Start() {
  running_.store(true);

  if (recovery_manager_) {
    recovery_manager_->Start();
  }

  heartbeat_thread_ = std::thread([this]() {
    HeartbeatLoop();
  });

  stats_thread_ = std::thread([this]() {
    StatsCollectionLoop();
  });

  if (config_.enable_auto_recovery) {
    health_check_thread_ = std::thread([this]() {
      HealthCheckLoop();
    });
    recovery_worker_thread_ = std::thread([this]() {
      RecoveryLoop();
    });
  }

  return Status::OK();
}

Status StorageService::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
  if (stats_thread_.joinable()) {
    stats_thread_.join();
  }
  if (health_check_thread_.joinable()) {
    health_check_thread_.join();
  }
  if (recovery_worker_thread_.joinable()) {
    recovery_worker_thread_.join();
  }

  if (recovery_manager_) {
    recovery_manager_->Stop();
  }

  std::unique_lock<std::shared_mutex> lock(partitions_mutex_);
  partitions_.clear();

  return Status::OK();
}

Status StorageService::CreatePartition(const std::string& space_name,
                                       PartitionID partition_id,
                                       bool as_leader) {
  std::unique_lock<std::shared_mutex> lock(partitions_mutex_);
  std::string key = space_name + ":" + std::to_string(partition_id);
  if (partitions_.find(key) != partitions_.end()) {
    return Status::InvalidArgument("Partition already exists: " + key);
  }
  auto ps = std::make_unique<PartitionStorage>(partition_id, space_name);
  auto s = ps->Initialize(config_.data_dir + "/" + space_name + "/" +
                          std::to_string(partition_id));
  if (!s.ok()) return s;
  ps->SetLeader(as_leader);
  partitions_[key] = std::move(ps);
  return Status::OK();
}

Status StorageService::DropPartition(const std::string& space_name,
                                     PartitionID partition_id) {
  std::unique_lock<std::shared_mutex> lock(partitions_mutex_);
  std::string key = space_name + ":" + std::to_string(partition_id);
  auto it = partitions_.find(key);
  if (it == partitions_.end()) {
    return Status::NotFound("Partition not found: " + key);
  }
  it->second->Shutdown();
  partitions_.erase(it);
  return Status::OK();
}

Status StorageService::SetPartitionLeader(const std::string& space_name,
                                          PartitionID partition_id,
                                          bool is_leader) {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  std::string key = space_name + ":" + std::to_string(partition_id);
  auto it = partitions_.find(key);
  if (it == partitions_.end()) {
    return Status::NotFound("Partition not found: " + key);
  }
  it->second->SetLeader(is_leader);
  return Status::OK();
}

StatusOr<PartitionStorage*> StorageService::GetPartition(
    const std::string& space_name, PartitionID partition_id) {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  std::string key = space_name + ":" + std::to_string(partition_id);
  auto it = partitions_.find(key);
  if (it == partitions_.end()) {
    return Status::NotFound("Partition not found: " + key);
  }
  return it->second.get();
}

std::vector<std::pair<std::string, PartitionID>>
StorageService::ListPartitions() const {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  std::vector<std::pair<std::string, PartitionID>> result;
  result.reserve(partitions_.size());
  for (const auto& [key, ps] : partitions_) {
    result.emplace_back(ps->GetSpaceName(), ps->GetPartitionId());
  }
  return result;
}

Status StorageService::SendHeartbeat() {
  // TODO: Implement gRPC heartbeat to MetaD
  return Status::OK();
}

StorageService::Stats StorageService::GetStats() const {
  std::shared_lock<std::shared_mutex> lock(partitions_mutex_);
  Stats stats;
  stats.total_partitions = partitions_.size();
  for (const auto& [key, ps] : partitions_) {
    if (ps->IsLeader()) {
      stats.leader_partitions++;
    } else {
      stats.follower_partitions++;
    }
    stats.total_data_size += ps->GetDataSize();
    stats.total_keys += ps->GetKeyCount();
  }
  return stats;
}

void StorageService::EnableAutoRecovery(bool enabled) {
  recovery_enabled_.store(enabled);
  if (recovery_manager_) {
    recovery_manager_->SetAutoRecoveryEnabled(enabled);
  }
}

bool StorageService::IsAutoRecoveryEnabled() const {
  return recovery_enabled_.load();
}

Status StorageService::TriggerRecovery(
    AutomatedRecoveryManager::FailureType type,
    const std::string& details) {
  if (!recovery_manager_) {
    return Status::NotFound("Recovery manager not initialized");
  }
  AutomatedRecoveryManager::FailureEvent event;
  event.type = type;
  event.node_id = config_.node_id;
  event.timestamp = std::chrono::system_clock::now();
  event.details = details;
  event.severity = 3;  // medium severity
  recovery_manager_->ReportFailure(event);
  return Status::OK();
}

std::vector<std::pair<AutomatedRecoveryManager::FailureEvent, Status>>
StorageService::GetRecoveryHistory() const {
  if (!recovery_manager_) {
    return {};
  }
  return recovery_manager_->GetRecoveryHistory();
}

// =============================================================================
// Background Loops
// =============================================================================

Status StorageService::ConnectToMetaD() {
  // TODO: Implement MetaD connection
  return Status::OK();
}

void StorageService::HeartbeatLoop() {
  while (running_.load()) {
    auto status = SendHeartbeat();
    if (!status.ok()) {
      std::cerr << "[StorageService] Heartbeat failed: " << status.ToString()
                << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

void StorageService::StatsCollectionLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

void StorageService::HealthCheckLoop() {
  while (running_.load() && recovery_enabled_.load()) {
    auto disk = CheckDiskHealth();
    auto mem = CheckMemoryHealth();
    auto net = CheckNetworkHealth();
    auto raft = CheckRaftHealth();

    (void)disk;
    (void)mem;
    (void)net;
    (void)raft;

    std::this_thread::sleep_for(config_.health_check_interval);
  }
}

void StorageService::RecoveryLoop() {
  while (running_.load() && recovery_enabled_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// =============================================================================
// Health Checks (Stubs)
// =============================================================================

Status StorageService::CheckDiskHealth() {
  return Status::OK();
}

Status StorageService::CheckMemoryHealth() {
  return Status::OK();
}

Status StorageService::CheckNetworkHealth() {
  return Status::OK();
}

Status StorageService::CheckRaftHealth() {
  return Status::OK();
}

// =============================================================================
// DataRouter Stub
// =============================================================================

PartitionID DataRouter::RouteKey(const CedarKey& key, uint32_t num_partitions) {
  return key.entity_id() % num_partitions;
}

StatusOr<NodeID> DataRouter::GetPartitionLeader(
    const std::string& space_name,
    PartitionID partition_id,
    const std::vector<std::pair<uint32_t, std::string>>& metad_endpoints) {
  (void)space_name;
  (void)partition_id;
  (void)metad_endpoints;
  return Status::NotFound("Leader lookup not implemented");
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
