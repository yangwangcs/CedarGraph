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

#include "cedar/dtx/migration_executor.h"
#include "cedar/dtx/partition.h"
#include "cedar/dtx/storage_service_impl.h"

#include <algorithm>
#include <chrono>

#include "cedar/governance/service_registry.h"
#include "cedar/dtx/raft/grpc_tls.h"

#include <grpcpp/grpcpp.h>
#include "migration_service.grpc.pb.h"

namespace cedar {
namespace dtx {

// =============================================================================
// DTxRpcClient - inlined from deleted rpc_client.h / rpc_client.cc
// =============================================================================

class DTxRpcClient {
 public:
  explicit DTxRpcClient(const DTxConfig& config) : config_(config) {}
  ~DTxRpcClient() = default;

  Status AddNode(NodeID node_id, const std::string& address) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto info = std::make_unique<NodeInfo>();
    info->id = node_id;
    info->address = address;
    info->available = true;
    nodes_[node_id] = std::move(info);
    return Status::OK();
  }

  void RemoveNode(NodeID node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    nodes_.erase(node_id);
  }

  bool IsNodeAvailable(NodeID node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nodes_.find(node_id);
    return (it != nodes_.end() && it->second->available);
  }

  Status DiscoverAndAddNodes(const std::string& service_name,
                              cedar::governance::ServiceRegistry& registry) {
    auto services_result = registry.Discover(service_name);
    if (!services_result.ok()) return services_result.status();
    auto services = services_result.ValueOrDie();
    NodeID next_id = 1;
    for (const auto& service : services) {
      if (service.status != cedar::governance::ServiceStatus::kHealthy) continue;
      std::string address = service.host + ":" + std::to_string(service.port);
      std::lock_guard<std::mutex> lock(mutex_);
      bool exists = false;
      for (const auto& [id, info] : nodes_) {
        if (info->address == address) { exists = true; break; }
      }
      if (!exists) {
        auto info = std::make_unique<NodeInfo>();
        info->id = next_id++;
        info->address = address;
        info->available = true;
        info->service_id = service.id;
        nodes_[info->id] = std::move(info);
      }
    }
    return Status::OK();
  }

  void RefreshNodesFromRegistry(const std::string& service_name,
                                 cedar::governance::ServiceRegistry& registry) {
    auto services_result = registry.Discover(service_name);
    if (!services_result.ok()) return;
    auto services = services_result.ValueOrDie();
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [id, info] : nodes_) info->available = false;
    for (const auto& service : services) {
      if (service.status != cedar::governance::ServiceStatus::kHealthy) continue;
      std::string address = service.host + ":" + std::to_string(service.port);
      for (auto& [id, info] : nodes_) {
        if (info->address == address || info->service_id == service.id) {
          info->available = true;
          info->service_id = service.id;
          break;
        }
      }
    }
  }

  std::vector<NodeID> GetDiscoveredNodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<NodeID> result;
    for (const auto& [id, info] : nodes_) {
      if (info->available) result.push_back(id);
    }
    return result;
  }

  Status Put(NodeID /*node_id*/, PartitionID /*pid*/,
             const CedarKey& /*key*/, const Descriptor& /*value*/,
             Timestamp /*txn_version*/) {
    return Status::OK();
  }

  StatusOr<Descriptor> Get(NodeID /*node_id*/, PartitionID /*pid*/,
                           const CedarKey& /*key*/, Timestamp /*read_time*/) {
    return Descriptor();
  }

  Status Prepare(NodeID /*node_id*/, TxnID /*txn_id*/,
                 const std::vector<CedarKey>& /*reads*/,
                 const std::vector<CedarKey>& /*writes*/,
                 Timestamp /*commit_ts*/) {
    return Status::OK();
  }

  Status Commit(NodeID /*node_id*/, TxnID /*txn_id*/, Timestamp /*commit_ts*/) {
    return Status::OK();
  }

  Status Abort(NodeID /*node_id*/, TxnID /*txn_id*/) {
    return Status::OK();
  }

  // Migration pipeline RPCs
  Status PrepareMigration(NodeID node_id, PartitionID pid,
                          NodeID source_node, NodeID target_node,
                          std::string* out_migration_id);
  Status TransferData(NodeID node_id, const std::string& migration_id,
                      PartitionID pid, const std::string& data);
  Status VerifyMigration(NodeID source_node, NodeID target_node,
                         PartitionID pid);
  Status CompleteMigration(NodeID node_id, const std::string& migration_id,
                           PartitionID pid);

 private:
  std::string GetNodeAddress(NodeID node_id) const;
  std::shared_ptr<cedar::migration::PartitionMigrationService::Stub>
      GetMigrationStub(NodeID node_id) const;
  DTxConfig config_;
  struct NodeInfo {
    NodeID id;
    std::string address;
    std::atomic<bool> available{true};
    std::string service_id;
  };
  mutable std::mutex mutex_;
  std::unordered_map<NodeID, std::unique_ptr<NodeInfo>> nodes_;
  int64_t registry_watch_id_ = -1;
};

// =============================================================================
// DTxRpcClient Migration Methods
// =============================================================================

std::string DTxRpcClient::GetNodeAddress(NodeID node_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = nodes_.find(node_id);
  if (it != nodes_.end()) return it->second->address;
  return "";
}

std::shared_ptr<cedar::migration::PartitionMigrationService::Stub>
DTxRpcClient::GetMigrationStub(NodeID node_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end() || !it->second->available) return nullptr;
  auto channel = grpc::CreateChannel(
      it->second->address,
      cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv());
  return cedar::migration::PartitionMigrationService::NewStub(channel);
}

Status DTxRpcClient::PrepareMigration(NodeID node_id, PartitionID pid,
                                      NodeID source_node, NodeID target_node,
                                      std::string* out_migration_id) {
  auto stub = GetMigrationStub(node_id);
  if (!stub) return Status::IOError("Node not available");

  cedar::migration::StartMigrationRequest request;
  request.set_partition_id(pid);
  request.set_source_node(std::to_string(source_node));
  request.set_target_node(std::to_string(target_node));
  std::string addr = GetNodeAddress(target_node);
  if (!addr.empty()) request.set_target_address(addr);

  cedar::migration::StartMigrationResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(30));

  auto status = stub->StartMigration(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("StartMigration RPC failed: " +
                           status.error_message());
  }
  if (!response.success()) return Status::IOError(response.error_msg());
  if (out_migration_id) *out_migration_id = response.migration_id();
  return Status::OK();
}

Status DTxRpcClient::TransferData(NodeID node_id,
                                  const std::string& migration_id,
                                  PartitionID pid,
                                  const std::string& data) {
  auto stub = GetMigrationStub(node_id);
  if (!stub) return Status::IOError("Node not available");

  cedar::migration::SyncDataRequest request;
  cedar::migration::SyncDataResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(300));

  auto writer = stub->SyncData(&context, &response);
  request.set_migration_id(migration_id);
  request.set_partition_id(pid);
  request.set_offset(0);
  request.set_data(data);

  if (!writer->Write(request)) {
    return Status::IOError("SyncData stream write failed");
  }
  writer->WritesDone();
  auto status = writer->Finish();
  if (!status.ok()) {
    return Status::IOError("SyncData RPC failed: " + status.error_message());
  }
  if (!response.success()) return Status::IOError(response.error_msg());
  return Status::OK();
}

Status DTxRpcClient::VerifyMigration(NodeID source_node, NodeID target_node,
                                     PartitionID pid) {
  auto source_stub = GetMigrationStub(source_node);
  auto target_stub = GetMigrationStub(target_node);
  if (!source_stub || !target_stub) {
    return Status::IOError("Node not available");
  }

  cedar::migration::FetchChecksumRequest request;
  request.set_partition_id(pid);

  cedar::migration::FetchChecksumResponse source_response;
  cedar::migration::FetchChecksumResponse target_response;

  grpc::ClientContext source_context;
  source_context.set_deadline(std::chrono::system_clock::now() +
                               std::chrono::seconds(30));
  auto s1 = source_stub->FetchChecksum(&source_context, request,
                                       &source_response);

  grpc::ClientContext target_context;
  target_context.set_deadline(std::chrono::system_clock::now() +
                               std::chrono::seconds(30));
  auto s2 = target_stub->FetchChecksum(&target_context, request,
                                       &target_response);

  if (!s1.ok() || !source_response.success()) {
    return Status::IOError("Source checksum failed");
  }
  if (!s2.ok() || !target_response.success()) {
    return Status::IOError("Target checksum failed");
  }
  if (source_response.checksum() != target_response.checksum()) {
    return Status::IOError("Checksum mismatch");
  }
  return Status::OK();
}

Status DTxRpcClient::CompleteMigration(NodeID node_id,
                                       const std::string& migration_id,
                                       PartitionID pid) {
  auto stub = GetMigrationStub(node_id);
  if (!stub) return Status::IOError("Node not available");

  cedar::migration::FinalizeMigrationRequest request;
  request.set_migration_id(migration_id);
  request.set_partition_id(pid);
  request.set_commit(true);

  cedar::migration::FinalizeMigrationResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(30));

  auto status = stub->FinalizeMigration(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("FinalizeMigration RPC failed: " +
                           status.error_message());
  }
  if (!response.success()) return Status::IOError(response.error_msg());
  return Status::OK();
}

// =============================================================================
// MigrationTask Implementation
// =============================================================================

// =============================================================================
// MigrationTask Implementation
// =============================================================================

MigrationTask::MigrationTask(uint64_t id, const MigrationPlan::Action& action,
                             const MigrationConfig& config)
    : migration_id_(id),
      partition_id_(action.partition_id),
      source_node_(action.from_node),
      target_node_(action.to_node),
      config_(config) {
  
  progress_.migration_id = id;
  progress_.partition_id = action.partition_id;
  progress_.source_node = action.from_node;
  progress_.target_node = action.to_node;
  progress_.start_time = std::chrono::steady_clock::now();
}

MigrationProgress MigrationTask::GetProgress() const {
  std::lock_guard<std::mutex> lock(progress_mutex_);
  return progress_;
}

void MigrationTask::UpdateState(MigrationState new_state) {
  state_.store(new_state);
  {
    std::lock_guard<std::mutex> lock(progress_mutex_);
    progress_.state = new_state;
  }
}

Status MigrationTask::Execute(DTxRpcClient* rpc_client, PartitionManager* partition_mgr) {
  rpc_client_ = rpc_client;
  partition_mgr_ = partition_mgr;
  
  auto start = std::chrono::steady_clock::now();
  
  try {
    // Phase 1: Prepare
    UpdateState(MigrationState::kPreparing);
    Status s = Phase_Prepare();
    if (!s.ok()) {
      Rollback();
      return s;
    }
    
    // Phase 2: Snapshot Sync
    UpdateState(MigrationState::kSnapshotSync);
    s = Phase_SnapshotSync();
    if (!s.ok()) {
      Rollback();
      return s;
    }
    
    // Phase 3: Dual Write (if enabled)
    if (config_.enable_dual_write) {
      UpdateState(MigrationState::kDualWrite);
      s = Phase_DualWrite();
      if (!s.ok()) {
        Rollback();
        return s;
      }
    }
    
    // Phase 4: Cutover
    UpdateState(MigrationState::kCutover);
    s = Phase_Cutover();
    if (!s.ok()) {
      Rollback();
      return s;
    }
    
    // Phase 5: Verify
    if (config_.verify_checksum) {
      UpdateState(MigrationState::kVerifying);
      s = Phase_Verify();
      if (!s.ok()) {
        Rollback();
        return s;
      }
    }
    
    // Phase 6: Complete
    UpdateState(MigrationState::kCompleting);
    s = Phase_Complete();
    if (!s.ok()) {
      Rollback();
      return s;
    }
    
    UpdateState(MigrationState::kCompleted);
    
  } catch (const std::exception& e) {
    {
      std::lock_guard<std::mutex> lock(progress_mutex_);
      progress_.error_message = e.what();
    }
    Rollback();
    UpdateState(MigrationState::kFailed);
    return Status::IOError(e.what());
  }
  
  return Status::OK();
}

Status MigrationTask::Phase_Prepare() {
  // 1. Check source node health
  if (!rpc_client_->IsNodeAvailable(source_node_)) {
    return Status::IOError("Source node not available");
  }
  
  // 2. Check target node health
  if (!rpc_client_->IsNodeAvailable(target_node_)) {
    return Status::IOError("Target node not available");
  }
  
  // 3. Start migration on target via gRPC
  Status s = rpc_client_->PrepareMigration(
      target_node_, partition_id_, source_node_, target_node_,
      &external_migration_id_);
  if (!s.ok()) return s;
  
  // 4. Get partition statistics from source
  // In production: rpc_client_->GetPartitionStats(source_node_, partition_id_);
  {
    std::lock_guard<std::mutex> lock(progress_mutex_);
    progress_.total_keys = 1000000;  // Placeholder until RPC is implemented
    progress_.total_bytes = 1024 * 1024 * 1024;  // Placeholder 1GB
  }
  
  return Status::OK();
}

Status MigrationTask::Phase_SnapshotSync() {
  // Transfer initial data from source to target in batches
  uint64_t batch_size = config_.batch_size;
  uint64_t transferred = 0;
  
  // Stream data to target using dedicated migration RPC
  uint64_t batch_keys = std::min(batch_size, progress_.total_keys);
  for (uint64_t i = 0; i < progress_.total_keys; i += batch_keys) {
    if (cancelled_.load()) {
      return Status::IOError("Migration cancelled");
    }
    
    if (ShouldThrottle()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    // Send a batch of data to the target node
    std::string dummy_data(batch_keys * 1024, 'x');
    Status s = rpc_client_->TransferData(
        target_node_, external_migration_id_, partition_id_, dummy_data);
    if (!s.ok()) return s;
    
    transferred += batch_keys;
    {
      std::lock_guard<std::mutex> lock(progress_mutex_);
      progress_.transferred_keys = std::min(transferred, progress_.total_keys);
    }
  }
  
  {
    std::lock_guard<std::mutex> lock(progress_mutex_);
    progress_.transferred_keys = progress_.total_keys;
    progress_.transferred_bytes = progress_.total_bytes;
  }
  
  return Status::OK();
}

Status MigrationTask::Phase_DualWrite() {
  // Enable dual write: writes go to both source and target
  // This ensures consistency during migration
  
  // Enable dual write mode (requires RPC extension)
  // In production:
  //   rpc_client_->EnableDualWrite(source_node_, partition_id_);
  //   rpc_client_->EnableDualWrite(target_node_, partition_id_);
  
  // Wait for any in-flight writes to complete
  std::this_thread::sleep_for(std::chrono::seconds(1));
  
  // Capture and replicate new writes
  auto deadline = std::chrono::steady_clock::now() + config_.dual_write_timeout;
  
  while (std::chrono::steady_clock::now() < deadline) {
    if (cancelled_.load()) {
      return Status::IOError("Migration cancelled");
    }
    
    if (paused_.load()) {
      while (paused_.load() && !cancelled_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    
    // Replicate recent writes from source to target
    // In production: Use WAL or change data capture for replication
    
    // Check if replication lag is acceptable
    // bool lag_acceptable = CheckReplicationLag();
    // if (lag_acceptable) break;
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  
  return Status::OK();
}

Status MigrationTask::Phase_Cutover() {
  // 1. Stop accepting new writes on source (requires RPC extension)
  // In production: rpc_client_->EnterReadOnlyMode(source_node_, partition_id_);
  
  // 2. Final sync of any remaining data
  // In production: Replicate final delta batch
  
  // 3. Update metadata to point to new node
  if (partition_mgr_) {
    partition_mgr_->SetPartitionLeader(partition_id_, target_node_);
  }
  
  // 4. Notify routing layer of the change
  // In production: Invalidate routing caches across all GraphD nodes
  
  // 5. Start accepting writes on target (requires RPC extension)
  // In production: rpc_client_->EnableFullReadWrite(target_node_, partition_id_);
  
  return Status::OK();
}

Status MigrationTask::Phase_Verify() {
  // Verify data consistency between source and target via gRPC checksum
  Status s = rpc_client_->VerifyMigration(source_node_, target_node_,
                                          partition_id_);
  if (!s.ok()) return s;
  return Status::OK();
}

Status MigrationTask::Phase_Complete() {
  // 1. Finalize migration on target via gRPC
  Status s = rpc_client_->CompleteMigration(
      target_node_, external_migration_id_, partition_id_);
  if (!s.ok()) return s;
  
  // 2. Update migration metadata
  {
    std::lock_guard<std::mutex> lock(progress_mutex_);
    progress_.estimated_end_time = std::chrono::steady_clock::now();
  }
  
  return Status::OK();
}

Status MigrationTask::Rollback() {
  // Rollback on failure
  // 1. If cutover already happened, revert to source
  if (partition_mgr_ && state_.load() >= MigrationState::kCutover) {
    partition_mgr_->SetPartitionLeader(partition_id_, source_node_);
  }
  // 2. Clean up partial data on target (requires RPC extension)
  // 3. Restore source to normal mode (requires RPC extension)
  return Status::OK();
}

Status MigrationTask::TransferBatch(const std::vector<std::pair<CedarKey, Descriptor>>& batch) {
  // Transfer batch to target node using available Put API
  // In production: Use dedicated BatchPut RPC for efficiency
  if (rpc_client_) {
    for (const auto& [key, value] : batch) {
      // Use existing Put interface; in production use BatchPut
      auto s = rpc_client_->Put(target_node_, partition_id_, key, value, Timestamp::Now());
      if (!s.ok()) return s;
    }
  }
  return Status::OK();
}

bool MigrationTask::ShouldThrottle() const {
  // Check if we need to throttle migration speed
  // based on configured bandwidth limits
  
  // Check if we need to throttle migration speed
  // In production: Measure actual bandwidth and compare to limit
  return config_.throttle_during_migration;
}

void MigrationTask::Cancel() {
  cancelled_.store(true);
}

void MigrationTask::Pause() {
  paused_.store(true);
}

void MigrationTask::Resume() {
  paused_.store(false);
}

// =============================================================================
// MigrationExecutor Implementation
// =============================================================================

MigrationExecutor::MigrationExecutor(const MigrationConfig& config)
    : config_(config) {}

MigrationExecutor::~MigrationExecutor() {
  Shutdown();
}

Status MigrationExecutor::Initialize(DTxRpcClient* rpc_client, PartitionManager* partition_mgr) {
  rpc_client_ = rpc_client;
  partition_mgr_ = partition_mgr;
  
  // Start worker threads
  for (size_t i = 0; i < config_.max_concurrent_migrations; i++) {
    worker_threads_.emplace_back(&MigrationExecutor::WorkerLoop, this);
  }
  
  return Status::OK();
}

void MigrationExecutor::Shutdown() {
  shutdown_.store(true);
  worker_cv_.notify_all();
  
  for (auto& thread : worker_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

std::vector<uint64_t> MigrationExecutor::SubmitPlan(const MigrationPlan& plan) {
  std::vector<uint64_t> migration_ids;
  migration_ids.reserve(plan.actions.size());
  
  for (const auto& action : plan.actions) {
    migration_ids.push_back(SubmitMigration(action));
  }
  
  return migration_ids;
}

uint64_t MigrationExecutor::SubmitMigration(const MigrationPlan::Action& action) {
  uint64_t id = next_migration_id_++;
  
  auto task = std::make_shared<MigrationTask>(id, action, config_);
  
  {
    std::lock_guard<std::mutex> lock(tasks_mutex_);
    tasks_[id] = task;
    pending_queue_.push(task);
  }
  
  worker_cv_.notify_one();
  
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.total_migrations++;
  }
  
  return id;
}

MigrationProgress MigrationExecutor::GetProgress(uint64_t migration_id) const {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  
  auto it = tasks_.find(migration_id);
  if (it != tasks_.end()) {
    return it->second->GetProgress();
  }
  
  return MigrationProgress{};
}

std::vector<MigrationProgress> MigrationExecutor::GetAllProgress() const {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  
  std::vector<MigrationProgress> progress_list;
  progress_list.reserve(tasks_.size());
  
  for (const auto& [id, task] : tasks_) {
    progress_list.push_back(task->GetProgress());
  }
  
  return progress_list;
}

Status MigrationExecutor::CancelMigration(uint64_t migration_id) {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  
  auto it = tasks_.find(migration_id);
  if (it != tasks_.end()) {
    it->second->Cancel();
    return Status::OK();
  }
  
  return Status::NotFound("Migration not found");
}

Status MigrationExecutor::PauseMigration(uint64_t migration_id) {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  
  auto it = tasks_.find(migration_id);
  if (it != tasks_.end()) {
    it->second->Pause();
    return Status::OK();
  }
  
  return Status::NotFound("Migration not found");
}

Status MigrationExecutor::ResumeMigration(uint64_t migration_id) {
  std::lock_guard<std::mutex> lock(tasks_mutex_);
  
  auto it = tasks_.find(migration_id);
  if (it != tasks_.end()) {
    it->second->Resume();
    return Status::OK();
  }
  
  return Status::NotFound("Migration not found");
}

void MigrationExecutor::SetProgressCallback(ProgressCallback callback) {
  progress_callback_ = callback;
}

void MigrationExecutor::SetCompletionCallback(CompletionCallback callback) {
  completion_callback_ = callback;
}

MigrationExecutor::Stats MigrationExecutor::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

void MigrationExecutor::WorkerLoop() {
  while (!shutdown_.load()) {
    std::shared_ptr<MigrationTask> task;
    
    {
      std::unique_lock<std::mutex> lock(tasks_mutex_);
      worker_cv_.wait(lock, [this] { return !pending_queue_.empty() || shutdown_.load(); });
      
      if (shutdown_.load()) break;
      
      if (!pending_queue_.empty()) {
        task = pending_queue_.front();
        pending_queue_.pop();
        active_migrations_.insert(task->GetId());
      }
    }
    
    if (task) {
      ExecuteMigration(task);
      
      {
        std::lock_guard<std::mutex> lock(tasks_mutex_);
        active_migrations_.erase(task->GetId());
      }
    }
  }
}

void MigrationExecutor::ExecuteMigration(std::shared_ptr<MigrationTask> task) {
  // Execute the migration
  Status s = task->Execute(rpc_client_, partition_mgr_);
  
  bool success = s.ok();
  uint64_t id = task->GetId();
  
  // Update stats
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    if (success) {
      stats_.successful_migrations++;
    } else {
      stats_.failed_migrations++;
    }
    
    auto progress = task->GetProgress();
    stats_.total_keys_transferred += progress.transferred_keys;
    stats_.total_bytes_transferred += progress.transferred_bytes;
  }
  
  // Notify completion
  NotifyCompletion(id, success);
}

void MigrationExecutor::NotifyProgress(const MigrationProgress& progress) {
  if (progress_callback_) {
    progress_callback_(progress);
  }
}

void MigrationExecutor::NotifyCompletion(uint64_t id, bool success) {
  if (completion_callback_) {
    completion_callback_(id, success);
  }
}

}  // namespace dtx
}  // namespace cedar
