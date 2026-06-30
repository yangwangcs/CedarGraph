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

#include "cedar/core/logging.h"

#include <algorithm>
#include <climits>
#include <deque>
#include <fstream>
#include <iostream>

#include "cedar/dtx/cross_dc_replicator.h"
#include "cedar/dtx/raft/grpc_tls.h"
#include "dtx_protocol.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace cedar {
namespace dtx {

CrossDCReplicator::CrossDCReplicator() = default;
CrossDCReplicator::~CrossDCReplicator() {
  try {
    Stop();
  } catch (...) {
    CEDAR_LOG_ERROR() << "[CrossDCReplicator] Destructor exception suppressed" << std::endl;
  }
}

Status CrossDCReplicator::Initialize(
    const DCReplicationConfig& config,
    const std::string& local_dc_id,
    const std::vector<std::string>& peer_dcs) {
  if (local_dc_id.empty()) {
    return Status::InvalidArgument("CrossDCReplicator::Initialize",
                                   "local_dc_id cannot be empty");
  }
  if (config.batch_size == 0) {
    return Status::InvalidArgument("CrossDCReplicator::Initialize",
                                   "batch_size must be greater than 0");
  }
  if (config.max_reconciliation_queue_size == 0) {
    return Status::InvalidArgument("CrossDCReplicator::Initialize",
                                   "max_reconciliation_queue_size must be greater than 0");
  }
  if (config.reconciliation_ttl <= std::chrono::seconds::zero()) {
    return Status::InvalidArgument("CrossDCReplicator::Initialize",
                                   "reconciliation_ttl must be positive");
  }

  config_ = config;
  local_dc_id_ = local_dc_id;
  peer_dcs_ = peer_dcs;

  for (const auto& dc : peer_dcs) {
    dc_statuses_[dc] = ReplicationStatus{};
  }

  for (const auto& dc : peer_dcs) {
    auto it = config.remote_dc_endpoints.find(dc);
    if (it != config.remote_dc_endpoints.end()) {
      std::shared_ptr<grpc::ChannelCredentials> creds;
      if (config_.tls_enabled) {
        cedar::dtx::raft::TlsConfig tls;
        tls.enabled = true;
        tls.ca_cert_file = config_.tls_config.ca_cert_file;
        tls.client_cert_file = config_.tls_config.client_cert_file;
        tls.client_key_file = config_.tls_config.client_key_file;
        tls.mtls_enabled = !tls.client_cert_file.empty() || !tls.client_key_file.empty();
        auto tls_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(tls);
        if (!tls_creds.ok()) {
          CEDAR_LOG_ERROR() << "[CrossDCReplicator] Failed to create TLS credentials for "
                            << dc << ": " << tls_creds.status().ToString() << std::endl;
          return tls_creds.status();
        }
        creds = tls_creds.ValueOrDie();
      } else {
        if (!config_.allow_insecure) {
          CEDAR_LOG_ERROR() << "[CrossDCReplicator] FATAL: TLS is required for cross-DC replication. "
                    << "Set tls_enabled=true or allow_insecure=true (dev only)." << std::endl;
          return Status::IOError("Cross-DC replication requires TLS or explicit insecure override");
        }
        creds = grpc::InsecureChannelCredentials();
      }
      auto channel = grpc::CreateChannel(it->second, creds);
      dc_channels_[dc] = channel;
    }
  }

  return Status::OK();
}

Status CrossDCReplicator::Start() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("CrossDCReplicator::Start", "Already running");
  }
  stop_requested_.store(false, std::memory_order_release);
  
  if (config_.mode == ReplicationMode::kAsync) {
    try {
      replication_thread_ = std::thread(&CrossDCReplicator::ReplicationLoop, this);
    } catch (...) {
      running_ = false;
      CEDAR_LOG_ERROR() << "[CrossDCReplicator] Failed to start replication thread" << std::endl;
      return Status::IOError("Failed to start replication thread");
    }
  }

  // Start reconciliation thread regardless of mode — it handles
  // synchronous-replication cleanup failures and async retries.
  try {
    reconciliation_thread_ = std::thread(&CrossDCReplicator::ReconciliationLoop, this);
  } catch (...) {
    running_ = false;
    // Join the already-started replication thread to avoid std::terminate
    if (replication_thread_.joinable()) {
      replication_thread_.join();
    }
    CEDAR_LOG_ERROR() << "[CrossDCReplicator] Failed to start reconciliation thread" << std::endl;
    return Status::IOError("Failed to start reconciliation thread");
  }
  
  return Status::OK();
}

void CrossDCReplicator::Stop() {
  stop_requested_.store(true, std::memory_order_release);
  TryCancelActiveContexts();
  reconciliation_cv_.notify_all();
  queue_cv_.notify_all();

  if (!running_.exchange(false)) {
    return;
  }
  
  try {
    if (replication_thread_.joinable()) {
      replication_thread_.join();
    }
  } catch (...) {
    CEDAR_LOG_ERROR() << "[CrossDCReplicator] Replication thread join exception" << std::endl;
  }

  try {
    if (reconciliation_thread_.joinable()) {
      reconciliation_thread_.join();
    }
  } catch (...) {
    CEDAR_LOG_ERROR() << "[CrossDCReplicator] Reconciliation thread join exception" << std::endl;
  }
}

void CrossDCReplicator::RegisterActiveContext(::grpc::ClientContext* context) {
  if (context == nullptr) {
    return;
  }
  bool should_cancel = false;
  {
    std::lock_guard<std::mutex> lock(active_contexts_mutex_);
    should_cancel = stop_requested_.load(std::memory_order_acquire);
    if (!should_cancel) {
      active_contexts_.insert(context);
    }
  }
  if (should_cancel) {
    context->TryCancel();
  }
}

void CrossDCReplicator::UnregisterActiveContext(::grpc::ClientContext* context) {
  if (context == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(active_contexts_mutex_);
  active_contexts_.erase(context);
}

void CrossDCReplicator::TryCancelActiveContexts() {
  std::lock_guard<std::mutex> lock(active_contexts_mutex_);
  for (auto* context : active_contexts_) {
    if (context != nullptr) {
      context->TryCancel();
    }
  }
}

Status CrossDCReplicator::Replicate(const std::string& key,
                                     const Descriptor& value,
                                     Timestamp timestamp) {
  ::cedar::CedarKey ck;
  ck.SetEntityId(std::hash<std::string>{}(key));
  return Replicate(ck, value, timestamp);
}

Status CrossDCReplicator::Replicate(const ::cedar::CedarKey& key,
                                     const Descriptor& value,
                                     Timestamp timestamp) {
  ReplicationLog log;
  // Detect wraparound and bump generation
  if (sequence_counter_.load(std::memory_order_relaxed) == UINT64_MAX) {
    sequence_generation_.fetch_add(1, std::memory_order_relaxed);
    sequence_counter_.store(0, std::memory_order_relaxed);
  }
  log.sequence_num = ++sequence_counter_;
  log.generation = sequence_generation_.load(std::memory_order_relaxed);
  log.key = key;
  log.value = value;
  log.timestamp = timestamp;
  log.source_dc = local_dc_id_;
  log.target_dcs = peer_dcs_;
  log.created_at = std::chrono::system_clock::now();

  if (config_.mode == ReplicationMode::kSync) {
    // Phase 1: Attempt replication to ALL peer DCs.
    std::vector<std::string> succeeded_dcs;
    Status first_failure;
    for (const auto& dc : peer_dcs_) {
      Status s = ReplicateToDC(log, dc);
      ReplicationCallback callback;
      {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        callback = replication_callback_;
      }
      if (callback) {
        try {
          callback(log, s);
        } catch (...) {
          CEDAR_LOG_ERROR() << "[CrossDCReplicator] Callback exception" << std::endl;
        }
      }
      if (!s.ok()) {
        if (first_failure.ok()) {
          first_failure = s;
        }
        // Do NOT break early — we need to know which DCs succeeded so we can
        // attempt cleanup. But we continue trying the rest for observability.
        continue;
      }
      succeeded_dcs.push_back(dc);
    }

    // If any DC failed, we must roll back the ones that succeeded.
    if (!first_failure.ok()) {
      bool all_cleaned = true;
      for (const auto& succ_dc : succeeded_dcs) {
        Status del_status = DeleteFromDC(log.key, succ_dc);
        if (!del_status.ok()) {
          CEDAR_LOG_ERROR() << "[CrossDCReplicator] Sync rollback FAILED for " << succ_dc
                    << ": " << del_status.ToString() << std::endl;
          EnqueueReconciliation(log.key, succ_dc);
          all_cleaned = false;
        }
      }

      if (!all_cleaned) {
        // Critical: we could not undo the partial write. We MUST NOT return OK.
        // Return the ORIGINAL failure so the caller (2PC coordinator) knows
        // the transaction did NOT commit globally and can retry or abort.
        CEDAR_LOG_ERROR() << "[CrossDCReplicator] CRITICAL: sync replication partially "
                     "committed and rollback incomplete. Returning failure to caller."
                  << std::endl;
      }
      return first_failure;
    }

    return Status::OK();
  }

  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    replication_queue_.push(log);
  }
  queue_cv_.notify_one();

  return Status::OK();
}

Status CrossDCReplicator::ReplicateBatch(const std::vector<ReplicationLog>& logs) {
  for (const auto& log : logs) {
    Status s = Replicate(log.key, log.value, log.timestamp);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status CrossDCReplicator::ReceiveReplication(const ReplicationLog& log) {
  if (log.source_dc == local_dc_id_) {
    return Status::InvalidArgument("CrossDCReplicator::ReceiveReplication",
        "Cannot receive replication from local DC");
  }

  {
    std::lock_guard<std::mutex> lock(status_mutex_);
    auto it = dc_statuses_.find(log.source_dc);
    if (it != dc_statuses_.end()) {
      bool is_valid = false;
      if (log.generation > it->second.last_generation) {
        // New generation: always valid (wraparound happened)
        is_valid = true;
      } else if (log.generation == it->second.last_generation) {
        if (it->second.last_sequence == 0) {
          // First log from this DC in this generation
          is_valid = true;
        } else {
          is_valid = (log.sequence_num > it->second.last_sequence);
        }
      } else {
        // log.generation < last_generation: stale log from a previous epoch
        is_valid = false;
      }

      if (!is_valid) {
        return Status::InvalidArgument(
            "Out-of-order replication received: expected (gen=" +
            std::to_string(it->second.last_generation) +
            ", seq>" + std::to_string(it->second.last_sequence) +
            ") got (gen=" + std::to_string(log.generation) +
            ", seq=" + std::to_string(log.sequence_num) + ") from " + log.source_dc);
      }

      it->second.last_generation = log.generation;
      it->second.last_sequence = log.sequence_num;
      it->second.replicated_count++;
    }
  }

  // Apply to local storage if configured
  if (storage_) {
    auto s = storage_->Put(log.key.entity_id(), log.key.timestamp().value(),
                            log.value, log.timestamp);
    if (!s.ok()) {
      return Status::IOError("Storage Put failed in ReceiveReplication: " + s.ToString());
    }
  }

  return Status::OK();
}

ReplicationStatus CrossDCReplicator::GetStatus(const std::string& dc_id) const {
  std::lock_guard<std::mutex> lock(status_mutex_);
  
  auto it = dc_statuses_.find(dc_id);
  if (it != dc_statuses_.end()) {
    return it->second;
  }
  
  return ReplicationStatus{};
}

std::map<std::string, ReplicationStatus> CrossDCReplicator::GetAllStatuses() const {
  std::lock_guard<std::mutex> lock(status_mutex_);
  return dc_statuses_;
}

Status CrossDCReplicator::SyncWithDC(const std::string& dc_id) {
  auto it = dc_channels_.find(dc_id);
  if (it == dc_channels_.end()) {
    return Status::IOError("No gRPC channel for DC: " + dc_id);
  }

  auto stub = cedar::dtx::DTXService::NewStub(it->second);

  cedar::dtx::ReplicateRequest request;
  request.set_target_dc(dc_id);
  // Empty logs = sync handshake

  cedar::dtx::ReplicateResponse response;
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                        config_.replication_timeout);

  RegisterActiveContext(&context);
  auto status = stub->Replicate(&context, request, &response);
  UnregisterActiveContext(&context);
  if (!status.ok()) {
    return Status::IOError("SyncWithDC RPC failed: " + status.error_message());
  }
  if (!response.success()) {
    return Status::IOError("Remote DC rejected sync: " + response.error_msg());
  }
  return Status::OK();
}

Status CrossDCReplicator::ResolveConflict(
    const std::string& key,
    const std::vector<ReplicationLog>& conflicting_logs) {

  if (conflicting_logs.empty()) {
    return Status::OK();
  }

  const ReplicationLog* winner = &conflicting_logs[0];
  for (const auto& log : conflicting_logs) {
    if (log.timestamp > winner->timestamp) {
      winner = &log;
    }
  }

  return ReceiveReplication(*winner);
}

void CrossDCReplicator::SetReplicationCallback(ReplicationCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  replication_callback_ = callback;
}

void CrossDCReplicator::ReplicationLoop() {
  while (running_) {
    try {
      ProcessReplicationQueue();
    } catch (...) {
      CEDAR_LOG_ERROR() << "[CrossDCReplicator] Replication loop exception" << std::endl;
    }
    // Wait for new data or timeout; avoids busy-waiting when queue is empty.
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait_for(lock, std::chrono::milliseconds(10),
                         [this]() { return !replication_queue_.empty() || !running_; });
    }
  }
}

void CrossDCReplicator::ProcessReplicationQueue() {
  DrainPendingQueue();

  std::vector<ReplicationLog> batch;
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    while (!replication_queue_.empty() && batch.size() < config_.batch_size) {
      batch.push_back(replication_queue_.front());
      replication_queue_.pop();
    }
  }

  for (const auto& log : batch) {
    for (const auto& dc : log.target_dcs) {
      Status s = SendToRemoteDCWithRetry(log, dc);
      ReplicationCallback callback;
      {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        callback = replication_callback_;
      }
      if (callback) {
        try {
          callback(log, s);
        } catch (...) {
          CEDAR_LOG_ERROR() << "[CrossDCReplicator] Callback exception" << std::endl;
        }
      }
      if (!s.ok()) {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_queue_.push_back({log, dc, 0, std::chrono::steady_clock::now()});
      }
    }
  }
}

void CrossDCReplicator::DrainPendingQueue() {
  auto now = std::chrono::steady_clock::now();
  std::vector<PendingLog> to_retry;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_queue_.begin();
    while (it != pending_queue_.end()) {
      if (it->next_attempt <= now) {
        to_retry.push_back(*it);
        it = pending_queue_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& entry : to_retry) {
    Status s = SendToRemoteDCWithRetry(entry.log, entry.dc_id);
    if (!s.ok()) {
      entry.retry_count++;
      auto delay = config_.retry_delay * (1ULL << std::min(entry.retry_count, 6u));
      entry.next_attempt = now + delay;
      std::lock_guard<std::mutex> lock(pending_mutex_);
      pending_queue_.push_back(std::move(entry));
    }
    ReplicationCallback callback;
    {
      std::lock_guard<std::mutex> lock(callback_mutex_);
      callback = replication_callback_;
    }
    if (callback) {
      try {
        callback(entry.log, s);
      } catch (...) {
        CEDAR_LOG_ERROR() << "[CrossDCReplicator] Callback exception" << std::endl;
      }
    }
  }
}

Status CrossDCReplicator::SendToRemoteDCWithRetry(const ReplicationLog& log,
                                                   const std::string& dc_id) {
  uint32_t attempts = 0;
  Status s;
  while (attempts < config_.max_retry_attempts) {
    s = SendToRemoteDC(log, dc_id);
    if (s.ok()) {
      return Status::OK();
    }
    if (s.IsNotSupportedError() || s.IsInvalidArgument() ||
        s.ToString().find("No gRPC channel") != std::string::npos) {
      return s;
    }
    // Don't retry on timeout — move to pending queue for backoff
    if (s.ToString().find("Deadline") != std::string::npos ||
        s.ToString().find("Timeout") != std::string::npos ||
        s.ToString().find("TIMEOUT") != std::string::npos) {
      return s;
    }
    attempts++;
    auto delay = config_.retry_delay * (1ULL << std::min(attempts, 6u));
    if (WaitForRetryDelay(delay)) {
      return Status::IOError("Replication retry aborted: replicator stopping");
    }
  }
  return s;
}

Status CrossDCReplicator::ReplicateToDC(const ReplicationLog& log, 
                                         const std::string& dc_id) {
  uint32_t attempts = 0;
  Status s;
  
  while (attempts < config_.max_retry_attempts) {
    s = SendToRemoteDC(log, dc_id);
    if (s.ok()) {
      UpdateLag(dc_id);
      return Status::OK();
    }

    // Don't retry on permanent errors (NotSupported, no endpoint, timeout)
    if (s.IsNotSupportedError() ||
        s.IsInvalidArgument() ||
        s.ToString().find("No gRPC channel") != std::string::npos ||
        s.ToString().find("Deadline") != std::string::npos ||
        s.ToString().find("Timeout") != std::string::npos ||
        s.ToString().find("TIMEOUT") != std::string::npos) {
      break;
    }

    attempts++;
    // Exponential backoff with cap
    auto delay = config_.retry_delay * (1ULL << std::min(attempts, 6u));
    if (WaitForRetryDelay(delay)) {
      return Status::IOError("Replication retry aborted: replicator stopping");
    }
  }
  
  std::lock_guard<std::mutex> lock(status_mutex_);
  auto it = dc_statuses_.find(dc_id);
  if (it != dc_statuses_.end()) {
    it->second.failed_count++;
    it->second.is_healthy = false;
  }
  
  return s;
}

bool CrossDCReplicator::WaitForRetryDelay(std::chrono::milliseconds delay) {
  if (stop_requested_.load(std::memory_order_acquire)) {
    return true;
  }

  std::unique_lock<std::mutex> lock(reconciliation_mutex_);
  return reconciliation_cv_.wait_for(lock, delay, [this]() {
    return stop_requested_.load(std::memory_order_acquire);
  });
}

Status CrossDCReplicator::DeleteFromDC(const ::cedar::CedarKey& key,
                                        const std::string& dc_id) {
  ReplicationLog tombstone_log;
  tombstone_log.sequence_num = ++sequence_counter_;
  tombstone_log.key = key;
  tombstone_log.value = Descriptor::Tombstone();
  tombstone_log.timestamp = Timestamp::Now();
  tombstone_log.source_dc = local_dc_id_;
  tombstone_log.target_dcs = {dc_id};
  tombstone_log.created_at = std::chrono::system_clock::now();
  return ReplicateToDC(tombstone_log, dc_id);
}

Status CrossDCReplicator::SendToRemoteDC(const ReplicationLog& log,
                                          const std::string& dc_id) {
  auto it = dc_channels_.find(dc_id);
  if (it == dc_channels_.end()) {
    return Status::IOError("No gRPC channel for DC: " + dc_id);
  }

  auto stub = cedar::dtx::DTXService::NewStub(it->second);

  cedar::dtx::ReplicateRequest request;
  request.set_target_dc(dc_id);

  auto* entry = request.add_logs();
  entry->set_sequence_num(log.sequence_num);

  // CedarKey serialization
  std::string key_bytes;
  key_bytes.resize(sizeof(::cedar::CedarKey));
  std::memcpy(&key_bytes[0], &log.key, sizeof(::cedar::CedarKey));
  entry->set_key(key_bytes);

  // Descriptor serialization
  uint64_t desc_raw = log.value.AsRaw();
  std::string desc_bytes(reinterpret_cast<const char*>(&desc_raw), sizeof(uint64_t));
  entry->set_value(desc_bytes);

  entry->set_timestamp(log.timestamp.value());
  entry->set_source_dc(log.source_dc);
  entry->set_generation(log.generation);
  for (const auto& target : log.target_dcs) {
    entry->add_target_dcs(target);
  }

  cedar::dtx::ReplicateResponse response;
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                        config_.replication_timeout);

  RegisterActiveContext(&context);
  auto status = stub->Replicate(&context, request, &response);
  UnregisterActiveContext(&context);
  if (!status.ok()) {
    return Status::IOError("Replicate RPC failed: " + status.error_message());
  }
  if (!response.success()) {
    return Status::IOError("Remote DC rejected replication: " + response.error_msg());
  }
  return Status::OK();
}

void CrossDCReplicator::UpdateLag(const std::string& dc_id) {
  std::lock_guard<std::mutex> lock(status_mutex_);
  
  auto it = dc_statuses_.find(dc_id);
  if (it != dc_statuses_.end()) {
    it->second.replication_lag = std::chrono::milliseconds(0);
    it->second.is_healthy = true;
  }
}

ReplicationLog CrossDCReplicator::CreateTimestampBasedResolution(
    const std::vector<ReplicationLog>& logs) {
  
  ReplicationLog winner = logs[0];
  
  for (const auto& log : logs) {
    if (log.timestamp > winner.timestamp) {
      winner = log;
    }
  }
  
  return winner;
}

// ---------------------------------------------------------------------------
// Reconciliation loop: handles cleanup failures after partial sync-replication
// success by asynchronously retrying delete compensation.
// ---------------------------------------------------------------------------
void CrossDCReplicator::ReconciliationLoop() {
  while (running_.load(std::memory_order_relaxed)) {
    std::vector<ReconcileEntry> batch;
    {
      std::lock_guard<std::mutex> lock(reconciliation_mutex_);
      auto now = std::chrono::steady_clock::now();
      const auto ttl = config_.reconciliation_ttl;
      const size_t max_size = config_.max_reconciliation_queue_size;

      // TTL eviction pass: remove expired entries
      size_t evicted_ttl = 0;
      reconciliation_queue_.erase(
          std::remove_if(reconciliation_queue_.begin(),
                         reconciliation_queue_.end(),
                         [&](const ReconcileEntry& e) {
                           auto age = now - e.enqueued_at;
                           if (age > ttl) {
                             evicted_ttl++;
                             return true;
                           }
                           return false;
                         }),
          reconciliation_queue_.end());
      if (evicted_ttl > 0) {
        CEDAR_LOG_ERROR() << "[CrossDCReplicator] Reconciliation TTL eviction: dropped "
                  << evicted_ttl << " expired entries" << std::endl;
      }

      // Bound the queue: if still oversized, drop oldest (FIFO)
      while (reconciliation_queue_.size() > max_size) {
        CEDAR_LOG_ERROR() << "[CrossDCReplicator] Reconciliation queue overflow: dropping oldest entry"
                  << std::endl;
        reconciliation_queue_.pop_front();
      }

      for (auto it = reconciliation_queue_.begin();
           it != reconciliation_queue_.end() && batch.size() < 64;) {
        if (it->next_attempt <= now) {
          batch.push_back(std::move(*it));
          it = reconciliation_queue_.erase(it);
        } else {
          ++it;
        }
      }
    }

    if (batch.empty()) {
      std::unique_lock<std::mutex> wait_lock(reconciliation_mutex_);
      reconciliation_cv_.wait_for(wait_lock, std::chrono::seconds(1), [this] {
        return !running_.load(std::memory_order_relaxed) ||
               !reconciliation_queue_.empty();
      });
      continue;
    }

    for (auto& entry : batch) {
      ReconcileKey(entry.key, entry.dc_id);
      reconciliation_retried_.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void CrossDCReplicator::ReconcileKey(const ::cedar::CedarKey& key,
                                     const std::string& dc_id) {
  if (!storage_) {
    CEDAR_LOG_ERROR() << "[CrossDCReplicator] ReconcileKey: no storage set, cannot "
                 "reconcile " << key.ToString() << std::endl;
    return;
  }
  auto desc_opt = storage_->Get(key.entity_id(), key.entity_type(),
                                key.column_id(), Timestamp::Now());

  Status s;
  if (desc_opt.has_value()) {
    ReplicationLog log;
    log.sequence_num = ++sequence_counter_;
    log.generation = sequence_generation_.load(std::memory_order_relaxed);
    log.key = key;
    log.value = desc_opt.value();
    log.timestamp = Timestamp::Now();
    log.source_dc = local_dc_id_;
    log.target_dcs = {dc_id};
    log.created_at = std::chrono::system_clock::now();
    s = ReplicateToDC(log, dc_id);
  } else {
    s = DeleteFromDC(key, dc_id);
  }

  if (s.ok()) {
    reconciliation_resolved_.fetch_add(1, std::memory_order_relaxed);
  } else {
    EnqueueReconciliation(key, dc_id);
  }
}

void CrossDCReplicator::EnqueueReconciliation(const ::cedar::CedarKey& key,
                                              const std::string& dc_id) {
  {
    std::lock_guard<std::mutex> lock(reconciliation_mutex_);
    auto now = std::chrono::steady_clock::now();

    for (auto& existing : reconciliation_queue_) {
      if (existing.key.entity_id() == key.entity_id() &&
          existing.key.entity_type() == key.entity_type() &&
          existing.key.column_id() == key.column_id() &&
          existing.dc_id == dc_id) {
        existing.attempt_count = existing.attempt_count + 1;
        uint32_t backoff_power = std::min(existing.attempt_count, 6u);
        auto delay = std::chrono::seconds(5 * (1 << backoff_power));
        existing.next_attempt = now + delay;
        reconciliation_cv_.notify_one();
        return;
      }
    }

    ReconcileEntry retry;
    retry.key = key;
    retry.dc_id = dc_id;
    retry.attempt_count = 1;
    retry.enqueued_at = now;
    retry.next_attempt = now + std::chrono::seconds(5);
    reconciliation_queue_.push_back(std::move(retry));

    while (reconciliation_queue_.size() > config_.max_reconciliation_queue_size) {
      reconciliation_queue_.pop_front();
    }
  }
  reconciliation_cv_.notify_one();
}

CrossDCReplicator::ReconciliationStatus
CrossDCReplicator::GetReconciliationStatus() const {
  ReconciliationStatus status;
  {
    std::lock_guard<std::mutex> lock(reconciliation_mutex_);
    status.pending_count = reconciliation_queue_.size();
  }
  status.retried_count = reconciliation_retried_.load(std::memory_order_relaxed);
  status.resolved_count = reconciliation_resolved_.load(std::memory_order_relaxed);
  status.last_run = std::chrono::steady_clock::now();
  return status;
}

}  // namespace dtx
}  // namespace cedar
