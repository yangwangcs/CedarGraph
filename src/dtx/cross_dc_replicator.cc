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

#include <iostream>

#include "cedar/dtx/cross_dc_replicator.h"
#include "dtx_protocol.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace cedar {
namespace dtx {

CrossDCReplicator::CrossDCReplicator() = default;
CrossDCReplicator::~CrossDCReplicator() {
  try {
    Stop();
  } catch (...) {
    std::cerr << "[CrossDCReplicator] Destructor exception suppressed" << std::endl;
  }
}

Status CrossDCReplicator::Initialize(
    const DCReplicationConfig& config,
    const std::string& local_dc_id,
    const std::vector<std::string>& peer_dcs) {

  config_ = config;
  local_dc_id_ = local_dc_id;
  peer_dcs_ = peer_dcs;

  for (const auto& dc : peer_dcs) {
    dc_statuses_[dc] = ReplicationStatus{};
  }

  for (const auto& dc : peer_dcs) {
    auto it = config.remote_dc_endpoints.find(dc);
    if (it != config.remote_dc_endpoints.end()) {
      auto channel = grpc::CreateChannel(it->second, grpc::InsecureChannelCredentials());
      dc_channels_[dc] = channel;
    }
  }

  return Status::OK();
}

Status CrossDCReplicator::Start() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("CrossDCReplicator::Start", "Already running");
  }
  
  if (config_.mode == ReplicationMode::kAsync) {
    try {
      replication_thread_ = std::thread(&CrossDCReplicator::ReplicationLoop, this);
    } catch (...) {
      running_ = false;
      std::cerr << "[CrossDCReplicator] Failed to start replication thread" << std::endl;
      return Status::IOError("Failed to start replication thread");
    }
  }
  
  return Status::OK();
}

void CrossDCReplicator::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  try {
    if (replication_thread_.joinable()) {
      replication_thread_.join();
    }
  } catch (...) {
    std::cerr << "[CrossDCReplicator] Replication thread join exception" << std::endl;
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
  log.sequence_num = ++sequence_counter_;
  log.key = key;
  log.value = value;
  log.timestamp = timestamp;
  log.source_dc = local_dc_id_;
  log.target_dcs = peer_dcs_;
  log.created_at = std::chrono::system_clock::now();

  if (config_.mode == ReplicationMode::kSync) {
    for (const auto& dc : peer_dcs_) {
      Status s = ReplicateToDC(log, dc);
      if (replication_callback_) {
        replication_callback_(log, s);
      }
      if (!s.ok()) {
        return s;
      }
    }
    return Status::OK();
  }

  std::lock_guard<std::mutex> lock(queue_mutex_);
  replication_queue_.push(log);

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

  // Apply to local storage if configured
  if (storage_) {
    auto s = storage_->Put(log.key.entity_id(), log.key.timestamp().value(),
                            log.value, log.timestamp);
    if (!s.ok()) {
      return Status::IOError("Storage Put failed in ReceiveReplication: " + s.ToString());
    }
  }

  std::lock_guard<std::mutex> lock(status_mutex_);
  auto it = dc_statuses_.find(log.source_dc);
  if (it != dc_statuses_.end()) {
    it->second.last_sequence = std::max(it->second.last_sequence, log.sequence_num);
    it->second.replicated_count++;
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

  auto status = stub->Replicate(&context, request, &response);
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
      std::cerr << "[CrossDCReplicator] Replication loop exception" << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void CrossDCReplicator::ProcessReplicationQueue() {
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
          std::cerr << "[CrossDCReplicator] Callback exception" << std::endl;
        }
      }
    }
  }
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
        s.ToString().find("DEADLINE_EXCEEDED") != std::string::npos ||
        s.ToString().find("TIMEOUT") != std::string::npos) {
      break;
    }

    attempts++;
    // Exponential backoff with cap
    auto delay = config_.retry_delay * (1ULL << std::min(attempts, 6u));
    std::this_thread::sleep_for(delay);
  }
  
  std::lock_guard<std::mutex> lock(status_mutex_);
  auto it = dc_statuses_.find(dc_id);
  if (it != dc_statuses_.end()) {
    it->second.failed_count++;
    it->second.is_healthy = false;
  }
  
  return s;
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
  for (const auto& target : log.target_dcs) {
    entry->add_target_dcs(target);
  }

  cedar::dtx::ReplicateResponse response;
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                        config_.replication_timeout);

  auto status = stub->Replicate(&context, request, &response);
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

}  // namespace dtx
}  // namespace cedar
