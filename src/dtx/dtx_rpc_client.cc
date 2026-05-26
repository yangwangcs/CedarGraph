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
// DTX RPC Client Implementation - Real gRPC for 2PC
// =============================================================================

#include "cedar/dtx/dtx_rpc_client.h"

#include "cedar/dtx/raft/grpc_tls.h"
#include <algorithm>
#include <chrono>
#include <thread>
#include <future>
#include <iostream>
#include "cedar/common/logging.h"

namespace cedar {
namespace dtx {

StatusOr<std::shared_ptr<grpc::ChannelCredentials>> CreateClientCredentialsFromEnv() {
  return cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
}

// =============================================================================
// Constructor / Destructor
// =============================================================================

DTXRpcClient::DTXRpcClient(const DTXRpcConfig& config)
    : config_(config),
      thread_pool_(std::make_unique<cedar::ThreadPool>(
          config.max_rpc_threads > 0 ? config.max_rpc_threads : 64)) {
  LOG(WARNING) << "DTXRpcClient initialized but DTXService has no server "
               << "implementation in the current codebase. RPCs will fail "
               << "with UNIMPLEMENTED unless a custom server is provided.";
  if (config.tls_config.enabled) {
    auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(
        config.tls_config);
    if (!creds.ok()) {
      std::cerr << "DTXRpcClient TLS error: " << creds.status().ToString() << std::endl;
      credentials_ = grpc::InsecureChannelCredentials();
    } else {
      credentials_ = creds.ValueOrDie();
    }
  } else {
    auto creds = cedar::dtx::CreateClientCredentialsFromEnv();
    if (!creds.ok()) {
      std::cerr << "DTXRpcClient TLS error: " << creds.status().ToString() << std::endl;
      credentials_ = grpc::InsecureChannelCredentials();
    } else {
      credentials_ = creds.ValueOrDie();
    }
  }
}

DTXRpcClient::~DTXRpcClient() = default;

// =============================================================================
// Connection Management
// =============================================================================

Status DTXRpcClient::AddParticipant(NodeID id, const std::string& endpoint) {
  auto participant = std::make_shared<ParticipantInfo>();
  participant->id = id;
  participant->endpoint = endpoint;
  participant->available = true;
  participant->last_health_check = std::chrono::steady_clock::now();
  participant->consecutive_failures = 0;

  grpc::ChannelArguments args;
  args.SetMaxSendMessageSize(64 * 1024 * 1024);
  args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
  participant->channel = grpc::CreateCustomChannel(endpoint, credentials_, args);
  participant->stub = std::shared_ptr<cedar::dtx::DTXService::Stub>(
      cedar::dtx::DTXService::NewStub(participant->channel));

  std::lock_guard<std::mutex> lock(participants_mutex_);
  participants_[id] = participant;

  std::cerr << "[DTXRpcClient] Added participant " << id << " at " << endpoint << std::endl;
  return Status::OK();
}

Status DTXRpcClient::RemoveParticipant(NodeID id) {
  std::lock_guard<std::mutex> lock(participants_mutex_);
  auto it = participants_.find(id);
  if (it != participants_.end()) {
    participants_.erase(it);
    std::cerr << "[DTXRpcClient] Removed participant " << id << std::endl;
    return Status::OK();
  }
  return Status::NotFound("Participant not found: " + std::to_string(id));
}

void DTXRpcClient::UpdateEndpoint(NodeID id, const std::string& new_endpoint) {
  std::lock_guard<std::mutex> lock(participants_mutex_);
  auto it = participants_.find(id);
  if (it != participants_.end()) {
    it->second->endpoint = new_endpoint;
    grpc::ChannelArguments args;
    args.SetMaxSendMessageSize(64 * 1024 * 1024);
    args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    it->second->channel = grpc::CreateCustomChannel(new_endpoint, credentials_, args);
    it->second->stub = std::shared_ptr<cedar::dtx::DTXService::Stub>(
        cedar::dtx::DTXService::NewStub(it->second->channel));
    std::cerr << "[DTXRpcClient] Updated participant " << id << " endpoint to " << new_endpoint << std::endl;
  }
}

// =============================================================================
// 2PC Protocol RPCs
// =============================================================================

Status DTXRpcClient::Prepare(
    NodeID participant_id,
    const std::string& txn_id,
    const std::string& coordinator_id,
    uint64_t prepare_version,
    const std::vector<cedar::dtx::CedarKey>& reads,
    const std::vector<cedar::dtx::CedarKey>& writes,
    int32_t isolation_level,
    int64_t timeout_ms,
    cedar::dtx::PrepareResponse* response) {

  auto stub = GetStub(participant_id);
  if (!stub) {
    return Status::NotFound("Participant not found: " + std::to_string(participant_id));
  }

  cedar::dtx::PrepareRequest request;
  request.set_txn_id(txn_id);
  request.set_coordinator_id(coordinator_id);
  request.set_prepare_version(prepare_version);
  request.set_isolation_level(isolation_level);
  request.set_timeout_ms(timeout_ms);

  for (const auto& key : reads) {
    *request.add_reads() = key;
  }
  for (const auto& key : writes) {
    *request.add_writes() = key;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(timeout_ms > 0 ? timeout_ms : config_.rpc_timeout_ms);
  context.set_deadline(deadline);

  grpc::Status grpc_status = stub->Prepare(&context, request, response);

  if (grpc_status.ok()) {
    MarkParticipantHealthy(participant_id);
  } else {
    MarkParticipantUnhealthy(participant_id);
    return Status::IOError("Prepare RPC failed: " + grpc_status.error_message());
  }

  return Status::OK();
}

Status DTXRpcClient::Commit(
    NodeID participant_id,
    const std::string& txn_id,
    const std::string& coordinator_id,
    uint64_t commit_version,
    cedar::dtx::CommitResponse* response) {

  auto stub = GetStub(participant_id);
  if (!stub) {
    return Status::NotFound("Participant not found: " + std::to_string(participant_id));
  }

  cedar::dtx::CommitRequest request;
  request.set_txn_id(txn_id);
  request.set_coordinator_id(coordinator_id);
  request.set_commit_version(commit_version);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(config_.rpc_timeout_ms);
  context.set_deadline(deadline);

  grpc::Status grpc_status = stub->Commit(&context, request, response);

  if (grpc_status.ok()) {
    MarkParticipantHealthy(participant_id);
  } else {
    MarkParticipantUnhealthy(participant_id);
    return Status::IOError("Commit RPC failed: " + grpc_status.error_message());
  }

  return Status::OK();
}

Status DTXRpcClient::Abort(
    NodeID participant_id,
    const std::string& txn_id,
    const std::string& coordinator_id,
    const std::string& reason,
    cedar::dtx::AbortResponse* response) {

  auto stub = GetStub(participant_id);
  if (!stub) {
    return Status::NotFound("Participant not found: " + std::to_string(participant_id));
  }

  cedar::dtx::AbortRequest request;
  request.set_txn_id(txn_id);
  request.set_coordinator_id(coordinator_id);
  request.set_reason(reason);
  request.set_initiator("coordinator");

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(config_.rpc_timeout_ms);
  context.set_deadline(deadline);

  grpc::Status grpc_status = stub->Abort(&context, request, response);

  if (grpc_status.ok()) {
    MarkParticipantHealthy(participant_id);
  } else {
    MarkParticipantUnhealthy(participant_id);
    return Status::IOError("Abort RPC failed: " + grpc_status.error_message());
  }

  return Status::OK();
}

Status DTXRpcClient::Inquire(
    NodeID participant_id,
    const std::string& txn_id,
    cedar::dtx::InquireResponse* response) {

  auto stub = GetStub(participant_id);
  if (!stub) {
    return Status::NotFound("Participant not found: " + std::to_string(participant_id));
  }

  cedar::dtx::InquireRequest request;
  request.set_txn_id(txn_id);

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(config_.rpc_timeout_ms);
  context.set_deadline(deadline);

  grpc::Status grpc_status = stub->Inquire(&context, request, response);

  if (!grpc_status.ok()) {
    return Status::IOError("Inquire RPC failed: " + grpc_status.error_message());
  }

  return Status::OK();
}

// =============================================================================
// Parallel Execution
// =============================================================================

std::vector<std::pair<NodeID, cedar::dtx::PrepareResponse>> DTXRpcClient::PrepareAll(
    const std::vector<NodeID>& participant_ids,
    const std::string& txn_id,
    const std::string& coordinator_id,
    uint64_t prepare_version,
    const std::vector<cedar::dtx::CedarKey>& reads,
    const std::vector<cedar::dtx::CedarKey>& writes,
    int32_t isolation_level,
    int64_t timeout_ms) {

  std::vector<std::pair<NodeID, cedar::dtx::PrepareResponse>> results;
  results.reserve(participant_ids.size());

  std::vector<std::future<void>> futures;
  futures.reserve(participant_ids.size());
  std::mutex results_mutex;

  for (NodeID participant_id : participant_ids) {
    auto promise = std::make_shared<std::promise<void>>();
    futures.push_back(promise->get_future());

    thread_pool_->Schedule([
        this, participant_id, &txn_id, &coordinator_id, prepare_version,
        &reads, &writes, isolation_level, timeout_ms, &results, &results_mutex,
        promise]() {
      try {
        cedar::dtx::PrepareResponse response;
        Status status = Prepare(participant_id, txn_id, coordinator_id, prepare_version,
                               reads, writes, isolation_level, timeout_ms, &response);
        if (!status.ok()) {
          response.set_success(false);
          response.set_error_msg(status.ToString());
        }
        std::lock_guard<std::mutex> lock(results_mutex);
        results.emplace_back(participant_id, std::move(response));
      } catch (...) {
        std::cerr << "[DtxRpcClient] Async task exception for participant " << participant_id << std::endl;
      }
      promise->set_value();
    });
  }

  for (auto& f : futures) {
    f.wait();
  }

  return results;
}

std::vector<std::pair<NodeID, cedar::dtx::CommitResponse>> DTXRpcClient::CommitAll(
    const std::vector<NodeID>& participant_ids,
    const std::string& txn_id,
    const std::string& coordinator_id,
    uint64_t commit_version) {

  std::vector<std::pair<NodeID, cedar::dtx::CommitResponse>> results;
  results.reserve(participant_ids.size());

  std::vector<std::future<void>> futures;
  futures.reserve(participant_ids.size());
  std::mutex results_mutex;

  for (NodeID participant_id : participant_ids) {
    auto promise = std::make_shared<std::promise<void>>();
    futures.push_back(promise->get_future());

    thread_pool_->Schedule([
        this, participant_id, &txn_id, &coordinator_id, commit_version,
        &results, &results_mutex, promise]() {
      try {
        cedar::dtx::CommitResponse response;
        Status status = Commit(participant_id, txn_id, coordinator_id, commit_version, &response);
        if (!status.ok()) {
          response.set_success(false);
          response.set_error_msg(status.ToString());
        }
        std::lock_guard<std::mutex> lock(results_mutex);
        results.emplace_back(participant_id, std::move(response));
      } catch (...) {
        std::cerr << "[DtxRpcClient] Async task exception for participant " << participant_id << std::endl;
      }
      promise->set_value();
    });
  }

  for (auto& f : futures) {
    f.wait();
  }

  return results;
}

std::vector<std::pair<NodeID, cedar::dtx::AbortResponse>> DTXRpcClient::AbortAll(
    const std::vector<NodeID>& participant_ids,
    const std::string& txn_id,
    const std::string& coordinator_id,
    const std::string& reason) {

  std::vector<std::pair<NodeID, cedar::dtx::AbortResponse>> results;
  results.reserve(participant_ids.size());

  std::vector<std::future<void>> futures;
  futures.reserve(participant_ids.size());
  std::mutex results_mutex;

  for (NodeID participant_id : participant_ids) {
    auto promise = std::make_shared<std::promise<void>>();
    futures.push_back(promise->get_future());

    thread_pool_->Schedule([
        this, participant_id, &txn_id, &coordinator_id, &reason,
        &results, &results_mutex, promise]() {
      try {
        cedar::dtx::AbortResponse response;
        Status status = Abort(participant_id, txn_id, coordinator_id, reason, &response);
        if (!status.ok()) {
          response.set_success(false);
          response.set_error_msg(status.ToString());
        }
        std::lock_guard<std::mutex> lock(results_mutex);
        results.emplace_back(participant_id, std::move(response));
      } catch (...) {
        std::cerr << "[DtxRpcClient] Async task exception for participant " << participant_id << std::endl;
      }
      promise->set_value();
    });
  }

  for (auto& f : futures) {
    f.wait();
  }

  return results;
}

// =============================================================================
// Health Management
// =============================================================================

bool DTXRpcClient::IsParticipantHealthy(NodeID id) const {
  std::lock_guard<std::mutex> lock(participants_mutex_);
  auto it = participants_.find(id);
  if (it != participants_.end()) {
    return it->second->available;
  }
  return false;
}

void DTXRpcClient::MarkParticipantUnhealthy(NodeID id) {
  std::lock_guard<std::mutex> lock(participants_mutex_);
  auto it = participants_.find(id);
  if (it != participants_.end()) {
    it->second->available = false;
    it->second->consecutive_failures++;
  }
}

void DTXRpcClient::MarkParticipantHealthy(NodeID id) {
  std::lock_guard<std::mutex> lock(participants_mutex_);
  auto it = participants_.find(id);
  if (it != participants_.end()) {
    it->second->available = true;
    it->second->consecutive_failures = 0;
    it->second->last_health_check = std::chrono::steady_clock::now();
  }
}

std::vector<NodeID> DTXRpcClient::GetHealthyParticipants() const {
  std::lock_guard<std::mutex> lock(participants_mutex_);
  std::vector<NodeID> healthy;
  for (const auto& [id, info] : participants_) {
    if (info->available) {
      healthy.push_back(id);
    }
  }
  return healthy;
}

size_t DTXRpcClient::GetParticipantCount() const {
  std::lock_guard<std::mutex> lock(participants_mutex_);
  return participants_.size();
}

// =============================================================================
// Private Helpers
// =============================================================================

std::shared_ptr<DTXService::Stub> DTXRpcClient::GetStub(NodeID id) {
  std::lock_guard<std::mutex> lock(participants_mutex_);
  auto it = participants_.find(id);
  if (it != participants_.end() && it->second->stub) {
    return it->second->stub;
  }
  return nullptr;
}

}  // namespace dtx
}  // namespace cedar
