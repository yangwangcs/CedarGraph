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
// Raft RPC Client Implementation
// =============================================================================

#include "cedar/dtx/storage/raft_rpc_client.h"
#include "cedar/dtx/dtx_rpc_client.h"

#include "cedar/dtx/raft/grpc_tls.h"
#include <algorithm>
#include <chrono>
#include <thread>

namespace cedar {
namespace dtx {
namespace storage {

RaftRpcClient::RaftRpcClient() {
  options_ = Options();
  credentials_ = cedar::dtx::CreateClientCredentialsFromEnv();
}

RaftRpcClient::RaftRpcClient(const Options& options) : options_(options) {
  credentials_ = cedar::dtx::CreateClientCredentialsFromEnv();
}

RaftRpcClient::~RaftRpcClient() {
  Shutdown();
}

void RaftRpcClient::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  std::lock_guard<std::mutex> lock(peers_mutex_);
  connections_.clear();
}

Status RaftRpcClient::Initialize(
    const std::unordered_map<NodeID, std::string>& peer_addresses) {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("RaftRpcClient already initialized");
  }

  peer_addresses_ = peer_addresses;

  // Pre-create connections for all peers
  for (const auto& [node_id, address] : peer_addresses) {
    auto conn = std::make_unique<PeerConnection>();
    conn->channel = grpc::CreateCustomChannel(address, credentials_, grpc::ChannelArguments());
    conn->stub = cedar::raft::internal::PartitionRaftService::NewStub(conn->channel);
    conn->last_success = std::chrono::steady_clock::now();
    connections_[node_id] = std::move(conn);
  }

  return Status::OK();
}

Status RaftRpcClient::AddPeer(NodeID node_id, const std::string& address) {
  std::lock_guard<std::mutex> lock(peers_mutex_);

  peer_addresses_[node_id] = address;

  if (connections_.find(node_id) == connections_.end()) {
    auto conn = std::make_unique<PeerConnection>();
    conn->channel = grpc::CreateCustomChannel(address, credentials_, grpc::ChannelArguments());
    conn->stub = cedar::raft::internal::PartitionRaftService::NewStub(conn->channel);
    conn->last_success = std::chrono::steady_clock::now();
    connections_[node_id] = std::move(conn);
  }

  return Status::OK();
}

Status RaftRpcClient::RemovePeer(NodeID node_id) {
  std::lock_guard<std::mutex> lock(peers_mutex_);

  peer_addresses_.erase(node_id);
  connections_.erase(node_id);

  return Status::OK();
}

PeerConnection* RaftRpcClient::GetOrCreateConnection(NodeID node_id) {
  std::lock_guard<std::mutex> lock(peers_mutex_);

  auto it = connections_.find(node_id);
  if (it != connections_.end()) {
    return it->second.get();
  }

  // Create new connection
  auto address_it = peer_addresses_.find(node_id);
  if (address_it == peer_addresses_.end()) {
    return nullptr;
  }

  auto conn = std::make_unique<PeerConnection>();
  conn->channel = grpc::CreateCustomChannel(address_it->second, credentials_, grpc::ChannelArguments());
  conn->stub = cedar::raft::internal::PartitionRaftService::NewStub(conn->channel);
  conn->last_success = std::chrono::steady_clock::now();

  auto* ptr = conn.get();
  connections_[node_id] = std::move(conn);
  return ptr;
}

StatusOr<PeerConnection*> RaftRpcClient::GetConnection(NodeID node_id) {
  std::lock_guard<std::mutex> lock(peers_mutex_);

  auto it = connections_.find(node_id);
  if (it == connections_.end()) {
    return Status::NotFound("Peer not found: " + std::to_string(node_id));
  }

  return it->second.get();
}

StatusOr<RaftRpcClient::VoteResponse> RaftRpcClient::RequestVote(
    NodeID target,
    uint64_t term,
    uint32_t partition_id,
    uint64_t last_log_index,
    uint64_t last_log_term) {

  auto conn_result = GetConnection(target);
  if (!conn_result.ok()) {
    return conn_result.status();
  }

  auto* conn = conn_result.ValueOrDie();
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(options_.rpc_timeout_ms);
  context.set_deadline(deadline);

  cedar::raft::internal::VoteRequest request;
  request.set_term(term);
  request.set_partition_id(partition_id);
  request.set_last_log_index(last_log_index);
  request.set_last_log_term(last_log_term);

  cedar::raft::internal::VoteResponse response;

  grpc::Status grpc_status = conn->stub->RequestVote(&context, request, &response);

  if (grpc_status.ok()) {
    conn->last_success = std::chrono::steady_clock::now();
    conn->consecutive_failures.store(0);
    conn->healthy.store(true);

    VoteResponse result;
    result.term = response.term();
    result.vote_granted = response.vote_granted();
    result.voter_id = response.voter_id();
    return result;
  } else {
    conn->consecutive_failures++;
    if (conn->consecutive_failures.load() > 3) {
      conn->healthy.store(false);
    }

    return Status::IOError("RequestVote RPC failed: " + grpc_status.error_message());
  }
}

StatusOr<RaftRpcClient::AppendEntriesResponse> RaftRpcClient::AppendEntries(
    NodeID target,
    uint64_t term,
    uint32_t partition_id,
    uint64_t prev_log_index,
    uint64_t prev_log_term,
    uint64_t leader_commit,
    const std::vector<cedar::raft::internal::LogEntry>& entries) {

  auto conn_result = GetConnection(target);
  if (!conn_result.ok()) {
    return conn_result.status();
  }

  auto* conn = conn_result.ValueOrDie();
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(options_.rpc_timeout_ms);
  context.set_deadline(deadline);

  cedar::raft::internal::AppendEntriesRequest request;
  request.set_term(term);
  request.set_partition_id(partition_id);
  request.set_prev_log_index(prev_log_index);
  request.set_prev_log_term(prev_log_term);
  request.set_leader_commit(leader_commit);

  for (const auto& entry : entries) {
    *request.add_entries() = entry;
  }

  cedar::raft::internal::AppendEntriesResponse response;

  grpc::Status grpc_status = conn->stub->AppendEntries(&context, request, &response);

  if (grpc_status.ok()) {
    conn->last_success = std::chrono::steady_clock::now();
    conn->consecutive_failures.store(0);
    conn->healthy.store(true);

    AppendEntriesResponse result;
    result.term = response.term();
    result.success = response.success();
    result.match_index = response.match_index();
    result.follower_id = response.follower_id();
    return result;
  } else {
    conn->consecutive_failures++;
    if (conn->consecutive_failures.load() > 3) {
      conn->healthy.store(false);
    }

    return Status::IOError("AppendEntries RPC failed: " + grpc_status.error_message());
  }
}

StatusOr<RaftRpcClient::HeartbeatResponse> RaftRpcClient::Heartbeat(
    NodeID target,
    uint64_t term,
    uint32_t partition_id,
    uint64_t commit_index) {

  auto conn_result = GetConnection(target);
  if (!conn_result.ok()) {
    return conn_result.status();
  }

  auto* conn = conn_result.ValueOrDie();
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(options_.rpc_timeout_ms / 2);
  context.set_deadline(deadline);

  cedar::raft::internal::HeartbeatRequest request;
  request.set_term(term);
  request.set_partition_id(partition_id);
  request.set_commit_index(commit_index);

  cedar::raft::internal::HeartbeatResponse response;

  grpc::Status grpc_status = conn->stub->SendHeartbeat(&context, request, &response);

  if (grpc_status.ok()) {
    conn->last_success = std::chrono::steady_clock::now();
    conn->consecutive_failures.store(0);
    conn->healthy.store(true);

    HeartbeatResponse result;
    result.term = response.term();
    result.success = response.success();
    return result;
  } else {
    conn->consecutive_failures++;
    if (conn->consecutive_failures.load() > 3) {
      conn->healthy.store(false);
    }

    return Status::IOError("Heartbeat RPC failed: " + grpc_status.error_message());
  }
}

StatusOr<RaftRpcClient::SnapshotResponse> RaftRpcClient::InstallSnapshot(
    NodeID target,
    uint64_t term,
    uint32_t partition_id,
    uint64_t last_included_index,
    uint64_t last_included_term,
    uint64_t offset,
    const std::string& data,
    bool done) {

  auto conn_result = GetConnection(target);
  if (!conn_result.ok()) {
    return conn_result.status();
  }

  auto* conn = conn_result.ValueOrDie();
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto deadline = std::chrono::system_clock::now() +
                  std::chrono::milliseconds(options_.rpc_timeout_ms * 10);
  context.set_deadline(deadline);

  cedar::raft::internal::SnapshotRequest request;
  request.set_term(term);
  request.set_partition_id(partition_id);
  request.set_last_included_index(last_included_index);
  request.set_last_included_term(last_included_term);
  request.set_offset(offset);
  request.set_data(data);
  request.set_done(done);

  cedar::raft::internal::SnapshotResponse response;

  grpc::Status grpc_status = conn->stub->InstallSnapshot(&context, request, &response);

  if (grpc_status.ok()) {
    conn->last_success = std::chrono::steady_clock::now();
    conn->consecutive_failures.store(0);
    conn->healthy.store(true);

    SnapshotResponse result;
    result.term = response.term();
    result.success = response.success();
    return result;
  } else {
    conn->consecutive_failures++;
    if (conn->consecutive_failures.load() > 3) {
      conn->healthy.store(false);
    }

    return Status::IOError("InstallSnapshot RPC failed: " + grpc_status.error_message());
  }
}

bool RaftRpcClient::IsPeerHealthy(NodeID node_id) const {
  std::lock_guard<std::mutex> lock(peers_mutex_);

  auto it = connections_.find(node_id);
  if (it == connections_.end()) {
    return false;
  }

  return it->second->healthy.load();
}

StatusOr<RaftRpcClient::PeerStats> RaftRpcClient::GetPeerStats(NodeID node_id) const {
  std::lock_guard<std::mutex> lock(peers_mutex_);

  auto it = connections_.find(node_id);
  if (it == connections_.end()) {
    return Status::NotFound("Peer not found: " + std::to_string(node_id));
  }

  PeerStats stats;
  stats.healthy = it->second->healthy.load();
  stats.consecutive_failures = it->second->consecutive_failures.load();
  stats.last_success = it->second->last_success;
  return stats;
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
