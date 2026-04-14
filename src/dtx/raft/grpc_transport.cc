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

#include "cedar/dtx/raft/grpc_transport.h"

#include <chrono>
#include <thread>
#include <iostream>

#include <grpcpp/create_channel.h>
#include "raft.grpc.pb.h"

namespace cedar {
namespace dtx {
namespace raft {

namespace grpc_pb = ::cedar::dtx::raft::grpc;

// =============================================================================
// Peer State Management
// =============================================================================

struct GrpcRaftTransport::PeerChannel {
  std::shared_ptr<::grpc::Channel> channel;
  std::unique_ptr<grpc_pb::RaftService::Stub> stub;
  std::atomic<bool> healthy{true};
};

struct GrpcRaftTransport::PeerState {
  std::string address;
  std::vector<std::unique_ptr<PeerChannel>> channels;
  std::atomic<size_t> round_robin_idx{0};
  std::atomic<size_t> failed_requests{0};
  std::atomic<size_t> successful_requests{0};
  std::chrono::steady_clock::time_point last_health_check;
};

// =============================================================================
// Helper functions for proto conversion
// =============================================================================

namespace {

void ToProto(const VoteRequest& src, grpc_pb::GrpcVoteRequest* dst) {
  dst->set_term(src.term);
  dst->set_candidate_id(src.candidate_id);
  dst->set_last_log_index(src.last_log_index);
  dst->set_last_log_term(src.last_log_term);
}

void FromProto(const grpc_pb::GrpcVoteResponse& src, VoteResponse* dst) {
  dst->term = src.term();
  dst->vote_granted = src.vote_granted();
}

void ToProto(const AppendEntriesRequest& src, grpc_pb::GrpcAppendEntriesRequest* dst) {
  dst->set_term(src.term);
  dst->set_leader_id(src.leader_id);
  dst->set_prev_log_index(src.prev_log_index);
  dst->set_prev_log_term(src.prev_log_term);
  dst->set_leader_commit(src.leader_commit);
  for (const auto& entry : src.entries) {
    auto* proto_entry = dst->add_entries();
    proto_entry->set_term(entry.term);
    proto_entry->set_index(entry.index);
    proto_entry->set_data(entry.data);
  }
}

void FromProto(const grpc_pb::GrpcAppendEntriesResponse& src, AppendEntriesResponse* dst) {
  dst->term = src.term();
  dst->success = src.success();
  dst->match_index = src.match_index();
}

void ToProto(const InstallSnapshotRequest& src, grpc_pb::GrpcInstallSnapshotRequest* dst) {
  dst->set_term(src.term);
  dst->set_leader_id(src.leader_id);
  dst->set_offset(src.offset);
  dst->set_done(src.done);
  dst->set_data(src.snapshot.data);
  auto* metadata = dst->mutable_metadata();
  metadata->set_last_included_term(src.snapshot.last_included_term);
  metadata->set_last_included_index(src.snapshot.last_included_index);
}

void FromProto(const grpc_pb::GrpcInstallSnapshotResponse& src, InstallSnapshotResponse* dst) {
  dst->term = src.term();
  dst->success = src.success();
}

}  // anonymous namespace

// =============================================================================
// GrpcRaftTransport Implementation
// =============================================================================

GrpcRaftTransport::GrpcRaftTransport(
    const Options& options,
    const std::vector<std::pair<NodeId, std::string>>& peers)
    : options_(options) {
  
  // Initialize credentials
  credentials_ = TlsCredentialFactory::CreateClientCredentials(options.tls_config);
  if (!credentials_) {
    std::cerr << "[GrpcTransport] Failed to create credentials, using insecure" << std::endl;
    credentials_ = ::grpc::InsecureChannelCredentials();
  }
  
  // Add initial peers
  for (const auto& [id, addr] : peers) {
    AddPeer(id, addr);
  }
}

GrpcRaftTransport::~GrpcRaftTransport() = default;

Status GrpcRaftTransport::SendVoteRequest(NodeId target, const VoteRequest& req,
                                          VoteResponse* resp) {
  auto* channel = GetChannel(target);
  if (!channel) {
    return Status::NotFound("Peer not found: " + std::to_string(target));
  }

  ::grpc::ClientContext context;
  auto deadline = std::chrono::system_clock::now() + 
                  std::chrono::milliseconds(options_.rpc_timeout_ms);
  context.set_deadline(deadline);

  grpc_pb::GrpcVoteRequest request;
  ToProto(req, &request);
  grpc_pb::GrpcVoteResponse response;

  ::grpc::Status status = channel->stub->RequestVote(&context, request, &response);
  
  // Update stats
  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    auto it = peers_.find(target);
    if (it != peers_.end()) {
      if (status.ok()) {
        it->second->successful_requests++;
        channel->healthy = true;
      } else {
        it->second->failed_requests++;
        channel->healthy = false;
      }
    }
  }
  
  if (!status.ok()) {
    return Status::IOError("RPC failed: " + status.error_message());
  }

  FromProto(response, resp);
  return Status::OK();
}

Status GrpcRaftTransport::SendAppendEntries(NodeId target, 
                                            const AppendEntriesRequest& req,
                                            AppendEntriesResponse* resp) {
  auto* channel = GetChannel(target);
  if (!channel) {
    return Status::NotFound("Peer not found: " + std::to_string(target));
  }

  ::grpc::ClientContext context;
  auto deadline = std::chrono::system_clock::now() + 
                  std::chrono::milliseconds(options_.rpc_timeout_ms);
  context.set_deadline(deadline);

  grpc_pb::GrpcAppendEntriesRequest request;
  ToProto(req, &request);
  grpc_pb::GrpcAppendEntriesResponse response;

  ::grpc::Status status = channel->stub->AppendEntries(&context, request, &response);
  
  // Update stats
  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    auto it = peers_.find(target);
    if (it != peers_.end()) {
      if (status.ok()) {
        it->second->successful_requests++;
        channel->healthy = true;
      } else {
        it->second->failed_requests++;
        channel->healthy = false;
      }
    }
  }
  
  if (!status.ok()) {
    return Status::IOError("RPC failed: " + status.error_message());
  }

  FromProto(response, resp);
  return Status::OK();
}

Status GrpcRaftTransport::SendInstallSnapshot(NodeId target,
                                              const InstallSnapshotRequest& req,
                                              InstallSnapshotResponse* resp) {
  auto* channel = GetChannel(target);
  if (!channel) {
    return Status::NotFound("Peer not found: " + std::to_string(target));
  }

  ::grpc::ClientContext context;
  auto deadline = std::chrono::system_clock::now() + 
                  std::chrono::milliseconds(options_.rpc_timeout_ms * 10);
  context.set_deadline(deadline);

  grpc_pb::GrpcInstallSnapshotResponse response;
  
  // InstallSnapshot is a client streaming RPC
  auto writer = channel->stub->InstallSnapshot(&context, &response);
  
  // For large snapshots, we should chunk the data
  constexpr size_t CHUNK_SIZE = 64 * 1024;  // 64KB chunks
  size_t offset = 0;
  bool done = false;
  
  while (offset < req.snapshot.data.size() || (offset == 0 && !done)) {
    grpc_pb::GrpcInstallSnapshotRequest request;
    
    // Set metadata on first chunk
    if (offset == 0) {
      request.set_term(req.term);
      request.set_leader_id(req.leader_id);
      auto* metadata = request.mutable_metadata();
      metadata->set_last_included_term(req.snapshot.last_included_term);
      metadata->set_last_included_index(req.snapshot.last_included_index);
    }
    
    // Set chunk data
    size_t chunk_end = std::min(offset + CHUNK_SIZE, req.snapshot.data.size());
    request.set_data(req.snapshot.data.substr(offset, chunk_end - offset));
    request.set_offset(offset);
    
    // Mark as done on last chunk
    done = (chunk_end >= req.snapshot.data.size());
    request.set_done(done);
    
    if (!writer->Write(request)) {
      return Status::IOError("Failed to write snapshot chunk at offset " + 
                              std::to_string(offset));
    }
    
    offset = chunk_end;
    if (done) break;
  }
  
  writer->WritesDone();
  ::grpc::Status status = writer->Finish();
  
  if (!status.ok()) {
    return Status::IOError("RPC failed: " + status.error_message());
  }

  FromProto(response, resp);
  return Status::OK();
}

std::vector<NodeId> GrpcRaftTransport::GetPeers() const {
  std::lock_guard<std::mutex> lock(peers_mutex_);
  std::vector<NodeId> result;
  for (const auto& [id, _] : peer_addresses_) {
    result.push_back(id);
  }
  return result;
}

std::string GrpcRaftTransport::GetNodeAddress(NodeId id) const {
  return GetPeerAddress(id);
}

Status GrpcRaftTransport::AddPeer(NodeId id, const std::string& address) {
  {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    peer_addresses_[id] = address;
  }

  // Create channels for new peer
  auto state = std::make_unique<PeerState>();
  state->address = address;
  state->last_health_check = std::chrono::steady_clock::now();
  
  for (size_t i = 0; i < options_.max_channels_per_peer; ++i) {
    auto channel = ::grpc::CreateChannel(address, credentials_);
    auto stub = grpc_pb::RaftService::NewStub(channel);
    auto pc = std::make_unique<PeerChannel>();
    pc->channel = channel;
    pc->stub = std::move(stub);
    state->channels.push_back(std::move(pc));
  }

  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    peers_[id] = std::move(state);
  }

  std::cout << "[GrpcTransport] Added peer " << id << " at " << address 
            << " with " << options_.max_channels_per_peer << " channels" << std::endl;
  return Status::OK();
}

Status GrpcRaftTransport::RemovePeer(NodeId id) {
  {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    peer_addresses_.erase(id);
  }
  {
    std::lock_guard<std::mutex> lock(channels_mutex_);
    peers_.erase(id);
  }
  std::cout << "[GrpcTransport] Removed peer " << id << std::endl;
  return Status::OK();
}

void GrpcRaftTransport::UpdateTlsConfig(const TlsConfig& config) {
  // Create new credentials
  auto new_creds = TlsCredentialFactory::CreateClientCredentials(config);
  if (new_creds) {
    credentials_ = new_creds;
    options_.tls_config = config;
    
    // Refresh all channels
    std::lock_guard<std::mutex> lock(channels_mutex_);
    for (auto& [id, state] : peers_) {
      RefreshChannels(id);
    }
  }
}

bool GrpcRaftTransport::IsPeerHealthy(NodeId id) const {
  std::lock_guard<std::mutex> lock(channels_mutex_);
  auto it = peers_.find(id);
  if (it == peers_.end()) return false;
  
  // Check if at least one channel is healthy
  for (const auto& ch : it->second->channels) {
    if (ch->healthy.load()) return true;
  }
  return false;
}

GrpcRaftTransport::ConnectionStats GrpcRaftTransport::GetStats(NodeId id) const {
  ConnectionStats stats{0, 0, 0, 0};
  
  std::lock_guard<std::mutex> lock(channels_mutex_);
  auto it = peers_.find(id);
  if (it != peers_.end()) {
    stats.total_channels = it->second->channels.size();
    stats.active_channels = 0;
    for (const auto& ch : it->second->channels) {
      if (ch->healthy.load()) stats.active_channels++;
    }
    stats.failed_requests = it->second->failed_requests.load();
    stats.successful_requests = it->second->successful_requests.load();
  }
  
  return stats;
}

GrpcRaftTransport::PeerChannel* GrpcRaftTransport::GetChannel(NodeId target) {
  std::lock_guard<std::mutex> lock(channels_mutex_);
  
  auto it = peers_.find(target);
  if (it == peers_.end() || it->second->channels.empty()) {
    return nullptr;
  }

  // Round-robin selection
  size_t idx = it->second->round_robin_idx.load();
  PeerChannel* result = it->second->channels[idx].get();
  it->second->round_robin_idx.store((idx + 1) % it->second->channels.size());
  return result;
}

std::string GrpcRaftTransport::GetPeerAddress(NodeId id) const {
  std::lock_guard<std::mutex> lock(peers_mutex_);
  auto it = peer_addresses_.find(id);
  if (it != peer_addresses_.end()) {
    return it->second;
  }
  return "";
}

void GrpcRaftTransport::RefreshChannels(NodeId id) {
  auto it = peers_.find(id);
  if (it == peers_.end()) return;
  
  // Recreate all channels with new credentials
  for (size_t i = 0; i < options_.max_channels_per_peer; ++i) {
    auto channel = ::grpc::CreateChannel(it->second->address, credentials_);
    auto stub = grpc_pb::RaftService::NewStub(channel);
    
    if (i < it->second->channels.size()) {
      it->second->channels[i]->channel = channel;
      it->second->channels[i]->stub = std::move(stub);
      it->second->channels[i]->healthy = true;
    }
  }
}

}  // namespace raft
}  // namespace dtx
}  // namespace cedar
