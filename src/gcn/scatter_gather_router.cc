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
// ScatterGatherRouter - Distributed sub-query routing between GCNs
// =============================================================================

#include "cedar/gcn/scatter_gather_router.h"

#include <chrono>

namespace cedar {
namespace gcn {

ScatterGatherRouter::ScatterGatherRouter()
    : hash_ring_(cedar::storage::HashRingConfig{}) {}
ScatterGatherRouter::~ScatterGatherRouter() = default;

void ScatterGatherRouter::RegisterPeer(const std::string& gcn_id,
                                       std::shared_ptr<grpc::Channel> channel) {
  peers_[gcn_id] = std::move(channel);
  if (peers_[gcn_id]) {
    stubs_[gcn_id] = GcnService::NewStub(peers_[gcn_id]);
  }
  hash_ring_.AddNode(gcn_id);
}

void ScatterGatherRouter::UnregisterPeer(const std::string& gcn_id) {
  peers_.erase(gcn_id);
  stubs_.erase(gcn_id);
  hash_ring_.RemoveNode(gcn_id);
}

std::string ScatterGatherRouter::GetTargetGCN(uint64_t entity_id) {
  return hash_ring_.GetNode(std::to_string(entity_id));
}

SubQueryResponse ScatterGatherRouter::ScatterByEntity(const SubQueryRequest& req) {
  std::string target = GetTargetGCN(req.current_entity_id());
  if (target.empty()) {
    SubQueryResponse response;
    response.set_trace_id(req.trace_id());
    response.set_success(false);
    response.set_error_msg("No GCN available in hash ring");
    return response;
  }
  return Scatter(req, target);
}

size_t ScatterGatherRouter::PeerCount() const {
  return peers_.size();
}

SubQueryResponse ScatterGatherRouter::Scatter(const SubQueryRequest& req,
                                              const std::string& target_gcn) {
  SubQueryResponse response;
  response.set_trace_id(req.trace_id());

  auto stub_it = stubs_.find(target_gcn);
  if (stub_it == stubs_.end()) {
    response.set_success(false);
    response.set_error_msg("Unknown target GCN: " + target_gcn);
    return response;
  }

  // Backpressure check
  if (!backpressure_.AcquireSlot(target_gcn)) {
    response.set_success(false);
    response.set_error_msg("Backpressure: GCN " + target_gcn + " at capacity");
    return response;
  }

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  grpc::Status status = stub_it->second->SubQuery(&ctx, req, &response);

  backpressure_.ReleaseSlot(target_gcn);

  if (!status.ok()) {
    response.set_success(false);
    response.set_error_msg("RPC failed: " + status.error_message());
  } else {
    response.set_success(true);
  }

  return response;
}

TraversalResponse ScatterGatherRouter::Gather(
    std::vector<SubQueryResponse>& responses) {
  TraversalResponse merged;
  bool any_truncated = false;
  bool all_success = true;

  for (const auto& resp : responses) {
    if (!resp.trace_id().empty() && merged.trace_id().empty()) {
      merged.set_trace_id(resp.trace_id());
    }
    for (int i = 0; i < resp.next_entity_ids_size(); ++i) {
      merged.add_visited_entity_ids(resp.next_entity_ids(i));
    }
    if (resp.truncated()) {
      any_truncated = true;
    }
    if (!resp.success()) {
      all_success = false;
    }
  }

  merged.set_truncated(any_truncated);
  merged.set_success(all_success);
  return merged;
}

TraversalResponse ScatterGatherRouter::ScatterTraversal(
    const TraversalRequest& req, const std::string& target_gcn) {
  TraversalResponse response;

  auto stub_it = stubs_.find(target_gcn);
  if (stub_it == stubs_.end()) {
    response.set_success(false);
    response.set_error_msg("Unknown target GCN: " + target_gcn);
    return response;
  }

  if (!backpressure_.AcquireSlot(target_gcn)) {
    response.set_success(false);
    response.set_error_msg("Backpressure: GCN " + target_gcn + " at capacity");
    return response;
  }

  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  grpc::Status status = stub_it->second->Traverse(&ctx, req, &response);

  backpressure_.ReleaseSlot(target_gcn);

  if (!status.ok()) {
    response.set_success(false);
    response.set_error_msg("RPC failed: " + status.error_message());
  }

  return response;
}

TraversalResponse ScatterGatherRouter::ScatterTraversalByEntity(
    const TraversalRequest& req) {
  std::string target = GetTargetGCN(req.root_entity_id());
  if (target.empty()) {
    TraversalResponse response;
    response.set_success(false);
    response.set_error_msg("No GCN available in hash ring");
    return response;
  }
  return ScatterTraversal(req, target);
}

void ScatterGatherRouter::UpdatePeerHealth(const std::string& gcn_id,
                                           double health_score) {
  backpressure_.UpdateHealth(gcn_id, health_score);
}

}  // namespace gcn
}  // namespace cedar
