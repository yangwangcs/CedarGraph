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

#ifndef CEDAR_GCN_SCATTER_GATHER_ROUTER_H_
#define CEDAR_GCN_SCATTER_GATHER_ROUTER_H_

#include <grpcpp/grpcpp.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/gcn/backpressure_controller.h"
#include "cedar/storage/consistent_hash_ring.h"
#include "gcn_service.grpc.pb.h"

namespace cedar {
namespace gcn {

class ScatterGatherRouter {
 public:
  ScatterGatherRouter();
  ~ScatterGatherRouter();

  // Disable copy
  ScatterGatherRouter(const ScatterGatherRouter&) = delete;
  ScatterGatherRouter& operator=(const ScatterGatherRouter&) = delete;

  // Register a gRPC channel to a peer GCN.
  // Also updates the consistent-hash ring for entity-based routing.
  void RegisterPeer(const std::string& gcn_id,
                    std::shared_ptr<grpc::Channel> channel);

  // Remove a peer from the router and hash ring.
  void UnregisterPeer(const std::string& gcn_id);

  // Send a SubQueryRequest to an explicitly named target GCN.
  // Respects backpressure: if the target GCN is at capacity, returns a
  // response with success=false and an error_msg indicating backpressure.
  SubQueryResponse Scatter(const SubQueryRequest& req,
                           const std::string& target_gcn);

  // Send a SubQueryRequest to the GCN responsible for the entity
  // (via consistent hashing). Returns backpressure response if the
  // hashed GCN is at capacity.
  SubQueryResponse ScatterByEntity(const SubQueryRequest& req);

  // Send a TraversalRequest to an explicitly named target GCN.
  TraversalResponse ScatterTraversal(const TraversalRequest& req,
                                     const std::string& target_gcn);

  // Send a TraversalRequest to the GCN responsible for the root entity
  // (via consistent hashing).
  TraversalResponse ScatterTraversalByEntity(const TraversalRequest& req);

  // Look up the target GCN for an entity_id using the consistent-hash ring.
  std::string GetTargetGCN(uint64_t entity_id);

  // Merge multiple SubQueryResponses into a single TraversalResponse.
  // Sets the truncated flag if any sub-query was truncated.
  TraversalResponse Gather(std::vector<SubQueryResponse>& responses);

  // Update health score for a peer GCN (from Issue B MDHS).
  void UpdatePeerHealth(const std::string& gcn_id, double health_score);

  size_t PeerCount() const;

 private:
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> peers_;
  std::unordered_map<std::string, std::unique_ptr<GcnService::Stub>> stubs_;
  BackpressureController backpressure_;
  cedar::storage::ConsistentHashRing hash_ring_;
};

}  // namespace gcn
}  // namespace cedar

#endif  // CEDAR_GCN_SCATTER_GATHER_ROUTER_H_
