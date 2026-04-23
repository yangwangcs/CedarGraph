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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gcn_service.pb.h"

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
  void RegisterPeer(const std::string& gcn_id,
                    std::shared_ptr<grpc::Channel> channel);

  // Send a SubQueryRequest to a target GCN.  For now this is a stub that
  // returns a default SubQueryResponse; real async RPC will be wired later.
  SubQueryResponse Scatter(const SubQueryRequest& req,
                           const std::string& target_gcn);

  // Merge multiple SubQueryResponses into a single TraversalResponse.
  // Sets the truncated flag if any sub-query was truncated.
  TraversalResponse Gather(std::vector<SubQueryResponse>& responses);

 private:
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> peers_;
};

}  // namespace gcn
}  // namespace cedar

#endif  // CEDAR_GCN_SCATTER_GATHER_ROUTER_H_
