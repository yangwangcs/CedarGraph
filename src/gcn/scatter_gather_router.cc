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

namespace cedar {
namespace gcn {

ScatterGatherRouter::ScatterGatherRouter() = default;
ScatterGatherRouter::~ScatterGatherRouter() = default;

void ScatterGatherRouter::RegisterPeer(const std::string& gcn_id,
                                       std::shared_ptr<grpc::Channel> channel) {
  peers_[gcn_id] = std::move(channel);
}

SubQueryResponse ScatterGatherRouter::Scatter(const SubQueryRequest& req,
                                              const std::string& /*target_gcn*/) {
  // Stub: return a default response.  Real RPC dispatch will be added later.
  SubQueryResponse response;
  response.set_trace_id(req.trace_id());
  return response;
}

TraversalResponse ScatterGatherRouter::Gather(std::vector<SubQueryResponse>& responses) {
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

}  // namespace gcn
}  // namespace cedar
