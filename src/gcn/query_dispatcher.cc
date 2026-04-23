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
// QueryDispatcher Implementation
// =============================================================================

#include "cedar/gcn/query_dispatcher.h"

namespace cedar {
namespace gcn {

QueryDispatcher::QueryDispatcher(TMVEngine* engine) : engine_(engine) {}

grpc::Status QueryDispatcher::DispatchTraversal(const TraversalRequest& req,
                                                TraversalResponse* resp) {
  resp->Clear();

  if (!engine_) {
    resp->set_success(false);
    resp->set_error_msg("TMVEngine not available");
    return grpc::Status::OK;
  }

  // Scan outbound edges at the requested query time.
  // TraversalRequest does not carry a direction field; outbound is the
  // default traversal direction.
  std::vector<TMVEdge> edges =
      engine_->ScanAtTime(req.root_entity_id(), Direction::kOut,
                          req.query_time());

  if (!edges.empty()) {
    // Local hit: populate response with reachable entity ids
    resp->set_success(true);
    resp->set_trace_id(req.trace_id());
    for (const auto& edge : edges) {
      resp->add_visited_entity_ids(edge.target_id);
    }
  } else {
    // Local miss: return empty response.
    // Bootstrap-then-retry logic will be added in Task 4.x.
    resp->set_success(false);
  }

  return grpc::Status::OK;
}

}  // namespace gcn
}  // namespace cedar
