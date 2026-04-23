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
// CoordinatorClient Implementation (stubs)
// =============================================================================

#include "cedar/gcn/coordinator_client.h"

namespace cedar {
namespace gcn {

CoordinatorClient::CoordinatorClient(std::shared_ptr<grpc::Channel> channel)
    : channel_(std::move(channel)) {}

std::optional<coordinator::CacheWindow> CoordinatorClient::Locate(
    uint64_t entity_id, uint64_t query_time) {
  // Stub: return a hard-coded window for entity 42.
  if (entity_id == 42) {
    coordinator::CacheWindow window;
    window.entity_id = 42;
    window.cached_from = 0;
    window.cached_to = 2000;
    window.gcn_node_id = 7;
    window.version = 3;
    window.expire_at = 3000;
    return window;
  }
  (void)query_time;
  return std::nullopt;
}

void CoordinatorClient::ReportCache(const coordinator::CacheWindow& window) {
  // Stub: no-op until real gRPC is wired.
  (void)window;
}

void CoordinatorClient::Heartbeat(
    const std::vector<coordinator::CacheWindow>& windows) {
  // Stub: no-op until real gRPC is wired.
  (void)windows;
}

}  // namespace gcn
}  // namespace cedar
