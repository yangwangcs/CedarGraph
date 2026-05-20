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

#ifndef CEDAR_GCN_COORDINATOR_CLIENT_H_
#define CEDAR_GCN_COORDINATOR_CLIENT_H_

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "cedar/coordinator/location_table.h"

namespace cedar {
namespace gcn {

// CoordinatorClient is the GCN-side client to the Coordinator (metad) for
// cache location lookups.  For now all methods are stubs that return
// hard-coded values; real gRPC calls will be wired in a later task.
class CoordinatorClient {
 public:
  explicit CoordinatorClient(std::shared_ptr<grpc::Channel> channel);

  void SetGcnNodeId(uint32_t node_id) { gcn_node_id_ = node_id; }

  // Query the coordinator for the cache window covering entity_id at
  // query_time.  Returns std::nullopt if no cache is registered.
  std::optional<coordinator::CacheWindow> Locate(uint64_t entity_id,
                                                  uint64_t query_time);

  // Report a locally-held cache window to the coordinator.
  void ReportCache(const coordinator::CacheWindow& window);

  // Send a heartbeat containing all currently cached windows.
  void Heartbeat(const std::vector<coordinator::CacheWindow>& windows);

 private:
  std::shared_ptr<grpc::Channel> channel_;
  uint32_t gcn_node_id_ = 0;
};

}  // namespace gcn
}  // namespace cedar

#endif  // CEDAR_GCN_COORDINATOR_CLIENT_H_
