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
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/coordinator/location_table.h"
#include "cedar/dtx/meta_service.h"
#include "meta_service.grpc.pb.h"

namespace cedar {
namespace gcn {

// CoordinatorClient is the GCN-side client to the Coordinator (metad) for
// cache location lookups.
class CoordinatorClient {
 public:
  explicit CoordinatorClient(std::shared_ptr<grpc::Channel> channel);

  void SetGcnNodeId(uint32_t node_id) { gcn_node_id_ = node_id; }

  // Query the coordinator for the cache window covering entity_id at
  // query_time.  Returns std::nullopt if no cache is registered.
  std::optional<coordinator::CacheWindow> Locate(uint64_t entity_id,
                                                  uint64_t query_time);

  // Report a locally-held cache window to the coordinator.
  cedar::Status ReportCache(const coordinator::CacheWindow& window);

  // Send a heartbeat containing all currently cached windows.
  cedar::Status Heartbeat(const std::vector<coordinator::CacheWindow>& windows);

  cedar::Status RegisterGcn(uint64_t gcn_id,
                            const std::string& endpoint,
                            uint64_t incarnation);

  cedar::StatusOr<std::vector<cedar::dtx::GcnLease>> RenewGcnLeases(
      uint64_t gcn_id,
      uint64_t incarnation,
      const std::vector<cedar::dtx::GcnPartitionProgress>& progress);

  cedar::StatusOr<cedar::dtx::GcnRoute> LocateGcn(
      uint32_t partition_id,
      uint64_t required_version);

 private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::meta::MetaService::Stub> stub_;  // Cached stub
  uint32_t gcn_node_id_ = 0;
};

}  // namespace gcn
}  // namespace cedar

#endif  // CEDAR_GCN_COORDINATOR_CLIENT_H_
