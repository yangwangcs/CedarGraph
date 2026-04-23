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

#ifndef CEDAR_COORDINATOR_LOCATION_TABLE_H_
#define CEDAR_COORDINATOR_LOCATION_TABLE_H_

#include <cstdint>
#include <optional>
#include <vector>

namespace cedar {
namespace coordinator {

struct CacheWindow {
  uint64_t entity_id;
  uint64_t cached_from;
  uint64_t cached_to;
  uint32_t gcn_node_id;
  uint64_t version;
  uint64_t expire_at;
};

class VertexLocationTable {
 public:
  VertexLocationTable() = default;
  ~VertexLocationTable() = default;

  // Non-copyable, non-movable
  VertexLocationTable(const VertexLocationTable&) = delete;
  VertexLocationTable& operator=(const VertexLocationTable&) = delete;

  std::optional<CacheWindow> Locate(uint64_t entity_id, uint64_t query_time) {
    // Stub implementation
    (void)entity_id;
    (void)query_time;
    return std::nullopt;
  }

  void ReportCache(const CacheWindow& window) {
    // Stub implementation
    (void)window;
  }

  void Heartbeat(const std::vector<CacheWindow>& windows) {
    // Stub implementation
    (void)windows;
  }
};

}  // namespace coordinator
}  // namespace cedar

#endif  // CEDAR_COORDINATOR_LOCATION_TABLE_H_
