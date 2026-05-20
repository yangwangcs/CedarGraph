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
// Backpressure Controller — Token-bucket based concurrency regulator
// =============================================================================
// Dynamically adjusts max outstanding requests per GCN based on health score
// (from Issue B MDHS). When a GCN is degraded, we reduce its token rate to
// prevent overload cascade.
// =============================================================================

#ifndef CEDAR_GCN_BACKPRESSURE_CONTROLLER_H_
#define CEDAR_GCN_BACKPRESSURE_CONTROLLER_H_

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace cedar {
namespace gcn {

class BackpressureController {
 public:
  struct Slot {
    std::atomic<uint32_t> in_use{0};
    std::atomic<uint32_t> max_concurrency{16};
  };

  BackpressureController() = default;

  // Try to acquire a slot for the given GCN. Returns false if at limit.
  bool AcquireSlot(const std::string& gcn_id);

  // Release a previously acquired slot.
  void ReleaseSlot(const std::string& gcn_id);

  // Adjust max concurrency based on health score (0-100).
  // score < 30 -> max = 4 (severely degraded)
  // score < 60 -> max = 8 (degraded)
  // otherwise    -> max = 16 (normal)
  void UpdateHealth(const std::string& gcn_id, double health_score);

  // Current outstanding requests for a GCN.
  uint32_t InFlight(const std::string& gcn_id) const;

 private:
  mutable std::mutex mutex_;
  std::unordered_map<std::string, Slot> slots_;
};

}  // namespace gcn
}  // namespace cedar

#endif  // CEDAR_GCN_BACKPRESSURE_CONTROLLER_H_
