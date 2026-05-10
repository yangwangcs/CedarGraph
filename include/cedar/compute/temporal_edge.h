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

#ifndef CEDAR_COMPUTE_TEMPORAL_EDGE_H_
#define CEDAR_COMPUTE_TEMPORAL_EDGE_H_

#include <cstdint>

namespace cedar {

enum class EdgeOperation : uint8_t {
  kCreate = 0,
  kDelete = 1,
  kUpdate = 2,
};

// 16B: four per 64B cache line
struct alignas(16) TemporalEdge {
  uint64_t target_id;   // 8B
  uint64_t timestamp;   // 8B: high bit stores operation flag

  static constexpr uint64_t kOpFlagMask = 0x8000000000000000ULL;

  EdgeOperation op() const {
    return (timestamp & kOpFlagMask) ? EdgeOperation::kDelete : EdgeOperation::kCreate;
  }
  void set_op(EdgeOperation op) {
    if (op == EdgeOperation::kDelete) {
      timestamp |= kOpFlagMask;
    } else {
      timestamp &= ~kOpFlagMask;
    }
  }
  uint64_t ts() const { return timestamp & ~kOpFlagMask; }
  void set_ts(uint64_t ts) {
    uint64_t op_bit = timestamp & kOpFlagMask;
    timestamp = (ts & ~kOpFlagMask) | op_bit;
  }
};

static_assert(sizeof(TemporalEdge) == 16, "TemporalEdge must be 16 bytes");
static_assert(alignof(TemporalEdge) == 16, "TemporalEdge must be 16-byte aligned");

}  // namespace cedar

#endif  // CEDAR_COMPUTE_TEMPORAL_EDGE_H_
