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

#ifndef CEDAR_COMPUTE_EPOCH_CHUNK_H_
#define CEDAR_COMPUTE_EPOCH_CHUNK_H_

#include "cedar/compute/temporal_edge.h"
#include <cstdint>
#include <atomic>

namespace cedar {

struct EpochChunk {
  static constexpr size_t kCapacity = 65536;  // 1MB = 65536 * 16B

  std::atomic<uint64_t> min_timestamp{UINT64_MAX};
  std::atomic<uint64_t> max_timestamp{0};
  std::atomic<bool> is_sealed{false};
  std::atomic<size_t> event_count{0};
  TemporalEdge edges[kCapacity];
  EpochChunk* next_chunk = nullptr;

  bool CanAppend() const {
    return !is_sealed.load(std::memory_order_acquire) &&
           event_count.load(std::memory_order_acquire) < kCapacity;
  }

  // Lock-free append. Returns index on success, -1 on full.
  ssize_t Append(const TemporalEdge& edge) {
    size_t idx = event_count.fetch_add(1, std::memory_order_acq_rel);
    if (idx >= kCapacity) {
      event_count.fetch_sub(1, std::memory_order_relaxed);
      return -1;
    }
    edges[idx] = edge;
    // Update min/max timestamps (relaxed: monotonic write order guaranteed by caller)
    uint64_t ts = edge.ts();
    uint64_t prev_min = min_timestamp;
    while (ts < prev_min && !min_timestamp.compare_exchange_weak(prev_min, ts));
    uint64_t prev_max = max_timestamp;
    while (ts > prev_max && !max_timestamp.compare_exchange_weak(prev_max, ts));
    return static_cast<ssize_t>(idx);
  }

  void Seal() {
    is_sealed.store(true, std::memory_order_release);
  }
};

static_assert(sizeof(EpochChunk) <= 1024 * 1024 + 64,
              "EpochChunk should be ~1MB plus linkage overhead");

}  // namespace cedar

#endif  // CEDAR_COMPUTE_EPOCH_CHUNK_H_
