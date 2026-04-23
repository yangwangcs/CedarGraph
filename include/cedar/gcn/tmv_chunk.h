#pragma once

#include <atomic>
#include <cstdint>
#include <limits>

#include "cedar/gcn/tmv_edge.h"

namespace cedar {
namespace gcn {

struct alignas(4096) TMVChunk {
  static constexpr uint32_t kCapacity = 65536;

  // Metadata
  std::atomic<uint32_t> min_valid_from{std::numeric_limits<uint32_t>::max()};
  std::atomic<uint32_t> max_valid_to{0};
  std::atomic<uint32_t> event_count{0};
  std::atomic<bool> sealed{false};
  char pad[9];

  // Pointers
  TMVChunk* next = nullptr;
  TMVChunk* next_freelist = nullptr;

  // Data
  TMVEdge edges[kCapacity];

  bool CanAppend() const {
    if (sealed.load(std::memory_order_acquire)) {
      return false;
    }
    uint32_t count = event_count.load(std::memory_order_acquire);
    return count < kCapacity;
  }

  int Append(const TMVEdge& edge) {
    if (sealed.load(std::memory_order_acquire)) {
      return -1;
    }

    uint32_t idx = event_count.fetch_add(1, std::memory_order_acq_rel);
    if (idx >= kCapacity) {
      return -1;
    }

    edges[idx] = edge;

    // Update min_valid_from with CAS
    uint32_t expected_min = min_valid_from.load(std::memory_order_relaxed);
    while (expected_min > edge.valid_from &&
           !min_valid_from.compare_exchange_weak(
               expected_min, edge.valid_from,
               std::memory_order_relaxed,
               std::memory_order_relaxed)) {
      // retry
    }

    // Update max_valid_to with CAS
    uint32_t expected_max = max_valid_to.load(std::memory_order_relaxed);
    while (expected_max < edge.valid_to &&
           !max_valid_to.compare_exchange_weak(
               expected_max, edge.valid_to,
               std::memory_order_relaxed,
               std::memory_order_relaxed)) {
      // retry
    }

    return static_cast<int>(idx);
  }

  void Seal() {
    sealed.store(true, std::memory_order_release);
  }
};

static_assert(alignof(TMVChunk) == 4096, "TMVChunk must be 4096-byte aligned");

}  // namespace gcn
}  // namespace cedar
