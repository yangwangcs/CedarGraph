#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>

#include "cedar/gcn/tmv_chunk.h"

namespace cedar {
namespace gcn {

struct TMVVertexEntry {
  uint64_t entity_id = 0;

  mutable std::mutex list_mutex;  // Protects linked list mutations

  std::atomic<TMVChunk*> out_chunk_head{nullptr};
  std::atomic<TMVChunk*> out_chunk_tail{nullptr};

  std::atomic<TMVChunk*> in_chunk_head{nullptr};
  std::atomic<TMVChunk*> in_chunk_tail{nullptr};

  std::atomic<uint64_t> out_edge_count{0};
  std::atomic<uint64_t> in_edge_count{0};

  std::atomic<uint32_t> earliest_chunk_timestamp{
      std::numeric_limits<uint32_t>::max()};

  TMVVertexEntry() = default;

  // Move constructor (required for std::map compatibility)
  TMVVertexEntry(TMVVertexEntry&& other) noexcept
      : entity_id(other.entity_id),
        out_chunk_head(
            other.out_chunk_head.load(std::memory_order_relaxed)),
        out_chunk_tail(
            other.out_chunk_tail.load(std::memory_order_relaxed)),
        in_chunk_head(
            other.in_chunk_head.load(std::memory_order_relaxed)),
        in_chunk_tail(
            other.in_chunk_tail.load(std::memory_order_relaxed)),
        out_edge_count(
            other.out_edge_count.load(std::memory_order_relaxed)),
        in_edge_count(
            other.in_edge_count.load(std::memory_order_relaxed)),
        earliest_chunk_timestamp(
            other.earliest_chunk_timestamp.load(std::memory_order_relaxed)) {}

  // Move assignment (required for flat_hash_map rehashing)
  TMVVertexEntry& operator=(TMVVertexEntry&& other) noexcept {
    if (this != &other) {
      entity_id = other.entity_id;
      out_chunk_head.store(
          other.out_chunk_head.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
      out_chunk_tail.store(
          other.out_chunk_tail.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
      in_chunk_head.store(
          other.in_chunk_head.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
      in_chunk_tail.store(
          other.in_chunk_tail.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
      out_edge_count.store(
          other.out_edge_count.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
      in_edge_count.store(
          other.in_edge_count.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
      earliest_chunk_timestamp.store(
          other.earliest_chunk_timestamp.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
    }
    return *this;
  }

  // Non-copyable
  TMVVertexEntry(const TMVVertexEntry&) = delete;
  TMVVertexEntry& operator=(const TMVVertexEntry&) = delete;
};

}  // namespace gcn
}  // namespace cedar
