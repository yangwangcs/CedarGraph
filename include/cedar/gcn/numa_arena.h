#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "cedar/gcn/tmv_chunk.h"

namespace cedar {
namespace gcn {

class ArenaPool {
 public:
  explicit ArenaPool(size_t max_chunks);
  ~ArenaPool();

  // Non-copyable, non-movable
  ArenaPool(const ArenaPool&) = delete;
  ArenaPool& operator=(const ArenaPool&) = delete;

  TMVChunk* Alloc();
  void Free(TMVChunk* chunk);

  size_t FreeCount() const;
  size_t TotalCount() const;

 private:
  size_t total_count_;
  std::atomic<size_t> free_count_{0};
  std::atomic<TMVChunk*> free_head_{nullptr};
  std::vector<TMVChunk*> chunks_;
};

}  // namespace gcn
}  // namespace cedar
