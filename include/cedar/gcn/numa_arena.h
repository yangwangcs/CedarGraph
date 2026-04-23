#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "cedar/gcn/tmv_chunk.h"

namespace cedar {
namespace gcn {

class NumaArenaPool {
 public:
  explicit NumaArenaPool(size_t max_chunks);
  ~NumaArenaPool();

  // Non-copyable, non-movable
  NumaArenaPool(const NumaArenaPool&) = delete;
  NumaArenaPool& operator=(const NumaArenaPool&) = delete;

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
