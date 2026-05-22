#include "cedar/gcn/numa_arena.h"

#include <cstdlib>
#include <limits>
#include <new>

namespace cedar {
namespace gcn {

ArenaPool::ArenaPool(size_t max_chunks) : total_count_(max_chunks) {
  chunks_.reserve(max_chunks);
  for (size_t i = 0; i < max_chunks; ++i) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, 4096, sizeof(TMVChunk)) != 0) {
      throw std::bad_alloc();
    }
    TMVChunk* chunk = new (ptr) TMVChunk();
    chunks_.push_back(chunk);

    // Push onto free list (single-threaded during construction)
    chunk->next_freelist.store(free_head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    free_head_.store(chunk, std::memory_order_relaxed);
  }
  free_count_.store(max_chunks, std::memory_order_relaxed);
}

ArenaPool::~ArenaPool() {
  for (TMVChunk* chunk : chunks_) {
    chunk->~TMVChunk();
    free(chunk);
  }
}

TMVChunk* ArenaPool::Alloc() {
  TMVChunk* head = free_head_.load(std::memory_order_acquire);
  while (head != nullptr) {
    TMVChunk* next = head->next_freelist.load(std::memory_order_acquire);
    if (free_head_.compare_exchange_weak(
            head, next,
            std::memory_order_acquire,
            std::memory_order_relaxed)) {
      // Reset chunk state
      head->min_valid_from.store(std::numeric_limits<uint32_t>::max(),
                                 std::memory_order_relaxed);
      head->max_valid_to.store(0, std::memory_order_relaxed);
      head->event_count.store(0, std::memory_order_relaxed);
      head->sealed.store(false, std::memory_order_relaxed);
      head->next.store(nullptr, std::memory_order_relaxed);
      head->next_freelist.store(nullptr, std::memory_order_relaxed);
      free_count_.fetch_sub(1, std::memory_order_relaxed);
      return head;
    }
    // head was updated by another thread; retry with the new head
  }
  return nullptr;
}

void ArenaPool::Free(TMVChunk* chunk) {
  if (chunk == nullptr) {
    return;
  }
  TMVChunk* expected = free_head_.load(std::memory_order_relaxed);
  chunk->next_freelist.store(expected, std::memory_order_relaxed);
  while (!free_head_.compare_exchange_weak(
      expected, chunk,
      std::memory_order_release,
      std::memory_order_relaxed)) {
    // retry: another thread updated free_head_; update our next pointer
    chunk->next_freelist.store(expected, std::memory_order_relaxed);
  }
  free_count_.fetch_add(1, std::memory_order_relaxed);
}

size_t ArenaPool::FreeCount() const {
  return free_count_.load(std::memory_order_relaxed);
}

size_t ArenaPool::TotalCount() const {
  return total_count_;
}

}  // namespace gcn
}  // namespace cedar
