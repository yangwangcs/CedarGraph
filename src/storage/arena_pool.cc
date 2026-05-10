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

#include "cedar/compute/arena_pool.h"
#include <cstdlib>

namespace cedar {

ArenaPool::ArenaPool(size_t max_chunks) : total_chunks_(max_chunks) {
  chunks_.reserve(max_chunks);
  for (size_t i = 0; i < max_chunks; ++i) {
    // Allocate 1MB + padding aligned to 4KB page boundary
    void* mem = nullptr;
    if (posix_memalign(&mem, 4096, sizeof(EpochChunk)) != 0) {
      break;
    }
    auto* chunk = new (mem) EpochChunk();
    chunk->next_chunk = free_head_.load(std::memory_order_relaxed);
    while (!free_head_.compare_exchange_weak(
        chunk->next_chunk, chunk,
        std::memory_order_release,
        std::memory_order_relaxed));
    chunks_.push_back(chunk);
  }
}

ArenaPool::~ArenaPool() {
  for (EpochChunk* chunk : chunks_) {
    chunk->~EpochChunk();
    free(chunk);
  }
}

EpochChunk* ArenaPool::Alloc() {
  EpochChunk* head = free_head_.load(std::memory_order_acquire);
  while (head != nullptr) {
    EpochChunk* next = head->next_chunk;
    if (free_head_.compare_exchange_weak(
            head, next,
            std::memory_order_acquire,
            std::memory_order_relaxed)) {
      // Reset chunk state
      head->min_timestamp = UINT64_MAX;
      head->max_timestamp = 0;
      head->is_sealed.store(false, std::memory_order_relaxed);
      head->event_count.store(0, std::memory_order_relaxed);
      head->next_chunk = nullptr;
      return head;
    }
  }
  return nullptr;
}

void ArenaPool::Free(EpochChunk* chunk) {
  if (!chunk) return;
  chunk->next_chunk = free_head_.load(std::memory_order_relaxed);
  while (!free_head_.compare_exchange_weak(
      chunk->next_chunk, chunk,
      std::memory_order_release,
      std::memory_order_relaxed));
}

size_t ArenaPool::FreeCount() const {
  size_t count = 0;
  EpochChunk* p = free_head_.load(std::memory_order_acquire);
  while (p) {
    ++count;
    p = p->next_chunk;
  }
  return count;
}

}  // namespace cedar
