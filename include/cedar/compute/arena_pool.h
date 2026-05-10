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

#ifndef CEDAR_COMPUTE_ARENA_POOL_H_
#define CEDAR_COMPUTE_ARENA_POOL_H_

#include "cedar/compute/epoch_chunk.h"
#include "cedar/core/status.h"
#include <vector>
#include <atomic>
#include <mutex>

namespace cedar {

class ArenaPool {
 public:
  explicit ArenaPool(size_t max_chunks);
  ~ArenaPool();

  ArenaPool(const ArenaPool&) = delete;
  ArenaPool& operator=(const ArenaPool&) = delete;

  // Allocate a zeroed EpochChunk. Returns nullptr if at capacity.
  EpochChunk* Alloc();

  // Return a chunk to the free list. Thread-safe.
  void Free(EpochChunk* chunk);

  size_t FreeCount() const;
  size_t TotalCount() const { return total_chunks_; }

 private:
  std::vector<EpochChunk*> chunks_;       // owned backing memory
  std::atomic<EpochChunk*> free_head_{nullptr};
  size_t total_chunks_ = 0;
};

}  // namespace cedar

#endif  // CEDAR_COMPUTE_ARENA_POOL_H_
