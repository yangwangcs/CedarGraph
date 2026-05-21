#include "cedar/gcn/tmv_engine.h"

#include <algorithm>
#include <limits>

namespace cedar {
namespace gcn {

TMVEngine::TMVEngine(size_t max_chunks) : pool_(max_chunks) {}

cedar::Status TMVEngine::BootstrapVertex(uint64_t entity_id,
                                         Direction dir,
                                         const std::vector<TMVEdge>& edges,
                                         bool reverse) {
  TMVVertexEntry* entry = index_.FindOrCreate(entity_id);

  std::atomic<TMVChunk*>* head_ptr =
      (dir == Direction::kOut) ? &entry->out_chunk_head : &entry->in_chunk_head;
  std::atomic<TMVChunk*>* tail_ptr =
      (dir == Direction::kOut) ? &entry->out_chunk_tail : &entry->in_chunk_tail;
  std::atomic<uint64_t>* count_ptr = (dir == Direction::kOut)
                                         ? &entry->out_edge_count
                                         : &entry->in_edge_count;

  TMVChunk* first_chunk = nullptr;
  TMVChunk* prev_chunk = nullptr;
  size_t edge_idx = 0;
  size_t total_bootstrapped = 0;

  while (edge_idx < edges.size()) {
    TMVChunk* chunk = pool_.Alloc();
    if (!chunk) {
      // Rollback: free all previously staged chunks
      TMVChunk* rollback = first_chunk;
      while (rollback) {
        TMVChunk* next = rollback->next.load(std::memory_order_relaxed);
        pool_.Free(rollback);
        rollback = next;
      }
      return cedar::Status::ResourceExhausted("TMV pool exhausted during bootstrap");
    }

    chunk->next.store(nullptr, std::memory_order_relaxed);
    chunk->event_count.store(0, std::memory_order_relaxed);
    chunk->min_valid_from.store(std::numeric_limits<uint32_t>::max(),
                                std::memory_order_relaxed);
    chunk->max_valid_to.store(0, std::memory_order_relaxed);
    chunk->sealed.store(false, std::memory_order_relaxed);

    uint32_t min_from = std::numeric_limits<uint32_t>::max();
    uint32_t max_to = 0;
    size_t count = 0;

    while (edge_idx < edges.size() && count < TMVChunk::kCapacity) {
      chunk->edges[count] = edges[edge_idx];
      if (edges[edge_idx].valid_from < min_from) {
        min_from = edges[edge_idx].valid_from;
      }
      if (edges[edge_idx].valid_to > max_to) {
        max_to = edges[edge_idx].valid_to;
      }
      ++count;
      ++edge_idx;
    }

    chunk->event_count.store(static_cast<uint32_t>(count),
                             std::memory_order_relaxed);
    chunk->min_valid_from.store(min_from, std::memory_order_relaxed);
    chunk->max_valid_to.store(max_to, std::memory_order_relaxed);

    if (prev_chunk) {
      prev_chunk->next.store(chunk, std::memory_order_release);
    } else {
      first_chunk = chunk;
    }
    prev_chunk = chunk;
    total_bootstrapped += count;
  }

  if (first_chunk) {
    TMVChunk* old_tail = tail_ptr->exchange(prev_chunk, std::memory_order_relaxed);
    if (old_tail) {
      old_tail->next.store(first_chunk, std::memory_order_release);
    } else {
      head_ptr->store(first_chunk, std::memory_order_release);
    }

    count_ptr->fetch_add(total_bootstrapped, std::memory_order_relaxed);

    // Update earliest_chunk_timestamp
    uint32_t global_min = std::numeric_limits<uint32_t>::max();
    for (size_t i = 0; i < edges.size(); ++i) {
      if (edges[i].valid_from < global_min) {
        global_min = edges[i].valid_from;
      }
    }
    uint32_t expected =
        entry->earliest_chunk_timestamp.load(std::memory_order_relaxed);
    while (global_min < expected &&
           !entry->earliest_chunk_timestamp.compare_exchange_weak(
               expected, global_min, std::memory_order_relaxed,
               std::memory_order_relaxed)) {
      // retry
    }
  }

  if (reverse && dir == Direction::kOut) {
    for (const auto& edge : edges) {
      TMVEdge reverse_edge = edge;
      reverse_edge.target_id = entity_id;
      cedar::Status s = AppendEdge(edge.target_id, Direction::kIn, reverse_edge, false);
      if (!s.ok()) {
        return s;
      }
    }
  }

  return cedar::Status::OK();
}

cedar::Status TMVEngine::AppendEdge(uint64_t entity_id,
                                    Direction dir,
                                    const TMVEdge& edge,
                                    bool reverse) {
  TMVVertexEntry* entry = index_.FindOrCreate(entity_id);
  TMVChunk* new_chunk = nullptr;
  TMVChunk* old_tail = nullptr;
  if (!AppendToEntry(entry, dir, edge, &new_chunk, &old_tail)) {
    return cedar::Status::ResourceExhausted("TMV pool exhausted");
  }

  if (reverse && dir == Direction::kOut) {
    TMVEdge reverse_edge = edge;
    reverse_edge.target_id = entity_id;
    cedar::Status s = AppendEdge(edge.target_id, Direction::kIn, reverse_edge, false);
    if (!s.ok()) {
      if (new_chunk) {
        // Rollback: unlink and free the forward edge chunk
        std::atomic<TMVChunk*>* head_ptr =
            (dir == Direction::kOut) ? &entry->out_chunk_head : &entry->in_chunk_head;
        std::atomic<TMVChunk*>* tail_ptr =
            (dir == Direction::kOut) ? &entry->out_chunk_tail : &entry->in_chunk_tail;
        std::atomic<uint64_t>* count_ptr = (dir == Direction::kOut)
                                               ? &entry->out_edge_count
                                               : &entry->in_edge_count;

        if (old_tail) {
          tail_ptr->store(old_tail, std::memory_order_relaxed);
          old_tail->next.store(nullptr, std::memory_order_release);
        } else {
          head_ptr->store(nullptr, std::memory_order_relaxed);
          tail_ptr->store(nullptr, std::memory_order_relaxed);
        }
        count_ptr->fetch_sub(1, std::memory_order_relaxed);
        pool_.Free(new_chunk);
      }
      return s;
    }
  }

  return cedar::Status::OK();
}

bool TMVEngine::AppendToEntry(TMVVertexEntry* entry,
                              Direction dir,
                              const TMVEdge& edge,
                              TMVChunk** out_new_chunk,
                              TMVChunk** out_old_tail) {
  std::atomic<TMVChunk*>* head_ptr =
      (dir == Direction::kOut) ? &entry->out_chunk_head : &entry->in_chunk_head;
  std::atomic<TMVChunk*>* tail_ptr =
      (dir == Direction::kOut) ? &entry->out_chunk_tail : &entry->in_chunk_tail;
  std::atomic<uint64_t>* count_ptr = (dir == Direction::kOut)
                                         ? &entry->out_edge_count
                                         : &entry->in_edge_count;

  TMVChunk* tail = tail_ptr->load(std::memory_order_relaxed);
  if (tail && tail->CanAppend()) {
    int idx = tail->Append(edge);
    if (idx >= 0) {
      count_ptr->fetch_add(1, std::memory_order_relaxed);

      uint32_t expected =
          entry->earliest_chunk_timestamp.load(std::memory_order_relaxed);
      while (edge.valid_from < expected &&
             !entry->earliest_chunk_timestamp.compare_exchange_weak(
                 expected, edge.valid_from, std::memory_order_relaxed,
                 std::memory_order_relaxed)) {
        // retry
      }
      return true;
    }
  }

  // Allocate a new chunk
  TMVChunk* new_chunk = pool_.Alloc();
  if (!new_chunk) {
    return false;
  }

  new_chunk->next.store(nullptr, std::memory_order_relaxed);
  new_chunk->event_count.store(0, std::memory_order_relaxed);
  new_chunk->min_valid_from.store(std::numeric_limits<uint32_t>::max(),
                                  std::memory_order_relaxed);
  new_chunk->max_valid_to.store(0, std::memory_order_relaxed);
  new_chunk->sealed.store(false, std::memory_order_relaxed);

  int idx = new_chunk->Append(edge);
  if (idx < 0) {
    pool_.Free(new_chunk);
    return false;
  }

  // Link the new chunk
  TMVChunk* old_tail = tail_ptr->exchange(new_chunk, std::memory_order_relaxed);
  if (old_tail) {
    old_tail->next.store(new_chunk, std::memory_order_release);
  } else {
    head_ptr->store(new_chunk, std::memory_order_release);
  }

  count_ptr->fetch_add(1, std::memory_order_relaxed);

  uint32_t expected =
      entry->earliest_chunk_timestamp.load(std::memory_order_relaxed);
  while (edge.valid_from < expected &&
         !entry->earliest_chunk_timestamp.compare_exchange_weak(
             expected, edge.valid_from, std::memory_order_relaxed,
             std::memory_order_relaxed)) {
    // retry
  }

  if (out_new_chunk) {
    *out_new_chunk = new_chunk;
  }
  if (out_old_tail) {
    *out_old_tail = old_tail;
  }
  return true;
}

std::vector<TMVEdge> TMVEngine::ScanAtTime(uint64_t entity_id,
                                           Direction dir,
                                           uint64_t query_time) {
  std::vector<TMVEdge> candidates;

  uint32_t shard_idx = static_cast<uint32_t>(entity_id) & (TMVIndex::kNumShards - 1);
  TMVIndex::Shard& shard = index_.shards_[shard_idx];
  absl::base_internal::SpinLockHolder holder{shard.lock};

  auto it = shard.entries.find(entity_id);
  if (it == shard.entries.end()) {
    return candidates;
  }
  TMVVertexEntry* entry = &it->second;

  TMVChunk* head = (dir == Direction::kOut)
                       ? entry->out_chunk_head.load(std::memory_order_acquire)
                       : entry->in_chunk_head.load(std::memory_order_acquire);

  // Saturate query_time to uint32_t range to match TMVEdge timestamp fields.
  // This prevents the chunk-level filter from incorrectly skipping all chunks
  // when query_time exceeds UINT32_MAX.
  const uint32_t effective_query_time =
      (query_time > std::numeric_limits<uint32_t>::max())
          ? std::numeric_limits<uint32_t>::max()
          : static_cast<uint32_t>(query_time);

  // Step 1 & 2: Chunk skip + linear scan, collect edges where valid_from <= query_time
  TMVChunk* chunk = head;
  while (chunk) {
    uint32_t min_from = chunk->min_valid_from.load(std::memory_order_relaxed);
    uint32_t max_to = chunk->max_valid_to.load(std::memory_order_relaxed);

    // Chunk-level filter: skip if chunk cannot contain any valid edge
    if (min_from > effective_query_time || max_to < effective_query_time) {
      chunk = chunk->next.load(std::memory_order_acquire);
      continue;
    }

    uint32_t count = chunk->event_count.load(std::memory_order_acquire);
    for (uint32_t i = 0; i < count; ++i) {
      const TMVEdge& edge = chunk->edges[i];
      if (edge.valid_from <= effective_query_time) {
        candidates.push_back(edge);
      }
    }
    chunk = chunk->next.load(std::memory_order_acquire);
  }

  // Step 3: Sort by target_id
  std::sort(candidates.begin(), candidates.end(),
            [](const TMVEdge& a, const TMVEdge& b) {
              return a.target_id < b.target_id;
            });

  // Step 4 & 5: Group by target_id, keep only the one with largest valid_from.
  // If the kept edge's valid_to != MAX and valid_to <= query_time, discard it.
  std::vector<TMVEdge> result;
  result.reserve(candidates.size());

  for (size_t i = 0; i < candidates.size();) {
    uint64_t target_id = candidates[i].target_id;
    size_t best_idx = i;

    // Find the edge with largest valid_from for this target_id
    size_t j = i + 1;
    while (j < candidates.size() && candidates[j].target_id == target_id) {
      if (candidates[j].valid_from > candidates[best_idx].valid_from) {
        best_idx = j;
      }
      ++j;
    }

    const TMVEdge& best = candidates[best_idx];
    // Discard if deleted (valid_to != MAX and valid_to <= query_time)
    if (best.valid_to == std::numeric_limits<uint32_t>::max() ||
        best.valid_to > query_time) {
      result.push_back(best);
    }

    i = j;
  }

  return result;
}

size_t TMVEngine::DropChunksBelowWatermark(std::atomic<TMVChunk*>* head_ptr,
                                           std::atomic<TMVChunk*>* tail_ptr,
                                           uint64_t watermark) {
  TMVChunk* head = head_ptr->load(std::memory_order_relaxed);
  if (!head) {
    return 0;
  }

  TMVChunk* prev = nullptr;
  TMVChunk* curr = head;
  TMVChunk* new_head = nullptr;
  TMVChunk* new_tail = nullptr;
  size_t dropped = 0;

  while (curr) {
    TMVChunk* next = curr->next.load(std::memory_order_acquire);
    if (curr->max_valid_to.load(std::memory_order_relaxed) < watermark) {
      if (prev) {
        prev->next.store(next, std::memory_order_release);
      }
      pool_.Free(curr);
      ++dropped;
    } else {
      if (!new_head) {
        new_head = curr;
      }
      new_tail = curr;
      prev = curr;
    }
    curr = next;
  }

  head_ptr->store(new_head, std::memory_order_release);
  tail_ptr->store(new_tail, std::memory_order_release);

  return dropped;
}

size_t TMVEngine::DropBelowWatermark(uint64_t watermark) {
  size_t dropped = 0;

  for (uint32_t s = 0; s < TMVIndex::kNumShards; ++s) {
    TMVIndex::Shard& shard = index_.shards_[s];
    absl::base_internal::SpinLockHolder holder{shard.lock};

    for (auto& [id, entry] : shard.entries) {
      dropped +=
          DropChunksBelowWatermark(&entry.out_chunk_head, &entry.out_chunk_tail,
                                   watermark);
      dropped +=
          DropChunksBelowWatermark(&entry.in_chunk_head, &entry.in_chunk_tail,
                                   watermark);
    }
  }

  return dropped;
}

size_t TMVEngine::InvalidateVertex(uint64_t entity_id) {
  uint32_t shard_idx = static_cast<uint32_t>(entity_id) & (TMVIndex::kNumShards - 1);
  TMVIndex::Shard& shard = index_.shards_[shard_idx];
  absl::base_internal::SpinLockHolder holder{shard.lock};

  auto it = shard.entries.find(entity_id);
  if (it == shard.entries.end()) {
    return 0;
  }

  size_t freed = 0;
  // Free outgoing edge chunks
  TMVChunk* chunk = it->second.out_chunk_head.load(std::memory_order_relaxed);
  while (chunk) {
    TMVChunk* next = chunk->next.load(std::memory_order_acquire);
    pool_.Free(chunk);
    ++freed;
    chunk = next;
  }
  // Free incoming edge chunks
  chunk = it->second.in_chunk_head.load(std::memory_order_relaxed);
  while (chunk) {
    TMVChunk* next = chunk->next.load(std::memory_order_acquire);
    pool_.Free(chunk);
    ++freed;
    chunk = next;
  }

  shard.entries.erase(it);
  return freed;
}

size_t TMVEngine::VertexCount() const {
  size_t count = 0;
  for (uint32_t s = 0; s < TMVIndex::kNumShards; ++s) {
    const TMVIndex::Shard& shard = index_.shards_[s];
    absl::base_internal::SpinLockHolder holder{shard.lock};
    count += shard.entries.size();
  }
  return count;
}

size_t TMVEngine::ChunkCount() const {
  return pool_.TotalCount() - pool_.FreeCount();
}

}  // namespace gcn
}  // namespace cedar
