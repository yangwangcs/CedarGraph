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
  std::lock_guard<std::mutex> append_guard(append_mutex_);
  TMVVertexEntry* entry = index_.FindOrCreate(entity_id);

  {
    std::lock_guard<std::mutex> guard(entry->list_mutex);

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
      TMVChunk* old_tail =
          tail_ptr->exchange(prev_chunk, std::memory_order_relaxed);
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
  }

  if (reverse && dir == Direction::kOut) {
    for (const auto& edge : edges) {
      TMVEdge reverse_edge = edge;
      reverse_edge.target_id = entity_id;
      TMVVertexEntry* reverse_entry = index_.FindOrCreate(edge.target_id);
      if (!AppendToEntry(reverse_entry, Direction::kIn, reverse_edge)) {
        return cedar::Status::ResourceExhausted("TMV pool exhausted");
      }
    }
  }

  return cedar::Status::OK();
}

cedar::Status TMVEngine::AppendEdge(uint64_t entity_id,
                                    Direction dir,
                                    const TMVEdge& edge,
                                    bool reverse) {
  return AppendEdgesAtomic({EdgeAppend{entity_id, dir, edge, reverse}});
}

cedar::Status TMVEngine::AppendEdgesAtomic(
    const std::vector<EdgeAppend>& appends) {
  std::lock_guard<std::mutex> append_guard(append_mutex_);
  std::vector<AppendMutation> mutations;
  mutations.reserve(appends.size() * 2);
  std::vector<uint64_t> created_entries;
  created_entries.reserve(appends.size() * 2);
  auto rollback = [&]() {
    for (auto it = mutations.rbegin(); it != mutations.rend(); ++it) {
      RollbackAppend(*it);
    }
    for (auto it = created_entries.rbegin(); it != created_entries.rend();
         ++it) {
      RemoveEntryIfEmpty(*it);
    }
  };

  for (const auto& append : appends) {
    bool created = false;
    TMVVertexEntry* entry = FindOrCreateEntry(append.entity_id, &created);
    if (created) {
      created_entries.push_back(append.entity_id);
    }
    AppendMutation mutation;
    if (!AppendToEntry(entry, append.dir, append.edge, &mutation)) {
      rollback();
      return cedar::Status::ResourceExhausted("TMV pool exhausted");
    }
    mutations.push_back(mutation);

    if (append.reverse && append.dir == Direction::kOut) {
      TMVEdge reverse_edge = append.edge;
      reverse_edge.target_id = append.entity_id;
      bool reverse_created = false;
      TMVVertexEntry* reverse_entry =
          FindOrCreateEntry(append.edge.target_id, &reverse_created);
      if (reverse_created) {
        created_entries.push_back(append.edge.target_id);
      }
      AppendMutation reverse_mutation;
      if (!AppendToEntry(reverse_entry, Direction::kIn, reverse_edge,
                         &reverse_mutation)) {
        rollback();
        return cedar::Status::ResourceExhausted("TMV pool exhausted");
      }
      mutations.push_back(reverse_mutation);
    }
  }

  return cedar::Status::OK();
}

cedar::Status TMVEngine::ReplaceVerticesAtomic(
    const std::set<uint64_t>& entity_ids,
    const std::vector<EdgeAppend>& appends) {
  std::lock_guard<std::mutex> append_guard(append_mutex_);
  std::vector<VertexBackup> backups;
  backups.reserve(entity_ids.size());
  for (uint64_t entity_id : entity_ids) {
    backups.push_back(BackupVertex(entity_id));
  }
  for (uint64_t entity_id : entity_ids) {
    InvalidateVertex(entity_id);
  }

  std::vector<AppendMutation> mutations;
  mutations.reserve(appends.size() * 2);
  std::vector<uint64_t> created_entries;
  created_entries.reserve(appends.size() * 2);
  auto rollback = [&]() {
    for (auto it = mutations.rbegin(); it != mutations.rend(); ++it) {
      RollbackAppend(*it);
    }
    for (auto it = created_entries.rbegin(); it != created_entries.rend();
         ++it) {
      RemoveEntryIfEmpty(*it);
    }
  };

  for (const auto& append : appends) {
    bool created = false;
    TMVVertexEntry* entry = FindOrCreateEntry(append.entity_id, &created);
    if (created) {
      created_entries.push_back(append.entity_id);
    }
    AppendMutation mutation;
    if (!AppendToEntry(entry, append.dir, append.edge, &mutation)) {
      rollback();
      for (const auto& backup : backups) {
        if (!RestoreVertex(backup)) {
          return cedar::Status::ResourceExhausted(
              "TMV pool exhausted restoring replaced vertices");
        }
      }
      return cedar::Status::ResourceExhausted("TMV pool exhausted");
    }
    mutations.push_back(mutation);

    if (append.reverse && append.dir == Direction::kOut) {
      TMVEdge reverse_edge = append.edge;
      reverse_edge.target_id = append.entity_id;
      bool reverse_created = false;
      TMVVertexEntry* reverse_entry =
          FindOrCreateEntry(append.edge.target_id, &reverse_created);
      if (reverse_created) {
        created_entries.push_back(append.edge.target_id);
      }
      AppendMutation reverse_mutation;
      if (!AppendToEntry(reverse_entry, Direction::kIn, reverse_edge,
                         &reverse_mutation)) {
        rollback();
        for (const auto& backup : backups) {
          if (!RestoreVertex(backup)) {
            return cedar::Status::ResourceExhausted(
                "TMV pool exhausted restoring replaced vertices");
          }
        }
        return cedar::Status::ResourceExhausted("TMV pool exhausted");
      }
      mutations.push_back(reverse_mutation);
    }
  }

  return cedar::Status::OK();
}

cedar::Status TMVEngine::ReplacePartitionEdgesAtomic(
    uint32_t partition_id,
    const std::vector<EdgeAppend>& appends) {
  std::lock_guard<std::mutex> append_guard(append_mutex_);
  for (const auto& append : appends) {
    if (append.edge.reserved != partition_id) {
      return cedar::Status::InvalidArgument(
          "partition replacement append belongs to a different partition");
    }
  }

  std::vector<VertexBackup> backups;
  for (uint32_t s = 0; s < TMVIndex::kNumShards; ++s) {
    TMVIndex::Shard& shard = index_.shards_[s];
    std::lock_guard<std::mutex> holder{shard.lock};
    for (const auto& [entity_id, entry] : shard.entries) {
      VertexBackup backup;
      backup.entity_id = entity_id;
      bool has_partition_edge = false;
      std::lock_guard<std::mutex> entry_guard(entry.list_mutex);
      auto collect = [&](TMVChunk* chunk, std::vector<TMVEdge>* edges) {
        while (chunk) {
          uint32_t count = chunk->event_count.load(std::memory_order_acquire);
          for (uint32_t i = 0; i < count; ++i) {
            edges->push_back(chunk->edges[i]);
            if (chunk->edges[i].reserved == partition_id) {
              has_partition_edge = true;
            }
          }
          chunk = chunk->next.load(std::memory_order_acquire);
        }
      };
      collect(entry.out_chunk_head.load(std::memory_order_acquire),
              &backup.out_edges);
      collect(entry.in_chunk_head.load(std::memory_order_acquire),
              &backup.in_edges);
      if (has_partition_edge) {
        backups.push_back(std::move(backup));
      }
    }
  }

  auto restore_original_backups = [&]() -> cedar::Status {
    for (const auto& backup : backups) {
      InvalidateVertex(backup.entity_id);
    }
    for (const auto& backup : backups) {
      if (!RestoreVertex(backup)) {
        return cedar::Status::ResourceExhausted(
            "TMV pool exhausted restoring partition replacement");
      }
    }
    return cedar::Status::OK();
  };

  for (const auto& backup : backups) {
    InvalidateVertex(backup.entity_id);
  }
  for (const auto& backup : backups) {
    TMVVertexEntry* entry = nullptr;
    bool entry_created = false;
    auto restore_filtered = [&](Direction dir,
                                const std::vector<TMVEdge>& edges) -> bool {
      for (const auto& edge : edges) {
        if (edge.reserved == partition_id) {
          continue;
        }
        if (!entry) {
          entry = FindOrCreateEntry(backup.entity_id, &entry_created);
        }
        if (!AppendToEntry(entry, dir, edge)) {
          return false;
        }
      }
      return true;
    };
    if (!restore_filtered(Direction::kOut, backup.out_edges) ||
        !restore_filtered(Direction::kIn, backup.in_edges)) {
      CEDAR_RETURN_IF_ERROR(restore_original_backups());
      return cedar::Status::ResourceExhausted(
          "TMV pool exhausted filtering partition replacement");
    }
  }

  std::vector<AppendMutation> mutations;
  mutations.reserve(appends.size() * 2);
  std::vector<uint64_t> created_entries;
  created_entries.reserve(appends.size() * 2);
  auto rollback_appends = [&]() {
    for (auto it = mutations.rbegin(); it != mutations.rend(); ++it) {
      RollbackAppend(*it);
    }
    for (auto it = created_entries.rbegin(); it != created_entries.rend();
         ++it) {
      RemoveEntryIfEmpty(*it);
    }
  };

  for (const auto& append : appends) {
    bool created = false;
    TMVVertexEntry* entry = FindOrCreateEntry(append.entity_id, &created);
    if (created) {
      created_entries.push_back(append.entity_id);
    }
    AppendMutation mutation;
    if (!AppendToEntry(entry, append.dir, append.edge, &mutation)) {
      rollback_appends();
      CEDAR_RETURN_IF_ERROR(restore_original_backups());
      return cedar::Status::ResourceExhausted("TMV pool exhausted");
    }
    mutations.push_back(mutation);

    if (append.reverse && append.dir == Direction::kOut) {
      TMVEdge reverse_edge = append.edge;
      reverse_edge.target_id = append.entity_id;
      bool reverse_created = false;
      TMVVertexEntry* reverse_entry =
          FindOrCreateEntry(append.edge.target_id, &reverse_created);
      if (reverse_created) {
        created_entries.push_back(append.edge.target_id);
      }
      AppendMutation reverse_mutation;
      if (!AppendToEntry(reverse_entry, Direction::kIn, reverse_edge,
                         &reverse_mutation)) {
        rollback_appends();
        CEDAR_RETURN_IF_ERROR(restore_original_backups());
        return cedar::Status::ResourceExhausted("TMV pool exhausted");
      }
      mutations.push_back(reverse_mutation);
    }
  }

  return cedar::Status::OK();
}

std::vector<TMVEngine::EdgeAppend> TMVEngine::ExportPartitionEdges(
    uint32_t partition_id) const {
  std::lock_guard<std::mutex> append_guard(append_mutex_);
  std::vector<EdgeAppend> appends;
  for (uint32_t s = 0; s < TMVIndex::kNumShards; ++s) {
    const TMVIndex::Shard& shard = index_.shards_[s];
    std::lock_guard<std::mutex> holder{shard.lock};
    for (const auto& item : shard.entries) {
      const uint64_t entity_id = item.first;
      const TMVVertexEntry& entry = item.second;
      std::lock_guard<std::mutex> entry_guard(entry.list_mutex);
      auto collect = [&](Direction dir, TMVChunk* chunk) {
        while (chunk) {
          uint32_t count = chunk->event_count.load(std::memory_order_acquire);
          for (uint32_t i = 0; i < count; ++i) {
            const TMVEdge& edge = chunk->edges[i];
            if (edge.reserved == partition_id) {
              appends.push_back(EdgeAppend{entity_id, dir, edge, false});
            }
          }
          chunk = chunk->next.load(std::memory_order_acquire);
        }
      };
      collect(Direction::kOut,
              entry.out_chunk_head.load(std::memory_order_acquire));
      collect(Direction::kIn,
              entry.in_chunk_head.load(std::memory_order_acquire));
    }
  }
  return appends;
}

TMVEngine::VertexBackup TMVEngine::BackupVertex(uint64_t entity_id) const {
  VertexBackup backup;
  backup.entity_id = entity_id;
  uint32_t shard_idx = static_cast<uint32_t>(entity_id) &
                       (TMVIndex::kNumShards - 1);
  const TMVIndex::Shard& shard = index_.shards_[shard_idx];
  std::lock_guard<std::mutex> holder{shard.lock};
  auto it = shard.entries.find(entity_id);
  if (it == shard.entries.end()) {
    return backup;
  }
  const TMVVertexEntry& entry = it->second;
  std::lock_guard<std::mutex> entry_guard(entry.list_mutex);
  auto collect = [](TMVChunk* chunk, std::vector<TMVEdge>* edges) {
    while (chunk) {
      uint32_t count = chunk->event_count.load(std::memory_order_acquire);
      for (uint32_t i = 0; i < count; ++i) {
        edges->push_back(chunk->edges[i]);
      }
      chunk = chunk->next.load(std::memory_order_acquire);
    }
  };
  collect(entry.out_chunk_head.load(std::memory_order_acquire),
          &backup.out_edges);
  collect(entry.in_chunk_head.load(std::memory_order_acquire),
          &backup.in_edges);
  return backup;
}

bool TMVEngine::RestoreVertex(const VertexBackup& backup) {
  if (backup.out_edges.empty() && backup.in_edges.empty()) {
    return true;
  }
  TMVVertexEntry* entry = index_.FindOrCreate(backup.entity_id);
  for (const auto& edge : backup.out_edges) {
    if (!AppendToEntry(entry, Direction::kOut, edge)) {
      return false;
    }
  }
  for (const auto& edge : backup.in_edges) {
    if (!AppendToEntry(entry, Direction::kIn, edge)) {
      return false;
    }
  }
  return true;
}

bool TMVEngine::AppendToEntry(TMVVertexEntry* entry,
                              Direction dir,
                              const TMVEdge& edge,
                              AppendMutation* mutation) {
  std::lock_guard<std::mutex> guard(entry->list_mutex);
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
      if (mutation) {
        mutation->entry = entry;
        mutation->dir = dir;
        mutation->chunk = tail;
        mutation->appended_index = static_cast<uint32_t>(idx);
        mutation->new_chunk = nullptr;
        mutation->old_tail = nullptr;
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

  if (mutation) {
    mutation->entry = entry;
    mutation->dir = dir;
    mutation->chunk = new_chunk;
    mutation->appended_index = 0;
    mutation->new_chunk = new_chunk;
    mutation->old_tail = old_tail;
  }
  return true;
}

TMVVertexEntry* TMVEngine::FindOrCreateEntry(uint64_t entity_id,
                                             bool* created) {
  if (created) {
    *created = false;
  }
  uint32_t shard_idx = static_cast<uint32_t>(entity_id) &
                       (TMVIndex::kNumShards - 1);
  TMVIndex::Shard& shard = index_.shards_[shard_idx];
  std::lock_guard<std::mutex> holder{shard.lock};
  auto it = shard.entries.find(entity_id);
  if (it != shard.entries.end()) {
    return &it->second;
  }

  auto result = shard.entries.emplace(entity_id, TMVVertexEntry{});
  result.first->second.entity_id = entity_id;
  if (created) {
    *created = true;
  }
  return &result.first->second;
}

void TMVEngine::RemoveEntryIfEmpty(uint64_t entity_id) {
  uint32_t shard_idx = static_cast<uint32_t>(entity_id) &
                       (TMVIndex::kNumShards - 1);
  TMVIndex::Shard& shard = index_.shards_[shard_idx];
  std::lock_guard<std::mutex> holder{shard.lock};
  auto it = shard.entries.find(entity_id);
  if (it == shard.entries.end()) {
    return;
  }

  TMVVertexEntry& entry = it->second;
  std::lock_guard<std::mutex> entry_guard(entry.list_mutex);
  if (entry.out_chunk_head.load(std::memory_order_acquire) == nullptr &&
      entry.in_chunk_head.load(std::memory_order_acquire) == nullptr &&
      entry.out_edge_count.load(std::memory_order_relaxed) == 0 &&
      entry.in_edge_count.load(std::memory_order_relaxed) == 0) {
    shard.entries.erase(it);
  }
}

void TMVEngine::RollbackAppend(const AppendMutation& mutation) {
  if (!mutation.entry || !mutation.chunk) {
    return;
  }

  std::lock_guard<std::mutex> guard(mutation.entry->list_mutex);
  std::atomic<TMVChunk*>* head_ptr =
      (mutation.dir == Direction::kOut) ? &mutation.entry->out_chunk_head
                                        : &mutation.entry->in_chunk_head;
  std::atomic<TMVChunk*>* tail_ptr =
      (mutation.dir == Direction::kOut) ? &mutation.entry->out_chunk_tail
                                        : &mutation.entry->in_chunk_tail;
  std::atomic<uint64_t>* count_ptr =
      (mutation.dir == Direction::kOut) ? &mutation.entry->out_edge_count
                                        : &mutation.entry->in_edge_count;

  if (mutation.new_chunk) {
    if (mutation.old_tail) {
      tail_ptr->store(mutation.old_tail, std::memory_order_relaxed);
      mutation.old_tail->next.store(nullptr, std::memory_order_release);
    } else {
      head_ptr->store(nullptr, std::memory_order_relaxed);
      tail_ptr->store(nullptr, std::memory_order_relaxed);
    }
    count_ptr->fetch_sub(1, std::memory_order_relaxed);
    pool_.Free(mutation.new_chunk);
  } else {
    mutation.chunk->event_count.store(mutation.appended_index,
                                      std::memory_order_release);
    RecomputeChunkMetadata(mutation.chunk, mutation.appended_index);
    count_ptr->fetch_sub(1, std::memory_order_relaxed);
  }

  RecomputeEarliestTimestamp(mutation.entry);
}

void TMVEngine::RecomputeChunkMetadata(TMVChunk* chunk, uint32_t count) {
  uint32_t min_from = std::numeric_limits<uint32_t>::max();
  uint32_t max_to = 0;
  for (uint32_t i = 0; i < count; ++i) {
    min_from = std::min(min_from, chunk->edges[i].valid_from);
    max_to = std::max(max_to, chunk->edges[i].valid_to);
  }
  chunk->min_valid_from.store(min_from, std::memory_order_relaxed);
  chunk->max_valid_to.store(max_to, std::memory_order_relaxed);
}

void TMVEngine::RecomputeEarliestTimestamp(TMVVertexEntry* entry) {
  uint32_t earliest = std::numeric_limits<uint32_t>::max();
  auto scan = [&earliest](TMVChunk* chunk) {
    while (chunk) {
      earliest = std::min(
          earliest, chunk->min_valid_from.load(std::memory_order_relaxed));
      chunk = chunk->next.load(std::memory_order_acquire);
    }
  };
  scan(entry->out_chunk_head.load(std::memory_order_acquire));
  scan(entry->in_chunk_head.load(std::memory_order_acquire));
  entry->earliest_chunk_timestamp.store(earliest, std::memory_order_relaxed);
}

std::vector<TMVEdge> TMVEngine::ScanAtTime(uint64_t entity_id,
                                           Direction dir,
                                           uint64_t query_time) {
  std::lock_guard<std::mutex> append_guard(append_mutex_);
  std::vector<TMVEdge> candidates;

  uint32_t shard_idx = static_cast<uint32_t>(entity_id) & (TMVIndex::kNumShards - 1);
  TMVIndex::Shard& shard = index_.shards_[shard_idx];
  std::lock_guard<std::mutex> holder{shard.lock};

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
    // Discard if deleted (valid_to != MAX and valid_to <= effective_query_time)
    if (best.valid_to == std::numeric_limits<uint32_t>::max() ||
        best.valid_to > effective_query_time) {
      result.push_back(best);
    }

    i = j;
  }

  return result;
}

size_t TMVEngine::DropChunksBelowWatermark(std::atomic<TMVChunk*>* head_ptr,
                                           std::atomic<TMVChunk*>* tail_ptr,
                                           uint64_t watermark) {
  TMVChunk* head = head_ptr->load(std::memory_order_acquire);
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
    std::lock_guard<std::mutex> holder{shard.lock};

    for (auto& [id, entry] : shard.entries) {
      std::lock_guard<std::mutex> entry_guard(entry.list_mutex);
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
  std::lock_guard<std::mutex> holder{shard.lock};

  auto it = shard.entries.find(entity_id);
  if (it == shard.entries.end()) {
    return 0;
  }

  size_t freed = 0;
  auto& entry = it->second;
  std::lock_guard<std::mutex> entry_guard(entry.list_mutex);
  // Free outgoing edge chunks
  TMVChunk* chunk = entry.out_chunk_head.load(std::memory_order_acquire);
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
  std::lock_guard<std::mutex> append_guard(append_mutex_);
  size_t count = 0;
  for (uint32_t s = 0; s < TMVIndex::kNumShards; ++s) {
    const TMVIndex::Shard& shard = index_.shards_[s];
    std::lock_guard<std::mutex> holder{shard.lock};
    count += shard.entries.size();
  }
  return count;
}

size_t TMVEngine::ChunkCount() const {
  std::lock_guard<std::mutex> append_guard(append_mutex_);
  return pool_.TotalCount() - pool_.FreeCount();
}

}  // namespace gcn
}  // namespace cedar
