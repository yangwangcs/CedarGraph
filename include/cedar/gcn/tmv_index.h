#pragma once

#include <cstdint>

#include "absl/base/internal/spinlock.h"
#include "absl/container/flat_hash_map.h"

#include "cedar/gcn/tmv_vertex_entry.h"

namespace cedar {
namespace gcn {

class TMVIndex {
 public:
  static constexpr uint32_t kShardBits = 8;
  static constexpr uint32_t kNumShards = 1 << kShardBits;

  struct Shard {
    mutable absl::base_internal::SpinLock lock;
    absl::flat_hash_map<uint64_t, TMVVertexEntry> entries;
  };

  friend class TMVEngine;

  TMVIndex() = default;

  TMVVertexEntry* FindOrCreate(uint64_t entity_id) {
    uint32_t shard_idx = ShardIndex(entity_id);
    Shard& shard = shards_[shard_idx];

    absl::base_internal::SpinLockHolder holder{shard.lock};
    auto it = shard.entries.find(entity_id);
    if (it != shard.entries.end()) {
      return &it->second;
    }

    auto result = shard.entries.emplace(entity_id, TMVVertexEntry{});
    result.first->second.entity_id = entity_id;
    return &result.first->second;
  }

  TMVVertexEntry* Find(uint64_t entity_id) const {
    uint32_t shard_idx = ShardIndex(entity_id);
    const Shard& shard = shards_[shard_idx];

    absl::base_internal::SpinLockHolder holder{shard.lock};
    auto it = shard.entries.find(entity_id);
    if (it != shard.entries.end()) {
      return const_cast<TMVVertexEntry*>(&it->second);
    }
    return nullptr;
  }

  void Reserve(uint64_t total_entries) {
    uint64_t per_shard = total_entries / kNumShards;
    if (per_shard == 0) {
      per_shard = 4;
    }
    for (uint32_t i = 0; i < kNumShards; ++i) {
      absl::base_internal::SpinLockHolder holder{shards_[i].lock};
      shards_[i].entries.reserve(per_shard);
    }
  }

 private:
  static uint32_t ShardIndex(uint64_t entity_id) {
    return static_cast<uint32_t>(entity_id) & (kNumShards - 1);
  }

  mutable Shard shards_[kNumShards];
};

}  // namespace gcn
}  // namespace cedar
