// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#ifndef CEDAR_PARTITION_MTH_PARTITION_STATE_H_
#define CEDAR_PARTITION_MTH_PARTITION_STATE_H_

#include <cstdint>
#include <vector>
#include <unordered_set>
#include "partition/mth/cedar_key.h"

namespace cedar {
namespace partition {

struct PartitionState {
  uint16_t partition_id;
  size_t capacity;

  std::vector<CedarKey> events;
  std::unordered_set<uint64_t> S;

  uint64_t timestamp_sum = 0;

  explicit PartitionState(uint16_t pid, size_t cap = 0)
      : partition_id(pid), capacity(cap) {}

  void AddEvent(const CedarKey& key);
  bool IsFull() const;
  size_t EventCount() const { return events.size(); }
  double AvgTimestamp() const;
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_MTH_PARTITION_STATE_H_
