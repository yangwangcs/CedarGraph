// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#include "cedar/partition/mth/partition_state.h"

namespace cedar {
namespace partition {

void PartitionState::AddEvent(const CedarKey& key) {
  events.push_back(key);
  S.insert(key.entity_id);
  timestamp_sum += CedarKey::DecodeTimestamp(key.timestamp_be);
}

bool PartitionState::IsFull() const {
  if (capacity == 0) return false;
  return events.size() >= capacity;
}

double PartitionState::AvgTimestamp() const {
  if (events.empty()) return 0.0;
  return static_cast<double>(timestamp_sum) / static_cast<double>(events.size());
}

} // namespace partition
} // namespace cedar
