// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#include "cedar/partition/mth/streaming_partitioner.h"
#include "cedar/partition/mth/op_type.h"
#include <cmath>
#include <limits>

namespace cedar {
namespace partition {

StreamingPartitioner::StreamingPartitioner(
    uint16_t num_partitions, size_t capacity,
    double alpha, double beta, double gamma,
    double eta, double temporal_alpha)
    : num_partitions_(num_partitions),
      capacity_(capacity),
      alpha_(alpha), beta_(beta), gamma_(gamma),
      eta_(eta), temporal_alpha_(temporal_alpha) {
  for (uint16_t i = 0; i < num_partitions; ++i) {
    states_.emplace_back(i, capacity);
  }
}

void StreamingPartitioner::SetMigrationAffinity(
    const std::unordered_map<uint64_t, uint16_t>& affinity) {
  std::lock_guard<std::mutex> lock(mutex_);
  migration_affinity_ = affinity;
}

void StreamingPartitioner::UpdateTemporalAffinity(uint64_t entity_id, uint64_t ts_micros) {
  auto it = temporal_affinity_.find(entity_id);
  if (it == temporal_affinity_.end()) return;
  auto& affs = it->second;
  auto& lasts = temporal_last_update_[entity_id];
  for (uint16_t i = 0; i < num_partitions_; ++i) {
    if (lasts[i] > 0) {
      double dt = static_cast<double>(ts_micros > lasts[i] ? ts_micros - lasts[i] : 0);
      affs[i] *= std::exp(-temporal_alpha_ * dt);
    }
    lasts[i] = ts_micros;
  }
}

void StreamingPartitioner::BumpTemporalAffinity(uint64_t entity_id, uint16_t pid, uint64_t ts_micros) {
  auto it = temporal_affinity_.find(entity_id);
  if (it == temporal_affinity_.end()) {
    temporal_affinity_[entity_id] = std::vector<double>(num_partitions_, 0.0);
    temporal_last_update_[entity_id] = std::vector<uint64_t>(num_partitions_, 0);
    it = temporal_affinity_.find(entity_id);
  }
  it->second[pid] += 1.0;
  temporal_last_update_[entity_id][pid] = ts_micros;
}

uint16_t StreamingPartitioner::ScoreAndPick(uint64_t entity_id, uint64_t ts_micros) {
  UpdateTemporalAffinity(entity_id, ts_micros);

  uint16_t best_pid = 0;
  double best_score = std::numeric_limits<double>::max();
  bool first = true;

  for (uint16_t i = 0; i < num_partitions_; ++i) {
    if (states_[i].IsFull()) continue;
    double topo_cost = (states_[i].S.find(entity_id) != states_[i].S.end()) ? 0.0 : 1.0;
    double temp_cost = (states_[i].EventCount() == 0)
        ? 0.0
        : std::abs(static_cast<double>(ts_micros) - states_[i].AvgTimestamp());
    double mig_cost = 0.0;
    auto mit = migration_affinity_.find(entity_id);
    if (mit != migration_affinity_.end() && mit->second != i) {
      mig_cost = 1.0;
    }
    double affinity = 0.0;
    auto ait = temporal_affinity_.find(entity_id);
    if (ait != temporal_affinity_.end()) {
      affinity = ait->second[i];
    }

    double score = alpha_ * topo_cost + beta_ * temp_cost + gamma_ * mig_cost - eta_ * affinity;
    if (first || score < best_score) {
      best_score = score;
      best_pid = i;
      first = false;
    }
  }

  if (first) {
    size_t min_load = states_[0].EventCount();
    best_pid = 0;
    for (uint16_t i = 1; i < num_partitions_; ++i) {
      if (states_[i].EventCount() < min_load) {
        min_load = states_[i].EventCount();
        best_pid = i;
      }
    }
  }

  BumpTemporalAffinity(entity_id, best_pid, ts_micros);
  return best_pid;
}

uint16_t StreamingPartitioner::AssignEvent(const CedarKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  uint8_t op = key.flags & KeyFlags::kOpTypeMask;
  uint64_t entity_id = key.entity_id;
  uint64_t ts = CedarKey::DecodeTimestamp(key.timestamp_be);

  if (op == OpType::kUpdate || op == OpType::kDelete) {
    auto it = entity_home_.find(entity_id);
    if (it != entity_home_.end()) {
      uint16_t pid = it->second;
      states_[pid].AddEvent(key);
      return pid;
    }
  }

  uint16_t pid = ScoreAndPick(entity_id, ts);
  entity_home_[entity_id] = pid;
  states_[pid].AddEvent(key);
  return pid;
}

void StreamingPartitioner::PartitionStream(const std::vector<CedarKey>& events) {
  for (const auto& key : events) {
    AssignEvent(key);
  }
}

} // namespace partition
} // namespace cedar
