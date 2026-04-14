// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#include "partition/mth/temporal_sketch.h"
#include <cmath>
#include <random>
#include <algorithm>
#include <limits>

namespace cedar {
namespace partition {

TemporalSketch::TemporalSketch(uint16_t num_partitions, int depth, int width,
                               double temporal_bonus_weight,
                               double temporal_alpha,
                               uint64_t seed)
    : num_partitions_(num_partitions),
      depth_(depth),
      width_(width),
      temporal_bonus_weight_(temporal_bonus_weight),
      temporal_alpha_(temporal_alpha),
      tables_(depth, std::vector<std::vector<SketchCell>>(
                         num_partitions, std::vector<SketchCell>(width))) {
  std::mt19937_64 rng(seed);
  for (int i = 0; i < depth; ++i) {
    seeds_.push_back(static_cast<int>(rng() & 0x7FFFFFFF));
  }
}

int TemporalSketch::Hash(uint64_t vertex, int seed_index) const {
  return static_cast<int>(std::hash<uint64_t>{}(vertex ^ static_cast<uint64_t>(seeds_[seed_index]))) % width_;
}

void TemporalSketch::Observe(uint64_t vertex, uint16_t partition_id, uint64_t ts_micros) {
  for (int d = 0; d < depth_; ++d) {
    int idx = Hash(vertex, d);
    auto& cell = tables_[d][partition_id][idx];
    __atomic_fetch_add(&cell.count, 1, __ATOMIC_RELAXED);
    __atomic_store_n(&cell.last_ts, ts_micros, __ATOMIC_RELAXED);
  }
}

uint32_t TemporalSketch::Estimate(uint64_t vertex, uint16_t partition_id) const {
  uint32_t min_val = std::numeric_limits<uint32_t>::max();
  for (int d = 0; d < depth_; ++d) {
    int idx = Hash(vertex, d);
    uint32_t val = __atomic_load_n(&tables_[d][partition_id][idx].count, __ATOMIC_RELAXED);
    min_val = std::min(min_val, val);
  }
  return min_val;
}

std::pair<uint16_t, double> TemporalSketch::SuggestPartition(uint64_t vertex, uint64_t ts_micros) const {
  std::vector<double> scores(num_partitions_, 0.0);
  for (uint16_t pid = 0; pid < num_partitions_; ++pid) {
    uint32_t count = std::numeric_limits<uint32_t>::max();
    for (int d = 0; d < depth_; ++d) {
      int idx = Hash(vertex, d);
      uint32_t val = __atomic_load_n(&tables_[d][pid][idx].count, __ATOMIC_RELAXED);
      count = std::min(count, val);
    }
    double time_bonus = 0.0;
    for (int d = 0; d < depth_; ++d) {
      int idx = Hash(vertex, d);
      const auto& cell = tables_[d][pid][idx];
      uint32_t cell_count = __atomic_load_n(&cell.count, __ATOMIC_RELAXED);
      if (cell_count > 0) {
        uint64_t last_ts = __atomic_load_n(&cell.last_ts, __ATOMIC_RELAXED);
        double dt = static_cast<double>(ts_micros > last_ts ? ts_micros - last_ts : 0);
        double bonus = std::exp(-temporal_alpha_ * dt);
        time_bonus = std::max(time_bonus, bonus);
      }
    }
    scores[pid] = static_cast<double>(count) + temporal_bonus_weight_ * time_bonus;
  }

  uint16_t best_pid = 0;
  double best_score = scores[0];
  for (uint16_t pid = 1; pid < num_partitions_; ++pid) {
    if (scores[pid] > best_score) {
      best_score = scores[pid];
      best_pid = pid;
    }
  }
  return {best_pid, best_score};
}

void TemporalSketch::ApplyDecay(double decay_factor) {
  for (int d = 0; d < depth_; ++d) {
    for (uint16_t pid = 0; pid < num_partitions_; ++pid) {
      for (int i = 0; i < width_; ++i) {
        auto& cell = tables_[d][pid][i];
        uint32_t old_val = __atomic_load_n(&cell.count, __ATOMIC_RELAXED);
        __atomic_store_n(&cell.count, static_cast<uint32_t>(old_val * decay_factor), __ATOMIC_RELAXED);
      }
    }
  }
}

void TemporalSketch::CopyFrom(const TemporalSketch& other) {
  if (other.depth_ != depth_ || other.width_ != width_ ||
      other.num_partitions_ != num_partitions_) {
    return;
  }
  seeds_ = other.seeds_;
  for (int d = 0; d < depth_; ++d) {
    for (uint16_t pid = 0; pid < num_partitions_; ++pid) {
      for (int i = 0; i < width_; ++i) {
        uint32_t c = __atomic_load_n(&other.tables_[d][pid][i].count, __ATOMIC_RELAXED);
        uint64_t t = __atomic_load_n(&other.tables_[d][pid][i].last_ts, __ATOMIC_RELAXED);
        __atomic_store_n(&tables_[d][pid][i].count, c, __ATOMIC_RELAXED);
        __atomic_store_n(&tables_[d][pid][i].last_ts, t, __ATOMIC_RELAXED);
      }
    }
  }
}

} // namespace partition
} // namespace cedar
