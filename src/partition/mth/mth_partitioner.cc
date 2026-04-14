// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#include "partition/mth/mth_partitioner.h"
#include "partition/mth/op_type.h"
#include <cmath>

namespace cedar {
namespace partition {

MTHPartitioner::MTHPartitioner(
    uint16_t num_partitions, size_t capacity,
    double alpha, double beta, double gamma,
    double eta, double temporal_alpha,
    int sketch_depth, int sketch_width,
    double fast_path_threshold,
    double load_relaxation,
    int decay_interval,
    double decay_factor)
    : StreamingPartitioner(num_partitions, capacity, alpha, beta, gamma, eta, temporal_alpha),
      sketch_(num_partitions, sketch_depth, sketch_width, 1.0, temporal_alpha),
      fast_path_threshold_(fast_path_threshold),
      load_relaxation_(load_relaxation),
      decay_interval_(decay_interval),
      decay_factor_(decay_factor) {}

uint16_t MTHPartitioner::AssignEvent(const CedarKey& key) {
  call_count_.fetch_add(1, std::memory_order_relaxed);

  if (decay_interval_ > 0) {
    if (edge_counter_.fetch_add(1, std::memory_order_relaxed) % decay_interval_ == 0) {
      sketch_.ApplyDecay(decay_factor_);
    }
  }

  uint64_t entity_id = key.entity_id;
  uint64_t ts = key.timestamp_be;

  auto [pid, score] = sketch_.SuggestPartition(entity_id, ts);

  bool can_use_fast = false;
  if (score >= fast_path_threshold_) {
    // When capacity is unconstrained and load_relaxation is disabled,
    // we can decide fast-path outside the lock since sketch is lock-free.
    if (capacity_ == 0 && load_relaxation_ <= 0.0) {
      can_use_fast = true;
    } else {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!states_[pid].IsFull()) {
        double avg_size = 0.0;
        if (load_relaxation_ > 0.0) {
          size_t total = 0;
          for (const auto& s : states_) total += s.EventCount();
          avg_size = static_cast<double>(total) / static_cast<double>(num_partitions_);
        }
        double max_allowed = avg_size * (1.0 + load_relaxation_);
        if (load_relaxation_ <= 0.0 || static_cast<double>(states_[pid].EventCount()) < max_allowed) {
          can_use_fast = true;
        }
      }

      if (can_use_fast) {
        fast_path_count_.fetch_add(1, std::memory_order_relaxed);
        sketch_.Observe(entity_id, pid, ts);
        uint8_t op = key.flags & KeyFlags::kOpTypeMask;
        if (op == OpType::kCreate) {
          entity_home_[entity_id] = pid;
        }
        states_[pid].AddEvent(key);
        return pid;
      }

      pid = StreamingPartitioner::AssignEvent(key);
      sketch_.Observe(entity_id, pid, ts);
      return pid;
    }
  }

  if (can_use_fast) {
    fast_path_count_.fetch_add(1, std::memory_order_relaxed);
    sketch_.Observe(entity_id, pid, ts);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      uint8_t op = key.flags & KeyFlags::kOpTypeMask;
      if (op == OpType::kCreate) {
        entity_home_[entity_id] = pid;
      }
      states_[pid].AddEvent(key);
    }
    return pid;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  pid = StreamingPartitioner::AssignEvent(key);
  sketch_.Observe(entity_id, pid, ts);
  return pid;
}

double MTHPartitioner::FastPathRatio() const {
  return static_cast<double>(fast_path_count_.load(std::memory_order_relaxed))
         / static_cast<double>(std::max(call_count_.load(std::memory_order_relaxed), 1));
}

void MTHPartitioner::WarmStart(const TemporalSketch& other) {
  sketch_.CopyFrom(other);
}

} // namespace partition
} // namespace cedar
