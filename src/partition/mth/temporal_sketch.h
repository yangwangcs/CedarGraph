// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#ifndef CEDAR_PARTITION_MTH_TEMPORAL_SKETCH_H_
#define CEDAR_PARTITION_MTH_TEMPORAL_SKETCH_H_

#include <cstdint>
#include <vector>
#include <utility>

namespace cedar {
namespace partition {

struct SketchCell {
  uint32_t count = 0;
  uint64_t last_ts = 0;
};

class TemporalSketch {
 public:
  TemporalSketch(uint16_t num_partitions,
                 int depth = 3, int width = 64,
                 double temporal_bonus_weight = 1.0,
                 double temporal_alpha = 0.01,
                 uint64_t seed = 42);

  void Observe(uint64_t vertex, uint16_t partition_id, uint64_t ts_micros);
  uint32_t Estimate(uint64_t vertex, uint16_t partition_id) const;
  std::pair<uint16_t, double> SuggestPartition(uint64_t vertex, uint64_t ts_micros) const;

  void ApplyDecay(double decay_factor);

  int depth() const { return depth_; }
  int width() const { return width_; }
  uint16_t num_partitions() const { return num_partitions_; }

  const std::vector<std::vector<std::vector<SketchCell>>>& tables() const {
    return tables_;
  }

  void CopyFrom(const TemporalSketch& other);

 private:
  int Hash(uint64_t vertex, int seed_index) const;

  uint16_t num_partitions_;
  int depth_;
  int width_;
  double temporal_bonus_weight_;
  double temporal_alpha_;
  std::vector<int> seeds_;
  std::vector<std::vector<std::vector<SketchCell>>> tables_;
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_MTH_TEMPORAL_SKETCH_H_
