// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#ifndef CEDAR_PARTITION_MTH_MTH_PARTITIONER_H_
#define CEDAR_PARTITION_MTH_MTH_PARTITIONER_H_

#include <cstdint>
#include <atomic>
#include <mutex>
#include "cedar/partition/mth/streaming_partitioner.h"
#include "cedar/partition/mth/temporal_sketch.h"

namespace cedar {
namespace partition {

class MTHPartitioner : public StreamingPartitioner {
 public:
  MTHPartitioner(uint16_t num_partitions, size_t capacity,
                 double alpha = 1.0, double beta = 1.0, double gamma = 0.0,
                 double eta = 0.0, double temporal_alpha = 0.01,
                 int sketch_depth = 3, int sketch_width = 64,
                 double fast_path_threshold = 0.6,
                 double load_relaxation = 0.0,
                 int decay_interval = 0,
                 double decay_factor = 0.95);

  uint16_t AssignEvent(const CedarKey& key) override;
  double FastPathRatio() const;
  void WarmStart(const TemporalSketch& other);

  TemporalSketch& sketch() { return sketch_; }
  const TemporalSketch& sketch() const { return sketch_; }

 private:
  TemporalSketch sketch_;
  double fast_path_threshold_;
  double load_relaxation_;
  int decay_interval_;
  double decay_factor_;
  std::atomic<int> call_count_{0};
  std::atomic<int> fast_path_count_{0};
  std::atomic<int> edge_counter_{0};
  mutable std::mutex mutex_;
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_MTH_MTH_PARTITIONER_H_
