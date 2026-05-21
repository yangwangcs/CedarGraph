// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#ifndef CEDAR_PARTITION_MTH_STREAMING_PARTITIONER_H_
#define CEDAR_PARTITION_MTH_STREAMING_PARTITIONER_H_

#include <cstdint>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include "cedar/partition/mth/cedar_key.h"
#include "cedar/partition/mth/partition_state.h"

namespace cedar {
namespace partition {

// 基类分区器接口
class IEventPartitioner {
 public:
  virtual ~IEventPartitioner() = default;
  virtual uint16_t AssignEvent(const CedarKey& key) = 0;
  virtual void PartitionStream(const std::vector<CedarKey>& events) = 0;
  virtual void SetMigrationAffinity(
      const std::unordered_map<uint64_t, uint16_t>& affinity) = 0;
};

class StreamingPartitioner : public IEventPartitioner {
 public:
  StreamingPartitioner(uint16_t num_partitions, size_t capacity,
                       double alpha = 1.0, double beta = 1.0,
                       double gamma = 1.0, double eta = 0.0,
                       double temporal_alpha = 0.01);

  uint16_t AssignEvent(const CedarKey& key) override;
  void PartitionStream(const std::vector<CedarKey>& events) override;
  void SetMigrationAffinity(
      const std::unordered_map<uint64_t, uint16_t>& affinity) override;

  const std::vector<PartitionState>& states() const { return states_; }

 protected:
  virtual uint16_t ScoreAndPick(uint64_t entity_id, uint64_t ts_micros);
  void UpdateTemporalAffinity(uint64_t entity_id, uint64_t ts_micros);
  void BumpTemporalAffinity(uint64_t entity_id, uint16_t pid, uint64_t ts_micros);

  uint16_t num_partitions_;
  size_t capacity_;
  double alpha_, beta_, gamma_, eta_;
  double temporal_alpha_;

  mutable std::mutex mutex_;

  std::vector<PartitionState> states_;
  std::unordered_map<uint64_t, uint16_t> entity_home_;
  std::unordered_map<uint64_t, uint16_t> migration_affinity_;
  std::unordered_map<uint64_t, std::vector<double>> temporal_affinity_;
  std::unordered_map<uint64_t, std::vector<uint64_t>> temporal_last_update_;
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_MTH_STREAMING_PARTITIONER_H_
