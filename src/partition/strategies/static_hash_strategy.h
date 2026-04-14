// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_PARTITION_STRATEGIES_STATIC_HASH_STRATEGY_H_
#define CEDAR_PARTITION_STRATEGIES_STATIC_HASH_STRATEGY_H_

#include <atomic>
#include <functional>
#include "partition/partition_strategy.h"

namespace cedar {
namespace partition {

// NebulaGraph 风格的静态哈希分区
class StaticHashStrategy : public IPartitionStrategy {
 public:
  explicit StaticHashStrategy(uint32_t num_partitions = 65536);
  
  const char* Name() const override { return "StaticHash"; }
  
  // 核心路由逻辑: hash(vid) % num_partitions
  PartitionAssignment RouteVertex(uint64_t vertex_id) override;
  
  // 边路由: 分别计算 src 和 dst 的分区
  std::pair<PartitionAssignment, PartitionAssignment> 
      RouteEdge(uint64_t src_id, uint64_t dst_id) override;
  
  // 配置
  Status Configure(const std::string& key, const std::string& value) override;
  
  // 统计信息
  StatusOr<std::string> GetStats() const override;

 private:
  uint32_t num_partitions_;
  mutable std::atomic<uint64_t> route_count_{0};
  
  // 哈希函数（可配置）
  uint64_t HashVertexId(uint64_t vertex_id) const;
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_STRATEGIES_STATIC_HASH_STRATEGY_H_
