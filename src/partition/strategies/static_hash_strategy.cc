// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/partition/strategies/static_hash_strategy.h"

#include <iostream>
#include <sstream>

namespace cedar {
namespace partition {

StaticHashStrategy::StaticHashStrategy(uint32_t num_partitions)
    : num_partitions_(num_partitions) {}

PartitionAssignment StaticHashStrategy::RouteVertex(uint64_t vertex_id) {
  route_count_.fetch_add(1, std::memory_order_relaxed);
  
  if (num_partitions_ == 0) {
    return PartitionAssignment(0, 0.0, "StaticHash");
  }
  uint32_t partition_id = static_cast<uint32_t>(vertex_id % num_partitions_);
  return PartitionAssignment(partition_id, 1.0, "StaticHash");
}

std::pair<PartitionAssignment, PartitionAssignment> 
StaticHashStrategy::RouteEdge(uint64_t src_id, uint64_t dst_id) {
  return {
    RouteVertex(src_id),
    RouteVertex(dst_id)
  };
}

Status StaticHashStrategy::Configure(const std::string& key, 
                                      const std::string& value) {
  if (key == "num_partitions") {
    try {
      num_partitions_ = static_cast<uint32_t>(std::stoul(value));
      return Status::OK();
    } catch (...) {
      std::cerr << "[StaticHashStrategy] Invalid num_partitions value: " << value << std::endl;
      return Status::InvalidArgument("Invalid num_partitions value");
    }
  }
  return Status::InvalidArgument("Unknown configuration key: " + key);
}

StatusOr<std::string> StaticHashStrategy::GetStats() const {
  std::ostringstream oss;
  oss << "StaticHashStrategy Stats:\n"
      << "  Total Routes: " << route_count_.load() << "\n"
      << "  Num Partitions: " << num_partitions_ << "\n"
      << "  Strategy: hash(vid) % " << num_partitions_;
  return oss.str();
}

uint64_t StaticHashStrategy::HashVertexId(uint64_t vertex_id) const {
  // 简单的取模哈希，可扩展为其他哈希函数
  return vertex_id;
}

} // namespace partition
} // namespace cedar
