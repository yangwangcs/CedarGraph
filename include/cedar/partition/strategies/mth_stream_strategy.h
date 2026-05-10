// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_PARTITION_STRATEGIES_MTH_STREAM_STRATEGY_H_
#define CEDAR_PARTITION_STRATEGIES_MTH_STREAM_STRATEGY_H_

#include <memory>
#include "cedar/partition/partition_strategy.h"
#include "cedar/partition/mth/mth_partitioner.h"

namespace cedar {
namespace partition {

// 基于 subgraph2 MTHPartitioner 的流式分区策略
class MTHStreamStrategy : public IPartitionStrategy {
 public:
  struct Config {
    uint32_t num_partitions = 65536;
    size_t capacity = 1000000;            // 每分区容量
    double alpha = 1.0;                   // 边界顶点权重
    double beta = 1.0;                    // 复制因子权重
    double gamma = 0.0;                   // 负载均衡权重
    double eta = 0.0;                     // 迁移成本权重
    double temporal_alpha = 0.01;         // 时态衰减系数
    int sketch_depth = 3;                 // Count-Min Sketch 深度
    int sketch_width = 64;                // Count-Min Sketch 宽度
    double fast_path_threshold = 0.6;     // 快速路径阈值
    double load_relaxation = 0.0;         // 负载松弛度
    int decay_interval = 0;               // 衰减间隔（0=禁用）
    double decay_factor = 0.95;           // 衰减因子
  };
  
  explicit MTHStreamStrategy(const Config& config);
  
  const char* Name() const override { return "MTHStream"; }
  
  // 路由接口实现
  PartitionAssignment RouteVertex(uint64_t vertex_id) override;
  PartitionAssignment RouteVertexTemporal(
      uint64_t vertex_id, uint64_t timestamp) override;
  std::pair<PartitionAssignment, PartitionAssignment> 
      RouteEdge(uint64_t src_id, uint64_t dst_id) override;
  
  // 流式事件处理
  Status ProcessEventStream(const std::vector<GraphEvent>& events) override;
  
  // 支持时态路由
  bool SupportsTemporalRouting() const override { return true; }
  
  // 配置
  Status Configure(const std::string& key, const std::string& value) override;
  
  // 统计信息
  StatusOr<std::string> GetStats() const override;
  
  // MTH 特有接口
  double GetFastPathRatio() const;
  void WarmStartFrom(const MTHStreamStrategy& other);

 private:
  Config config_;
  std::unique_ptr<MTHPartitioner> partitioner_;
  
  // 将 GraphEvent 转换为 CedarKey
  CedarKey ConvertToCedarKey(const GraphEvent& event);
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_STRATEGIES_MTH_STREAM_STRATEGY_H_
