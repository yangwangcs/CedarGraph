// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_PARTITION_PARTITION_STRATEGY_MANAGER_H_
#define CEDAR_PARTITION_PARTITION_STRATEGY_MANAGER_H_

#include <memory>
#include <unordered_map>
#include <mutex>
#include "partition/partition_strategy.h"

namespace cedar {
namespace partition {

enum class StrategyType {
  STATIC_HASH,    // 静态哈希
  MTH_STREAM,     // MTH 流式
  AUTO            // 自动选择（根据查询特征）
};

// 策略选择配置
struct StrategySelectionConfig {
  StrategyType default_strategy = StrategyType::STATIC_HASH;
  
  // 自动选择阈值
  bool enable_auto_selection = false;
  uint64_t temporal_query_threshold = 100;  // 时态查询次数阈值
  double locality_ratio_threshold = 0.7;    // 局部性比例阈值
};

class PartitionStrategyManager {
 public:
  PartitionStrategyManager();
  ~PartitionStrategyManager();
  
  // 禁止拷贝
  PartitionStrategyManager(const PartitionStrategyManager&) = delete;
  PartitionStrategyManager& operator=(const PartitionStrategyManager&) = delete;
  
  // 初始化
  Status Initialize(const StrategySelectionConfig& config);
  
  // 注册策略
  Status RegisterStrategy(std::unique_ptr<IPartitionStrategy> strategy);
  
  // 选择当前策略
  Status SetActiveStrategy(StrategyType type);
  Status SetActiveStrategy(const std::string& strategy_name);
  
  // 获取当前策略
  IPartitionStrategy* GetActiveStrategy() const;
  
  // 路由接口（代理到当前策略）
  PartitionAssignment RouteVertex(uint64_t vertex_id);
  PartitionAssignment RouteVertexTemporal(uint64_t vertex_id, uint64_t timestamp);
  std::pair<PartitionAssignment, PartitionAssignment> 
      RouteEdge(uint64_t src_id, uint64_t dst_id);
  
  // 流式事件处理（仅 MTH 有效）
  Status ProcessEventStream(const std::vector<GraphEvent>& events);
  
  // 自动选择策略
  void UpdateQueryStats(bool is_temporal_query, bool has_locality);
  void MaybeAutoSwitchStrategy();
  
  // 获取所有策略的统计信息
  std::string GetAllStats() const;

 private:
  mutable std::mutex mutex_;
  StrategySelectionConfig config_;
  std::unordered_map<std::string, std::unique_ptr<IPartitionStrategy>> strategies_;
  IPartitionStrategy* active_strategy_ = nullptr;
  
  // 自动选择统计
  struct QueryStats {
    uint64_t total_queries = 0;
    uint64_t temporal_queries = 0;
    uint64_t locality_queries = 0;
  } stats_;
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_PARTITION_STRATEGY_MANAGER_H_
