// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_PARTITION_PARTITION_STRATEGY_H_
#define CEDAR_PARTITION_PARTITION_STRATEGY_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include "cedar/core/status.h"

namespace cedar {
namespace partition {

// 分区分配结果
struct PartitionAssignment {
  uint32_t partition_id;
  double confidence = 1.0;      // 置信度 (0.0-1.0)
  std::string strategy_tag;      // 使用的策略标识
  
  PartitionAssignment() = default;
  PartitionAssignment(uint32_t pid, double conf = 1.0, const std::string& tag = "")
      : partition_id(pid), confidence(conf), strategy_tag(tag) {}
};

// 事件类型（用于 MTH 流式分区）
struct GraphEvent {
  uint64_t entity_id = 0;        // 顶点或边的一端ID
  uint64_t target_id = 0;        // 边的另一端ID（顶点时为0）
  uint64_t timestamp = 0;        // 微秒级时间戳
  uint16_t type_id = 0;          // 边类型或列ID
  uint8_t  entity_type = 0;      // 0=Vertex, 1=EdgeOut, 2=EdgeIn
  uint8_t  op_type = 0;          // 0=CREATE, 1=UPDATE, 2=DELETE
  
  GraphEvent() = default;
  GraphEvent(uint64_t eid, uint64_t tid, uint64_t ts, 
             uint16_t ty, uint8_t et, uint8_t op)
      : entity_id(eid), target_id(tid), timestamp(ts),
        type_id(ty), entity_type(et), op_type(op) {}
};

// 分区策略接口
class IPartitionStrategy {
 public:
  virtual ~IPartitionStrategy() = default;
  
  // 策略名称
  virtual const char* Name() const = 0;
  
  // 单点查询路由 - 静态/简单场景
  virtual PartitionAssignment RouteVertex(uint64_t vertex_id) = 0;
  
  // 边查询路由 - 返回两个端点的分区
  virtual std::pair<PartitionAssignment, PartitionAssignment> 
      RouteEdge(uint64_t src_id, uint64_t dst_id) = 0;
  
  // 批量事件处理 - 流式场景 (MTH 专用)
  virtual Status ProcessEventStream(const std::vector<GraphEvent>& events) {
    return Status::NotSupported("Event stream processing not supported");
  }
  
  // 获取分区统计信息
  virtual StatusOr<std::string> GetStats() const {
    return Status::NotSupported("Stats not available");
  }
  
  // 配置参数
  virtual Status Configure(const std::string& key, const std::string& value) {
    return Status::NotSupported("Configuration not supported");
  }
  
  // 是否支持时态路由
  virtual bool SupportsTemporalRouting() const { return false; }
  
  // 时态路由（带时间戳的查询）
  virtual PartitionAssignment RouteVertexTemporal(
      uint64_t vertex_id, uint64_t timestamp) {
    (void)timestamp;
    return RouteVertex(vertex_id);
  }
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_PARTITION_STRATEGY_H_
