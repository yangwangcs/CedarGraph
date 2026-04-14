# CedarGraph 双模式分区策略设计

## 概述

CedarGraph 将支持两种图分区策略，允许用户根据工作负载特征选择最适合的分区方式：

| 模式 | 名称 | 适用场景 | 特点 |
|------|------|----------|------|
| Mode 1 | **StaticHash** | 通用场景、简单查询 | 低延迟、无状态、均匀分布 |
| Mode 2 | **MTHStream** | 时态查询、局部性强的负载 | 高局部性、自适应、适合时态图分析 |

---

## 1. 架构设计

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      PartitionStrategyManager                            │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────┐  │
│  │  Strategy       │    │  Strategy       │    │  Strategy Registry  │  │
│  │  Factory        │───▶│  Selector       │───▶│  - static_hash      │  │
│  │                 │    │                 │    │  - mth_stream       │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────┘  │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
    ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
    │  StaticHash     │ │  MTHStream      │ │  (Future        │
    │  Strategy       │ │  Strategy       │ │   Strategies)   │
    │                 │ │                 │ │                 │
    │ hash(vid) % n   │ │ TemporalSketch  │ │                 │
    │                 │ │ + Affinity      │ │                 │
    └─────────────────┘ └─────────────────┘ └─────────────────┘
```

---

## 2. 核心接口设计

### 2.1 分区策略基类

```cpp
// src/partition/partition_strategy.h
#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "cedar/core/status.h"

namespace cedar {
namespace partition {

// 分区分配结果
struct PartitionAssignment {
  uint32_t partition_id;
  double confidence;      // 置信度 (0.0-1.0)，用于 MTH 的 fast-path 统计
  std::string strategy_tag; // 使用的策略标识
};

// 事件类型（用于 MTH 流式分区）
struct GraphEvent {
  uint64_t entity_id;     // 顶点或边的一端ID
  uint64_t target_id;     // 边的另一端ID（顶点时为0）
  uint64_t timestamp;     // 微秒级时间戳
  uint16_t type_id;       // 边类型或列ID
  uint8_t  entity_type;   // 0=Vertex, 1=EdgeOut, 2=EdgeIn
  uint8_t  op_type;       // 0=CREATE, 1=UPDATE, 2=DELETE
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
    return RouteVertex(vertex_id);
  }
};

} // namespace partition
} // namespace cedar
```

### 2.2 静态哈希策略（NebulaGraph 风格）

```cpp
// src/partition/strategies/static_hash_strategy.h
#pragma once

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
```

### 2.3 MTH 流式策略（subgraph2 算法）

```cpp
// src/partition/strategies/mth_stream_strategy.h
#pragma once

#include "partition/partition_strategy.h"
#include "cedar/mth_partitioner.h"  // 从 subgraph2 引入

namespace cedar {
namespace partition {

// 基于 subgraph2 MTHPartitioner 的流式分区策略
class MTHStreamStrategy : public IPartitionStrategy {
 public:
  struct Config {
    uint32_t num_partitions = 65536;
    size_t capacity = 1000000;           // 每分区容量
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
  cedar::CedarKey ConvertToCedarKey(const GraphEvent& event);
};

} // namespace partition
} // namespace cedar
```

---

## 3. 策略管理器

```cpp
// src/partition/partition_strategy_manager.h
#pragma once

#include "partition/partition_strategy.h"
#include <memory>
#include <unordered_map>

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
```

---

## 4. 配置集成

```yaml
# config/partition.yaml
partition:
  # 全局分区数
  num_partitions: 65536
  
  # 策略选择
  strategy:
    # 可选: static_hash, mth_stream, auto
    type: "static_hash"
    
    # 自动选择配置（当 type=auto 时有效）
    auto_selection:
      temporal_query_threshold: 100
      locality_ratio_threshold: 0.7
      evaluation_window: 3600  # 秒
  
  # 静态哈希策略配置
  static_hash:
    hash_function: "murmur3"  # murmur3, cityhash, xxhash
    
  # MTH 流式策略配置
  mth_stream:
    capacity: 1000000
    alpha: 1.0
    beta: 1.0
    gamma: 0.0
    eta: 0.0
    temporal_alpha: 0.01
    sketch_depth: 3
    sketch_width: 64
    fast_path_threshold: 0.6
    load_relaxation: 0.0
    decay_interval: 0
    decay_factor: 0.95
```

---

## 5. 使用示例

### 5.1 初始化与策略选择

```cpp
#include "partition/partition_strategy_manager.h"

// 初始化
auto manager = std::make_unique<PartitionStrategyManager>();

// 配置
StrategySelectionConfig config;
config.default_strategy = StrategyType::STATIC_HASH;
manager->Initialize(config);

// 注册静态哈希策略
auto static_hash = std::make_unique<StaticHashStrategy>(65536);
manager->RegisterStrategy(std::move(static_hash));

// 注册 MTH 策略
MTHStreamStrategy::Config mth_config;
mth_config.num_partitions = 65536;
mth_config.fast_path_threshold = 0.6;
auto mth_stream = std::make_unique<MTHStreamStrategy>(mth_config);
manager->RegisterStrategy(std::move(mth_stream));

// 设置活跃策略
manager->SetActiveStrategy(StrategyType::STATIC_HASH);
// 或
manager->SetActiveStrategy("MTHStream");
```

### 5.2 查询路由

```cpp
// 单点查询
uint64_t vertex_id = 12345;
auto assignment = manager->RouteVertex(vertex_id);
std::cout << "Vertex " << vertex_id << " -> Partition " 
          << assignment.partition_id << std::endl;

// 时态查询
uint64_t timestamp = 1712563200000000;  // 微秒
auto assignment = manager->RouteVertexTemporal(vertex_id, timestamp);

// 边查询
auto [src_assign, dst_assign] = manager->RouteEdge(src_id, dst_id);
```

### 5.3 流式事件处理（MTH 模式）

```cpp
// 批量事件处理（MTH 模式）
std::vector<GraphEvent> events;
events.push_back({100, 200, timestamp, 0, 0, 0});  // CREATE Vertex
events.push_back({100, 200, timestamp, 1, 1, 0});  // CREATE EdgeOut

cedar::Status status = manager->ProcessEventStream(events);
```

---

## 6. 与现有系统集成

### 6.1 修改 GraphServiceRouter

```cpp
// src/service/graph_service_router.cc

uint32_t GraphServiceRouter::CalculatePartition(uint64_t entity_id) {
  // 使用策略管理器替代直接取模
  auto assignment = strategy_manager_->RouteVertex(entity_id);
  return assignment.partition_id;
}

// 新增时态路由
uint32_t GraphServiceRouter::CalculatePartitionTemporal(
    uint64_t entity_id, uint64_t timestamp) {
  auto assignment = strategy_manager_->RouteVertexTemporal(entity_id, timestamp);
  return assignment.partition_id;
}
```

### 6.2 MetaD 分区分配器集成

```cpp
// src/service/partition_allocator.cc

// 根据策略类型选择合适的分区分配方式
Status PartitionAllocator::AllocatePartition(uint32_t partition_id) {
  if (strategy_manager_->GetActiveStrategy()->Name() == "MTHStream") {
    // MTH 模式：需要维护实体到分区的映射表
    return AllocateWithMTHStrategy(partition_id);
  }
  // 静态哈希模式
  return AllocateWithStaticStrategy(partition_id);
}
```

---

## 7. 性能对比

| 指标 | StaticHash | MTHStream |
|------|------------|-----------|
| 路由延迟 | O(1) | O(1) fast-path / O(k) fallback |
| 内存开销 | 无 | ~MB级 (Count-Min Sketch) |
| 局部性 | 随机 | 高（时态+拓扑） |
| 跨分区查询 | 高概率 | 低概率 |
| 适合工作负载 | 均匀随机访问 | 时序局部性、图遍历 |
| 吞吐量 | ~1M+ ops/s | ~400K+ ops/s |

---

## 8. 迁移路径

### 阶段 1: 基础实现
1. 实现 `IPartitionStrategy` 接口
2. 移植 subgraph2 的 MTHPartitioner
3. 实现 `StaticHashStrategy`

### 阶段 2: 集成测试
1. 单元测试两种策略
2. 集成到 GraphServiceRouter
3. 性能基准测试

### 阶段 3: 生产就绪
1. 动态切换支持
2. 监控和指标
3. 自动策略选择

---

## 9. 文件结构

```
src/partition/
├── partition_strategy.h              # 基类接口
├── partition_strategy_manager.h/cc   # 策略管理器
└── strategies/
    ├── static_hash_strategy.h/cc     # 静态哈希实现
    └── mth_stream_strategy.h/cc      # MTH 流式实现

# 从 subgraph2 移植
src/partition/mth/
├── cedar_key.h/cc
├── temporal_sketch.h/cc
├── streaming_partitioner.h/cc
├── mth_partitioner.h/cc
└── partition_state.h/cc
```
