# CedarGraph 图分割算法集成设计文档

**文档版本**: 1.0  
**创建日期**: 2026-04-12  
**设计目标**: 将主流图分割算法（METIS、PowerLyra、Ginger）集成到 CedarGraph，实现高性能图分区

---

## 1. 设计概览

### 1.1 背景

CedarGraph 当前采用一致性哈希进行分区，虽然简单高效，但缺乏对图拓扑结构的感知能力。本设计引入先进的图分割算法，优化：

- **查询性能**: 减少跨分区遍历的通信开销
- **负载均衡**: 处理自然图的幂律分布特性
- **扩展性**: 支持在线和离线分区策略

### 1.2 设计原则

1. **渐进式集成**: 不破坏现有架构，逐步引入新算法
2. **可插拔设计**: 支持多种分区策略动态切换
3. **零停机迁移**: 支持运行时动态重分区
4. **向后兼容**: 现有分区数据无缝迁移

### 1.3 架构总览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CedarGraph Graph Partitioning                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    GraphPartitioningEngine                            │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │  │
│  │  │   METIS      │  │  PowerLyra   │  │    Ginger    │               │  │
│  │  │  Partitioner │  │  Partitioner │  │  Partitioner │               │  │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘               │  │
│  │         └──────────────────┼──────────────────┘                        │  │
│  │                            ▼                                          │  │
│  │                   ┌─────────────────┐                                 │  │
│  │                   │ Strategy Factory│                                 │  │
│  │                   └────────┬────────┘                                 │  │
│  └────────────────────────────┼─────────────────────────────────────────┘  │
│                               ▼                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    PartitionManager (Enhanced)                        │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │  │
│  │  │ Graph-aware  │  │   Hybrid     │  │    Hash      │               │  │
│  │  │   Router     │  │   Cut        │  │  (Legacy)    │               │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                               ▼                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    DynamicRepartitioningManager                       │  │
│  │  • Hotspot Detection    • Migration Planning    • Online Adjustment   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 模块设计

### 2.1 图分割算法模块

#### 2.1.1 模块位置

```
include/cedar/partition/
├── graph_partitioner.h           # 抽象接口
├── metis/
│   ├── metis_adapter.h           # METIS 适配器
│   └── metis_partitioner.h       # METIS 分区实现
├── powerlyra/
│   ├── hybrid_cut.h              # Hybrid-Cut 算法
│   └── powerlyra_partitioner.h   # PowerLyra 分区器
├── ginger/
│   ├── fennel_heuristic.h        # Fennel 启发式
│   └── ginger_partitioner.h      # Ginger 分区器
└── common/
    ├── graph_topology.h          # 图拓扑表示
    ├── partition_cost_model.h    # 分区成本模型
    └── metrics.h                 # 评估指标

src/partition/
├── metis/
│   └── metis_partitioner.cc
├── powerlyra/
│   └── hybrid_cut.cc
├── ginger/
│   └── ginger_partitioner.cc
└── common/
    └── metrics.cc
```

#### 2.1.2 抽象接口设计

```cpp
// include/cedar/partition/graph_partitioner.h

namespace cedar {
namespace partition {

/**
 * @brief 图分区算法接口
 */
class GraphPartitioner {
 public:
  virtual ~GraphPartitioner() = default;
  
  /**
   * @brief 执行图分区
   * 
   * @param graph 输入图拓扑
   * @param num_partitions 目标分区数
   * @param options 分区选项
   * @return 分区结果（顶点 -> 分区映射）
   */
  virtual std::unordered_map<VertexID, PartitionID> Partition(
      const GraphTopology& graph,
      size_t num_partitions,
      const PartitionOptions& options) = 0;
  
  /**
   * @brief 获取分区器名称
   */
  virtual std::string Name() const = 0;
  
  /**
   * @brief 获取算法复杂度信息
   */
  virtual PartitionComplexity GetComplexity() const = 0;
  
  /**
   * @brief 是否支持增量分区
   */
  virtual bool SupportsIncremental() const { return false; }
  
  /**
   * @brief 增量更新分区（如有支持）
   */
  virtual std::unordered_map<VertexID, PartitionID> IncrementalPartition(
      const GraphTopology& delta_graph,
      const std::unordered_map<VertexID, PartitionID>& current_partition) {
    return {};
  }
};

/**
 * @brief 分区选项
 */
struct PartitionOptions {
  // 通用选项
  double imbalance_factor = 1.05;     // 不平衡因子 (1.0 = 完美平衡)
  uint32_t random_seed = 0;            // 随机种子
  
  // METIS 特定选项
  struct MetisOptions {
    int objective_type = 0;            // 0=edge-cut, 1=volume
    int coarsening_level = 0;          // 粗化级别 (0=自动)
    int refinement_iterations = 0;     // 细化迭代次数 (0=自动)
  } metis;
  
  // PowerLyra 特定选项
  struct PowerLyraOptions {
    int degree_threshold = 1000;       // 高度/低度阈值
    bool use_greedy_heuristic = true;  // 是否使用 Ginger 启发式
  } powerlyra;
  
  // Ginger 特定选项
  struct GingerOptions {
    double locality_weight = 1.0;      // 局部性权重
    double balance_weight = 1.0;       // 平衡权重
  } ginger;
};

}  // namespace partition
}  // namespace cedar
```

### 2.2 METIS 集成

#### 2.2.1 适配器设计

```cpp
// include/cedar/partition/metis/metis_adapter.h

namespace cedar {
namespace partition {

/**
 * @brief METIS 适配器
 * 
 * 将 CedarGraph 的图拓扑转换为 METIS 格式
 */
class MetisAdapter {
 public:
  /**
   * @brief 转换为 METIS CSR 格式
   */
  static MetisCSRGraph ConvertToCSR(const GraphTopology& graph);
  
  /**
   * @brief 应用分区结果
   */
  static std::unordered_map<VertexID, PartitionID> ApplyPartitionResult(
      const idx_t* metis_partition,
      size_t num_vertices,
      const std::vector<VertexID>& original_ids);
  
 private:
  struct MetisCSRGraph {
    std::vector<idx_t> xadj;      // CSR 偏移数组
    std::vector<idx_t> adjncy;    // CSR 邻接数组
    std::vector<idx_t> vwgt;      // 顶点权重（可选）
    std::vector<idx_t> adjwgt;    // 边权重（可选）
    idx_t nvertices;
    idx_t nedges;
  };
};

/**
 * @brief METIS 分区器实现
 */
class MetisPartitioner : public GraphPartitioner {
 public:
  std::unordered_map<VertexID, PartitionID> Partition(
      const GraphTopology& graph,
      size_t num_partitions,
      const PartitionOptions& options) override;
  
  std::string Name() const override { return "METIS"; }
  
  PartitionComplexity GetComplexity() const override {
    return {O_E_LOG_V, "O(|E| log |V|)"};
  }
  
 private:
  Status ValidateOptions(const PartitionOptions& options);
  void SetMetisOptions(idx_t* options, const PartitionOptions& opts);
};

}  // namespace partition
}  // namespace cedar
```

#### 2.2.2 实现要点

```cpp
// src/partition/metis/metis_partitioner.cc

std::unordered_map<VertexID, PartitionID> MetisPartitioner::Partition(
    const GraphTopology& graph,
    size_t num_partitions,
    const PartitionOptions& options) {
  
  // 1. 转换为 METIS CSR 格式
  auto csr = MetisAdapter::ConvertToCSR(graph);
  
  // 2. 设置 METIS 选项
  idx_t options[METIS_NOPTIONS];
  METIS_SetDefaultOptions(options);
  SetMetisOptions(options, options);
  
  // 3. 调用 METIS
  idx_t nparts = static_cast<idx_t>(num_partitions);
  idx_t ncon = 1;  // 约束数
  idx_t edgecut;
  std::vector<idx_t> partition(csr.nvertices);
  
  int result = METIS_PartGraphKway(
      &csr.nvertices,     // 顶点数
      &ncon,              // 约束数
      csr.xadj.data(),    // CSR 偏移
      csr.adjncy.data(),  // CSR 邻接
      csr.vwgt.data(),    // 顶点权重
      nullptr,            // 顶点大小
      csr.adjwgt.data(),  // 边权重
      &nparts,            // 分区数
      nullptr,            // 目标分区权重
      nullptr,            // 不平衡因子
      options,            // 选项
      &edgecut,           // 边切割数
      partition.data()    // 输出分区
  );
  
  if (result != METIS_OK) {
    LOG(ERROR) << "METIS partitioning failed with code: " << result;
    return {};
  }
  
  LOG(INFO) << "METIS partitioning complete. Edge cut: " << edgecut;
  
  // 4. 转换回 CedarGraph 格式
  return MetisAdapter::ApplyPartitionResult(
      partition.data(), csr.nvertices, graph.GetOriginalIds());
}
```

### 2.3 PowerLyra 集成

#### 2.3.1 Hybrid-Cut 算法实现

```cpp
// include/cedar/partition/powerlyra/hybrid_cut.h

namespace cedar {
namespace partition {

/**
 * @brief Hybrid-Cut 算法实现
 * 
 * 对低度顶点使用 Edge-Cut
 * 对高度顶点使用 Vertex-Cut
 */
class HybridCutAlgorithm {
 public:
  struct Config {
    int degree_threshold = 1000;       // 高度/低度阈值
    bool use_destination_hash = true;  // 对低度点使用目的点哈希
  };
  
  /**
   * @brief 执行 Hybrid-Cut 分区
   */
  std::unordered_map<EdgeID, PartitionID> PartitionEdges(
      const GraphTopology& graph,
      size_t num_partitions,
      const Config& config);
  
  /**
   * @brief 获取高度顶点集合
   */
  std::unordered_set<VertexID> GetHighDegreeVertices(
      const GraphTopology& graph,
      int threshold);
  
 private:
  bool IsHighDegree(const Vertex& v, int threshold) {
    return v.in_degree + v.out_degree >= threshold;
  }
};

/**
 * @brief PowerLyra 分区器
 */
class PowerLyraPartitioner : public GraphPartitioner {
 public:
  std::unordered_map<VertexID, PartitionID> Partition(
      const GraphTopology& graph,
      size_t num_partitions,
      const PartitionOptions& options) override;
  
  std::string Name() const override { return "PowerLyra-HybridCut"; }
  
  PartitionComplexity GetComplexity() const override {
    return {O_E, "O(|E|)"};
  }
  
 private:
  HybridCutAlgorithm hybrid_cut_;
};

}  // namespace partition
}  // namespace cedar
```

#### 2.3.2 算法流程

```cpp
std::unordered_map<VertexID, PartitionID> PowerLyraPartitioner::Partition(
    const GraphTopology& graph,
    size_t num_partitions,
    const PartitionOptions& options) {
  
  const auto& config = options.powerlyra;
  
  // 1. 识别高度顶点
  auto high_degree_vertices = hybrid_cut_.GetHighDegreeVertices(
      graph, config.degree_threshold);
  
  LOG(INFO) << "PowerLyra: " << high_degree_vertices.size() 
            << " high-degree vertices identified";
  
  // 2. 分配边到分区
  std::unordered_map<EdgeID, PartitionID> edge_assignment;
  
  for (const auto& edge : graph.GetEdges()) {
    PartitionID assigned_partition;
    
    if (high_degree_vertices.count(edge.dst)) {
      // 高度顶点：Vertex-Cut（按源点哈希）
      assigned_partition = Hash(edge.src) % num_partitions;
    } else {
      // 低度顶点：Edge-Cut（按目的点哈希）
      assigned_partition = Hash(edge.dst) % num_partitions;
    }
    
    edge_assignment[edge.id] = assigned_partition;
  }
  
  // 3. 转换边分配为顶点分配（用于 CedarGraph 兼容性）
  return ConvertEdgeAssignmentToVertexAssignment(
      edge_assignment, graph, high_degree_vertices);
}
```

### 2.4 Ginger 集成

#### 2.4.1 Fennel 启发式实现

```cpp
// include/cedar/partition/ginger/fennel_heuristic.h

namespace cedar {
namespace partition {

/**
 * @brief Fennel 启发式评分函数
 */
class FennelHeuristic {
 public:
  struct Config {
    double alpha = 1.5;        // 超参数 alpha
    double gamma = 1.0;        // 平衡因子
    size_t total_edges = 0;    // 总边数
  };
  
  /**
   * @brief 计算顶点到分区的成本
   * 
   * cost(v, p) = |N(v) ∩ Vp| - b(p)
   * 
   * 其中 b(p) 是归一化平衡成本
   */
  double ComputeCost(
      VertexID vertex,
      PartitionID partition,
      const GraphTopology& graph,
      const PartitionState& state,
      const Config& config);
  
  /**
   * @brief 计算平衡成本
   */
  double ComputeBalanceCost(
      PartitionID partition,
      const PartitionState& state,
      const Config& config);
  
 private:
  // 计算顶点在分区中的邻居数量
  size_t CountNeighborsInPartition(
      VertexID vertex,
      PartitionID partition,
      const GraphTopology& graph,
      const PartitionState& state);
};

}  // namespace partition
}  // namespace cedar
```

#### 2.4.2 Ginger 分区器

```cpp
class GingerPartitioner : public GraphPartitioner {
 public:
  std::unordered_map<VertexID, PartitionID> Partition(
      const GraphTopology& graph,
      size_t num_partitions,
      const PartitionOptions& options) override;
  
  std::string Name() const override { return "Ginger"; }
  
  bool SupportsIncremental() const override { return true; }
  
 private:
  // 第一遍：处理所有边（基于目的点哈希）
  void FirstPass(GraphTopology& graph,
                 size_t num_partitions,
                 const GingerOptions& options);
  
  // 第二遍：处理低度顶点（使用 Fennel 启发式）
  void SecondPassForLowDegree(
      GraphTopology& graph,
      size_t num_partitions,
      const GingerOptions& options,
      const std::unordered_set<VertexID>& high_degree_vertices);
  
  // 第二遍：处理高度顶点（使用源点哈希）
  void SecondPassForHighDegree(
      GraphTopology& graph,
      size_t num_partitions,
      const std::unordered_set<VertexID>& high_degree_vertices);
  
  FennelHeuristic fennel_;
};
```

---

## 3. 与现有系统集成

### 3.1 分区策略工厂

```cpp
// include/cedar/partition/partition_strategy_factory.h

class PartitionStrategyFactory {
 public:
  enum class StrategyType {
    kHash,           // 现有哈希策略
    kRange,          // 范围分区
    kMetis,          // METIS 高质量分割
    kPowerLyra,      // PowerLyra 混合分割
    kGinger,         // Ginger 局部性优化
    kGraphAware,     // 现有图感知策略（增强）
  };
  
  /**
   * @brief 创建分区策略
   */
  static std::unique_ptr<PartitionStrategy> Create(
      StrategyType type,
      const StrategyConfig& config);
  
  /**
   * @brief 根据图特征自动选择策略
   */
  static StrategyType AutoSelectStrategy(
      const GraphTopology& graph,
      const WorkloadCharacteristics& workload);
  
  /**
   * @brief 获取策略描述
   */
  static std::string GetStrategyDescription(StrategyType type);
};
```

### 3.2 PartitionManager 扩展

```cpp
// include/cedar/partition/enhanced_partition_manager.h

class EnhancedPartitionManager : public PartitionManager {
 public:
  /**
   * @brief 初始化图分区引擎
   */
  Status InitializeGraphPartitioning(
      std::unique_ptr<GraphPartitioner> partitioner);
  
  /**
   * @brief 使用图算法重新分区
   * 
   * @param algorithm 分区算法类型
   * @param dry_run 仅生成计划不执行
   * @return 迁移计划
   */
  StatusOr<RepartitionPlan> RepartitionWithAlgorithm(
      PartitionStrategyFactory::StrategyType algorithm,
      bool dry_run = false);
  
  /**
   * @brief 分析当前分区质量
   */
  PartitionQualityMetrics AnalyzePartitionQuality();
  
  /**
   * @brief 获取图拓扑（用于分区算法）
   */
  StatusOr<GraphTopology> ExportGraphTopology(
      const std::vector<PartitionID>& partitions);
  
 private:
  std::unique_ptr<GraphPartitioner> graph_partitioner_;
  std::unique_ptr<DynamicRepartitioningManager> repartition_manager_;
};
```

### 3.3 动态重分区管理器

```cpp
// include/cedar/partition/dynamic_repartitioning_manager.h

class DynamicRepartitioningManager {
 public:
  struct TriggerCondition {
    double load_imbalance_threshold = 0.2;     // 负载不平衡阈值
    double edge_cut_threshold = 0.3;           // 边切割比例阈值
    uint64_t min_repartition_interval_sec = 3600;  // 最小重分区间隔
  };
  
  /**
   * @brief 检查是否需要重分区
   */
  bool NeedsRepartitioning(const PartitionStats& stats);
  
  /**
   * @brief 生成迁移计划
   */
  StatusOr<MigrationPlan> GenerateMigrationPlan(
      const std::unordered_map<VertexID, PartitionID>& new_partitioning);
  
  /**
   * @brief 执行渐进式迁移
   */
  Status ExecuteGradualMigration(
      const MigrationPlan& plan,
      const MigrationCallbacks& callbacks);
  
 private:
  TriggerCondition triggers_;
  std::chrono::steady_clock::time_point last_repartition_time_;
  
  // 热点检测
  std::unordered_set<VertexID> DetectHotspots(const PartitionStats& stats);
  
  // 计算迁移成本
  double ComputeMigrationCost(
      const MigrationPlan& plan,
      const PartitionStats& stats);
};
```

---

## 4. 数据流与工作流程

### 4.1 批量导入时的离线分区

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Offline Partitioning (Bulk Load)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Input Graph Data                                                            │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────┐                                                         │
│  │ Graph Sampling  │  (如果图太大，进行采样)                                  │
│  └────────┬────────┘                                                         │
│           ▼                                                                  │
│  ┌─────────────────┐     ┌─────────────────┐                                │
│  │ Build Topology  │────▶│ Select Algorithm│                                │
│  └────────┬────────┘     │ (Auto/METIS/etc)│                                │
│           │              └─────────────────┘                                │
│           ▼                                                                  │
│  ┌─────────────────┐                                                         │
│  │ Run Partitioner │  ◄── 导出为 CedarGraph 分区格式                          │
│  └────────┬────────┘                                                         │
│           ▼                                                                  │
│  ┌─────────────────┐                                                         │
│  │ Assign to Nodes │  ◄── 使用一致性哈希放置分区                              │
│  └────────┬────────┘                                                         │
│           ▼                                                                  │
│  ┌─────────────────┐                                                         │
│  │ Data Import     │  ◄── 实际数据导入                                        │
│  └─────────────────┘                                                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 运行时动态重分区

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Dynamic Repartitioning at Runtime                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │ Load Monitor │───▶│ Hotspot Detect│───▶│ Trigger Check│                   │
│  └──────────────┘    └──────────────┘    └──────┬───────┘                   │
│                                                  │                          │
│                    Yes                           ▼                          │
│           ┌──────────────────────────────────────────────┐                  │
│           │  Start Repartitioning Process               │                  │
│           └──────────┬───────────────────────────────────┘                  │
│                      ▼                                                       │
│           ┌─────────────────────┐                                           │
│           │ Export Subgraph     │  ◄── 导出热点区域子图                        │
│           └──────────┬──────────┘                                           │
│                      ▼                                                       │
│           ┌─────────────────────┐                                           │
│           │ Run Incremental     │  ◄── 运行轻量级分区算法                      │
│           │ Partitioning        │     (Ginger/PowerLyra)                     │
│           └──────────┬──────────┘                                           │
│                      ▼                                                       │
│           ┌─────────────────────┐                                           │
│           │ Generate Migration  │  ◄── 生成顶点迁移计划                        │
│           │ Plan                │                                           │
│           └──────────┬──────────┘                                           │
│                      ▼                                                       │
│           ┌─────────────────────┐                                           │
│           │ Execute Migration   │  ◄── 渐进式迁移（不影响服务）                │
│           │ (Online)            │                                           │
│           └──────────┬──────────┘                                           │
│                      ▼                                                       │
│           ┌─────────────────────┐                                           │
│           │ Update Routing Table│  ◄── 更新路由元数据                         │
│           └─────────────────────┘                                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. 配置与部署

### 5.1 配置示例

```yaml
# config/graph_partitioning.yaml

partitioning:
  # 默认分区策略
  default_strategy: "ginger"
  
  # 自动选择策略配置
  auto_select:
    enabled: true
    threshold_metis_vertices: 1000000   # >1M 顶点使用 METIS
    threshold_powerlyra_degree: 1000    # 最大度 >1000 使用 PowerLyra
  
  # METIS 配置
  metis:
    objective_type: "edge_cut"          # edge_cut 或 volume
    imbalance_factor: 1.05
    coarsening_level: 0                 # 0 = 自动
    
  # PowerLyra 配置
  powerlyra:
    degree_threshold: 1000
    use_greedy_heuristic: true
    
  # Ginger 配置
  ginger:
    locality_weight: 1.0
    balance_weight: 1.0
    incremental_enabled: true
    
  # 动态重分区配置
  dynamic_repartitioning:
    enabled: true
    check_interval_sec: 300             # 5 分钟检查一次
    load_imbalance_threshold: 0.2       # 20% 不平衡触发
    min_repartition_interval_sec: 3600  # 至少间隔 1 小时
    max_migration_rate_mbps: 100        # 迁移限速 100MB/s
```

### 5.2 API 设计

```protobuf
// proto/graph_partitioning.proto

syntax = "proto3";

package cedar.partition;

service GraphPartitioningService {
  // 获取当前分区策略
  rpc GetPartitioningStrategy(GetPartitioningStrategyRequest) 
      returns (GetPartitioningStrategyResponse);
  
  // 设置分区策略
  rpc SetPartitioningStrategy(SetPartitioningStrategyRequest) 
      returns (SetPartitioningStrategyResponse);
  
  // 执行重分区
  rpc Repartition(RepartitionRequest) 
      returns (stream RepartitionProgress);
  
  // 获取分区质量指标
  rpc GetPartitionQuality(GetPartitionQualityRequest) 
      returns (GetPartitionQualityResponse);
  
  // 分析分区建议
  rpc AnalyzePartitioning(AnalyzePartitioningRequest) 
      returns (AnalyzePartitioningResponse);
}

message PartitionQualityMetrics {
  double edge_cut_ratio = 1;           // 边切割比例
  double load_imbalance = 2;           // 负载不平衡度
  double locality_score = 3;           // 局部性评分
  uint64_t cross_partition_edges = 4;  // 跨分区边数
  double replication_factor = 5;       // 复制因子
}

message RepartitionRequest {
  string strategy = 1;                 // 使用策略
  bool dry_run = 2;                    // 仅预览
  bool online = 3;                     // 在线迁移
  uint32 max_parallel_migrations = 4;  // 最大并行迁移数
}
```

---

## 6. 性能评估

### 6.1 评估指标

| 指标 | 说明 | 目标值 |
|------|------|-------|
| **Edge Cut Ratio** | 跨分区边数 / 总边数 | < 10% |
| **Load Imbalance** | 最大分区大小 / 平均分区大小 | < 1.1 |
| **Query Latency** | 典型查询延迟（1-3 跳） | 降低 20-50% |
| **Throughput** | 查询吞吐量 | 提升 20-40% |
| **Migration Time** | 重分区迁移时间 | < 1 小时/TB |

### 6.2 对比测试计划

```
测试数据集:
1. LDBC SNB (Social Network Benchmark)
   - 规模: 1M / 10M / 100M 顶点
   - 特征: 社交网络，幂律分布

2. Twitter 图
   - 规模: 41M 顶点, 1.4B 边
   - 特征: 真实社交网络，高度倾斜

3. CedarGraph 合成数据
   - 时态图数据
   - 多类型边

测试维度:
1. 不同分区策略对比 (Hash vs METIS vs PowerLyra vs Ginger)
2. 不同查询类型性能 (点查、1跳、2跳、3跳)
3. 动态重分区开销
```

---

## 7. 实现路线图

### Phase 1: 基础框架 (2 周)

- [ ] 创建分区模块目录结构
- [ ] 实现 GraphPartitioner 抽象接口
- [ ] 实现 GraphTopology 数据结构
- [ ] 集成 METIS 库（CMake 配置）

### Phase 2: METIS 集成 (2 周)

- [ ] 实现 METIS 适配器
- [ ] 实现 CSR 格式转换
- [ ] 实现分区结果映射
- [ ] 离线批量导入集成
- [ ] 性能测试与优化

### Phase 3: PowerLyra 集成 (2 周)

- [ ] 实现 Hybrid-Cut 算法
- [ ] 实现高度顶点识别
- [ ] 边分配与顶点分配转换
- [ ] 与现有路由系统集成
- [ ] 性能对比测试

### Phase 4: Ginger 集成 (2 周)

- [ ] 实现 Fennel 启发式
- [ ] 实现增量分区
- [ ] 局部性优化
- [ ] 运行时集成
- [ ] 综合性能测试

### Phase 5: 动态重分区 (2 周)

- [ ] 实现动态重分区管理器
- [ ] 热点检测算法
- [ ] 渐进式迁移实现
- [ ] 在线重分区测试
- [ ] 容错与回滚机制

### Phase 6: 生产就绪 (1 周)

- [ ] 完整测试覆盖
- [ ] 性能基准测试
- [ ] 文档完善
- [ ] 配置工具
- [ ] 监控告警

---

## 8. 风险评估与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| METIS 处理大图性能差 | 中 | 采样 + 分阶段分区 |
| 动态重分区影响服务 | 高 | 限速迁移、低峰期执行、回滚机制 |
| 新分区策略不兼容旧数据 | 高 | 保持哈希策略兼容、渐进迁移 |
| 算法复杂度导致延迟 | 中 | 异步执行、缓存结果 |
| 图拓扑变化快导致频繁重分区 | 低 | 防抖机制、最小间隔限制 |

---

## 9. 总结

本设计文档提供了将主流图分割算法集成到 CedarGraph 的完整方案：

1. **METIS**: 用于离线批量导入，提供高质量分割
2. **PowerLyra**: 处理幂律分布图，混合策略平衡负载
3. **Ginger**: 运行时局部性优化，支持增量分区

通过可插拔架构和渐进式集成，CedarGraph 将获得：

- 最高 5.53x 的性能提升（基于 PowerLyra 论文数据）
- 显著降低的跨分区通信开销
- 自适应工作负载的动态分区能力
- 向后兼容的平滑迁移路径

---

*设计文档完成 - 等待评审*
