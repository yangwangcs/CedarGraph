# 图分割算法及 CedarGraph 集成调研报告

**调研日期**: 2026-04-12  
**调研目标**: 调研主流图分割算法，为 CedarGraph 图分割集成方案提供技术基础

---

## 1. NebulaGraph 图分割策略

### 1.1 核心设计理念

NebulaGraph 采用**边分割（Edge-Cut）**策略，这是图数据库中常见的分布式存储方案，与"点分割（Vertex-Cut）"形成对比。

### 1.2 基于 VID 的哈希分区

| 特性 | 说明 |
|------|------|
| 分区算法 | 静态 Hash：`hash(vertex_id) % partition_num` |
| 默认分区数 | 100（创建图空间时指定） |
| 可修改性 | **创建后不可修改** |
| 分区放置 | 随机映射到存储节点 |

**分区计算公式**:
```
pId = (vid % number_of_partitions) + 1
```

**设计优势**:
- 同一个点的所有 Tag、出边和入边存储在同一分片
- 极大提升从某点开始的遍历查询效率
- 支持百亿级顶点、万亿级边规模

### 1.3 边的存储策略（双向存储）

NebulaGraph 中**逻辑上的一条边对应硬盘上的两个键值对**：

```
SrcVertex -[EdgeA]-> DstVertex
```

**存储布局**:

| 存储位置 | 内容 | Key 字段 |
|---------|------|---------|
| Partition x | 点 SrcVertex | Type, PartID(x), VID(Src), TagID |
| Partition x | 边 EdgeA_Out | Type, PartID(x), VID(Src), +EdgeType, Rank, VID(Dst) |
| Partition y | 点 DstVertex | Type, PartID(y), VID(Dst), TagID |
| Partition y | 边 EdgeA_In | Type, PartID(y), VID(Dst), -EdgeType, Rank, VID(Src) |

**设计考量**:
- EdgeA_Out：用于从起点开始的遍历 `(a)-[]->()`
- EdgeA_In：用于从终点开始的逆序遍历 `()-[]->(a)`
- 存储空间翻倍，但查询效率最大化

### 1.4 分区放置与 Raft 复制

```
┌─────────────────────────────────────────────────────────────┐
│                      Partition 1                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ Leader   │  │ Follower │  │ Follower │  ← Raft Group    │
│  │ Node A   │  │ Node B   │  │ Node C   │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

- 每个分片组成一个 Raft Group
- Leader 处理所有写请求
- Follower 同步数据，Leader 故障时选举新 Leader
- 副本因子（replica_factor）默认 3

### 1.5 扩容限制

| 限制项 | 说明 |
|-------|------|
| 分区数不可变 | CREATE SPACE 后无法修改 partition_num |
| 扩容粒度 | 整个图复制（不是单个分片） |
| 建议 | 初始设置时预留未来扩容空间 |

---

## 2. 主流图分割算法

### 2.1 METIS - 多级图分割算法

#### 2.1.1 算法概述

METIS 是由 Karypis 和 Kumar 开发的高性能图分割库，采用**多级（Multilevel）**分割策略。

#### 2.1.2 核心算法流程

```
原始图 → [Coarsening 粗化] → 小图 → [Initial Partitioning 初始分割] → 
[Uncoarsening 细化] → 最终分割结果
```

**Phase 1: Graph Coarsening（图粗化）**
- 通过收缩重边（heavy-edge matching）简化图
- 重复直到图足够小
- 更新顶点权重和边权重

**Phase 2: Initial Partitioning（初始分割）**
- 在最粗级别图上进行分割
- 可使用递归二分或 k-way 分割

**Phase 3: Refinement（细化）**
- 逐步展开图（uncoarsening）
- 每一步使用 Kernighan-Lin 算法细化分割
- 优化边切割数和负载均衡

#### 2.1.3 API 示例

```cpp
// METIS 基本调用
idx_t nvertices = 1000000;  // 顶点数
idx_t xadj[] = {...};       // CSR 格式偏移数组
idx_t adjncy[] = {...};     // CSR 格式邻接数组
idx_t nparts = 16;          // 分区数
idx_t part[nvertices];      // 输出分区数组

METIS_PartGraphKway(
    &nvertices,       // 顶点数
    &ncon,            // 约束数
    xadj,             // CSR 偏移
    adjncy,           // CSR 邻接
    vwgt,             // 顶点权重
    vsize,            // 顶点大小
    adjwgt,           // 边权重
    &nparts,          // 分区数
    tpwgts,           // 目标分区权重
    ubvec,            // 不平衡因子
    options,          // 选项
    &edgecut,         // 输出边切割数
    part              // 输出分区
);
```

#### 2.1.4 特性与变体

| 特性 | 说明 |
|------|------|
| METIS_PartGraphRecursive | 递归二分分割 |
| METIS_PartGraphKway | k-way 直接分割 |
| Multi-Constraint | 多约束分区（同时平衡 CPU 和内存） |
| Weighted | 支持加权图 |
| ParMETIS | 并行版本，适合分布式内存系统 |

#### 2.1.5 复杂度与适用场景

- **时间复杂度**: O(|E| log |V|)
- **适用规模**: 百万到千万级顶点
- **最佳场景**: 静态图、需要高质量分割的场景

---

### 2.2 PowerLyra - 差异化图计算与分割

#### 2.2.1 核心思想

PowerLyra 针对**自然图的幂律分布（Power-Law Distribution）**特性，提出**差异化处理**策略：

> 高度顶点（High-degree）和低度顶点（Low-degree）应该采用不同的计算和分区策略。

#### 2.2.2 现有系统问题分析

| 系统 | 策略 | 高度顶点问题 | 低度顶点问题 |
|------|------|-------------|-------------|
| Pregel/GraphLab | Edge-Cut | 负载不均衡、高竞争 | - |
| PowerGraph/GraphX | Vertex-Cut | - | 高通信开销、内存消耗 |

#### 2.2.3 Hybrid-Cut 算法

```
┌─────────────────────────────────────────────────────────────┐
│                    PowerLyra Hybrid-Cut                      │
├─────────────────────────────────────────────────────────────┤
│  Low-degree Vertices (degree < threshold)                    │
│  ├── 策略: Edge-Cut (如传统系统)                             │
│  ├── 顶点与边存储在一起                                       │
│  └── 避免频繁通信                                            │
├─────────────────────────────────────────────────────────────┤
│  High-degree Vertices (degree >= threshold)                  │
│  ├── 策略: Vertex-Cut (如 PowerGraph)                        │
│  ├── 边均匀分布到多台机器                                     │
│  └── 平衡负载                                                │
└─────────────────────────────────────────────────────────────┘
```

**算法伪代码**:
```
for each edge e = (u, v):
    if degree(v) < threshold:           // 低度终点
        assign e to hash(v)             // Edge-cut
    else:                                // 高度终点
        assign e to hash(u)             // 使用源点哈希

for each high-degree vertex u:
    distribute edges of u evenly        // Vertex-cut
```

#### 2.2.4 性能提升

| 指标 | 提升 |
|------|------|
| 真实图性能 | 比 PowerGraph 快 **1.24x - 5.53x** |
| 合成图性能 | 比 PowerGraph 快 **1.49x - 3.26x** |
| 内存消耗 | 显著减少（复制因子降低） |

---

### 2.3 Ginger - Workload-Aware 分区

#### 2.3.1 核心思想

Ginger 是基于 Fennel 启发式的**异构感知**分区算法，在 PowerLyra 基础上进一步优化。

#### 2.3.2 成本函数

对于低度顶点，Ginger 使用以下成本函数决定分区分配：

```
c(v, p) = |N(v) ∩ Vp| - b(p)
```

其中：
- `|N(v) ∩ Vp|`: 顶点 v 在候选分区 p 中的邻居数量（局部性）
- `b(p)`: 归一化平衡因子，考虑边数和顶点数

#### 2.3.3 Skewed Ginger Cut

```
procedure SGINGER:
    // 第一遍：处理所有边
    for e ∈ E:
        d ← Dest(e)
        p ← iSk[HashV(d)]
        e.owner ← p
    
    // 第二遍：处理低度顶点
    for v ∈ V:
        if inDegree(v) > Threshold:
            // 高度顶点：Vertex-cut
            for e ∈ Edges(v):
                s ← Src(e)
                p ← iSk[HashV(s)]
                e.owner ← p
        else:
            // 低度顶点：使用 Fennel 启发式
            for p ∈ P:
                Vp ← Verts(p)
                cost[p] ← |N(v) ∩ Vp| - (1-Sk[p]) * b(p)
            e.owner ← argmax(cost)
```

#### 2.3.4 优势

- **复制因子**: 比 Random 算法显著降低
- **负载均衡**: 考虑异构节点的处理能力
- **局部性优化**: 最大化邻居聚集度

---

### 2.4 时态图分割研究

#### 2.4.1 时态图特性

时态图（Temporal Graph）具有时间维度，边带有时间戳：

```
G = (V, E, T)
E = {(u, v, t) | u, v ∈ V, t ∈ T}
```

#### 2.4.2 主要分割策略

| 策略 | 描述 | 适用场景 |
|------|------|---------|
| **Time-based Partitioning** | 按时间窗口分割 | 历史数据分析 |
| **Vertex-Temporal Hybrid** | 先按顶点分区，再按时间分片 | 实时查询 |
| **Chunk-based (PGC)** | 同时考虑空间和时间维度 | 动态图神经网络训练 |

#### 2.4.3 TeGraph 时态图引擎

TeGraph 提出时态感知图格式：

```
存储格式:
- 按时间递增顺序存储边
- 同一时间点的顶点分组存储
- 分区为可装入内存的块
```

**核心优化**:
- 单次扫描执行（Single-Pass Execution）
- 顶点分组优化变换
- 并行重标记

---

## 3. 图数据库分区策略对比

### 3.1 主流图数据库对比

| 数据库 | 分区策略 | 一致性协议 | 架构 |
|--------|---------|-----------|------|
| **NebulaGraph** | 静态哈希（Edge-Cut） | Raft | 分布式 |
| **JanusGraph** | 随机分区，支持显式指定 | Paxos | 分布式 |
| **TigerGraph** | 编码后分散（粗粒度副本） | - | 非对等分布式 |
| **Dgraph** | 自动分区 | Raft | 分布式 |
| **Neo4j** | 不支持分区（单机） | - | 单机/集群 |

### 3.2 Edge-Cut vs Vertex-Cut

| 维度 | Edge-Cut | Vertex-Cut |
|------|---------|-----------|
| **存储** | 顶点复制 | 边复制 |
| **通信** | 顶点同步 | 边同步 |
| **适用** | 低度图 | 高度图 |
| **负载** | 可能不均衡 | 更均衡 |
| **代表** | NebulaGraph, Pregel | PowerGraph, GraphX |

### 3.3 混合策略趋势

现代图系统趋向于**自适应混合策略**：

```
┌─────────────────────────────────────────────────────────────┐
│                   自适应图分区策略                            │
├─────────────────────────────────────────────────────────────┤
│  1. 分析图特征（度分布、局部性）                              │
│  2. 根据特征选择分区算法                                      │
│  3. 运行时监控负载                                            │
│  4. 动态调整分区（必要时）                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 4. CedarGraph 当前分区架构分析

### 4.1 现有架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                   CedarGraph 分区架构                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              PartitionRouter                        │   │
│  │   • 分区路由入口                                     │   │
│  │   • 支持一致性哈希                                   │   │
│  │   • 支持负载均衡策略                                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           PartitionRaftManager                      │   │
│  │   • Raft Group 管理                                 │   │
│  │   • Leader/Follower 管理                            │   │
│  │   • 分区状态管理                                     │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           PartitionAllocator                        │   │
│  │   • 分区分配策略                                     │   │
│  │   • 节点负载监控                                     │   │
│  │   • 动态重平衡                                       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 分区数量与映射

**当前设计**:
- 分区数量：可配置（推测 65536 或类似值）
- 分区到节点映射：一致性哈希 + 虚拟节点
- 每个物理节点：150 个虚拟节点

```cpp
// ConsistentHashRing 配置
struct HashRingConfig {
  uint32_t virtual_nodes_per_physical = 150;
  uint32_t replication_factor = 3;
};
```

### 4.3 分区分配策略

| 策略 | 说明 | 适用场景 |
|------|------|---------|
| **ROUND_ROBIN** | 轮询分配 | 均匀负载 |
| **CONSISTENT_HASH** | 一致性哈希 | 节点动态增减 |
| **LOAD_BALANCED** | 基于负载分数 | 异构硬件 |
| **CAPACITY_BASED** | 基于容量 | 数据密集型 |

### 4.4 GLTR 相关分区支持

CedarGraph 已有图感知分区基础：

```cpp
// GraphAwarePartitionStrategy（已有基础）
class GraphAwarePartitionStrategy : public PartitionStrategy {
  // 顶点 -> 分区映射
  std::unordered_map<uint64_t, PartitionID> vertex_partition_map_;
  
  // 顶点 -> 子图映射
  std::unordered_map<uint64_t, SubgraphID> vertex_subgraph_map_;
};
```

### 4.5 分区元数据

```cpp
struct PartitionMeta {
  PartitionID partition_id;
  NodeID primary_node;
  std::vector<NodeID> replicas;
  
  // 拓扑信息
  std::unordered_set<SubgraphID> subgraphs;
  std::atomic<uint64_t> vertex_count;
  std::atomic<uint64_t> edge_count;
  
  // 负载信息
  std::atomic<uint64_t> txn_rate;
  std::atomic<uint64_t> conflict_rate;
  
  // 热点检测
  uint64_t hot_key_count;
  double locality_score;
};
```

---

## 5. 关键发现与建议

### 5.1 关键发现

1. **NebulaGraph 策略简单有效**：静态哈希 + Edge-Cut 适合大多数 OLTP 场景
2. **PowerLyra 针对幂律图优化**：混合策略显著提升高度图性能
3. **Ginger 进一步优化局部性**：Fennel 启发式减少通信开销
4. **CedarGraph 已有良好基础**：分区框架完善，易于扩展新策略

### 5.2 对 CedarGraph 的建议

| 优先级 | 建议项 | 说明 |
|-------|--------|------|
| P0 | 支持图感知分区 | 基于顶点邻接关系优化分区 |
| P1 | 集成 METIS | 离线批量导入时使用高质量分割 |
| P2 | 混合分割策略 | 低度点 Edge-Cut + 高度点 Vertex-Cut |
| P3 | 时态感知分区 | 按时间维度优化时态图查询 |

---

## 6. 参考资料

1. [METIS - Serial Graph Partitioning](https://github.com/KarypisLab/METIS)
2. [PowerLyra: Differentiated Graph Computation and Partitioning on Skewed Graphs](https://ipads.se.sjtu.edu.cn/projects/powerlyra/)
3. [NebulaGraph Documentation](https://docs.nebula-graph.com.cn/)
4. [TeGraph: A Novel General-Purpose Temporal Graph Computing Engine](https://madsys.cs.tsinghua.edu.cn/publication/tegraph/)
5. [Ginger: Architecture- and Workload-Aware Graph (Re)Partitioning](https://d-scholarship.pitt.edu/33074/)

---

*报告完成 - 准备进入设计阶段*
