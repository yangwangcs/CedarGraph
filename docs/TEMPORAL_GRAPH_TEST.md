# CedarGraph 时态图性能测试指南

## 概述

本文档介绍如何在 Docker 环境下测试 CedarGraph 的时态图性能，包括：

- 时态点查询（Temporal Point Query）
- 时态范围查询（Temporal Range Query）
- 图分析算法（Graph Analytics）
- 时态写入（Temporal Write）

## 时态图数据模型

### 时态顶点 (Temporal Vertex)

```cpp
struct TemporalVertex {
  uint64_t vertex_id;       // 顶点ID
  uint64_t valid_from;      // 开始时间戳
  uint64_t valid_until;     // 结束时间戳 (UINT64_MAX = 永久有效)
  string properties;        // JSON 属性
};
```

### 时态边 (Temporal Edge)

```cpp
struct TemporalEdge {
  uint64_t edge_id;         // 边ID
  uint64_t from_vertex;     // 起点
  uint64_t to_vertex;       // 终点
  uint64_t valid_from;      // 开始时间戳
  uint64_t valid_until;     // 结束时间戳
  string edge_type;         // 边类型
  string properties;        // JSON 属性
};
```

## 快速开始

### 1. 本地快速测试（无需 Docker）

```bash
# 测试时态点查询性能
cd build && ./test_temporal_graph_perf --nodes 3 --duration 30 --test-point

# 测试时态范围查询性能
./test_temporal_graph_perf --nodes 5 --duration 30 --test-range

# 测试图分析算法性能
./test_temporal_graph_perf --nodes 7 --duration 30 --test-analytics

# 混合负载测试
./test_temporal_graph_perf --nodes 5 --duration 60 \
  --point-ratio 30 --range-ratio 30 --analytics-ratio 20 --write-ratio 20
```

### 2. Docker 多节点测试

```bash
# 一键测试 3 节点时态图性能
./test_temporal_docker.sh 3

# 一键测试 5 节点时态图性能
./test_temporal_docker.sh 5

# 一键测试 7 节点时态图性能
./test_temporal_docker.sh 7

# 测试所有配置（3/5/7 节点）
./test_temporal_docker.sh all
```

### 3. 查看可视化结果

```bash
# 查看 ASCII 图表和对比分析
python3 scripts/visualize_temporal_results.py
```

## 测试类型详解

### 1. 时态点查询 (Temporal Point Query)

**定义**: 查询特定顶点或边在特定时间戳的状态

**性能特征**:
- 延迟: 1-3ms (P50)
- 吞吐: 400-500 qps (单节点)
- 适用场景: 快照查询、时间旅行查询

**测试命令**:
```bash
./test_temporal_graph_perf --nodes 3 --test-point --point-ratio 100
```

### 2. 时态范围查询 (Temporal Range Query)

**定义**: 查询时间范围内的所有图数据变化

**性能特征**:
- 延迟: 5-10ms (P50)
- 吞吐: 200-300 qps (单节点)
- 适用场景: 历史分析、变化追踪

**测试命令**:
```bash
./test_temporal_graph_perf --nodes 3 --test-range --range-ratio 100
```

### 3. 图分析算法 (Graph Analytics)

支持的算法:

| 算法 | 延迟 | 吞吐 | 适用场景 |
|------|------|------|----------|
| PageRank | 20-50ms | 20-30 qps | 影响力分析 |
| 最短路径 | 10-30ms | 30-50 qps | 路由规划 |
| 连通分量 | 30-80ms | 10-20 qps | 社区发现 |
| 时态模式匹配 | 20-60ms | 15-25 qps | 时序模式分析 |

**测试命令**:
```bash
./test_temporal_graph_perf --nodes 5 --test-analytics --analytics-ratio 100
```

### 4. 时态写入 (Temporal Write)

**定义**: 插入或更新带时间戳的顶点和边

**性能特征**:
- 延迟: 1-3ms (P50)
- 吞吐: 300-400 ops/s (单节点)
- 特点: 支持时间版本控制

**测试命令**:
```bash
./test_temporal_graph_perf --nodes 3 --test-write --write-ratio 100
```

## 性能参考值

### 3 节点集群

| 查询类型 | 吞吐 (qps) | P50 延迟 | P99 延迟 |
|----------|-----------|----------|----------|
| 时态点查询 | 440 | 1.5ms | 6ms |
| 时态范围查询 | 434 | 7ms | 14ms |
| 图分析 | 302 | 33ms | 80ms |
| 时态写入 | 304 | 2.4ms | 7ms |
| **混合负载** | **1,480** | - | - |

### 扩展性预期

| 节点数 | 混合负载吞吐 | 扩展效率 |
|--------|-------------|----------|
| 3 | 1,480 qps | 100% |
| 5 | 2,400 qps | 97% |
| 7 | 3,300 qps | 95% |

## 测试参数说明

### 命令行参数

```bash
./test_temporal_graph_perf [options]

--nodes <n>              # 节点数量 (3/5/7)
--duration <sec>         # 测试持续时间
--clients <n>            # 并发客户端数
--vertices <n>           # 图顶点数量
--edges <n>              # 图边数量
--point-ratio <%>        # 点查询比例
--range-ratio <%>        # 范围查询比例
--analytics-ratio <%>    # 分析算法比例
--write-ratio <%>        # 写入比例
--test-point             # 只测试点查询
--test-range             # 只测试范围查询
--test-analytics         # 只测试分析算法
--test-write             # 只测试写入
--output <file>          # 输出结果到文件
```

### 测试配置示例

**场景1: 社交网络分析**
```bash
./test_temporal_graph_perf --nodes 5 --duration 60 \
  --vertices 1000000 --edges 5000000 \
  --point-ratio 60 --range-ratio 20 --analytics-ratio 20
```

**场景2: 金融交易追踪**
```bash
./test_temporal_graph_perf --nodes 7 --duration 120 \
  --point-ratio 20 --range-ratio 60 --write-ratio 20
```

**场景3: 知识图谱分析**
```bash
./test_temporal_graph_perf --nodes 5 --duration 300 \
  --vertices 500000 --edges 2000000 \
  --analytics-ratio 80
```

## 结果解读

### 示例输出

```
Temporal Graph Performance Results

Duration: 10.06 seconds

Query Statistics:
  Total Queries: 14895
  Point Queries: 4428
  Range Queries: 4368
  Analytics:     3037
  Write Ops:     3062
  Failed:        31

Throughput:
  Total:         1480.9 queries/sec
  Point Query:   440.2 queries/sec
  Range Query:   434.3 queries/sec
  Analytics:     302.0 queries/sec
  Write:         304.4 ops/sec

Latency Statistics (microseconds):
  Point Query:
    P50: 1565 µs
    P99: 5879 µs
    Avg: 1686.4 µs
  ...
```

### 关键指标

| 指标 | 说明 | 健康值 |
|------|------|--------|
| Total Throughput | 总查询吞吐量 | >1000 qps (3节点) |
| Point Query Latency | 点查询延迟 | P50 < 5ms |
| Range Query Latency | 范围查询延迟 | P50 < 15ms |
| Analytics Latency | 分析算法延迟 | P50 < 100ms |
| Failed Rate | 失败率 | < 1% |

## 故障排查

### 测试失败

```bash
# 检查 Docker 容器状态
docker-compose -f docker-compose.scalable.yml ps

# 查看容器日志
docker logs cedar-storaged1

# 检查网络连通性
docker exec cedar-storaged1 ping storaged2
```

### 性能不达标

1. **检查资源限制**:
   ```bash
   docker stats
   ```

2. **调整并发数**:
   ```bash
   ./test_temporal_graph_perf --clients 32  # 增加并发
   ```

3. **减少图规模**（如果内存不足）:
   ```bash
   ./test_temporal_graph_perf --vertices 50000 --edges 200000
   ```

## 高级用法

### 自定义时间范围

```cpp
// 修改代码中的时间范围
TemporalBenchmarkConfig config;
config.time_range_seconds = 604800;  // 一周
```

### 自定义分析算法

```cpp
// 在 temporal_graph_benchmark.cc 中添加新的算法
Status RunCustomAlgorithm(uint64_t& latency_us) {
  // 实现自定义算法
}
```

## 相关文件

| 文件 | 说明 |
|------|------|
| `test_temporal_graph_perf.cpp` | 主测试程序 |
| `src/dtx/temporal_graph_benchmark.cc` | 测试框架实现 |
| `test_temporal_docker.sh` | Docker 测试脚本 |
| `scripts/visualize_temporal_results.py` | 结果可视化 |

## 参考文档

- [Docker 部署指南](DOCKER_DEPLOYMENT.md)
- [性能测试指南](DOCKER_PERFORMANCE_TEST.md)
- [系统就绪报告](../SYSTEM_READINESS.md)
