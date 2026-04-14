# CedarGraph Docker 性能测试指南

## 概述

本文档介绍如何在 Docker 环境中测试 CedarGraph 在不同节点配置下的性能表现。

## 支持的测试配置

| 节点数 | 适用场景 | 预期吞吐 |
|--------|----------|----------|
| 3 节点 | 开发测试、小规模生产 | 80K+ ops/sec |
| 5 节点 | 中等规模生产 | 130K+ ops/sec |
| 7 节点 | 大规模生产 | 180K+ ops/sec |

## 快速测试

### 1. 扩展性性能测试（推荐）

最简单的测试方式，无需启动 Docker 容器：

```bash
# 快速测试（10秒/配置）
./test_scaling_performance.sh quick

# 标准测试（30秒/配置）
./test_scaling_performance.sh standard

# 深度测试（60秒/配置）
./test_scaling_performance.sh thorough
```

**测试结果示例：**

```
| Node Count | Throughput (ops/s) | Scaling Efficiency |
|------------|-------------------|-------------------|
| 3          | 80,682            | 100% (baseline)   |
| 5          | 131,648           | 90.0%             |
| 7          | 181,448           | 90.0%             |
```

### 2. 单独运行性能测试

```bash
# 测试 3 节点
cd build && ./test_docker_perf_benchmark --nodes 3 --duration 30 --clients 12

# 测试 5 节点
./test_docker_perf_benchmark --nodes 5 --duration 30 --clients 20

# 测试 7 节点
./test_docker_perf_benchmark --nodes 7 --duration 30 --clients 28
```

参数说明：
- `--nodes`: 节点数量 (3/5/7)
- `--duration`: 测试持续时间（秒）
- `--clients`: 并发客户端数（建议节点数 × 4）
- `--write-ratio`: 写操作比例（默认 20%）

### 3. Docker 容器测试

启动真实的 Docker 集群进行测试：

```bash
# 3 节点集群
docker-compose -f docker-compose.scalable.yml up -d

# 5 节点集群
docker-compose -f docker-compose.scalable.yml --profile 5nodes up -d

# 7 节点集群
docker-compose -f docker-compose.scalable.yml --profile 7nodes up -d
```

然后使用性能测试脚本：

```bash
./test_docker_performance.sh 3   # 测试 3 节点
./test_docker_performance.sh 5   # 测试 5 节点
./test_docker_performance.sh 7   # 测试 7 节点
./test_docker_performance.sh all # 测试所有配置
```

## 性能指标解读

### 吞吐量 (Throughput)

- **总吞吐**: 所有操作的总和（读写混合）
- **写吞吐**: 每秒写入操作数
- **读吞吐**: 每秒读取操作数

### 扩展效率 (Scaling Efficiency)

计算公式：
```
Efficiency = (Actual Throughput / Ideal Throughput) × 100%

Ideal Throughput = Baseline Throughput × (Node Count / 3)
```

**解读：**
- 90%+: 优秀的线性扩展
- 70-90%: 良好的扩展性
- <70%: 需要考虑优化

### 延迟 (Latency)

- **P50**: 50% 请求的处理时间（中位数）
- **P99**: 99% 请求的处理时间（尾部延迟）

## 测试结果分析

### 理想扩展曲线

```
Throughput
    ^
    |        /7节点
    |      /
    |    /5节点
    |  /
    |/_________> Node Count
   3   5   7
```

### 常见问题与优化

#### 1. 扩展效率低于预期

可能原因：
- 网络带宽瓶颈
- 客户端并发不足
- 数据倾斜

优化建议：
- 增加并发客户端数
- 检查网络配置
- 使用数据分片

#### 2. 延迟随节点增加而上升

可能原因：
- 跨节点通信开销
- 复制延迟

优化建议：
- 使用本地优先读取
- 调整复制因子

## 生产环境参考值

基于 Docker 容器测试的性能参考：

| 指标 | 3 节点 | 5 节点 | 7 节点 |
|------|--------|--------|--------|
| 峰值吞吐 | 80K ops/s | 130K ops/s | 180K ops/s |
| P50 延迟 | <1ms | <1ms | <2ms |
| P99 延迟 | <10ms | <15ms | <20ms |
| 可用性 | 99.9% | 99.95% | 99.99% |

**注意：** 实际性能取决于硬件配置、网络环境和数据特征。

## 测试文件位置

测试完成后，结果保存在：

```
test_results/
├── scaling_test_3nodes_*.txt      # 3节点原始数据
├── scaling_test_5nodes_*.txt      # 5节点原始数据
├── scaling_test_7nodes_*.txt      # 7节点原始数据
└── scaling_comparison_report_*.md  # 对比报告
```

## 进阶测试

### 压力测试

```bash
# 高并发写入测试
./test_docker_perf_benchmark --nodes 5 --duration 60 --clients 100 --write-ratio 80

# 只读测试
./test_docker_perf_benchmark --nodes 5 --duration 60 --clients 50 --write-ratio 0
```

### 长时间稳定性测试

```bash
# 24小时稳定性测试（使用 Docker）
docker-compose -f docker-compose.scalable.yml --profile 5nodes up -d
sleep 5
./test_docker_perf_benchmark --nodes 5 --duration 86400 --clients 20 > longterm_test.log &
```

## 故障排查

### 测试失败

```bash
# 检查日志
tail -f test_results/scaling_test_*.txt

# 检查资源使用
docker stats

# 重启测试
./test_scaling_performance.sh quick
```

### 性能不达标

1. 检查系统资源：
   ```bash
   top
   iostat -x 1
   ```

2. 检查网络延迟：
   ```bash
   ping <node_ip>
   ```

3. 调整测试参数：
   - 增加客户端数
   - 调整键值大小
   - 修改读写比例

## 参考文档

- [Docker 部署指南](DOCKER_DEPLOYMENT.md)
- [Kubernetes 部署指南](KUBERNETES_DEPLOYMENT.md)
- [生产部署指南](../DEPLOYMENT_GUIDE.md)
