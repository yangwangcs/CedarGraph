# CedarGraph Multi-Raft 集成指南

## 概述

本指南介绍如何将 Multi-Raft 集成到 CedarGraph 存储系统中，实现分布式、高可用的图数据库。

## 架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    CedarGraph with Multi-Raft                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              MultiRaftStorageService                       │  │
│  │         (管理多个分区的 Raft 组)                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│       ┌──────────────────────┼──────────────────────┐           │
│       │                      │                      │           │
│       ▼                      ▼                      ▼           │
│  ┌─────────┐          ┌─────────┐          ┌─────────┐         │
│  │ Thread  │          │  Batch  │          │ Leader  │         │
│  │  Pool   │          │Heartbeat│          │Rebalance│         │
│  └────┬────┘          └────┬────┘          └────┬────┘         │
│       │                    │                    │              │
│       └────────────────────┼────────────────────┘              │
│                            ▼                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              RaftPartitionStorage (x N)                    │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │  ┌─────────────┐        ┌───────────────────────┐  │  │  │
│  │  │  │ StorageRaft │───────►│ CedarGraphStorage     │  │  │  │
│  │  │  │   Group     │ Apply  │ (LSM-Tree)            │  │  │  │
│  │  │  └─────────────┘        └───────────────────────┘  │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  │                       ...                                │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 单节点开发模式

```cpp
#include "cedar/dtx/storage/raft_storage_integration.h"

using namespace cedar::dtx::storage;

// 配置
MultiRaftStorageService::Config config;
config.node_id = 1;
config.data_root = "/data/cedar/node1";
config.default_partition_count = 16;
config.replication_factor = 3;

// 线程池配置
config.thread_pool_config.min_threads = 4;
config.thread_pool_config.max_threads = 16;

// 批量心跳配置
config.heartbeat_config.batch_interval = std::chrono::milliseconds(10);
config.heartbeat_config.max_batch_size = 100;

// 启动服务
MultiRaftStorageService service;
auto status = service.Initialize(config);
if (!status.ok()) {
  // 处理错误
}

// 创建空间
std::vector<NodeID> nodes = {1, 2, 3};
status = service.CreateSpace("social_graph", 16, nodes);

// 使用...

// 关闭
service.Shutdown();
```

### 2. 写入数据

```cpp
// 获取分区
auto* partition = service.GetPartition(0);
if (!partition) {
  // 分区不存在
}

// 检查是否为 Leader（只有 Leader 可以写入）
if (partition->IsLeader()) {
  // 构建 Key
  cedar::CedarKey key;
  key.SetVertexId(12345, 0);
  key.SetTimestamp(1000000);
  key.SetPartId(0);
  
  // 构建 Value
  cedar::Descriptor desc;
  desc.SetColumnId(0);
  
  // 写入（通过 Raft 复制）
  auto status = partition->Put(key, desc, cedar::Timestamp(1));
  if (status.ok()) {
    std::cout << "写入成功" << std::endl;
  }
} else {
  // 重定向到 Leader
  std::cout << "当前不是 Leader，请连接到 Leader 节点" << std::endl;
}
```

### 3. 读取数据

```cpp
// 本地读取（可能略微滞后，但性能最好）
auto result = partition->Get(key);
if (result.ok()) {
  auto desc = result.value();
  // 使用数据
}

// 线性一致性读取（保证读到最新数据，通过 Raft）
auto linear_result = partition->GetLinearizable(
    key, std::chrono::milliseconds(5000));
if (linear_result.ok()) {
  auto desc = linear_result.value();
  // 使用数据
}
```

## 集群部署

### 3 节点集群示例

```
Node-1                    Node-2                    Node-3
┌─────────┐              ┌─────────┐              ┌─────────┐
│Raft-P0  │              │Raft-P0  │              │Raft-P0  │
│ (Leader)│◄────────────►│(Follower)◄────────────►│(Follower)│
├─────────┤              ├─────────┤              ├─────────┤
│Raft-P1  │              │Raft-P1  │              │Raft-P1  │
│(Follower)◄────────────►│ (Leader)│◄────────────►│(Follower)│
├─────────┤              ├─────────┤              ├─────────┤
│Raft-P2  │              │Raft-P2  │              │Raft-P2  │
│(Follower)◄────────────►│(Follower)◄────────────►│ (Leader) │
└─────────┘              └─────────┘              └─────────┘
  127.0.0.1:7001         127.0.0.1:7002         127.0.0.1:7003
```

### 启动脚本

```bash
#!/bin/bash
# start_cluster.sh

# Node 1
./storaged --node_id=1 --addr=127.0.0.1:7001 --peers="1:127.0.0.1:7001,2:127.0.0.1:7002,3:127.0.0.1:7003" &

# Node 2
./storaged --node_id=2 --addr=127.0.0.1:7002 --peers="1:127.0.0.1:7001,2:127.0.0.1:7002,3:127.0.0.1:7003" &

# Node 3
./storaged --node_id=3 --addr=127.0.0.1:7003 --peers="1:127.0.0.1:7001,2:127.0.0.1:7002,3:127.0.0.1:7003" &

wait
```

## API 详解

### MultiRaftStorageService

| 方法 | 说明 |
|------|------|
| `Initialize()` | 初始化服务 |
| `CreateSpace()` | 创建图空间 |
| `DeleteSpace()` | 删除图空间 |
| `GetPartition()` | 获取指定分区 |
| `GetPartitionForKey()` | 根据 Key 获取分区 |
| `GetLocalPartitions()` | 获取本节点的所有分区 |
| `GetLeaderPartitions()` | 获取本节点是 Leader 的分区 |
| `GetStats()` | 获取统计信息 |
| `IsHealthy()` | 健康检查 |

### RaftPartitionStorage

| 方法 | 说明 |
|------|------|
| `IsLeader()` | 检查是否为 Leader |
| `Put()` | 写入数据（Raft 复制） |
| `Delete()` | 删除数据（Raft 复制） |
| `Get()` | 本地读取（可能滞后） |
| `GetLinearizable()` | 线性一致性读取 |
| `GetStats()` | 获取统计信息 |

## 配置调优

### 小集群 (3 节点, 10-100 分区)

```cpp
config.thread_pool_config = {
  .min_threads = 4,
  .max_threads = 8,
  .queue_capacity = 1000
};

config.heartbeat_config = {
  .batch_interval = 10ms,
  .max_batch_size = 50,
  .enabled = true
};

config.rebalancer_config = {
  .check_interval = 60s,
  .imbalance_threshold = 0.2,
  .auto_rebalance = true
};
```

### 大集群 (10+ 节点, 1000+ 分区)

```cpp
config.thread_pool_config = {
  .min_threads = 16,
  .max_threads = 32,
  .queue_capacity = 10000
};

config.heartbeat_config = {
  .batch_interval = 1ms,  // 更频繁
  .max_batch_size = 200,  // 更大批量
  .enabled = true
};

config.rebalancer_config = {
  .check_interval = 10s,   // 更频繁
  .imbalance_threshold = 0.1,  // 更严格
  .auto_rebalance = true
};
```

## 监控指标

### 关键指标

```cpp
auto stats = service.GetStats();

// 分区统计
std::cout << "Total partitions: " << stats.total_partitions << std::endl;
std::cout << "Leader partitions: " << stats.leader_partitions << std::endl;
std::cout << "Follower partitions: " << stats.follower_partitions << std::endl;

// 线程池
std::cout << "Thread pool threads: " << stats.thread_pool_stats.current_threads << std::endl;
std::cout << "Active threads: " << stats.thread_pool_stats.active_threads << std::endl;
std::cout << "Queue depth: " << stats.thread_pool_stats.total_tasks_submitted - 
                              stats.thread_pool_stats.total_tasks_executed << std::endl;

// 批量心跳
std::cout << "Avg batch size: " << stats.heartbeat_stats.avg_batch_size << std::endl;
std::cout << "Network bytes saved: " << stats.heartbeat_stats.network_bytes_saved << std::endl;

// Leader 均衡
std::cout << "Leader imbalance: " << stats.leader_imbalance_score << std::endl;
// < 0.2 表示良好，> 0.3 需要关注
```

### Prometheus 格式

```
#  CedarGraph Multi-Raft 指标

# 分区统计
raft_partitions_total 128
raft_partitions_leader 43
raft_partitions_follower 85

# 线程池
raft_thread_pool_threads 16
raft_thread_pool_active 4
raft_thread_pool_tasks_submitted 1000000
raft_thread_pool_tasks_executed 999998

# 批量心跳
raft_heartbeat_batches_sent 10000
raft_heartbeat_avg_size 15.5
raft_heartbeat_bytes_saved 15500000

# Leader 均衡
raft_leader_imbalance_score 0.15
```

## 故障处理

### 常见场景

#### 1. Leader 故障

```
时间线:
T+0s  - Leader 节点故障
T+1s  - Follower 检测心跳超时
T+2s  - Follower 发起选举
T+3s  - 新 Leader 选出
T+4s  - 服务恢复

处理: 自动，无需人工干预
```

#### 2. 网络分区

```
场景: 网络分区导致 Split Brain

处理:
1. 少数分区自动降级为 Follower
2. 多数分区继续提供服务
3. 网络恢复后自动同步

监控指标: raft_leader_imbalance_score
```

#### 3. 节点恢复

```cpp
// 节点重启后自动加入集群
MultiRaftStorageService service;
service.Initialize(config);

// 自动从其他节点同步缺失的数据
// 自动参与选举和服务
```

## 最佳实践

### 1. 分区数规划

```
推荐: 分区数 = 节点数 × 10

例如:
- 3 节点: 30 分区
- 10 节点: 100 分区
- 100 节点: 1000 分区
```

### 2. 写入策略

```cpp
// 写入到 Leader
auto* partition = service.GetPartition(pid);
if (partition->IsLeader()) {
  partition->Put(key, value, txn);
} else {
  // 重定向或失败
  return Status::NotLeader("Please connect to leader");
}
```

### 3. 读取策略

```cpp
// 根据一致性要求选择读取方式

// 最终一致性 (最快)
auto result = partition->Get(key);

// 强一致性 (稍慢)
auto result = partition->GetLinearizable(key, timeout);
```

### 4. 监控告警

```yaml
# 建议告警规则
alerts:
  - name: LeaderNotElected
    condition: raft_partitions_leader == 0
    duration: 30s
    severity: critical
    
  - name: HighLeaderImbalance
    condition: raft_leader_imbalance_score > 0.3
    duration: 5m
    severity: warning
    
  - name: ThreadPoolOverload
    condition: raft_thread_pool_queue_depth > 1000
    duration: 1m
    severity: warning
```

## 示例程序

完整示例代码位于：`examples/multi_raft_example.cc`

编译运行：
```bash
cd build
make multi_raft_example
./examples/multi_raft_example
```

## 故障排查

### 查看日志

```bash
# 查看节点日志
tail -f /data/cedar/node1/logs/storaged.log

# 查看 Raft 状态
curl http://localhost:9090/metrics | grep raft_
```

### 诊断命令

```cpp
// 获取健康状态
if (!service.IsHealthy()) {
  auto issues = service.GetHealthIssues();
  for (const auto& issue : issues) {
    std::cout << "Issue: " << issue << std::endl;
  }
}

// 获取分区详情
auto partitions = service.GetLocalPartitions();
for (auto* p : partitions) {
  auto stats = p->GetStats();
  std::cout << "Partition " << stats.current_role 
            << " applied: " << stats.applied_entries << std::endl;
}
```

## 参考

- `raft_storage_integration.h` - 集成头文件
- `raft_replication.h` - Raft 实现
- `multi_raft_optimization.h` - 优化组件
- `examples/multi_raft_example.cc` - 示例代码
