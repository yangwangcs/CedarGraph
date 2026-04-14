# CedarGraph Multi-Raft 架构设计

## 概述

CedarGraph 采用 **Multi-Raft** 架构，每个分区（Partition）拥有独立的 Raft 组，实现水平扩展和负载均衡。

## 什么是 Multi-Raft？

### 传统单 Raft vs Multi-Raft

```
┌─────────────────────────────────────────────────────────────────┐
│                    传统单 Raft 架构                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────────────────────────────────────────────┐      │
│   │                 Single Raft Group                   │      │
│   │                                                     │      │
│   │  ┌─────────┐      ┌─────────┐      ┌─────────┐     │      │
│   │  │ Node-1  │◄────►│ Node-2  │◄────►│ Node-3  │     │      │
│   │  │ Leader  │      │Follower │      │Follower │     │      │
│   │  │ P0-P999 │      │ P0-P999 │      │ P0-P999 │     │      │
│   │  └─────────┘      └─────────┘      └─────────┘     │      │
│   │                                                     │      │
│   └─────────────────────────────────────────────────────┘      │
│                                                                 │
│   问题:                                                          │
│   • 单点瓶颈 - Leader 处理所有写入                                │
│   • 扩展性差 - 无法水平扩展                                       │
│   • 故障域大 - Leader 故障影响所有分区                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    Multi-Raft 架构                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Node-1                    Node-2                    Node-3   │
│   ┌─────────┐              ┌─────────┐              ┌─────────┐│
│   │Raft-P0  │              │Raft-P0  │              │Raft-P0  ││
│   │ (Leader)│              │(Follower)              │(Follower)│
│   ├─────────┤              ├─────────┤              ├─────────┤│
│   │Raft-P1  │              │Raft-P1  │              │Raft-P1  ││
│   │(Follower)              │ (Leader)│              │(Follower)│
│   ├─────────┤              ├─────────┤              ├─────────┤│
│   │Raft-P2  │              │Raft-P2  │              │Raft-P2  ││
│   │(Follower)              │(Follower)              │ (Leader)│
│   └─────────┘              └─────────┘              └─────────┘│
│                                                                 │
│   优势:                                                          │
│   • 水平扩展 - 增加分区即可扩展容量                                │
│   • 负载均衡 - Leader 分布在不同节点                               │
│   • 故障隔离 - 单分区故障不影响其他分区                             │
│   • 就近访问 - 客户端可连接到最近的 Leader                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. StorageRaftGroup (单个 Raft 组)

每个分区拥有独立的 Raft 组：
- 独立的 Leader 选举
- 独立的日志复制
- 独立的状态机

```cpp
class StorageRaftGroup {
  PartitionID partition_id;           // 分区标识
  ReplicaState state;                  // Leader/Follower/Candidate
  std::vector<LogEntry> log_;          // 独立日志
  std::unordered_map<NodeID, ReplicaProgress> progress_;  // 复制进度
};
```

### 2. RaftStorageManager (Multi-Raft 管理器)

管理集群中所有 Raft 组：

```cpp
class RaftStorageManager {
  std::unordered_map<PartitionID, std::unique_ptr<StorageRaftGroup>> groups_;
  
  StorageRaftGroup* GetRaftGroup(PartitionID pid);
  StatusOr<StorageRaftGroup*> CreateRaftGroup(...);
};
```

### 3. 关键优化

#### 3.1 共享线程池 (RaftThreadPool)

**问题**: 每个 Raft 组一个线程，1000 个分区 = 1000 个线程

**解决方案**: 使用优先级任务队列的共享线程池

```cpp
class RaftThreadPool {
  // 优先级: Critical > High > Normal > Low
  // Critical: Leader 选举
  // High:     日志复制
  // Normal:   心跳
  // Low:      快照、压缩
  
  std::priority_queue<Task> task_queue_;
  std::vector<std::thread> workers_;
};
```

**效果**: 
- 4-32 个线程处理 1000+ 个 Raft 组
- 动态扩缩容
- 任务优先级保证关键操作

#### 3.2 批量心跳 (BatchHeartbeatManager)

**问题**: 1000 个分区 × 3 个副本 × 每秒 10 次心跳 = 30,000 包/秒

**解决方案**: 批量合并心跳

```cpp
class BatchHeartbeatManager {
  // 10ms 批量窗口
  // 将发往同一节点的多个心跳合并
  
  struct BatchedHeartbeat {
    NodeID to_node;
    std::vector<HeartbeatEntry> entries;  // 多个分区的心跳
  };
};
```

**效果**:
- 网络包减少 80-90%
- 延迟增加 < 10ms (可接受)

#### 3.3 Leader 均衡 (LeaderRebalancer)

**问题**: 所有 Leader 可能在同一节点，造成热点

**解决方案**: 自动 Leader 迁移

```cpp
class LeaderRebalancer {
  // 监控 Leader 分布
  // 当 imbalance > 20% 时触发迁移
  // 将 Leader 从过载节点迁移到轻载节点
  
  double CalculateImbalanceScore();
  std::vector<RebalanceAction> GenerateRebalancePlan();
};
```

**效果**:
- CPU/网络负载均衡
- 避免单节点瓶颈

## 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    CedarGraph Multi-Raft                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                  OptimizedMultiRaftManager               │   │
│  │                      (管理所有 Raft 组)                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│       ┌──────────────────────┼──────────────────────┐          │
│       │                      │                      │          │
│       ▼                      ▼                      ▼          │
│  ┌─────────┐          ┌─────────┐          ┌─────────┐         │
│  │ Thread  │          │  Batch  │          │ Leader  │         │
│  │  Pool   │          │Heartbeat│          │Rebalance│         │
│  │         │          │         │          │         │         │
│  │ 4-32    │          │Merge    │          │Distribute│        │
│  │ threads │          │heartbeats│         │leaders   │        │
│  └────┬────┘          └────┬────┘          └────┬────┘         │
│       │                    │                    │              │
│       └────────────────────┼────────────────────┘              │
│                            ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Raft Groups (Per Partition)                 │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐    │   │
│  │  │Raft-P0  │  │Raft-P1  │  │Raft-P2  │  │Raft-P3  │ ...│   │
│  │  │Leader   │  │Follower │  │Follower │  │Leader   │    │   │
│  │  │Node-A   │  │Node-A   │  │Node-B   │  │Node-C   │    │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Storage Layer                       │   │
│  │                   (CedarGraphStorage)                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 性能对比

| 指标 | 单 Raft | Multi-Raft (无优化) | Multi-Raft (优化后) |
|------|---------|---------------------|---------------------|
| **分区数** | 1 | 1000 | 1000 |
| **线程数** | 3 | 1000 | 8-32 |
| **网络包/秒** | 30 | 30,000 | 3,000 |
| **Leader 分布** | 固定 | 随机 | 自动均衡 |
| **扩展性** | 垂直扩展 | 水平扩展 | 水平扩展 |
| **故障影响** | 全集群 | 单分区 | 单分区 |

## 配置建议

### 小集群 (3 节点, 10-100 分区)
```cpp
RaftThreadPool::Config{
  .min_threads = 4,
  .max_threads = 8,
  .queue_capacity = 1000
};

BatchHeartbeatManager::Config{
  .batch_interval = 10ms,
  .max_batch_size = 50
};
```

### 中集群 (5-10 节点, 100-1000 分区)
```cpp
RaftThreadPool::Config{
  .min_threads = 8,
  .max_threads = 16,
  .queue_capacity = 5000
};

BatchHeartbeatManager::Config{
  .batch_interval = 5ms,
  .max_batch_size = 100
};
```

### 大集群 (10+ 节点, 1000+ 分区)
```cpp
RaftThreadPool::Config{
  .min_threads = 16,
  .max_threads = 32,
  .queue_capacity = 10000
};

BatchHeartbeatManager::Config{
  .batch_interval = 1ms,
  .max_batch_size = 200
};

LeaderRebalancer::Config{
  .check_interval = 10s,
  .imbalance_threshold = 0.1  // 更激进的均衡
};
```

## 最佳实践

### 1. 分区数规划
- 每个节点 10-100 个分区
- 分区数建议是节点数的倍数（便于均衡）
- 考虑未来扩容，预留 2-3 倍分区

### 2. Leader 均衡策略
- 自动均衡开启（建议）
- 冷却期 5 分钟（避免频繁切换）
- 不均衡阈值 20%

### 3. 监控指标
```
# 关键指标
raft_thread_pool_queue_depth      < 100
raft_thread_pool_wait_time_ms     < 10
raft_batch_heartbeat_avg_size     > 10
raft_leader_imbalance_score       < 0.2
raft_leader_changes_per_min       < 5
```

### 4. 故障处理
- 单分区 Leader 故障：自动选举（< 1秒）
- 节点故障：其他节点接管 Leader（自动）
- 网络分区：最小分区自动降级为 Follower

## 实现状态

| 组件 | 状态 | 文件 |
|------|------|------|
| StorageRaftGroup | ✅ 已实现 | `raft_replication.h/cc` |
| RaftStorageManager | ✅ 已实现 | `raft_replication.h/cc` |
| RaftThreadPool | ✅ 已实现 | `multi_raft_optimization.h/cc` |
| BatchHeartbeatManager | ✅ 已实现 | `multi_raft_optimization.h/cc` |
| LeaderRebalancer | ✅ 已实现 | `multi_raft_optimization.h/cc` |
| OptimizedMultiRaftManager | ✅ 已实现 | `multi_raft_optimization.h/cc` |

## 参考

- TiKV Multi-Raft: https://tikv.org/deep-dive/scalability/multi-raft/
- CockroachDB Range-based Replication
- Apache Kafka KRaft (similar concept)
