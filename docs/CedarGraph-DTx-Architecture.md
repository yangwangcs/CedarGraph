# CedarGraph-DTx 分布式架构详解

## 1. 架构概览

CedarGraph-DTx 采用**分层分区 + 多副本**的分布式架构：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CedarGraph-DTx 集群架构                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      客户端层 (Client Layer)                         │   │
│  │         Driver / gRPC API / Cypher API / Batch API                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                   协调器层 (Coordinator Layer)                       │   │
│  │     ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐   │   │
│  │     │   GLTR      │  │   TW-CD     │  │   LND-OCC               │   │   │
│  │     │   路由      │  │   冲突检测  │  │   分层提交              │   │   │
│  │     └─────────────┘  └─────────────┘  └─────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    存储节点层 (Storage Node Layer)                   │   │
│  │                                                                      │   │
│  │    Node 0          Node 1          Node 2          Node N            │   │
│  │   ┌───────┐       ┌───────┐       ┌───────┐       ┌───────┐         │   │
│  │   │Part 0 │◄─────►│Part 1 │◄─────►│Part 2 │◄─────►│Part N │         │   │
│  │   │Part 4 │       │Part 5 │       │Part 6 │       │...   │         │   │
│  │   └───────┘       └───────┘       └───────┘       └───────┘         │   │
│  │      │                │               │                │            │   │
│  │      └────────────────┴───────────────┴────────────────┘            │   │
│  │                              Raft Group                              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 核心概念

### 2.1 三层拓扑结构

| 层级 | 概念 | 数量级 | 说明 |
|------|------|--------|------|
| **Cluster** | 集群 | 1 | 完整的 CedarGraph 实例 |
| **Node** | 节点 | 10~100 | 物理/虚拟机，独立进程 |
| **Partition** | 分区 | 256~65,536 | 数据分片，逻辑单元 |
| **Shard** | 分片 | = Partition | 与 Partition 同义 |
| **Subgraph** | 子图 | 动态 | 图局部性单元 |

### 2.2 分区 (Partition/Shard)

**Partition** 是 CedarGraph-DTx 的核心数据分片单位：

- **ID 范围**: 16-bit (`PartitionID = uint16_t`)
- **最大数量**: 65,536 个分区
- **推荐配置**: 256~1024 个分区（根据集群规模）
- **数据分布**: 通过 CedarKey 的 `part_id` 字段路由

```cpp
// CedarKey 中的分区信息（已内置于 reserved 字段）
CedarKey key(entity_id, timestamp, ...);
key.SetPartId(partition_id);  // 设置分区

PartitionID pid = key.part_id();  // 获取分区
```

### 2.3 节点 (Node)

**Node** 是运行 CedarGraph 进程的物理或虚拟服务器：

- **ID 范围**: 32-bit (`NodeID = uint32_t`)
- **典型配置**: 
  - 小规模: 3~5 节点
  - 中规模: 10~20 节点
  - 大规模: 50+ 节点
- **每个节点**: 承载多个 Partition 的主副本或从副本

```cpp
// 节点与分区的关系
struct PartitionMeta {
    PartitionID partition_id;
    NodeID primary_node;           // 主节点（负责读写）
    std::vector<NodeID> replicas;  // 副本节点（提供读/容错）
};
```

---

## 3. 数据分布策略

### 3.1 分区策略

CedarGraph 支持三种分区策略：

| 策略 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| **HashPartition** | 通用场景 | 负载均衡 | 范围查询跨分区 |
| **RangePartition** | 时序查询 | 范围查询高效 | 可能热点 |
| **GraphAwarePartition** | 图遍历 | 局部性最优 | 需要预分析 |

```cpp
// 默认：哈希分区
PartitionManager mgr(config);
mgr.Initialize(256, std::make_unique<HashPartitionStrategy>());

// 图感知分区（GLTR 推荐）
mgr.Initialize(256, std::make_unique<GraphAwarePartitionStrategy>());
```

### 3.2 副本策略

每个 Partition 可以配置多个副本：

```
Partition 0:
├── Primary: Node 0  (读写)
├── Replica: Node 1  (读 + 故障转移)
└── Replica: Node 2  (读 + 故障转移)

Partition 1:
├── Primary: Node 1
├── Replica: Node 2
└── Replica: Node 0

... 轮询分布实现负载均衡
```

**副本角色**:
- **Primary**: 处理所有写请求，处理部分读请求
- **Replica**: 处理读请求，Primary 故障时提升

---

## 4. 事务路由 (GLTR)

### 4.1 单层事务 (Layer 1)

90%+ 的事务只涉及单个 Partition，**零 RPC 开销**:

```
Client ──► Node 0 (Primary of Partition 0)
              │
              └── 本地提交（无需协调）
```

### 4.2 跨分区事务 (Layer 2/3)

涉及多个 Partitions 的事务需要协调：

```
Client ──► Coordinator (任意节点)
              │
              ├──► Node 0 (Partition 0 Primary)
              ├──► Node 1 (Partition 1 Primary)
              └──► Node 2 (Partition 2 Primary)
                          │
                          └── 2PC 提交
```

### 4.3 局部性优化

通过 **Subgraph** 概念优化图遍历：

```
Subgraph A:        Subgraph B:
┌─────────┐        ┌─────────┐
│ Part 0  │◄──────►│ Part 1  │
│ Part 2  │        │ Part 3  │
└─────────┘        └─────────┘
   
   跨 Subgraph 边 = 跨分区边（需要协调）
   Subgraph 内边 = 局部边（无需协调）
```

---

## 5. 通信架构

### 5.1 节点间通信

```
┌─────────────────────────────────────────┐
│           gRPC Service                  │
├─────────────────────────────────────────┤
│  DTxService                             │
│   ├── Prepare(txn)     // 2PC Phase 1   │
│   ├── Commit(txn)      // 2PC Phase 2   │
│   └── Abort(txn)                        │
│                                         │
│  ValidationService                      │
│   └── Validate(read_set)                │
│                                         │
│  BookmarkService                        │
│   └── Propagate(bookmark)               │
└─────────────────────────────────────────┘
```

### 5.2 共识协议 (Raft)

分区内部使用 Raft 实现一致性：

```
Partition 0 Raft Group:
┌─────────┐
│ Leader  │◄── Node 0 (Primary)
├─────────┤
│ Follower│◄── Node 1
├─────────┤
│ Follower│◄── Node 2
└─────────┘
```

**职责分离**:
- **Raft**: 负责日志复制（分区内部）
- **2PC**: 负责跨分区事务（分布式事务）

---

## 6. 典型部署拓扑

### 6.1 最小部署 (3 节点)

```
                    ┌─────────┐
                    │ Client  │
                    └────┬────┘
                         │
           ┌─────────────┼─────────────┐
           │             │             │
        ┌──┴──┐      ┌──┴──┐      ┌──┴──┐
        │Node0│◄────►│Node1│◄────►│Node2│
        │ P0,P3│      │ P1,P4│      │ P2,P5│
        └──┬──┘      └──┬──┘      └──┬──┘
           │             │             │
           └─────────────┴─────────────┘
                  Raft + 2PC

- 6 Partitions (P0~P5)
- 每个 Partition 3 副本
- 可容忍 1 节点故障
```

### 6.2 生产部署 (10 节点)

```
- 256 Partitions
- 每个 Partition 3 副本
- 每个节点承载 ~77 个 Partition
- 可容忍 2 节点同时故障
```

### 6.3 大规模部署 (50+ 节点)

```
- 4096 Partitions
- 每个 Partition 3 副本
- 支持 100K+ TPS
- 多数据中心部署
```

---

## 7. 配置示例

### 7.1 单节点开发环境

```cpp
DTxConfig config;
config.node_id = 0;
config.node_list = {"127.0.0.1:50051"};
config.num_partitions = 4;  // 最小分区数
config.replication_factor = 1;  // 无副本
```

### 7.2 3 节点生产环境

```cpp
// Node 0 配置
DTxConfig config;
config.node_id = 0;
config.node_list = {
    "10.0.0.1:50051",  // Node 0
    "10.0.0.2:50051",  // Node 1
    "10.0.0.3:50051"   // Node 2
};
config.num_partitions = 256;
config.replication_factor = 3;
```

### 7.3 大规模集群

```cpp
DTxConfig config;
config.node_id = node_idx;
config.node_list = LoadNodeListFromConsul();  // 服务发现
config.num_partitions = 4096;
config.replication_factor = 3;
config.enable_cross_dc = true;
config.dc_list = {"dc1", "dc2", "dc3"};
```

---

## 8. 性能特征

### 8.1 横向扩展

| 节点数 | Partitions | 预期吞吐 | 效率 |
|--------|-----------|----------|------|
| 1 | 64 | 500K TPS | 100% |
| 3 | 256 | 1.2M TPS | 80% |
| 10 | 512 | 3.5M TPS | 70% |
| 50 | 4096 | 12M TPS | 48% |

### 8.2 事务延迟

| 事务类型 | 节点数 | 延迟 (P99) |
|----------|--------|-----------|
| 单分区 (Layer 1) | 任意 | < 1ms |
| 同时序跨分区 (Layer 2) | 3 | 2-5ms |
| 跨时序跨分区 (Layer 3) | 3 | 10-20ms |

---

## 9. 故障处理

### 9.1 节点故障

```
Before:              After Node 1 fails:
P0: N0(Primary)      P0: N0(Primary) - unchanged
    N1(Replica)          N2(Replica) - promoted
    N2(Replica)
```

**自动恢复流程**:
1. Raft 检测到 Leader 失效
2. 新 Leader 选举（< 1s）
3. 客户端重路由
4. 后台数据修复（如有需要）

### 9.2 网络分区

```
DC 1 (多数派)        DC 2 (少数派)
┌─────────┐         ┌─────────┐
│ N0, N1  │◄────X──►│   N2    │
│ (Active)│         │(Stale)  │
└─────────┘         └─────────┘

- 多数派继续服务
- 少数派进入只读模式
- 网络恢复后自动同步
```

---

## 10. 总结

CedarGraph-DTx 的分布式架构特点：

1. **分层分区**: Partition → Node → Cluster 三层拓扑
2. **图局部性感知**: GLTR 优化使 90%+ 事务无需协调
3. **灵活配置**: 支持 1~N 节点，256~65K 分区
4. **高可用**: Raft 共识 + 多副本，自动故障转移
5. **线性扩展**: 支持 50+ 节点，100K+ TPS

**推荐配置**:
- **开发环境**: 1 节点，64 分区
- **测试环境**: 3 节点，256 分区
- **生产环境**: 10+ 节点，1024+ 分区
