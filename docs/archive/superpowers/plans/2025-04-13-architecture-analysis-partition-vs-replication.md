# CedarGraph 架构分析：分区 (Partition) vs 主从复制 (Leader-Follower)

> **分析日期**: 2025-04-10  
> **问题**: 当前 FailoverManager 的主从设计与 part_id 分片架构不匹配

---

## 执行摘要

**发现问题**: 当前实现的 `FailoverManager` 使用**集群级主从架构**（整个集群一个 Leader），但 CedarGraph 的 `CedarKey` 和现有代码显示设计意图是**分区级分片架构**（类似 NebulaGraph，每个 part_id 独立）。

**影响**: 如果不调整，会导致 part_id 分片机制无法充分利用，限制水平扩展能力。

**建议**: 改造为 **Partition-Raft 架构**，每个 part_id 独立选主，实现真正的分片水平扩展。

---

## 1. 当前架构盘点

### 1.1 CedarKey 的分区设计（已存在）

```cpp
// include/cedar/types/cedar_key.h:208
// Offset 30-31: part_id (uint16_t) - distributed partition ID (0-65536)

static CedarKey Vertex(uint64_t vertex_id,
                       VertexColumnId col,
                       Timestamp ts,
                       uint16_t seq = 0,
                       uint16_t part_id = 0,  // ← 分区 ID
                       uint64_t extension = 0,
                       uint8_t flags = 0);
```

**结论**: 数据层面已支持 65536 个分区。

### 1.2 ComputePartition 实现（当前简单哈希）

```cpp
// src/storage/cedar_graph_storage.cc:45-48
static inline uint16_t ComputePartition(uint64_t entity_id) {
  // 默认使用 65536 (2^16) 个分区，直接取低 16 位
  return static_cast<uint16_t>(entity_id);
}
```

**结论**: 分区计算逻辑存在，但是简单的 entity_id % 65536。

### 1.3 PartitionStorage（已存在）

```cpp
// include/cedar/dtx/storage_service_impl.h:72-130
class PartitionStorage {
  PartitionID partition_id_;  // ← 分区 ID
  CedarGraphStorage* shared_storage_;  // 共享存储
  
  // 自动将 part_id 注入到 key
  CedarKey InjectPartitionId(const CedarKey& key) const;
  static PartitionID ExtractPartitionId(const CedarKey& key);
};
```

**结论**: 已有分区存储抽象，支持多个分区共享底层存储。

### 1.4 StoragePartitionManager（已存在）

```cpp
// include/cedar/dtx/storage_service_impl.h:136-163
class StoragePartitionManager {
  PartitionStorage* GetPartition(PartitionID pid);
  Status AddPartition(PartitionID pid);
  Status RemovePartition(PartitionID pid);
  std::vector<PartitionID> GetAllPartitions() const;
  
  static constexpr PartitionID kMaxPartitions = 65536;
};
```

**结论**: 已支持管理 65536 个分区。

---

## 2. 当前 FailoverManager 设计（问题所在）

```cpp
// include/cedar/storage/failover_manager.h (我们刚实现的)

class FailoverManager {
  std::string current_leader_;  // ← 整个集群只有一个 Leader！
  
  StatusOr<StorageNode> GetLeader() const;  // 全局 Leader
  StatusOr<StorageNode> GetNodeForRead();   // 任意健康节点
  StatusOr<StorageNode> GetNodeForWrite();  // 必须是全局 Leader
};
```

### 问题分析

| 方面 | 当前设计 | 理想设计 (NebulaGraph) |
|------|----------|------------------------|
| **Leader 粒度** | 集群级（1 个 Leader） | 分区级（每个 part_id 有 Leader） |
| **写入路由** | 所有写走 Leader | 写路由到对应分区的 Leader |
| **扩展性** | 垂直扩展（更强机器） | 水平扩展（更多节点） |
| **part_id 利用** | 仅用于数据分布 | 用于数据分布 + 并行处理 |

**关键冲突**: 
- FailoverManager 假设整个集群一个 Leader
- 但 CedarKey 和 PartitionStorage 设计是分区的
- 如果所有写都走全局 Leader，part_id 分片没有意义

---

## 3. NebulaGraph 架构参考

```
┌─────────────────────────────────────────────────────────────┐
│                        NebulaGraph                          │
├─────────────────────────────────────────────────────────────┤
│  Graph Space (图空间)                                        │
│  ├── Partition 0 (part_id=0) ← Raft Group (3 副本)          │
│  │   ├── Node A (Leader for part 0)                        │
│  │   ├── Node B (Follower for part 0)                      │
│  │   └── Node C (Follower for part 0)                      │
│  ├── Partition 1 (part_id=1) ← Raft Group (3 副本)          │
│  │   ├── Node B (Leader for part 1)                        │
│  │   ├── Node C (Follower for part 1)                      │
│  │   └── Node A (Follower for part 1)                      │
│  └── Partition 2..N ...                                     │
└─────────────────────────────────────────────────────────────┘

特点：
1. 每个 Partition 是独立的 Raft 组
2. 每个 Partition 有自己的 Leader（可能在不同节点）
3. 写操作根据 part_id 路由到对应 Partition 的 Leader
4. 读操作可以从对应 Partition 的任意副本读取
```

---

## 4. 建议的新架构：Partition-Raft

### 4.1 核心概念

```
┌──────────────────────────────────────────────────────────────┐
│                    CedarGraph Cluster                        │
├──────────────────────────────────────────────────────────────┤
│  Physical Nodes (3 台机器)                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Storage-1   │  │  Storage-2   │  │  Storage-3   │       │
│  │  (IP:9779)   │  │  (IP:9780)   │  │  (IP:9781)   │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         │                 │                 │               │
│         └─────────────────┼─────────────────┘               │
│                           │                                 │
│  ┌────────────────────────┼────────────────────────┐       │
│  │    Partition Raft Groups (每个 part_id 一个)      │       │
│  ├────────────────────────┼────────────────────────┤       │
│  │  Partition 0 (part_id=0)                        │       │
│  │  ├── Leader: Storage-1                          │       │
│  │  ├── Follower: Storage-2                        │       │
│  │  └── Follower: Storage-3                        │       │
│  ├────────────────────────┼────────────────────────┤       │
│  │  Partition 1 (part_id=1)                        │       │
│  │  ├── Leader: Storage-2  ← 不同分区的 Leader 在不同节点  │
│  │  ├── Follower: Storage-3                        │       │
│  │  └── Follower: Storage-1                        │       │
│  ├────────────────────────┼────────────────────────┤       │
│  │  Partition 2..65535 ...                         │       │
│  └────────────────────────┴────────────────────────┘       │
└──────────────────────────────────────────────────────────────┘
```

### 4.2 写入流程

```cpp
// 客户端写入 Vertex ID = 12345
uint64_t vertex_id = 12345;
uint16_t part_id = ComputePartition(vertex_id);  // e.g., part_id = 42

// 1. 根据 part_id 找到对应的 Raft Group
PartitionRaftGroup* group = GetPartitionGroup(part_id);

// 2. 找到该分区的 Leader（不一定是全局 Leader）
StorageNode leader = group->GetLeader();  // e.g., Storage-2

// 3. 将写操作发送到该分区的 Leader
dtx_client->Put(leader.address, key, value);
```

### 4.3 读取流程

```cpp
// 读取 Vertex ID = 12345
uint16_t part_id = ComputePartition(vertex_id);  // part_id = 42

// 1. 找到对应分区
PartitionRaftGroup* group = GetPartitionGroup(part_id);

// 2. 可以从该分区的任意副本读取（或只从 Leader 读强一致）
StorageNode node = group->GetNodeForRead(read_consistency);

// 3. 读取
dtx_client->Get(node.address, key);
```

---

## 5. 改造计划

### 5.1 需要修改的组件

| 组件 | 当前实现 | 需要改造为 |
|------|----------|------------|
| **FailoverManager** | 集群级单 Leader | **PartitionRaftManager** - 每个 part_id 独立管理 |
| **CedarGraphStorage** | 单节点存储 | **PartitionAwareStorage** - 路由到对应分区 |
| **StorageHealthMonitor** | 监控节点健康 | 扩展为监控 **分区-节点** 健康 |
| **MetaService** | 存储节点列表 | 存储 **分区 -> Leader 映射** |

### 5.2 新组件设计

#### PartitionRaftGroup（新）

```cpp
// 代表一个分区（part_id）的 Raft 组
class PartitionRaftGroup {
 public:
  PartitionID GetPartitionId() const { return part_id_; }
  
  // 该分区的 Leader（与其他分区独立）
  StorageNode GetLeader() const;
  
  // 该分区的所有副本
  std::vector<StorageNode> GetReplicas() const;
  
  // 从该分区的副本中选择一个用于读
  StorageNode GetNodeForRead(ReadConsistency consistency);
  
  // 执行故障转移（仅针对该分区）
  Status FailoverTo(const StorageNode& new_leader);
  
 private:
  PartitionID part_id_;
  std::vector<StorageNode> replicas_;  // 该分区的所有副本
  StorageNode current_leader_;
  RaftState raft_state_;  // Raft 状态机
};
```

#### PartitionRaftManager（替换 FailoverManager）

```cpp
// 管理所有分区的 Raft 组
class PartitionRaftManager {
 public:
  // 为指定分区创建 Raft 组（指定副本节点）
  Status CreatePartitionGroup(PartitionID part_id, 
                               const std::vector<StorageNode>& replicas);
  
  // 获取指定分区的 Raft 组
  PartitionRaftGroup* GetPartitionGroup(PartitionID part_id);
  
  // 路由写入（根据 part_id）
  StatusOr<StorageNode> RouteWrite(uint64_t entity_id);  // 内部计算 part_id
  
  // 路由读取
  StatusOr<StorageNode> RouteRead(uint64_t entity_id, ReadConsistency consistency);
  
  // 获取所有分区的 Leader 分布（用于监控）
  std::map<PartitionID, StorageNode> GetAllLeaders() const;
  
 private:
  std::array<PartitionRaftGroup*, 65536> partition_groups_;  // 所有分区
  ConsistentHashRing* hash_ring_;  // 用于副本放置策略
};
```

### 5.3 改造步骤

#### Step 1: 实现 PartitionRaftGroup
- 包装现有的 PartitionStorage
- 添加 Raft 状态机（或使用 embedded_raft）
- 实现分区内 Leader 选举

#### Step 2: 实现 PartitionRaftManager
- 替换当前的 FailoverManager
- 管理 65536 个分区的 Raft 组
- 实现基于 part_id 的路由

#### Step 3: 改造 CedarGraphStorage
- Put/Get/Delete 操作根据 entity_id 计算 part_id
- 路由到对应分区的 Leader
- 批量操作按 part_id 分组并行发送

#### Step 4: 改造 MetaService
- 存储分区 -> Leader 映射
- 客户端缓存分区路由表
- 分区迁移支持（扩缩容）

---

## 6. 兼容性分析

### 6.1 当前代码的兼容性

| 组件 | 兼容性 | 说明 |
|------|--------|------|
| CedarKey (part_id) | ✅ 完全兼容 | 已经是分区设计 |
| PartitionStorage | ✅ 兼容 | 需要包装进 RaftGroup |
| StoragePartitionManager | ✅ 兼容 | 作为 RaftManager 的基础 |
| FailoverManager | ❌ 需要重写 | 当前是集群级，需要分区级 |
| HealthMonitor | ⚠️ 需要扩展 | 需要支持分区-节点矩阵 |

### 6.2 迁移路径

```
Phase 1 (当前): 主从复制（单 Leader）
       ↓
Phase 2: 分区路由（计算 part_id，但所有分区共用一个 Leader）
       ↓
Phase 3: 完整 Partition-Raft（每个分区独立 Leader）
```

**建议**: 可以先实现 Phase 2（利用现有代码），然后逐步迁移到 Phase 3。

---

## 7. 决策点

### 选项 A: 保持当前主从架构（简单，但限制扩展）
- ✅ 实现简单，当前代码可用
- ❌ 无法利用 part_id 实现水平扩展
- ❌ 所有写操作集中在 Leader，成为瓶颈

### 选项 B: 分区路由但不改 Raft（中等复杂度）
- 根据 part_id 路由到不同节点，但每个节点处理多个分区
- 保持当前 FailoverManager 的节点级监控
- 需要修改写入路由逻辑

### 选项 C: 完整 Partition-Raft（推荐，但工作量大）
- ✅ 真正的水平扩展（类似 NebulaGraph/TiKV）
- ✅ 每个分区独立故障转移
- ✅ 支持分区级别扩缩容
- ❌ 工作量大，需要实现 Raft 组管理

---

## 8. 建议

**推荐方案**: 采用 **选项 C（Partition-Raft）**，但分阶段实施：

### 阶段 1: 立即实施（本周）
- 冻结当前 FailoverManager（作为单节点测试用途）
- 开始设计 PartitionRaftManager
- 定义分区路由接口

### 阶段 2: 短期（下周）
- 实现 PartitionRaftGroup（包装现有 PartitionStorage）
- 实现基于 part_id 的路由（写路由到对应节点）
- 支持单节点多分区（一个进程处理多个 part_id）

### 阶段 3: 中期（本月）
- 实现跨节点的分区副本（Raft 复制）
- 每个分区独立 Leader 选举
- 分区迁移支持（扩缩容）

### 阶段 4: 长期
- 自动分区均衡
- 热点分区检测和迁移

---

## 9. 附录：代码检查清单

### 需要检查/修改的文件

```
高优先级：
- [ ] include/cedar/storage/failover_manager.h (需要重写)
- [ ] src/storage/failover_manager.cc (需要重写)
- [ ] include/cedar/dtx/partition_manager.h (已存在，需要扩展)
- [ ] src/storage/cedar_graph_storage.cc (Put/Get 路由逻辑)

中优先级：
- [ ] include/cedar/dtx/meta_service.h (分区元数据)
- [ ] src/dtx/meta_service.cc (分区路由表)
- [ ] include/cedar/client/cedar_client.h (客户端路由缓存)

低优先级（当前可用）：
- [x] include/cedar/types/cedar_key.h (part_id 已存在)
- [x] include/cedar/dtx/storage_service_impl.h (PartitionStorage 已存在)
- [x] src/storage/cedar_graph_storage.cc (ComputePartition 已存在)
```

---

## 结论

**核心发现**: CedarGraph 的数据层（CedarKey、PartitionStorage）已经设计了分区机制，但服务层（FailoverManager）目前是主从架构，两者不匹配。

**建议**: 改造为 **Partition-Raft 架构**，让每个 part_id 成为独立的 Raft 组，实现真正的水平扩展。

**下一步**: 需要决定是：
1. 继续当前主从架构（简单但不扩展）
2. 实施 Partition-Raft 改造（推荐但工作量大）

---

*报告完成*
