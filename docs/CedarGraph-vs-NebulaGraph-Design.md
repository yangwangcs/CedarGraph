# CedarGraph vs NebulaGraph: 分布式设计对比分析

## 1. NebulaGraph 架构回顾

### 1.1 核心架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           NebulaGraph 架构                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        计算层 (GraphD)                               │
│  │                                                                      │
│  │   GraphD-0        GraphD-1        GraphD-2        ...               │
│  │  ┌───────┐       ┌───────┐       ┌───────┐                          │   │
│  │  │ Query │       │ Query │       │ Query │  无状态，可水平扩展        │   │
│  │  │ Engine│       │ Engine│       │ Engine│                          │   │
│  │  │(C++)  │       │(C++)  │       │(C++)  │                          │   │
│  │  └───┬───┘       └───┬───┘       └───┬───┘                          │   │
│  │      │               │               │                              │   │
│  │      └───────────────┴───────────────┘                              │   │
│  │                 查询计划生成 / 执行                                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        存储层 (StorageD)                             │
│  │                                                                      │   │
│  │   StorageD-0         StorageD-1         StorageD-2                  │   │
│  │  ┌───────────┐      ┌───────────┐      ┌───────────┐                 │   │
│  │  │ Part 1    │◄────►│ Part 2    │◄────►│ Part 3    │   ...          │   │
│  │  │ (Leader)  │      │ (Leader)  │      │ (Leader)  │                 │   │
│  │  │ Part 4    │      │ Part 5    │      │ Part 6    │                 │   │
│  │  │ (Follower)│      │ (Follower)│      │ (Follower)│                 │   │
│  │  └───────────┘      └───────────┘      └───────────┘                 │   │
│  │       │                  │                  │                        │   │
│  │       └──────────────────┴──────────────────┘                        │   │
│  │                    Raft Consensus                                    │   │
│  │                                                                      │   │
│  │  • 每个 Partition 是一个 Raft Group                                  │   │
│  │  • 默认 3 副本，Leader 处理读写                                       │   │
│  │  • 基于 vid 的 Hash 分片                                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        元数据层 (MetaD)                              │   │
│  │                                                                      │   │
│  │   MetaD-0 ◄────► MetaD-1 ◄────► MetaD-2                             │   │
│  │   (Leader)       (Follower)     (Follower)                          │   │
│  │                                                                      │   │
│  │  职责:                                                               │   │
│  │  • Schema 管理（Space、Tag、Edge 定义）                              │   │
│  │  • 分区到节点的映射（Part Location）                                  │   │
│  │  • 负载均衡指令                                                       │   │
│  │  • 用户认证授权                                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 NebulaGraph 核心设计特点

| 特性 | 设计 | 说明 |
|------|------|------|
| **存算分离** | GraphD + StorageD | 查询层无状态，可独立扩展 |
| **数据模型** | 属性图（Property Graph） | 点（Vertex）+ 边（Edge） |
| **分区策略** | vid Hash | `part_id = hash(vid) % num_parts` |
| **一致性协议** | Raft | 每个 Partition 独立 Raft Group |
| **存储引擎** | RocksDB | 每个 Partition 独立 RocksDB 实例 |
| **事务支持** | 有限 | 主要支持单点/单边操作，跨分区事务弱 |
| **查询语言** | nGQL | 类 SQL 的图查询语言 |

### 1.3 NebulaGraph 的事务模型

```
NebulaGraph 事务特点:

1. 单点操作原子性
   INSERT VERTEX/EDGE - 通过 Raft 保证

2. 无分布式事务
   - 不支持跨分区多点修改的原子性
   - 应用层需要自己处理一致性

3. 最终一致性读取
   - 可以从 Follower 读（可能读到旧数据）
   - 强一致读需要走 Leader

4. CAS 操作
   - 支持 Compare-and-Swap
   - 只能操作单个点/边
```

---

## 2. CedarGraph 当前设计与 NebulaGraph 对比

### 2.1 架构对比

| 维度 | NebulaGraph | CedarGraph-DTx (当前设计) | 差异分析 |
|------|-------------|--------------------------|---------|
| **存算分离** | ✅ GraphD/StorageD 分离 | ❌ 存算一体 | Nebula 更灵活扩展查询层 |
| **存储引擎** | RocksDB | 自研 LSM-Tree (VSL) | Cedar 针对时序优化 |
| **数据模型** | 属性图 | 时序事件流 | Cedar 支持时序版本 |
| **分区策略** | vid Hash | CedarKey part_id | 类似，但 Cedar 支持范围分片 |
| **共识协议** | Raft/Partition | Raft/Partition | 相同 |
| **元数据服务** | MetaD (Raft) | Metadata Service (Raft) | 相同 |
| **分布式事务** | ❌ 弱支持 | ✅ 强支持 (DTx) | Cedar 核心优势 |
| **时序支持** | ❌ 无原生支持 | ✅ 核心特性 | Cedar 核心优势 |

### 2.2 事务能力对比

```
NebulaGraph 事务限制:
┌─────────────────────────────────────────┐
│  场景: 转账操作 (A -> B)                │
│                                         │
│  UPDATE A.balance -= 100   [Partition 0]│
│  UPDATE B.balance += 100   [Partition 1]│
│                                         │
│  问题:                                  │
│  • 两个操作可能部分成功                 │
│  • 无原子性保证                         │
│  • 应用层需要自己实现补偿                │
└─────────────────────────────────────────┘

CedarGraph-DTx:
┌─────────────────────────────────────────┐
│  场景: 转账操作 (A -> B)                │
│                                         │
│  BEGIN TXN                              │
│    WRITE A.balance -= 100  [Partition 0]│
│    WRITE B.balance += 100  [Partition 1]│
│  COMMIT                                 │
│                                         │
│  保证:                                  │
│  • 2PC 保证原子性                       │
│  • 成功则全部成功                       │
│  • 失败则全部回滚                       │
└─────────────────────────────────────────┘
```

---

## 3. NebulaGraph 设计对 CedarGraph 的启示

### 3.1 值得借鉴的设计

#### 3.1.1 存储计算分离

```
CedarGraph 当前:
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
┌──────▼──────┐
│ StorageNode │  (查询 + 存储 耦合)
│  (VSL+LSM)  │
└─────────────┘

NebulaGraph 设计:
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
┌──────▼──────┐     ┌─────────────┐
│   GraphD    │────►│  StorageD   │
│Query Engine │     │  (Raft+LSM) │
└─────────────┘     └─────────────┘

优势:
• GraphD 无状态，可独立水平扩展
• 查询性能瓶颈与存储瓶颈分离
• 支持读写分离（多个 GraphD 读，少量写）
```

**对 CedarGraph 的建议**:

```cpp
// 可以引入查询引擎层

// Query Engine (无状态)
class QueryEngine {
public:
    // 解析 Cypher/时序查询
    ExecutionPlan Parse(const std::string& query);
    
    // 执行计划（路由到 StorageD）
    Result Execute(const ExecutionPlan& plan);
    
private:
    // 连接池到 StorageD
    std::unordered_map<PartitionID, StorageClient> storage_clients_;
};

// StorageD (有状态，Raft)
class StorageD {
public:
    // 本地查询执行
    Result LocalQuery(const SubPlan& plan);
    
    // Raft 复制
    Status Replicate(const WriteBatch& batch);
    
private:
    std::unique_ptr<RaftNode> raft_;
    std::unique_ptr<LsmEngine> storage_;
};
```

#### 3.1.2 分区策略优化

```
NebulaGraph 的 vid 设计:
• 支持生成/导入时指定 vid
• 支持字符串/整数 vid
• 通过 hash(vid) 确定分区

CedarGraph 可以借鉴:
• CedarKey 的 entity_id 可以类比 vid
• 支持应用层指定分区（数据局部性优化）
• 支持自动分区（Hash/Range）
```

#### 3.1.3 负载均衡

```
NebulaGraph 的负载均衡器:
• 监控 Partition 数据量和访问热点
• 自动触发 Partition 迁移
• 在线迁移（不影响服务）

CedarGraph 可以引入:
```cpp
class LoadBalancer {
public:
    // 检查分区负载
    LoadReport AnalyzeLoad();
    
    // 生成分区迁移计划
    MigrationPlan GeneratePlan(const LoadReport& report);
    
    // 执行迁移
    Status ExecuteMigration(const MigrationPlan& plan);
};
```

### 3.2 不建议借鉴的设计

#### 3.2.1 弱事务支持

```
NebulaGraph 的设计假设:
• 图遍历读多写少
• 大多数操作可以分区本地化
• 应用层处理跨分区一致性

CedarGraph 的不同:
• 时序数据写密集（持续写入新版本）
• 金融/IoT 场景需要强一致性
• 跨分区事务不可避免（时间序列跨分区）

结论: CedarGraph 必须保持强分布式事务能力
```

#### 3.2.2 每个 Partition 独立 RocksDB

```
NebulaGraph:
• 100 Partitions = 100 个 RocksDB 实例
• 每个实例有自己的 MemTable、WAL
• 资源开销大，但故障隔离好

CedarGraph 现状:
• 共享 LSM-Tree 引擎
• 更好的资源利用
• 需要额外的分区逻辑

权衡:
• Nebula: 更好的故障隔离，但资源开销大
• Cedar: 更好的资源利用，但故障影响面大

建议: CedarGraph 可以支持可选的物理隔离
```

---

## 4. CedarGraph 混合架构方案

结合两者优势，我提出以下混合架构：

### 4.1 混合架构设计

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CedarGraph Hybrid Architecture                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    查询引擎层 (Query Engine)                         │   │
│  │                    (借鉴 NebulaGraph - 无状态扩展)                    │   │
│  │                                                                      │   │
│  │   Query-0          Query-1          Query-2                         │   │
│  │  ┌─────────┐      ┌─────────┐      ┌─────────┐                      │   │
│  │  │ Cypher  │      │ Cypher  │      │ Cypher  │  无状态               │   │
│  │  │ Parser  │      │ Parser  │      │ Parser  │  可水平扩展           │   │
│  │  │ PlanGen │      │ PlanGen │      │ PlanGen │                      │   │
│  │  │ Cache   │      │ Cache   │      │ Cache   │                      │   │
│  │  └────┬────┘      └────┬────┘      └────┬────┘                      │   │
│  │       └─────────────────┴─────────────────┘                         │   │
│  │                  Load Balancer (L7)                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    协调器层 (Coordinator)                            │   │
│  │                    (Cedar DTx - 强事务)                              │   │
│  │                                                                      │   │
│  │   Coordinator-0      Coordinator-1     Coordinator-2                │   │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐              │   │
│  │  │  GLTR       │    │  GLTR       │    │  GLTR       │              │   │
│  │  │  TW-CD      │    │  TW-CD      │    │  TW-CD      │              │   │
│  │  │  LND-OCC    │    │  LND-OCC    │    │  LND-OCC    │              │   │
│  │  │  2PC        │    │  2PC        │    │  2PC        │              │   │
│  │  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘              │   │
│  │         │                  │                  │                      │   │
│  │         └──────────────────┼──────────────────┘                      │   │
│  │                            │                                         │   │
│  └────────────────────────────┼─────────────────────────────────────────┘   │
│                               │                                             │
│                               ▼                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    存储引擎层 (Storage Engine)                        │   │
│  │                    (Cedar VSL + Raft)                                │   │
│  │                                                                      │   │
│  │   Storage-0          Storage-1          Storage-2                   │   │
│  │  ┌───────────┐      ┌───────────┐      ┌───────────┐                 │   │
│  │  │ Partition │      │ Partition │      │ Partition │                 │   │
│  │  │  0 (L)    │◄────►│  1 (L)    │◄────►│  2 (L)    │   ...          │   │
│  │  │  3 (F)    │      │  4 (F)    │      │  5 (F)    │                 │   │
│  │  │  6 (F)    │      │  7 (F)    │      │  8 (F)    │                 │   │
│  │  ├───────────┤      ├───────────┤      ├───────────┤                 │   │
│  │  │ VSL       │      │ VSL       │      │ VSL       │                 │   │
│  │  │ LSM-Tree  │      │ LSM-Tree  │      │ LSM-Tree  │                 │   │
│  │  │ (Shared)  │      │ (Shared)  │      │ (Shared)  │                 │   │
│  │  └───────────┘      └───────────┘      └───────────┘                 │   │
│  │                                                                      │   │
│  │  L = Leader, F = Follower                                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    元数据服务 (Metadata Service)                      │   │
│  │                    (借鉴 Nebula - Raft 共识)                         │   │
│  │                                                                      │   │
│  │   Meta-0 ◄────► Meta-1 ◄────► Meta-2                                │   │
│  │   (Raft Group)                                                       │   │
│  │                                                                      │   │
│  │  职责:                                                               │   │
│  │  • Schema 管理 (Space, Tag, Edge)                                    │   │
│  │  • Part Location (分区映射)                                          │   │
│  │  • 负载均衡决策                                                       │   │
│  │  • 全局事务ID (HLC)                                                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 关键改进点

| 改进 | 来源 | 说明 |
|------|------|------|
| **存算分离** | NebulaGraph | Query Engine 无状态，独立扩展 |
| **协调器独立** | Cedar DTx | 保持强分布式事务能力 |
| **共享存储** | Cedar | 避免 Nebula 多 RocksDB 开销 |
| **Raft/Partition** | 两者 | 保持高可用和数据一致性 |
| **元数据 Raft** | Nebula | 成熟的元数据管理方式 |

---

## 5. 分层职责对比

### 5.1 NebulaGraph 职责

```
┌──────────────┬─────────────────────────────────────┐
│ Layer        │ Responsibility                      │
├──────────────┼─────────────────────────────────────┤
│ GraphD       │ 查询解析、执行计划、结果聚合        │
│ StorageD     │ 数据存储、Raft 复制、本地查询       │
│ MetaD        │ Schema、Part 位置、负载均衡         │
└──────────────┴─────────────────────────────────────┘
```

### 5.2 建议的 CedarGraph 职责

```
┌──────────────────┬─────────────────────────────────────────────────┐
│ Layer            │ Responsibility                                  │
├──────────────────┼─────────────────────────────────────────────────┤
│ Query Engine     │ Cypher 解析、执行计划、时序分析、结果聚合       │
│ Coordinator      │ 事务路由 (GLTR)、冲突检测 (TW-CD)、2PC 协调     │
│ StorageD         │ VSL 存储、Raft 复制、LSM-Tree、本地事务         │
│ Meta Service     │ Schema、Part 位置、负载均衡、HLC                │
└──────────────────┴─────────────────────────────────────────────────┘
```

---

## 6. 实现建议

### 6.1 Phase 1: 引入 Query Engine（可选优化）

```cpp
// 如果当前性能瓶颈在查询，可以优先实现

class CedarQueryEngine {
public:
    // 解析 Cypher 查询
    ExecutionPlan Parse(const std::string& cypher);
    
    // 分布式执行
    ResultSet Execute(const ExecutionPlan& plan) {
        // 1. 确定涉及的 Partitions
        auto partitions = GetPartitions(plan);
        
        // 2. 并行执行子计划
        std::vector<Future<Result>> futures;
        for (auto& part : partitions) {
            futures.push_back(
                storage_clients_[part].ExecuteSubPlan(plan.SubPlan(part))
            );
        }
        
        // 3. 聚合结果
        return AggregateResults(futures);
    }
};
```

### 6.2 Phase 2: 保持存储层不变

```
当前 CedarGraph 的 Storage 层已经很好：
• VSL (Version Chain SkipList) - 高效的时序查询
• Zone-Columnar SST - 优化的存储格式
• 只需要添加 Raft 复制层
```

### 6.3 Phase 3: 协调器优化

```cpp
// 从 DTx 中分离出独立的协调器
class CedarCoordinator {
public:
    // 事务管理
    TxnHandle BeginTransaction();
    Status Commit(TxnHandle handle);
    Status Abort(TxnHandle handle);
    
    // 使用 DTx 的五项创新
    // GLTR, TW-CD, LND-OCC, DVC-Val, BBCC
};
```

---

## 7. 总结

| 方面 | 建议 |
|------|------|
| **存算分离** | 可选优化，Query Engine 独立扩展查询能力 |
| **事务能力** | **保持 Cedar DTx 的强事务能力，这是核心优势** |
| **存储引擎** | 保持 VSL+LSM，不引入多 RocksDB 开销 |
| **Raft 设计** | 参考 Nebula，每个 Partition 独立 Raft Group |
| **元数据** | 参考 Nebula MetaD，3-5 节点 Raft 组 |
| **分区策略** | 支持 Hash/Range/Graph-Aware，灵活选择 |

### 核心结论

1. **NebulaGraph 的优势**：存算分离、成熟的分布式元数据管理
2. **CedarGraph 的优势**：强分布式事务、原生时序支持、高效的 VSL 存储
3. **最佳实践**：**保持 Cedar DTx 的事务能力，可选引入存算分离架构**

### 推荐路径

```
当前 CedarGraph ──► 添加 Raft/Partition ──► 可选添加 Query Engine
      │                    │                      │
      │                    │                      └─ 优化查询扩展性
      │                    └─ 实现高可用和数据复制
      └─ 已完成 DTx 强事务
```

---

## 附录：快速决策表

| 问题 | 使用 Nebula 方案 | 使用 Cedar 方案 | 混合方案 |
|------|-----------------|----------------|---------|
| 查询层扩展 | ✅ 存算分离 | ❌ 单体 | ✅ 可选分离 |
| 强分布式事务 | ❌ 弱支持 | ✅ DTx | ✅ 保持 DTx |
| 时序数据支持 | ❌ 无原生 | ✅ 核心 | ✅ 保持 VSL |
| 存储资源利用 | ❌ 多 RocksDB | ✅ 共享 LSM | ✅ 共享 LSM |
| 故障隔离 | ✅ 分区隔离 | ⚠️ 需要设计 | ⚠️ 可选隔离 |
| 部署复杂度 | ⚠️ 3 层组件 | ✅ 2 层组件 | ⚠️ 3 层组件 |
| 运维经验 | ✅ 社区成熟 | ❌ 需自建 | ⚠️ 部分借鉴 |

**最终建议**:
- **短期**: 保持当前 Cedar DTx 设计，添加 Raft/Partition
- **中期**: 实现元数据服务（类似 MetaD）
- **长期**: 根据查询负载，可选引入 Query Engine 层

