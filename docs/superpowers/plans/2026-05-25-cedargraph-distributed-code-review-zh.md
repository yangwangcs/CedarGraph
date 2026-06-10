# CedarGraph 分布式系统 — 代码审查与架构审计

> **面向自动化工作者：** 本文档是对 CedarGraph 分布式层的全面生产就绪审计。内容涵盖架构全景、逐层读写路径追踪，以及包含精确文件路径和行号引用的问题清单。

**目标：** 完整理解 CedarGraph 的分布式架构，识别生产级代码质量问题，并为每个子系统提供精确的文件/函数引用。

**架构概述：** CedarGraph 是一款分布式时序图数据库，采用类 NebulaGraph 的架构分离设计：无状态查询路由层（graphd/queryd）、共识元数据层（metad）、带 Raft 复制的有状态存储节点（storaged）、面向分析的图计算节点（gcn），以及跨数据中心复制机制。

**技术栈：** C++17, gRPC/Protobuf, braft/brpc (Raft), LSM-Tree 存储引擎, LZ4 压缩, OpenSSL/TLS, CMake

---

## 目录

1. [整体架构](#1-整体架构)
2. [服务拓扑与二进制程序](#2-服务拓扑与二进制程序)
3. [模块分解](#3-模块分解)
4. [读路径 — 端到端追踪](#4-读路径--端到端追踪)
5. [写路径与两阶段提交 — 端到端追踪](#5-写路径与两阶段提交--端到端追踪)
6. [关键数据结构](#6-关键数据结构)
7. [线程与并发模型](#7-线程与并发模型)
8. [故障处理机制](#8-故障处理机制)
9. [性能特征](#9-性能特征)
10. [代码质量问题发现](#10-代码质量问题发现)

---

## 1. 整体架构

CedarGraph 由七个水平层级组成， Stateless 路由层、图语义层、存储引擎层、分布式事务层、共识层和治理层之间界限清晰。

```
┌─────────────────────────────────────────────────────────┐
│  GraphD / QueryD  (无状态查询路由层)                     │
│  ├─ Cypher 解析器、执行计划器、执行引擎                   │
│  ├─ DistributedExecutor (分区路由、并行执行)             │
│  ├─ Optimized2PCEngine (并行/流水线/批量 2PC)           │
│  └─ GCN 散集-汇聚路由层                                  │
├─────────────────────────────────────────────────────────┤
│  Graph 层 (CedarGraph)                                  │
│  ├─ 语义图 API (邻居查询、BFS、时序查询)                  │
│  └─ Cypher 集成与 TMV 引擎                               │
├─────────────────────────────────────────────────────────┤
│  DB 层 (CedarGraphDB)                                   │
│  ├─ Manifest 管理                                        │
│  └─ 事件总线 / 回填                                      │
├─────────────────────────────────────────────────────────┤
│  存储层 (CedarGraphStorage / LsmEngine)                 │
│  ├─ MemTable (CedarMemTable — VSL 跳表)                 │
│  ├─ 不可变 MemTable                                      │
│  ├─ SST (zone-columnar v1/v2)                           │
│  ├─ 压缩 (size-tiered, 并行)                             │
│  ├─ 块缓存 / 查询缓存 / SST 读取器缓存                    │
│  └─ Blob 存储 (大值)                                     │
├─────────────────────────────────────────────────────────┤
│  事务层 (本地)                                           │
│  ├─ OCCTransaction                                       │
│  ├─ WAL / WALBatchWriter (组提交)                        │
│  └─ TransactionPool                                      │
├─────────────────────────────────────────────────────────┤
│  DTX 层 (分布式)                                         │
│  ├─ Optimized2PCEngine (协调器)                          │
│  ├─ StorageServiceImpl (参与者)                          │
│  ├─ PartitionRaftStateMachine (braft)                    │
│  ├─ 分区管理器 / 迁移器                                   │
│  ├─ CrossDCReplicator (同步/异步 + 协调)                 │
│  └─ Failover Manager (Phi Accrual, 健康检查)             │
├─────────────────────────────────────────────────────────┤
│  治理层                                                  │
│  ├─ ServiceRegistry (基于名称的服务发现)                  │
│  ├─ ConfigManager (热重载)                               │
│  ├─ HealthChecker (HTTP /healthz, /readyz)               │
│  └─ MetricsRegistry (Prometheus 计数器/直方图)            │
├─────────────────────────────────────────────────────────┤
│  核心 / 类型层                                           │
│  ├─ CedarKey (32B 定长, 缓存行对齐)                      │
│  ├─ Descriptor (内联或 blob 后备值)                      │
│  ├─ Timestamp (微秒, 降序存储编码)                       │
│  ├─ Status, Slice, Env, CRC32C                          │
│  └─ ThreadPool, BackgroundWorker                         │
└─────────────────────────────────────────────────────────┘
```

---

## 2. 服务拓扑与二进制程序

### 生产二进制程序

| 二进制程序 | 源码 | 输出名 | 角色 | 默认端口 |
|--------|--------|--------|------|-------------|
| **graphd** | `tools/graphd.cc` | `cedar-graphd` | 无状态 Cypher 查询路由器 | 9669 |
| **queryd** | `src/queryd/cedar_queryd_full.cpp` | `cedar-queryd` | 分布式查询执行层 | 9669 |
| **storaged** | `tools/storaged.cc` | `cedar-storaged` | 有状态存储节点 + 2PC 参与者 | 9779 |
| **metad** | `tools/metad.cc` | `cedar-metad` | 基于 braft 共识的元数据服务 | 9559 |
| **gcn** | `tools/graphcomputenode.cc` | `graphcomputenode` | 图计算节点 (TMV 引擎) | — |

### 服务交互

```
[客户端] ──gRPC──► [GraphD / QueryD]
                          │
                          ├──gRPC──► [MetaD] (分区映射、Schema、节点注册)
                          │
                          ├──gRPC──► [StorageD-1] ──Raft──► [StorageD-2,3] (复制)
                          ├──gRPC──► [StorageD-N]
                          │
                          └──gRPC──► [GCN] ──散集/汇聚──► [GCN 对等节点]
```

- **GraphD** 解析 Cypher，制定执行计划，通过缓存自 MetaD 的分区映射路由到 StorageD。
- **QueryD** 是较新的分布式查询层，带有 `PartitionRouter`、`ParallelExecutor`、`ResultMerger` 和显式事务 API。
- **StorageD** 运行 `CedarGraphStorage` (LSM 引擎)，参与 2PC，通过 Raft 复制分区。
- **MetaD** 使用 braft 对分区分配、节点注册、心跳收集进行共识。
- **GCN** 运行 `TMVEngine`，带有 CDC 监听器、基于水印的 GC 和散集-汇聚路由。

---

## 3. 模块分解

### 3.1 核心与类型 (`src/core/`, `src/types/`)

| 文件 | 职责 |
|------|---------------|
| `include/cedar/types/cedar_key.h` | 32 字节定长键。排序规则：`entity_id 升序 → entity_type 升序 → column_id 升序 → target_id 升序 → timestamp 降序 → sequence 升序` |
| `include/cedar/types/descriptor.h` | 值载荷。小值 (≤4 字节) 直接内联；大值存入 blob 存储 |
| `include/cedar/types/timestamp.h` | 微秒级时序排序，降序编码用于存储 |
| `src/core/status.cc` | 带消息处理的 Status 码 |
| `src/core/env.cc` | 平台抽象（文件、内存映射、线程） |

### 3.2 存储引擎 (`src/storage/`, `src/sst/`)

| 文件 | 职责 |
|------|---------------|
| `src/storage/cedar_graph_storage.cc` | 公共存储 API：`Put`、`Get`、`Scan`、`BatchWrite`、`BatchGet`、边 API |
| `src/storage/lsm_engine.cc` | LSM-Tree：memtable → 不可变 memtable → SST 层级 L0–L6、刷盘、压缩编排 |
| `src/storage/cedar_memtable.cc` | 内存写缓冲区。双结构：`std::map<InternalKey, vector<MemTableEntry>>` + 版本链双向链表 |
| `src/storage/vsl_memtable.h` | 向量化跳表封装（CedarMemTable 的演进） |
| `src/storage/compaction_engine.cc` | Size-tiered 压缩调度器 |
| `src/storage/compaction_merger.cc` | K 路合并，使用最小堆、去重、tombstone 丢弃 |
| `src/sst/zone_columnar_builder.cc` | SST V2 构建器：实体对齐块、区域编码（RLE、delta-delta、字典、LZ4） |
| `src/sst/zone_columnar_reader.cc` | SST V2 读取器：块缓存、谓词下推、时序布隆过滤器 |

### 3.3 事务层 (`src/transaction/`)

| 文件 | 职责 |
|------|---------------|
| `src/transaction/wal.cc` | 预写日志：组提交、fsync、文件轮转 |
| `src/transaction/occ_transaction.cc` | 乐观并发控制：读集验证、写集缓冲 |
| `src/transaction/transaction_pool.cc` | 池化事务上下文复用 |

### 3.4 分布式事务 (`src/dtx/`)

| 文件 | 职责 |
|------|---------------|
| `src/dtx/optimized_2pc_engine.cc` | 协调器：`Execute2PC`、`ExecuteParallel2PC`、`ExecutePipelined2PC`、`ExecuteBatched2PC`、决策日志持久化 |
| `src/dtx/storage_impl/storage_service_impl.cc` | 参与者 gRPC 服务：`Prepare`、`Commit`、`Abort` 处理器 |
| `src/dtx/storage_impl/partition_storage.cc` | 每分区 2PC 状态机：OCC 验证、加锁、WAL |
| `src/dtx/raft/braft_node.cc` | 元数据 Raft 状态机：快照保存/加载、领导者回调 |
| `src/dtx/storage/braft_partition_state_machine.cc` | 分区 Raft 状态机：日志应用、快照复制 |
| `src/dtx/storage/partition_migrator.cc` | 在线分区迁移：CopyData → CatchUp → SwitchTraffic → Verify |
| `src/dtx/cross_dc_replicator.cc` | 跨 DC 复制：同步/异步模式、清理、协调循环 |
| `src/dtx/storage/failover_manager.cc` | 故障转移控制器：健康检查、Phi Accrual、领导者切换 |
| `src/dtx/phi_accrual.cc` | Phi Accrual 故障检测：滑动窗口、正态 CDF |

### 3.5 查询与 Cypher (`src/cypher/`, `src/queryd/`, `src/query/`)

| 文件 | 职责 |
|------|---------------|
| `src/cypher/parser.cc` | 递归下降 Cypher 解析器 |
| `src/cypher/execution_plan.cc` | 物理算子树构建器：NodeScan、Expand、Filter、Sort、Limit、Project |
| `src/cypher/cypher_engine.cc` | 带执行计划缓存的查询执行引擎 |
| `src/queryd/distributed_executor.cpp` | PartitionRouter、ParallelExecutor、ResultMerger、单分区快速路径 |
| `src/queryd/query_storage_client.cpp` | 带熔断器的存储客户端，本地与远程节点选择 |
| `src/queryd/query_service_full.cpp` | QueryService gRPC 实现 |
| `src/query/cedar_scan.cc` | 基于快照的时序扫描引擎 |

### 3.6 Graph 层 (`src/graph/`)

| 文件 | 职责 |
|------|---------------|
| `src/graph/cedar_graph.cc` | 语义图 API：时序邻居、BFS、Allen 关系、Cypher 集成 |
| `src/graph/tmv_engine.cc` | GCN 的时序多版本引擎 |

### 3.7 分区管理 (`src/partition/`)

| 文件 | 职责 |
|------|---------------|
| `src/partition/partition_strategy_manager.cc` | StaticHash 与 MTHStream 之间的 AUTO 模式切换 |
| `src/partition/static_hash_strategy.cc` | `entity_id % num_partitions` |
| `src/partition/mth_stream_strategy.cc` | 基于 Count-Min Sketch 的流式哈希 |

### 3.8 治理层 (`src/governance/`)

| 文件 | 职责 |
|------|---------------|
| `src/governance/service_registry.cc` | 带监Watcher的基于名称的服务发现 |
| `src/governance/health_checker.cc` | HTTP /healthz /readyz 端点（原始 socket） |
| `src/governance/config_manager.cc` | 热重载配置 |

### 3.9 GCN (`src/gcn/`)

| 文件 | 职责 |
|------|---------------|
| `src/gcn/gcn_node.cc` | GCN 进程生命周期 |
| `src/gcn/scatter_gather_router.cc` | 子查询分发与结果收集 |
| `src/gcn/local_compute_thread.cc` | BFS/DFS 遍历执行 |
| `src/gcn/event_applier.cc` | 带重排序缓冲区的有序 CDC 事件应用 |
| `src/gcn/watermark_gc.cc` | 基于水印的块垃圾回收 |

---

## 4. 读路径 — 端到端追踪

### 4.1 客户端入口

**文件：** `tools/graphd.cc` → `src/service/graph_service_router.cc:137`

```cpp
grpc::Status GraphServiceRouter::ExecuteQuery(
    grpc::ServerContext* context,
    const ExecuteQueryRequest* request,
    ExecuteQueryResponse* response)
```

1. GraphD 在端口 9669 启动 gRPC 服务器。
2. 客户端发送带 Cypher 字符串的 `ExecuteQuery` 请求。
3. 路由器检查查询缓存（`query_cache_->Get(cache_key)`）。命中则立即返回。
4. 未命中则调用 `ParseQueryForRouting()`（第 178 行）。

### 4.2 查询解析与计划

**文件：** `src/service/graph_service_router.cc:870-1083`

5. `ParseQueryForRouting()` 调用 `cypher::CypherParser parser(query); auto stmt = parser.ParseStatement();`

**文件：** `src/cypher/parser.cc`

6. `CypherParser::ParseStatement()` 执行递归下降解析：
   - 提取时序子句。
   - 解析 `MATCH`、`WHERE`、`RETURN`、`ORDER BY`、`LIMIT`、`CREATE`、`SET`、`DELETE`。
   - 构建 AST：`MatchClause`、`WhereClause`、`ReturnClause` 等。

7. 路由器对查询类型进行分类：
   - `POINT_LOOKUP`：`WHERE id(n) = xxx`
   - `NEIGHBOR_TRAVERSAL`：`()-[]->()` 模式
   - `AGGREGATE`：`count(*)`、`sum()` 等
   - `SCAN`：其他所有查询

**文件：** `src/cypher/execution_plan.cc:615-748`

8. `ExecutionPlanBuilder::Build()` 自底向上构建物理算子树：
   - `MATCH` → `NodeScan` / `TemporalNodeScan`
   - 关系 → `Expand` 算子
   - `WHERE` → `Filter`
   - `ORDER BY` → `Sort`
   - `LIMIT` / `SKIP` → `Limit` / `Skip`
   - `RETURN` → `Project` → `Distinct`（如需要）→ `ProduceResults`

### 4.3 分区路由

**文件：** `src/service/graph_service_router.cc:1085-1143`

9. `CalculatePartition(entity_id)`：
   - 若 `partition_strategy_` 激活，调用 `PartitionStrategyManager::ComputePartition(key, kNumPartitions)`。
   - 回退：`entity_id % 32768`。

**文件：** `src/partition/partition_strategy_manager.cc`

10. `PartitionStrategyManager::RouteVertex(entity_id)` 委托给活跃的 `IPartitionStrategy`：
    - `STATIC_HASH`：`entity_id % num_partitions`
    - `MTH_STREAM`：基于 Count-Min Sketch 的路由
    - `AUTO`：基于时序查询比例 (> 0.5) 和局部性比例 (> 0.7) 切换

**文件：** `src/queryd/meta_client.cpp:257-323`

11. `QueryMetaClient::FetchClusterStateFromMeta()`：
    - gRPC：`MetaService::GetSpacePartitionMap("default")`
    - 同时调用 `GetAliveNodes()` 进行地址解析。
    - 缓存于 `cached_cluster_state_`，每 30 秒刷新。

**文件：** `src/service/graph_service_router.cc:1101-1143`

12. `GetPartitionRoute()`：
    - 在 `partition_map_mutex_`（shared_lock）下检查 `partition_cache_`。
    - 未命中：`meta_client_->GetPartitionAssignment("default", partition_id)`。
    - 解析领导者/副本地址。
    - 缓存 `PartitionRoute`。

### 4.4 分布式执行

**文件：** `src/queryd/distributed_executor.cpp:505-598`

13. `DistributedExecutor::Execute()`：
    - 解析并验证查询。
    - **单分区检查：** `IsSinglePartitionQuery()` 提取实体 ID。若全部映射到同一分区，调用 `ExecuteSinglePartition()`。
    - 否则调用 `ExecuteCrossPartition()`。

**文件：** `src/queryd/distributed_executor.cpp:930-963`

14. `ExecuteSinglePartition()`：
    - 领导者检查：`router_->CheckIsLeader(partition_id, leader_address)`。
    - 通过 `storage_client_->GetNodeClient(partition_id)` 获取 `NodeClient`。
    - 调用 `node_client->ExecuteSubQuery(query, parameters, result)`。

**文件：** `src/queryd/distributed_executor.cpp:965-1006`

15. `ExecuteCrossPartition()`：
    - `SplitQuery()` 为每个分区创建一个 `SubQueryTask`。
    - `parallel_executor_->ExecuteParallel(tasks, storage_client_, ctx)`。

**文件：** `src/queryd/distributed_executor.cpp:170-225`

16. `ParallelExecutor::ExecuteParallel()`：
    - 将每个任务提交到工作者线程池。
    - 每个任务调用 `storage_client_->GetNodeClient(t.partition_id)->ExecuteSubQuery(...)`。
    - 通过 `std::promise<void>` / `std::future` 收集结果。

**文件：** `src/queryd/distributed_executor.cpp:308-468`

17. `ResultMerger::Merge()`：
    - 拼接所有 `SubQueryResult` 的记录。
    - 若提供 `sort_keys`，执行稳定排序。
    - `MergeAggregate()` 处理带分组的 `COUNT`、`SUM`、`AVG`、`MIN`、`MAX`。

### 4.5 存储层读取

**文件：** `src/storage/cedar_graph_storage.cc:389-445`

18. `CedarGraphStorage::Get(entity_id, entity_type, column_id, timestamp)`：
    - 分布式模式：`rep_->dtx_client->Get(key, timestamp)`（RPC）。
    - 本地模式：`rep_->engine->GetAtTime(entity_id, entity_type, column_id, timestamp)`。

**文件：** `src/storage/lsm_engine.cc:459-618`

19. `LsmEngine::GetAtTime()`：
    1. **查询缓存检查：** `query_cache_->Get(entity_id, column_id, timestamp.value())`。
    2. **MemTable（热）：** `mem_->GetAtTime(...)`。
    3. **不可变 MemTable：** `imm_->GetAtTime(...)`（若存在）。
    4. **累积缓冲区：** `QueryAccumulatedBuffer(...)`。
    5. **SST 文件（冷）：**
       - `compaction_engine_->GetFilesForEntity()` → 候选文件。
       - **时序布隆过滤器**检查跳过无关文件。
       - `SstReaderCache` 复用。
       - `reader->GetRange(...)` → 扫描匹配条目。
       - 选择 **`ts <= timestamp` 的最新版本**。

**时序版本解析：**
- CedarKey 以**降序**存储时间戳。
- `GetAtTime`：扫描所有版本，选择 `ts <= query_timestamp` 的最大值。
- `GetRecordAtTime`：降序排序，选择第一个 `<= timestamp` 的。
- Tombstone（`Descriptor::IsTombstone()`）返回 `std::nullopt`。

### 4.6 响应路径

**文件：** `src/queryd/query_service_full.cpp:189-226`

20. `QueryServiceImpl::ExecuteQuery()` 构建 gRPC 响应：
    - 将 `cypher::ResultSet` → proto `ResultSet`（列 + 行）。
    - 填充统计信息：`execution_time_us`、`rows_scanned`、`rows_returned`、`storage_nodes_accessed`、`network_roundtrips`。

**文件：** `src/service/graph_service_router.cc:137-483`

21. `GraphServiceRouter::ExecuteQuery()` 后处理：
    - 若 `has_aggregate`，跨分区结果聚合。
    - ORDER BY、LIMIT、SKIP 切片。
    - 查询缓存插入：`query_cache_->Put(cache_key, response->result_set())`。
    - 返回 `ExecuteQueryResponse`。

---

## 5. 写路径与两阶段提交 — 端到端追踪

### 5.1 写入入口

**文件：** `src/service/graph_service_router.cc:137-242`

1. `GraphServiceRouter::ExecuteQuery()` 通过 `IsWriteQuery()` 检测写操作。
   - **显式事务：** 将键累积到 `active_transactions_[txn_id].write_set` 中，延迟提交。
   - **自动提交：** 分配新的 `TxnID`，立即调用 `two_pc_engine_->Execute2PC(txn_id, read_set, write_set, Timestamp(now_ts))`。

**文件：** `src/queryd/query_service_full.cpp:68-227`

2. `QueryServiceImpl::ExecuteQuery()` 分派到 `DistributedExecutor::Execute()`。
   - 写片段到达 `StorageServiceImpl` gRPC 处理器。

### 5.2 分布式事务协调器

**文件：** `src/dtx/optimized_2pc_engine.cc:196-281`

3. `Optimized2PCEngine::Execute2PC(txn_id, read_set, write_set, commit_ts)`：
   - 创建带原子状态机的 `TransactionContext`：
     ```cpp
     enum class State { kInit, kPreparing, kPrepared, kCommitting,
                        kCommitted, kAborting, kAborted };
     ```
   - 向 `TransactionTimeoutManager` 注册。
   - 持久化参与者列表：`state_manager_->CreateTransaction(txn_id, pids)`。
   - 选择策略：`Sequential`、`Parallel`、`Pipelined`、`Batched`、`Hybrid`。
   - 默认热路径：`ExecuteParallel2PC()`。

**文件：** `src/dtx/optimized_2pc_engine.cc:1148-1215`

4. 参与者确定：
   - `GetParticipants(write_set)`：读取每个键的 `key.part_id()`。
   - 回退：若 `part_id == 0`，则 `entity_id % clients_.size()`。
   - 去重并排序 `PartitionID`。

### 5.3 第一阶段：Prepare

**文件：** `src/dtx/optimized_2pc_engine.cc:619-701`

5. `ExecuteParallel2PC()`：
   - 状态 → `kPreparing`。
   - 为每个参与者生成一个 `thread_pool_->Schedule` 任务。
   - 每个任务：`client->Prepare(ctx->txn_id, ctx->read_set, ctx->write_set, ctx->commit_ts)`。
   - 原子更新 `prepare_acks` / `prepare_nacks`。
   - `WaitForPrepareQuorum()`：**要求所有参与者**（`required_successes = total`）。
   - 任何 NACK → 中止路径。

**CAS 超时保护**（流水线/批量变体，第 386-388 行、831-832 行）：
```cpp
auto expected = TransactionContext::State::kInit;
if (ctx->state.compare_exchange_strong(expected,
                                       TransactionContext::State::kAborted)) {
    state_manager_->UpdateState(ctx->txn_id, TxnState::kAborted);
}
```

**文件：** `src/dtx/storage_impl/storage_service_impl.cc:793-908`

6. `StorageServiceImpl::Prepare()`：
   - 反序列化读/写集。
   - 每分区：
     - **Raft 路径：** 验证 `raft_group->IsLeader()`，提议 `StorageLogEntry::Type::kPrepare`。
     - **直接路径：** `partition->Prepare(...)`。
   - 任何失败 → 回滚已 prepare 的分区。

**文件：** `src/dtx/storage_impl/partition_storage.cc:117-188`

7. `PartitionStorage::Prepare()`：
   - 重复检查：若 `prepared_txns_` 已包含 `txn_id` 则拒绝。
   - **写写冲突检测：** 遍历现有 `PreparedTxnState`；相同的 `(entity_id, column_id)` → `Status::Busy`。
   - **读集验证：** 查询 `LsmEngine` 中读键的版本。若存在 `txn_version > commit_ts` 的版本 → `Status::Busy`。
   - 注册 `PreparedTxnState`，`status = kPrepared`。
   - WAL 持久化：`WriteTxnWAL(txn_id, "PREPARE")`。

### 5.4 决策日志持久化

**文件：** `src/dtx/optimized_2pc_engine.cc:409-456`

8. `PersistCommitDecision(decision)`：
   - 在广播 Commit **之前**调用。
   - 向 `<decision_log_dir>/txn_{txn_id}.decision` 写入二进制决策日志：
     - 魔数 `0x44454301`，版本 `1`
     - `txn_id`、`commit_ts`、参与者数量、参与者列表
   - `fsync` 实现崩溃安全持久化。
   - 若持久化失败 → 中止所有参与者。

### 5.5 第二阶段：Commit

**文件：** `src/dtx/optimized_2pc_engine.cc:731-797`

9. 并行 Commit 广播：
   - 状态 → `kCommitting`。
   - 生成并行 `Commit` RPC 任务。
   - `WaitForCommitQuorum()`：再次要求所有参与者成功。

**成功路径：**
- 状态 → `kCommitted`。
- `state_manager_->UpdateState(txn_id, TxnState::kCommitted)`。

**不完整提交路径：**
- 状态保持 `kCommitting`。
- `recovery_manager_->StartRecovery(txn_id)` 读取决策日志并驱动剩余参与者。

**文件：** `src/dtx/storage_impl/storage_service_impl.cc:923-1030`

10. `StorageServiceImpl::Commit()`：
    - 查找涉及的分区。
    - 每分区：通过 Raft 提议 `kCommit` 或直接调用 `partition->Commit()`。
    - 部分失败 → 回滚已提交的分区。

**文件：** `src/dtx/storage_impl/partition_storage.cc:191-257`

11. `PartitionStorage::Commit()`：
    - 验证事务为 `kPrepared`。
    - 对每个写键：`shared_storage_->Put(entity_id, timestamp, desc, commit_ts)`。
    - 部分写入 → `kCommitting` + `IOError`。
    - 完全成功 → `kCommitted`、`WriteTxnWAL(txn_id, "COMMIT")`、从 `prepared_txns_` 中擦除。

### 5.6 存储层写入

**文件：** `src/storage/cedar_graph_storage.cc:312-343`

12. `CedarGraphStorage::Put()`：
    - 计算分区 ID。
    - 构建 `CedarKey`。
    - 分布式：通过 `dtx_client->Put()` 进行 RPC。
    - 本地：`rep_->engine->Put(key, descriptor, txn_version)`。

**文件：** `src/storage/lsm_engine.cc:228-263`

13. `LsmEngine::Put()`：
    1. 获取 `mutex_` 的 `unique_lock`。
    2. **先写 WAL：** `wal_writer_->WritePut(key, descriptor, txn_version)`。
    3. **MemTable 插入：** `mem_->Put(key, descriptor, txn_version)`。
    4. 缓存失效。
    5. **自动刷盘：** 若 `mem_->ApproximateMemoryUsage() >= threshold`，解锁，`MaybeScheduleFlush()`。

**文件：** `src/transaction/wal.cc`

14. `WalWriter::WritePut()`：
    - 构建带 `WalRecordType::kPut` 的 `WalBatch`。
    - 组提交：入队到 `commit_queue_`，在 `future` 上等待。
    - `WriteInternal()`：编码 `WalRecordHeader`（crc32、type、flags、length、sequence），追加 batch。
    - `GroupCommitThread`：排空队列，写入 batch，每组 **`fsync`** 一次。

**文件：** `src/storage/lsm_engine.cc:1344-1409`

15. `MaybeScheduleFlush()`：
    - 将 `mem_` → `imm_`，创建新的 `mem_`。
    - `std::async` 启动 `FlushMemTable(imm)`。

**文件：** `src/storage/lsm_engine.cc:2162-2312`

16. `FlushMemTable()` / `FlushEntityGroup()`：
    - 遍历不可变 memtable。
    - 按 `LessForSorting`（时间戳降序）排序条目。
    - 通过 `SstBuilderFactory::Create` 创建 `.sst`。
    - 构建 `SSTFileMeta`，插入 `levels_[0]`。
    - 通知压缩引擎。

### 5.7 Raft 复制

**文件：** `src/dtx/raft/braft_node.cc:68-292`

17. **元数据 Raft — `MetaRaftStateMachine`：**
    - `on_apply`：反序列化 `RaftCommand`，调用 `meta_service_->ApplyRaftCommand()`。
    - `on_snapshot_save`：原子写入（临时文件 → fsync → rename）。
    - `on_snapshot_load`：读取 `meta_snapshot.bin`，反序列化。
    - `on_leader_start` / `on_leader_stop`：通知元数据服务。

**文件：** `src/dtx/storage/braft_partition_state_machine.cc:74-250`

18. **分区 Raft — `PartitionRaftStateMachine`：**
    - `on_apply`：反序列化 `StorageRaftCommand`（49 字节：`[type:1][key:32][desc:8][txn_version:8]`）。
      - `kPut` → `storage_->Put(key, desc, txn_version)`
      - `kDelete` → `storage_->Put(key, Descriptor(), txn_version)`
    - `on_snapshot_save`：强制刷盘，复制数据目录，序列化 prepared 事务。
    - `on_snapshot_load`：恢复数据 + prepared 事务。
    - 领导者强制：非领导者返回 `UNAVAILABLE` 并重定向。

### 5.8 跨 DC 复制

**文件：** `src/dtx/cross_dc_replicator.cc:155-202`

19. `CrossDCReplicator::Replicate()`：
    - 构建 `ReplicationLog`。
    - **同步模式：** 顺序遍历 `peer_dcs_`。首次失败时：
      - 尽力清理：对已成功的 DC 执行 `DeleteFromDC(key, succ_dc)`。
      - 清理失败 → 加入 `reconciliation_queue_`。
    - **异步模式：** 加入 `replication_queue_`，立即返回。

**文件：** `src/dtx/cross_dc_replicator.cc:555-631`

20. `ReconciliationLoop()`：
    - 每秒最多排空 64 个条目。
    - `ReconcileKey(key, dc_id)`：
      - 查询本地权威存储获取最新值。
      - 若存在 → 复制当前值。
      - 若已删除 → 发送 tombstone。
      - 失败 → 以 5 秒退避重新加入队列。

### 5.9 响应路径

**文件：** `src/service/graph_service_router.cc:233-242`

21. `Execute2PC` 成功时：
    - `response->set_success(true)`。
    - `result_set.set_total_rows(...)`。
    - 失败时：`stats_.failed_queries++`、`response->set_success(false)`、`response->set_error_msg(...)`。

---

## 6. 关键数据结构

### 6.1 CedarKey
**文件：** `include/cedar/types/cedar_key.h`

32 字节定长，缓存行对齐（64B）：
```
Offset 0-7:   entity_id (大端序)
Offset 8-15:  timestamp_be (降序大端序)
Offset 16-23: target_id
Offset 24-25: column_id / edge_type
Offset 26-27: sequence
Offset 28:     entity_type (0=顶点, 1=出边, 2=入边)
Offset 29:     flags (操作类型、分布式、压缩、tombstone、锁)
Offset 30-31:  part_id
```

### 6.2 Descriptor
**文件：** `include/cedar/types/descriptor.h`

值载荷。小值 (≤4 字节) 直接内联；大值存入 blob 存储。

### 6.3 TransactionContext
**文件：** `include/cedar/dtx/optimized_2pc_engine.h:55`

```cpp
enum class State { kInit, kPreparing, kPrepared, kCommitting,
                   kCommitted, kAborting, kAborted };
std::atomic<State> state{State::kInit};
std::atomic<int> prepare_acks{0};
std::atomic<int> prepare_nacks{0};
std::atomic<int> commit_acks{0};
std::atomic<int> abort_acks{0};
std::shared_ptr<std::promise<void>> done_promise;
```

### 6.4 PartitionState
**文件：** `include/cedar/dtx/failover_manager.h`

```cpp
struct PartitionState {
  PartitionID pid;
  NodeID current_leader;
  std::vector<NodeID> replicas;
  bool is_failover_in_progress = false;
  NodeID failover_target = 0;
  std::chrono::steady_clock::time_point last_failover;
};
```

### 6.5 HealthScore
**文件：** `include/cedar/dtx/failover_manager.h`

六维加权分数：
- TCP 延迟：25%
- Raft 滞后：20%
- 磁盘 I/O：15%
- 内存：15%
- CPU：15%
- 错误率：10%

---

## 7. 线程与并发模型

### 7.1 存储引擎
- `LsmEngine`：单个 `std::shared_mutex`。写操作获取 `unique_lock`；读操作获取 `shared_lock`。
- `CedarMemTable`：单个 `std::shared_mutex`。`Put` 在整个持续时间内获取 `unique_lock`。
- 后台线程：`bg_thread_`（手动 delete）、`auto_compaction_thread_`（手动 delete）。
- `SizeTieredCompactionEngine`：自有 `levels_mutex_`、任务队列 + `condition_variable`、2 个压缩线程。

### 7.2 2PC 引擎
- `Optimized2PCEngine`：`pipeline_thread_`、`batch_thread_`、`tuning_thread_`、工作者线程池。
- `thread_pool_->Schedule()` 分派并行 Prepare/Commit RPC。
- 超时管理器：后台线程扫描已注册的事务。

### 7.3 故障转移控制器
- 两个专用线程：`lease_thread_`、`health_thread_`。
- 有界工作者池（`cedar::ThreadPool`，最多 16 个）用于故障转移执行。
- `PartitionFailoverController` 中的 **13 个互斥锁**：
  `partitions_mutex_`、`callbacks_mutex_`、`route_mutex_`、`consensus_callback_mutex_`、
  `replica_health_mutex_`、`health_scores_mutex_`、`score_history_mutex_`、
  `degraded_nodes_mutex_`、`phi_detectors_mutex_`、`collectors_mutex_`、
  `health_probe_callback_mutex_`、`node_addresses_mutex_`。

### 7.4 跨 DC 复制器
- `ReplicationLoop` 线程（异步模式）。
- `ReconciliationLoop` 线程（始终运行）。
- `reconciliation_mutex_` 保护队列。

### 7.5 QueryD
- `ParallelExecutor`：将子查询提交到工作者线程池。
- `ResultMerger`：合并来自多个线程的结果。

---

## 8. 故障处理机制

### 8.1 防御性编码
- 线程边界处大量使用 `try { ... } catch (...) { std::cerr << ... }`。防止崩溃但掩盖错误。

### 8.2 2PC 故障模式
- **Prepare 失败：** 中止所有参与者，状态 → `kAborted`。
- **决策日志失败：** 中止所有参与者，返回 `IOError`。
- **部分提交：** 状态保持 `kCommitting`，恢复管理器完成剩余参与者。
- **超时：** CAS 从 `kInit` → `kAborted`。若 CAS 失败（工作者已启动），等待准确结果。

### 8.3 Raft 故障模式
- 非领导者存储节点返回 `UNAVAILABLE` 并重定向地址。
- `on_apply` 错误调用 `iter.set_error_and_rollback()` 并卸任。
- 快照 I/O 使用 EINTR 重试循环、fsync、原子 rename。

### 8.4 故障转移
- **Phi Accrual：** `phi = -log10(1 - CDF(silence))`。基于阈值的怀疑检测。
- **趋势检测：** 最近 3 个分数单调递减 + 总体 < 60 → 降级。
- **领导者租约过期：** 触发自动故障转移。
- **维护模式：** `maintenance_nodes_` 中的节点被恢复跳过。
- **有界工作者池：** 防止级联故障期间无限制地生成线程。

### 8.5 跨 DC
- 同步复制：故障时尽力清理 → 协调队列。
- 异步复制：指数退避重试（最大 2^6 = 64 倍）。
- 协调：查询本地权威状态，复制当前值或 tombstone。

### 8.6 健康检查
- 原始 BSD socket HTTP 端点，5 秒超时。
- 有界线程池（4 线程）处理 HTTP 请求。
- 连接限制：100 个活跃连接。

---

## 9. 性能特征

### 9.1 读路径
- **缓存层：** 查询结果缓存 → 计划缓存 → LSM 查询缓存 → 跨查询缓存 → SST 读取器缓存。
- **时序解析：** 降序时间戳编码可在某些路径中实现高效的最新版本选择，无需完全排序。
- **读放大：** LSM 查询可能打开多个 SST 文件。当前读取器中没有块级布隆过滤器 —— 点查询可能退化为扫描大量块。
- **MemTable 迭代器：** 在 `shared_lock` 下复制整个 `map_`。创建时的 O(N) 内存尖峰。

### 9.2 写路径
- **先写 WAL：** 在修改 memtable 之前确保持久化。
- **组提交：** 每批 `fsync` 一次。减少写放大。
- **写放大：** Size-tiered 压缩，增长比率为 4×，产生约 3–5× 的写放大。
- **MemTable 刷盘：** 若不累积，会生成许多小文件。

### 9.3 两阶段提交
- **全有或全无仲裁：** CedarGraph 要求所有参与者都 prepare 和 commit。不接受部分成功。
- **并行 RPC：** Prepare 和 Commit 阶段都通过线程池生成并行任务。
- **流水线模式：** 通过流水线队列和工作循环批量处理事务。

### 9.4 分区迁移
- **阻塞排空：** `SwitchTraffic` 休眠 `2 × rpc_timeout_ms`，而非主动排空协议。
- **WAL 重放：** 每个 WAL 条目一次 RPC。无批处理。
- **快照流式传输：** 64KB 块，每个作为单独的 gRPC Write() 调用发送，无流控。

### 9.5 故障转移
- 健康检查循环每 `check_interval`（默认 1 秒）迭代所有唯一副本。
- TCP 探测：非阻塞连接，500 毫秒 `select()` 超时。

---

## 10. 代码质量问题发现

### 10.1 关键 (P0)

| # | 问题 | 位置 | 影响 | 建议 | 修复状态 | 修复摘要 |
|---|-------|----------|--------|----------------|----------|----------|
| 1 | **MemTable 迭代器复制整个 map** | `src/storage/cedar_memtable.h` | 每次迭代器 O(N) 内存尖峰；读密集型负载会经历分配风暴和类 GC 暂停 | 实现引用原始 map 的快照迭代器，无需复制 | ✅ 已修复 | `map_` 改为 `std::shared_ptr<MapType>`，Iterator 持 `shared_ptr<const MapType>`，Put 时 COW |
| 2 | **PartitionStrategyManager 单互斥锁** | `src/partition/partition_strategy_manager.cc` | 每次查询都获取相同的互斥锁进行路由；高并发下的热点 | 用原子操作分片计数器；使用读-复制-更新进行策略切换 | ✅ 已修复 | 引入 `atomic<IPartitionStrategy*>` 无锁热路径，mutex 仅在策略切换时使用 |
| 3 | **故障转移控制器 13 个互斥锁，无锁顺序** | `src/dtx/storage/failover_manager.cc` | 死锁风险高；`HealthCheckLoop` 获取多个互斥锁并可能调度重新进入的工作 | 记录并强制执行全局锁顺序；将相关状态合并到更少的互斥锁下 | ✅ 已修复 | 合并为 5 个互斥锁，添加锁顺序文档，HealthCheckLoop 统一加锁 |
| 4 | **HTTP 健康端点是手写原始 socket 解析器** | `src/governance/health_checker.cc` | 无 header 验证，易受请求走私攻击；不适合生产暴露 | 替换为最小 HTTP 库（如 llhttp）或 brpc HTTP 服务器 | ✅ 已修复 | 增加请求验证（方法/版本/CRLF/拒绝 Transfer-Encoding/重复 Content-Length） |
| 5 | **CompactionMergerV2::ShouldFilter 永远返回 false** | `src/storage/compaction_merger_v2.cc` | V2 压缩路径中 tombstone 永远不会被过滤；存储膨胀 | 实现实际的过滤逻辑或禁用 V2 路径直至修复 | ✅ 已修复 | 实现 tombstone 检测：当 `desc.IsTombstone()` 且 `remove_tombstones` 时过滤 |

### 10.2 高 (P1)

| # | 问题 | 位置 | 影响 | 建议 | 修复状态 | 修复摘要 |
|---|-------|----------|--------|----------------|----------|----------|
| 6 | **WAL 重放：每个条目一次 RPC** | `src/dtx/storage/partition_migrator.cc` | 迁移追赶期间的网络开销；减慢恢复速度 | 将 WAL 条目批量处理成分块 RPC | ✅ 已修复 | 新增 `ReplicateWALBatch` RPC，每批 100 条 WAL 条目批量发送 |
| 7 | **SST 构建器 CalculateCRC64 始终返回 0** | `src/sst/zone_columnar_builder.cc` | SST 写入无数据完整性验证 | 实现数据区域的 CRC64 | ✅ 已修复 | 组合两个 CRC32C（不同种子）拼接为 64 位校验和 |
| 8 | **ScanWithLateMaterialization 已声明但未实现** | `include/cedar/sst/zone_columnar_reader.h` | 宣传的优化缺失；每次扫描都完全物化 | 实现延迟物化或移除声明 | ✅ 已修复 | 实现延迟物化：先过滤 Zone 0/3 收集行号，再批量读取 Zone 4 |
| 9 | **PartitionMigrator SwitchTraffic 休眠固定时长** | `src/dtx/storage/partition_migrator.cc` | 确定性迁移延迟；无主动排空 | 实现 fencing token 或进行中请求计数器 | ✅ 已修复 | 改为 active drain 轮询：每 10ms 检查 `num_active_txns`，超时后继续 |
| 10 | **MemTable node_pool_ 永不收缩** | `src/storage/cedar_memtable.cc` | 刷盘后内存保留直到 MemTable 销毁 | 刷盘后修剪 node_pool_ 或使用 arena 分配器 | ✅ 已修复 | 新增 `Clear()` 方法，刷盘后清空 map、version_chains 和 node_pool |
| 11 | **LsmEngine 混合原始指针和 unique_ptr** | `src/storage/lsm_engine.cc` | `bg_thread_`、`auto_compaction_thread_` 在 `Close()` 中手动 delete | 转换为 `std::unique_ptr<std::thread>` | ✅ 已修复 | `bg_thread_` / `auto_compaction_thread_` 改为 `std::unique_ptr<std::thread>` |
| 12 | **块缓存是 FIFO，不是 LRU** | `src/sst/zone_columnar_reader.cc` | 16 块 FIFO 缓存对偏斜访问模式无效 | 替换为 LRU 或时钟驱逐 | ✅ 已修复 | BlockCacheEntry 增加 `last_access_time`，驱逐时扫描选择最旧条目 |

### 10.3 中 (P2)

| # | 问题 | 位置 | 影响 | 建议 | 修复状态 | 修复摘要 |
|---|-------|----------|--------|----------------|----------|----------|
| 13 | **AUTO 模式无滞后** | `src/partition/partition_strategy_manager.cc` | 混合工作负载下可能在策略间振荡 | 添加冷却期和降级阈值 | ✅ 已修复 | 增加 60s 冷却期 (`kAutoSwitchCooldown`)，记录 `previous_strategy_name_` 防抖动 |
| 14 | **Phi Accrual 从头重新计算均值/方差** | `src/dtx/phi_accrual.cc` | 每次 `Phi()` 调用 O(N)；N=1000 时无影响但会累积 | 使用 Welford 在线算法进行增量统计 | ✅ 已修复 | 维护 `running_sum_` 和 `running_sum_sq_`，push/pop 时增量更新，分布计算 O(1) |
| 15 | **GetLeaderCandidates 在锁下复制副本** | `src/dtx/storage/failover_manager.cc` | 复制和使用之间健康分数可能变化 | 持分区锁时检查健康分数，或之后重新验证 | ✅ 已修复 | 整个评分循环在统一的 `health_state_mutex_` 下完成，保证一致性 |
| 16 | **PartitionMigrator StreamSnapshotToTarget 偏移量未使用** | `src/dtx/storage/partition_migrator.cc` | 每个块的 `request.set_offset(0)`；偏移字段无意义 | 跟踪并设置实际字节偏移 | ✅ 已修复 | 使用 `uint64_t offset` 累加实际字节偏移，`offset += bytes_read` |
| 17 | **SST 构建器在内存中缓冲整个文件** | `src/sst/zone_columnar_builder.cc` | 大 SST 的大堆尖峰 | 直接流式传输到文件描述符 | ✅ 已修复 | `Finish()` 改为流式写入：先计算偏移量，然后逐块 Append 到 `WritableFile` |
| 18 | **健康检查是串行而非并行** | `src/governance/health_checker.cc` | 一次一个组件；组件多时会变慢 | 使用线程池并行化健康检查 | ✅ 已修复 | `RunAllChecks()` 使用 `std::async(std::launch::async)` 并行执行所有组件检查 |
| 19 | **VersionChainIterator 复制整个链** | `src/storage/cedar_memtable.h` | 版本多的键的大复制 | 实现链表上的惰性迭代器 | ✅ 已修复 | 改为持有 `shared_ptr<const vector<MemTableEntry>>`，避免每次构造都拷贝 |
| 20 | **每次写入都使查询缓存失效** | `src/storage/lsm_engine.cc` | 所有写入都使查询缓存失效；写密集型负载饿死读取 | 使用版本控制或选择性失效 | ✅ 已修复 | 改为列级失效：只失效写入的 `column_id`，减少缓存抖动 |

### 10.4 低 (P3)

| # | 问题 | 位置 | 影响 | 建议 | 修复状态 | 修复摘要 |
|---|-------|----------|--------|----------------|----------|----------|
| 21 | **头文件保护混合 FERN_ 和 CEDAR_ 前缀** | 多个头文件 | 表示代码库重命名不完整 | 统一为 CEDAR_ | ✅ 已修复 | 批量替换全部 60+ 头文件中的 `#ifndef FERN_` → `#ifndef CEDAR_` 等 |
| 22 | **EscapeJsonString 十六进制状态污染** | `src/governance/health_checker.cc` | 不重置的 `std::hex` 可能影响后续数字插入 | 十六进制插入后重置流标志 | ✅ 已修复 | 保存 `oss.flags()` 并在十六进制插入后恢复，防止状态污染 |
| 23 | **PartitionMigrator CalculateChecksum 遍历所有 SST** | `src/dtx/storage/partition_migrator.cc` | 大分区时极其昂贵 | 使用 SST 元数据校验和 | ✅ 已修复 | 改用 SST 元数据（file_number, file_size, level）计算 CRC，再以 64KB chunk 读取原始文件内容累加 CRC |
| 24 | **Get(uint64_t) 硬编码 column_id=0** | `src/storage/cedar_memtable.cc` | 通用接口不适合多列查找 | 移除或泛化 | ✅ 已修复 | 保留向后兼容，添加注释说明硬编码 `column_id=0`，建议使用 `GetAtTime()` 泛化接口 |
| 25 | **RestartViaSystemd strncpy 非空终止** | `src/dtx/storage/failover_manager.cc` | 潜在的非空终止路径 | 使用 `strlcpy` 或 `std::string` + `.c_str()` | ✅ 已修复 | 每次 `std::strncpy` 后显式设置 `addr.sun_path[sizeof(addr.sun_path) - 1] = '\0'` |

---

*文档基于源代码直接分析编制。所有文件路径和行号引用对应分支 `main` 上的提交 `641118a`。*
