# CedarGraph-Core 架构说明：分布式时态图事务、存储与计算

## 1. 研究问题与设计命题

CedarGraph-Core 的架构围绕一个核心命题展开：分布式时态图系统不应把所有图更新都退化为昂贵的全局协调，而应利用时间窗口、分区局部性和 LSM 存储特征，把大部分操作限制在更小的验证和提交范围内。

在传统分布式图数据库中，系统通常将一致性问题简化为 key 级读写冲突和跨分区原子提交。这种做法在静态图或普通 KV 系统中较直接，但在时态图中会产生两个浪费：一是时间上互不相交的访问被误判为冲突；二是单分区事务仍被迫承担分布式协调成本。CedarGraph-Core 的架构以 LND-OCC、TW-CD、MTH 分区和 GCN/TMV 为主要机制，分别处理协调、冲突检测、数据放置和计算加速问题。

## 2. 总体架构

系统由四类服务和若干库模块组成。

| 层次 | 模块/服务 | 主要职责 | 关键代码 |
|---|---|---|---|
| 元数据层 | MetaD | schema、space、partition、服务注册、索引元数据 | `src/dtx/meta/*`、`src/governance/*` |
| 存储层 | StorageD | LSM、WAL、Raft、MVCC、备份恢复 | `src/storage/*`、`src/dtx/storage*`、`src/sst/*` |
| 查询事务层 | GraphD | Cypher、路由、事务协调、结果聚合 | `src/cypher/*`、`src/service/*`、`src/queryd/*` |
| 计算层 | GCN | TMV 视图、CDC、遍历、Scatter-Gather | `src/gcn/*` |
| 客户端与运维 | Client/Scripts | 连接池、监控、部署、负载均衡 | `src/client/*`、`scripts/*` |

服务间通信主要使用 gRPC/Protobuf；底层一致性组件使用 vendored brpc/braft。构建系统会生成 `proto/*.proto` 对应的 gRPC 代码，并将其链接到核心静态库和服务可执行文件中。

## 3. 数据模型

### 3.1 实体类型

CedarGraph 将图数据归一为三种实体：

| 类型 | 含义 | 查询意义 |
|---|---|---|
| `Vertex` | 顶点属性或状态 | 点查询、属性过滤 |
| `EdgeOut` | 从源点出发的边 | 出邻居遍历 |
| `EdgeIn` | 指向目标点的反向边 | 入邻居遍历 |

出边和入边双写使正向和反向遍历都能走有序 key 范围，而不需要全图扫描。

### 3.2 CedarKey 布局

`CedarKey` 是 32 字节固定长度 key，面向 LSM 排序和现代 CPU cache 友好性设计。

| 偏移 | 字段 | 说明 |
|---:|---|---|
| 0-7 | `entity_id` | 顶点、源点或路由实体 |
| 8-15 | `timestamp_be` | 降序存储时间戳，新版本排前 |
| 16-23 | `target_id` | 边目标点、内联值或扩展值 |
| 24-25 | `column_id` | 属性列、边类型或锚点列 |
| 26-27 | `sequence` | 同一时间戳下的顺序号 |
| 28 | `entity_type` | Vertex / EdgeOut / EdgeIn |
| 29 | `flags` | create/update/delete、压缩、锁、墓碑 |
| 30-31 | `part_id` | 分区 ID |

时间戳使用降序编码，使 `GetRecordAtTime()` 可以快速找到不晚于快照时间的最新可见版本。

### 3.3 描述符与值

`Descriptor` 记录值的位置、长度、压缩方式和 schema 版本。小值可以通过 key 或 descriptor 内联，大值可进入 blob 文件。SST 层提供列编码、zone-columnar 布局、Bloom Filter 和压缩，以减少时态版本和属性列查询的 I/O。

## 4. 存储引擎

存储层采用 LSM-Tree 路线。

| 组件 | 作用 |
|---|---|
| WAL | 持久化提交前日志，用于崩溃恢复 |
| MemTable / VSLMemTable | 接收最新写入，支持多版本 |
| Immutable MemTable | flush 前缓冲 |
| SST | 持久化有序文件 |
| Block Cache / Reader Cache | 降低重复读 I/O |
| Blob Manager | 管理大值文件和 GC |
| Compaction | 合并版本、回收墓碑、整理层级 |

时态查询由 `CedarScan` 提供快照视图。它会优先使用状态锚点和区间锚点判断实体是否存在，再回退到普通版本查找。边查询通过 `ScanEdgesWithFolding()` 折叠同一边的多个版本，只返回快照下可见的最新边。

## 5. 分布式事务架构

### 5.1 事务上下文

`DistributedTxnContext` 记录事务 ID、开始时间、提交时间、事务类型、参与分区、读写集、时序窗口和因果依赖。读写集项携带 `TemporalWindow`，使冲突检测不只关心 key，还关心访问的时间范围。

### 5.2 LND-OCC 分层提交

`LndOccEngine` 是分布式事务协议入口。它为每个分区维护 `LocalTransactionCoordinator`，并按事务类型选择提交策略。

单分区路径：

1. 若上下文未开始，`SinglePartitionCommit()` 会调用本地 coordinator 的 `BeginTransaction()`。
2. `BeginTransaction()` 分配事务 ID、开始时间戳、参与分区和 TW-CD 窗口。
3. `Validate()` 注册写集并调用 TW-CD。
4. `Commit()` 分配提交时间戳，写 WAL，写 MemTable。
5. 清理 TW-CD 写集索引和窗口索引。

跨分区路径：

1. `SameTemporalRangeCommit()` 创建事务状态，执行参与者 prepare。
2. 若 prepare 全部成功，则提交；若失败，则中止已准备参与者。
3. `FullTwoPhaseCommit()` 通过 StorageClient 对参与者执行完整 prepare/commit/abort。
4. 若 commit 阶段部分失败，事务状态保留为 committing，等待恢复管理器补全。

### 5.3 TW-CD 冲突检测

TW-CD 的检测流程：

1. 活跃事务注册自己的 `TemporalWindow` 到区间树。
2. 写事务注册写集到 `key_to_txns_`。
3. 当前事务验证时，先查询时间窗口重叠的事务集合。
4. 若集合为空，直接无冲突。
5. 若存在时间重叠，再检查当前读集是否命中其他事务写集，形成读写冲突。
6. 再检查当前写集是否命中其他事务写集，形成写写冲突。
7. 提交或中止后，注销写集和窗口。

本次代码检查修复了两个关键问题：

| 问题 | 风险 | 修复 |
|---|---|---|
| `LndOccEngine` 没有创建 `TwcdEngine` | 默认路径实际跳过 TW-CD 验证 | 构造时按 `enable_twcd` 创建并持有 TW-CD |
| `UnregisterWindow()` 先删事务记录，写集索引可能残留 | key 索引污染，长跑误判冲突 | 窗口注销前先清理写集索引，并保持调用方幂等 |

已补回归测试：

- `TwcdEngineTest.UnregisterWindowAlsoClearsWriteSetIndex`
- `LndOccEngineTest.DefaultInitializationEnablesTwcdConflictDetection`

## 6. 分区架构

系统保留两类分区策略。

| 策略 | 特点 | 适用场景 |
|---|---|---|
| Static Hash | 实现简单，负载均匀性依赖 hash | baseline、无明显图局部性场景 |
| MTH Stream | 根据 sketch、时间、实体归属和负载 fast path 分配 | 时态事件流、热点实体、邻接遍历 |

MTH 的核心路径是 `MTHPartitioner::AssignEvent()`。当 sketch 对某个实体和时间给出高置信分区，且分区容量和负载约束允许时，系统走 fast path；否则回退到通用流式分区器。该策略通过 `FastPathRatio()` 暴露 fast path 比例，便于实验观测。

## 7. 查询架构

### 7.1 Cypher 层

Cypher 模块包含 parser、validator、execution plan、expression evaluator、fingerprint、cost optimizer 和 temporal dialect。系统支持基础图查询和时态语义扩展，例如 `FOR SYSTEM_TIME AS OF` 与时间范围查询。GraphD 将查询解析后路由到本地存储、远端 storage 或 GCN。

### 7.2 CedarScan

`CedarScan` 是快照读接口，提供：

- `CedarScan::At(ts, engine)`
- `CedarScan::Now(engine)`
- `GetNode(node_id)`
- `OutEdges(src_id, edge_type)`
- `InEdges(dst_id, edge_type)`

快照读的关键优化包括状态锚点、区间锚点、主动实体 bitmap、双向边 key 和版本折叠。

### 7.3 QueryD / GraphD 路由

QueryD 相关代码已合并到 GraphD 方向，承担分布式执行、storage client、meta client 和执行上下文功能。GraphD 是无状态查询入口，适合水平扩展；元数据和分区 leader 信息由 MetaD 和服务发现模块提供。

## 8. GCN/TMV 计算架构

GCN 面向低延迟图遍历和近实时计算视图。

| 组件 | 作用 |
|---|---|
| `TMVEngine` | 多版本顶点/边 chunk 存储 |
| `QueryDispatcher` | 本地遍历和子查询派发 |
| `ScatterGatherRouter` | 本地 miss 后分布式路由 |
| `EventApplier` | 应用 CDC 事件 |
| `StorageBackfillService` | 计算视图缺失时回源补全 |
| `WatermarkGc` | 按 watermark 回收旧 chunk |

GCN gRPC service 支持 `Traverse`、`SubQuery`、`OnCacheInvalidate` 和 `OnEventStream`。服务中已接入安全检查；当启用认证时，请求需要携带 authorization metadata。

## 9. 故障恢复与治理

系统包含以下治理与恢复机制：

| 领域 | 机制 |
|---|---|
| Raft | braft partition raft、状态机、snapshot、read index |
| 事务恢复 | transaction state manager、recovery manager、decision log |
| 超时 | transaction timeout manager |
| 死锁 | deadlock detector |
| 服务发现 | registry、health checker、DNS safety |
| 安全 | JWT、RBAC、TLS 配置、安全审计 |
| 监控 | metrics registry、Prometheus exporter、storage metrics |

历史生产审计文档记录了若干安全、共识、持久化和配置问题的修复，但这些文档是历史状态。当前发布或论文实验应重新运行测试和压力验证。

## 10. 正确性不变量

架构实现应保持以下不变量：

1. 单分区事务不得绕过本地生命周期初始化；提交前必须拥有 txn id、start timestamp、参与分区和 TW-CD 窗口。
2. TW-CD key 索引中只能包含活跃事务写集；提交或中止后必须清理。
3. 同一 key 但时间窗口不重叠的事务不应被 TW-CD 判为冲突。
4. 时间窗口重叠且 key 读写或写写相交的活跃事务必须被判为冲突。
5. 2PC 在 commit 决策持久化后，即使部分参与者失败，也必须保留可恢复状态。
6. CedarScan 快照读不得返回快照时间之后创建的版本。
7. EdgeOut 与 EdgeIn 的方向语义必须归一，避免入边查询返回反向错误。
8. GCN 缓存失效和 backfill 不得让已删除顶点继续参与遍历结果。

## 11. 验证策略

最小正确性验证：

```bash
cmake --build build --target test_twcd_engine test_lnd_occ -j4
./build/tests/test_twcd_engine
./build/tests/test_lnd_occ
```

扩展验证建议：

| 模块 | 建议测试 |
|---|---|
| DTX | `ctest -R dtx`、2PC 恢复、部分提交、超时 |
| Storage | `ctest -R storage`、WAL replay、compaction、SSTv2 |
| Cypher | `ctest -R cypher`、时态语法、计划缓存并发 |
| GCN | `ctest -R gcn`、事件流、backfill、watermark GC |
| Partition | MTH 与 StaticHash 对比实验 |
| End-to-end | 启动 MetaD/StorageD/GraphD，执行写入、时态查询和故障注入 |

## 12. 预期研究贡献

从系统论文角度，CedarGraph-Core 的可陈述贡献为：

1. 提出一种面向分布式时态图的分层事务提交框架，将单分区、本时间范围跨分区和跨时间范围事务分开处理。
2. 提出并实现 TW-CD 时序窗口冲突检测，用时间维度降低 key-only OCC 的冲突误判。
3. 实现融合时间 sketch、实体归属和负载约束的 MTH 流式分区策略。
4. 将 LSM 时态存储、快照扫描和 GCN/TMV 计算视图连接为一套可运行的时态图系统内核。
5. 提供覆盖事务、存储、查询、分区和 GCN 的测试基础，为后续可复现实验提供工程基线。
