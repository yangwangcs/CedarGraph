# CedarGraph-Core 分布式系统启动报告

> 报告生成时间: 2026-05-26
> 审查范围: 全部 C++ 源码（不含 MD 文档）
> 审查方式: 逐行代码审查 + 调用链追踪
> 排除范围: 登录/注册模块（按用户要求）

---

## 1. 执行摘要

CedarGraph-Core 是一个具备**完整架构骨架**的分布式时序图数据库，包含 MetaD（元数据/Raft 共识）、GraphD（查询路由）、StorageD（LSM 存储）、GCN（图计算加速）四大服务。代码总量约 3 万行 C++，采用 C++17 标准。

**当前状态**: 单机存储和本地查询路径**基本可用**，但**分布式端到端运行存在关键阻断点**。系统目前处于"架构完整、实现待补"的阶段。

**关键结论**: 要让系统流畅跑起来，需要优先修复 3 个 P0 级问题，然后完成 4 个 P1 级问题。预计需要 2-3 个迭代周期。

---

## 2. 服务组件详细状态

### 2.1 MetaD (元数据服务) — 状态: ✅ 基本可用

**入口**: `tools/metad.cc` (177 行)

| 功能 | 状态 | 说明 |
|------|------|------|
| CLI 参数解析 | ✅ | `--node_id`, `--listen`, `--peer`, `--test_mode` 等 |
| braft Raft 共识 | ✅ | `MetadataService` 封装 braft，`test_mode` 可跳过 |
| gRPC 服务 | ✅ | `MetaServiceGrpcServer` 独立启动 |
| 节点注册 | ✅ | `RegisterNode` RPC 正常 |
| 心跳检查 | ✅ | 后台线程检查节点存活 |
| 分区管理 | ✅ | `GetPartitionAssignment`, `GetSpacePartitionMap` |
| Schema 管理 | ✅ | `CreateLabelSchema`, `GetSchema` |

**风险**: 无严重问题。生产环境需确保 braft 的 peer 配置正确。

---

### 2.2 GraphD (查询服务) — 状态: ⚠️ 部分可用

**入口**: `tools/graphd.cc` (226 行)

| 功能 | 状态 | 说明 |
|------|------|------|
| gRPC 服务启动 | ✅ | `QueryService` + `CedarGraphService` |
| Cypher 查询解析 | ✅ | `cypher::CypherParser` + AST 分析 |
| 查询路由 | ✅ | `ParseQueryForRouting` 支持 MATCH/WHERE/RETURN/ORDER BY/LIMIT |
| 单分区查询 | ⚠️ | 依赖 `ExecuteSubQuery` 本地路径 |
| 跨分区查询 | ❌ | `RemoteRPCNodeClient::ExecuteSubQuery` 会调用 StorageD 未实现的 RPC |
| 图遍历 (Traverse) | ❌ | `DistributedExecutor::Traverse` 调用 `ScanOutEdges`/`ScanInEdges`，但 `QueryStorageClient` 未设置 `base_client_` |
| 2PC 分布式事务 | ⚠️ | `Optimized2PCEngine` 已初始化，但 `StorageClient` 连接可能不完整 |
| 查询缓存 | ✅ | 指纹缓存 + TTL |
| 健康检查 | ✅ | HTTP 9668 端口 |
| 指标收集 | ✅ | Prometheus 格式 9667 端口 |

**关键代码路径分析**:

```
客户端 -> graphd::ExecuteQuery
  -> ParseQueryForRouting (解析 Cypher，确定查询类型)
  -> IsWriteQuery?
     YES -> two_pc_engine_->Execute2PC (可用，但依赖 StorageClient)
     NO  -> distributed_executor_->Execute
            -> IsSinglePartitionQuery?
               YES -> node_client->ExecuteSubQuery
                      -> 本地: NodeClientImpl (Cypher 解析 + StorageBackedExecutionContext)
                      -> 远程: RemoteRPCNodeClient -> ExecuteSubQuery RPC (❌ 未实现)
               NO  -> SplitQuery -> ParallelExecutor::ExecuteParallel
                      -> 每个分区调用 node_client->ExecuteSubQuery (同上)
```

**P0 阻断**: `QueryStorageClient` 在 `graphd.cc:119-120` 创建时**未调用 `SetBaseClient()`**，导致：
- `ScanNode()` 返回 `NotSupported("Independent mode not implemented")`
- `ScanOutEdges()` 同上
- `ScanInEdges()` 同上
- 这直接阻塞了 `StorageBackedExecutionContext` 的所有存储访问（`get_all_entities_fn`, `get_out_neighbors_fn`, `get_in_neighbors_fn`）

**P0 阻断**: `RemoteRPCNodeClient` 调用 `ExecuteSubQuery` RPC，但 `tools/storaged.cc` 的 `StorageServiceImpl` **没有实现该方法**。

---

### 2.3 StorageD (存储服务) — 状态: ⚠️ 存储引擎可用，查询服务不完整

**入口**: `tools/storaged.cc` (513 行)

| 功能 | 状态 | 说明 |
|------|------|------|
| LSM 引擎启动 | ✅ | `CedarGraphStorage::Open` -> `LsmEngine::Open` |
| WAL 恢复 | ✅ | `ReplayWAL(1)` 恢复 memtable |
| SST 读写 | ✅ | Zone-columnar V2，6 个 zone，含 MVCC Zone-5 |
| 压缩 (Compaction) | ✅ | `SizeTieredCompactionEngine` + `CompactionMerger` |
| Put/Get/Delete | ✅ | gRPC `StorageService` 已实现 |
| 2PC (Prepare/Commit/Abort) | ⚠️ | 内存中的状态机，不真正持久化到 LSM |
| Scan/ScanNodeV2/ScanEdgeV2 | ⚠️ | 协议定义存在，但 `storaged.cc` 中 `StorageServiceImpl` 未实现这些 method |
| ExecuteSubQuery | ❌ | **完全未实现**，返回 UNIMPLEMENTED |
| 心跳 | ✅ | 向 MetaD 注册并定期心跳 |
| 健康检查 | ✅ | HTTP 7000 端口 |
| Raft 状态机 | ❌ | `StorageRaftStateMachine::on_apply()` 是空壳 |

**关键代码路径**:

```
StorageD::main
  -> CedarGraphStorage::Open (data_dir, options)
     -> LsmEngine::Open
        -> LoadSstFiles (加载现有 SST)
        -> InitWAL + ReplayWAL (恢复未刷盘数据)
        -> SizeTieredCompactionEngine::Open
  -> StorageServiceImpl (gRPC 服务)
     -> Put: storage_->Put(entity_id, timestamp, desc, txn_version) ✅
     -> Get: storage_->Get(entity_id, timestamp) ✅
     -> Delete: storage_->Delete(...) ✅
     -> Prepare: 内存 txn_states_ map ⚠️
     -> Commit: 内存状态更新 ⚠️
     -> ExecuteSubQuery: **不存在** ❌
     -> ScanNodeV2: **不存在** ❌
     -> ScanEdgeV2: **不存在** ❌
```

**P0 阻断**: `ExecuteSubQuery` 是实现分布式查询下推的核心 RPC。没有它，GraphD 无法将 Cypher 子查询发送到远程 StorageD 执行。

**P0 阻断**: `ScanNodeV2` 和 `ScanEdgeV2` 未实现，导致 GraphD 的 `ExecutePartitionQuery` 中的 SCAN 和 NEIGHBOR_TRAVERSAL 查询类型会失败。

**P1 问题**: `StorageRaftStateMachine::on_apply()` (`src/dtx/storage/storaged_raft_state_machine.cc:10-16`) 只有 TODO，Raft 日志不应用到存储。这意味着多节点部署时，数据不会通过 Raft 复制。

---

### 2.4 GCN (图计算节点) — 状态: ⚠️ 骨架可用

**入口**: `tools/graphcomputenode.cc` (56 行)

| 功能 | 状态 | 说明 |
|------|------|------|
| TMV 引擎 | ✅ | `gcn::TMVEngine` 初始化 |
| gRPC 服务 | ✅ | `GcnServiceImpl` 注册 |
| Scatter-Gather 路由 | ✅ | 多 GCN 支持 |
| 心跳 | ✅ | 向 MetaD 发送存活心跳 |
| CDC 事件接收 | ❌ | `CdcListenerLoop()` 是 sleep 循环，无实际 CDC 连接 |
| 存储回填 | ⚠️ | `StorageBackfillService` 存在，但默认关闭 |

**P2 问题**: CDC 监听器未连接到 WAL 变更流，GCN 缓存会过时。

---

## 3. 跨服务调用链分析

### 3.1 写操作路径

```
客户端 -> GraphD::ExecuteQuery (写查询)
  -> two_pc_engine_->Execute2PC(txn_id, read_set, write_set, commit_ts)
     -> 向每个参与的 StorageD 发送 Prepare RPC
        -> StorageServiceImpl::Prepare (storaged.cc:150-162)
           -> 仅更新内存 txn_states_ map，**不调用 LSM 存储**
     -> 如果全部 Prepared，发送 Commit RPC
        -> StorageServiceImpl::Commit (storaged.cc:164-175)
           -> 仅更新内存状态，**不调用 LSM 存储**
```

**结论**: 2PC 目前是"内存游戏"，写操作不会真正持久化到 StorageD 的 LSM 引擎。要让写操作工作，需要：
1. `Prepare` 时锁定 key + 写入 WAL
2. `Commit` 时实际调用 `storage_->Put()`

### 3.2 读操作路径（单分区）

```
客户端 -> GraphD::ExecuteQuery (读查询)
  -> distributed_executor_->Execute
     -> IsSinglePartitionQuery = true
     -> ExecuteSinglePartition
        -> storage_client_->GetNodeClient(partition_id)
           -> IsLocalPartition? 
              YES -> NodeClientImpl
                     -> ExecuteSubQuery
                        -> CypherParser::ParseStatement
                        -> ExecutionPlanBuilder::Build
                        -> StorageBackedExecutionContext (需要 ScanNode/ScanOutEdges)
                           -> storage_client_->ScanNode ❌ (无 base_client_)
                           -> storage_client_->ScanOutEdges ❌ (无 base_client_)
              NO  -> RemoteRPCNodeClient
                     -> ExecuteSubQuery RPC ❌ (StorageD 未实现)
```

**结论**: 即使单分区查询，由于 `QueryStorageClient` 缺少 `base_client_`，本地执行路径中的存储访问也会失败。

### 3.3 读操作路径（跨分区）

```
客户端 -> GraphD::ExecuteQuery
  -> distributed_executor_->Execute
     -> IsSinglePartitionQuery = false
     -> ExecuteCrossPartition
        -> SplitQuery -> ParallelExecutor::ExecuteParallel
           -> 对每个分区: node_client->ExecuteSubQuery
              -> 本地/远程都面临同上问题 ❌
```

---

## 4. 阻塞问题清单

### P0 — 系统无法端到端运行

| # | 问题 | 文件位置 | 影响 | 修复复杂度 |
|---|------|----------|------|------------|
| P0-1 | QueryStorageClient 未设置 base_client_ | `tools/graphd.cc:119-120` | 所有本地存储访问失败 (ScanNode/ScanOutEdges/ScanInEdges) | 低 |
| P0-2 | StorageD 未实现 ExecuteSubQuery | `tools/storaged.cc:146-283` | 分布式查询下推完全不可用 | 中 |
| P0-3 | StorageD 未实现 ScanNodeV2/ScanEdgeV2 | `tools/storaged.cc:146-283` | GraphD 的 ExecutePartitionQuery 中 SCAN/TRAVERSE 失败 | 中 |

### P1 — 功能严重受限

| # | 问题 | 文件位置 | 影响 | 修复复杂度 |
|---|------|----------|------|------------|
| P1-1 | StorageRaftStateMachine 是空壳 | `src/dtx/storage/storaged_raft_state_machine.cc:10-16` | 多节点数据复制不工作 | 高 |
| P1-2 | 2PC Prepare/Commit 不持久化 | `tools/storaged.cc:150-175` | 写操作是内存状态，重启丢失 | 中 |
| P1-3 | GCN CDC 监听器未连接 | `src/gcn/gcn_node.cc:203-209` | GCN 缓存永远不过期/不更新 | 中 |
| P1-4 | FlushEntityGroup 丢失 txn_version | `src/storage/lsm_engine.cc` | 批量写入时 MVCC 版本丢失 | 低 |

### P2 — 质量/性能问题

| # | 问题 | 文件位置 | 影响 |
|---|------|----------|------|
| P2-1 | BlockHeader::kSize 注释错误 | `include/cedar/sst/zone_columnar_reader.h` | 48 bytes vs 44 bytes，误导开发者 |
| P2-2 | QueryStorageClient::CreateScan 返回 nullptr | `src/queryd/query_storage_client.cpp:237-241` | CedarScan 不可用 |
| P2-3 | ConfigManager JSON/YAML save 未实现 | `src/storage/cedar_config.cc:475-481` | 配置无法持久化 |
| P2-4 | GraphD::Traverse 中 Scan 结果未处理 | `src/service/graph_service_router.cc:700-706` | (void)path; (void)item; |

---

## 5. 数据流完整性检查

### 5.1 MVCC 数据流

```
写入:
  OCCTransaction -> VSLMemTable::Insert(key, desc, txn_version) ✅
  -> LockedVSL::Traverse (回调带 txn_version) ✅
  -> LsmEngine::FlushMemTable (accumulated_entries_ 为 tuple) ✅
  -> ZoneColumnarSstBuilderV2::Add(key, desc, txn_version) ✅
  -> Zone-5 编码 txn_versions ✅
  -> SST 文件写入 ✅

读取:
  ZoneColumnarSstReader::GetRange -> tuple<CedarKey, Descriptor, Timestamp> ✅
  -> LsmEngine::GetRange/GetRangeLimit/GetRecordAtTime (3 元素解构) ✅
  -> CompactionMerger (MergeHeapItem 带 txn_version) ✅
  -> 新生成 SST 保留 txn_version ✅
```

**结论**: MVCC Zone-5 数据流**完整且正确**。

### 5.2 事务数据流

```
GraphD::ExecuteQuery (写)
  -> two_pc_engine_->Execute2PC ⚠️ (能发起 2PC，但 StorageD 端不持久化)
  
StorageD::StorageServiceImpl::Prepare
  -> 内存 txn_states_[txn_id] = kPrepared ⚠️ (未写 WAL/LSM)
  
StorageD::StorageServiceImpl::Commit  
  -> 内存 txn_states_[txn_id] = kCommitted ⚠️ (未实际 Put 到 LSM)
```

**结论**: 事务协调层存在，但**参与者端不完整**，写操作不会真正持久化。

---

## 6. 构建系统检查

**CMakeLists.txt** 状态良好：
- ✅ 4 个主要可执行文件: `graphd`, `metad`, `storaged`, `graphcomputenode`
- ✅ Proto 生成规则完整
- ✅ 库依赖正确（braft, brpc, gRPC, LZ4, yaml-cpp）
- ✅ 测试目标丰富（`test_distributed_simple`, `test_2pc_optimized` 等）
- ⚠️ `storaged_legacy` 和 `storaged` 两个存储目标并存，可能混淆

---

## 7. 风险评估

| 风险 | 等级 | 说明 |
|------|------|------|
| 无法执行跨分区查询 | 🔴 高 | P0-2 + P0-3，系统退化为单节点 |
| 无法执行本地图遍历 | 🔴 高 | P0-1，所有需要邻域扩展的查询失败 |
| 写操作重启丢失 | 🟡 中 | P1-2，演示场景可接受，生产不可接受 |
| 数据无法复制 | 🟡 中 | P1-1，单节点可运行，多节点不一致 |
| GCN 缓存失效 | 🟢 低 | P1-3，不影响核心查询路径 |

---

## 8. 启动就绪性结论

**当前系统可以运行的模式**:
1. ✅ **单节点 StorageD**: 直接通过 gRPC Put/Get 存取数据，LSM 引擎完整工作
2. ✅ **单节点 MetaD**: 元数据管理、节点注册、心跳检查
3. ❌ **GraphD + StorageD 协同**: 查询下推路径断裂
4. ❌ **多节点集群**: Raft 复制不工作

**要让系统"流畅跑起来"（端到端查询+写入+分布式），必须完成**:
1. 修复 P0-1: GraphD 中设置 QueryStorageClient 的 base_client_
2. 修复 P0-2: StorageD 实现 ExecuteSubQuery
3. 修复 P0-3: StorageD 实现 ScanNodeV2/ScanEdgeV2
4. 修复 P1-2: StorageD 的 2PC 真正持久化到 LSM

---

*报告结束。下一步: 基于本报告设计分阶段实施计划。*
