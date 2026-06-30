# CedarGraph 分布式上线 readiness 审计报告

> **历史归档说明**：本文记录 2026-04-28 当时的审计结论，不代表当前代码状态。当前上线判断请以 `README.md`、`docs/user-manual/README.md` 和最新 `./scripts/preflight_release_gate.sh` 结果为准。

**审计日期**: 2026-04-28
**审计范围**: MetaD / StorageD / GraphD / QueryD / 故障恢复
**历史结论**: 当时评估为 **不可上线**，存在 P0 阻塞问题和多个 P1 严重缺陷

---

## 1. 组件成熟度总览

| 组件 | 完成度 | 状态 |
|------|--------|------|
| MetaD (braft 核心) | ~50% | Raft 集群可运行，但元数据逻辑缺失 |
| StorageD (braft 复制) | ~50% | 单键复制可用，快照/2PC/恢复缺失 |
| GraphD/QueryD | ~35% | 查询路由框架完整，但存储 RPC 执行全 stub |
| 节点生命周期 (注册/心跳/故障检测) | ~40% | 注册和心跳可用，故障检测为空 |
| 故障恢复 | ~30% | FailoverManager 多处 TODO |

---

## 2. 历史 P0 阻塞问题（当时必须修复才能上线）

### 2.1 MetaD: 快照序列化损坏 → 重启后状态丢失

**文件**: `src/dtx/meta/meta_service.cc`
**问题**: `MetadataStateMachine::Serialize()` 只保存 `spaces:<count>;nodes:<count>;`，不保存实际的 space/node/assignment 数据。`Deserialize()` 是 no-op。
**影响**: MetaD 节点重启后无法从快照恢复，状态完全丢失。
**修复建议**: 使用 protobuf 或 JSON 完整序列化 `spaces_`、`nodes_`、`partition_maps_`。

### 2.2 MetaD: 无分区分配算法 → StorageD 不知道负责哪些分区

**文件**: `src/dtx/meta/meta_service.cc:119-130`
**问题**: `ApplyCreateSpace()` 创建 `SpacePartitionMap` 时，`assignments` 映射为空。没有任何逻辑将分区分配给已注册的 StorageD 节点。
**影响**: StorageD 调用 `GetSpacePartitionMap()` 返回空分配，无法创建 Raft 组。
**修复建议**: 在 `ApplyCreateSpace()` 或 `ApplyRegisterNode()` 中实现一致性哈希/轮询分配算法。

### 2.3 MetaD: 无节点故障检测 → 死节点永远在线

**文件**: `src/dtx/meta/meta_service.cc:424-430`
**问题**: `HeartbeatCheckLoop()` 是空循环，只有 `// TODO: implement node failure detection`。
**影响**: StorageD 节点崩溃后，MetaD 永远认为它在线，不会触发重新分配或副本提升。
**修复建议**: 实现心跳超时检查，将超时节点标记为 OFFLINE，触发分区重新分配。

### 2.4 StorageD: 快照是 stub → Raft 日志无限增长

**文件**: `src/dtx/storage/braft_partition_raft.cc:220-227`
**问题**: `on_snapshot_save` 和 `on_snapshot_load` 都是 stub（只打日志）。
**影响**: Raft 日志永不截断，磁盘会耗尽；新节点/落后节点无法通过快照快速追赶。
**修复建议**: 使用 `CedarGraphStorage` 的 checkpoint 机制实现快照保存/加载。

### 2.5 StorageD: 2PC 未通过 Raft 复制 → 事务状态本地唯一

**文件**: `src/dtx/storage_impl/storage_service_impl.cc`
**问题**: `Prepare()`/`Commit()`/`Abort()` 直接调用 `partition->Prepare/Commit/Abort()`，不走 Raft Propose。
**影响**: Leader 崩溃后，新 Leader 不知道之前有 prepared 的事务，2PC 协调器无法 inquire，事务悬挂。
**修复建议**: 将 2PC 操作（Prepare/Commit/Abort）序列化为 Raft 日志条目，通过 `Propose()` 提交。

### 2.6 StorageD: Commit() 丢失所有描述符 → 数据损坏

**文件**: `src/dtx/storage_impl/partition_storage.cc`
**问题**: `Prepare()` 没有填充 `state.write_descriptors`，`Commit()` 从中读取描述符时总是得到空值。
**影响**: 所有通过 2PC Commit 写入的数据都存储空 `Descriptor`，数据完整性被破坏。
**修复建议**: 在 `Prepare()` 中将写入的描述符存入 `write_descriptors`。

### 2.7 StorageD: Raft 日志持久化未配置 → 日志可能丢失

**文件**: `src/dtx/storage/braft_partition_raft.cc`
**问题**: `BraftPartitionNode::Init()` 未设置 `log_uri`、`raft_meta_uri`、`snapshot_uri`。
**影响**: braft 可能不将日志持久化到磁盘，崩溃后日志丢失。
**修复建议**: 设置 `options.log_uri`、`raft_meta_uri`、`snapshot_uri` 指向 `data_path` 下的子目录。

---

## 3. P1 严重缺陷（影响可用性和正确性）

### 3.1 MetaD: 客户端无故障转移

**文件**: `src/dtx/grpc/meta_service_grpc.cc`
**问题**: `MetaServiceGrpcClient::Connect()` 只连接第一个地址，无重试/重定向。`TryReconnect()` 声明但未实现。
**影响**: 连接的 MetaD 节点故障后，客户端无法切换到其他节点。

### 3.2 MetaD: 领导变更回调未实现

**文件**: `src/dtx/raft/braft_node.cc`
**问题**: `SetLeaderChangeCallback()` 声明但未实现。`MetaRaftStateMachine::on_leader_start/stop` 只打日志，不调用 `meta_service_->OnBecomeLeader()/OnStepDown()`。
**影响**: MetaD 无法感知领导权变更，无法触发重新平衡等后台任务。

### 3.3 StorageD: GetLeaderId() 返回 nullopt

**文件**: `src/dtx/storage/braft_partition_raft.cc:339`
**问题**: 解析 leader 地址后无条件返回 `std::nullopt`。
**影响**: StorageD 的 "Not leader" 响应无法提供重定向地址，客户端不知道向谁重试。

### 3.4 GraphD/QueryD: 分布式读查询执行全 stub

**文件**: `src/service/graph_service_router.cc:714-725`, `src/queryd/distributed_executor.cpp`
**问题**: `ExecutePartitionQuery`、`ExecuteSinglePartition`、`ParallelExecutor::ExecuteParallel` 都是 TODO stub，不发送任何 RPC。
**影响**: 读查询返回空结果，系统无法查询实际数据。

### 3.5 GraphD/QueryD: StorageD 扫描/遍历 RPC 返回 NotSupported

**文件**: `src/queryd/query_storage_client.cpp`
**问题**: `ScanNode`、`ScanOutEdges`、`ScanInEdges` 都返回 `Status::NotSupported`。
**影响**: 无法执行范围扫描、邻域遍历等图查询核心操作。

### 3.6 StorageD: 无 WAL 恢复

**文件**: `src/dtx/storage_impl/partition_storage.cc`
**问题**: `WriteTxnWAL()` 写入二进制记录，但没有任何代码在重启时读取 WAL 恢复 prepared 事务。
**影响**: StorageD 重启后丢失所有 prepared 事务状态。

### 3.7 FailoverManager: 多处核心逻辑 TODO

**文件**: `src/dtx/storage/failover_manager.cc`
**问题**: 副本健康检查、租约续期、Leader 健康检查、服务重启、Leader 切换、副本提升都是 TODO。
**影响**: 自动故障转移完全不可用。

---

## 4. 可上线性评估

### 当前能做什么

| 场景 | 是否可用 | 说明 |
|------|----------|------|
| 启动 MetaD 3 节点集群 | ✅ | braft 选举、日志复制正常 |
| 启动 StorageD 并注册到 MetaD | ✅ | 心跳上报正常 |
| 单键 Put/Delete 复制 | ✅ | 通过 braft Propose，leader/follower 同步 |
| 2PC 分布式事务（写） | ⚠️ | 流程完整，但数据可能损坏（空 Descriptor）且不复制 |
| 读查询 | ❌ | 返回空结果或 NotSupported |
| 分区分配 | ❌ | CreateSpace 不分配分区 |
| 节点故障检测 | ❌ | HeartbeatCheckLoop 为空 |
| 快照恢复 | ❌ | 快照 stub，WAL 无恢复 |
| 自动故障转移 | ❌ | FailoverManager 全 stub |

### 结论

**历史结论：CedarGraph 在 2026-04-28 当时不可作为分布式数据库上线运行。**

虽然 Raft 共识层（braft）和 2PC 事务框架的骨架已经搭建完成，但以下核心能力缺失或损坏：

1. **元数据服务无法分配分区** — StorageD 不知道自己该服务哪些分区
2. **无故障检测和自动恢复** — 节点崩溃后系统不会感知或恢复
3. **快照和 WAL 恢复损坏/缺失** — 重启后状态丢失
4. **读查询无法执行** — GraphD/QueryD 到 StorageD 的 RPC 路径全 stub
5. **2PC 事务数据损坏** — Commit 丢失描述符

### 修复工作量估计

| 问题类别 | 预计工作量 | 优先级 |
|----------|-----------|--------|
| 快照序列化/恢复（MetaD + StorageD） | 2-3 天 | P0 |
| 分区分配算法 | 1-2 天 | P0 |
| 节点故障检测 + 重新分配 | 2-3 天 | P0 |
| 2PC Raft 复制 | 2-3 天 | P0 |
| 修复 write_descriptors bug | 0.5 天 | P0 |
| Raft 日志持久化配置 | 0.5 天 | P0 |
| 读查询 RPC 路径（GraphD/QueryD → StorageD） | 3-5 天 | P1 |
| 客户端故障转移/重定向 | 1-2 天 | P1 |
| WAL 恢复 | 1-2 天 | P1 |
| 自动故障转移（FailoverManager） | 3-5 天 | P1 |

**总计**: 约 **15-25 人天** 达到最小可上线状态（单空间、固定分区数、基础读写）。
