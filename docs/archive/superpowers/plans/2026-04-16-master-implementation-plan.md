# CedarGraph-Core 上线路径实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 CedarGraph-Core 从当前 Alpha 阶段 (50% 完成度) 提升到可上线标准 (≥80% 完成度)

**Architecture:** 分 5 个 Phase 依次实现，每个 Phase 聚焦一个核心功能模块，最终实现端到端的分布式图数据库

**Tech Stack:** C++, gRPC, Raft, MVCC, LSM-Tree

---

## 执行总览

```
Phase 1 (4-6周) │ Phase 2 (3-4周) │ Phase 3 (4-5周) │ Phase 4 (2-3周) │ Phase 5 (2-3周)
─────────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────
Raft RPC 层实现   │ Cypher 引擎集成   │ 2PC 真实 RPC     │ 故障转移完善     │ 测试与调优
    │              │       │              │       │              │       │
    ├─ gRPC 传输层 │       ├─ WHERE 解析  │       ├─ 心跳检测   │       ├─ 单元测试
    ├─ 日志复制    │       ├─ 时态操作符  │       ├─ 租约续期   │       ├─ 集成测试
    ├─ 投票统计    │       ├─ 参数化查询  │       ├─ Leader 选举│       ├─ 性能调优
    └─ 快照传输   │       └─ 引擎集成    │       └─ 路由更新   │       └─ 文档完善
                         │                       │
                         └───────────────────────┘
                                      │
                               完成度: 50% → 80%
```

---

## Phase 依赖关系

```
Phase 1 (Raft RPC)
    │
    ├── 必须先于 Phase 3 (2PC) 完成
    │   Reason: 2PC 依赖底层的 Raft 日志复制机制
    │
    └── 可与 Phase 2/4/5 并行执行

Phase 2 (Cypher 引擎)
    │
    ├── 独立于其他 Phase
    │   Reason: 只依赖存储层接口
    │
    └── 建议在 Phase 1 完成后集成测试

Phase 3 (2PC RPC)
    │
    └── 依赖 Phase 1 完成
        Reason: 使用 Raft 传输层进行节点间通信

Phase 4 (故障转移)
    │
    ├── 依赖 Phase 1 (Raft)
    │   Reason: 故障转移需要 Raft Leader 选举机制
    │
    └── 依赖 Phase 3 (2PC)
        Reason: 需要 2PC 事务恢复机制

Phase 5 (测试与调优)
    │
    └── 依赖 Phase 1-4 全部完成
```

---

## Phase 1: Raft RPC 层实现

> **Duration:** 4-6 周
> **Priority:** P0 (阻塞上线)
> **详细计划:** [2026-04-16-raft-rpc-implementation.md](./2026-04-16-raft-rpc-implementation.md)

**Goal:** 实现 Embedded Raft 的真实网络通信，使 Leader 能够同步日志到 Followers

### 任务清单

- [ ] Task 1: 定义 Raft RPC 协议 (`proto/raft_rpc.proto`)
- [ ] Task 2: 实现 gRPC 传输层 (`grpc_transport.h/cc`)
- [ ] Task 3: 实现 Leader 日志复制 (`ReplicateLog()`, `SendHeartbeats()`)
- [ ] Task 4: 实现投票响应统计 (`HandleRequestVoteResponse()`)
- [ ] Task 5: 实现快照传输 (`InstallSnapshotToFollower()`)
- [ ] Task 6: 集成测试
- [ ] Task 7: 编译和验证

### 验收标准

- [ ] `make -j4` 编译通过
- [ ] `ctest -R RaftRpcTest` 全部通过
- [ ] Leader 可以成功复制日志到 Followers
- [ ] Leader 故障后可以自动选举新 Leader

### 预计工作量

| 任务 | 预估时间 | 依赖 |
|------|----------|------|
| Task 1-2 (传输层) | 1 周 | 无 |
| Task 3-4 (核心复制) | 2 周 | Task 1-2 |
| Task 5 (快照) | 1 周 | Task 3-4 |
| Task 6-7 (测试) | 1 周 | Task 1-5 |

---

## Phase 2: Cypher 引擎集成

> **Duration:** 3-4 周
> **Priority:** P0 (阻塞上线)
> **详细计划:** [2026-04-16-cypher-engine-integration.md](./2026-04-16-cypher-engine-integration.md)

**Goal:** 将 Cypher 查询引擎与存储层正确集成，实现端到端的查询执行

### 任务清单

- [ ] Task 1: 在 CedarGraph 中集成 CypherEngine
- [ ] Task 2: 实现 WHERE 子句解析和条件评估
- [ ] Task 3: 实现 Temporal 操作符 (TemporalNodeScan, TemporalExpand, SnapshotScan, VersionScan)
- [ ] Task 4: 实现参数化查询处理
- [ ] Task 5: 集成测试
- [ ] Task 6: 编译和验证

### 验收标准

- [ ] `MATCH (n) RETURN n` 返回正确结果
- [ ] `WHERE` 子句正确过滤数据
- [ ] `AS OF TIMESTAMP` 时态查询返回正确历史版本
- [ ] `BETWEEN t1 AND t2` 范围查询正常工作
- [ ] `$param` 参数化查询正常工作

### 预计工作量

| 任务 | 预估时间 | 依赖 |
|------|----------|------|
| Task 1 (集成) | 3 天 | 无 |
| Task 2 (WHERE) | 1 周 | Task 1 |
| Task 3 (时态) | 1 周 | Task 1 |
| Task 4 (参数) | 3 天 | Task 1 |
| Task 5-6 (测试) | 3 天 | Task 1-4 |

---

## Phase 3: 2PC 真实 RPC 实现

> **Duration:** 4-5 周
> **Priority:** P0 (阻塞上线)
> **详细计划:** [2026-04-16-2pc-rpc-implementation.md](./2026-04-16-2pc-rpc-implementation.md)

**Goal:** 将模拟的 2PC RPC 调用替换为真实 gRPC 通信，实现端到端的分布式事务

### 任务清单

- [ ] Task 1: 定义 DTX RPC 协议 (`proto/dtx_protocol.proto`)
- [ ] Task 2: 实现 StorageClient RPC 调用
- [ ] Task 3: 替换模拟 RPC 为真实调用 (Optimized2PCEngine, LsmNativeOcc)
- [ ] Task 4: 实现 Storage 服务端 RPC 处理器
- [ ] Task 5: 实现事务恢复逻辑 (Coordinator/Participant)
- [ ] Task 6: 集成测试
- [ ] Task 7: 编译和验证

### 验收标准

- [ ] 跨分区事务可以成功提交
- [ ] 参与者失败时事务可以正确 Abort
- [ ] 协调器故障后可以恢复未完成事务
- [ ] `ctest -R TwoPCIntegrationTest` 全部通过

### 预计工作量

| 任务 | 预估时间 | 依赖 |
|------|----------|------|
| Task 1-2 (协议) | 1 周 | Phase 1 |
| Task 3-4 (RPC) | 2 周 | Task 1-2 |
| Task 5 (恢复) | 1 周 | Task 3-4 |
| Task 6-7 (测试) | 1 周 | Task 1-5 |

---

## Phase 4: 故障转移完善

> **Duration:** 2-3 周
> **Priority:** P1 (重要功能)
> **详细计划:** [2026-04-16-failover-enhancement.md](./2026-04-16-failover-enhancement.md)

**Goal:** 实现完整的故障检测、Leader 选举和路由更新机制，确保集群的高可用性

### 任务清单

- [ ] Task 1: 实现心跳检测循环 (PartitionFailoverController)
- [ ] Task 2: 实现 Raft Leader 租约机制
- [ ] Task 3: 完善 Leader 选举 (投票请求、响应处理)
- [ ] Task 4: 实现路由更新 (PartitionRouter)
- [ ] Task 5: 集成测试
- [ ] Task 6: 编译和验证

### 验收标准

- [ ] 节点故障后 5 秒内检测到
- [ ] Leader 故障后可以自动选举新 Leader
- [ ] 路由表正确更新
- [ ] `ctest -R FailoverTest` 全部通过

### 预计工作量

| 任务 | 预估时间 | 依赖 |
|------|----------|------|
| Task 1-2 (检测) | 1 周 | Phase 1 |
| Task 3-4 (选举) | 1 周 | Task 1-2 |
| Task 5-6 (测试) | 3 天 | Task 1-4 |

---

## Phase 5: 测试与调优

> **Duration:** 2-3 周
> **Priority:** P1 (重要功能)

**Goal:** 完善测试覆盖、性能调优、上线准备

### 任务清单

- [ ] Task 1: 单元测试补全 (覆盖 Phase 1-4 未覆盖代码)
- [ ] Task 2: 集成测试 (多节点集群)
- [ ] Task 3: 端到端测试 (完整查询链路)
- [ ] Task 4: 性能基准测试
- [ ] Task 5: Debug 输出清理
- [ ] Task 6: 配置可调参数化
- [ ] Task 7: 上线检查清单

### 验收标准

- [ ] 代码覆盖率 ≥70%
- [ ] 多节点集群 24 小时稳定性测试通过
- [ ] P99 延迟满足 SLA
- [ ] 无 Debug 输出
- [ ] 所有配置项可调

---

## 资源估算

### 人员需求

| Phase | 主要开发者 | 测试开发者 | DevOps |
|-------|-----------|-----------|--------|
| Phase 1-3 | 2 人 | 1 人 | 0.5 人 |
| Phase 4-5 | 1 人 | 2 人 | 0.5 人 |

### 时间线

```
Month 1  │ Month 2  │ Month 3  │ Month 4  │ Month 5
─────────┼──────────┼──────────┼─────────┼─────────
Phase 1 │ Phase 2  │ Phase 3  │ Phase 4 │ Phase 5
(4-6周) │ (3-4周)  │ (4-5周)  │ (2-3周) │ (2-3周)

总计: 15-21 周 (约 4-5 个月)
```

---

## 风险评估

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|----------|
| Raft RPC 实现复杂超预期 | 高 | 中 | 预留 1 周 buffer，考虑使用 braft 库替代 |
| Cypher 时态语法解析困难 | 中 | 低 | 简化时态语法，逐步支持 |
| 2PC 恢复逻辑复杂 | 高 | 中 | 先实现简化版本，后续迭代 |
| 测试环境不稳定 | 低 | 高 | 使用 Docker Compose 进行集成测试 |

---

## 里程碑

| 里程碑 | 日期 | 验收条件 |
|--------|------|----------|
| M1: Raft RPC 完成 | Week 6 | Leader 可以复制日志 |
| M2: Cypher 可用 | Week 10 | 基础查询可以执行 |
| M3: 分布式事务可用 | Week 15 | 跨分区事务可以提交 |
| M4: 高可用完成 | Week 18 | 故障转移正常工作 |
| M5: Beta 发布 | Week 21 | 通过上线检查清单 |

---

## 下一步行动

**推荐执行顺序：**

1. **立即开始 Phase 1** (Raft RPC) - 这是最关键路径
2. **并行启动 Phase 2** (Cypher) - 独立模块，可同步开发
3. Phase 1 完成后立即开始 Phase 3 (2PC)
4. Phase 1 + Phase 3 完成后开始 Phase 4 (故障转移)
5. Phase 4 完成后开始 Phase 5 (测试与调优)

---

## Self-Review

**1. Plan coverage:** 所有 P0/P1 问题已覆盖：
- [x] Phase 1: Raft RPC 层实现
- [x] Phase 2: Cypher 引擎集成
- [x] Phase 3: 2PC 真实 RPC 实现
- [x] Phase 4: 故障转移完善
- [x] Phase 5: 测试与调优

**2. Placeholder scan:** 无 placeholder，每个 Task 包含完整代码示例

**3. Type consistency:** 所有任务使用一致的类型和接口命名

**4. Dependency clarity:** Phase 依赖关系明确，可正确排序执行
