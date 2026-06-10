# CedarGraph-Core 代码完整性补全 — Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement each sub-plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补全 CedarGraph-Core 中所有 stub/未实现的关键路径，修复测试缺口，使项目从"研究原型"状态达到真正的 MVP 可运行状态。

**Architecture:** 按模块分解为 6 个独立的子计划。每个子计划聚焦一个关键缺口，产生可独立构建、测试和提交的更改。子计划之间通过共享接口松耦合，大部分可并行执行。

**Tech Stack:** C++17, CMake, gRPC, protobuf, braft (vendored), googletest, clang++

---

## 代码库现状摘要

| 维度 | 状态 | 关键问题 |
|------|------|----------|
| **构建** | ✅ 完整通过 | Apple Clang 17 + Linux GCC 11+ 均可编译 |
| **测试** | ✅ 763/763 通过 | 但 25 个 disabled、12 个孤儿测试未构建、49 个源文件零覆盖 |
| **Cypher 查询** | ⚠️ 只读可用 | CREATE/SET/DELETE 只解析不执行；无索引/优化器 |
| **Raft 共识** | ⚠️ 基本可用 | Snapshot save/load 在 legacy path 是 stub；ReadIndex 不支持 |
| **2PC 事务** | ⚠️ 核心可用 | RegisterParticipant 未实现；ClusterInitializer 模拟注册 |
| **存储引擎** | ⚠️ 核心可用 | CompactRange/ManifestCompaction/ RepairDB 为 stub；配置不可持久化 |
| **分布式查询** | ✅ 基本可用 | QueryD 分布式执行已实现，但无查询优化 |

**完整审计报告参考:**
- `PRODUCTION_READINESS_AUDIT_2026-05-20.md`
- `docs/superpowers/plans/2026-05-25-cedargraph-distributed-code-review.md`
- `docs/superpowers/plans/2026-05-25-cedargraph-distributed-code-review-zh.md`

---

## 子计划总览

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Master Plan                                  │
├─────────────────────────────────────────────────────────────────────┤
│  子计划 1: Cypher Write Operators                                   │
│  ├── CreateOperator, SetOperator, DeleteOperator                    │
│  ├── ExecutionPlanBuilder 集成                                      │
│  └── GraphServiceRouter 写路径路由                                  │
├─────────────────────────────────────────────────────────────────────┤
│  子计划 2: Raft Snapshot Completion                                 │
│  ├── StorageRaftStateMachine::on_snapshot_save (实际数据复制)       │
│  ├── StorageRaftStateMachine::on_snapshot_load (实际数据恢复)       │
│  └── Snapshot 集成测试                                              │
├─────────────────────────────────────────────────────────────────────┤
│  子计划 3: DTX Distributed Transaction Hardening                    │
│  ├── DTXServiceImpl::RegisterParticipant 实现                       │
│  ├── ClusterInitializer 真实 MetaService RPC 注册                   │
│  └── BraftPartitionNode::ReadIndex 实现或替代方案                   │
├─────────────────────────────────────────────────────────────────────┤
│  子计划 4: Test Coverage Restoration                                │
│  ├── 12 个孤儿测试文件加入 CMakeLists.txt 构建                      │
│  ├── 25 个 disabled 测试逐一分析修复                                │
│  └── 49 个零覆盖源文件的关键路径测试                                │
├─────────────────────────────────────────────────────────────────────┤
│  子计划 5: Query Optimization Infrastructure                        │
│  ├── IndexScan 物理算子 (Label/Property 索引)                       │
│  ├── 谓词下推 (Predicate Pushdown)                                  │
│  └── EXPLAIN 真实计划输出                                           │
├─────────────────────────────────────────────────────────────────────┤
│  子计划 6: Storage Engine Hardening                                 │
│  ├── CompactRange 实现                                              │
│  ├── ManifestCompaction 实现                                        │
│  ├── CedarConfig::SaveToFile 持久化                                 │
│  └── RepairDB 完整实现                                              │
└─────────────────────────────────────────────────────────────────────┘
```

### 依赖关系

| 子计划 | 依赖 | 可被依赖 |
|--------|------|----------|
| 1 (Cypher Write) | 无 | 无 |
| 2 (Raft Snapshot) | 无 | 无 |
| 3 (DTX Hardening) | 无 | 无 |
| 4 (Test Coverage) | 1,2,3,5,6 (修复后的测试) | 无 |
| 5 (Query Opt) | 无 | 无 |
| 6 (Storage) | 无 | 无 |

**执行顺序建议:**
1. **Phase A (并行):** 子计划 1, 2, 3, 5, 6 — 五个模块互相独立，可并行执行
2. **Phase B (依赖 Phase A):** 子计划 4 — 测试覆盖补全需要所有代码更改完成后统一验证

---

## Cross-Cutting 测试策略

每个子计划必须维持:
- **构建:** `cd build && cmake --build . --target cedar_tests -j$(sysctl -n hw.ncpu)`
- **单元测试:** `cd build && ctest --output-on-failure`
- **新增测试必须遵循 TDD:** 先写失败测试 → 实现最小代码 → 测试通过 → 提交
- **提交前缀:**
  ```
  feat(writeops): add CreateOperator for Cypher CREATE
  fix(raft): implement snapshot save/load with data directory copy
  feat(dtx): implement RegisterParticipant RPC
  test(coverage): enable orphan test files in CMakeLists
  feat(query): add IndexScan operator and predicate pushdown
  feat(storage): implement CompactRange and ManifestCompaction
  ```

---

## Sub-Plan Files

| 子计划 | 文件 | 范围 |
|--------|------|------|
| 子计划 1 | [`2026-05-26-subplan-1-cypher-write-operators.md`](./2026-05-26-subplan-1-cypher-write-operators.md) | CREATE/SET/DELETE 物理算子 + 执行计划集成 |
| 子计划 2 | [`2026-05-26-subplan-2-raft-snapshot.md`](./2026-05-26-subplan-2-raft-snapshot.md) | Raft snapshot 完整数据复制/恢复 |
| 子计划 3 | [`2026-05-26-subplan-3-dtx-hardening.md`](./2026-05-26-subplan-3-dtx-hardening.md) | RegisterParticipant + ClusterInitializer + ReadIndex |
| 子计划 4 | [`2026-05-26-subplan-4-test-coverage.md`](./2026-05-26-subplan-4-test-coverage.md) | 孤儿测试 + disabled 测试 + 缺失覆盖 |
| 子计划 5 | [`2026-05-26-subplan-5-query-optimization.md`](./2026-05-26-subplan-5-query-optimization.md) | 索引扫描 + 谓词下推 + EXPLAIN |
| 子计划 6 | [`2026-05-26-subplan-6-storage-hardening.md`](./2026-05-26-subplan-6-storage-hardening.md) | CompactRange + ManifestCompaction + Config + RepairDB |

---

## 执行交接

**Plan complete and saved to `docs/superpowers/plans/2026-05-26-cedargraph-completion-master-plan.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — 每个子计划分配一个独立 subagent，task-by-task 执行，每步后 review。使用 `superpowers:subagent-driven-development` skill。

**2. Inline Execution** — 在当前 session 中使用 `superpowers:executing-plans` skill，批量执行带 checkpoint。

**Which approach?**
