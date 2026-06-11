# CedarGraph-Core 剩余已知限制修复 — Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement each sub-plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 CedarGraph-Core 剩余的 4 个已知功能限制，使数据库在功能完整性、查询能力和配置管理方面达到生产可用标准。

**Architecture:** 按功能领域分为 4 个独立子计划。E（查询优化）和 F（Cypher 子句）是查询引擎的核心扩展；G（累积刷新）和 H（配置解析）是存储和运维层的完善。每个子计划可独立构建和测试。

**Tech Stack:** C++17, CMake, gRPC, protobuf, braft (vendored), googletest, rapidjson (via brpc)

---

## 已知限制摘要

| # | 限制 | 影响 | 修复复杂度 | 子计划 |
|---|------|------|------------|--------|
| 1 | **无索引/查询优化器** | IndexScan 实为全表扫描+过滤，无 B-tree 索引；谓词下推无效 | 中-高（需新数据结构） | E |
| 2 | **缺失 Cypher 子句** | MERGE、WITH、UNWIND 未解析、未实现 | 中（需 parser + 算子） | F |
| 3 | **累积刷新对 GetRange 不可见** | `enable_accumulated_flush=true` 时范围扫描丢失数据 | 低（已有参考实现） | G |
| 4 | **配置解析器不完整** | 手写行解析器不支持嵌套、数组、JSON；Save/Load 不对称 | 中（替换为 rapidjson） | H |

---

## 依赖关系

```
Sub-Plan E (Query Optimizer) ───────────────┐
  ├─ 需要现有 IndexScan / PredicatePushdown 框架    │
  └─ 不依赖其他子计划                                │
Sub-Plan F (Cypher MERGE/WITH/UNWIND) ───────┤──→ 全部可并行
  ├─ 需要现有 CreateOperator / Project 模式        │
  └─ 不依赖其他子计划                                │
Sub-Plan G (Accumulated Flush Visibility) ───┤
  ├─ 仅需 LsmEngine 内部修改                        │
  └─ 不依赖其他子计划                                │
Sub-Plan H (Config Parser) ──────────────────┘
  ├─ 使用 rapidjson（brpc 自带）                   │
  └─ 不依赖其他子计划
```

**执行顺序：** 4 个子计划完全独立，可并行执行。

---

## Cross-Cutting Testing Strategy

每个子计划必须维持：
- **构建：** `cd build && cmake --build . --target cedar_tests -j$(sysctl -n hw.ncpu)`
- **单元测试：** `cd build && ctest --output-on-failure`
- **TDD 模式：** 先写失败测试 → 实现最小代码 → 测试通过 → 提交
- **提交前缀：**
  ```
  feat(query): add label/property index infrastructure
  feat(cypher): implement MERGE/WITH/UNWIND clauses
  fix(storage): include accumulated entries in GetRange/GetAll
  feat(config): replace hand-rolled parser with rapidjson
  ```

---

## Sub-Plan Files

| 子计划 | 文件 | 范围 |
|--------|------|------|
| E | [`2026-06-11-subplan-e-query-optimizer.md`](./2026-06-11-subplan-e-query-optimizer.md) | Label/Property 索引 + IndexScan 真实索引查找 |
| F | [`2026-06-11-subplan-f-cypher-clauses.md`](./2026-06-11-subplan-f-cypher-clauses.md) | MERGE / WITH / UNWIND parser + 算子 + 测试 |
| G | [`2026-06-11-subplan-g-accumulated-flush.md`](./2026-06-11-subplan-g-accumulated-flush.md) | GetRange/GetAll 包含 accumulated_entries_ |
| H | [`2026-06-11-subplan-h-config-parser.md`](./2026-06-11-subplan-h-config-parser.md) | rapidjson 替换手写解析器 + 完整 JSON round-trip |

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-11-remaining-limitations-master-plan.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Use `superpowers:subagent-driven-development` skill.

**2. Inline Execution** — Execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints.

**Which approach?**
