# Production Readiness — Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement each sub-plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remediate all 16 BLOCKER and 19+ CRITICAL issues discovered during the 4-dimension parallel deep audit (Security, Distributed Correctness, Operational Readiness, Stability) so CedarGraph-Core meets production deployment standards.

**Architecture:** The remediation is split into 4 independent phases. Phases 1–2 target data safety and consensus correctness (highest risk). Phase 3 hardens security boundaries. Phase 4 fixes operational and stability gaps. Each phase produces independently buildable and testable changes.

**Tech Stack:** C++17, CMake, gRPC, protobuf, braft (vendored), googletest, clang++

---

## Audit Summary

| Dimension | BLOCKERs | CRITICALs | Status |
|-----------|----------|-----------|--------|
| Security | 5 | 4 | Audited |
| Distributed Correctness | 5 | 3 | Audited |
| Operational Readiness | 6 | 5 | Audited |
| Stability | 3 | 7 | Audited |

**Full issue tracker:** See active-issues in the compacted conversation context.

---

## Phase Dependency Graph

```
Phase 1: Data Safety ──────────────────────┐
  ├─ Task 1.1 强制 TLS                         │
  ├─ Task 1.2 2PC 原子性修复                   │
  ├─ Task 1.3 FailoverManager Raft 选举       │
  └─ Task 1.4 LSM 稳定性（double-join/flush）  │
                                              │
Phase 2: Replication & Consistency ──────────┤
  ├─ Task 2.1 CrossDCReplicator 有序持久化     │ 可并行
  ├─ Task 2.2 PartitionStorage 原子批量写入    │
  ├─ Task 2.3 EventApplier 背压                │
  └─ Task 2.4 2PC 引擎/存储语义一致化          │
                                              │
Phase 3: Security Hardening ─────────────────┤
  ├─ Task 3.1 JWT 注入修复                     │ 可并行
  ├─ Task 3.2 RBAC 精确匹配                    │
  ├─ Task 3.3 审计日志安全                     │
  ├─ Task 3.4 错误消息脱敏                     │
  └─ Task 3.5 Prometheus 转义                  │
                                              │
Phase 4: Operational & Stability ────────────┘
  ├─ Task 4.1 信号安全
  ├─ Task 4.2 HealthChecker 线程池限流
  ├─ Task 4.3 ConfigManager mtime 热重载
  ├─ Task 4.4 结构化日志 + 轮转
  ├─ Task 4.5 K8s 探针/NetworkPolicy/PDB
  ├─ Task 4.6 默认值修复（/tmp, 超时, TLS）
  └─ Task 4.7 LSM/WAL 竞态修复（destructor/ForceFlush/WAL race）
```

**Execution order recommendation:**
1. **Phase 1 first** — it fixes data-loss and split-brain risks.
2. **Phase 2 and Phase 3 in parallel** — they touch disjoint file sets.
3. **Phase 4 last** — it depends on no other phase, but is lower risk.

---

## Sub-Plan Files

| Phase | File | Scope |
|-------|------|-------|
| Phase 1 | [`2026-05-20-phase-1-data-safety.md`](./2026-05-20-phase-1-data-safety.md) | 强制 TLS + 2PC 修复 + FailoverManager Raft + LSM 稳定性 |
| Phase 2 | [`2026-05-20-phase-2-replication-consistency.md`](./2026-05-20-phase-2-replication-consistency.md) | CrossDC 有序持久化 + 原子批量写入 + EventApplier 背压 |
| Phase 3 | [`2026-05-20-phase-3-security-hardening.md`](./2026-05-20-phase-3-security-hardening.md) | JWT + RBAC + 审计日志 + 错误脱敏 + Prometheus 转义 |
| Phase 4 | [`2026-05-20-phase-4-operational-stability.md`](./2026-05-20-phase-4-operational-stability.md) | 信号安全 + 健康检查 + 配置 + K8s + 默认值 + LSM/WAL 竞态 |

---

## Cross-Cutting Testing Strategy

Every phase must maintain:
- **Build:** `cd build && cmake --build . --target cedar_tests -j$(sysctl -n hw.ncpu)`
- **Unit tests:** `cd build && ctest --output-on-failure`
- **Sanitizer run (post-Phase 1):** Rebuild with `-fsanitize=thread` and run `cedar_tests`
- **Chaos integration test (post-Phase 2):** `tests/end_to_end/chaos_failover_test.cc` — network partition, leader kill

---

## Commit Message Convention

Prefix all commits with the phase:
```
feat(phase1): enforce TLS — remove insecure fallback
fix(phase1): 2PC abort-after-commit race
fix(phase2): CrossDCReplicator ordered delivery with seqno
refactor(phase3): replace ad-hoc JWT parser with nlohmann/json
chore(phase4): replace SIGKILL fallback with repeated SIGTERM
```

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-20-production-readiness-master-plan.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Use `superpowers:subagent-driven-development` skill.

**2. Inline Execution** — Execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints.

**Which approach?**
