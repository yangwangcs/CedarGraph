# CedarGraph-Core Production Blocking Fixes — Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement each sub-plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 10 P0 and 7 P1 production-blocking issues discovered in the 2026-06-10 distributed-systems audit so CedarGraph-Core meets minimum production deployment standards.

**Architecture:** The remediation is split into 4 independent sub-plans by subsystem. Sub-plans A (Security) and B (Storage Engine) are highest risk and should execute first. Sub-plans C (Distributed Consistency) and D (Resource Management) can follow in parallel. Each sub-plan produces independently buildable and testable changes.

**Tech Stack:** C++17, CMake, gRPC, protobuf, braft (vendored), googletest, pthread

---

## Audit Summary

| Severity | Count | Categories |
|----------|-------|------------|
| **P0 — Production Blocker** | 10 | Security bypass (3), Data atomicity (3), Consistency violations (3), Split-brain (1) |
| **P1 — High Risk** | 7 | Memory leaks (3), Resource exhaustion (2), Thread safety (2) |

**Full audit reports:** See previous agent summaries for `src/dtx/`, `src/storage/`, `src/governance/`, `src/transaction/`, `src/sst/`.

---

## Sub-Plan Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Master Plan                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  Sub-Plan A: Security Authentication Enforcement                            │
│  ├── P0-1: Wire AuthenticateAndAuthorize into ALL StorageService gRPC handlers│
│  ├── P0-2: Wire AuthenticateAndAuthorize into GraphServiceRouter            │
│  └── P0-3: Remove all gRPC insecure-fallback paths (fail hard on TLS error) │
├─────────────────────────────────────────────────────────────────────────────┤
│  Sub-Plan B: Storage Engine Atomicity & Isolation                           │
│  ├── P0-4: Atomic PutEdge (2PC batch or rollback journal)                   │
│  ├── P0-5: Fix BatchWrite stale-memtable-pointer capture                    │
│  └── P0-6: Fix Validate→Commit race (global commit lock)                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  Sub-Plan C: Distributed Consistency                                        │
│  ├── P0-7: Cross-DC sync replication cleanup failure handling               │
│  ├── P0-8: Bounded reconciliation queue with TTL eviction                   │
│  └── P0-9: Failover leader-transfer fencing (quorum verification)           │
├─────────────────────────────────────────────────────────────────────────────┤
│  Sub-Plan D: Resource Management & Stability                                │
│  ├── P1-1: Participant registry cleanup on Commit/Abort                     │
│  ├── P1-2: DTX RPC client OOM-safe promise handling                         │
│  ├── P1-3: HealthChecker thread-pool instead of std::async                  │
│  ├── P1-4: ServiceRegistry watcher exception safety                         │
│  ├── P1-5: ServiceDiscovery DNS use-after-free fix                          │
│  └── P1-6: Audit and document 28 disabled tests                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Dependency Graph

```
Sub-Plan A (Security) ───────────────┐
Sub-Plan B (Storage Engine) ─────────┤──→ All can be developed in parallel
Sub-Plan C (Distributed Consistency) ─┤    but A and B should merge first
Sub-Plan D (Resource Management) ────┘    (highest data-safety risk)
```

**Execution order recommendation:**
1. **Sub-Plan A + B first** — they fix security bypass and data-loss risks.
2. **Sub-Plan C + D in parallel** — they touch disjoint file sets.

---

## Cross-Cutting Testing Strategy

Every sub-plan must maintain:
- **Build:** `cd build && cmake --build . --target cedar_tests -j$(sysctl -n hw.ncpu)`
- **Unit tests:** `cd build && ctest --output-on-failure`
- **Security tests:** Run `test_security_blockers`, `test_jwt_json_parser` after Sub-Plan A
- **Chaos integration test (post Sub-Plan C):** `tests/end_to_end/chaos_failover_test.cc`

---

## Commit Message Convention

Prefix all commits with the sub-plan:
```
feat(security): enforce auth on all StorageService gRPC handlers
fix(storage): atomic PutEdge with rollback journal
fix(distributed): bounded reconciliation queue with TTL
fix(governance): thread-pool health checks + watcher exception safety
```

---

## Sub-Plan Files

| Sub-Plan | File | Scope |
|----------|------|-------|
| A | [`2026-06-10-subplan-a-security-auth.md`](./2026-06-10-subplan-a-security-auth.md) | Auth enforcement + TLS hardening |
| B | [`2026-06-10-subplan-b-storage-atomicity.md`](./2026-06-10-subplan-b-storage-atomicity.md) | PutEdge atomic + memtable fix + commit lock |
| C | [`2026-06-10-subplan-c-distributed-consistency.md`](./2026-06-10-subplan-c-distributed-consistency.md) | Cross-DC cleanup + queue bounds + failover fencing |
| D | [`2026-06-10-subplan-d-resource-management.md`](./2026-06-10-subplan-d-resource-management.md) | Registry cleanup + OOM safety + thread pools + DNS fix |

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-10-production-blocking-fixes-master-plan.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Use `superpowers:subagent-driven-development` skill.

**2. Inline Execution** — Execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints.

**Which approach?**
