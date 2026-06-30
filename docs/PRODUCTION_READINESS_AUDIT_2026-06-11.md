# CedarGraph-Core Production Readiness Audit

> **Historical archive note:** This audit records the repository state on 2026-06-11 and is not the current release verdict. Use `README.md`, `docs/user-manual/README.md`, and the latest `./scripts/preflight_release_gate.sh` result as the current readiness evidence.

**Date:** 2026-06-11
**Commit:** `2eef0fa`
**Test Status:** 1285/1285 passed (100%), 0 failed

---

## Executive Summary

| Category | Verdict | Blockers |
|----------|---------|----------|
| **Security & Auth** | ❌ FAIL | 3 |
| **Distributed Consensus** | ❌ FAIL | 2 |
| **Storage Durability** | ❌ FAIL | 1 |
| **Config & Deployment** | ❌ FAIL | 2 |
| **Monitoring & Alerting** | ❌ FAIL | 0 |
| **Test Coverage** | ✅ PASS | 0 |

**Overall: NOT PRODUCTION-READY**

There are **8 BLOCKER-level issues** that must be resolved before production deployment. An additional 6 HIGH-severity issues should be addressed in the first maintenance window after launch.

---

## BLOCKER Issues (Must Fix Before Production)

### B1. Unauthenticated Internal Services
**Category:** Security  
**Files:** `src/gcn/gcn_service.cc`, `src/dtx/grpc/meta_service_grpc.cc`, `src/dtx/dtx_service_impl.cc`  
**Severity:** BLOCKER

StorageD (18 handlers) and GraphD (11 handlers) correctly enforce `CheckAuth`. However, three critical internal services have **zero authentication** on any RPC handler:

- **GCN Service** (`gcn_service.cc:66,84,108,122`) — `Traverse`, `SubQuery`, `OnCacheInvalidate`, `OnEventStream`
- **MetaD gRPC** (`meta_service_grpc.cc:50,78,95,117,141,171,188,216,233,297,350,378,403,423,439`) — all 15 handlers including `CreateSpace`, `RegisterNode`, `Heartbeat`, `GetSchema`
- **DTX Service** (`dtx_service_impl.cc:70,116,198,233,265,296,338`) — `Replicate`, `Prepare`, `Commit`, `Abort`, `RegisterParticipant`

**Impact:** Any network-reachable attacker can create spaces, register nodes, commit/abort transactions, or replicate data without credentials.

**Fix:** Add `CheckAuth()` call as the first gate in every handler, identical to the pattern in `storage_service_impl.cc` and `graph_service_router.cc`.

---

### B2. JWT Timing Attack
**Category:** Security  
**File:** `src/dtx/security/security_manager.cc:583`  
**Severity:** BLOCKER

`ParseJWT` compares the HMAC signature with standard string inequality:

```cpp
if (encoded_signature != expected_encoded_sig) { return Status::InvalidArgument("Invalid signature"); }
```

This is **not a constant-time comparison**. The existing codebase already implements `VerifyPassword` with constant-time comparison (`security_manager.cc:419-424`), but JWT signature verification was missed.

**Impact:** An attacker can forge valid JWTs through statistically observable timing differences.

**Fix:** Replace `!=` with a constant-time comparison (e.g., `CRYPTO_memcmp` or the existing `ConstantTimeCompare` helper).

---

### B3. Unbounded User Inputs (DoS)
**Category:** Security  
**Files:** `src/service/graph_service_router.cc`, `src/dtx/storage_impl/storage_service_impl.cc`, `src/dtx/grpc/meta_service_grpc.cc`  
**Severity:** BLOCKER

No length/size bounds are enforced on:

- Query string length (`graph_service_router.cc:285`)
- Batch item count (`storage_service_impl.cc:671`)
- Parameter map size (`graph_service_router.cc:386`)
- Timeout values (`graph_service_router.cc:386`)
- `leader_partitions` / `follower_partitions` repeated field sizes (`meta_service_grpc.cc:199-204`)
- Node `address` and `data_path` strings (`meta_service_grpc.cc:171`)

**Impact:** A single malicious client can submit a multi-gigabyte batch request or a multi-megabyte query, causing OOM or excessive CPU consumption.

**Fix:** Add upper-bound validation at RPC entry points (e.g., `query.length() <= 1MB`, `items_size() <= 10_000`, `timeout_ms <= 300_000`).

---

### B4. Raft State Machine Divergence on I/O Error
**Category:** Distributed Consensus  
**File:** `src/dtx/storage/braft_partition_state_machine.cc:100-112`  
**Severity:** BLOCKER

`braft_partition_raft.cc` correctly calls `iter.set_error_and_rollback()` on apply failures, but `braft_partition_state_machine.cc` does **not** propagate this rollback:

```cpp
// braft_partition_state_machine.cc:100-112
if (op.type == kPut) {
    Status s = ApplyPut(...);
    if (!s.ok()) { LOG(ERROR) << ...; }  // Only logs, does NOT rollback
}
```

**Impact:** One replica advances `last_applied_index_` while others roll back, causing permanent state divergence across the Raft group.

**Fix:** On `ApplyPut`/`ApplyDelete` failure, call `iter.set_error_and_rollback()` and return immediately.

---

### B5. Stale Leader Can Serve Writes
**Category:** Distributed Consensus  
**File:** `src/dtx/storage_impl/storage_service_impl.cc:291,446,738,935,1070,1223`  
**Severity:** BLOCKER

Write paths (`Put`, `Delete`, `BatchPut`, `Prepare`, `Commit`, `Abort`) check `IsLeader()` but **do NOT check `IsLeaseValid()`**:

```cpp
if (!raft_group->IsLeader()) { return Status::NotLeader(...); }
// Missing: if (!raft_group->IsLeaseValid()) { return Status::NotLeader(...); }
```

Reads correctly check both via `CheckReadLeader()`.

**Impact:** A leader that has lost its lease (e.g., due to network partition) can still propose destructive writes, violating linearizability and potentially causing split-brain data corruption.

**Fix:** Add `IsLeaseValid()` check immediately after `IsLeader()` in all write handlers.

---

### B6. Compaction Deletes Files Before Manifest Save
**Category:** Storage Durability  
**File:** `src/storage/size_tiered_compaction.cc:837-849`  
**Severity:** BLOCKER

`DoZoneCompaction()` performs operations in this order:

1. Adds output SST to `levels_[output_level]`
2. **Deletes input SST physical files**
3. **Then** saves `MANIFEST-zc`

If the process crashes between step 2 and 3, the manifest still references deleted files. On restart, `LoadManifest()` loads metadata for non-existent files, causing query failures.

**Impact:** Crash during compaction can render the database unrecoverable without `RepairDB`.

**Fix:** Reorder: write output → update in-memory metadata → `SaveManifest()` → delete old files.

---

### B7. Docker Entrypoint Missing `queryd` Role
**Category:** Deployment  
**File:** `cedar-docker-compose/docker-entrypoint.sh`  
**Severity:** BLOCKER

The entrypoint script handles `metad`, `storaged`, and `graphd`, but **rejects `queryd`** with:

```
ERROR: Unknown or missing NODE_ROLE='queryd'.
```

The `docker-compose.yml` sets `NODE_ROLE=queryd`, so query containers immediately crash.

**Fix:** Add `queryd` case to the entrypoint script and update Dockerfile `HEALTHCHECK` to check port 9889.

---

### B8. K8s Network Policy Blocks Critical Ports
**Category:** Deployment  
**File:** `k8s/network-policy.yaml`  
**Severity:** BLOCKER

The network policy omits ports **9779** (storaged) and **9889** (queryd). If applied, it blocks all inter-pod traffic for storage and query services, rendering the cluster non-functional.

**Fix:** Add `9779` and `9889` to the ingress rules in `network-policy.yaml`, or remove the manifest if not actively maintained.

---

## HIGH Issues (Fix Before GA or in First Maintenance Window)

| ID | Issue | File | Impact |
|----|-------|------|--------|
| H1 | Participant registry leak on RPC failure | `dtx_service_impl.cc:250-253,280-284` | Unbounded memory growth |
| H2 | `Delete()` missing explicit WAL sync | `lsm_engine.cc:393-424` | Delete acks lost on crash |
| H3 | Config reload not race-safe | `cedar_config_manager.cc` | Readers see partially updated config |
| H4 | K8s PDB selector too broad | `k8s/pod-disruption-budget.yaml` | Single PDB matches all pods |
| H5 | RepairDB does not rebuild `MANIFEST-zc` | `graph_db.cc:112-284` | Compaction engine unaware of recovered SSTs |
| H6 | Alert rules reference non-existent metrics | `config/cedar_alerts.yml` | Alerts never fire |

---

## PASS Areas (Production-Ready)

| Area | Evidence |
|------|----------|
| **TLS enforcement** | Fail-hard everywhere; no insecure fallback (`grpc_tls.cc`) |
| **SecurityManager** | Proper singleton, config-gated auth (`security_manager.cc`) |
| **WAL durability** | CRC32 verified, group commit, replay on open (`wal.cc`) |
| **Raft snapshot** | Save/load tested; `FullSnapshotRoundTrip` passes |
| **Cross-DC rollback** | Best-effort compensation; correct error propagation |
| **Reconciliation queue** | TTL eviction, size bounding, seqno wraparound protection |
| **Helm chart** | Complete templates for all 4 components |
| **Test coverage** | 1285/1285 tests passing |

---

## Recommendations

1. **Do not deploy to production until all 8 BLOCKERs are resolved.**
2. **Prioritize B1 + B5** (unauthenticated services + stale leader writes) as they represent the highest risk of data corruption and unauthorized access.
3. **B6** (compaction ordering) should be validated with a crash-injection test before release.
4. After BLOCKERs are fixed, run a **chaos engineering suite** (random pod kills, network partitions, disk fills) to validate recovery paths.

---

*Generated by automated audit on 2026-06-11.*
