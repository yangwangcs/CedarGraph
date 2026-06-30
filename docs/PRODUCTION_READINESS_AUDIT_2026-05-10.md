# CedarGraph Production Readiness Audit

> **Historical archive note:** This audit records the repository state on 2026-05-10 and is not the current release verdict. Use `README.md`, `docs/user-manual/README.md`, and the latest `./scripts/preflight_release_gate.sh` result as the current readiness evidence.

**Date:** 2026-05-10
**Auditor:** Automated readiness check plan
**Commit:** 44a57dea9e210e73ee81507482e880e0a9713c4f

## Executive Summary

| Area | Status | Notes |
|------|--------|-------|
| Compilation | PASS | All 5 primary targets built with 0 errors |
| Unit Tests (enabled) | 384/384 passed | 23 intentionally disabled, 3 placeholders |
| Unit Tests (re-enabled) | 16/16 passed | test_cedar_graph_storage (11/11), test_end_to_end_partition (5/5) |
| Critical Stubs | CLEAN with 1 concern | Legacy binary has empty write_descriptors; main path correct |
| 2PC Prepare Path | PASS (main) / FAIL (legacy) | StorageServiceImpl correct; storage_server_with_grpc.cc broken |
| Raft Consensus | PASS | braft integration active, state machine applies real logs |
| Cross-DC Replication | PASS | Wired in StorageServer, 6/6 tests pass |
| Query Execution | PASS with concern | DistributedExecutor real; IsSinglePartitionQuery always false |
| Configuration | COMPLETE | cedar.yaml has all required sections |
| Deployment | PARTIAL | K8s graphd path bug blocking; missing queryd in K8s/Compose/Helm |

**Overall Verdict:** GO_WITH_CAVEATS

## Detailed Findings

### Blocking Issues (Must Fix Before Production)

1. **K8s graphd.yaml uses wrong binary path**
   - Location: `k8s/graphd.yaml` (container command)
   - Description: The manifest specifies `/bin/graphd` but the Dockerfile installs the binary to `/usr/local/bin/cedar-graphd`. This will cause CrashLoopBackOff on deployment.
   - Fix: Change command to `/usr/local/bin/cedar-graphd` or create a symlink in the Dockerfile.

2. **Legacy storage_server_with_grpc.cc drops 2PC write descriptors**
   - Location: `src/dtx/storage/storage_server_with_grpc.cc:609-611`
   - Description: The standalone legacy binary passes an empty `write_descriptors` map to `partition->Prepare()`, ignoring `request->write_descriptors()` from the proto. This causes silent data loss for 2PC transactions using this binary.
   - Fix: Backport the proto-to-native descriptor conversion from `storage_service_impl.cc:762-839`, or deprecate and remove this binary.
   - Note: The main production binary (`cedar-storaged`) is NOT affected.

### Non-Blocking Gaps (Acceptable for Initial Release)

1. **IsSinglePartitionQuery always returns false**
   - Location: `src/queryd/distributed_executor.cpp:765-783`
   - Description: All queries are routed through the cross-partition parallel executor, adding unnecessary RPC overhead for single-partition queries.
   - Impact: Performance degradation for point lookups and single-partition traversals.
   - Fix: Implement entity-ID extraction from AST to enable the fast path.

2. **Missing queryd service in deployment artifacts**
   - Location: `k8s/`, `cedar-docker-compose/`, `helm-chart/`
   - Description: QueryD is not defined in K8s manifests, Docker Compose, or Helm chart.
   - Impact: Query routing must be done via GraphD directly or external load balancer.
   - Fix: Add queryd.yaml, queryd service in docker-compose.yml, and queryd templates in Helm.

3. **K8s image naming inconsistency**
   - Location: `k8s/graphd.yaml` vs `k8s/metad.yaml`/`k8s/storaged.yaml`
   - Description: graphd uses `cedargraph/cedar:latest`; others use `cedargraph:latest`. Kustomize image rewriting only matches `cedargraph`.
   - Impact: graphd image tag won't be rewritten by Kustomize.
   - Fix: Normalize all images to `cedargraph:latest` or add a second Kustomize image entry.

4. **Missing ConfigMaps for storaged and graphd**
   - Location: `k8s/`
   - Description: Only `metad-config` ConfigMap exists.
   - Impact: storaged/graphd must rely on default configs or command-line args.
   - Fix: Add `storaged-config` and `graphd-config` ConfigMaps.

5. **Disabled core tests missing source files**
   - Location: `tests/cluster/`, `tests/`
   - Description: `test_partition_router`, `test_partition_raft_manager`, `test_storage_interface` source files do not exist.
   - Impact: Critical distributed paths (partition routing, raft manager, storage interface) lack automated coverage.
   - Fix: Create the missing test source files or remove the commented CMake entries.

6. **Docker Compose health checks are port-only**
   - Location: `cedar-docker-compose/docker-compose.yml`
   - Description: All healthchecks use `nc -z` which only checks TCP port listening, not application readiness.
   - Impact: A hung process that still accepts TCP connections will appear healthy.
   - Fix: Use the `cedar_health_check.sh` script for deeper health validation.

7. **DTXService 2PC methods return UNIMPLEMENTED**
   - Location: `src/dtx/dtx_service_impl.cc:102-135`
   - Description: Prepare/Commit/Abort/Inquire return UNIMPLEMENTED.
   - Impact: None — this is by design. The active 2PC path is through StorageService, not DTXService.
   - Note: DTXService's Replicate/ApplyReplication (cross-DC) are fully implemented.

### Tests Re-enabled and Results

| Test | Source Exists | Build Status | Test Status |
|------|--------------|--------------|-------------|
| test_partition_router | NO | N/A | N/A |
| test_partition_raft_manager | NO | N/A | N/A |
| test_storage_interface | NO | N/A | N/A |
| test_cedar_graph_storage | YES | BUILT | 11/11 PASSED |
| test_end_to_end_partition | YES | BUILT | 5/5 PASSED |

## Recommendations

1. **Fix K8s graphd binary path before any deployment.** This is a one-line change with immediate impact.
2. **Deprecate or fix `storage_server_with_grpc.cc`.** If the legacy binary is still used anywhere, backport the write_descriptors fix. If not, remove it to prevent accidental use.
3. **Add queryd to all deployment artifacts** (K8s, Docker Compose, Helm) to enable full query routing.
4. **Implement `IsSinglePartitionQuery` fast path** to reduce RPC overhead for simple queries.
5. **Create missing test source files** for partition router, raft manager, and storage interface to close the coverage gap.
6. **Normalize K8s image names** so Kustomize rewriting works for all services.
