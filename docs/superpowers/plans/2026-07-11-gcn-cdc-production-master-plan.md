# GCN/CDC Production Integration Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy GCN as a usable, recoverable compute service backed by StorageD's durable CDC stream and verified through real multi-process tests.

**Architecture:** StorageD persists committed per-partition change records and exposes pull/snapshot RPCs. GCN leases partitions from MetaD, consumes changes with durable checkpoints, and serves TMV reads only at a proven version; GraphD dynamically discovers eligible GCNs and otherwise preserves full StorageD query semantics.

**Tech Stack:** C++17, Protobuf/gRPC, CedarGraph LSM/WAL and braft, GoogleTest/CTest, Docker Compose, Kubernetes/Kustomize, Helm, Bash/Ruby preflight scripts.

## Global Constraints

- StorageD remains the only authoritative graph-data source.
- CDC delivery is at least once; `(partition_id, offset)` application is idempotent.
- Aborted or uncommitted transactions never become visible CDC records.
- A GCN response is usable only when its partition epoch and served version satisfy the request.
- GCN remains optional; disabling or losing every GCN must preserve the StorageD path without a synthetic timeout.
- Production TLS/mTLS must not silently downgrade to plaintext.
- Existing `OnEventStream` remains wire-compatible but is not the production CDC path.
- Do not stage or overwrite the user's existing changes in `docs/architecture/README.md` or `docs/architecture/transaction-system.md`.
- No completion claim may rely on `BootstrapVertex` or test-only `EnqueueEvent` as the data source.

---

## Plan Set And Dependency Order

1. [StorageD durable CDC](2026-07-11-gcn-cdc-storage-plan.md)
   - Produces `ChangeRecord`, `ChangeLog`, `GetChangeLogState`, `FetchChanges`, and `GetComputeSnapshot`.
2. [GCN consumer and recovery](2026-07-11-gcn-cdc-consumer-plan.md)
   - Consumes phase 1 RPCs and produces durable checkpoints, backfill, readiness, and served-version reporting.
3. [MetaD leases and GraphD routing](2026-07-11-gcn-cdc-routing-plan.md)
   - Consumes GCN progress and produces dynamic discovery, lease epochs, version gating, and semantic fallback.
4. [Build and deployment integration](2026-07-11-gcn-cdc-deployment-plan.md)
   - Packages the four services in install, Docker, Compose, K8s, Helm, and blocking static preflights.
5. [Multi-process acceptance and recovery](2026-07-11-gcn-cdc-e2e-plan.md)
   - Proves the complete production path and failure semantics.

Each phase must pass its own tests before the next phase begins. A green earlier phase is not evidence that the overall objective is complete.

## Cross-Plan Interfaces

```cpp
namespace cedar::cdc {
struct ChangeRecord;
class PartitionChangeLog;
}  // namespace cedar::cdc

namespace cedar::gcn {
struct PartitionCheckpoint;
class CheckpointStore;
class StorageCdcClient;
class PartitionConsumer;
}  // namespace cedar::gcn
```

```proto
rpc GetChangeLogState(GetChangeLogStateRequest) returns (GetChangeLogStateResponse);
rpc FetchChanges(FetchChangesRequest) returns (FetchChangesResponse);
rpc GetComputeSnapshot(GetComputeSnapshotRequest) returns (stream ComputeSnapshotBatch);

rpc RegisterGcn(RegisterGcnRequest) returns (RegisterGcnResponse);
rpc RenewGcnLeases(RenewGcnLeasesRequest) returns (RenewGcnLeasesResponse);
rpc LocateGcn(LocateGcnRequest) returns (LocateGcnResponse);
```

## Global Verification Gate

- [ ] Configure the full test build.

Run: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON`

Expected: configuration exits `0` and generates all new test targets.

- [ ] Build all production binaries and focused tests.

Run: `cmake --build build --target metad storaged graphd graphcomputenode test_change_log test_storage_cdc_rpc test_gcn_cdc_consumer test_gcn_checkpoint test_meta_gcn_lease test_graphd_gcn_routing -j4`

Expected: every target builds successfully.

- [ ] Run focused CTest labels/names.

Run: `ctest --test-dir build --output-on-failure -R 'change_log|storage_cdc|gcn_cdc|gcn_checkpoint|meta_gcn_lease|graphd_gcn_routing'`

Expected: all selected tests pass.

- [ ] Run deployment and multi-process gates.

Run: `./scripts/preflight_gcn_cdc_e2e.sh`

Expected: real write, CDC persistence, GCN consumption, TMV query, restart recovery, and StorageD fallback all pass.

Run: `./scripts/preflight_docker_static.sh && ./scripts/preflight_k8s_static.sh && ./scripts/preflight_helm_static.sh`

Expected: all commands exit `0` and assert a real GCN workload/service rather than a string-only port reference.

- [ ] Run the repository release gate.

Run: `./scripts/preflight_release_gate.sh`

Expected: exits `0`; the GCN/CDC E2E step is blocking and is not configured with `continue-on-error`.

## Completion Audit

- [ ] Map every requirement in `docs/superpowers/specs/2026-07-11-gcn-cdc-production-design.md` to a passing test, rendered manifest, or runtime observation.
- [ ] Confirm `rg -n 'BootstrapVertex|EnqueueEvent' scripts/preflight_gcn_cdc_e2e.sh tests/end_to_end/test_gcn_cdc_cluster.cc` returns no production data-source usage.
- [ ] Confirm `git status --short` contains no accidental generated files and retains the user's pre-existing architecture-document edits.
- [ ] Record exact command output, commit SHA, runtime topology, and residual risks in the final handoff.
