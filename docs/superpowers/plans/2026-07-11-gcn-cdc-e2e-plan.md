# GCN CDC Multi-Process Acceptance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the deployed MetaD, StorageD, GCN, and GraphD path works with real writes, restart recovery, fallback, backfill, and leader changes.

**Architecture:** A dedicated harness starts real production binaries on isolated ports and temporary persistent directories, performs client-visible operations, observes MetaD/StorageD/GCN state through supported APIs, injects process failures, and treats every failed assertion as a blocking exit.

**Tech Stack:** C++ integration test client, Bash process harness, CTest, Docker Compose, existing preflight release gate.

## Global Constraints

- No test-only injection into TMV or GcnService queues.
- All readiness waits are condition based and bounded; no correctness assertion relies on arbitrary sleeps.
- Every child process is terminated and every temporary directory cleaned on success or failure.
- Tests use production TLS/auth mode where the corresponding release gate claims production coverage.
- Results must compare full path contents, not only node counts.

---

### Task 1: Build A Real Cluster Test Client

**Files:**
- Create: `tests/end_to_end/gcn_cdc_cluster_client.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces: CLI actions `write`, `wait-gcn`, `traverse`, `cdc-state`, and `assert-route` used by the shell harness.

- [ ] **Step 1: Write the client contract test**

Add a CTest that invokes `gcn_cdc_cluster_client --help` and asserts all commands are present. The client must use public gRPC APIs only.

- [ ] **Step 2: Build and verify failure**

Run: `cmake --build build --target gcn_cdc_cluster_client -j4`

Expected: FAIL because the target is missing.

- [ ] **Step 3: Implement deterministic commands and machine-readable output**

```text
write --graphd HOST:PORT --fixture chain --version-out FILE
wait-gcn --metad HOST:PORT --partition N --min-version V --timeout-ms N
traverse --graphd HOST:PORT --root ID --depth N --required-version V --expect-path 42,100,200
cdc-state --storaged HOST:PORT --partition N
assert-route --metad HOST:PORT --partition N --min-version V
```

Print one JSON object to stdout and nonzero exit on an unmet assertion. Reuse strict TLS/auth flags from existing test clients.

- [ ] **Step 4: Run help and argument validation tests**

Run: `cmake --build build --target gcn_cdc_cluster_client -j4 && ctest --test-dir build --output-on-failure -R gcn_cdc_cluster_client`

Expected: all client contract tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/end_to_end/gcn_cdc_cluster_client.cc tests/CMakeLists.txt
git commit -m "test(gcn): add production cluster client"
```

### Task 2: Add The Blocking Multi-Process Harness

**Files:**
- Create: `scripts/preflight_gcn_cdc_e2e.sh`
- Modify: `scripts/preflight_manifest_syntax.sh`
- Modify: `scripts/preflight_release_gate.sh`

**Interfaces:**
- Consumes: four production binaries and `gcn_cdc_cluster_client`.
- Produces: a blocking end-to-end gate.

- [ ] **Step 1: Add failing manifest assertions for the new blocking gate**

```ruby
release_gate = File.read('scripts/preflight_release_gate.sh')
abort('release gate must run GCN CDC E2E') unless
  release_gate.include?('preflight_gcn_cdc_e2e.sh')
```

Also reject patterns that wrap this step in `continue-on-error`, `|| true`, or an optional skip enabled by default.

- [ ] **Step 2: Run and verify failure**

Run: `./scripts/preflight_manifest_syntax.sh`

Expected: FAIL with `release gate must run GCN CDC E2E`.

- [ ] **Step 3: Implement condition-driven process orchestration**

The script creates isolated ports/directories, generates test TLS material using the existing helper, starts one MetaD, one StorageD, one GCN, and one GraphD, and registers a cleanup trap. Poll supported health/metadata RPCs until ready. Then:

1. write a deterministic two-hop graph through GraphD;
2. read StorageD CDC state and assert high watermark advanced;
3. wait until MetaD reports GCN applied version;
4. traverse through GraphD and assert full expected paths;
5. stop GCN, assert immediate StorageD fallback returns the same paths;
6. restart GCN with the same data directory and assert checkpoint continuation;
7. restart with an expired checkpoint fixture and assert snapshot backfill;
8. trigger supported StorageD leader-change mode and assert CDC continuation.

- [ ] **Step 4: Run the harness repeatedly**

Run: `for i in 1 2 3; do ./scripts/preflight_gcn_cdc_e2e.sh || exit 1; done`

Expected: all three runs pass without leaked processes or reused ports.

Run: `./scripts/preflight_manifest_syntax.sh`

Expected: exits `0` and proves the gate is blocking.

- [ ] **Step 5: Commit**

```bash
git add scripts/preflight_gcn_cdc_e2e.sh scripts/preflight_manifest_syntax.sh scripts/preflight_release_gate.sh
git commit -m "test(gcn): gate the real CDC cluster path"
```

### Task 3: Add Focused Fault And Recovery Tests

**Files:**
- Create: `tests/end_to_end/test_gcn_cdc_cluster.cc`
- Modify: `tests/CMakeLists.txt`
- Modify: `scripts/preflight_gcn_cdc_e2e.sh`

**Interfaces:**
- Produces: automated assertions for duplicate delivery, corruption, lag, stale epoch, and snapshot recovery.

- [ ] **Step 1: Write failing test cases around production APIs and fault hooks**

```cpp
TEST_F(GcnCdcClusterTest, DuplicateDeliveryDoesNotDuplicateEdges);
TEST_F(GcnCdcClusterTest, VersionLagFallsBackWithoutReturningStaleData);
TEST_F(GcnCdcClusterTest, StaleLeaseOwnerStopsServing);
TEST_F(GcnCdcClusterTest, ExpiredOffsetRebuildsFromSnapshot);
TEST_F(GcnCdcClusterTest, CorruptCheckpointTriggersRebuild);
TEST_F(GcnCdcClusterTest, MiddleSegmentCorruptionFailsStoragePartition);
```

Use explicit test-only fault RPCs or startup flags guarded by `CEDAR_TESTING`; do not mutate private files while a process is running unless the test specifically validates crash recovery.

- [ ] **Step 2: Build and verify failures**

Run: `cmake --build build --target test_gcn_cdc_cluster -j4 && ./build/tests/test_gcn_cdc_cluster`

Expected: the new scenarios fail until their production handling exists.

- [ ] **Step 3: Complete missing fault handling one behavior at a time**

For each failing scenario, make the smallest production change in its owning module, rerun only that scenario, and commit it separately. Never weaken expected versions, path equality, or corruption behavior to make the test green.

- [ ] **Step 4: Run focused and full GCN/CDC suites**

Run: `ctest --test-dir build --output-on-failure -R 'change_log|storage_cdc|gcn_cdc|gcn_checkpoint|meta_gcn|graphd_gcn|gcn_cdc_cluster'`

Expected: all selected tests pass.

- [ ] **Step 5: Commit the acceptance suite**

```bash
git add tests/end_to_end/test_gcn_cdc_cluster.cc tests/CMakeLists.txt scripts/preflight_gcn_cdc_e2e.sh
git commit -m "test(gcn): cover CDC recovery failures"
```

### Task 4: Run The Completion Audit

**Files:**
- Create: `docs/verification/gcn-cdc-production-verification.md`
- Modify only if evidence exposes a defect: files owned by earlier tasks.

**Interfaces:**
- Produces: requirement-by-requirement evidence for the design completion criteria.

- [ ] **Step 1: Run build, focused tests, E2E, deployment renders, and release gate**

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON
cmake --build build -j4
ctest --test-dir build --output-on-failure -R 'change_log|storage_cdc|gcn_cdc|gcn_checkpoint|meta_gcn|graphd_gcn|gcn_cdc_cluster'
./scripts/preflight_gcn_cdc_e2e.sh
./scripts/preflight_docker_static.sh
./scripts/preflight_k8s_static.sh
./scripts/preflight_helm_static.sh
./scripts/preflight_release_gate.sh
```

Expected: every command exits `0`.

- [ ] **Step 2: Inspect authoritative runtime and manifest evidence**

Record process versions, MetaD GCN routes, StorageD watermarks, GCN checkpoints/applied versions, GraphD route/fallback counters, rendered K8s/Helm resources, Docker binary inventory, and all command exit codes.

- [ ] **Step 3: Write the verification matrix**

For every item in design section 17, mark `PROVEN` only with a direct command/test/runtime reference. Mark missing or indirect evidence `NOT PROVEN` and return to its owning task; do not publish a completion statement while any item remains unproven.

- [ ] **Step 4: Verify the verification document and worktree**

Run: `rg -n 'NOT PROVEN|PLACEHOLDER|INCOMPLETE' docs/verification/gcn-cdc-production-verification.md`

Expected: no matches at final completion.

Run: `git diff --check && git status --short`

Expected: no unintended generated files; the user's pre-existing architecture-document edits remain intact.

- [ ] **Step 5: Commit verification evidence**

```bash
git add docs/verification/gcn-cdc-production-verification.md
git commit -m "docs: verify production GCN CDC integration"
```
