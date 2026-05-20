# CedarGraph Distributed System Readiness Check

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Systematically validate CedarGraph's distributed system components to determine production-readiness and produce a go/no-go recommendation.

**Architecture:** Run a multi-phase validation: (1) compilation sanity, (2) unit test execution, (3) critical code-path audit, (4) disabled test re-enablement and execution, (5) configuration/deployment validation, (6) final report.

**Tech Stack:** C++17, CMake, ctest, gRPC, braft, googletest

---

## File Structure

No new files created. This plan modifies:
- `tests/CMakeLists.txt` — re-enable disabled tests
- `docs/PRODUCTION_READINESS_AUDIT_2026-05-10.md` — final report (generated)

Files read/verified:
- `src/dtx/storage_impl/storage_service_impl.cc` — 2PC Prepare path
- `src/dtx/storage/storage_server_with_grpc.cc` — standalone binary 2PC path
- `src/queryd/distributed_executor.cpp` — `IsSinglePartitionQuery`
- `tests/CMakeLists.txt` — test enablement status
- `config/cedar.yaml`, `k8s/*.yaml`, `cedar-docker-compose/docker-compose.yml` — deployment configs

---

## Task 1: Compilation Sanity Check

**Files:**
- Read: `CMakeLists.txt`

- [ ] **Step 1: Clean build all primary targets**

```bash
cd build && rm -rf CMakeCache.txt CMakeFiles && cmake .. && make cedar cedar_queryd graphd metad storaged -j4 2>&1 | tee /tmp/build.log
```

Expected: `[100%] Built target cedar`, `[100%] Built target cedar_queryd`, etc. Zero errors from Cedar sources.

- [ ] **Step 2: Verify no linker errors**

```bash
grep -i "error:" /tmp/build.log | grep -v "third_party" | grep -v "warning:" | head -20
```

Expected: Empty output (no Cedar-side compilation/linker errors).

- [ ] **Step 3: Commit build status note**

```bash
git commit --allow-empty -m "check(readiness): Task 1 compilation sanity passed"
```

---

## Task 2: Run All Enabled Unit Tests

**Files:**
- Read: `tests/CMakeLists.txt`

- [ ] **Step 1: Run full test suite**

```bash
cd build && ctest -j4 --output-on-failure 2>&1 | tee /tmp/ctest.log
```

Expected: Report showing tests passed/failed. Note total count and any failures.

- [ ] **Step 2: Count test results**

```bash
grep -E "tests passed|tests failed|Test #" /tmp/ctest.log | tail -20
echo "---"
grep -c "Passed" /tmp/ctest.log || true
grep -c "Failed" /tmp/ctest.log || true
```

Expected: Document the numbers. Any failure is a red flag.

- [ ] **Step 3: Investigate any test failures**

If any test fails, run it in isolation:
```bash
cd build && ctest -R <failing_test_name> -V
```

Document the failure reason. If it's a pre-existing flaky test, note it. If it's a real regression, flag as BLOCKING.

- [ ] **Step 4: Commit**

```bash
git commit --allow-empty -m "check(readiness): Task 2 full test suite executed"
```

---

## Task 3: Critical Stub / NotSupported Audit

**Files:**
- Read: Multiple source files (search-based)

- [ ] **Step 1: Search for blocking NotSupported in critical paths**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core
grep -rn "Status::NotSupported" src/dtx/storage_impl/ src/queryd/ src/graphd/ src/metad/ src/dtx/dtx_service_impl.cc | grep -v "test" | head -30
```

Expected: Only intentional/acceptable NotSupported:
- `DTXServiceImpl::Prepare/Commit/Abort` returning UNIMPLEMENTED — **acceptable by design** (2PC goes through StorageService)
- `ReadIndex` in braft — **acceptable** (leader-check fallback)
- `AUTO strategy not yet implemented` — **non-blocking**

Any NotSupported in `StorageServiceImpl::Prepare/Commit/Abort/Scan`, `QueryServiceImpl::ExecuteQuery`, `MetaServiceGrpcImpl::GetPartitionAssignment`, or `PartitionMigrationServiceImpl::SyncData` is **BLOCKING**.

- [ ] **Step 2: Verify 2PC Prepare writes real descriptors**

Read `src/dtx/storage_impl/storage_service_impl.cc` around lines 750-780:

```cpp
// Look for Prepare handler:
auto status = partition->Prepare(
    txn_id,
    read_set,
    write_set,
    write_descriptors,  // MUST be populated, not empty
    commit_ts);
```

Verify `write_descriptors` is populated from the proto request. If it's `{}`, that's **BLOCKING**.

Then read `src/dtx/storage/storage_server_with_grpc.cc` around lines 200-250:

```cpp
// Look for the same Prepare call in the standalone binary
auto status = partition->Prepare(txn_id, read_set, write_set, {}, commit_ts);
```

If the standalone binary passes `{}` as `write_descriptors`, flag as:
- **Non-blocking** if the standalone binary is deprecated/unused
- **Blocking** if the standalone binary is the production deployment target

- [ ] **Step 3: Check `IsSinglePartitionQuery` optimization**

Read `src/queryd/distributed_executor.cpp`, find `IsSinglePartitionQuery`:

```cpp
bool DistributedExecutor::IsSinglePartitionQuery(
    const std::shared_ptr<cypher::QueryStatement>& query) {
  // If this always returns false, flag as non-blocking performance issue
}
```

If the function body is `return false;`, document as **non-blocking** (safety fallback, causes extra RPC overhead).

- [ ] **Step 4: Commit**

```bash
git commit --allow-empty -m "check(readiness): Task 3 critical path stub audit complete"
```

---

## Task 4: Re-enable and Run Disabled Core Tests

**Files:**
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Re-enable partition router test**

In `tests/CMakeLists.txt`, uncomment:
```cmake
# add_executable(test_partition_router cluster/test_partition_router.cc)
# target_link_libraries(test_partition_router ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_partition_router)
```

If the source file `tests/cluster/test_partition_router.cc` does not exist, skip this test and document "MISSING".

- [ ] **Step 2: Re-enable partition raft manager test**

In `tests/CMakeLists.txt`, uncomment:
```cmake
# add_executable(test_partition_raft_manager cluster/test_partition_raft_manager.cc)
# target_link_libraries(test_partition_raft_manager ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_partition_raft_manager)
```

If source missing, skip.

- [ ] **Step 3: Re-enable storage interface tests**

In `tests/CMakeLists.txt`, uncomment:
```cmake
# add_executable(test_storage_interface test_storage_interface.cc)
# target_link_libraries(test_storage_interface ${CEDAR_TEST_LIBS})
# gtest_discover_tests(test_storage_interface)
```

If source missing, skip.

- [ ] **Step 4: Re-enable cedar_graph_storage test**

In `tests/CMakeLists.txt`, uncomment:
```cmake
# add_cedar_test(test_cedar_graph_storage)
```

- [ ] **Step 5: Build and run re-enabled tests**

```bash
cd build && cmake .. && make test_partition_router test_partition_raft_manager test_storage_interface test_cedar_graph_storage -j4 2>&1 | tee /tmp/reenabled_build.log
```

If build succeeds:
```bash
cd build && ctest -R "test_partition_router|test_partition_raft_manager|test_storage_interface|test_cedar_graph_storage" -V 2>&1 | tee /tmp/reenabled_tests.log
```

For each test, document:
- **BUILT** / **BUILD_FAILED**
- **PASSED** / **FAILED** (with failure reason)

- [ ] **Step 6: Revert CMakeLists.txt if tests fail to build**

If re-enabled tests fail to build due to missing files or API mismatches, revert the uncommenting to keep the build clean:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core && git checkout tests/CMakeLists.txt
```

Document which tests are missing vs. broken.

- [ ] **Step 7: Commit**

```bash
git commit --allow-empty -m "check(readiness): Task 4 disabled core test evaluation complete"
```

---

## Task 5: End-to-End Partition Test

**Files:**
- Read: `tests/test_end_to_end_partition.cc`
- Modify: `tests/CMakeLists.txt` (if needed)

- [ ] **Step 1: Check if test is in tests/CMakeLists.txt**

```bash
grep "test_end_to_end_partition" tests/CMakeLists.txt
```

If NOT found, add it:
```cmake
add_cedar_test(test_end_to_end_partition)
```

- [ ] **Step 2: Build and run**

```bash
cd build && cmake .. && make test_end_to_end_partition -j4 && ctest -R test_end_to_end_partition -V
```

Expected: Test builds and passes. If it fails, analyze the output.

- [ ] **Step 3: Commit**

```bash
git commit --allow-empty -m "check(readiness): Task 5 end-to-end partition test evaluated"
```

---

## Task 6: Configuration & Deployment Completeness

**Files:**
- Read: `config/cedar.yaml`
- Read: `k8s/*.yaml`
- Read: `cedar-docker-compose/docker-compose.yml`
- Read: `cedar-docker-compose/Dockerfile`

- [ ] **Step 1: Validate `config/cedar.yaml` has all required sections**

```bash
grep -E "^\w+:" config/cedar.yaml | head -30
```

Required sections (document if missing):
- `metad:` (address, heartbeat interval)
- `storaged:` (data_root, listen_address, port, raft)
- `graphd:` (queryd addresses)
- `queryd:` (storage addresses)
- `cluster:` (node_id, partition_count)
- `logging:` (level, output)
- `tls:` (cert paths, or `enabled: false`)

- [ ] **Step 2: Validate K8s manifests reference correct images and ports**

```bash
grep -E "image:|containerPort:|targetPort:" k8s/*.yaml
```

Expected: All services use consistent ports (e.g., metad 9559, storaged 9779, graphd 9669). Document any mismatches.

- [ ] **Step 3: Validate Docker Compose has health checks**

```bash
grep -A3 "healthcheck" cedar-docker-compose/docker-compose.yml
```

Expected: `storaged`, `metad`, `graphd` all have health checks. Document if any are missing.

- [ ] **Step 4: Validate Dockerfile builds cedar targets**

```bash
grep -E "make|cmake|cedar" cedar-docker-compose/Dockerfile | head -20
```

Expected: Dockerfile builds `cedar`, `storaged`, `metad`, `graphd`, `queryd` targets.

- [ ] **Step 5: Commit**

```bash
git commit --allow-empty -m "check(readiness): Task 6 config and deployment validation complete"
```

---

## Task 7: Cross-DC Replication Integration Verification

**Files:**
- Read: `src/dtx/storage_impl/storage_server.cc`
- Read: `src/dtx/cross_dc_replicator.cc`

- [ ] **Step 1: Verify CrossDCReplicator is initialized in StorageServer**

```bash
grep -n "CrossDCReplicator\|cross_dc_replicator_\|DTXServiceImpl\|dtx_service_impl_\|dtx_server" src/dtx/storage_impl/storage_server.cc | head -20
```

Expected: Lines showing:
- `cross_dc_replicator_ = std::make_unique<CrossDCReplicator>()`
- `cross_dc_replicator_->Initialize(...)`
- `cross_dc_replicator_->SetStorage(...)`
- `cross_dc_replicator_->Start()`
- DTX gRPC server created and started

If any of these are missing, flag as **BLOCKING** for multi-DC deployments, **non-blocking** for single-DC.

- [ ] **Step 2: Run cross-dc replication test**

```bash
cd build && ctest -R CrossDC -V
```

Expected: All cross-dc tests pass.

- [ ] **Step 3: Commit**

```bash
git commit --allow-empty -m "check(readiness): Task 7 cross-DC replication verified"
```

---

## Task 8: Final Report Generation

**Files:**
- Create: `docs/PRODUCTION_READINESS_AUDIT_2026-05-10.md`

- [ ] **Step 1: Generate structured report**

Create `docs/PRODUCTION_READINESS_AUDIT_2026-05-10.md` with this exact structure:

```markdown
# CedarGraph Production Readiness Audit

**Date:** 2026-05-10
**Auditor:** Automated readiness check plan
**Commit:** <fill in current HEAD>

## Executive Summary

| Area | Status | Notes |
|------|--------|-------|
| Compilation | <PASS/FAIL> | |
| Unit Tests (enabled) | <N/M passed> | |
| Unit Tests (re-enabled) | <N/M passed> | |
| Critical Stubs | <CLEAN/WARNINGS> | |
| 2PC Prepare Path | <PASS/FAIL> | |
| Raft Consensus | <PASS/FAIL> | |
| Cross-DC Replication | <PASS/FAIL> | |
| Query Execution | <PASS/FAIL> | |
| Configuration | <COMPLETE/PARTIAL> | |
| Deployment | <COMPLETE/PARTIAL> | |

**Overall Verdict:** <GO / NO-GO / GO_WITH_CAVEATS>

## Detailed Findings

### Blocking Issues (Must Fix Before Production)

1. **<Issue Title>**
   - Location: `file:line`
   - Description: <what's wrong>
   - Fix: <what needs to change>

### Non-Blocking Gaps (Acceptable for Initial Release)

1. **<Issue Title>**
   - Description: <what's missing>
   - Impact: <performance/observability/etc>

### Tests Re-enabled and Results

| Test | Build Status | Test Status |
|------|-------------|-------------|
| test_partition_router | <BUILT/FAILED/MISSING> | <PASSED/FAILED/N/A> |
| test_partition_raft_manager | <BUILT/FAILED/MISSING> | <PASSED/FAILED/N/A> |
| test_storage_interface | <BUILT/FAILED/MISSING> | <PASSED/FAILED/N/A> |
| test_cedar_graph_storage | <BUILT/FAILED/MISSING> | <PASSED/FAILED/N/A> |
| test_end_to_end_partition | <BUILT/FAILED/MISSING> | <PASSED/FAILED/N/A> |

## Recommendations

1. <Recommendation 1>
2. <Recommendation 2>
```

Fill in all sections with actual findings from Tasks 1-7.

- [ ] **Step 2: Commit report**

```bash
git add docs/PRODUCTION_READINESS_AUDIT_2026-05-10.md
git commit -m "docs(audit): production readiness audit report 2026-05-10"
```

---

## Self-Review

### 1. Spec Coverage

| Requirement | Task |
|-------------|------|
| Compilation check | Task 1 |
| Unit test execution | Task 2 |
| Critical stub audit | Task 3 |
| 2PC Prepare path verification | Task 3 |
| Disabled test re-enablement | Task 4 |
| End-to-end partition test | Task 5 |
| Config validation | Task 6 |
| Deployment validation | Task 6 |
| Cross-DC replication check | Task 7 |
| Final report | Task 8 |

### 2. Placeholder Scan

- No TBD/TODO/"implement later" steps
- All commands include exact file paths
- All expected outputs are specified

### 3. Type Consistency

- File paths match actual codebase structure
- Test names match `tests/CMakeLists.txt` patterns
- gRPC service names match proto definitions

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-10-distributed-system-readiness-check.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
