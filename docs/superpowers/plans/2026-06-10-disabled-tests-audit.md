# Sub-Plan D P1-6 — Disabled Tests Audit Report

**Date:** 2026-06-10  
**Project:** CedarGraph-Core  
**Scope:** Audit every `DISABLED_` test in the five files listed in Sub-Plan D and classify as Enable / Fix+Enable / Document.

## Summary

| Metric | Count |
|--------|-------|
| Disabled tests audited | 24 |
| Enabled (removed `DISABLED_` prefix) | 18 |
| Kept disabled with blocker comment | 6 |
| Crashes observed | 0 |

> **Note:** The Sub-Plan D implementation plan references 28 disabled tests, but only 24 `DISABLED_` tests exist across the listed files after inspection. The remaining 4 appear to have been enabled or removed in earlier work.

## Methodology

1. Built all affected test binaries:
   - `tests/test_cedar_update_validation`
   - `tests/test_cedar_update_persistence`
   - `tests/test_cedar_update_e2e`
   - `tests/test_temporal_minimal`
   - `tests/test_storage_skeleton_cache` *(newly added to `tests/CMakeLists.txt` so the storage skeleton-cache suite is actually compiled)*
2. Ran each binary with `--gtest_also_run_disabled_tests`.
3. Classified each test based on the run result and test-body completeness.

## Classification Details

### `tests/test_cedar_update_validation.cc` (9 audited)

| # | Test | Action | Reason |
|---|------|--------|--------|
| 1 | `ValidateExistingNode` | **Document** | Empty test body; requires full storage layer with strict existence checking before it can assert anything useful. |
| 2 | `TemporalAnachronism` | **Document** | Body exists but requires a real temporal validation engine; skeleton passes the check through. |
| 3 | `CacheHitOptimization` | **Enable** | Body complete; passes with current skeleton. |
| 4 | `CacheNegativeResult` | **Enable** | Body complete; passes with current skeleton. |
| 5 | `CacheLRUEviction` | **Enable** | Body complete; passes with current skeleton. |
| 6 | `SameBatchDependency` | **Document** | Test body is incomplete (ends mid-implementation). Re-enable after PendingWrites dependency logic is finished. |
| 7 | `FullWorkflowSuccess` | **Enable** | Body complete; passes with current skeleton. |
| 8 | `FullWorkflowFailMissingDst` | **Enable** | Body complete; passes with current skeleton. |
| 9 | `ValidationPerformance` | **Document** | Performance benchmark, not a correctness test; keep disabled in CI. |

### `tests/test_cedar_update_e2e.cc` (8 audited)

| # | Test | Action | Reason |
|---|------|--------|--------|
| 1 | `CreateEdgeWithFullKeyInfo` | **Enable** | Body complete; passes with current skeleton. |
| 2 | `VerifyCedarKeyAllFields` | **Enable** | Body complete; passes with current skeleton. |
| 3 | `TemporalVersioning` | **Enable** | Body complete; passes with current skeleton. |
| 4 | `BatchOperations` | **Enable** | Body complete; passes with current skeleton. |
| 5 | `DeleteVertexTemporalTombstone` | **Enable** | Body complete; passes with current skeleton. |
| 6 | `StrictModeValidation` | **Enable** | Body complete; passes with current skeleton. |
| 7 | `WritePerformance` | **Document** | Empty performance benchmark; keep disabled in CI. |
| 8 | `FullKeyInfoPersistence` | **Enable** | Body complete; passes with current skeleton. |

### `tests/test_cedar_update_persistence.cc` (5 audited)

| # | Test | Action | Reason |
|---|------|--------|--------|
| 1 | `SingleVertexPersistence` | **Enable** | Body complete; passes with current skeleton. |
| 2 | `EdgeBidirectionalPersistence` | **Enable** | Body complete; passes with current skeleton. |
| 3 | `TemporalVersioningPersistence` | **Document** | Storage skeleton does not retain the DELETE tombstone in `GetAll` (returns 2 versions instead of 3). Re-enable after the temporal versioning engine persists and returns DELETE records. |
| 4 | `BatchOperationsPersistence` | **Enable** | Body complete; passes with current skeleton. |
| 5 | `CedarKey32ByteIntegrity` | **Enable** | Body complete; passes with current skeleton. |

### `tests/test_temporal_minimal.cc` (1 audited)

| # | Test | Action | Reason |
|---|------|--------|--------|
| 1 | `WriteThenRead` | **Enable** | Body complete; scan returns all 5 vertices with the current skeleton. |

### `tests/storage/test_skeleton_cache.cc` (1 audited)

| # | Test | Action | Reason |
|---|------|--------|--------|
| 1 | `EmptyAndDeletedVertices` | **Enable** | Body complete; passes with current skeleton after the suite was wired into `tests/CMakeLists.txt`. |

## Build / Test Results

### Pre-audit run (`--gtest_also_run_disabled_tests`)

- `test_cedar_update_validation` — 12/12 passed (9 disabled + 3 enabled).
- `test_cedar_update_persistence` — 4/5 disabled passed; `DISABLED_TemporalVersioningPersistence` failed because `history.size()` was 2 instead of 3.
- `test_cedar_update_e2e` — 8/8 disabled passed.
- `test_temporal_minimal` — 1/1 disabled passed.
- `test_storage_skeleton_cache` — 1/1 disabled passed (after adding the target to CMake).

No crashes were observed in any disabled test.

### Post-fix run

After removing the `DISABLED_` prefix from the 18 tests classified as **Enable** and adding blocker comments to the 6 **Document** tests, the affected binaries were rebuilt and rerun:

- All newly-enabled tests pass.
- All documented (still-disabled) tests continue to run without crashing when invoked with `--gtest_also_run_disabled_tests`.

## Files Modified

- `tests/test_cedar_update_validation.cc`
- `tests/test_cedar_update_e2e.cc`
- `tests/test_cedar_update_persistence.cc`
- `tests/test_temporal_minimal.cc`
- `tests/storage/test_skeleton_cache.cc`
- `tests/CMakeLists.txt` (added `test_storage_skeleton_cache` target so the storage suite is built)

## Follow-up Work

The following tests remain disabled and have explicit blocker comments pointing to the subsystem that needs to land before they can be enabled:

1. `CedarUpdateValidationTest.DISABLED_ValidateExistingNode`
2. `CedarUpdateValidationTest.DISABLED_TemporalAnachronism`
3. `CedarUpdateValidationTest.DISABLED_SameBatchDependency`
4. `CedarUpdateValidationTest.DISABLED_ValidationPerformance`
5. `CedarUpdateE2ETest.DISABLED_WritePerformance`
6. `CedarUpdatePersistenceTest.DISABLED_TemporalVersioningPersistence`
