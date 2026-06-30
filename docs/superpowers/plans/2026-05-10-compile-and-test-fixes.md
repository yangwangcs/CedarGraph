# Compile System & Test Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all compilation errors and flaky tests so that `make -j$(nproc) && ctest -j$(nPROC)` passes 100% on macOS (Apple Clang 17) and Linux (GCC 11+).

**Architecture:** One-line fix for undefined `quorum` variable (already committed), plus migration of fixed-path temp directories to unique-per-test directories to eliminate filesystem races during parallel test execution.

**Tech Stack:** C++17, CMake, GoogleTest, Apple Clang 17 / GCC 11+

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/dtx/optimized_2pc_engine.cc` | Already fixed | `quorum` → `required_successes` |
| `tests/cluster/test_distributed_crud.cc` | Modify | Flaky test fixture using shared `/tmp/test_crud` |
| `tests/cluster/test_distributed_batch.cc` | Modify | Flaky test fixture using shared `/tmp/test_batch` |
| `tests/end_to_end/test_distributed_storage_api.cc` | Modify | Multiple tests using shared `/tmp/test_distributed_api` and `/tmp/test_discovery_api` |
| `tests/CMakeLists.txt` | Modify | Add `TIMEOUT` property to long-running performance tests |

---

## Context

### Already Fixed (Committed)
`src/dtx/optimized_2pc_engine.cc:1058` had an undefined identifier `quorum`. It was changed to `required_successes` (defined on line 1028 as `total`). This single fix unblocked compilation of the `cedar` static library, which in turn unblocked the 4 previously NOT_BUILT tests (`test_where_clause`, `test_parameterized_query`, `test_cypher_gcn_routing`, `test_storage_extensions`). All 4 now compile and pass.

### Remaining Problem
Running `ctest -j4` reveals flaky failures in tests that use **fixed temporary directory paths** (e.g., `/tmp/test_crud`). When ctest launches multiple gtest-filtered processes in parallel, they collide on the same directory: one process calls `DestroyDB` while another has the DB open.

Affected files:
- `tests/cluster/test_distributed_crud.cc` → `/tmp/test_crud`
- `tests/cluster/test_distributed_batch.cc` → `/tmp/test_batch`
- `tests/end_to_end/test_distributed_storage_api.cc` → `/tmp/test_distributed_api`, `/tmp/test_discovery_api`, `/tmp/test_single_node`, `/tmp/test_options_flag`

---

## Task 1: Fix `test_distributed_crud.cc` Directory Race

**Files:**
- Modify: `tests/cluster/test_distributed_crud.cc:20-45`

- [ ] **Step 1: Replace fixed `/tmp/test_crud` with unique directory per test instance**

Current fixture uses a hard-coded path:

```cpp
void SetUp() override {
    CedarGraphStorage::DestroyDB("/tmp/test_crud", CedarOptions());
    ...
    Status s = CedarGraphStorage::Open(options, "/tmp/test_crud", &storage_);
}

void TearDown() override {
    ...
    CedarGraphStorage::DestroyDB("/tmp/test_crud", CedarOptions());
}
```

Replace the fixture with:

```cpp
class DistributedCrudTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/test_crud_" + std::to_string(getpid()) + "_" +
                std::to_string(
                    std::chrono::steady_clock::now().time_since_epoch().count());

    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());

    CedarOptions options;
    options.create_if_missing = true;
    options.distributed_mode = false;

    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
    ASSERT_TRUE(s.ok());
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());
  }
};
```

- [ ] **Step 2: Build and run the specific test binary**

Run:
```bash
cd <repo-root>/build
cmake --build . --target test_distributed_crud
./tests/test_distributed_crud
```

Expected: All 6 tests pass.

- [ ] **Step 3: Verify parallel execution stability**

Run:
```bash
cd <repo-root>/build
ctest -j8 -R "DistributedCrudTest" --output-on-failure
```

Expected: `100% tests passed, 0 tests failed` even under `-j8` parallelism.

- [ ] **Step 4: Commit**

```bash
cd <repo-root>
git add tests/cluster/test_distributed_crud.cc
git commit -m "test(crud): use unique temp dirs per test to fix parallel race"
```

---

## Task 2: Fix `test_distributed_batch.cc` Directory Race

**Files:**
- Modify: `tests/cluster/test_distributed_batch.cc:20-35`

- [ ] **Step 1: Inspect current fixture**

```bash
cd <repo-root>
grep -n "test_batch\|SetUp\|TearDown" tests/cluster/test_distributed_batch.cc | head -20
```

- [ ] **Step 2: Replace fixed `/tmp/test_batch` with unique directory**

Apply the same pattern as Task 1:

```cpp
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/test_batch_" + std::to_string(getpid()) + "_" +
                std::to_string(
                    std::chrono::steady_clock::now().time_since_epoch().count());
    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());
    ...
    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
  }

  void TearDown() override {
    ...
    CedarGraphStorage::DestroyDB(test_dir_, CedarOptions());
  }
```

- [ ] **Step 3: Build and verify**

```bash
cd <repo-root>/build
cmake --build . --target test_distributed_batch
ctest -j8 -R "DistributedBatchTest" --output-on-failure
```

Expected: All tests pass under parallelism.

- [ ] **Step 4: Commit**

```bash
git add tests/cluster/test_distributed_batch.cc
git commit -m "test(batch): use unique temp dirs per test to fix parallel race"
```

---

## Task 3: Fix `test_distributed_storage_api.cc` Directory Races

**Files:**
- Modify: `tests/end_to_end/test_distributed_storage_api.cc:20-110`

- [ ] **Step 1: Inspect all fixed paths in the file**

```bash
cd <repo-root>
grep -n '"/tmp/test' tests/end_to_end/test_distributed_storage_api.cc
```

Expected output will show:
- `/tmp/test_distributed_api`
- `/tmp/test_discovery_api`
- `/tmp/test_single_node`
- `/tmp/test_options_flag`

- [ ] **Step 2: Replace each fixed path with a unique directory helper**

Add a helper function at file scope (before any test fixtures):

```cpp
static std::string MakeTestDir(const std::string& basename) {
  return "/tmp/" + basename + "_" + std::to_string(getpid()) + "_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}
```

Then update every fixture:

```cpp
// Before
Status s = CedarGraphStorage::Open(options, "/tmp/test_distributed_api", &storage);

// After
std::string test_dir = MakeTestDir("test_distributed_api");
Status s = CedarGraphStorage::Open(options, test_dir, &storage);
```

Do this for every test fixture in the file. Ensure `DestroyDB` and cleanup also use the unique variable.

- [ ] **Step 3: Build and verify**

```bash
cd <repo-root>/build
cmake --build . --target test_distributed_storage_api
ctest -j8 -R "DistributedStorageApiTest" --output-on-failure
```

Expected: All tests pass under parallelism.

- [ ] **Step 4: Commit**

```bash
git add tests/end_to_end/test_distributed_storage_api.cc
git commit -m "test(e2e): use unique temp dirs to fix parallel race in storage api tests"
```

---

## Task 4: Add CTest TIMEOUT to Long-Running Performance Tests

**Files:**
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Identify long-running tests**

The following tests take > 10 seconds and occasionally timeout under parallel load:
- `test_rw_performance` (CachePerformance ~16s)
- `test_large_sst` (LargeSST_64MB ~3s — borderline)

- [ ] **Step 2: Add TIMEOUT properties in tests/CMakeLists.txt**

Find the existing `add_executable(test_rw_performance ...)` and `add_executable(test_large_sst ...)` lines, then add `set_tests_properties` after `gtest_discover_tests`:

```cmake
add_executable(test_rw_performance storage/test_rw_performance.cc)
target_link_libraries(test_rw_performance ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_rw_performance)
set_tests_properties(${test_rw_performance_TESTS} PROPERTIES TIMEOUT 60)

add_executable(test_large_sst storage/test_large_sst.cc)
target_link_libraries(test_large_sst ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_large_sst)
set_tests_properties(${test_large_sst_TESTS} PROPERTIES TIMEOUT 30)
```

> Note: `gtest_discover_tests` sets a variable like `${test_rw_performance_TESTS}` containing all discovered test names. If this variable name is different, find it by checking the generated `cmake_test_discovery_*.json` files.

Alternative (simpler, if variable names are unclear): set timeout on the test binary target itself using `set_property(TEST test_rw_performance PROPERTY TIMEOUT 60)` after discovery. Actually, `gtest_discover_tests` creates individual tests, so we need to set timeout on all of them. The safest approach is:

```cmake
add_executable(test_rw_performance storage/test_rw_performance.cc)
target_link_libraries(test_rw_performance ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_rw_performance XML_OUTPUT_DIR ${CMAKE_BINARY_DIR}/test-results)
# After gtest_discover_tests, individual test names are in a generated property.
# Use a post-config command or simply rely on ctest default timeout.
```

Simpler alternative: increase the global ctest default timeout:

```cmake
# At the top of tests/CMakeLists.txt, after include(GoogleTest)
set(CTEST_TEST_TIMEOUT 60)
```

Or add to root `CMakeLists.txt`:
```cmake
set(CMAKE_CTEST_ARGUMENTS "--timeout;60")
```

Recommended approach — modify `tests/CMakeLists.txt` near the top:

```cmake
include(GoogleTest)

# Prevent flaky timeouts on slow performance tests under parallel ctest
set(CTEST_TEST_TIMEOUT 60)
```

- [ ] **Step 3: Reconfigure and verify**

```bash
cd <repo-root>/build
cmake ..
ctest -j4 --output-on-failure
```

Expected: `100% tests passed, 0 tests failed` (excluding intentionally Disabled tests).

- [ ] **Step 4: Commit**

```bash
git add tests/CMakeLists.txt
git commit -m "build(tests): set ctest timeout to 60s to prevent flaky performance test failures"
```

---

## Self-Review Checklist

- [ ] **Spec coverage**: Every flaky test from the audit has a task.
- [ ] **Placeholder scan**: No TBD, TODO, or "implement later" in any step.
- [ ] **Type consistency**: `test_dir_` is `std::string` in all fixtures; `MakeTestDir` returns `std::string`.

---

## Appendix: Quick Verification

After all tasks are complete, run the full verification:

```bash
cd <repo-root>
rm -rf build && mkdir build && cd build
cmake .. -DBUILD_TESTS=ON -DBUILD_EXAMPLES=ON -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
ctest -j$(nproc) --output-on-failure
```

Expected final output:
```
100% tests passed, 0 tests failed out of 492
```
(The ~24 Disabled tests will show as "Not Run" — this is expected and correct.)
