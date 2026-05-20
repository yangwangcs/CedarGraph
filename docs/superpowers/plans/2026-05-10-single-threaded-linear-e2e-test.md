# Single-Threaded Linear End-to-End Pipeline Test

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a single-threaded, linear end-to-end test that writes a value through the full CedarGraph storage stack (client → transaction layer → storage engine → disk), simulates a process restart, reads the value back, and verifies correctness.

**Architecture:** One test fixture with a strict linear lifecycle: `Setup → Write (2PC Prepare+Commit) → Flush → Restart → Read → Verify → Teardown`. No concurrency, no distributed RPC, all in one process.

**Tech Stack:** C++17, googletest, CedarGraphStorage, PartitionStorage, 2PC, LSM-Tree

---

## File Structure

- **Create:** `tests/test_linear_end_to_end_pipeline.cc`
- **Modify:** `tests/CMakeLists.txt`

---

## Task 1: Create Test File with Setup and Teardown

**Files:**
- Create: `tests/test_linear_end_to_end_pipeline.cc`

- [ ] **Step 1: Write test skeleton**

```cpp
// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
using namespace cedar::dtx;

class LinearEndToEndPipelineTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<StoragePartitionManager> partition_manager_;

  void SetUp() override {
    data_dir_ = "/tmp/cedar_linear_e2e_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    CedarOptions options;
    options.create_if_missing = true;
    options.enable_accumulated_flush = false;
    auto status = CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);

    partition_manager_ = std::make_unique<StoragePartitionManager>();
    StoragePartitionManager::PartitionConfig config;
    config.data_root = data_dir_;
    ASSERT_TRUE(partition_manager_->Initialize(config).ok());
    ASSERT_TRUE(partition_manager_->AddPartition(1).ok());
  }

  void TearDown() override {
    partition_manager_->Shutdown();
    partition_manager_.reset();
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};
```

- [ ] **Step 2: Commit**

```bash
git add tests/test_linear_end_to_end_pipeline.cc && git commit -m "test(e2e): add linear pipeline test skeleton"
```

---

## Task 2: Implement Write Phase (2PC Prepare + Commit)

**Files:**
- Modify: `tests/test_linear_end_to_end_pipeline.cc`

- [ ] **Step 1: Write the write-phase test body**

Add this test to the file:

```cpp
TEST_F(LinearEndToEndPipelineTest, WriteViaTwoPhaseCommit) {
  auto* partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Construct a CedarKey for entity 123, column 1, type Vertex
  CedarKey key;
  key.SetEntityId(123);
  key.SetColumnId(1);
  key.SetEntityType(1);  // Vertex

  // Prepare transaction data
  TxnID txn_id = 42;
  std::vector<CedarKey> read_set;
  std::vector<CedarKey> write_set = {key};

  std::unordered_map<uint64_t, Descriptor> write_descriptors;
  write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, 42);

  Timestamp prepare_ts(1000);
  Timestamp commit_ts(2000);

  // Phase 1: Prepare
  auto status = partition->Prepare(txn_id, read_set, write_set, write_descriptors, prepare_ts);
  EXPECT_TRUE(status.ok()) << "Prepare failed: " << status.ToString();

  // Phase 2: Commit
  status = partition->Commit(txn_id, commit_ts);
  EXPECT_TRUE(status.ok()) << "Commit failed: " << status.ToString();

  // Verify no prepared transactions remain
  EXPECT_TRUE(partition->GetPreparedTransactions().empty());
}
```

- [ ] **Step 2: Build and run**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake .. && make test_linear_end_to_end_pipeline -j4 && ctest -R LinearEndToEndPipelineTest.WriteViaTwoPhaseCommit -V
```

Expected: Test passes. Prepare and Commit succeed.

- [ ] **Step 3: Commit**

```bash
git add tests/test_linear_end_to_end_pipeline.cc && git commit -m "test(e2e): add 2PC write phase to linear pipeline test"
```

---

## Task 3: Implement Flush and Restart Phase

**Files:**
- Modify: `tests/test_linear_end_to_end_pipeline.cc`

- [ ] **Step 1: Add flush and restart test**

Add this test to the file:

```cpp
TEST_F(LinearEndToEndPipelineTest, FlushAndRestartPreservesData) {
  auto* partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write data
  CedarKey key;
  key.SetEntityId(456);
  key.SetColumnId(1);
  key.SetEntityType(1);

  TxnID txn_id = 100;
  std::vector<CedarKey> write_set = {key};
  std::unordered_map<uint64_t, Descriptor> write_descriptors;
  write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, 99);

  ASSERT_TRUE(partition->Prepare(txn_id, {}, write_set, write_descriptors, Timestamp(1000)).ok());
  ASSERT_TRUE(partition->Commit(txn_id, Timestamp(2000)).ok());

  // Flush to disk
  ASSERT_TRUE(storage_->ForceFlush().ok());

  // Simulate process restart: close everything
  partition_manager_->Shutdown();
  partition_manager_.reset();
  delete storage_;
  storage_ = nullptr;

  // Reopen storage
  CedarOptions options;
  options.create_if_missing = false;
  auto status = CedarGraphStorage::Open(options, data_dir_, &storage_);
  ASSERT_TRUE(status.ok()) << "Reopen failed: " << status.ToString();
  ASSERT_NE(storage_, nullptr);

  // Recreate partition manager and re-add partition
  partition_manager_ = std::make_unique<StoragePartitionManager>();
  StoragePartitionManager::PartitionConfig config;
  config.data_root = data_dir_;
  ASSERT_TRUE(partition_manager_->Initialize(config).ok());
  ASSERT_TRUE(partition_manager_->AddPartition(1).ok());

  // Read back
  auto* partition_after_restart = partition_manager_->GetPartition(1);
  ASSERT_NE(partition_after_restart, nullptr);

  std::vector<std::pair<Timestamp, Descriptor>> versions;
  status = storage_->ScanNode(456, Timestamp::Max(), &versions);
  ASSERT_TRUE(status.ok()) << "ScanNode failed: " << status.ToString();
  ASSERT_FALSE(versions.empty()) << "No versions found after restart";

  // Verify latest version
  const auto& latest = versions.back();
  EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
  EXPECT_EQ(latest.second.GetPayload(), 99);
}
```

- [ ] **Step 2: Build and run**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake .. && make test_linear_end_to_end_pipeline -j4 && ctest -R LinearEndToEndPipelineTest.FlushAndRestartPreservesData -V
```

Expected: Test passes. Data survives restart.

- [ ] **Step 3: Commit**

```bash
git add tests/test_linear_end_to_end_pipeline.cc && git commit -m "test(e2e): add flush and restart phase to linear pipeline test"
```

---

## Task 4: Add Full Pipeline Test (Write → Read → Verify in One Test)

**Files:**
- Modify: `tests/test_linear_end_to_end_pipeline.cc`

- [ ] **Step 1: Add the complete linear pipeline test**

Add this test to the file:

```cpp
TEST_F(LinearEndToEndPipelineTest, CompleteLinearPipeline) {
  // ========================================================================
  // Phase 1: Setup (already done in SetUp)
  // ========================================================================
  auto* partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // ========================================================================
  // Phase 2: Write (2PC Prepare + Commit)
  // ========================================================================
  CedarKey key;
  key.SetEntityId(789);
  key.SetColumnId(1);
  key.SetEntityType(1);

  const int32_t expected_value = 12345;
  TxnID txn_id = 999;
  std::vector<CedarKey> write_set = {key};
  std::unordered_map<uint64_t, Descriptor> write_descriptors;
  write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, expected_value);

  Timestamp prepare_ts(1000);
  Timestamp commit_ts(2000);

  ASSERT_TRUE(partition->Prepare(txn_id, {}, write_set, write_descriptors, prepare_ts).ok());
  ASSERT_TRUE(partition->Commit(txn_id, commit_ts).ok());

  // ========================================================================
  // Phase 3: In-Memory Read Verification
  // ========================================================================
  {
    std::vector<std::pair<Timestamp, Descriptor>> versions;
    auto status = storage_->ScanNode(789, Timestamp::Max(), &versions);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(versions.empty());

    const auto& latest = versions.back();
    EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
    EXPECT_EQ(latest.second.GetPayload(), expected_value);
  }

  // ========================================================================
  // Phase 4: Flush to Disk
  // ========================================================================
  ASSERT_TRUE(storage_->ForceFlush().ok());

  // ========================================================================
  // Phase 5: Restart (simulate crash + recovery)
  // ========================================================================
  partition_manager_->Shutdown();
  partition_manager_.reset();
  delete storage_;
  storage_ = nullptr;

  CedarOptions reopen_options;
  reopen_options.create_if_missing = false;
  auto status = CedarGraphStorage::Open(reopen_options, data_dir_, &storage_);
  ASSERT_TRUE(status.ok()) << "Storage reopen failed: " << status.ToString();

  partition_manager_ = std::make_unique<StoragePartitionManager>();
  StoragePartitionManager::PartitionConfig config;
  config.data_root = data_dir_;
  ASSERT_TRUE(partition_manager_->Initialize(config).ok());
  ASSERT_TRUE(partition_manager_->AddPartition(1).ok());

  // ========================================================================
  // Phase 6: Post-Restart Read Verification
  // ========================================================================
  {
    std::vector<std::pair<Timestamp, Descriptor>> versions;
    auto status = storage_->ScanNode(789, Timestamp::Max(), &versions);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(versions.empty()) << "Data lost after restart!";

    const auto& latest = versions.back();
    EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
    EXPECT_EQ(latest.second.GetPayload(), expected_value)
        << "Value corruption detected after restart!";
  }

  // ========================================================================
  // Phase 7: WAL Recovery Verification (optional but valuable)
  // ========================================================================
  // After restart, PartitionStorage should have no prepared transactions
  // because Commit wrote the WAL and we replayed it.
  auto* partition_after_restart = partition_manager_->GetPartition(1);
  ASSERT_NE(partition_after_restart, nullptr);
  EXPECT_TRUE(partition_after_restart->GetPreparedTransactions().empty());
}
```

- [ ] **Step 2: Add CMake target**

In `tests/CMakeLists.txt`, add:
```cmake
add_executable(test_linear_end_to_end_pipeline test_linear_end_to_end_pipeline.cc)
target_link_libraries(test_linear_end_to_end_pipeline ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_linear_end_to_end_pipeline)
```

- [ ] **Step 3: Build and run all three tests**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake .. && make test_linear_end_to_end_pipeline -j4 && ctest -R LinearEndToEndPipelineTest -V
```

Expected:
- `WriteViaTwoPhaseCommit` — Passed
- `FlushAndRestartPreservesData` — Passed
- `CompleteLinearPipeline` — Passed

- [ ] **Step 4: Commit**

```bash
git add tests/test_linear_end_to_end_pipeline.cc tests/CMakeLists.txt && git commit -m "test(e2e): complete single-threaded linear end-to-end pipeline test"
```

---

## Self-Review

### 1. Spec Coverage

| Requirement | Task |
|-------------|------|
| Single-threaded | All tasks — no threading, no async |
| Linear pipeline | Task 4 — strict Phase 1→7 ordering |
| Client writes value | Task 2, 4 — Construct CedarKey + Descriptor |
| Through all modules | Task 2, 4 — PartitionStorage (transaction layer) → CedarGraphStorage (engine layer) → filesystem |
| Data lands on disk | Task 3, 4 — ForceFlush |
| Read back after restart | Task 3, 4 — Close, reopen, ScanNode |
| Verify correctness | Task 4 — GetKind() == InlineInt, GetPayload() == expected_value |

### 2. Placeholder Scan

- No TBD/TODO/"implement later"
- All code changes include exact file paths
- All commands include expected output

### 3. Type Consistency

- `CedarKeyHash` is in `cedar::dtx` namespace
- `Descriptor::InlineInt(0, value)` — column_id=0, payload=value
- `Descriptor::GetPayload()` returns `uint32_t`
- `EntryKind::InlineInt` is kind 0
- `PartitionStorage::Prepare/Commit` signatures match existing code

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-10-single-threaded-linear-e2e-test.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
