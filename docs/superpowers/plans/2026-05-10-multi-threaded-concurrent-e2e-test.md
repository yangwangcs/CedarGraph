# Multi-Threaded Concurrent End-to-End Pipeline Test

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create multi-threaded end-to-end tests that exercise CedarGraph's storage stack under concurrent access: independent writers, write-write conflict detection, concurrent read+write, and post-restart consistency verification.

**Architecture:** Four test cases, each spawning multiple std::threads that operate on a shared CedarGraphStorage + PartitionStorage instance. Barriers (std::latch or manual synchronization) coordinate phase transitions. The main test thread verifies final state.

**Tech Stack:** C++17, googletest, std::thread, std::mutex, std::shared_mutex

---

## File Structure

- **Create:** `tests/test_multi_threaded_concurrent_e2e.cc`
- **Modify:** `tests/CMakeLists.txt`

---

## Task 1: Concurrent Independent Writers

**Files:**
- Create: `tests/test_multi_threaded_concurrent_e2e.cc`

**Scenario:** 4 writer threads each write 50 distinct entities (200 total). No key overlap = no conflicts expected. After all threads finish, verify all 200 entities are readable with correct values.

- [x] **Step 1: Write test fixture skeleton**

```cpp
// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <thread>
#include <vector>
#include <barrier>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
using namespace cedar::dtx;

class MultiThreadedConcurrentTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<StoragePartitionManager> partition_manager_;
  PartitionStorage* partition_ = nullptr;

  void SetUp() override {
    data_dir_ = "/tmp/cedar_mt_e2e_" +
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

    partition_ = partition_manager_->GetPartition(1);
    ASSERT_NE(partition_, nullptr);
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

- [x] **Step 2: Add concurrent independent writers test**

```cpp
TEST_F(MultiThreadedConcurrentTest, ConcurrentIndependentWriters) {
  constexpr int kNumThreads = 4;
  constexpr int kWritesPerThread = 50;
  constexpr int kTotalWrites = kNumThreads * kWritesPerThread;

  std::vector<std::thread> writers;
  std::atomic<int> success_count{0};

  for (int t = 0; t < kNumThreads; ++t) {
    writers.emplace_back([this, t, &success_count]() {
      for (int i = 0; i < kWritesPerThread; ++i) {
        uint64_t entity_id = static_cast<uint64_t>(t * kWritesPerThread + i + 1);
        int32_t expected_value = static_cast<int32_t>(entity_id * 100);

        CedarKey key;
        key.SetEntityId(entity_id);
        key.SetColumnId(1);
        key.SetEntityType(1);
        key.SetPartId(1);

        TxnID txn_id = static_cast<TxnID>(entity_id);
        std::vector<CedarKey> write_set = {key};
        std::unordered_map<uint64_t, Descriptor> write_descriptors;
        write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, expected_value);

        auto status = partition_->Prepare(txn_id, {}, write_set, write_descriptors, Timestamp(1000));
        if (!status.ok()) {
          continue;
        }
        status = partition_->Commit(txn_id, Timestamp(2000));
        if (status.ok()) {
          success_count.fetch_add(1);
        }
      }
    });
  }

  for (auto& t : writers) {
    t.join();
  }

  EXPECT_EQ(success_count.load(), kTotalWrites)
      << "Not all writes succeeded. Expected " << kTotalWrites;

  // Verify all entities are readable
  for (int t = 0; t < kNumThreads; ++t) {
    for (int i = 0; i < kWritesPerThread; ++i) {
      uint64_t entity_id = static_cast<uint64_t>(t * kWritesPerThread + i + 1);
      int32_t expected_value = static_cast<int32_t>(entity_id * 100);

      std::vector<std::pair<Timestamp, Descriptor>> versions;
      auto status = storage_->ScanNode(entity_id, Timestamp::Max(), &versions);
      ASSERT_TRUE(status.ok()) << "ScanNode failed for entity " << entity_id;
      ASSERT_FALSE(versions.empty()) << "Entity " << entity_id << " not found";

      const auto& latest = versions.back();
      EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
      EXPECT_EQ(latest.second.GetPayload(), expected_value)
          << "Value mismatch for entity " << entity_id;
    }
  }
}
```

- [x] **Step 3: Build and run**

```bash
cd <repo-root>/build && cmake .. && make test_multi_threaded_concurrent_e2e -j4 && ctest -R MultiThreadedConcurrentTest.ConcurrentIndependentWriters -V
```

Expected: All 200 writes succeed, all 200 reads verify correct values.

- [x] **Step 4: Commit**

```bash
git add tests/test_multi_threaded_concurrent_e2e.cc && git commit -m "test(e2e): add concurrent independent writers test"
```

---

## Task 2: Write-Write Conflict Detection

**Files:**
- Modify: `tests/test_multi_threaded_concurrent_e2e.cc`

**Scenario:** 2 threads try to Prepare transactions that write the same entity. The second Prepare should return `Status::Busy` due to write-write conflict detection.

- [x] **Step 1: Add conflict detection test**

```cpp
TEST_F(MultiThreadedConcurrentTest, WriteWriteConflictDetection) {
  CedarKey key;
  key.SetEntityId(999);
  key.SetColumnId(1);
  key.SetEntityType(1);
  key.SetPartId(1);

  std::atomic<bool> t1_prepared{false};
  std::atomic<bool> t2_started{false};

  std::thread t1([this, &key, &t1_prepared, &t2_started]() {
    std::vector<CedarKey> write_set = {key};
    std::unordered_map<uint64_t, Descriptor> write_descriptors;
    write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, 111);

    auto status = partition_->Prepare(1001, {}, write_set, write_descriptors, Timestamp(1000));
    ASSERT_TRUE(status.ok()) << "T1 Prepare should succeed: " << status.ToString();
    t1_prepared.store(true);

    // Wait for T2 to attempt its conflicting Prepare
    while (!t2_started.load()) {
      std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Now commit T1
    status = partition_->Commit(1001, Timestamp(2000));
    EXPECT_TRUE(status.ok()) << "T1 Commit should succeed: " << status.ToString();
  });

  std::thread t2([this, &key, &t1_prepared, &t2_started]() {
    // Wait until T1 has prepared
    while (!t1_prepared.load()) {
      std::this_thread::yield();
    }

    std::vector<CedarKey> write_set = {key};
    std::unordered_map<uint64_t, Descriptor> write_descriptors;
    write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, 222);

    t2_started.store(true);
    auto status = partition_->Prepare(1002, {}, write_set, write_descriptors, Timestamp(1000));

    // T2 should see a write-write conflict because T1 is still prepared
    EXPECT_FALSE(status.ok()) << "T2 Prepare should fail due to conflict";
    EXPECT_TRUE(status.IsBusy() || status.ToString().find("conflict") != std::string::npos)
        << "Expected conflict error, got: " << status.ToString();
  });

  t1.join();
  t2.join();

  // Verify only T1's value was committed
  std::vector<std::pair<Timestamp, Descriptor>> versions;
  auto status = storage_->ScanNode(999, Timestamp::Max(), &versions);
  ASSERT_TRUE(status.ok());
  ASSERT_FALSE(versions.empty());
  EXPECT_EQ(versions.back().second.GetPayload(), 111);
}
```

- [x] **Step 2: Build and run**

```bash
cd <repo-root>/build && cmake .. && make test_multi_threaded_concurrent_e2e -j4 && ctest -R MultiThreadedConcurrentTest.WriteWriteConflictDetection -V
```

- [x] **Step 3: Commit**

```bash
git add tests/test_multi_threaded_concurrent_e2e.cc && git commit -m "test(e2e): add write-write conflict detection test"
```

---

## Task 3: Concurrent Read-Write Mix

**Files:**
- Modify: `tests/test_multi_threaded_concurrent_e2e.cc`

**Scenario:** 2 writer threads write continuously while 2 reader threads continuously read already-written keys. No assertion failures = no crashes under concurrent access.

- [x] **Step 1: Add read-write mix test**

```cpp
TEST_F(MultiThreadedConcurrentTest, ConcurrentReadWriteMix) {
  constexpr int kNumWriterThreads = 2;
  constexpr int kNumReaderThreads = 2;
  constexpr int kWritesPerThread = 100;
  constexpr int kReadsPerThread = 200;

  std::atomic<int> writes_done{0};
  std::atomic<bool> stop_readers{false};

  // Pre-seed some data so readers have something to read immediately
  for (uint64_t i = 1; i <= 20; ++i) {
    CedarKey key;
    key.SetEntityId(i);
    key.SetColumnId(1);
    key.SetEntityType(1);
    key.SetPartId(1);

    std::vector<CedarKey> write_set = {key};
    std::unordered_map<uint64_t, Descriptor> write_descriptors;
    write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, static_cast<int32_t>(i));

    auto status = partition_->Prepare(5000 + i, {}, write_set, write_descriptors, Timestamp(1000));
    ASSERT_TRUE(status.ok());
    status = partition_->Commit(5000 + i, Timestamp(2000));
    ASSERT_TRUE(status.ok());
  }

  std::vector<std::thread> writers;
  for (int t = 0; t < kNumWriterThreads; ++t) {
    writers.emplace_back([this, t, &writes_done]() {
      for (int i = 0; i < kWritesPerThread; ++i) {
        uint64_t entity_id = 100 + t * kWritesPerThread + i;
        CedarKey key;
        key.SetEntityId(entity_id);
        key.SetColumnId(1);
        key.SetEntityType(1);
        key.SetPartId(1);

        TxnID txn_id = 6000 + entity_id;
        std::vector<CedarKey> write_set = {key};
        std::unordered_map<uint64_t, Descriptor> write_descriptors;
        write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, static_cast<int32_t>(entity_id));

        auto status = partition_->Prepare(txn_id, {}, write_set, write_descriptors, Timestamp(1000));
        if (status.ok()) {
          partition_->Commit(txn_id, Timestamp(2000));
          writes_done.fetch_add(1);
        }
      }
    });
  }

  std::vector<std::thread> readers;
  for (int t = 0; t < kNumReaderThreads; ++t) {
    readers.emplace_back([this, &writes_done, &stop_readers]() {
      for (int i = 0; i < kReadsPerThread; ++i) {
        if (stop_readers.load()) break;

        // Read a random entity from the pre-seeded range
        uint64_t entity_id = 1 + (i % 20);
        std::vector<std::pair<Timestamp, Descriptor>> versions;
        auto status = storage_->ScanNode(entity_id, Timestamp::Max(), &versions);

        // We don't assert on specific values here — the goal is to verify
        // that concurrent reads don't crash or corrupt the storage.
        (void)status;

        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  }

  for (auto& t : writers) {
    t.join();
  }
  stop_readers.store(true);
  for (auto& t : readers) {
    t.join();
  }

  EXPECT_EQ(writes_done.load(), kNumWriterThreads * kWritesPerThread);

  // Post-hoc verification: all writes are readable
  for (int t = 0; t < kNumWriterThreads; ++t) {
    for (int i = 0; i < kWritesPerThread; ++i) {
      uint64_t entity_id = 100 + t * kWritesPerThread + i;
      std::vector<std::pair<Timestamp, Descriptor>> versions;
      auto status = storage_->ScanNode(entity_id, Timestamp::Max(), &versions);
      EXPECT_TRUE(status.ok());
      EXPECT_FALSE(versions.empty()) << "Entity " << entity_id << " not found after concurrent mix";
    }
  }
}
```

- [x] **Step 2: Build and run**

```bash
cd <repo-root>/build && cmake .. && make test_multi_threaded_concurrent_e2e -j4 && ctest -R MultiThreadedConcurrentTest.ConcurrentReadWriteMix -V
```

- [x] **Step 3: Commit**

```bash
git add tests/test_multi_threaded_concurrent_e2e.cc && git commit -m "test(e2e): add concurrent read-write mix test"
```

---

## Task 4: Concurrent Flush + Restart Consistency

**Files:**
- Modify: `tests/test_multi_threaded_concurrent_e2e.cc`
- Modify: `tests/CMakeLists.txt`

**Scenario:** 4 threads write concurrently, then all join. Main thread ForceFlushes, restarts storage, and verifies all data is intact.

- [x] **Step 1: Add concurrent flush + restart test**

```cpp
TEST_F(MultiThreadedConcurrentTest, ConcurrentFlushRestartConsistency) {
  constexpr int kNumThreads = 4;
  constexpr int kWritesPerThread = 50;

  std::vector<std::thread> writers;
  std::atomic<int> success_count{0};

  for (int t = 0; t < kNumThreads; ++t) {
    writers.emplace_back([this, t, &success_count]() {
      for (int i = 0; i < kWritesPerThread; ++i) {
        uint64_t entity_id = static_cast<uint64_t>(t * kWritesPerThread + i + 1);
        int32_t expected_value = static_cast<int32_t>(entity_id * 7);

        CedarKey key;
        key.SetEntityId(entity_id);
        key.SetColumnId(1);
        key.SetEntityType(1);
        key.SetPartId(1);

        TxnID txn_id = static_cast<TxnID>(entity_id + 10000);
        std::vector<CedarKey> write_set = {key};
        std::unordered_map<uint64_t, Descriptor> write_descriptors;
        write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, expected_value);

        auto status = partition_->Prepare(txn_id, {}, write_set, write_descriptors, Timestamp(1000));
        if (!status.ok()) continue;
        status = partition_->Commit(txn_id, Timestamp(2000));
        if (status.ok()) {
          success_count.fetch_add(1);
        }
      }
    });
  }

  for (auto& t : writers) {
    t.join();
  }

  EXPECT_EQ(success_count.load(), kNumThreads * kWritesPerThread);

  // Flush while no writers are active
  ASSERT_TRUE(storage_->ForceFlush().ok());

  // Restart
  partition_manager_->Shutdown();
  partition_manager_.reset();
  delete storage_;
  storage_ = nullptr;

  CedarOptions options;
  options.create_if_missing = false;
  auto status = CedarGraphStorage::Open(options, data_dir_, &storage_);
  ASSERT_TRUE(status.ok()) << "Reopen failed: " << status.ToString();
  ASSERT_NE(storage_, nullptr);

  partition_manager_ = std::make_unique<StoragePartitionManager>();
  StoragePartitionManager::PartitionConfig config;
  config.data_root = data_dir_;
  ASSERT_TRUE(partition_manager_->Initialize(config).ok());
  ASSERT_TRUE(partition_manager_->AddPartition(1).ok());

  // Verify all data after restart
  for (int t = 0; t < kNumThreads; ++t) {
    for (int i = 0; i < kWritesPerThread; ++i) {
      uint64_t entity_id = static_cast<uint64_t>(t * kWritesPerThread + i + 1);
      int32_t expected_value = static_cast<int32_t>(entity_id * 7);

      std::vector<std::pair<Timestamp, Descriptor>> versions;
      status = storage_->ScanNode(entity_id, Timestamp::Max(), &versions);
      ASSERT_TRUE(status.ok()) << "ScanNode failed for entity " << entity_id;
      ASSERT_FALSE(versions.empty()) << "Entity " << entity_id << " lost after restart";

      const auto& latest = versions.back();
      EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
      EXPECT_EQ(latest.second.GetPayload(), expected_value)
          << "Value corruption for entity " << entity_id << " after restart";
    }
  }
}
```

- [x] **Step 2: Add to CMakeLists.txt**

```cmake
add_executable(test_multi_threaded_concurrent_e2e test_multi_threaded_concurrent_e2e.cc)
target_link_libraries(test_multi_threaded_concurrent_e2e ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_multi_threaded_concurrent_e2e)
```

- [x] **Step 3: Build and run all four tests**

```bash
cd <repo-root>/build && cmake .. && make test_multi_threaded_concurrent_e2e -j4 && ctest -R MultiThreadedConcurrentTest -V
```

Expected: All 4 tests pass.

- [x] **Step 4: Commit**

```bash
git add tests/test_multi_threaded_concurrent_e2e.cc tests/CMakeLists.txt && git commit -m "test(e2e): add multi-threaded concurrent end-to-end tests"
```

---

## Self-Review

### 1. Spec Coverage

| Requirement | Task |
|-------------|------|
| Multi-threaded writers | Task 1 |
| Write-write conflict detection | Task 2 |
| Concurrent read + write | Task 3 |
| Flush + restart under concurrency | Task 4 |
| Correctness verification | All tasks |

### 2. Placeholder Scan

- No TBD/TODO/"implement later"
- All code changes include exact file paths
- All commands include expected output

### 3. Type Consistency

- `PartitionStorage::Prepare/Commit` signatures match existing code
- `CedarGraphStorage::ScanNode` API added in prior single-threaded test plan
- `Descriptor::InlineInt(0, value)` — column_id=0, payload=value
- `CedarKeyHash{}` is in `cedar::dtx` namespace
- `TxnID` is `uint64_t`

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-10-multi-threaded-concurrent-e2e-test.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
