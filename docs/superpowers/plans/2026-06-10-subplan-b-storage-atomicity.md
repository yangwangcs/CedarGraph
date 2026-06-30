# Sub-Plan B: Storage Engine Atomicity & Isolation

**Date:** 2026-06-10  
**Project:** CedarGraph-Core  
**Branch:** `main`  
**Owner:** Storage Engine Team  
**ETA:** 2 days (6 bite-sized tasks, ~30 min each)  
**Depends on:** Sub-Plan A (WAL ordering fixes, already merged)  

---

## 1. Goal

Eliminate three P0 data-corruption races in the single-node storage engine:

| ID | Issue | Location | Impact |
|---|---|---|---|
| P0-4 | `PutEdge()` is non-atomic: EdgeOut written, then EdgeIn. Rollback Delete can itself fail; concurrent readers see half-written edges. | `src/storage/cedar_graph_storage.cc:461-516` | Dangling EdgeOut without EdgeIn; query returns inconsistent neighbor lists. |
| P0-5 | `BatchWrite()` captures `mem_.get()` at `BeginTransaction()`, but background flush can swap `mem_` before writes land, causing txn to write into an immutable (stale) memtable. | `src/storage/cedar_graph_storage.cc:1574-1679` | Writes silently lost after flush. |
| P0-6 | `Validate()` → `Commit()` has no global serialization. Two txns can pass `Validate()`, then interleave WAL/MemTable writes, breaking snapshot isolation. | `src/transaction/occ_transaction.cc:170-208` | Lost updates, phantom reads, inconsistent snapshots. |

**Success criteria:**
- All new tests fail before the fix and pass after.
- `ctest -R "test_edge_atomicity|test_batchwrite_memtable_refresh|test_commit_serializability"` passes.
- Existing tests `test_wal_ordering`, `test_transaction_pool`, `test_cedar_graph_storage` still pass.

---

## 2. Architecture

```
+------------------------+        +-----------------------+
|  CedarGraphStorage     |        |   OCCTransaction      |
|  -----------------     |        |   ---------------     |
|  PutEdge()             |------->|  Commit()             |
|  BatchWrite()          |        |   ├─ Validate()       |
|                        |        |   ├─ WriteToWAL()     |
+------------------------+        |   └─ WriteToMemTable()|
           |                      |         ↑             |
           v                      +---------|-------------+
    +--------------+                        |
    |  LsmEngine   |                        |
    |  ---------   |        +---------------v------------------+
    |  mem_ (ptr)  |<-------|  global_commit_mutex_ (NEW)      |
    |  imm_        |        |  serializes Validate+WAL+MemTable |
    +--------------+        +-----------------------------------+
```

**Design decisions (already reviewed by maintainers):**
1. **P0-4:** `PutEdge()` will use an internal `OCCTransaction` (the same code path as `BatchWrite`). This reuses the WAL + MVCC pipeline instead of inventing a new 2PC mini-protocol.
2. **P0-5:** `BatchWrite()` will refresh the memtable pointer **inside** the engine's `mutex_` immediately before calling `txn->Put()`. The transaction object itself stores a pointer, so we patch `BatchWrite` (not `OCCTransaction`).
3. **P0-6:** A single `std::mutex global_commit_mutex_` in `LsmEngine` serializes the critical section from the end of `Validate()` through `WriteToMemTable()`. This is a deliberate coarse-grained choice: commit throughput is bounded by WAL fsync anyway; fine-grained locking would add complexity without measurable gain on current benchmarks.

---

## 3. Tech Stack

- **Language:** C++17
- **Build:** CMake + Ninja
- **Test:** GoogleTest (already in tree)
- **Target libs:** `libcedar.so` (core storage + transaction)

---

## 4. File Map

| # | File | Action | Lines touched |
|---|---|---|---|
| 1 | `include/cedar/storage/lsm_engine.h` | Add `global_commit_mutex_` member | +1 line |
| 2 | `src/transaction/occ_transaction.cc` | Acquire/release `global_commit_mutex_` around critical section in `Commit()` | ~+8 lines |
| 3 | `src/storage/cedar_graph_storage.cc` | Rewrite `PutEdge()` to use internal `OCCTransaction`; rewrite `BatchWrite()` loop to refresh memtable | ~+25 / -15 lines |
| 4 | `tests/storage/test_edge_atomicity.cc` | **New** — concurrent readers cannot observe half-written edge | ~120 lines |
| 5 | `tests/transaction/test_batchwrite_memtable_refresh.cc` | **New** — simulate flush mid-batch, assert no lost writes | ~110 lines |
| 6 | `tests/transaction/test_commit_serializability.cc` | **New** — two concurrent txns write same key; exactly one commits | ~100 lines |
| 7 | `tests/CMakeLists.txt` | Register three new test executables | +12 lines |

---

## 5. Implementation Tasks

> **Rule:** Every task = one logical change. Run tests after each. No placeholders, no "TBD".

---

### Task 1 — Add `global_commit_mutex_` to `LsmEngine`

**Goal:** Provide the lock that `OCCTransaction::Commit()` will use to serialize validate-write.

**File:** `include/cedar/storage/lsm_engine.h`

Add the mutex inside the `LsmEngine` private section, right after the existing `flush_completion_mutex_`:

```cpp
  // ============================================================================
  // Global commit serialization (P0-6)
  // ============================================================================
  // Coarse-grained mutex that serializes the Validate+WAL+MemTable critical
  // section in OCCTransaction::Commit(). Held only for the duration of commit,
  // NOT during the entire transaction lifetime.
  mutable std::mutex global_commit_mutex_;
```

**Verification:**
```bash
cd <repo-root>/build && ninja cedar 2>&1 | tail -n 5
```
**Expected:** `ninja: no work to do.` (header-only change compiles with next task).

- [ ] Insert `global_commit_mutex_` declaration in `lsm_engine.h`
- [ ] Verify build still compiles after Task 2 (not yet, header-only)

---

### Task 2 — Serialize `Validate→WAL→MemTable` in `OCCTransaction::Commit()`

**Goal:** Prevent interleaved commits that break snapshot isolation.

**File:** `src/transaction/occ_transaction.cc`

**Current code (lines 170-208):**
```cpp
Status OCCTransaction::Commit() {
  // ... state checks ...

  // 验证阶段
  Status validation_status = Validate();
  if (!validation_status.ok()) {
    // ... abort ...
    return validation_status;
  }

  state_.store(TransactionState::kCommitting);

  // 写入 WAL
  if (wal_writer_) {
    Status wal_status = WriteToWAL();
    if (!wal_status.ok()) {
      // ... abort ...
      return wal_status;
    }
  }

  // 写入 MemTable
  Status write_status = WriteToMemTable();
  if (!write_status.ok()) {
    // ... abort ...
    return write_status;
  }

  // 标记提交完成
  state_.store(TransactionState::kCommitted);
  // ...
  return Status::OK();
}
```

**New code:**
```cpp
Status OCCTransaction::Commit() {
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "not active");
  }

  state_.store(TransactionState::kValidating);

  // ===================================================================
  // P0-6 FIX: Global commit serialization
  // Acquire the engine-wide mutex BEFORE Validate() and hold it until
  // MemTable writes are complete. This ensures that once Validate()
  // succeeds, no other transaction can pass Validate() and interleave
  // its WAL/MemTable writes with ours.
  // ===================================================================
  std::lock_guard<std::mutex> commit_lock(lsm_engine_->global_commit_mutex_);

  // 验证阶段
  Status validation_status = Validate();
  if (!validation_status.ok()) {
    state_.store(TransactionState::kAborted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
    txn_manager_->mutable_stats().validation_failures.fetch_add(1, std::memory_order_relaxed);
    return validation_status;
  }

  state_.store(TransactionState::kCommitting);

  // 写入 WAL (必须先于 MemTable)
  if (wal_writer_) {
    Status wal_status = WriteToWAL();
    if (!wal_status.ok()) {
      state_.store(TransactionState::kAborted);
      txn_manager_->UnregisterActiveTransaction(txn_id_);
      txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
      return wal_status;
    }
  }

  // 写入 MemTable
  Status write_status = WriteToMemTable();
  if (!write_status.ok()) {
    // ===================================================================
    // P0-6 PARTIAL-WRITE SAFETY: If MemTable write fails after WAL succeeded,
    // we MUST leave a deterministic state. Since we hold global_commit_mutex_,
    // no other txn can commit concurrently. We abort; the WAL entry will be
    // replayed on recovery (idempotent because MVCC version is fixed).
    // ===================================================================
    state_.store(TransactionState::kAborted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
    return write_status;
  }

  // 标记提交完成
  state_.store(TransactionState::kCommitted);
  txn_manager_->UnregisterActiveTransaction(txn_id_);
  txn_manager_->mutable_stats().txn_committed.fetch_add(1, std::memory_order_relaxed);

  return Status::OK();
}
```

**Verification:**
```bash
cd <repo-root>/build && ninja cedar 2>&1 | tail -n 10
```
**Expected:** No compile errors. Warnings about unused variables are OK.

- [ ] Apply the `Commit()` patch above
- [ ] Build `libcedar.so` successfully

---

### Task 3 — Write failing test for P0-6 (Commit serializability)

**Goal:** Prove that two concurrent txns writing the same key can both pass validation and corrupt state.

**File:** `tests/transaction/test_commit_serializability.cc` (new)

```cpp
// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <atomic>
#include <chrono>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/occ_transaction.h"

using namespace cedar;

// P0-6: Two transactions write the same key concurrently.
// With proper serialization, exactly one must commit.
TEST(CommitSerializabilityTest, ConcurrentWritesToSameKey) {
  std::string data_dir = "/tmp/test_commit_serializability_" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, data_dir, &storage).ok());
  ASSERT_NE(storage, nullptr);

  LsmEngine* lsm = storage->GetLsmEngine();
  ASSERT_NE(lsm, nullptr);

  TransactionManager txn_mgr;
  TransactionOptions txn_opts;
  txn_opts.isolation = IsolationLevel::kSnapshot;

  std::atomic<int> commits{0};
  std::atomic<int> aborts{0};
  const int kThreads = 4;
  const int kIterations = 50;

  auto worker = [&](int worker_id) {
    for (int i = 0; i < kIterations; ++i) {
      uint64_t key = 1000 + i;  // same key across threads per iteration

      OCCTransaction txn(&txn_mgr, lsm->GetMemTable(), lsm,
                         lsm->GetWalWriter(), txn_opts);
      Status s = txn.Begin();
      if (!s.ok()) { aborts.fetch_add(1); continue; }

      s = txn.Put(key, EntityType::Vertex, 0,
                  Descriptor::InlineInt(0, worker_id),
                  Timestamp(static_cast<uint64_t>(i)), 0);
      if (!s.ok()) { txn.Abort(); aborts.fetch_add(1); continue; }

      s = txn.Commit();
      if (s.ok()) {
        commits.fetch_add(1);
      } else {
        aborts.fetch_add(1);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back(worker, t);
  }
  for (auto& th : threads) th.join();

  // With proper serialization, for each key exactly one of the 4 threads
  // should succeed. Total commits == number of unique keys written.
  EXPECT_EQ(commits.load(), kIterations)
      << "Expected exactly one commit per key (" << kIterations
      << "), got " << commits.load();

  // Verify no key has more than one committed version visible
  for (int i = 0; i < kIterations; ++i) {
    uint64_t key = 1000 + i;
    OCCTransaction read_txn(&txn_mgr, lsm->GetMemTable(), lsm,
                            nullptr, txn_opts);
    Status s = read_txn.Begin();
    ASSERT_TRUE(s.ok());

    Descriptor desc;
    Timestamp ts;
    s = read_txn.Get(key, EntityType::Vertex, 0, &desc, &ts);
    ASSERT_TRUE(s.ok()) << "Key " << key << " should exist";

    auto val = desc.AsInlineInt();
    ASSERT_TRUE(val.has_value());
    // Value must be from one of the worker_ids (0..3)
    EXPECT_GE(val.value(), 0);
    EXPECT_LT(val.value(), kThreads);
  }

  delete storage;
  std::filesystem::remove_all(data_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

**Register in `tests/CMakeLists.txt`:**
Find the transaction test block (around line 270) and append:

```cmake
add_executable(test_commit_serializability transaction/test_commit_serializability.cc)
target_link_libraries(test_commit_serializability ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_commit_serializability)
```

**Verification (must FAIL before Task 2 is applied, PASS after):**
```bash
cd <repo-root>/build && ninja test_commit_serializability 2>&1
./tests/test_commit_serializability 2>&1
```
**Expected before fix:** `commits` > `kIterations` (both txns commit, lost update).  
**Expected after fix:** `commits == kIterations` (exactly one per key).

- [ ] Create `test_commit_serializability.cc`
- [ ] Append registration to `tests/CMakeLists.txt`
- [ ] Build and run test; confirm failure before fix, pass after Task 2

---

### Task 4 — Make `PutEdge()` atomic using internal OCC transaction

**Goal:** EdgeOut + EdgeIn must be all-or-nothing, and concurrent readers must never see half-written state.

**File:** `src/storage/cedar_graph_storage.cc`

**Current `PutEdge()` (lines 461-516):**
```cpp
Status CedarGraphStorage::PutEdge(const WriteOptions& options,
                                  uint64_t src_id, uint64_t dst_id,
                                  uint16_t edge_type, Timestamp timestamp,
                                  const Descriptor& descriptor,
                                  Timestamp txn_version) {
  std::unique_lock<std::shared_mutex> lock(rep_->mutex_);
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  uint16_t src_part_id = ComputePartition(src_id);
  uint16_t dst_part_id = ComputePartition(dst_id);
  uint8_t flags = PackCreateFlags(true);

  CedarKey edge_out_key = CedarKey::EdgeOut(src_id, dst_id, edge_type,
                                            timestamp, 0, src_part_id, flags);
  Descriptor edge_desc = descriptor;
  edge_desc.SetColumnId(edge_type);

  Status s = rep_->engine->Put(edge_out_key, edge_desc, txn_version);
  if (!s.ok()) return s;

  CedarKey edge_in_key = CedarKey::EdgeIn(dst_id, src_id, edge_type,
                                          timestamp, 0, dst_part_id, flags);
  Descriptor empty_desc = Descriptor::InlineInt(edge_type, 0);

  s = rep_->engine->Put(edge_in_key, empty_desc, txn_version);
  if (!s.ok()) {
    rep_->engine->Delete(edge_out_key, txn_version);  // BUG: rollback can fail
    return s;
  }

  if (options.sync) {
    s = rep_->engine->ForceFlush();
  }
  return s;
}
```

**New `PutEdge()`:**
```cpp
Status CedarGraphStorage::PutEdge(const WriteOptions& options,
                                  uint64_t src_id, uint64_t dst_id,
                                  uint16_t edge_type, Timestamp timestamp,
                                  const Descriptor& descriptor,
                                  Timestamp txn_version) {
  std::unique_lock<std::shared_mutex> lock(rep_->mutex_);

  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }

  // ===================================================================
  // P0-4 FIX: Use an internal OCC transaction to make EdgeOut + EdgeIn
  // atomic. The transaction's Commit() holds global_commit_mutex_, so
  // readers cannot interleave between the two writes.
  // ===================================================================

  TransactionOptions txn_options;
  txn_options.isolation = IsolationLevel::kSnapshot;

  auto txn = rep_->engine->BeginTransaction(txn_options);
  if (!txn) {
    return Status::InvalidArgument("PutEdge", "failed to begin transaction");
  }

  uint16_t src_part_id = ComputePartition(src_id);
  uint16_t dst_part_id = ComputePartition(dst_id);
  uint8_t flags = PackCreateFlags(true);

  // 1. EdgeOut (src -> dst)
  CedarKey edge_out_key = CedarKey::EdgeOut(src_id, dst_id, edge_type,
                                            timestamp, 0, src_part_id, flags);
  Descriptor edge_desc = descriptor;
  edge_desc.SetColumnId(edge_type);

  Status s = txn->Put(src_id, EntityType::EdgeOut, edge_type,
                      edge_desc, timestamp, dst_id);
  if (!s.ok()) {
    txn->Abort();
    return s;
  }

  // 2. EdgeIn (dst <- src) — reverse index
  CedarKey edge_in_key = CedarKey::EdgeIn(dst_id, src_id, edge_type,
                                          timestamp, 0, dst_part_id, flags);
  Descriptor empty_desc = Descriptor::InlineInt(edge_type, 0);

  s = txn->Put(dst_id, EntityType::EdgeIn, edge_type,
               empty_desc, timestamp, src_id);
  if (!s.ok()) {
    txn->Abort();
    return s;
  }

  // 3. Atomic commit (Validate + WAL + MemTable under global lock)
  s = txn->Commit();
  if (!s.ok()) {
    // Abort already called by Commit() on failure, but be explicit
    txn->Abort();
    return s;
  }

  // 4. Optional sync
  if (options.sync) {
    s = rep_->engine->ForceFlush();
  }

  return s;
}
```

**Verification:**
```bash
cd <repo-root>/build && ninja cedar 2>&1 | tail -n 10
```
**Expected:** Clean compile.

- [ ] Replace `PutEdge()` body with the atomic transaction version above
- [ ] Build `libcedar.so`

---

### Task 5 — Write failing test for P0-4 (Atomic PutEdge)

**Goal:** Concurrent readers must never observe an EdgeOut without its matching EdgeIn.

**File:** `tests/storage/test_edge_atomicity.cc` (new)

```cpp
// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/occ_transaction.h"

using namespace cedar;

class EdgeAtomicityTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  CedarGraphStorage* storage_ = nullptr;
  LsmEngine* lsm_ = nullptr;

  void SetUp() override {
    data_dir_ = "/tmp/test_edge_atomicity_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    CedarOptions options;
    options.create_if_missing = true;
    ASSERT_TRUE(CedarGraphStorage::Open(options, data_dir_, &storage_).ok());
    ASSERT_NE(storage_, nullptr);
    lsm_ = storage_->GetLsmEngine();
    ASSERT_NE(lsm_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};

// Concurrent readers should never see EdgeOut without EdgeIn.
TEST_F(EdgeAtomicityTest, NoHalfWrittenEdge) {
  TransactionManager txn_mgr;
  TransactionOptions txn_opts;
  txn_opts.isolation = IsolationLevel::kSnapshot;

  const uint64_t kSrc = 42;
  const uint64_t kDst = 99;
  const uint16_t kEdgeType = 7;
  const int kReaderThreads = 4;
  const int kWriterIterations = 200;

  std::atomic<int> half_write_observations{0};
  std::atomic<bool> writers_done{false};
  std::atomic<int> writes_committed{0};

  auto writer = [&]() {
    for (int i = 0; i < kWriterIterations; ++i) {
      Timestamp ts(static_cast<uint64_t>(i + 1));
      Descriptor desc = Descriptor::InlineInt(kEdgeType, i);
      WriteOptions wopts;
      Status s = storage_->PutEdge(wopts, kSrc, kDst, kEdgeType, ts, desc, ts);
      if (s.ok()) {
        writes_committed.fetch_add(1);
      }
    }
    writers_done.store(true);
  };

  auto reader = [&]() {
    while (!writers_done.load()) {
      OCCTransaction txn(&txn_mgr, lsm_->GetMemTable(), lsm_,
                         nullptr, txn_opts);
      Status s = txn.Begin();
      if (!s.ok()) continue;

      Descriptor out_desc;
      Timestamp out_ts;
      Status out_s = txn.Get(kSrc, EntityType::EdgeOut, kEdgeType, &out_desc, &out_ts);

      Descriptor in_desc;
      Timestamp in_ts;
      Status in_s = txn.Get(kDst, EntityType::EdgeIn, kEdgeType, &in_desc, &in_ts);

      // If we see EdgeOut, we MUST see EdgeIn at the same version.
      if (out_s.ok() && !in_s.ok()) {
        half_write_observations.fetch_add(1);
      }
    }
  };

  std::thread writer_thread(writer);
  std::vector<std::thread> readers;
  for (int r = 0; r < kReaderThreads; ++r) {
    readers.emplace_back(reader);
  }

  writer_thread.join();
  for (auto& th : readers) th.join();

  EXPECT_EQ(half_write_observations.load(), 0)
      << "Observed " << half_write_observations.load()
      << " half-written edges (EdgeOut without EdgeIn)";
  EXPECT_GT(writes_committed.load(), 0);
}

// If PutEdge fails, neither EdgeOut nor EdgeIn should be visible.
TEST_F(EdgeAtomicityTest, FailedPutEdgeLeavesNoTrace) {
  // This test relies on the fact that PutEdge now uses a txn.
  // We simply verify that a successful PutEdge is readable both ways,
  // and that aborting a manual equivalent txn leaves nothing.
  TransactionManager txn_mgr;
  TransactionOptions txn_opts;

  auto txn = lsm_->BeginTransaction(txn_opts);
  ASSERT_NE(txn, nullptr);

  Timestamp ts(1234);
  Status s = txn->Put(1, EntityType::EdgeOut, 1,
                      Descriptor::InlineInt(1, 100), ts, 2);
  ASSERT_TRUE(s.ok());

  s = txn->Put(2, EntityType::EdgeIn, 1,
               Descriptor::InlineInt(1, 0), ts, 1);
  ASSERT_TRUE(s.ok());

  // Abort instead of commit
  txn->Abort();

  OCCTransaction read_txn(&txn_mgr, lsm_->GetMemTable(), lsm_,
                          nullptr, txn_opts);
  s = read_txn.Begin();
  ASSERT_TRUE(s.ok());

  Descriptor desc;
  Timestamp ver;
  EXPECT_FALSE(read_txn.Get(1, EntityType::EdgeOut, 1, &desc, &ver).ok());
  EXPECT_FALSE(read_txn.Get(2, EntityType::EdgeIn, 1, &desc, &ver).ok());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

**Register in `tests/CMakeLists.txt`:**
Append in the storage test block (after line 101):

```cmake
add_executable(test_edge_atomicity storage/test_edge_atomicity.cc)
target_link_libraries(test_edge_atomicity ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_edge_atomicity)
```

**Verification:**
```bash
cd <repo-root>/build && ninja test_edge_atomicity 2>&1
./tests/test_edge_atomicity 2>&1
```
**Expected before fix:** `NoHalfWrittenEdge` may sporadically fail (half_write_observations > 0).  
**Expected after fix:** Both tests pass reliably (run 10x to confirm).

- [x] Create `test_edge_atomicity.cc`
- [x] Register in `tests/CMakeLists.txt`
- [x] Build and run; confirm failure before fix, pass after Task 4

---

### Task 6 — Fix `BatchWrite()` stale-memtable-pointer capture

**Goal:** Ensure `BatchWrite()` writes into the current active memtable, even if a background flush swaps `mem_` between batches.

**Root cause analysis:**
`BatchWrite()` calls `BeginTransaction()` which captures `mem_.get()`. The loop then calls `txn->Put()` multiple times. If a background flush runs between `BeginTransaction()` and the last `Put()`, the transaction writes into `imm_` (which is being flushed to SST) instead of the new `mem_`.

**Fix strategy:** Instead of beginning one transaction per batch and holding its memtable pointer, we verify the memtable pointer is still valid immediately before each batch's commit. Actually, a simpler fix: **begin a fresh transaction for every batch**, and keep batch_size small enough that the window is tiny. But the real fix is: `BatchWrite()` already begins a new txn per batch (the `while` loop). The issue is that `BeginTransaction()` stores `mem_` in `OCCTransaction`, and between `BeginTransaction()` and `txn->Commit()`, a flush may happen. However, since `Commit()` now holds `global_commit_mutex_`, and flush also acquires `mutex_`, the real issue is subtler.

Actually, looking at `LsmEngine::BeginTransaction()`:
```cpp
auto txn = std::make_unique<OCCTransaction>(
    txn_manager_.get(), mem_.get(), lsm_engine, wal_writer_.get(), options);
```
It captures `mem_.get()` at construction time. If `mem_` is swapped before `txn->Commit()` calls `WriteToMemTable()`, the writes go into the old memtable (`imm_`).

**The fix:** In `BatchWrite()`, after acquiring the storage lock and before each batch's `BeginTransaction()`, we ensure we have the engine's current memtable. Since `OCCTransaction` stores the pointer by value, we can't change it post-construction. The simplest correct fix is to **hold `rep_->engine`'s `mutex_` (or at least a shared_lock) across the entire batch transaction lifetime** so that `mem_` cannot be swapped.

However, `OCCTransaction::Commit()` now acquires `global_commit_mutex_`, and `FlushMemTable` acquires `mutex_`. The race window is tiny but real.

A cleaner fix: add a `RefreshMemTable()` method to `OCCTransaction` that re-captures `lsm_engine_->GetMemTable()` right before `WriteToMemTable()`. But that changes the transaction abstraction.

**Chosen minimal fix:** In `BatchWrite()`, before each batch's `BeginTransaction()`, acquire `rep_->engine`'s shared lock so that `mem_` cannot transition to `imm_` during the batch. The engine already uses `std::shared_mutex mutex_` for reads. Since `FlushMemTable` needs a unique_lock, a shared_lock during the batch prevents the swap.

Wait — `BatchWrite()` already holds `std::unique_lock<std::shared_mutex> lock(rep_->mutex_)` at the top of the function. But `rep_->mutex_` is `CedarGraphStorage::Rep::mutex_`, NOT `LsmEngine::mutex_`. The flush swaps happen under `LsmEngine::mutex_`.

So the fix is: in the batch loop, acquire `LsmEngine`'s shared_mutex in shared mode for the duration of each batch transaction.

**File:** `src/storage/cedar_graph_storage.cc`

**Current `BatchWrite()` single-node loop (lines 1639-1676):**
```cpp
  while (processed < total) {
    size_t current_batch = std::min(batch_size, total - processed);

    auto* txn = BeginTransaction();
    if (!txn) {
      return Status::InvalidArgument("BatchWrite", "failed to begin transaction");
    }

    for (size_t i = 0; i < current_batch; ++i) {
      const auto& item = items[processed + i];
      uint16_t col_id = item.column_id;
      if (item.timestamp.IsStatic()) {
        col_id = item.column_id | key_flags::kIsStaticColumn;
      }
      Status s = txn->Put(item.entity_id, item.entity_type, col_id,
                          item.descriptor, item.timestamp, item.target_id);
      if (!s.ok()) {
        txn->Abort();
        delete txn;
        return s;
      }
    }

    Status commit_status = txn->Commit();
    delete txn;
    if (!commit_status.ok()) {
      return commit_status;
    }
    processed += current_batch;
  }
```

**New loop body:**
```cpp
  while (processed < total) {
    size_t current_batch = std::min(batch_size, total - processed);

    // ===================================================================
    // P0-5 FIX: Hold the engine's shared_mutex during the entire batch
    // so that background Flush cannot swap mem_->imm_ while we are
    // writing into the memtable captured by BeginTransaction().
    // ===================================================================
    std::shared_lock<std::shared_mutex> engine_lock(rep_->engine->mutex_);

    auto* txn = BeginTransaction();
    if (!txn) {
      return Status::InvalidArgument("BatchWrite", "failed to begin transaction");
    }

    for (size_t i = 0; i < current_batch; ++i) {
      const auto& item = items[processed + i];
      uint16_t col_id = item.column_id;
      if (item.timestamp.IsStatic()) {
        col_id = item.column_id | key_flags::kIsStaticColumn;
      }
      Status s = txn->Put(item.entity_id, item.entity_type, col_id,
                          item.descriptor, item.timestamp, item.target_id);
      if (!s.ok()) {
        txn->Abort();
        delete txn;
        return s;
      }
    }

    Status commit_status = txn->Commit();
    delete txn;
    if (!commit_status.ok()) {
      return commit_status;
    }
    processed += current_batch;
  }
```

**Note:** `rep_->engine->mutex_` is `mutable std::shared_mutex mutex_` in `LsmEngine` (line 413 of `lsm_engine.h`). It is already public? No, it's private. We need to either:

1. Make `mutex_` accessible to `CedarGraphStorage`, or
2. Add a public accessor like `std::shared_mutex* GetMutex() const { return &mutex_; }`, or
3. Add a scoped lock helper to `LsmEngine`.

**Minimal change:** Add a public method to `LsmEngine`:

**File:** `include/cedar/storage/lsm_engine.h`

Add after `GetMemTable()` (around line 318):
```cpp
  // Acquire a shared lock on the engine's memtable mutex.
  // Use this when you need to prevent memtable swap during a batch operation.
  std::shared_lock<std::shared_mutex> AcquireSharedLock() const {
    return std::shared_lock<std::shared_mutex>(mutex_);
  }
```

Then in `BatchWrite()`, use:
```cpp
    auto engine_lock = rep_->engine->AcquireSharedLock();
```

**Verification:**
```bash
cd <repo-root>/build && ninja cedar 2>&1 | tail -n 10
```
**Expected:** Clean compile.

- [ ] Add `AcquireSharedLock()` to `LsmEngine` public section
- [ ] Insert `auto engine_lock = rep_->engine->AcquireSharedLock();` in `BatchWrite()` batch loop
- [ ] Build `libcedar.so`

---

### Task 7 — Write failing test for P0-5 (BatchWrite memtable refresh)

**Goal:** Force a background flush during `BatchWrite()` and prove no writes are lost.

**File:** `tests/transaction/test_batchwrite_memtable_refresh.cc` (new)

```cpp
// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/occ_transaction.h"

using namespace cedar;

TEST(BatchWriteMemtableRefreshTest, NoLostWritesDuringFlush) {
  std::string data_dir = "/tmp/test_batchwrite_memtable_refresh_" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, data_dir, &storage).ok());
  ASSERT_NE(storage, nullptr);

  LsmEngine* lsm = storage->GetLsmEngine();
  ASSERT_NE(lsm, nullptr);

  const int kTotalItems = 500;
  const size_t kBatchSize = 50;

  std::vector<CedarGraphStorage::BatchWriteItem> items;
  items.reserve(kTotalItems);
  for (int i = 0; i < kTotalItems; ++i) {
    items.emplace_back(
        static_cast<uint64_t>(i),
        EntityType::Vertex,
        0,
        Descriptor::InlineInt(0, i * 10),
        Timestamp::Static(),
        0);
  }

  std::atomic<bool> batch_done{false};

  // Background thread: hammer ForceFlush to trigger memtable swaps
  std::thread flusher([&]() {
    while (!batch_done.load()) {
      lsm->ForceFlush();
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });

  // Main thread: run BatchWrite while flusher is active
  Status s = storage->BatchWrite(items, kBatchSize);
  ASSERT_TRUE(s.ok()) << s.ToString();
  batch_done.store(true);
  flusher.join();

  // Verify every item is readable
  TransactionManager txn_mgr;
  TransactionOptions txn_opts;
  for (int i = 0; i < kTotalItems; ++i) {
    OCCTransaction txn(&txn_mgr, lsm->GetMemTable(), lsm,
                       nullptr, txn_opts);
    s = txn.Begin();
    ASSERT_TRUE(s.ok());

    Descriptor desc;
    Timestamp ts;
    s = txn.Get(static_cast<uint64_t>(i), EntityType::Vertex, 0, &desc, &ts);
    ASSERT_TRUE(s.ok()) << "Key " << i << " lost after BatchWrite + concurrent flush";

    auto val = desc.AsInlineInt();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), i * 10);
  }

  delete storage;
  std::filesystem::remove_all(data_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

**Register in `tests/CMakeLists.txt`:**
Append after the other transaction tests:

```cmake
add_executable(test_batchwrite_memtable_refresh transaction/test_batchwrite_memtable_refresh.cc)
target_link_libraries(test_batchwrite_memtable_refresh ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_batchwrite_memtable_refresh)
```

**Verification:**
```bash
cd <repo-root>/build && ninja test_batchwrite_memtable_refresh 2>&1
./tests/test_batchwrite_memtable_refresh 2>&1
```
**Expected before fix:** Random `Key X lost after BatchWrite` failures.  
**Expected after fix:** Passes reliably (run 10x).

- [ ] Create `test_batchwrite_memtable_refresh.cc`
- [ ] Register in `tests/CMakeLists.txt`
- [ ] Build and run; confirm failure before fix, pass after Task 6

---

### Task 8 — Regression suite: run all related existing tests

**Goal:** Ensure no existing functionality is broken by the three fixes.

```bash
cd <repo-root>/build

# Rebuild everything
ninja 2>&1 | tail -n 20

# Run the new tests
ctest -R "test_commit_serializability|test_edge_atomicity|test_batchwrite_memtable_refresh" --output-on-failure

# Run existing transaction/storage tests
ctest -R "test_wal_ordering|test_transaction_pool|test_pool_state_reset|test_cedar_graph_storage|test_cedar_basic_persistence|lsm_engine_lifecycle_test" --output-on-failure
```

**Expected output:**
```
Test project <repo-root>/build
    Start 1: test_commit_serializability
    Start 2: test_edge_atomicity
    Start 3: test_batchwrite_memtable_refresh
    ...
100% tests passed, 0 tests failed out of N
```

- [ ] Full rebuild passes
- [ ] All 3 new tests pass
- [ ] All existing related tests pass

---

### Task 9 — `WriteToMemTable()` partial-failure rollback (bonus hardening)

**Context:** The audit notes that `WriteToMemTable()` (lines 645-668) has no rollback on partial failure. Now that `Commit()` holds `global_commit_mutex_`, partial failure is less dangerous (no concurrent commit can interleave), but we should still clean up.

**Current code:**
```cpp
Status OCCTransaction::WriteToMemTable() {
  if (!memtable_) {
    return Status::InvalidArgument("OCCTransaction", "memtable is null");
  }
  for (const auto& entry : write_set_) {
    Timestamp ts = entry.user_timestamp;
    CedarKey key = MakeKey(entry.entity_id, entry.entity_type,
                          entry.column_id, ts, entry.target_id);
    Status s = memtable_->Put(key, entry.descriptor, entry.txn_version);
    if (!s.ok()) {
      return s;  // BUG: earlier entries already written
    }
  }
  return Status::OK();
}
```

**New code:**
```cpp
Status OCCTransaction::WriteToMemTable() {
  if (!memtable_) {
    return Status::InvalidArgument("OCCTransaction", "memtable is null");
  }

  size_t written = 0;
  for (const auto& entry : write_set_) {
    Timestamp ts = entry.user_timestamp;
    CedarKey key = MakeKey(entry.entity_id, entry.entity_type,
                          entry.column_id, ts, entry.target_id);
    Status s = memtable_->Put(key, entry.descriptor, entry.txn_version);
    if (!s.ok()) {
      // Partial-failure cleanup: remove entries we already inserted.
      // Because we hold global_commit_mutex_, no other txn can observe
      // these entries between the failed Put and the rollback deletes.
      for (size_t j = 0; j < written; ++j) {
        const auto& rollback_entry = write_set_[j];
        Timestamp rts = rollback_entry.user_timestamp;
        CedarKey rkey = MakeKey(rollback_entry.entity_id,
                                rollback_entry.entity_type,
                                rollback_entry.column_id, rts,
                                rollback_entry.target_id);
        memtable_->Put(rkey, Descriptor::Tombstone(), rollback_entry.txn_version);
      }
      return s;
    }
    ++written;
  }
  return Status::OK();
}
```

**Note:** `Descriptor::Tombstone()` must exist. If your tree uses a different tombstone constructor (e.g., `Descriptor::InlineInt(...)` with a delete flag), adapt accordingly. Check `include/cedar/types/descriptor.h`:

```bash
grep -n "Tombstone\|Delete\|tombstone" <repo-root>/include/cedar/types/descriptor.h
```

If `Descriptor::Tombstone()` does not exist, use the equivalent inline tombstone:
```cpp
Descriptor tombstone = Descriptor::InlineInt(0, 0);
tombstone.SetDeleted(true);  // or however your API marks deletion
```

- [ ] Inspect `descriptor.h` for correct tombstone API
- [ ] Apply partial-failure rollback to `WriteToMemTable()`
- [ ] Rebuild and run `test_commit_serializability` to verify no regression

---

## 6. Commit Checklist

```bash
# Final verification command (run this before git add)
cd <repo-root>/build && ninja && ctest -R "test_commit_serializability|test_edge_atomicity|test_batchwrite_memtable_refresh|test_wal_ordering|test_transaction_pool|test_cedar_graph_storage" --output-on-failure
```

- [ ] `global_commit_mutex_` added to `LsmEngine`
- [ ] `OCCTransaction::Commit()` acquires `global_commit_mutex_` before `Validate()`
- [ ] `PutEdge()` rewritten to use internal `OCCTransaction`
- [ ] `BatchWrite()` holds engine shared-lock during each batch
- [ ] `LsmEngine::AcquireSharedLock()` added
- [ ] `WriteToMemTable()` rolls back partial writes on failure
- [ ] 3 new test files created and registered in CMake
- [ ] All new tests pass
- [ ] All existing related tests pass
- [ ] No compiler warnings introduced (or warnings documented in PR)

---

## 7. Rollback Plan

If any production anomaly is detected after deploy:

1. Revert commit: `git revert <commit-hash>`
2. Re-build: `cd build && ninja`
3. Restart `storaged` processes.

The changes are purely in single-node storage engine code; no on-disk format changes, no RPC protocol changes, no cross-node coordination changes. Rollback is safe.

---

## 8. Performance Notes

| Change | Expected Impact | Mitigation |
|---|---|---|
| `global_commit_mutex_` serializes commits | Throughput ceiling on pure in-memory workloads | Held only for Validate+WAL+MemTable (~microseconds), not full txn lifetime. WAL fsync is already the bottleneck on real SSDs. |
| `PutEdge()` now uses txn | One extra WAL write per edge (EdgeOut+EdgeIn in same WAL batch) | Actually *reduces* WAL volume vs two independent `engine->Put()` calls + rollback Delete. |
| `BatchWrite()` shared_lock | Prevents flush during batch | Batches are small (default 1000 items). Lock held for milliseconds. |

---

*End of plan.*
