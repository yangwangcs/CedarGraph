# Sub-Plan G: Fix Accumulated Flush Visibility in GetRange/GetAll

**Date:** 2026-06-11  
**Scope:** `src/storage/lsm_engine.cc`  
**Estimated Duration:** 25 min  
**Risk:** Low — mechanical addition of existing proven code path to two sibling functions.

---

## 1. Goal

Add `accumulated_entries_` scanning to `GetRange` and `GetAll` so that data sitting in the accumulated flush buffer (not yet written to SST) is visible to range and full-entity queries. `GetRangeLimit` already does this correctly at lines 875–886; we replicate that logic into `GetRange` and `GetAll` with lock-order discipline matching each function.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     LsmEngine Query Path                     │
├─────────────────────────────────────────────────────────────┤
│  GetAll(entity, type, col)                                   │
│  GetRange(entity, type, col, start, end)                     │
│  GetRangeLimit(entity, type, col, start, end, max)  ← fixed │
├─────────────────────────────────────────────────────────────┤
│  1. MemTable            (mutex_ shared)                     │
│  2. Immutable MemTable  (mutex_ shared)                     │
│  3. Accumulated Buffer  (accumulated_mutex_)   ← INSERT HERE│
│  4. SST files           (no mutex_ / reader cache)          │
└─────────────────────────────────────────────────────────────┘
```

`accumulated_entries_` is a `std::vector<std::tuple<CedarKey, Descriptor, Timestamp>>` protected by `accumulated_mutex_`. It is populated during background memtable flush when `enable_accumulated_flush` is on and the accumulated buffer has not yet reached its size threshold.

---

## 3. Tech Stack

| Layer | Tech |
|-------|------|
| Language | C++17 |
| Build | CMake + Ninja |
| Tests | GoogleTest (`gtest`) |
| Concurrency | `std::shared_mutex` (`mutex_`), `std::mutex` (`accumulated_mutex_`) |

---

## 4. File Map

| File | Role | Lines Touched |
|------|------|---------------|
| `src/storage/lsm_engine.cc` | Engine implementation — add accumulated scan to `GetAll` and `GetRange` | 401–475, 751–821 |
| `tests/storage/test_accumulated_buffer_race.cc` | Existing race test — **extend** with `GetRange`/`GetAll` coverage | new tests appended |
| `tests/CMakeLists.txt` | Test registration (already exists, no change needed) | — |

---

## 5. Detailed Tasks (TDD — Red → Green → Commit)

> Each task is a bite-sized unit (2–5 min). Run the build/test command after every implementation step.

---

### Task 1 — Add failing tests for `GetRange` and `GetAll` over accumulated buffer

**File:** `tests/storage/test_accumulated_buffer_race.cc`

Append two new test cases that force data into `accumulated_entries_` (by writing enough to trigger memtable flush but not enough to hit the 64 MB accumulated threshold), then assert that `GetRange` and `GetAll` can see the data.

```cpp
TEST_F(AccumulatedBufferRaceTest, GetRangeSeesAccumulatedEntries) {
  const uint64_t kEntityId = 42;
  const uint16_t kColumnId = 7;
  const EntityType kType = EntityType::Vertex;

  // Write 1024 versions to trigger memtable → accumulated flush.
  for (int i = 0; i < 1024; ++i) {
    CedarKey key = CedarKey::Vertex(kEntityId, kColumnId, Timestamp(static_cast<uint64_t>(i) + 1));
    Descriptor desc = Descriptor::InlineInt(kColumnId, i);
    Status s = engine_->Put(key, desc, Timestamp(1));
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Wait for background flush to move data into accumulated_entries_.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // GetRange must see entries that are only in the accumulated buffer.
  auto range_results = engine_->GetRange(
      kEntityId, kType, kColumnId,
      Timestamp(500), Timestamp(600));

  EXPECT_GE(range_results.size(), 101u);  // timestamps 500..600 inclusive
  for (const auto& entry : range_results) {
    EXPECT_GE(entry.timestamp.value(), 500u);
    EXPECT_LE(entry.timestamp.value(), 600u);
  }
}

TEST_F(AccumulatedBufferRaceTest, GetAllSeesAccumulatedEntries) {
  const uint64_t kEntityId = 99;
  const uint16_t kColumnId = 3;
  const EntityType kType = EntityType::Vertex;

  for (int i = 0; i < 512; ++i) {
    CedarKey key = CedarKey::Vertex(kEntityId, kColumnId, Timestamp(static_cast<uint64_t>(i) + 1));
    Descriptor desc = Descriptor::InlineInt(kColumnId, i * 2);
    Status s = engine_->Put(key, desc, Timestamp(1));
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  auto all_results = engine_->GetAll(kEntityId, kType, kColumnId);

  EXPECT_GE(all_results.size(), 512u);
  for (const auto& entry : all_results) {
    EXPECT_EQ(entry.timestamp.value(), static_cast<uint64_t>(entry.descriptor.GetInlineInt().value_or(0) / 2) + 1);
  }
}
```

**Build & run (expect RED/fail):**
```bash
cd <repo-root>/build && \
  cmake --build . --target test_accumulated_buffer_race -j$(sysctl -n hw.ncpu) && \
  ./tests/test_accumulated_buffer_race --gtest_filter="AccumulatedBufferRaceTest.GetRangeSeesAccumulatedEntries:AccumulatedBufferRaceTest.GetAllSeesAccumulatedEntries"
```

**Expected:** Tests fail because `GetRange` and `GetAll` return 0 results from accumulated buffer.

**Commit checkpoint:** `git add tests/storage/test_accumulated_buffer_race.cc && git commit -m "test: add failing tests for accumulated buffer visibility in GetRange/GetAll"`

---

### Task 2 — Add `accumulated_entries_` scan to `GetAll`

**File:** `src/storage/lsm_engine.cc` (lines 401–475)

Insert an accumulated-buffer scan **between** the immutable-memtable scan (line 419) and the SST-files scan (line 422). `GetAll` holds `mutex_` as a `shared_lock` for its entire body, so we take `accumulated_mutex_` while still inside that scope.

**Exact replacement:** replace lines 419–421 (the blank line before `// 3. Query SST files`) with:

```cpp
  // 3. Query Accumulated Buffer (for accumulated flush mode)
  if (!accumulated_entries_.empty()) {
    std::lock_guard<std::mutex> lock(accumulated_mutex_);
    for (const auto& [key, descriptor, txn_version] : accumulated_entries_) {
      if (key.entity_id() != entity_id) continue;
      if (static_cast<uint8_t>(key.entity_type()) != static_cast<uint8_t>(entity_type)) continue;
      if (key.column_id() != column_id) continue;
      std::optional<uint64_t> dst_id = (key.target_id() != 0)
          ? std::optional<uint64_t>(key.target_id())
          : std::nullopt;
      results.emplace_back(key.timestamp(), descriptor, dst_id, txn_version);
    }
  }

  // 4. Query SST files via Size-Tiered Compaction Engine
```

Then update the existing comment `// 3. Query SST files` → `// 4. Query SST files` and `// Sort by timestamp descending...` remains unchanged (it will now also sort/dedup the accumulated entries together with memtable and SST results).

**Build & run (expect partial GREEN):**
```bash
cd <repo-root>/build && \
  cmake --build . --target test_accumulated_buffer_race -j$(sysctl -n hw.ncpu) && \
  ./tests/test_accumulated_buffer_race --gtest_filter="AccumulatedBufferRaceTest.GetAllSeesAccumulatedEntries"
```

**Commit checkpoint:** `git add src/storage/lsm_engine.cc && git commit -m "fix(GetAll): scan accumulated_entries_ before SST files"`

---

### Task 3 — Add `accumulated_entries_` scan to `GetRange`

**File:** `src/storage/lsm_engine.cc` (lines 751–821)

`GetRange` releases `mutex_` after the memtable/immutable scan (line 771) and then queries SST files without any engine lock. We insert the accumulated scan **after** releasing `mutex_` and **before** the SST scan, mirroring the lock-order discipline.

**Exact replacement:** replace lines 771–773 (the closing brace of the `mutex_` lock scope and the blank line before `// 3. Query SST files`) with:

```cpp
  }

  // 3. Query Accumulated Buffer (for accumulated flush mode)
  if (!accumulated_entries_.empty()) {
    std::lock_guard<std::mutex> lock(accumulated_mutex_);
    for (const auto& [key, descriptor, txn_version] : accumulated_entries_) {
      if (key.entity_id() != entity_id) continue;
      if (static_cast<uint8_t>(key.entity_type()) != static_cast<uint8_t>(entity_type)) continue;
      if (key.column_id() != column_id) continue;
      if (key.timestamp().value() < start.value() || key.timestamp().value() > end.value()) continue;
      std::optional<uint64_t> dst_id = (key.target_id() != 0)
          ? std::optional<uint64_t>(key.target_id())
          : std::nullopt;
      results.emplace_back(key.timestamp(), descriptor, dst_id, txn_version);
    }
  }

  // 4. Query SST files via Size-Tiered Compaction Engine - no lock
```

Then update the existing comment `// 3. Query SST files` → `// 4. Query SST files`.

**Build & run (expect full GREEN):**
```bash
cd <repo-root>/build && \
  cmake --build . --target test_accumulated_buffer_race -j$(sysctl -n hw.ncpu) && \
  ./tests/test_accumulated_buffer_race --gtest_filter="AccumulatedBufferRaceTest.*"
```

**Commit checkpoint:** `git add src/storage/lsm_engine.cc && git commit -m "fix(GetRange): scan accumulated_entries_ after mem/imm, before SST"`

---

### Task 4 — Run the full accumulated-buffer test suite and existing LSM tests

Verify no regressions in existing accumulated-buffer race tests or general LSM lifecycle tests.

```bash
cd <repo-root>/build && \
  cmake --build . --target test_accumulated_buffer_race lsm_engine_lifecycle_test -j$(sysctl -n hw.ncpu) && \
  ./tests/test_accumulated_buffer_race && \
  ./tests/lsm_engine_lifecycle_test
```

If any failure appears, check whether `GetAll` or `GetRange` now returns duplicates because accumulated entries overlap with memtable entries (this should not happen because memtable is cleared on flush, but verify with debug logging if needed).

**Commit checkpoint:** `git commit --allow-empty -m "test: verified GetRange/GetAll accumulated buffer visibility — all green"`

---

### Task 5 — Self-review checklist & final commit

- [ ] `GetAll` scans `accumulated_entries_` under `accumulated_mutex_` **inside** the `mutex_` shared-lock scope.
- [ ] `GetRange` scans `accumulated_entries_` under `accumulated_mutex_` **outside** the `mutex_` shared-lock scope (matches SST-file discipline).
- [ ] Filtering logic matches `GetRangeLimit`: `entity_id`, `entity_type`, `column_id`, and (for range methods) timestamp bounds.
- [ ] `dst_id` extracted from `key.target_id()` identically to the existing SST-reader paths in both functions.
- [ ] No placeholders, no `TBD`, no `TODO` left in the diff.
- [ ] Tests compile, run, and pass.

Final squash / cleanup (optional):
```bash
git log --oneline -5
# Optional: git rebase -i HEAD~3 to squash into a single commit if preferred
```

---

## 6. Code Diff Summary (Expected Final State)

### `src/storage/lsm_engine.cc`

**`GetAll` (around line 419):**
```diff
   }
   
-  // 3. Query SST files via Size-Tiered Compaction Engine
+  // 3. Query Accumulated Buffer (for accumulated flush mode)
+  if (!accumulated_entries_.empty()) {
+    std::lock_guard<std::mutex> lock(accumulated_mutex_);
+    for (const auto& [key, descriptor, txn_version] : accumulated_entries_) {
+      if (key.entity_id() != entity_id) continue;
+      if (static_cast<uint8_t>(key.entity_type()) != static_cast<uint8_t>(entity_type)) continue;
+      if (key.column_id() != column_id) continue;
+      std::optional<uint64_t> dst_id = (key.target_id() != 0)
+          ? std::optional<uint64_t>(key.target_id())
+          : std::nullopt;
+      results.emplace_back(key.timestamp(), descriptor, dst_id, txn_version);
+    }
+  }
+
+  // 4. Query SST files via Size-Tiered Compaction Engine
```

**`GetRange` (around line 771):**
```diff
   }
 
-  // 3. Query SST files via Size-Tiered Compaction Engine - no lock
+  // 3. Query Accumulated Buffer (for accumulated flush mode)
+  if (!accumulated_entries_.empty()) {
+    std::lock_guard<std::mutex> lock(accumulated_mutex_);
+    for (const auto& [key, descriptor, txn_version] : accumulated_entries_) {
+      if (key.entity_id() != entity_id) continue;
+      if (static_cast<uint8_t>(key.entity_type()) != static_cast<uint8_t>(entity_type)) continue;
+      if (key.column_id() != column_id) continue;
+      if (key.timestamp().value() < start.value() || key.timestamp().value() > end.value()) continue;
+      std::optional<uint64_t> dst_id = (key.target_id() != 0)
+          ? std::optional<uint64_t>(key.target_id())
+          : std::nullopt;
+      results.emplace_back(key.timestamp(), descriptor, dst_id, txn_version);
+    }
+  }
+
+  // 4. Query SST files via Size-Tiered Compaction Engine - no lock
```

---

## 7. Rollback Plan

If anything breaks:
```bash
git revert HEAD   # or HEAD~1 / HEAD~2 depending on commit structure
cd build && cmake --build . --target cedar_storage -j$(sysctl -n hw.ncpu)
```

The change is purely additive (two new `if` blocks) and touches no other logic, so a simple revert of the implementation commits restores the previous state cleanly.
