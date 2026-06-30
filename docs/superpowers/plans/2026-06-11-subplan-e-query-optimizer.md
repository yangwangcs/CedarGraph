# Sub-Plan E: Query Optimizer Infrastructure (Label/Property Index)

**Date:** 2026-06-11  
**Scope:** Add a minimal but real in-memory secondary index for label and property lookups to replace the fake `IndexScan` range filter.  
**Estimated Duration:** 4–5 hours  
**Risk:** Low — purely additive, no on-disk format changes.

---

## Goal

Replace the current `IndexScan` (lines 348–483 of `src/cypher/execution_plan.cc`) — which iterates `1..max_entity_id` and applies a predicate filter — with a **real** label and property index backed by in-memory `std::map` structures inside `LsmEngine`.

`PredicatePushdown` (lines 1187–1216) already exists and already emits `IndexScan` nodes. After this sub-plan, those `IndexScan` nodes will actually use an index instead of doing a full entity scan.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Cypher Query Layer                           │
│  ┌─────────────────┐   ┌──────────────────┐                     │
│  │ ExecutionPlan   │──▶│ IndexScan        │                     │
│  │ Builder         │   │ (now real index) │                     │
│  └─────────────────┘   └────────┬─────────┘                     │
│                                 │                                │
│  ┌──────────────────────────────┴────────────────────────────┐  │
│  │  BuildLabelIndex()  /  BuildPropertyIndex()               │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     CedarGraphStorage                            │
│                         │                                        │
│                         ▼                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ LsmEngine                                                │   │
│  │  ┌────────────────────────┐  ┌────────────────────────┐  │   │
│  │  │ label_index_           │  │ property_index_        │  │   │
│  │  │ map<string→vector<id>> │  │ map<pair<col,str>→ids> │  │   │
│  │  └────────────────────────┘  └────────────────────────┘  │   │
│  │         ▲                              ▲                   │   │
│  │         │                              │                   │   │
│  │    Put()/Delete()                 Put()/Delete()          │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Key design decisions**

1. **Index location:** Inside `LsmEngine` (not `CedarGraphStorage`). This means every `Put`/`Delete` going through the engine automatically keeps the index consistent.
2. **Label storage convention:** Labels are not currently persisted as LSM values. We reserve column ID `0x0FFF` (`kLabelColumnId`) for label storage. `CreateOperator` writes each label as a static property with this column ID. `LsmEngine::Put` detects `kLabelColumnId` and routes the value to `label_index_`.
3. **Property index key:** `std::pair<uint16_t, std::string>` where the string is the canonical string representation of the `Descriptor` value (`InlineInt` → `std::to_string(int)`, `InlineFloat` → `std::to_string(float)`, `InlineShortStr` → the raw string). There is no `PropertyValue` type in the current codebase; `std::string` is the minimal viable key type.
4. **Scope:** The indexes are **in-memory only** (rebuilt on `Open()` by scanning existing SST files, or lazily populated as writes arrive). For the MVP we do not persist the index to disk.
5. **Thread safety:** Both indexes are protected by a dedicated `std::shared_mutex` (`index_mutex_`). Lookups take `shared_lock`; mutations take `unique_lock`.
6. **Fallback:** If an index lookup returns no results (or the storage context is unavailable), `IndexScan` falls back to the existing range scan so queries never return empty incorrectly.

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | C++17 |
| Build | CMake 3.14+ |
| Tests | GoogleTest |
| Index structure | `std::map` (sorted, deterministic, easy to snapshot) |
| Thread safety | `std::shared_mutex` |

---

## File Map

| File | Action | Lines touched (approx) |
|------|--------|------------------------|
| `include/cedar/storage/lsm_engine.h` | Add index members, lookup API, constants | +40 |
| `src/storage/lsm_engine.cc` | Index maintenance in `Put`/`Delete`, lookup impl, rebuild | +120 |
| `include/cedar/cypher/execution_plan.h` | Add `BuildLabelIndex()` / `BuildPropertyIndex()` to `ExecutionPlanBuilder` | +10 |
| `src/cypher/execution_plan.cc` | Rewrite `IndexScan::Init` to query index; add builder helpers | +80 / –30 |
| `src/cypher/operators/write_operators.cc` | Modify `CreateOperator::CreateNode` to persist labels with `kLabelColumnId` | +15 |
| `tests/storage/test_lsm_engine_index.cc` | **New** — unit tests for index maintenance and lookup | +120 |
| `tests/cypher/test_index_scan.cc` | Update existing tests to assert real index behaviour | +60 / –10 |
| `tests/cypher/test_predicate_pushdown.cc` | Update to assert index is used via `EXPLAIN` | +20 |
| `tests/CMakeLists.txt` | Register new `test_lsm_engine_index` | +4 |

---

## Implementation Tasks

> **Convention:** Every task below contains **complete, copy-pasteable C++ code**. No “TBD”, no placeholders.  
> **TDD cycle:** failing test → compile → run (red) → implement → run (green) → `git commit`.  
> **Build command (used throughout):**  
> ```bash
> cd <repo-root>/build && cmake .. && make -j$(sysctl -n hw.ncpu)
> ```

---

### Phase 1 — Add index data structures to `LsmEngine`

**Time:** ~5 min  
**Goal:** Declare the indexes and lookup API in the header.

- [ ] **1.1** Open `include/cedar/storage/lsm_engine.h`.

Add the following in the `public:` section of `LsmEngine`, just above `// ========== Size-Tiered Compaction 引擎 ==========` (around line 680):

```cpp
  // ============================================================================
  // Secondary Index (Label / Property) — MVP in-memory only
  // ============================================================================
  static constexpr uint16_t kLabelColumnId = 0x0FFF;

  // Add a label → entity_id mapping (called by CreateOperator)
  void IndexLabel(uint64_t entity_id, const std::string& label);

  // Remove an entity from all indexes (called by Delete)
  void RemoveFromIndexes(uint64_t entity_id);

  // Lookup entity IDs by exact label match
  std::vector<uint64_t> LookupLabelIndex(const std::string& label) const;

  // Lookup entity IDs by exact (column_id, value_string) match
  std::vector<uint64_t> LookupPropertyIndex(uint16_t column_id,
                                             const std::string& value) const;

  // Helper: convert a Descriptor to its canonical index string
  static std::string DescriptorToIndexString(const Descriptor& desc);
```

Add the following in the `private:` section, near the other mutexes (around line 720):

```cpp
  // Secondary indexes (in-memory only — rebuilt on Open or populated lazily)
  std::map<std::string, std::vector<uint64_t>> label_index_;
  std::map<std::pair<uint16_t, std::string>, std::vector<uint64_t>> property_index_;
  mutable std::shared_mutex index_mutex_;

  // Internal helpers
  void UpdatePropertyIndex(uint64_t entity_id, uint16_t column_id,
                           const std::string& value);
  void RemoveEntityFromLabelIndex(uint64_t entity_id);
  void RemoveEntityFromPropertyIndex(uint64_t entity_id);
```

- [ ] **1.2** Build to check for compile errors.

```bash
cd <repo-root>/build && cmake .. && make -j$(sysctl -n hw.ncpu) 2>&1 | tail -20
```

**Expected:** `make` completes with no errors (the new declarations are unused, so nothing breaks).

- [ ] **1.3** Commit.

```bash
git -C <repo-root> add include/cedar/storage/lsm_engine.h
git -C <repo-root> commit -m "subplan-e: declare label/property index structures in LsmEngine"
```

---

### Phase 2 — Implement index maintenance in `LsmEngine::Put` / `Delete`

**Time:** ~15 min  
**Goal:** Keep indexes consistent on every write and delete.

- [ ] **2.1** Open `src/storage/lsm_engine.cc`.

Insert the helper implementations **before** the `LsmEngine::Put(const CedarKey& ...)` definition (around line 239).

```cpp
// ============================================================================
// Secondary Index Helpers
// ============================================================================

std::string LsmEngine::DescriptorToIndexString(const Descriptor& desc) {
  switch (desc.GetKind()) {
    case EntryKind::InlineInt:
      return std::to_string(desc.AsInlineInt().value_or(0));
    case EntryKind::InlineFloat: {
      auto f = desc.AsInlineFloat();
      return f ? std::to_string(*f) : "";
    }
    case EntryKind::InlineShortStr:
      return desc.AsInlineShortStr();
    default:
      return "";
  }
}

void LsmEngine::IndexLabel(uint64_t entity_id, const std::string& label) {
  std::unique_lock<std::shared_mutex> lock(index_mutex_);
  auto& ids = label_index_[label];
  auto it = std::lower_bound(ids.begin(), ids.end(), entity_id);
  if (it == ids.end() || *it != entity_id) {
    ids.insert(it, entity_id);
  }
}

void LsmEngine::UpdatePropertyIndex(uint64_t entity_id, uint16_t column_id,
                                    const std::string& value) {
  if (value.empty()) return;
  std::unique_lock<std::shared_mutex> lock(index_mutex_);
  auto key = std::make_pair(column_id, value);
  auto& ids = property_index_[key];
  auto it = std::lower_bound(ids.begin(), ids.end(), entity_id);
  if (it == ids.end() || *it != entity_id) {
    ids.insert(it, entity_id);
  }
}

void LsmEngine::RemoveEntityFromLabelIndex(uint64_t entity_id) {
  std::unique_lock<std::shared_mutex> lock(index_mutex_);
  for (auto it = label_index_.begin(); it != label_index_.end(); ) {
    auto& ids = it->second;
    ids.erase(std::remove(ids.begin(), ids.end(), entity_id), ids.end());
    if (ids.empty()) {
      it = label_index_.erase(it);
    } else {
      ++it;
    }
  }
}

void LsmEngine::RemoveEntityFromPropertyIndex(uint64_t entity_id) {
  std::unique_lock<std::shared_mutex> lock(index_mutex_);
  for (auto it = property_index_.begin(); it != property_index_.end(); ) {
    auto& ids = it->second;
    ids.erase(std::remove(ids.begin(), ids.end(), entity_id), ids.end());
    if (ids.empty()) {
      it = property_index_.erase(it);
    } else {
      ++it;
    }
  }
}

void LsmEngine::RemoveFromIndexes(uint64_t entity_id) {
  RemoveEntityFromLabelIndex(entity_id);
  RemoveEntityFromPropertyIndex(entity_id);
}

std::vector<uint64_t> LsmEngine::LookupLabelIndex(const std::string& label) const {
  std::shared_lock<std::shared_mutex> lock(index_mutex_);
  auto it = label_index_.find(label);
  if (it != label_index_.end()) return it->second;
  return {};
}

std::vector<uint64_t> LsmEngine::LookupPropertyIndex(uint16_t column_id,
                                                      const std::string& value) const {
  std::shared_lock<std::shared_mutex> lock(index_mutex_);
  auto it = property_index_.find(std::make_pair(column_id, value));
  if (it != property_index_.end()) return it->second;
  return {};
}
```

- [ ] **2.2** Hook index updates into `LsmEngine::Put(const CedarKey&, const Descriptor&, Timestamp)` (around line 290, inside the `unique_lock` block, after `TrackColumnId`):

Replace the existing block:

```cpp
    if (!disable_column_tracking_) {
      TrackColumnId(key.entity_id(), key.column_id());
    }
```

With:

```cpp
    if (!disable_column_tracking_) {
      TrackColumnId(key.entity_id(), key.column_id());
    }

    // Update secondary indexes
    if (key.column_id() == kLabelColumnId) {
      std::string label = DescriptorToIndexString(descriptor);
      if (!label.empty()) {
        IndexLabel(key.entity_id(), label);
      }
    } else if (key.entity_type() == EntityType::Vertex && !descriptor.IsTombstone()) {
      std::string val = DescriptorToIndexString(descriptor);
      if (!val.empty()) {
        UpdatePropertyIndex(key.entity_id(), key.column_id(), val);
      }
    }
```

- [ ] **2.3** Hook index removal into `LsmEngine::Delete(const CedarKey&, Timestamp)` (around line 300, inside the `unique_lock` block, after `InvalidateQueryCache`):

Replace:

```cpp
    InvalidateQueryCache(key.entity_id());
    if (!disable_column_tracking_) {
      TrackColumnId(key.entity_id(), key.column_id());
    }
```

With:

```cpp
    InvalidateQueryCache(key.entity_id());
    if (!disable_column_tracking_) {
      TrackColumnId(key.entity_id(), key.column_id());
    }
    RemoveFromIndexes(key.entity_id());
```

- [ ] **2.4** Build.

```bash
cd <repo-root>/build && make -j$(sysctl -n hw.ncpu) 2>&1 | tail -20
```

**Expected:** clean build.

- [ ] **2.5** Commit.

```bash
git -C <repo-root> add src/storage/lsm_engine.cc
git -C <repo-root> commit -m "subplan-e: implement index maintenance in LsmEngine Put/Delete"
```

---

### Phase 3 — Write the failing storage test

**Time:** ~10 min  
**Goal:** Verify that `LsmEngine` correctly maintains indexes.

- [ ] **3.1** Create `tests/storage/test_lsm_engine_index.cc`.

```cpp
// Copyright (c) 2025 The Cedar Authors
// LsmEngine secondary index tests

#include <gtest/gtest.h>
#include <filesystem>
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class LsmEngineIndexTest : public ::testing::Test {
 protected:
  std::string db_path_;

  void SetUp() override {
    char buf[] = "/tmp/cedar_index_test_XXXXXX";
    char* dir = mkdtemp(buf);
    ASSERT_NE(dir, nullptr);
    db_path_ = dir;
  }

  void TearDown() override {
    if (!db_path_.empty() && std::filesystem::exists(db_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }
};

TEST_F(LsmEngineIndexTest, PropertyIndexMaintainedOnPut) {
  CedarOptions options;
  options.create_if_missing = true;

  LsmEngine engine(db_path_, options, nullptr);
  ASSERT_TRUE(engine.Open().ok());

  CedarKey key = CedarKey::Vertex(100, 1, Timestamp::Static());
  Descriptor desc = Descriptor::InlineInt(1, 42);
  ASSERT_TRUE(engine.Put(key, desc, Timestamp(1)).ok());

  auto ids = engine.LookupPropertyIndex(1, "42");
  ASSERT_EQ(ids.size(), 1);
  EXPECT_EQ(ids[0], 100);
}

TEST_F(LsmEngineIndexTest, LabelIndexMaintainedOnPut) {
  CedarOptions options;
  options.create_if_missing = true;

  LsmEngine engine(db_path_, options, nullptr);
  ASSERT_TRUE(engine.Open().ok());

  CedarKey key = CedarKey::Vertex(200, LsmEngine::kLabelColumnId, Timestamp::Static());
  auto desc_opt = Descriptor::InlineShortStr(LsmEngine::kLabelColumnId, Slice("Person"));
  ASSERT_TRUE(desc_opt.has_value());
  ASSERT_TRUE(engine.Put(key, *desc_opt, Timestamp(1)).ok());

  auto ids = engine.LookupLabelIndex("Person");
  ASSERT_EQ(ids.size(), 1);
  EXPECT_EQ(ids[0], 200);
}

TEST_F(LsmEngineIndexTest, IndexRemovesOnDelete) {
  CedarOptions options;
  options.create_if_missing = true;

  LsmEngine engine(db_path_, options, nullptr);
  ASSERT_TRUE(engine.Open().ok());

  CedarKey key = CedarKey::Vertex(300, 2, Timestamp::Static());
  Descriptor desc = Descriptor::InlineInt(2, 99);
  ASSERT_TRUE(engine.Put(key, desc, Timestamp(1)).ok());
  ASSERT_EQ(engine.LookupPropertyIndex(2, "99").size(), 1);

  ASSERT_TRUE(engine.Delete(key, Timestamp(2)).ok());
  EXPECT_EQ(engine.LookupPropertyIndex(2, "99").size(), 0);
}

TEST_F(LsmEngineIndexTest, DuplicateEntityIdNotDuplicated) {
  CedarOptions options;
  options.create_if_missing = true;

  LsmEngine engine(db_path_, options, nullptr);
  ASSERT_TRUE(engine.Open().ok());

  CedarKey key = CedarKey::Vertex(400, 3, Timestamp::Static());
  Descriptor desc = Descriptor::InlineInt(3, 7);
  ASSERT_TRUE(engine.Put(key, desc, Timestamp(1)).ok());
  ASSERT_TRUE(engine.Put(key, desc, Timestamp(2)).ok());

  auto ids = engine.LookupPropertyIndex(3, "7");
  EXPECT_EQ(ids.size(), 1);
  EXPECT_EQ(ids[0], 400);
}
```

- [ ] **3.2** Register the test in `tests/CMakeLists.txt`.

Add near the other storage tests (around line 115):

```cmake
add_executable(test_lsm_engine_index storage/test_lsm_engine_index.cc)
target_link_libraries(test_lsm_engine_index ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_lsm_engine_index)
```

- [ ] **3.3** Reconfigure CMake and run the new test.

```bash
cd <repo-root>/build && cmake .. && make -j$(sysctl -n hw.ncpu) test_lsm_engine_index
./tests/test_lsm_engine_index
```

**Expected output:**

```
[==========] Running 4 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 4 tests from LsmEngineIndexTest
[ RUN      ] LsmEngineIndexTest.PropertyIndexMaintainedOnPut
[       OK ] LsmEngineIndexTest.PropertyIndexMaintainedOnPut (X ms)
[ RUN      ] LsmEngineIndexTest.LabelIndexMaintainedOnPut
[       OK ] LsmEngineIndexTest.LabelIndexMaintainedOnPut (X ms)
[ RUN      ] LsmEngineIndexTest.IndexRemovesOnDelete
[       OK ] LsmEngineIndexTest.IndexRemovesOnDelete (X ms)
[ RUN      ] LsmEngineIndexTest.DuplicateEntityIdNotDuplicated
[       OK ] LsmEngineIndexTest.DuplicateEntityIdNotDuplicated (X ms)
[----------] 4 tests from LsmEngineIndexTest (X ms total)

[==========] 4 tests from 1 test suite ran. (X ms total)
[  PASSED  ] 4 tests.
```

- [ ] **3.4** Commit.

```bash
git -C <repo-root> add tests/storage/test_lsm_engine_index.cc tests/CMakeLists.txt
git -C <repo-root> commit -m "subplan-e: add LsmEngine index unit tests (TDD green)"
```

---

### Phase 4 — Modify `CreateOperator` to persist labels

**Time:** ~5 min  
**Goal:** Ensure `CREATE (n:Person)` writes the label to storage so the label index is populated.

- [ ] **4.1** Open `src/cypher/operators/write_operators.cc`.

In `CreateOperator::CreateNode`, after the property loop (around line 148), add:

```cpp
  // Persist labels using the reserved label column so LsmEngine can index them
  for (const auto& label : node.labels) {
    auto desc_opt = Descriptor::InlineShortStr(LsmEngine::kLabelColumnId, Slice(label));
    if (desc_opt.has_value()) {
      items.emplace_back(node_id, EntityType::Vertex,
                         LsmEngine::kLabelColumnId,
                         *desc_opt, Timestamp::Static(), 0);
    }
  }
```

> **Note:** `LsmEngine::kLabelColumnId` requires `#include "cedar/storage/lsm_engine.h"`. It is already transitively included via `cedar_graph_storage.h`, but if the compiler complains, add the include explicitly at the top of `write_operators.cc`.

- [ ] **4.2** Build.

```bash
cd <repo-root>/build && make -j$(sysctl -n hw.ncpu) 2>&1 | tail -20
```

**Expected:** clean build.

- [ ] **4.3** Commit.

```bash
git -C <repo-root> add src/cypher/operators/write_operators.cc
git -C <repo-root> commit -m "subplan-e: persist labels in CreateOperator for label indexing"
```

---

### Phase 5 — Rewrite `IndexScan::Init` to use the index

**Time:** ~10 min  
**Goal:** `IndexScan` no longer does a blind range scan; it asks `LsmEngine` for entity IDs.

- [ ] **5.1** Open `src/cypher/execution_plan.cc`.

Add a helper at the top of the file (after the existing includes, around line 14):

```cpp
#include "cedar/storage/cedar_graph_storage.h"
```

Add a static helper in the anonymous namespace (before `NeighborToRelationship`, around line 30):

```cpp
static uint16_t PropertyNameToColumnId(const std::string& name) {
  return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
}

static std::string ValueToIndexString(const Value& value) {
  if (value.IsInt()) {
    return std::to_string(value.GetInt());
  }
  if (value.IsFloat()) {
    return std::to_string(value.GetFloat());
  }
  if (value.IsString()) {
    return value.GetString();
  }
  if (value.IsBool()) {
    return value.GetBool() ? "1" : "0";
  }
  return "";
}
```

Now replace the **entire** `IndexScan::Init` method (lines 363–402) with:

```cpp
bool IndexScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  node_ids_.clear();

  // Try to use the real secondary index via storage
  bool used_index = false;
  if (ctx->storage) {
    auto* engine = ctx->storage->GetLsmEngine();
    if (engine) {
      if (!property_.empty() && op_ == ComparisonExpr::EQ) {
        uint16_t col_id = PropertyNameToColumnId(property_);
        std::string val = ValueToIndexString(literal_);
        if (!val.empty()) {
          node_ids_ = engine->LookupPropertyIndex(col_id, val);
          used_index = !node_ids_.empty();
        }
      }
      if (!used_index && label_.has_value()) {
        node_ids_ = engine->LookupLabelIndex(*label_);
        used_index = !node_ids_.empty();
      }
    }
  }

  // Fallback to range scan if index returned nothing or storage is unavailable
  if (!used_index) {
    constexpr uint64_t kDefaultMinEntityId = 1;
    constexpr uint64_t kDefaultMaxEntityId = 1000;
    uint64_t min_entity_id = kDefaultMinEntityId;
    uint64_t max_entity_id = kDefaultMaxEntityId;

    const char* env_max = std::getenv("CEDAR_SCAN_MAX_ENTITIES");
    if (env_max) {
      char* end_ptr = nullptr;
      errno = 0;
      unsigned long long parsed = std::strtoull(env_max, &end_ptr, 10);
      if (end_ptr != env_max && *end_ptr == '\0' && errno == 0) {
        constexpr uint64_t kMaxAllowedEntities = 10000000;
        if (parsed >= min_entity_id && parsed <= kMaxAllowedEntities) {
          max_entity_id = static_cast<uint64_t>(parsed);
        } else if (parsed > kMaxAllowedEntities) {
          max_entity_id = kMaxAllowedEntities;
        }
      }
    }

    if (ctx->get_all_entities_fn) {
      node_ids_ = ctx->get_all_entities_fn(min_entity_id, max_entity_id, 1);
    } else if (ctx->graph) {
      node_ids_ = ctx->graph->ScanVertices(ctx->time_range.first, ctx->time_range.second);
    } else {
      node_ids_.reserve(max_entity_id - min_entity_id + 1);
      for (uint64_t i = min_entity_id; i <= max_entity_id; ++i) {
        node_ids_.push_back(i);
      }
    }
  }

  current_index_ = 0;
  return true;
}
```

- [ ] **5.2** Build.

```bash
cd <repo-root>/build && make -j$(sysctl -n hw.ncpu) 2>&1 | tail -20
```

**Expected:** clean build.

- [ ] **5.3** Commit.

```bash
git -C <repo-root> add src/cypher/execution_plan.cc
git -C <repo-root> commit -m "subplan-e: rewrite IndexScan::Init to query LsmEngine secondary index"
```

---

### Phase 6 — Update existing `IndexScan` tests

**Time:** ~10 min  
**Goal:** The existing `test_index_scan.cc` tests now need a storage context to exercise the real index path.

- [ ] **6.1** Open `tests/cypher/test_index_scan.cc` and replace the entire file:

```cpp
// Copyright (c) 2025 The Cedar Authors
// IndexScan operator tests — with real secondary index

#include <gtest/gtest.h>
#include <filesystem>
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/descriptor.h"

using namespace cedar::cypher;
using namespace cedar;

class IndexScanRealIndexTest : public ::testing::Test {
 protected:
  std::string db_path_;
  CedarGraphStorage* storage_ = nullptr;

  void SetUp() override {
    char buf[] = "/tmp/cedar_indexscan_test_XXXXXX";
    char* dir = mkdtemp(buf);
    ASSERT_NE(dir, nullptr);
    db_path_ = dir;

    CedarOptions options;
    options.create_if_missing = true;
    Status s = CedarGraphStorage::Open(options, db_path_, &storage_);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    if (!db_path_.empty() && std::filesystem::exists(db_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }

  // Helper: write a static vertex property directly through storage
  void PutStaticProp(uint64_t entity_id, uint16_t col_id, int32_t value) {
    Descriptor desc = Descriptor::InlineInt(col_id, value);
    Status s = storage_->PutStaticVertex(entity_id, col_id, desc);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  static uint16_t PropertyNameToColumnId(const std::string& name) {
    return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
  }
};

TEST_F(IndexScanRealIndexTest, ConstructorStoresParameters) {
  auto index_scan = std::make_shared<IndexScan>(
      "n",
      std::optional<std::string>("Person"),
      "name",
      ComparisonExpr::EQ,
      Value("Alice"));

  EXPECT_EQ(index_scan->GetName(), "IndexScan");
  EXPECT_NE(index_scan->GetDetails().find("Person"), std::string::npos);
  EXPECT_NE(index_scan->GetDetails().find("name"), std::string::npos);
}

TEST_F(IndexScanRealIndexTest, ExplainOutputContainsDetails) {
  auto index_scan = std::make_shared<IndexScan>(
      "n",
      std::optional<std::string>("Person"),
      "age",
      ComparisonExpr::GE,
      Value(18));

  std::string explain = index_scan->Explain(0);
  EXPECT_NE(explain.find("IndexScan"), std::string::npos);
  EXPECT_NE(explain.find("Person"), std::string::npos);
  EXPECT_NE(explain.find("age"), std::string::npos);
}

TEST_F(IndexScanRealIndexTest, UsesPropertyIndexForEquality) {
  // Seed the index: entity 42 has age = 25
  PutStaticProp(42, PropertyNameToColumnId("age"), 25);
  PutStaticProp(43, PropertyNameToColumnId("age"), 30);

  auto index_scan = std::make_shared<IndexScan>(
      "n", std::nullopt, "age", ComparisonExpr::EQ, Value(25));

  ExecutionContext ctx;
  ctx.storage = storage_;
  ASSERT_TRUE(index_scan->Init(&ctx));

  int count = 0;
  while (index_scan->Next()) ++count;
  EXPECT_EQ(count, 1);  // Only entity 42 matches
}

TEST_F(IndexScanRealIndexTest, FallbackToRangeScanWhenNoIndexHit) {
  // No indexed data at all
  auto index_scan = std::make_shared<IndexScan>(
      "n", std::nullopt, "age", ComparisonExpr::EQ, Value(99));

  ExecutionContext ctx;
  ctx.storage = storage_;
  ASSERT_TRUE(index_scan->Init(&ctx));

  int count = 0;
  while (index_scan->Next()) ++count;
  // Fallback range scan produces ids 1..1000, none have property "age" = 99
  EXPECT_EQ(count, 0);
}

TEST_F(IndexScanRealIndexTest, UsesLabelIndexWhenAvailable) {
  // Seed label index via LsmEngine directly
  auto* engine = storage_->GetLsmEngine();
  ASSERT_NE(engine, nullptr);
  engine->IndexLabel(77, "Person");
  engine->IndexLabel(78, "Person");

  auto index_scan = std::make_shared<IndexScan>(
      "n", std::optional<std::string>("Person"), "name",
      ComparisonExpr::EQ, Value("Alice"));

  ExecutionContext ctx;
  ctx.storage = storage_;
  ASSERT_TRUE(index_scan->Init(&ctx));

  int count = 0;
  while (index_scan->Next()) ++count;
  EXPECT_EQ(count, 2);  // Entities 77 and 78
}
```

- [ ] **6.2** Build and run.

```bash
cd <repo-root>/build && make -j$(sysctl -n hw.ncpu) test_index_scan
./tests/cypher/test_index_scan
```

**Expected output:**

```
[==========] Running 5 tests from 1 test suite.
[----------] 5 tests from IndexScanRealIndexTest
[ RUN      ] IndexScanRealIndexTest.ConstructorStoresParameters
[       OK ] IndexScanRealIndexTest.ConstructorStoresParameters (X ms)
[ RUN      ] IndexScanRealIndexTest.ExplainOutputContainsDetails
[       OK ] IndexScanRealIndexTest.ExplainOutputContainsDetails (X ms)
[ RUN      ] IndexScanRealIndexTest.UsesPropertyIndexForEquality
[       OK ] IndexScanRealIndexTest.UsesPropertyIndexForEquality (X ms)
[ RUN      ] IndexScanRealIndexTest.FallbackToRangeScanWhenNoIndexHit
[       OK ] IndexScanRealIndexTest.FallbackToRangeScanWhenNoIndexHit (X ms)
[ RUN      ] IndexScanRealIndexTest.UsesLabelIndexWhenAvailable
[       OK ] IndexScanRealIndexTest.UsesLabelIndexWhenAvailable (X ms)
[----------] 5 tests from IndexScanRealIndexTest (X ms total)

[==========] 5 tests from 1 test suite ran. (X ms total)
[  PASSED  ] 5 tests.
```

- [ ] **6.3** Commit.

```bash
git -C <repo-root> add tests/cypher/test_index_scan.cc
git -C <repo-root> commit -m "subplan-e: update IndexScan tests for real index behaviour"
```

---

### Phase 7 — Add `BuildLabelIndex()` and `BuildPropertyIndex()` to `ExecutionPlanBuilder`

**Time:** ~10 min  
**Goal:** Provide explicit builder helpers so the planner can choose between label scan, property scan, or full node scan.

- [ ] **7.1** Open `include/cedar/cypher/execution_plan.h`.

In the `ExecutionPlanBuilder` private section (around line 720), add:

```cpp
  static std::shared_ptr<PhysicalOperator> BuildLabelIndex(
      const std::string& variable,
      const std::string& label);

  static std::shared_ptr<PhysicalOperator> BuildPropertyIndex(
      const std::string& variable,
      const std::optional<std::string>& label,
      const std::string& property,
      ComparisonExpr::Op op,
      const Value& literal);
```

- [ ] **7.2** Open `src/cypher/execution_plan.cc`.

Add the implementations before `ExecutionPlanBuilder::BuildMatchPlan` (around line 1218):

```cpp
std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildLabelIndex(
    const std::string& variable,
    const std::string& label) {
  // Label-only scan: look up all entities with this label.
  // For the MVP we reuse IndexScan with an empty property and a dummy EQ
  // predicate that forces Init to fall through to the label index lookup.
  return std::make_shared<IndexScan>(
      variable, std::optional(label), "",
      ComparisonExpr::EQ, Value::Null());
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildPropertyIndex(
    const std::string& variable,
    const std::optional<std::string>& label,
    const std::string& property,
    ComparisonExpr::Op op,
    const Value& literal) {
  return std::make_shared<IndexScan>(
      variable, label, property, op, literal);
}
```

- [ ] **7.3** Update `ApplyPredicatePushdown` (around line 1187) to use the new helper and pass the label through:

Replace the current `ApplyPredicatePushdown` body:

```cpp
static std::shared_ptr<PhysicalOperator> ApplyPredicatePushdown(
    std::shared_ptr<PhysicalOperator> root,
    const std::vector<PushablePredicate>& predicates) {
  if (!root) return root;

  if (auto node_scan = std::dynamic_pointer_cast<NodeScan>(root)) {
    const auto& pp = predicates[0];
    auto index_scan = std::make_shared<IndexScan>(
        pp.variable, std::nullopt, pp.property, pp.op, pp.literal);
    return index_scan;
  }

  auto& children = root->GetChildren();
  if (!children.empty()) {
    auto new_child = ApplyPredicatePushdown(children[0], predicates);
    const_cast<std::vector<std::shared_ptr<PhysicalOperator>>&>(children)[0] = new_child;
  }

  return root;
}
```

With:

```cpp
static std::shared_ptr<PhysicalOperator> ApplyPredicatePushdown(
    std::shared_ptr<PhysicalOperator> root,
    const std::vector<PushablePredicate>& predicates) {
  if (!root) return root;

  if (auto node_scan = std::dynamic_pointer_cast<NodeScan>(root)) {
    const auto& pp = predicates[0];
    auto index_scan = ExecutionPlanBuilder::BuildPropertyIndex(
        pp.variable,
        node_scan->label(),   // preserve the original label constraint
        pp.property,
        pp.op,
        pp.literal);
    return index_scan;
  }

  auto& children = root->GetChildren();
  if (!children.empty()) {
    auto new_child = ApplyPredicatePushdown(children[0], predicates);
    const_cast<std::vector<std::shared_ptr<PhysicalOperator>>&>(children)[0] = new_child;
  }

  return root;
}
```

> **Note:** `NodeScan::label()` does not exist yet. Add a public accessor in `include/cedar/cypher/execution_plan.h` inside `class NodeScan`:
>
> ```cpp
>   const std::optional<std::string>& label() const { return label_; }
> ```

- [ ] **7.4** Build.

```bash
cd <repo-root>/build && make -j$(sysctl -n hw.ncpu) 2>&1 | tail -20
```

**Expected:** clean build.

- [ ] **7.5** Run the predicate pushdown tests to confirm the label is now preserved.

```bash
cd <repo-root>/build && make -j$(sysctl -n hw.ncpu) test_predicate_pushdown
./tests/cypher/test_predicate_pushdown
```

**Expected output:**

```
[==========] Running 3 tests from 1 test suite.
[----------] 3 tests from PredicatePushdownTest
[ RUN      ] PredicatePushdownTest.SimpleEqualityPushedToIndexScan
[       OK ] PredicatePushdownTest.SimpleEqualityPushedToIndexScan (X ms)
[ RUN      ] PredicatePushdownTest.RangePredicatePushedToIndexScan
[       OK ] PredicatePushdownTest.RangePredicatePushedToIndexScan (X ms)
[ RUN      ] PredicatePushdownTest.NonPushablePredicateKeepsFilter
[       OK ] PredicatePushdownTest.NonPushablePredicateKeepsFilter (X ms)
[----------] 3 tests from PredicatePushdownTest (X ms total)

[==========] 3 tests from 1 test suite ran. (X ms total)
[  PASSED  ] 3 tests.
```

- [ ] **7.6** Commit.

```bash
git -C <repo-root> add include/cedar/cypher/execution_plan.h src/cypher/execution_plan.cc
git -C <repo-root> commit -m "subplan-e: add BuildLabelIndex/BuildPropertyIndex to ExecutionPlanBuilder"
```

---

### Phase 8 — Update `test_predicate_pushdown.cc` to assert label preservation

**Time:** ~5 min  
**Goal:** Make the pushdown test stricter: verify that when `NodeScan` had a label, the resulting `IndexScan` still carries it.

- [ ] **8.1** Open `tests/cypher/test_predicate_pushdown.cc` and add one extra assertion to the first test:

Replace the first test (`SimpleEqualityPushedToIndexScan`) with:

```cpp
TEST(PredicatePushdownTest, SimpleEqualityPushedToIndexScan) {
  // Parse: MATCH (n:Person) WHERE n.name = 'Alice' RETURN n
  CypherParser parser("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << parser.GetError();

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);

  std::string explain = plan->Explain(0);
  std::cout << "EXPLAIN:\n" << explain << std::endl;

  // After pushdown the plan should contain IndexScan, NOT NodeScan + Filter
  EXPECT_NE(explain.find("IndexScan"), std::string::npos);
  EXPECT_EQ(explain.find("NodeScan"), std::string::npos);
  EXPECT_EQ(explain.find("Filter"), std::string::npos);

  // The label must be preserved in the IndexScan details
  EXPECT_NE(explain.find("Person"), std::string::npos);
}
```

- [ ] **8.2** Build and run.

```bash
cd <repo-root>/build && make -j$(sysctl -n hw.ncpu) test_predicate_pushdown
./tests/cypher/test_predicate_pushdown
```

**Expected:** all 3 tests pass, and the `EXPLAIN` output contains `Person` inside the `IndexScan` line.

- [ ] **8.3** Commit.

```bash
git -C <repo-root> add tests/cypher/test_predicate_pushdown.cc
git -C <repo-root> commit -m "subplan-e: strengthen predicate pushdown test for label preservation"
```

---

### Phase 9 — Full test suite verification

**Time:** ~10 min  
**Goal:** Make sure the new code does not break any existing tests.

- [ ] **9.1** Run the cypher and storage test suites.

```bash
cd <repo-root>/build && ctest -R "test_index_scan|test_predicate_pushdown|test_lsm_engine_index|test_cedar_graph_storage|test_execution_operators|test_write_operators" --output-on-failure
```

**Expected output:**

```
Test project <repo-root>/build
    Start 1: test_lsm_engine_index
1/7 Test #1: test_lsm_engine_index ...............   Passed    X.XX sec
    Start 2: test_index_scan
2/7 Test #2: test_index_scan .....................   Passed    X.XX sec
    Start 3: test_predicate_pushdown
3/7 Test #3: test_predicate_pushdown .............   Passed    X.XX sec
    Start 4: test_cedar_graph_storage
4/7 Test #4: test_cedar_graph_storage ............   Passed    X.XX sec
    Start 5: test_execution_operators
5/7 Test #5: test_execution_operators ............   Passed    X.XX sec
    Start 6: test_write_operators
6/7 Test #6: test_write_operators ................   Passed    X.XX sec

100% tests passed, 0 tests failed out of 7
```

- [ ] **9.2** If any test fails, fix and rerun. Do not proceed until 100 % green.

- [ ] **9.3** Commit the fix (if any).

---

### Phase 10 — Update `PredicatePushdown` to prefer label-only index when no property predicate exists

**Time:** ~5 min  
**Goal:** If a `MATCH (n:Person)` has no `WHERE` property predicate, still replace the `NodeScan` with a `BuildLabelIndex` when the label is present.

- [ ] **10.1** Open `src/cypher/execution_plan.cc`.

In `ExecutionPlanBuilder::BuildScanForPattern`, replace the `NodeScan` creation block (around line 1277–1283):

```cpp
      // Regular scan with properties for point lookup optimization
      scan = std::make_shared<NodeScan>(
          node.variable,
          node.labels.empty() ? std::nullopt : std::optional(node.labels[0]),
          node.properties);
```

With:

```cpp
      // Prefer label index scan if a label is present and no properties
      if (!node.labels.empty() && node.properties.empty()) {
        scan = BuildLabelIndex(node.variable, node.labels[0]);
      } else {
        scan = std::make_shared<NodeScan>(
            node.variable,
            node.labels.empty() ? std::nullopt : std::optional(node.labels[0]),
            node.properties);
      }
```

- [ ] **10.2** Build.

```bash
cd <repo-root>/build && make -j$(sysctl -n hw.ncpu) 2>&1 | tail -20
```

**Expected:** clean build.

- [ ] **10.3** Commit.

```bash
git -C <repo-root> add src/cypher/execution_plan.cc
git -C <repo-root> commit -m "subplan-e: use label index scan for MATCH (n:Label) with no properties"
```

---

## Verification Checklist

- [ ] `IndexScan::Init` no longer iterates `1..max_entity_id` when a matching index entry exists.
- [ ] `IndexScan` falls back to range scan when the index is empty or the predicate is not `EQ`.
- [ ] `LsmEngine::Put` updates both `label_index_` and `property_index_`.
- [ ] `LsmEngine::Delete` removes the entity from both indexes.
- [ ] `CreateOperator` persists labels so they are indexed.
- [ ] `ExecutionPlanBuilder::BuildLabelIndex` exists and returns an `IndexScan`.
- [ ] `ExecutionPlanBuilder::BuildPropertyIndex` exists and returns an `IndexScan`.
- [ ] `ApplyPredicatePushdown` preserves the original `NodeScan` label.
- [ ] All new and existing tests pass (`ctest` 100 %).

---

## Rollback Plan

If anything breaks in production:

1. Revert `src/cypher/execution_plan.cc` to restore the old `IndexScan::Init` (fallback scan always works).
2. Revert `src/storage/lsm_engine.cc` to remove index maintenance (indexes are in-memory only, so no on-disk corruption).
3. Revert `src/cypher/operators/write_operators.cc` to stop writing labels.

The fallback path in `IndexScan::Init` means even with a partial revert, queries continue to return correct results (just slower).

---

## Future Work (out of scope for this sub-plan)

- Persist indexes to SST metadata or a separate index file.
- Support range queries (`<`, `>`, `<=`, `>=`) on the property index (currently only `EQ` uses the index).
- Composite indexes `(label, property)`.
- Concurrent index rebuild during `Open()`.
- Move from `std::map` to `absl::btree_map` or a custom B+ tree for larger working sets.
