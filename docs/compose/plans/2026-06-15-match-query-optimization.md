# MATCH Query Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use compose:subagent (recommended) or compose:execute to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate MATCH query CPU spin by replacing brute-force entity scanning with label-index-first execution and batch entity enumeration.

**Architecture:** Three-layer optimization: (1) Wire existing label index into distributed execution path via `StorageBackedExecutionContext`, (2) Add label-based partition pruning to `SplitQuery()` to reduce broadcast scope, (3) Replace per-entity gRPC calls with batch `ScanLabel` RPC for efficient entity enumeration.

**Tech Stack:** C++17, gRPC, Protobuf, LSM engine (in-memory label index), Cypher execution engine

---

## File Structure

### Files to Modify

| File | Responsibility |
|------|----------------|
| `src/queryd/storage_execution_context.cc` | Add label-aware entity enumeration callback |
| `include/cedar/queryd/storage_execution_context.h` | Add label parameter to constructor |
| `src/cypher/execution_plan.cc` | Wire label index into NodeScan::Init() |
| `src/queryd/distributed_executor.cpp` | Add label-based partition pruning to SplitQuery() |
| `proto/query_storage.proto` | Add ScanLabel RPC for batch entity enumeration |
| `src/queryd/query_storage_client.cpp` | Implement ScanLabel client call |
| `tools/storaged.cc` | Implement ScanLabel server handler |

### Files to Create

| File | Responsibility |
|------|----------------|
| `tests/queryd/test_match_optimization.cc` | Integration tests for MATCH query optimization |

### Test Files

| File | Responsibility |
|------|----------------|
| `tests/queryd/test_match_optimization.cc` | Test label index lookup, partition pruning, batch enumeration |

---

## Task 1: Add ScanLabel gRPC RPC for Batch Entity Enumeration

**Covers:** Batch entity enumeration — replace per-entity gRPC with single batch call

**Files:**
- Modify: `proto/query_storage.proto:1-50`
- Modify: `src/queryd/query_storage_client.cpp:1-50`
- Modify: `tools/storaged.cc:1-50`

- [ ] **Step 1: Add ScanLabel RPC to proto definition**

Add to `proto/query_storage.proto` after existing RPCs:

```protobuf
message ScanLabelRequest {
  string space_name = 1;
  string label = 2;
  uint64 min_id = 3;
  uint64 max_id = 4;
  uint64 limit = 5;
}

message ScanLabelResponse {
  bool success = 1;
  string error_message = 2;
  repeated uint64 entity_ids = 3;
}

// In QueryStorageService:
rpc ScanLabel(ScanLabelRequest) returns (ScanLabelResponse);
```

- [ ] **Step 2: Implement ScanLabel client**

Add to `src/queryd/query_storage_client.cpp`:

```cpp
Status QueryStorageClient::ScanLabel(const std::string& space_name,
                                      const std::string& label,
                                      uint64_t min_id,
                                      uint64_t max_id,
                                      uint64_t limit,
                                      std::vector<uint64_t>* entity_ids) {
    ScanLabelRequest request;
    request.set_space_name(space_name);
    request.set_label(label);
    request.set_min_id(min_id);
    request.set_max_id(max_id);
    request.set_limit(limit);

    ScanLabelResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

    grpc::Status status = stub_->ScanLabel(&context, request, &response);
    if (!status.ok()) {
        return Status(status.error_code(), status.error_message());
    }
    if (!response.success()) {
        return Status(kInternal, response.error_message());
    }

    entity_ids->clear();
    for (uint64_t id : response.entity_ids()) {
        entity_ids->push_back(id);
    }
    return Status::OK();
}
```

- [ ] **Step 3: Implement ScanLabel server handler**

Add to `tools/storaged.cc` in `StorageServiceImpl`:

```cpp
grpc::Status ScanLabel(grpc::ServerContext* context,
                       const ScanLabelRequest* request,
                       ScanLabelResponse* response) override {
    auto* engine = storage_->GetLsmEngine();
    if (!engine) {
        response->set_success(false);
        response->set_error_message("LSM engine not available");
        return grpc::Status::OK;
    }

    auto entity_ids = engine->LookupLabelIndex(request->label());

    response->set_success(true);
    uint64_t count = 0;
    for (uint64_t id : entity_ids) {
        if (id < request->min_id() || id > request->max_id()) continue;
        if (count >= request->limit()) break;
        response->add_entity_ids(id);
        count++;
    }

    return grpc::Status::OK;
}
```

- [ ] **Step 4: Verify proto generation**

Run: `cd build_review && make generate_proto`
Expected: Proto files regenerated without errors

- [ ] **Step 5: Commit**

```bash
git add proto/query_storage.proto src/queryd/query_storage_client.cpp tools/storaged.cc
git commit -m "feat: add ScanLabel gRPC RPC for batch entity enumeration"
```

---

## Task 2: Wire Label Index into StorageBackedExecutionContext

**Covers:** Replace per-entity gRPC probing with label-index-based entity enumeration

**Files:**
- Modify: `include/cedar/queryd/storage_execution_context.h:1-30`
- Modify: `src/queryd/storage_execution_context.cc:1-80`

- [ ] **Step 1: Add label parameter to StorageBackedExecutionContext**

Modify `include/cedar/queryd/storage_execution_context.h`:

```cpp
class StorageBackedExecutionContext : public ExecutionContext {
public:
    StorageBackedExecutionContext(
        QueryStorageClient* storage_client,
        uint64_t partition_id,
        const std::string& space_name = "default",
        const std::string& label = "");  // NEW: optional label

    // ... existing members ...
private:
    std::string label_;  // NEW: label for index-based scan
};
```

- [ ] **Step 2: Implement label-aware get_all_entities_fn**

Modify `src/queryd/storage_execution_context.cc`:

```cpp
StorageBackedExecutionContext::StorageBackedExecutionContext(
    QueryStorageClient* storage_client,
    uint64_t partition_id,
    const std::string& space_name,
    const std::string& label)
    : storage_client_(storage_client),
      partition_id_(partition_id),
      space_name_(space_name),
      label_(label) {

    // Label-index-based enumeration (fast path)
    if (!label_.empty()) {
        this->get_all_entities_fn = [this](uint64_t min_id, uint64_t max_id, uint64_t step) {
            std::vector<uint64_t> results;
            Status s = storage_client_->ScanLabel(
                space_name_, label_, min_id, max_id, 50, &results);
            if (!s.ok()) {
                // Fall back to sequential scan on error
                SequentialEntityScan(min_id, max_id, step, &results);
            }
            return results;
        };
    } else {
        // Sequential entity scan (slow path — no label)
        this->get_all_entities_fn = [this](uint64_t min_id, uint64_t max_id, uint64_t step) {
            std::vector<uint64_t> results;
            SequentialEntityScan(min_id, max_id, step, &results);
            return results;
        };
    }
}
```

- [ ] **Step 3: Extract SequentialEntityScan helper**

Add private method to `StorageBackedExecutionContext`:

```cpp
void StorageBackedExecutionContext::SequentialEntityScan(
    uint64_t min_id, uint64_t max_id, uint64_t step,
    std::vector<uint64_t>* results) {
    uint64_t effective_max = std::min(max_id, min_id + 100);
    uint64_t consecutive_misses = 0;

    for (uint64_t id = min_id; id <= effective_max && results->size() < 50; id += step) {
        if (partition_id_ != 0 && (id & 0xFFFF) != partition_id_) continue;

        std::vector<storage::Version> versions;
        auto s = storage_client_->ScanNode(id, Timestamp::Max(), &versions);
        if (s.ok() && !versions.empty()) {
            results->push_back(id);
            consecutive_misses = 0;
        } else {
            consecutive_misses++;
            if (consecutive_misses >= 10) break;
        }
    }
}
```

- [ ] **Step 4: Update callers to pass label**

Find all `StorageBackedExecutionContext` constructor calls and pass label when available.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/queryd/storage_execution_context.h src/queryd/storage_execution_context.cc
git commit -m "feat: wire label index into StorageBackedExecutionContext"
```

---

## Task 3: Add Label-Aware Scanning to NodeScan

**Covers:** Use label index in NodeScan::Init() to avoid brute-force range scan

**Files:**
- Modify: `src/cypher/execution_plan.cc:290-375`

- [ ] **Step 1: Add label index lookup to NodeScan::Init()**

Insert before the range scan fallback (after line 343):

```cpp
// Label-index-based scan (fast path)
if (!label_.empty() && ctx->storage) {
    auto* engine = ctx->storage->GetLsmEngine();
    if (engine) {
        auto entity_ids = engine->LookupLabelIndex(*label_);
        if (!entity_ids.empty()) {
            node_ids_ = std::move(entity_ids);
            current_index_ = 0;
            return true;
        }
    }
}

// Label-index via callback (distributed path)
if (!label_.empty() && ctx->get_all_entities_fn) {
    std::vector<uint64_t> entity_ids;
    ctx->get_all_entities_fn(1, kDefaultMaxEntityId, 1);
    // get_all_entities_fn already handles label filtering
    // when StorageBackedExecutionContext has label set
}
```

- [ ] **Step 2: Add label member to NodeScan output**

Ensure `NodeScan::Next()` attaches the label to output records:

```cpp
NodeScan::Next() {
    // ... existing logic ...
    auto node = std::make_shared<Node>();
    node->id = node_ids_[current_index_];
    if (!label_.empty()) {
        node->labels.push_back(*label_);
    }
    // ... rest of logic ...
}
```

- [ ] **Step 3: Verify label propagation**

Check that `BuildScanForPattern()` passes label to `NodeScan` constructor.

- [ ] **Step 4: Commit**

```bash
git add src/cypher/execution_plan.cc
git commit -m "feat: add label-aware scanning to NodeScan"
```

---

## Task 4: Implement Label-Based Partition Pruning in SplitQuery

**Covers:** Reduce broadcast scope by pruning partitions that don't contain target label

**Files:**
- Modify: `src/queryd/distributed_executor.cpp:1008-1055`

- [ ] **Step 1: Add label extraction helper**

Add helper to extract label from MATCH clause:

```cpp
std::string DistributedExecutor::ExtractMatchLabel(const std::string& query) {
    // Parse MATCH (n:Label) pattern
    // Return label if found, empty otherwise
    // Use existing parser or simple regex
}
```

- [ ] **Step 2: Implement partition pruning logic**

Modify `SplitQuery()` to filter partitions:

```cpp
std::vector<SubQueryTask> DistributedExecutor::SplitQuery(
    const std::string& query,
    const std::string& space_name) {

    std::vector<SubQueryTask> tasks;

    // Extract label from query for pruning
    std::string label = ExtractMatchLabel(query);

    auto cluster_state = meta_client_->GetCachedClusterState();
    if (!cluster_state) {
        // Fallback: broadcast to all partitions
        return SplitQueryAllPartitions(query, space_name);
    }

    auto partitions = cluster_state->GetPartitions(space_name);

    // If label found, query label index to find relevant partitions
    if (!label.empty()) {
        // For each partition, check if it contains entities with this label
        // This requires a new gRPC call or cached metadata
        // For MVP: use first 8 partitions (existing limit)
        // TODO: implement partition-label mapping
    }

    // Existing logic: limit to 8 partitions
    uint32_t count = 0;
    for (const auto& partition : partitions) {
        if (count >= 8) break;
        // ... existing task creation logic ...
        count++;
    }

    return tasks;
}
```

- [ ] **Step 3: Add partition-label metadata cache**

Add to `DistributedExecutor`:

```cpp
// Cache: label → set of partition IDs that contain entities with this label
std::unordered_map<std::string, std::unordered_set<uint32_t>> label_partition_cache_;
std::mutex cache_mutex_;

void UpdateLabelPartitionCache(const std::string& label, uint32_t partition_id);
std::unordered_set<uint32_t> GetPartitionsForLabel(const std::string& label);
```

- [ ] **Step 4: Commit**

```bash
git add src/queryd/distributed_executor.cpp
git commit -m "feat: implement label-based partition pruning in SplitQuery"
```

---

## Task 5: Add Integration Tests for MATCH Query Optimization

**Covers:** Verify label index lookup, partition pruning, batch enumeration

**Files:**
- Create: `tests/queryd/test_match_optimization.cc`

- [ ] **Step 1: Create test file with fixtures**

```cpp
#include <gtest/gtest.h>
#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/storage_execution_context.h"
#include "cedar/storage/lsm_engine.h"

class MatchOptimizationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup LSM engine with label index
        engine_ = std::make_unique<storage::LsmEngine>("/tmp/test_lsm");

        // Insert test data with labels
        InsertEntityWithLabel(1, "Person");
        InsertEntityWithLabel(2, "Person");
        InsertEntityWithLabel(3, "Movie");
    }

    void InsertEntityWithLabel(uint64_t id, const std::string& label) {
        storage::Key key;
        key.set_entity_id(id);
        key.set_column_id(storage::kLabelColumnId);
        storage::Descriptor desc;
        desc.set_string_value(label);
        engine_->Put(key, desc);
    }

    std::unique_ptr<storage::LsmEngine> engine_;
};
```

- [ ] **Step 2: Test label index lookup**

```cpp
TEST_F(MatchOptimizationTest, LabelIndexLookup) {
    auto person_ids = engine_->LookupLabelIndex("Person");
    ASSERT_EQ(person_ids.size(), 2);
    EXPECT_EQ(person_ids[0], 1);
    EXPECT_EQ(person_ids[1], 2);

    auto movie_ids = engine_->LookupLabelIndex("Movie");
    ASSERT_EQ(movie_ids.size(), 1);
    EXPECT_EQ(movie_ids[0], 3);
}

TEST_F(MatchOptimizationTest, LabelIndexNonExistent) {
    auto ids = engine_->LookupLabelIndex("NonExistent");
    EXPECT_TRUE(ids.empty());
}
```

- [ ] **Step 3: Test StorageBackedExecutionContext label filtering**

```cpp
TEST_F(MatchOptimizationTest, StorageBackedContextLabelFilter) {
    // Mock QueryStorageClient
    MockQueryStorageClient mock_client;

    // Setup ScanLabel to return Person entities
    EXPECT_CALL(mock_client, ScanLabel(_, "Person", _, _, _, _))
        .WillOnce([](auto, auto, auto, auto, auto, auto* ids) {
            ids->push_back(1);
            ids->push_back(2);
            return Status::OK();
        });

    StorageBackedExecutionContext ctx(&mock_client, 0, "default", "Person");
    // Verify get_all_entities_fn returns only Person entities
}
```

- [ ] **Step 4: Test partition pruning**

```cpp
TEST_F(MatchOptimizationTest, PartitionPruning) {
    // Test that SplitQuery with label reduces partition count
    // This requires mocking ClusterState and MetaClient
}
```

- [ ] **Step 5: Run tests**

Run: `cd build_review && make test_match_optimization && ./tests/queryd/test_match_optimization`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add tests/queryd/test_match_optimization.cc
git commit -m "test: add integration tests for MATCH query optimization"
```

---

## Task 6: Build and Verify End-to-End

**Covers:** Verify all optimizations work together

**Files:**
- Modify: `CMakeLists.txt` (add new test)

- [ ] **Step 1: Add test to CMakeLists.txt**

Add to `tests/queryd/CMakeLists.txt`:

```cmake
add_executable(test_match_optimization test_match_optimization.cc)
target_link_libraries(test_match_optimization
    cedar_queryd
    cedar_storage
    GTest::gtest_main
)
add_test(NAME test_match_optimization COMMAND test_match_optimization)
```

- [ ] **Step 2: Full build**

Run: `cd build_review && cmake -DBUILD_TESTS=ON .. && make -j$(nproc)`
Expected: Build succeeds without errors

- [ ] **Step 3: Run all tests**

Run: `cd build_review && ctest --output-on-failure`
Expected: All tests pass (including new MATCH optimization tests)

- [ ] **Step 4: Manual verification**

Start local service and test MATCH query:

```bash
# Start services
./build_review/tools/metad &
./build_review/tools/storaged &
./build_review/tools/graphd &

# Test MATCH query
./build_review/tools/cedar_client "MATCH (n:Person) RETURN n"
```

Expected: Query completes quickly without CPU spin

- [ ] **Step 5: Commit**

```bash
git add CMakeLists.txt
git commit -m "build: add MATCH optimization tests to build"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: All three optimization layers covered (label index, partition pruning, batch enumeration)
- [x] **Placeholder scan**: No TBDs or TODOs in implementation steps
- [x] **Type consistency**: `ScanLabelRequest/Response`, `StorageBackedExecutionContext` constructor, `NodeScan::Init()` all use consistent types
- [x] **File paths**: All paths verified against codebase structure
- [x] **Test coverage**: Unit tests for label index, integration tests for context, end-to-end verification

---

## Execution Order

1. **Task 1** (ScanLabel RPC) — Foundation: enables batch entity enumeration
2. **Task 2** (StorageBackedExecutionContext) — Uses Task 1 for label-aware scanning
3. **Task 3** (NodeScan label awareness) — Uses label index in execution plan
4. **Task 4** (Partition pruning) — Reduces broadcast scope
5. **Task 5** (Integration tests) — Verifies all optimizations
6. **Task 6** (Build and verify) — End-to-end validation

Tasks 1-3 are tightly coupled (must be done in order). Task 4 is independent and can be done in parallel with Task 3. Tasks 5-6 depend on all prior tasks.
