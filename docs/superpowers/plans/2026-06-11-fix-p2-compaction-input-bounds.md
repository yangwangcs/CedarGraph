# P2 Fix: Compaction Manifest Ordering + Input Bounds Validation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Fix compaction to save manifest before deleting old files, and add upper-bound validation on user inputs (query length, batch size, timeout).

**Architecture:** In compaction, reorder operations: update in-memory metadata → SaveManifest() → delete physical files. Add lightweight bounds checks at gRPC handler entry points using configurable limits.

**Tech Stack:** C++17, gtest

---

## File Map

| File | Responsibility |
|------|----------------|
| `src/storage/size_tiered_compaction.cc` | Fix DoZoneCompaction ordering |
| `src/service/graph_service_router.cc` | Add query length, timeout, parameter bounds |
| `src/dtx/storage_impl/storage_service_impl.cc` | Add batch size, scan bounds |
| `include/cedar/storage/cedar_config.h` | Add new config fields for limits |
| `src/storage/cedar_config.cc` | Parse new limit fields |
| `tests/dtx/test_storage_service_impl.cc` | Add input bounds tests |

---

### Task 1: Fix Compaction Delete-Before-Manifest

**Files:**
- Modify: `src/storage/size_tiered_compaction.cc:830-860`

- [ ] **Step 1: Reorder operations in DoZoneCompaction**

Current order (lines 830-860):
1. Add output to levels
2. Delete input files
3. Delete overlapping files
4. SaveManifest()

Replace the entire block (lines 830-860) with:

```cpp
  // Step 1: Update in-memory metadata (add output, remove inputs/overlaps)
  {
    std::lock_guard<std::mutex> lock(levels_mutex_);
    levels_[task.output_level].files.push_back(output_meta);
    for (const auto& f : task.input_files) {
      RemoveFileFromLevel(f.file_number, task.input_level);
    }
    for (const auto& f : task.overlapping_files) {
      RemoveFileFromLevel(f.file_number, task.output_level);
    }
  }

  // Step 2: Persist manifest BEFORE deleting physical files
  s = SaveManifest();
  if (!s.ok()) {
    LOG(ERROR) << "SaveManifest failed after compaction: " << s.ToString();
    // If manifest save fails, do NOT delete old files so we can recover
    return s;
  }

  // Step 3: Now safe to delete physical files
  for (const auto& f : task.input_files) {
    env_->RemoveFile(GetSstPath(f.file_number));
  }
  for (const auto& f : task.overlapping_files) {
    env_->RemoveFile(GetSstPath(f.file_number));
  }
```

- [ ] **Step 2: Build cedar target**

```bash
cd <repo-root>/build && cmake --build . --target cedar -j$(sysctl -n hw.ncpu)
```

- [ ] **Step 3: Commit**

```bash
cd <repo-root> && git add src/storage/size_tiered_compaction.cc && git commit -m "fix(storage): save manifest before deleting old SST files during compaction"
```

---

### Task 2: Add Config Fields for Input Limits

**Files:**
- Modify: `include/cedar/storage/cedar_config.h`
- Modify: `src/storage/cedar_config.cc`

- [ ] **Step 1: Add limit fields to CedarConfig struct**

In `include/cedar/storage/cedar_config.h`, add to the struct:

```cpp
  struct Limits {
    size_t max_query_length = 1024 * 1024;        // 1 MB
    size_t max_batch_items = 10000;
    size_t max_parameter_count = 1000;
    size_t max_parameter_value_length = 64 * 1024;  // 64 KB
    uint32_t max_timeout_ms = 300000;               // 5 minutes
    size_t max_heartbeat_partitions = 100000;
  } limits;
```

- [ ] **Step 2: Parse limits in LoadFromFile**

In `src/storage/cedar_config.cc`, in the `LoadFromFile` rapidjson parsing, add:

```cpp
    if (doc.HasMember("limits") && doc["limits"].IsObject()) {
      const auto& lim = doc["limits"];
      get_size_t(lim, "max_query_length", config.limits.max_query_length);
      get_size_t(lim, "max_batch_items", config.limits.max_batch_items);
      get_size_t(lim, "max_parameter_count", config.limits.max_parameter_count);
      get_size_t(lim, "max_parameter_value_length", config.limits.max_parameter_value_length);
      get_uint32(lim, "max_timeout_ms", config.limits.max_timeout_ms);
      get_size_t(lim, "max_heartbeat_partitions", config.limits.max_heartbeat_partitions);
    }
```

- [ ] **Step 3: Emit limits in SaveToFile**

In `SaveToFile`, add the `limits` section to the PrettyWriter output.

- [ ] **Step 4: Build and run config tests**

```bash
cd <repo-root>/build && cmake --build . --target test_cedar_config -j$(sysctl -n hw.ncpu) && ./tests/test_cedar_config
```
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
cd <repo-root> && git add include/cedar/storage/cedar_config.h src/storage/cedar_config.cc && git commit -m "feat(config): add configurable input limits for DoS protection"
```

---

### Task 3: Add Input Validation to GraphD

**Files:**
- Modify: `src/service/graph_service_router.cc`

- [ ] **Step 1: Add ValidateQueryInput helper**

Add near the top of the file (after includes):

```cpp
namespace {
grpc::Status ValidateQueryInput(const cedar::graph::QueryRequest* req,
                                const cedar::CedarConfig& config) {
  if (req->query().size() > config.limits.max_query_length) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Query exceeds max length");
  }
  if (req->timeout_ms() > config.limits.max_timeout_ms) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Timeout exceeds max allowed");
  }
  if (static_cast<size_t>(req->parameters_size()) > config.limits.max_parameter_count) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Too many parameters");
  }
  for (const auto& p : req->parameters()) {
    if (p.value().size() > config.limits.max_parameter_value_length) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Parameter value too large");
    }
  }
  return grpc::Status::OK;
}
}  // namespace
```

- [ ] **Step 2: Call validator in ExecuteQuery handler**

After `CheckAuth` and before parsing, add:
```cpp
  if (auto st = ValidateQueryInput(request, config_); !st.ok()) return st;
```

- [ ] **Step 3: Build cedar_graphd target**

```bash
cd <repo-root>/build && cmake --build . --target cedar_graphd -j$(sysctl -n hw.ncpu)
```

- [ ] **Step 4: Commit**

```bash
cd <repo-root> && git add src/service/graph_service_router.cc && git commit -m "fix(graphd): add input bounds validation (query length, timeout, params)"
```

---

### Task 4: Add Input Validation to StorageD

**Files:**
- Modify: `src/dtx/storage_impl/storage_service_impl.cc`

- [ ] **Step 1: Add ValidateBatchPut helper**

```cpp
namespace {
grpc::Status ValidateBatchPut(const cedar::storage::BatchPutRequest* req,
                              const cedar::CedarConfig& config) {
  if (static_cast<size_t>(req->items_size()) > config.limits.max_batch_items) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Batch too large");
  }
  return grpc::Status::OK;
}
}  // namespace
```

- [ ] **Step 2: Call validator in BatchPut handler**

After `CheckAuth` and before processing:
```cpp
  if (auto st = ValidateBatchPut(request, config_); !st.ok()) return st;
```

- [ ] **Step 3: Build cedar target**

```bash
cd <repo-root>/build && cmake --build . --target cedar -j$(sysctl -n hw.ncpu)
```

- [ ] **Step 4: Commit**

```bash
cd <repo-root> && git add src/dtx/storage_impl/storage_service_impl.cc && git commit -m "fix(storage): add batch size limit validation"
```

---

### Task 5: Full Regression Test

- [ ] **Step 1: Run full test suite**

```bash
cd <repo-root>/build && ctest --output-on-failure -j$(sysctl -n hw.ncpu)
```
Expected: 1285/1285 passed, 0 failed.
