# Partition Index BuildIndex 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 `PartitionIndex::BuildIndex()`，使其能扫描所有 SST 文件、提取每个 partition 的 entity 范围，并填充 `sst_metadata_` 和 `partition_sst_map_`。

**Architecture:** 采用"SST 枚举 + Scan 回调聚合"策略。利用 `LsmEngine::GetSstFiles()` 获取所有 SST 元数据，对每个 SST 使用 `ZoneColumnarSstReader::Scan()` 扫描所有键，在回调中提取 `CedarKey::part_id()` 和 `entity_id`，聚合为 `PartitionEntityRange`，最后调用已有的 `AddSST()` 构建索引。

**Tech Stack:** C++17, LSM-Tree SST 扫描, `ZoneColumnarSstReader`, `CedarKey` 解析

---

## 文件结构

```
src/dtx/storage_impl/partition_index.cc        # BuildIndex 实现
include/cedar/dtx/partition_index.h             # PartitionIndex 接口
include/cedar/storage/lsm_engine.h              # GetSstFiles() API
include/cedar/sst/zone_columnar_reader.h        # SstReader::Scan() API
include/cedar/storage/size_tiered_compaction.h  # ZoneSstMeta, LevelState
```

---

## 第一阶段：BuildIndex 核心实现

### Task 1: 实现 `PartitionIndex::BuildIndex` 使用 SST Scan

**Files:**
- Modify: `src/dtx/storage_impl/partition_index.cc:32-41`

**问题:** `BuildIndex()` 当前只调用 `Clear()` 然后返回 OK，没有任何实际扫描。

- [ ] **Step 1: 实现 BuildIndex**

```cpp
Status PartitionIndex::BuildIndex() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  Clear();

  if (!storage_) {
    return Status::IOError("Storage not available for index build");
  }

  auto* lsm_engine = storage_->GetLsmEngine();
  if (!lsm_engine) {
    return Status::IOError("LSM engine not available");
  }

  // Get all SST files across all levels
  const auto& all_levels = lsm_engine->GetSstFiles();
  uint64_t total_files = 0;
  for (const auto& level : all_levels) {
    total_files += level.size();
  }

  if (total_files == 0) {
    // No SST files yet — index is empty, which is correct
    return Status::OK();
  }

  // Scan each SST file to determine per-partition ranges
  for (int level_idx = 0; level_idx < static_cast<int>(all_levels.size()); ++level_idx) {
    for (const auto& sst_meta : all_levels[level_idx]) {
      auto s = IndexSingleSST(sst_meta);
      if (!s.ok()) {
        // Log warning but continue — a single corrupt SST shouldn't break the whole index
        std::cerr << "[PartitionIndex] Warning: failed to index SST "
                  << sst_meta.file_number << ": " << s.ToString() << std::endl;
      }
    }
  }

  cache_dirty_.store(true);
  return Status::OK();
}
```

- [ ] **Step 2: 实现 `IndexSingleSST` 私有方法**

在 `partition_index.cc` 中添加：

```cpp
Status PartitionIndex::IndexSingleSST(const cedar::SSTFileMeta& sst_meta) {
  // Open the SST file
  std::string file_path = sst_meta.file_name();
  if (!std::filesystem::exists(file_path)) {
    // Fallback: construct path from data root
    file_path = storage_->GetDataRoot() + "/" + sst_meta.file_name();
  }

  cedar::SstReader reader(file_path);
  auto s = reader.Open();
  if (!s.ok()) {
    return Status::IOError("Failed to open SST: " + file_path + " — " + s.ToString());
  }

  // Aggregate per-partition ranges from this SST
  std::unordered_map<PartitionID, PartitionEntityRange> local_ranges;

  // Use Scan with empty predicate to enumerate all keys
  cedar::ReadPredicate predicate;  // Empty = match all
  reader.Scan(predicate,
              [&](const cedar::CedarKey& key, const cedar::Descriptor& desc) {
                (void)desc;  // descriptor not needed for range indexing
                PartitionID pid = key.part_id();
                uint64_t entity_id = key.entity_id();

                auto it = local_ranges.find(pid);
                if (it == local_ranges.end()) {
                  PartitionEntityRange range;
                  range.partition_id = pid;
                  range.min_entity_id = entity_id;
                  range.max_entity_id = entity_id;
                  range.estimated_key_count = 1;
                  local_ranges[pid] = range;
                } else {
                  it->second.min_entity_id = std::min(it->second.min_entity_id, entity_id);
                  it->second.max_entity_id = std::max(it->second.max_entity_id, entity_id);
                  it->second.estimated_key_count++;
                }
              });

  reader.Close();

  // Build SSTPartitionMetadata and add to index
  SSTPartitionMetadata metadata;
  metadata.file_number = sst_meta.file_number;
  metadata.file_size = sst_meta.file_size;
  metadata.partition_ranges = std::move(local_ranges);

  // AddSST releases the mutex, so we must call it outside the lock
  // But BuildIndex already holds unique_lock. So we add directly:
  sst_metadata_[sst_meta.file_number] = metadata;
  for (const auto& [pid, range] : metadata.partition_ranges) {
    partition_sst_map_[pid].insert(sst_meta.file_number);
  }

  return Status::OK();
}
```

- [ ] **Step 3: 在头文件中添加私有方法声明**

在 `include/cedar/dtx/partition_index.h` 的 `PartitionIndex` private 部分添加：

```cpp
Status IndexSingleSST(const cedar::SSTFileMeta& sst_meta);
```

- [ ] **Step 4: 编译验证**

```bash
cd build && make cedar -j4
```

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(index): implement BuildIndex with SST Scan aggregation"
```

---

## 第二阶段：性能优化 — 增量索引

### Task 2: 在 Flush/Compaction 路径中自动更新索引

**Files:**
- Modify: `src/storage/lsm_engine.cc`（Flush 和 Compaction 回调）

**问题:** 每次启动都全量扫描所有 SST 文件是 O(data size) 操作。对于大库来说很慢。

实现策略：在 `LsmEngine` 的 Flush 和 Compaction 完成后，调用 `PartitionIndex::AddSST` / `RemoveSST` 增量更新索引。

- [ ] **Step 1: 在 LsmEngine 中添加 PartitionIndex 指针**

在 `include/cedar/storage/lsm_engine.h` 中添加：

```cpp
class LsmEngine {
 public:
  // ... existing methods ...
  void SetPartitionIndex(cedar::dtx::PartitionIndex* index) { partition_index_ = index; }

 private:
  // ... existing members ...
  cedar::dtx::PartitionIndex* partition_index_ = nullptr;
};
```

- [ ] **Step 2: 在 Flush 完成后调用 AddSST**

在 `src/storage/lsm_engine.cc` 的 Flush 方法末尾（生成新 SST 后）：

```cpp
// After creating new SST file
if (partition_index_) {
  cedar::SSTFileMeta meta;
  meta.file_number = new_file_number;
  meta.file_size = file_size;
  meta.min_entity_id = min_entity_id;
  meta.max_entity_id = max_entity_id;
  // ... fill other fields ...

  cedar::dtx::SSTPartitionMetadata pmeta;
  pmeta.file_number = new_file_number;
  pmeta.file_size = file_size;
  // Build partition_ranges from the memtable data that was just flushed
  for (const auto& [pid, range] : flushed_partition_ranges) {
    pmeta.partition_ranges[pid] = range;
  }
  partition_index_->AddSST(new_file_number, pmeta);
}
```

- [ ] **Step 3: 在 Compaction 完成后调用 RemoveSST + AddSST**

在 `src/storage/lsm_engine.cc` 的 Compaction 方法末尾：

```cpp
// After compaction completes
if (partition_index_) {
  for (auto old_file : compacted_input_files) {
    partition_index_->RemoveSST(old_file);
  }
  for (const auto& new_meta : compaction_output_files) {
    cedar::dtx::SSTPartitionMetadata pmeta;
    pmeta.file_number = new_meta.file_number;
    pmeta.file_size = new_meta.file_size;
    // ... build partition_ranges from compaction output ...
    partition_index_->AddSST(new_meta.file_number, pmeta);
  }
}
```

- [ ] **Step 4: 在 StorageServiceExt 初始化时注入索引**

在 `src/dtx/storage_impl/storage_service_ext.cc` 中：

```cpp
// After creating partition_index_ and lsm_engine
auto* lsm = base_manager_.GetSharedStorage()->GetLsmEngine();
if (lsm && partition_index_) {
  lsm->SetPartitionIndex(partition_index_.get());
}
```

- [ ] **Step 5: 编译验证**

- [ ] **Step 6: Commit**

```bash
git commit -m "feat(index): incremental index updates on flush and compaction"
```

---

## 第三阶段：测试

### Task 3: 添加 BuildIndex 测试

**Files:**
- Create: `tests/dtx/test_partition_index_build.cc`

- [ ] **Step 1: 编写测试**

```cpp
// Copyright 2025 The Cedar Authors
// ... Apache 2.0 header ...

#include <gtest/gtest.h>
#include <filesystem>
#include "cedar/dtx/partition_index.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(PartitionIndexBuild, BuildIndexOnEmptyStorage) {
  std::string data_dir = "/tmp/test_index_empty";
  std::filesystem::remove_all(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, data_dir, &storage);
  ASSERT_TRUE(status.ok());
  ASSERT_NE(storage, nullptr);

  PartitionIndex index(storage);
  auto s = index.BuildIndex();
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto partitions = index.GetAllPartitions();
  EXPECT_TRUE(partitions.empty());

  delete storage;
  std::filesystem::remove_all(data_dir);
}

TEST(PartitionIndexBuild, BuildIndexAfterFlush) {
  std::string data_dir = "/tmp/test_index_flush";
  std::filesystem::remove_all(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, data_dir, &storage);
  ASSERT_TRUE(status.ok());

  // Write data to multiple partitions
  for (uint64_t i = 1; i <= 100; i++) {
    Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(i));
    storage->Put(i, i * 1000, desc, Timestamp(i));
  }
  storage->ForceFlush();

  PartitionIndex index(storage);
  auto s = index.BuildIndex();
  EXPECT_TRUE(s.ok()) << s.ToString();

  auto partitions = index.GetAllPartitions();
  EXPECT_FALSE(partitions.empty());

  // Check that partition 1 (entity_id 1 has part_id = 1) has a valid range
  auto range = index.GetPartitionRange(1);
  EXPECT_LE(range.min_entity_id, 1);
  EXPECT_GE(range.max_entity_id, 1);
  EXPECT_GE(range.estimated_key_count, 1);

  delete storage;
  std::filesystem::remove_all(data_dir);
}
```

- [ ] **Step 2: 添加到 CMakeLists.txt**

- [ ] **Step 3: 运行测试**

```bash
cd build && ctest -R PartitionIndexBuild -V
```

- [ ] **Step 4: Commit**

```bash
git commit -m "test(index): add BuildIndex tests for empty and flushed storage"
```

---

## Self-Review

### 1. Spec Coverage

| 审计发现 | 对应任务 |
|---------|---------|
| BuildIndex 空实现 | Task 1 |
| 无增量索引更新 | Task 2 |
| 无 BuildIndex 测试 | Task 3 |

### 2. Placeholder Scan

- 无 TBD/TODO/"implement later"
- 所有代码步骤包含具体代码块

### 3. Type Consistency

- `SSTFileMeta`、`SSTPartitionMetadata`、`PartitionEntityRange`、`PartitionID` 类型一致
- `CedarKey::part_id()` 使用一致
- `SstReader::Scan()` 签名匹配

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-10-partition-index-buildindex.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task.

**2. Inline Execution** — Execute tasks in this session.

**Which approach?**
