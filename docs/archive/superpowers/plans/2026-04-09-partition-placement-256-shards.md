# Partition Placement with 256 Shards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make partition count configurable (default 256) and implement explicit partition-to-node placement so that one storage node can host multiple partitions, following NebulaGraph's shared-nothing sharding model.

**Architecture:** Introduce an `AssignPartitionsToNodes()` API on `PartitionManager` that uses round-robin placement. Replace the naive `pid % node_count` routing in the benchmark with a leader lookup against `PartitionMeta`. Benchmark config moves from 64 to 256 partitions while 3 nodes each host ~85 partitions.

**Tech Stack:** C++17, CedarGraph `PartitionManager`, `PartitionMeta`, `NodeID`

---

## File Structure

| File | Responsibility |
|------|----------------|
| `include/cedar/dtx/partition.h` | Adds `AssignPartitionsToNodes()` declaration to `PartitionManager` |
| `src/dtx/coordinator/partition.cc` | Implements round-robin assignment, updates `partition_metas_` and `node_partitions_` |
| `tests/test_mth_3node_temporal_performance.cc` | Bumps `num_partitions` to 256, calls assignment at init, replaces `%` routing with leader lookup |

---

## Task 1: Add `AssignPartitionsToNodes` to `PartitionManager` interface

**Files:**
- Modify: `include/cedar/dtx/partition.h:380-383`
- Modify: `src/dtx/coordinator/partition.cc:257-277`
- Test: `tests/test_mth_3node_temporal_performance.cc`

- [ ] **Step 1: Declare the new API in the header**

In `include/cedar/dtx/partition.h`, insert the new method declaration after `SetPartitionLeader`:

```cpp
  // Assign all partitions to the given nodes using round-robin placement.
  // Clears any existing node->partition mappings before assigning.
  Status AssignPartitionsToNodes(const std::vector<NodeID>& node_ids);
```

- [ ] **Step 2: Implement round-robin assignment in `partition.cc`**

In `src/dtx/coordinator/partition.cc`, add the implementation after `SetPartitionLeader`:

```cpp
Status PartitionManager::AssignPartitionsToNodes(const std::vector<NodeID>& node_ids) {
  if (node_ids.empty()) {
    return Status::InvalidArgument("PartitionManager", "node_ids cannot be empty");
  }

  std::unique_lock<std::shared_mutex> lock(meta_mutex_);
  std::unique_lock<std::shared_mutex> node_lock(node_partition_mutex_);

  // Clear existing node->partition mappings
  node_partitions_.clear();

  // Round-robin assignment
  for (PartitionID pid = 0; pid < num_partitions_; ++pid) {
    auto meta_it = partition_metas_.find(pid);
    if (meta_it == partition_metas_.end()) {
      continue;
    }
    NodeID node_id = node_ids[pid % node_ids.size()];
    meta_it->second->primary_node = node_id;
    node_partitions_[node_id].push_back(pid);
  }

  return Status::OK();
}
```

- [ ] **Step 3: Build the library to check for compile errors**

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar -j4
```

Expected: `[100%] Built target cedar` with no errors.

- [ ] **Step 4: Commit**

```bash
git add include/cedar/dtx/partition.h src/dtx/coordinator/partition.cc
git commit -m "feat(partition): add AssignPartitionsToNodes round-robin placement API"
```

---

## Task 2: Update benchmark to use 256 partitions and explicit placement

**Files:**
- Modify: `tests/test_mth_3node_temporal_performance.cc:38-40, 156-197`
- Test: `tests/test_mth_3node_temporal_performance.cc` (run the benchmark)

- [ ] **Step 1: Bump `num_partitions` to 256**

In `tests/test_mth_3node_temporal_performance.cc`, change:

```cpp
  PartitionID num_partitions = 256;  // was 64
```

- [ ] **Step 2: Initialize partition placement after creating nodes**

In the `MTH3NodeCluster` constructor, after the node creation loop (around line 178), add:

```cpp
    // Assign 256 partitions to 3 nodes via round-robin
    std::vector<NodeID> node_ids;
    for (int i = 0; i < config.node_count; ++i) {
      node_ids.push_back(static_cast<NodeID>(i));
    }
    status = partition_manager_->AssignPartitionsToNodes(node_ids);
    if (!status.ok()) {
      throw std::runtime_error("Failed to assign partitions: " + status.ToString());
    }
```

- [ ] **Step 3: Replace modulo routing with leader lookup**

Replace `RouteToNode` with:

```cpp
  // Route key to node based on partition leader assignment
  StorageNode* RouteToNode(const CedarKey& key) {
    PartitionID pid = partition_manager_->GetPartition(key);
    NodeID node_id = partition_manager_->GetPartitionLeader(pid);
    if (node_id == kInvalidNodeID || node_id >= static_cast<NodeID>(nodes_.size())) {
      // Fallback for safety
      node_id = static_cast<NodeID>(pid % nodes_.size());
    }
    return nodes_[static_cast<size_t>(node_id)].get();
  }
```

- [ ] **Step 4: Build the benchmark executable**

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_mth_3node_temporal_performance -j4
```

Expected: `[100%] Built target test_mth_3node_temporal_performance` with no errors.

- [ ] **Step 5: Run benchmark to verify no crashes and 256 partitions active**

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && ./test_mth_3node_temporal_performance 2>&1 | grep -E "Configuration:|Partitions:|Benchmark completed|Node Distribution"
```

Expected output contains:
```
  Partitions: 256
```
and ends with:
```
✅ Benchmark completed successfully!
```

- [ ] **Step 6: Commit**

```bash
git add tests/test_mth_3node_temporal_performance.cc
git commit -m "feat(benchmark): use 256 partitions with explicit round-robin placement"
```

---

## Self-Review Checklist

**1. Spec coverage:**
- Configurable partition count (256) → Task 2, Step 1
- One node can host multiple partitions → Task 1 implements round-robin; Task 2, Step 3 routes via leader lookup which respects multi-partition-per-node
- NebulaGraph-style shared-nothing sharding → explicit placement table instead of hard-coded modulo

**2. Placeholder scan:**
- No TBD/TODO/fill-in-details found.
- All code blocks contain complete, compilable code.

**3. Type consistency:**
- `NodeID` is `uint32_t` from `cedar/dtx/types.h`
- `AssignPartitionsToNodes` returns `Status` consistent with other `PartitionManager` methods
- `partition_metas_` and `node_partitions_` updated consistently with existing `SetPartitionLeader`

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-09-partition-placement-256-shards.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
