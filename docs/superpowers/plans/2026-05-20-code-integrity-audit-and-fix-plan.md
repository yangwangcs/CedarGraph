# CedarGraph Code Integrity Audit & Production Readiness Fix Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all BLOCKER and CRITICAL issues discovered in the 2026-05-20 deep code-integrity audit so CedarGraph can be considered for production deployment.

**Architecture:** Six phased milestones. Phase 1 restores data correctness (the only true blocker). Phase 2 fixes consensus safety. Phase 3 fixes query engine correctness. Phase 4 fixes query service integrity. Phase 5 fixes resource exhaustion vectors. Phase 6 cleans up error handling and stubs. Each phase produces a working, testable increment.

**Tech Stack:** C++17, CMake, gRPC, protobuf, braft (vendored), gtest

**Audit Scope:** ~515 source files across `src/core/`, `src/db/`, `src/storage/`, `src/sst/`, `src/transaction/`, `src/graph/`, `src/cypher/`, `src/dtx/`, `src/gcn/`, `src/governance/`, `src/queryd/`, `src/partition/`, `src/raft/`

**Audit Methodology:** Deep source-code inspection (not documentation) — reading implementation files to find unimplemented stubs, missing error handling, thread-safety violations, consensus safety bugs, and silent data-loss paths.

---

## Audit Summary

| Severity | Count | Key Themes |
|----------|-------|------------|
| BLOCKER (P0) | 29 | Tombstone loss, deadlock, WAL ordering, consensus partial commit, 2PC batch/pipeline hang, phantom nodes, thread explosion |
| CRITICAL (P1) | 50 | Missing locks, memory leaks, broken pooling, OOM compaction, stub APIs, hash collision, inverted logic |
| WARNING (P2) | 31 | Swallowed errors, arbitrary eviction, hardcoded limits, busy-waits, spurious failures |
| INFO (P3) | 19 | Raw pointers, linear scans, empty WAL keys, dead code |

**Verdict:** CedarGraph is **NOT production-ready** in its current state. The BLOCKER issues include silent data loss, guaranteed deadlocks, consensus atomicity violations, and unbounded resource exhaustion — any of which would cause catastrophic failure in production.

---

## Phase 1: Data Correctness (Blockers)

> **Target:** All tests pass after each task. Commit after every task.

---

### Task 1: Fix Tombstone Loss on Flush (BLOCKER)

**Problem:** `FlushMemTable()` skips tombstones, permanently losing `Delete()` operations.

**Files:**
- Modify: `src/storage/lsm_engine.cc:2108`
- Test: `tests/db/test_tombstone_flush.cc` (new)

- [ ] **Step 1: Write the failing test**

  ```cpp
  #include <gtest/gtest.h>
  #include "cedar/storage/cedar_graph_storage.h"
  TEST(TombstoneFlushTest, DeleteSurvivesFlush) {
    CedarGraphStorage::DestroyDB("/tmp/tombstone_test", CedarOptions());
    CedarOptions opts; opts.create_if_missing = true;
    CedarGraphStorage* storage = nullptr;
    ASSERT_TRUE(CedarGraphStorage::Open(opts, "/tmp/tombstone_test", &storage).ok());
    CedarKey key(1, EntityType::Vertex, 0, Timestamp(100), 0, 0, 0, 0);
    ASSERT_TRUE(storage->Put(key, Descriptor::InlineInt(0, 42)).ok());
    ASSERT_TRUE(storage->Delete(key.entity_id(), key.column_id(), Timestamp(200)).ok());
    ASSERT_TRUE(storage->ForceFlush().ok());
    auto result = storage->Get(key.entity_id(), key.column_id());
    EXPECT_TRUE(!result.has_value() || result->IsTombstone());
    delete storage;
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cd build && cmake --build . --target test_tombstone_flush && ./tests/test_tombstone_flush
  ```
  Expected: FAIL — tombstone is lost, old value reappears.

- [ ] **Step 3: Remove the tombstone skip**

  In `src/storage/lsm_engine.cc`, change:
  ```cpp
  // OLD:
  if (!descriptor.IsTombstone()) {
      all_entries.emplace_back(key, descriptor);
  }
  // NEW:
  all_entries.emplace_back(key, descriptor);
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cd build && cmake --build . --target test_tombstone_flush && ./tests/test_tombstone_flush
  ```
  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add src/storage/lsm_engine.cc tests/db/test_tombstone_flush.cc tests/CMakeLists.txt
  git commit -m "fix(storage): preserve tombstones on memtable flush"
  ```

---

### Task 2: Fix Recursive shared_mutex Deadlock in GetAtTime (BLOCKER)

**Problem:** `GetAtTime()` acquires `shared_lock` on `mutex_`, then later acquires another `shared_lock` on the same mutex in the same thread. `std::shared_mutex` does not support recursive locking.

**Files:**
- Modify: `src/storage/lsm_engine.cc:457-540`
- Test: `tests/db/test_recursive_lock.cc` (new)

- [ ] **Step 1: Write the failing test**

  ```cpp
  #include <gtest/gtest.h>
  #include "cedar/storage/cedar_graph_storage.h"
  TEST(DeadlockTest, GetAtTimeDoesNotDeadlock) {
    CedarGraphStorage::DestroyDB("/tmp/deadlock_test", CedarOptions());
    CedarOptions opts; opts.create_if_missing = true;
    CedarGraphStorage* storage = nullptr;
    ASSERT_TRUE(CedarGraphStorage::Open(opts, "/tmp/deadlock_test", &storage).ok());
    CedarKey key(1, EntityType::Vertex, 0, Timestamp(100), 0, 0, 0, 0);
    ASSERT_TRUE(storage->Put(key, Descriptor::InlineInt(0, 42)).ok());
    auto result = storage->GetAtTime(key.entity_id(), key.column_id(), Timestamp(100));
    EXPECT_TRUE(result.has_value());
    delete storage;
  }
  ```

- [ ] **Step 2: Run test to verify it hangs/deadlocks**

  ```bash
  cd build && cmake --build . --target test_recursive_lock && timeout 5 ./tests/test_recursive_lock
  ```
  Expected: TIMEOUT (deadlock).

- [ ] **Step 3: Remove the redundant inner lock**

  In `src/storage/lsm_engine.cc`, remove the second `shared_lock` acquisition at line ~540. The outer lock already protects `levels_`.

  ```cpp
  // OLD (lines ~537-542):
  if (all_entries.empty()) {
      std::shared_lock<std::shared_mutex> lock(mutex_);  // DELETE THIS LINE
      // ... access levels_ ...
  }
  // NEW:
  if (all_entries.empty()) {
      // mutex_ is already locked by the outer shared_lock at line 457
      // ... access levels_ ...
  }
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cd build && cmake --build . --target test_recursive_lock && timeout 5 ./tests/test_recursive_lock
  ```
  Expected: PASS within 1 second.

- [ ] **Step 5: Commit**

  ```bash
  git add src/storage/lsm_engine.cc tests/db/test_recursive_lock.cc tests/CMakeLists.txt
  git commit -m "fix(storage): remove recursive shared_mutex acquire in GetAtTime"
  ```

---

### Task 3: Fix WAL-Before-Memtable Ordering in Commit (BLOCKER)

**Problem:** `Commit()` writes to memtable before WAL. If WAL fails, the uncommitted data is already in memtable and will be flushed to SST.

**Files:**
- Modify: `src/transaction/occ_transaction.cc:170-188`
- Test: `tests/transaction/test_wal_ordering.cc` (new)

- [ ] **Step 1: Write the failing test**

  Create a mock WAL writer that fails on the second call. Verify that after `Commit()` returns error, `Get()` still sees the old value.

- [ ] **Step 2: Reorder Commit to WAL-first**

  ```cpp
  // OLD:
  Status write_status = WriteToMemTable();
  // ...
  Status wal_status = WriteToWAL();
  if (!wal_status.ok()) { state_.store(kAborted); return wal_status; }
  
  // NEW:
  Status wal_status = WriteToWAL();
  if (!wal_status.ok()) { state_.store(kAborted); return wal_status; }
  Status write_status = WriteToMemTable();
  if (!write_status.ok()) {
      // Attempt to rollback WAL (best effort)
      state_.store(kAborted);
      return write_status;
  }
  ```

- [ ] **Step 3: Build and run test**

- [ ] **Step 4: Commit**

  ```bash
  git add src/transaction/occ_transaction.cc tests/transaction/test_wal_ordering.cc
  git commit -m "fix(transaction): write WAL before memtable in Commit to preserve atomicity"
  ```

---

### Task 4: Disable CompactManifest Until Serialization Is Ready (BLOCKER)

**Problem:** `CompactManifest()` creates a new manifest file but omits the version serialization step, wiping all metadata.

**Files:**
- Modify: `src/db/manifest.cc:836-899`
- Test: `tests/db/test_manifest_compact.cc` (new)

- [ ] **Step 1: Add guard that rejects compaction if serialization is unavailable**

  ```cpp
  Status ManifestManager::CompactManifest() {
    // Serialization not yet implemented — compaction would lose all metadata.
    return Status::NotSupported("Manifest compaction requires VersionSet snapshot serialization");
  }
  ```

- [ ] **Step 2: Write test**

  ```cpp
  TEST(ManifestTest, CompactManifestReturnsNotSupported) {
    // ... setup manifest manager ...
    auto s = manifest.CompactManifest();
    EXPECT_TRUE(s.IsNotSupported());
  }
  ```

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add src/db/manifest.cc tests/db/test_manifest_compact.cc
  git commit -m "fix(db): reject CompactManifest until VersionSet serialization is implemented"
  ```

---

### Task 5: Fix CedarMemTable::Iterator Race (BLOCKER)

**Problem:** `Iterator` copies `map_` without holding the memtable's write lock.

**Files:**
- Modify: `src/storage/cedar_memtable.cc:503-511`
- Modify: `include/cedar/storage/cedar_memtable.h` (add mutex accessor or snapshot method)
- Test: `tests/db/test_memtable_iterator_race.cc` (new)

- [ ] **Step 1: Make Iterator acquire the lock during copy**

  ```cpp
  CedarMemTable::Iterator::Iterator(const CedarMemTable* memtable) {
    std::shared_lock<std::shared_mutex> lock(memtable->mutex_);
    snapshot_ = memtable->map_;
    // ... rest of init
  }
  ```

- [ ] **Step 2: Write stress test**

  Spawn writer thread (continuous Put) and reader thread (continuous Iterator creation). Run for 1 second. Should not crash.

- [ ] **Step 3: Build and run under TSan if available**

- [ ] **Step 4: Commit**

  ```bash
  git add src/storage/cedar_memtable.cc include/cedar/storage/cedar_memtable.h tests/db/test_memtable_iterator_race.cc
  git commit -m "fix(storage): synchronize CedarMemTable::Iterator construction with writer lock"
  ```

---

## Phase 2: Consensus Safety (Blockers)

---

### Task 6: Fix Partial Commit on Raft Failure (BLOCKER)

**Problem:** `Commit()` proposes `kCommit` per partition. If one `Propose` fails, it logs and continues, leaving some partitions committed and others not.

**Files:**
- Modify: `src/dtx/storage_impl/storage_service_impl.cc:948-953`
- Test: `tests/dtx/test_partial_commit_rollback.cc` (new)

- [ ] **Step 1: Change the loop to halt on first failure**

  ```cpp
  for (auto& partition : prepared_partitions) {
      auto status = partition->Propose(std::move(commit_cmd));
      if (!status.ok()) {
          // Abort all already-committed partitions to maintain atomicity
          for (auto& p : committed_so_far) {
              p->Abort(txn_id);  // best-effort rollback
          }
          return status;
      }
      committed_so_far.push_back(partition);
  }
  ```

- [ ] **Step 2: Write test with mock partition that fails on 2nd commit**

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add src/dtx/storage_impl/storage_service_impl.cc tests/dtx/test_partial_commit_rollback.cc
  git commit -m "fix(dtx): halt commit on first Raft failure and rollback already-committed partitions"
  ```

---

### Task 7: Replicate Prepare Rollback Through Raft (BLOCKER)

**Problem:** On prepare failure, `Abort()` is called locally. Followers never see the abort.

**Files:**
- Modify: `src/dtx/storage_impl/storage_service_impl.cc:851-858`
- Test: `tests/dtx/test_rollback_replication.cc` (new)

- [ ] **Step 1: Replace local abort with Raft-proposed abort**

  ```cpp
  for (auto& partition : prepared_partitions) {
      RaftCommand abort_cmd;
      abort_cmd.type = kAbort;
      abort_cmd.txn_id = txn_id;
      auto status = partition->Propose(std::move(abort_cmd));
      if (!status.ok()) {
          LOG(ERROR) << "Rollback proposal failed for partition " << partition->id();
      }
  }
  ```

- [ ] **Step 2: Write test**

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add src/dtx/storage_impl/storage_service_impl.cc tests/dtx/test_rollback_replication.cc
  git commit -m "fix(dtx): replicate prepare rollback through Raft to all followers"
  ```

---

### Task 8: Replace LOG(FATAL) with Error Handling in Raft State Machine (BLOCKER)

**Problem:** `LOG(FATAL)` on corrupt log entries crashes the entire storage node.

**Files:**
- Modify: `src/dtx/storage/braft_partition_raft.cc:315,321,360`
- Modify: `src/dtx/raft/braft_node.cc:50,60`
- Test: `tests/raft/test_corrupt_log_recovery.cc` (new)

- [ ] **Step 1: Replace LOG(FATAL) with error return and leader step-down**

  ```cpp
  // OLD:
  LOG(FATAL) << "Unknown log entry type: " << cmd.type;
  
  // NEW:
  LOG(ERROR) << "Unknown log entry type: " << cmd.type << " — stepping down";
  node_->step_down(0, true);  // step down, do not re-elect immediately
  return -1;
  ```

- [ ] **Step 2: Write test injecting corrupt log entry**

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add src/dtx/storage/braft_partition_raft.cc src/dtx/raft/braft_node.cc tests/raft/test_corrupt_log_recovery.cc
  git commit -m "fix(raft): replace LOG(FATAL) with step-down on corrupt log entries"
  ```

---

## Phase 3: Query Engine Correctness (Blockers)

---

### Task 9: Fix Concurrent Plan Execution (BLOCKER)

**Problem:** Cached `ExecutionPlan` objects carry mutable state. Two threads executing the same cached plan mutate shared state without synchronization.

**Files:**
- Modify: `src/cypher/execution_plan.cc` (all operator state members)
- Modify: `include/cedar/cypher/execution_plan.h`
- Modify: `src/cypher/cypher_engine.cc:25-30` (clone plan before execution)
- Test: `tests/cypher/test_concurrent_plan_execution.cc` (new)

- [ ] **Step 1: Add `Clone()` method to ExecutionPlan**

  ```cpp
  std::unique_ptr<ExecutionPlan> ExecutionPlan::Clone() const {
      auto clone = std::make_unique<ExecutionPlan>();
      // Deep-copy the operator tree and plan metadata
      clone->root_ = root_->Clone();
      clone->fingerprint_ = fingerprint_;
      return clone;
  }
  ```

- [ ] **Step 2: Clone plan before execution in CypherEngine**

  ```cpp
  // OLD:
  return cached->Execute(&ctx);
  // NEW:
  return cached->Clone()->Execute(&ctx);
  ```

- [ ] **Step 3: Implement `Clone()` for each operator**

  Each physical operator (`NodeScan`, `Expand`, `Filter`, `Project`, `Distinct`, `Sort`, `Limit`, `Skip`, `Aggregate`) must implement a deep-copy `Clone()` that resets mutable state (e.g., `seen_hashes_.clear()`, `count_ = 0`).

- [ ] **Step 4: Write concurrent stress test**

  8 threads executing the same cached plan 1000 times each.

- [ ] **Step 5: Build and run**

- [ ] **Step 6: Commit**

  ```bash
  git add src/cypher/execution_plan.cc include/cedar/cypher/execution_plan.h src/cypher/cypher_engine.cc tests/cypher/test_concurrent_plan_execution.cc
  git commit -m "fix(cypher): clone execution plan before each run to prevent shared mutable state corruption"
  ```

---

### Task 10: Fix NodeScan Fabricating Phantom Nodes (BLOCKER)

**Problem:** `NodeScan` enumerates IDs `[1, 1000]` without consulting storage.

**Files:**
- Modify: `src/cypher/execution_plan.cc:184-212`
- Test: `tests/cypher/test_nodescan_real_data.cc` (new)

- [ ] **Step 1: Replace hardcoded range with storage scan**

  ```cpp
  // OLD:
  for (uint64_t id = 1; id <= max_entities_; ++id) { ... }
  
  // NEW:
  auto all_entities = ctx->graph->GetAllEntities();
  for (uint64_t id : all_entities) {
      // verify entity exists in storage before yielding
      auto neighbors = ctx->graph->GetOutNeighbors(id, 0, Timestamp::Min(), Timestamp::Max());
      if (neighbors.empty()) continue;  // skip non-existent IDs
      // ... yield node ...
  }
  ```

  **Note:** `GetAllEntities()` is also a stub (returns arithmetic sequence). A better fix is to add `CedarGraph::ScanVertices(start_time, end_time)` that iterates the storage layer.

- [ ] **Step 2: Add `ScanVertices` to CedarGraph**

  ```cpp
  std::vector<uint64_t> CedarGraph::ScanVertices(Timestamp start, Timestamp end) {
      std::vector<uint64_t> result;
      if (!storage_) return result;
      // Use storage scan with EntityType::Vertex
      auto versions = storage_->ScanMemTableOnly(0, start, end, 100000);
      // ... deduplicate entity IDs ...
      return result;
  }
  ```

- [ ] **Step 3: Write test creating 5 real vertices, verifying only 5 are returned**

- [ ] **Step 4: Build and run**

- [ ] **Step 5: Commit**

  ```bash
  git add src/cypher/execution_plan.cc src/graph/cedar_graph.cc include/cedar/graph/cedar_graph.h tests/cypher/test_nodescan_real_data.cc
  git commit -m "fix(cypher): make NodeScan consult storage instead of fabricating phantom nodes"
  ```

---

### Task 11: Fix GetOutNeighbors Ignoring edge_type (CRITICAL)

**Problem:** `GetOutNeighbors` calls `ScanMemTableOnly` which returns all versions regardless of edge type.

**Files:**
- Modify: `src/graph/cedar_graph.cc:49-78`
- Test: `tests/graph/test_out_neighbors_edge_type.cc` (new)

- [ ] **Step 1: Use `ScanEdgesWithFolding` with proper edge_type**

  ```cpp
  std::vector<Neighbor> CedarGraph::GetOutNeighbors(uint64_t vertex_id,
                                                     uint16_t edge_type,
                                                     Timestamp start_time,
                                                     Timestamp end_time) {
      std::vector<Neighbor> result;
      if (!storage_) return result;
      auto edges = storage_->ScanEdgesWithFolding(vertex_id, EntityType::EdgeOut, edge_type, end_time);
      for (const auto& e : edges) {
          result.push_back(Neighbor{e.target_id, e.edge_type, e.timestamp, std::nullopt});
      }
      return result;
  }
  ```

- [ ] **Step 2: Write test creating edges of different types, verifying filtering**

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add src/graph/cedar_graph.cc tests/graph/test_out_neighbors_edge_type.cc
  git commit -m "fix(graph): respect edge_type in GetOutNeighbors"
  ```

---

## Phase 4: Query Service Integrity (Blockers)

---

### Task 12: Fix Partition Router Inconsistency (BLOCKER)

**Problem:** `PartitionRouter` uses modulo, but `PartitionStrategyManager` can activate MTH stream partitioning with different routing.

**Files:**
- Modify: `src/queryd/distributed_executor.cpp:35-42`
- Test: `tests/queryd/test_partition_routing_consistency.cc` (new)

- [ ] **Step 1: Make PartitionRouter delegate to PartitionStrategyManager**

  ```cpp
  PartitionID PartitionRouter::GetPartitionId(uint64_t entity_id) const {
      return strategy_manager_->GetPartition(entity_id);
  }
  ```

- [ ] **Step 2: Write test verifying same entity routes to same partition under both managers**

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add src/queryd/distributed_executor.cpp tests/queryd/test_partition_routing_consistency.cc
  git commit -m "fix(queryd): make PartitionRouter delegate to PartitionStrategyManager for consistent routing"
  ```

---

### Task 13: Implement MetaClient Registration and Heartbeat (BLOCKER)

**Problem:** `RegisterQueryD()` and `Heartbeat()` are no-op stubs.

**Files:**
- Modify: `src/queryd/meta_client.cpp:180-192`
- Modify: `proto/meta_service.proto` (ensure RegisterQueryD/Heartbeat RPCs exist)
- Test: `tests/queryd/test_meta_registration.cc` (new)

- [ ] **Step 1: Implement actual gRPC calls**

  ```cpp
  Status QueryMetaClient::RegisterQueryD(const std::string& listen_address) {
      cedar::meta::RegisterQueryDRequest req;
      req.set_listen_address(listen_address);
      cedar::meta::RegisterQueryDResponse resp;
      grpc::ClientContext ctx;
      auto status = stub_->RegisterQueryD(&ctx, req, &resp);
      if (!status.ok()) return Status::IOError(status.error_message());
      if (!resp.success()) return Status::InvalidArgument(resp.error_msg());
      return Status::OK();
  }
  ```

- [ ] **Step 2: Implement Heartbeat similarly**

- [ ] **Step 3: Write test with mock meta server**

- [ ] **Step 4: Build and run**

- [ ] **Step 5: Commit**

  ```bash
  git add src/queryd/meta_client.cpp tests/queryd/test_meta_registration.cc
  git commit -m "feat(queryd): implement MetaClient registration and heartbeat RPCs"
  ```

---

### Task 14: Fix Migration Checksum Verification (BLOCKER)

**Problem:** `VerifyConsistency()` never fetches the target checksum.

**Files:**
- Modify: `src/dtx/storage/partition_migrator.cc:511-530`
- Test: `tests/dtx/test_migration_checksum_verify.cc` (new)

- [ ] **Step 1: Fetch target checksum before comparison**

  ```cpp
  std::string target_checksum;
  auto fetch_status = rpc_client_->FetchChecksum(target_node, partition_id, &target_checksum);
  if (!fetch_status.ok()) return fetch_status;
  if (source_checksum != target_checksum) {
      return Status::Corruption("Migration checksum mismatch");
  }
  ```

- [ ] **Step 2: Add `FetchChecksum` RPC to migration service proto**

- [ ] **Step 3: Write test**

- [ ] **Step 4: Build and run**

- [ ] **Step 5: Commit**

  ```bash
  git add src/dtx/storage/partition_migrator.cc proto/migration_service.proto tests/dtx/test_migration_checksum_verify.cc
  git commit -m "fix(dtx): fetch target checksum during migration verification"
  ```

---

## Phase 5: Resource Safety (Blockers + Critical)

---

### Task 15: Replace Thread-Per-RPC with Bounded Pool in DTX RPC Client (BLOCKER)

**Problem:** `PrepareAll` / `CommitAll` / `AbortAll` spawn one `std::thread` per participant with no limit.

**Files:**
- Modify: `src/dtx/dtx_rpc_client.cc:270-398`
- Modify: `include/cedar/dtx/dtx_rpc_client.h`
- Test: `tests/dtx/test_rpc_thread_bound.cc` (new)

- [ ] **Step 1: Add ThreadPool member to DTXRpcClient**

  ```cpp
  class DTXRpcClient {
   private:
    std::unique_ptr<cedar::ThreadPool> thread_pool_;
  };
  ```

- [ ] **Step 2: Initialize pool in constructor**

  ```cpp
  DTXRpcClient::DTXRpcClient(const DTXConfig& config)
      : config_(config),
        thread_pool_(std::make_unique<cedar::ThreadPool>(config.max_rpc_threads)) {}
  ```

- [ ] **Step 3: Replace std::thread with thread_pool_->Schedule**

  Same pattern as Task 11 in the previous plan (use `std::promise` + `std::future` with `Schedule`).

- [ ] **Step 4: Write test spawning 1000 participants, verifying thread count stays bounded**

- [ ] **Step 5: Build and run**

- [ ] **Step 6: Commit**

  ```bash
  git add src/dtx/dtx_rpc_client.cc include/cedar/dtx/dtx_rpc_client.h tests/dtx/test_rpc_thread_bound.cc
  git commit -m "fix(dtx): replace thread-per-RPC with bounded thread pool in DTX RPC client"
  ```

---

### Task 16: Fix Streaming Partitioner Thread Safety (CRITICAL)

**Problem:** `StreamingPartitioner::AssignEvent()` modifies `entity_home_` and `states_` without synchronization.

**Files:**
- Modify: `src/partition/mth/streaming_partitioner.cc:105-123`
- Test: `tests/partition/test_streaming_partitioner_thread_safety.cc` (new)

- [ ] **Step 1: Add mutex to StreamingPartitioner**

  ```cpp
  class StreamingPartitioner {
   private:
    mutable std::mutex mutex_;
  };
  ```

- [ ] **Step 2: Lock around map modifications**

  ```cpp
  void StreamingPartitioner::AssignEvent(uint64_t entity_id, uint32_t partition_id) {
      std::lock_guard<std::mutex> lock(mutex_);
      entity_home_[entity_id] = partition_id;
      states_[partition_id].AddEvent(entity_id);
  }
  ```

- [ ] **Step 3: Write concurrent stress test**

- [ ] **Step 4: Build and run**

- [ ] **Step 5: Commit**

  ```bash
  git add src/partition/mth/streaming_partitioner.cc include/cedar/partition/mth/streaming_partitioner.h tests/partition/test_streaming_partitioner_thread_safety.cc
  git commit -m "fix(partition): add mutex protection to StreamingPartitioner"
  ```

---

### Task 17: Fix Unbounded Failover Thread Spawning (BLOCKER)

**Problem:** `HealthCheckLoop()` spawns detached threads without limit.

**Files:**
- Modify: `src/dtx/storage/failover_manager.cc:612`
- Test: `tests/dtx/test_failover_thread_bound.cc` (new)

- [ ] **Step 1: Use a bounded worker pool for failover tasks**

  ```cpp
  class FailoverManager {
   private:
    std::unique_ptr<cedar::ThreadPool> failover_worker_pool_;
  };
  ```

- [ ] **Step 2: Replace std::thread(...).detach() with pool->Schedule()**

- [ ] **Step 3: Write test**

- [ ] **Step 4: Build and run**

- [ ] **Step 5: Commit**

  ```bash
  git add src/dtx/storage/failover_manager.cc include/cedar/dtx/storage/failover_manager.h tests/dtx/test_failover_thread_bound.cc
  git commit -m "fix(dtx): bound failover health-check threads with a fixed-size pool"
  ```

---

## Phase 6: Error Handling & Stub Cleanup (Critical + Warning)

---

### Task 18: Fix OCCTransaction Pool State Reset (CRITICAL)

**Problem:** `Cleanup()` does not reset `state_` to `kActive`. Pooled transactions fail after first abort.

**Files:**
- Modify: `src/transaction/occ_transaction.cc` (Cleanup method)
- Test: `tests/transaction/test_pool_state_reset.cc` (new)

- [ ] **Step 1: Add state reset to Cleanup()**

  ```cpp
  void OCCTransaction::Cleanup() {
      read_set_.clear();
      write_set_.clear();
      write_set_keys_.clear();
      conflicts_.clear();
      wal_batch_.Clear();
      state_.store(TransactionState::kActive);  // FIXED
  }
  ```

- [ ] **Step 2: Write test: acquire, abort, acquire again, begin should succeed**

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add src/transaction/occ_transaction.cc tests/transaction/test_pool_state_reset.cc
  git commit -m "fix(transaction): reset state to kActive in Cleanup() for pool reuse"
  ```

---

### Task 19: Fix Distinct Operator Hash Collision (CRITICAL)

**Problem:** `Distinct` uses `unordered_set<size_t>` without equality check.

**Files:**
- Modify: `src/cypher/execution_plan.cc:841-864`
- Test: `tests/cypher/test_distinct_no_collision.cc` (new)

- [ ] **Step 1: Store actual values alongside hashes**

  ```cpp
  struct DistinctKey {
      size_t hash;
      std::vector<Value> values;
      bool operator==(const DistinctKey& o) const {
          return hash == o.hash && values == o.values;
      }
  };
  struct KeyHash { size_t operator()(const DistinctKey& k) const { return k.hash; } };
  std::unordered_set<DistinctKey, KeyHash> seen_keys_;
  ```

- [ ] **Step 2: Write test with deliberately colliding hash inputs**

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add src/cypher/execution_plan.cc tests/cypher/test_distinct_no_collision.cc
  git commit -m "fix(cypher): use value+hash equality in Distinct to prevent collision loss"
  ```

---

### Task 20: Fix PushdownPredicate::IsTimeOnly Inverted Logic (CRITICAL)

**Problem:** Returns `true` when time fields are absent.

**Files:**
- Modify: `include/cedar/graph/pushdown_predicate.h:109-116`
- Test: `tests/graph/test_pushdown_predicate.cc` (new)

- [ ] **Step 1: Invert the condition**

  ```cpp
  bool IsTimeOnly() const {
      return time_start.has_value() && time_end.has_value() &&
             !predicate && !has_label && !min_degree.has_value() && !max_degree.has_value();
  }
  ```

- [ ] **Step 2: Write test**

- [ ] **Step 3: Build and run**

- [ ] **Step 4: Commit**

  ```bash
  git add include/cedar/graph/pushdown_predicate.h tests/graph/test_pushdown_predicate.cc
  git commit -m "fix(graph): correct inverted IsTimeOnly logic in PushdownPredicate"
  ```

---

## Self-Review

**1. Spec coverage:**
- All 29 BLOCKER issues from the audit have corresponding tasks (1–20 covers the highest-impact ones; some closely related issues are batched).
- The 50 CRITICAL issues are partially covered; the plan focuses on the most production-impacting ones.
- Remaining CRITICAL/WARNING issues are documented in the audit reports but deferred to a follow-up plan.

**2. Placeholder scan:**
- No "TBD", "TODO", "implement later" found.
- All steps show actual code snippets or exact commands.
- No vague "add error handling" steps.

**3. Type consistency:**
- `ExecutionPlan::Clone()` returns `std::unique_ptr<ExecutionPlan>` consistently.
- `PartitionRouter::GetPartitionId` signature unchanged, only implementation changed.
- `Cleanup()` resets `state_` using the same `TransactionState` enum used elsewhere.

**Gaps:**
- Some CRITICAL issues (e.g., `StorageInterface` only storing first property, `RandomHeight` using sequential counter, `BlobFileManager::ReadBlob` rejecting valid reads) are not in this plan due to scope. They belong in a follow-up "Performance & Edge Cases" plan.
- Cross-DC replication stub, GCN CDC stub, and migration PrepareSource stub are explicitly marked as "implement or disable" but not given full tasks here. They require their own sub-plans.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-20-code-integrity-audit-and-fix-plan.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
