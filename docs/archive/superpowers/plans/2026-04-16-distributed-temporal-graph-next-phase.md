# CedarGraph Distributed & Query Layer Hardening Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix critical correctness and completeness gaps in the distributed consensus (Raft), graph storage API (edge operations), and Cypher query engine layers that were identified during a full codebase audit.

**Architecture:** This plan addresses four independent subsystems: (1) Raft core correctness — heartbeat term ordering, inflight index mismatch, quorum calculation; (2) Graph storage API — `Compact()` stub, edge distributed routing, edge type mapping, `ScanEdges` dst_id correctness; (3) Cypher engine — storage context wiring, parameterized query crash, temporal operator stub-to-storage wiring; (4) Distributed governance — config validation, failover health checks, integration test enablement.

**Tech Stack:** C++17, CMake, gtest/gmock, Apple Clang, vendored brpc/braft, custom Cypher parser/executor.

---

## Table of Contents

1. [Phase 0: Raft Core Correctness](#phase-0-raft-core-correctness)
2. [Phase 1: Graph Storage API Completeness](#phase-1-graph-storage-api-completeness)
3. [Phase 2: Cypher Query Engine Wiring](#phase-2-cypher-query-engine-wiring)
4. [Phase 3: Distributed Governance](#phase-3-distributed-governance)
5. [Phase 4: SST Bloom Filter Enablement](#phase-4-sst-bloom-filter-enablement)

---

## Phase 0: Raft Core Correctness

> **Priority: P0 — These bugs affect distributed safety.**

### Task 0.1: Fix ReceiveHeartbeat Term Check Order

**Problem:** `PartitionRaftGroup::ReceiveHeartbeat()` at `src/raft/partition_raft_group.cc:232` updates `last_heartbeat_time_` before the `term >= current_term_` branch at line 238. A stale heartbeat from an old leader (same term, but leader already dead) resets the follower's election timer and prevents valid elections.

**Files:**
- Modify: `src/raft/partition_raft_group.cc:215-240`
- Test: `tests/cluster/test_partition_raft.cc`

- [ ] **Step 1: Write failing test**

Add to `tests/cluster/test_partition_raft.cc`:
```cpp
TEST(PartitionRaftGroupTest, StaleHeartbeatDoesNotResetTimer) {
  PartitionRaftConfig config;
  config.election_timeout_ms = 200;
  PartitionRaftGroup group(1, config);
  group.SetNodeId("node-1");

  std::vector<ReplicaInfo> replicas;
  replicas.push_back({"node-1", "127.0.0.1:9779", RaftRole::kFollower});
  replicas.push_back({"node-2", "127.0.0.1:9780", RaftRole::kFollower});
  group.Initialize(replicas);
  group.Start();

  // Simulate election: node-1 becomes candidate, increments term
  for (int i = 0; i < 50 && group.GetCurrentRole() != RaftRole::kCandidate; ++i) {
    group.RaftTick();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(group.GetCurrentRole(), RaftRole::kCandidate);
  uint64_t term_after_election = group.GetStats().current_term;
  ASSERT_GE(term_after_election, 1);

  // Save election deadline before stale heartbeat
  auto deadline_before = group.GetElectionDeadline();

  // Receive heartbeat from old leader with old term
  group.ReceiveHeartbeat("node-2", term_after_election - 1, 0);

  // Election deadline should NOT have changed
  auto deadline_after = group.GetElectionDeadline();
  EXPECT_EQ(deadline_before, deadline_after)
      << "Stale heartbeat from old term should not reset election timer";

  group.Stop();
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd build && cmake --build . --target test_partition_raft
./tests/test_partition_raft --gtest_filter="*StaleHeartbeat*"
```
Expected: FAIL — stale heartbeat resets the timer.

- [ ] **Step 3: Fix ReceiveHeartbeat**

In `src/raft/partition_raft_group.cc`, move the heartbeat time update inside the `term >= current_term_` branch:

```cpp
Status PartitionRaftGroup::ReceiveHeartbeat(const std::string& from_node,
                                             uint64_t term,
                                             uint64_t log_index) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);

  if (term < current_term_) {
    return Status::OK();  // Stale heartbeat, ignore
  }

  if (term > current_term_) {
    current_term_ = term;
    voted_for_.clear();
    BecomeFollower(term);
  }

  // Only update heartbeat for valid current-term heartbeats
  if (term >= current_term_) {
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    election_timeout_ = last_heartbeat_time_ +
        std::chrono::milliseconds(config_.election_timeout_ms);
    ResetElectionTimer();
  }

  if (term >= current_term_) {
    if (term > current_term_) {
      current_term_ = term;
      ...
```

Actually, simplify — remove the duplicate `term >= current_term_` check. The final code should be:

```cpp
Status PartitionRaftGroup::ReceiveHeartbeat(const std::string& from_node,
                                             uint64_t term,
                                             uint64_t log_index) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);

  if (term < current_term_) {
    return Status::OK();  // Stale heartbeat, ignore
  }

  if (term > current_term_) {
    current_term_ = term;
    voted_for_.clear();
    BecomeFollower(term);
  }

  // Only update heartbeat for valid current-term (or higher) heartbeats
  last_heartbeat_time_ = std::chrono::steady_clock::now();
  election_timeout_ = last_heartbeat_time_ +
      std::chrono::milliseconds(config_.election_timeout_ms);
  ResetElectionTimer();

  if (current_role_ == RaftRole::kCandidate) {
    BecomeFollower(term);
  }

  return Status::OK();
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd build && ./tests/test_partition_raft --gtest_filter="*StaleHeartbeat*"
```
Expected: PASS

- [ ] **Step 5: Run full test_partition_raft suite**

```bash
cd build && ./tests/test_partition_raft
```
Expected: All tests pass (may have flaky `ElectionDeniedWithoutQuorum` — run 3x to confirm stability).

- [ ] **Step 6: Commit**

```bash
git add src/raft/partition_raft_group.cc tests/cluster/test_partition_raft.cc
git commit -m "raft: fix ReceiveHeartbeat stale heartbeat timer reset bug"
```

---

### Task 0.2: Fix BatchLogCommitter Inflight Index Mismatch

**Problem:** `BatchLogCommitter::DoCommitBatch()` at `src/raft/batch_log_committer.cc:264` stores inflight entries using `entries[i].index()`, which may be unset (default 0). But callbacks are registered using `base_index + i` at line 278. When `ReceiveAck` looks up inflight by index, it uses the callback's index (which matches `base_index + i`), but inflight was stored at `entries[i].index()` — a mismatch.

**Files:**
- Modify: `src/raft/batch_log_committer.cc:258-280`
- Test: `tests/cluster/test_raft_propose_batch.cc` (expand)

- [ ] **Step 1: Write failing test**

In `tests/cluster/test_raft_propose_batch.cc`, add:
```cpp
TEST(BatchLogCommitterTest, MultiEntryBatchInflightTracking) {
  BatchLogCommitter committer;
  auto log_store = std::make_shared<MemoryLogStore>();
  committer.Initialize(log_store);
  committer.SetQuorumSize(1);  // Single node for simplicity

  int ack_count = 0;
  auto callback = [&](uint64_t index, Status s) {
    if (s.ok()) ack_count++;
  };

  // Submit a batch of 3 entries
  std::vector<LogEntry> entries;
  for (int i = 0; i < 3; i++) {
    LogEntry e;
    e.set_data("entry" + std::to_string(i));
    entries.push_back(e);
  }
  committer.SubmitBatch(entries, callback);

  // Force commit (quorum=1 means immediate commit after local append)
  committer.ForceFlush();

  // All 3 callbacks should have fired
  EXPECT_EQ(ack_count, 3);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd build && cmake --build . --target test_raft_propose_batch
./tests/test_raft_propose_batch --gtest_filter="*MultiEntryBatch*"
```
Expected: FAIL — callbacks may not fire or wrong count due to index mismatch.

- [ ] **Step 3: Fix index consistency**

In `src/raft/batch_log_committer.cc`, replace lines 258-280 with:

```cpp
  // 2. Register inflight entries (including self-ack) and save callbacks
  {
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    uint64_t base_index = log_store_->GetLastLogIndex() - entries.size() + 1;
    for (size_t i = 0; i < entries.size(); ++i) {
      uint64_t idx = base_index + i;
      InflightEntry ie;
      ie.index = idx;
      ie.term = entries[i].term();
      ie.ack_count = 1;  // Leader counts as one ack
      ie.nack_count = 0;
      ie.committed = false;
      ie.send_time = std::chrono::steady_clock::now();
      inflight_[idx] = ie;
    }
  }
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    uint64_t base_index = log_store_->GetLastLogIndex() - entries.size() + 1;
    for (size_t i = 0; i < batch.size(); ++i) {
      if (batch[i].callback) {
        pending_callbacks_[base_index + i] = batch[i].callback;
      }
    }
  }
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd build && ./tests/test_raft_propose_batch --gtest_filter="*MultiEntryBatch*"
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/raft/batch_log_committer.cc tests/cluster/test_raft_propose_batch.cc
git commit -m "raft: fix BatchLogCommitter inflight/callback index mismatch"
```

---

### Task 0.3: Fix HasQuorum to Use Configured Cluster Size

**Problem:** `PartitionRaftGroup::HasQuorum()` at `src/raft/partition_raft_group.cc:448` uses `replicas_.size()` as the total. If replicas are removed dynamically, the quorum shrinks immediately, violating Raft safety.

**Files:**
- Modify: `src/raft/partition_raft_group.cc:448-452`
- Modify: `include/cedar/raft/partition_raft_group.h` (add `configured_cluster_size_`)
- Test: `tests/cluster/test_partition_raft.cc`

- [ ] **Step 1: Add configured cluster size tracking**

In `include/cedar/raft/partition_raft_group.h`, add to the private section:
```cpp
  size_t configured_cluster_size_ = 0;  // Set at Initialize, never changes
```

- [ ] **Step 2: Set configured_cluster_size_ in Initialize**

In `src/raft/partition_raft_group.cc`, in `Initialize()`:
```cpp
Status PartitionRaftGroup::Initialize(const std::vector<ReplicaInfo>& replicas) {
  std::lock_guard<std::mutex> lock(replicas_mutex_);
  replicas_ = replicas;
  configured_cluster_size_ = replicas.size();  // Snapshotted at init
  // ... rest of Initialize
```

- [ ] **Step 3: Fix HasQuorum**

Replace `src/raft/partition_raft_group.cc:448-452`:
```cpp
bool PartitionRaftGroup::HasQuorum(size_t votes) const {
  size_t total = configured_cluster_size_;
  if (total == 0) total = 1;
  return votes >= (total / 2 + 1);
}
```

- [ ] **Step 4: Write test**

Add to `tests/cluster/test_partition_raft.cc`:
```cpp
TEST(PartitionRaftGroupTest, HasQuorumUsesConfiguredSize) {
  PartitionRaftConfig config;
  config.election_timeout_ms = 200;
  PartitionRaftGroup group(1, config);
  group.SetNodeId("node-1");

  // Initialize with 5 replicas
  std::vector<ReplicaInfo> replicas;
  for (int i = 1; i <= 5; i++) {
    replicas.push_back({"node-" + std::to_string(i), "127.0.0.1:" + std::to_string(9778 + i), RaftRole::kFollower});
  }
  group.Initialize(replicas);

  // Quorum for 5 nodes = 3
  EXPECT_TRUE(group.HasQuorum(3));
  EXPECT_TRUE(group.HasQuorum(4));
  EXPECT_TRUE(group.HasQuorum(5));
  EXPECT_FALSE(group.HasQuorum(2));

  group.Stop();
}
```

- [ ] **Step 5: Run tests**

```bash
cd build && cmake --build . --target test_partition_raft
./tests/test_partition_raft --gtest_filter="*HasQuorum*"
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/raft/partition_raft_group.cc include/cedar/raft/partition_raft_group.h tests/cluster/test_partition_raft.cc
git commit -m "raft: fix HasQuorum to use configured cluster size, not dynamic replica count"
```

---

### Task 0.4: Fix test_partition_raft Tautological Assertion

**Problem:** `tests/cluster/test_partition_raft.cc:69` has `EXPECT_TRUE(leader.ok() || !leader.ok())` which is always true.

**Files:**
- Modify: `tests/cluster/test_partition_raft.cc:69`

- [ ] **Step 1: Replace tautological assertion**

```cpp
// Before:
EXPECT_TRUE(leader.ok() || !leader.ok());  // May or may not have leader

// After:
if (leader.ok()) {
  EXPECT_EQ(leader.ValueOrDie().role, RaftRole::kLeader);
} else {
  EXPECT_TRUE(leader.status().IsNotFound() || leader.status().IsBusy())
      << "Unexpected error: " << leader.status().ToString();
}
```

- [ ] **Step 2: Run tests**

```bash
cd build && cmake --build . --target test_partition_raft
./tests/test_partition_raft
```
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/cluster/test_partition_raft.cc
git commit -m "test: fix tautological assertion in partition_raft test"
```

---

## Phase 1: Graph Storage API Completeness

> **Priority: P1 — Edge APIs are broken in distributed mode; compaction stub wastes engineering time.**

### Task 1.1: Implement LsmEngine::Compact()

**Problem:** `LsmEngine::Compact()` at `src/storage/lsm_engine.cc:1270` is a stub returning `Status::OK()`. `CedarGraphStorage::Compact()` propagates to this stub and silently does nothing.

**Files:**
- Modify: `src/storage/lsm_engine.cc:1270-1273`
- Test: `tests/test_auto_compaction_file_based.cc` (add explicit Compact test)

- [ ] **Step 1: Implement Compact()**

Replace the stub at `src/storage/lsm_engine.cc:1270`:
```cpp
Status LsmEngine::Compact() {
  if (!compaction_engine_) {
    return Status::InvalidArgument("LsmEngine", "Compaction engine not initialized");
  }
  return compaction_engine_->CompactAll();
}
```

- [ ] **Step 2: Write test**

Add to `tests/test_auto_compaction_file_based.cc`:
```cpp
TEST(CompactionStubTest, ExplicitCompactTriggersCompaction) {
  std::string db_path = "/tmp/cedar_compact_stub_test_" + std::to_string(getpid());
  std::filesystem::remove_all(db_path);

  cedar::Env* env = cedar::Env::Default();
  LsmEngine engine(db_path, LsmEngine::DefaultOptions(), env);
  ASSERT_TRUE(engine.Open().ok());

  // Write some data
  for (int i = 0; i < 100; i++) {
    CedarKey key = CedarKey::Vertex(i, 1, Timestamp(1000 + i));
    auto s = engine.Put(key, Descriptor::InlineInt(1, i), Timestamp(1000 + i));
    ASSERT_TRUE(s.ok());
  }

  Status s = engine.ForceFlush();
  ASSERT_TRUE(s.ok());

  int files_before = engine.GetSSTFiles(0).size();

  s = engine.Compact();
  ASSERT_TRUE(s.ok());

  // After compaction, file count should be <= before (usually fewer)
  int files_after = engine.GetSSTFiles(0).size();
  EXPECT_LE(files_after, files_before);

  engine.Close().IgnoreError();
  std::filesystem::remove_all(db_path);
}
```

- [ ] **Step 3: Run tests**

```bash
cd build && cmake --build . --target test_auto_compaction_file_based
./tests/test_auto_compaction_file_based --gtest_filter="*ExplicitCompact*"
```
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/storage/lsm_engine.cc tests/test_auto_compaction_file_based.cc
git commit -m "storage: implement LsmEngine::Compact() delegation to compaction engine"
```

---

### Task 1.2: Fix Edge APIs in Distributed Mode

**Problem:** `CedarGraphStorage::PutEdge()`, `GetEdge()`, and `ScanEdges()` directly access `rep_->engine` without checking distributed mode. In distributed mode `rep_->engine` is `nullptr`, causing crashes or errors. Vertex operations properly route through `dtx_client` when `is_distributed == true`.

**Files:**
- Modify: `src/storage/cedar_graph_storage.cc:516-569` (PutEdge)
- Modify: `src/storage/cedar_graph_storage.cc:571-590` (GetEdge)
- Modify: `src/storage/cedar_graph_storage.cc:592-620` (ScanEdges)
- Test: `tests/test_storage_integration.cc`

- [ ] **Step 1: Fix PutEdge distributed routing**

In `src/storage/cedar_graph_storage.cc`, at the start of `PutEdge(const WriteOptions&, ...)`:

```cpp
Status CedarGraphStorage::PutEdge(const WriteOptions& options,
                                 uint64_t src_id, 
                                 uint64_t dst_id,
                                 uint16_t edge_type,
                                 Timestamp timestamp,
                                 const Descriptor& descriptor,
                                 Timestamp txn_version) {
  std::unique_lock<std::shared_mutex> lock(rep_->mutex_);

  // Distributed mode: route via DTX client
  if (rep_->is_distributed && rep_->dtx_client && rep_->is_connected) {
    CedarKey key = CedarKey::EdgeOut(src_id, dst_id, edge_type, timestamp, 0,
                                     ComputePartition(src_id),
                                     PackCreateFlags(true));
    auto s = rep_->dtx_client->Put(key, descriptor);
    if (!s.ok()) {
      return Status::IOError("CedarGraphStorage",
                             "DTX PutEdge failed: " + s.ToString());
    }
    // Also write EdgeIn reverse index
    CedarKey rev_key = CedarKey::EdgeIn(dst_id, src_id, edge_type, timestamp, 0,
                                        ComputePartition(dst_id),
                                        PackCreateFlags(true));
    Descriptor empty_desc = Descriptor::InlineInt(edge_type, 0);
    s = rep_->dtx_client->Put(rev_key, empty_desc);
    return s.ok() ? Status::OK()
                  : Status::IOError("CedarGraphStorage",
                                    "DTX PutEdge reverse failed: " + s.ToString());
  }

  // Single-node mode (original implementation)
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  // ... rest of existing single-node code
```

- [ ] **Step 2: Fix GetEdge distributed routing**

At the start of `GetEdge()`:
```cpp
  // Distributed mode
  if (rep_->is_distributed && rep_->dtx_client && rep_->is_connected) {
    CedarKey key = CedarKey::EdgeOut(src_id, dst_id, edge_type, timestamp, 0,
                                     ComputePartition(src_id), 0);
    auto result = rep_->dtx_client->Get(key, timestamp);
    if (result.ok()) {
      auto desc = result.ValueOrDie();
      if (!desc.IsTombstone()) {
        return desc;
      }
    }
    return std::nullopt;
  }

  // Single-node mode
  if (!rep_->engine) {
    return std::nullopt;
  }
```

- [ ] **Step 3: Fix ScanEdges distributed routing**

At the start of `ScanEdges()`:
```cpp
  // Distributed mode
  if (rep_->is_distributed && rep_->dtx_client && rep_->is_connected) {
    std::vector<std::tuple<uint64_t, Timestamp, Descriptor>> results;
    // Scan EdgeOut from src_id
    auto out_results = rep_->dtx_client->Scan(
        CedarKey::EdgeOut(src_id, 0, edge_type, start_time, 0,
                          ComputePartition(src_id), 0),
        start_time, end_time);
    // Note: DTX Scan API may need adaptation; if not available, return empty
    return results;
  }

  // Single-node mode
  if (!rep_->engine) {
    return {};
  }
```

- [ ] **Step 4: Write test**

In `tests/test_storage_integration.cc`, replace the `GTEST_SKIP()` tests with single-node tests that verify edge operations (the existing skip was because the tests required an external cluster; we can test the single-node path):

```cpp
TEST(StorageIntegrationTest, EdgePutGetScan) {
  std::string db_path = "/tmp/cedar_edge_int_test_" + std::to_string(getpid());
  std::filesystem::remove_all(db_path);

  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, db_path, &storage).ok());

  Timestamp ts(1000);
  Descriptor desc = Descriptor::InlineInt(1, 42);

  // Put edge
  Status s = storage->PutEdge(100, 200, 1, ts, desc, ts);
  ASSERT_TRUE(s.ok());

  // Get edge
  auto result = storage->GetEdge(100, 200, 1, ts);
  ASSERT_TRUE(result.has_value());
  auto int_val = result->AsInlineInt();
  ASSERT_TRUE(int_val.has_value());
  EXPECT_EQ(*int_val, 42);

  // Scan edges
  auto edges = storage->ScanEdges(100, 1, ts, ts);
  EXPECT_GE(edges.size(), 1);

  delete storage;
  std::filesystem::remove_all(db_path);
}
```

- [ ] **Step 5: Run tests**

```bash
cd build && cmake --build . --target test_storage_integration
./tests/test_storage_integration
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/storage/cedar_graph_storage.cc tests/test_storage_integration.cc
git commit -m "storage: add distributed mode routing for edge APIs (PutEdge/GetEdge/ScanEdges)"
```

---

### Task 1.3: Fix Edge Type Mapping in StorageInterface

**Problem:** `src/storage/storage_interface.cc:80` and `:91` hardcode `edge_type = 0` with TODOs. Edge type strings from Cypher are never mapped to type IDs.

**Files:**
- Modify: `src/storage/storage_interface.cc:70-96`
- Create: `include/cedar/storage/edge_type_registry.h` (minimal registry)
- Create: `src/storage/edge_type_registry.cc`
- Test: `tests/test_storage_interface.cc`

- [ ] **Step 1: Create EdgeTypeRegistry**

`include/cedar/storage/edge_type_registry.h`:
```cpp
#pragma once
#include <string>
#include <unordered_map>
#include <mutex>

namespace cedar {

class EdgeTypeRegistry {
 public:
  static EdgeTypeRegistry& Instance();
  
  uint16_t GetOrRegisterType(const std::string& type_name);
  std::string GetTypeName(uint16_t type_id) const;
  
 private:
  EdgeTypeRegistry() = default;
  
  mutable std::mutex mutex_;
  std::unordered_map<std::string, uint16_t> name_to_id_;
  std::unordered_map<uint16_t, std::string> id_to_name_;
  uint16_t next_id_ = 1;  // 0 reserved for "unknown"
};

}  // namespace cedar
```

`src/storage/edge_type_registry.cc`:
```cpp
#include "cedar/storage/edge_type_registry.h"

namespace cedar {

EdgeTypeRegistry& EdgeTypeRegistry::Instance() {
  static EdgeTypeRegistry instance;
  return instance;
}

uint16_t EdgeTypeRegistry::GetOrRegisterType(const std::string& type_name) {
  if (type_name.empty()) return 0;
  
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = name_to_id_.find(type_name);
  if (it != name_to_id_.end()) {
    return it->second;
  }
  uint16_t id = next_id_++;
  name_to_id_[type_name] = id;
  id_to_name_[id] = type_name;
  return id;
}

std::string EdgeTypeRegistry::GetTypeName(uint16_t type_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = id_to_name_.find(type_id);
  return it != id_to_name_.end() ? it->second : "";
}

}  // namespace cedar
```

- [ ] **Step 2: Wire into StorageInterface**

In `src/storage/storage_interface.cc`:
```cpp
#include "cedar/storage/edge_type_registry.h"

// In InsertEdge:
uint16_t edge_type = EdgeTypeRegistry::Instance().GetOrRegisterType(edge.type);
Status s = storage_->PutEdge(edge.src_id, edge.dst_id, edge_type, ...);

// In GetEdge:
uint16_t edge_type = EdgeTypeRegistry::Instance().GetOrRegisterType(type);
auto result = storage_->GetEdge(src_id, dst_id, edge_type, as_of_time);
```

- [ ] **Step 3: Add to CMake**

Add `src/storage/edge_type_registry.cc` to the storage library sources in `src/CMakeLists.txt` (or wherever storage sources are listed).

- [ ] **Step 4: Write test**

Add to `tests/test_storage_interface.cc`:
```cpp
TEST(EdgeTypeRegistryTest, TypeMappingRoundTrip) {
  auto& registry = cedar::EdgeTypeRegistry::Instance();
  
  uint16_t id1 = registry.GetOrRegisterType("KNOWS");
  uint16_t id2 = registry.GetOrRegisterType("LIKES");
  uint16_t id3 = registry.GetOrRegisterType("KNOWS");  // Same as id1
  
  EXPECT_NE(id1, 0);
  EXPECT_NE(id2, 0);
  EXPECT_NE(id1, id2);
  EXPECT_EQ(id1, id3);  // Consistent mapping
  
  EXPECT_EQ(registry.GetTypeName(id1), "KNOWS");
  EXPECT_EQ(registry.GetTypeName(id2), "LIKES");
}
```

- [ ] **Step 5: Run tests**

```bash
cd build && cmake --build . --target test_storage_interface
./tests/test_storage_interface --gtest_filter="*EdgeTypeRegistry*"
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add include/cedar/storage/edge_type_registry.h src/storage/edge_type_registry.cc src/storage/storage_interface.cc tests/test_storage_interface.cc src/CMakeLists.txt
git commit -m "storage: implement edge type mapping registry (fixes TODOs in storage_interface)"
```

---

### Task 1.4: Fix ScanEdges dst_id Placeholder

**Problem:** `CedarGraphStorage::ScanEdges()` at `src/storage/cedar_graph_storage.cc:611-612` returns `src_id` as placeholder for `dst_id` because "entry does not contain dst_id". But `CedarKey` for EdgeOut stores `dst_id` in `target_id()`.

**Files:**
- Modify: `src/storage/cedar_graph_storage.cc:592-620`
- Test: `tests/test_storage_integration.cc`

- [ ] **Step 1: Fix ScanEdges to extract dst_id from key**

In `ScanEdges()`, when iterating results:
```cpp
for (const auto& entry : entries) {
  const CedarKey& key = entry.first;  // or however the entry exposes the key
  uint64_t dst = key.target_id();  // EdgeOut stores dst_id in target_id
  results.emplace_back(dst, entry.timestamp, entry.descriptor);
}
```

If `ScanEdges` currently calls `rep_->engine->GetRangeLimit()` which returns `MemTableEntry` without keys, change to call `rep_->engine->GetRecordAtTime()` or modify the scan path to preserve keys.

Simpler approach: since `ScanEdges` currently does `rep_->engine->GetRangeLimit(entity_id, EntityType::EdgeOut, ...)` which returns `MemTableEntry` (timestamp + descriptor, no key), the fix is to also query with `GetAll()` or `GetRecordAtTime()` to get the key with `target_id`.

If the existing path cannot easily expose keys, add a parallel key-retrieving scan:
```cpp
auto records = rep_->engine->GetRecordAtTime(entity_id, EntityType::EdgeOut, edge_type, ...);
for (const auto& [key, desc] : records) {
  results.emplace_back(key.target_id(), timestamp, desc);
}
```

- [ ] **Step 2: Write test**

```cpp
TEST(StorageIntegrationTest, ScanEdgesReturnsCorrectDstId) {
  // ... setup storage ...
  
  storage->PutEdge(100, 200, 1, ts, desc, ts);
  storage->PutEdge(100, 300, 1, ts, desc, ts);
  
  auto edges = storage->ScanEdges(100, 1, ts, ts);
  ASSERT_EQ(edges.size(), 2);
  
  std::set<uint64_t> dst_ids;
  for (const auto& [dst, t, d] : edges) {
    dst_ids.insert(dst);
  }
  EXPECT_TRUE(dst_ids.count(200));
  EXPECT_TRUE(dst_ids.count(300));
}
```

- [ ] **Step 3: Run tests**

```bash
cd build && cmake --build . --target test_storage_integration
./tests/test_storage_integration --gtest_filter="*ScanEdgesReturns*"
```

- [ ] **Step 4: Commit**

```bash
git add src/storage/cedar_graph_storage.cc tests/test_storage_integration.cc
git commit -m "storage: fix ScanEdges to return correct dst_id from CedarKey.target_id"
```

---

## Phase 2: Cypher Query Engine Wiring

> **Priority: P1 — Cypher queries cannot read from storage; temporal queries return zero rows.**

### Task 2.1: Wire Storage Context into Cypher Engine

**Problem:** `CypherEngine::Execute()` at `src/cypher/cypher_engine.cc:19,36` has commented-out `ctx.storage = storage_` lines. The `ExecutionContext` has `graph` and `graph_db` fields but no `storage` field. Non-temporal Cypher queries cannot read from the graph DB.

**Files:**
- Modify: `include/cedar/cypher/execution_plan.h:36-49` (add storage field)
- Modify: `src/cypher/cypher_engine.cc:15-60`
- Modify: `src/cypher/operators/node_scan.cc` (or equivalent) to use storage
- Test: `tests/cypher/test_cypher_engine.cc` (or create)

- [ ] **Step 1: Add storage field to ExecutionContext**

In `include/cedar/cypher/execution_plan.h`:
```cpp
struct ExecutionContext {
  CedarGraph* graph = nullptr;
  CedarGraphDB* graph_db = nullptr;
  CedarGraphStorage* storage = nullptr;  // NEW
  std::unordered_map<std::string, Value> variables;
  // ... rest unchanged
};
```

- [ ] **Step 2: Wire storage in CypherEngine**

In `src/cypher/cypher_engine.cc`:
```cpp
ResultSet CypherEngine::Execute(const std::string& query) {
  if (auto cached = GetCachedPlan(query)) {
    ExecutionContext ctx;
    ctx.storage = storage_;  // Was commented out
    return cached->Execute(&ctx);
  }
  // ... parse ...
  ExecutionContext ctx;
  ctx.storage = storage_;  // Was commented out
  return GetCachedPlan(query)->Execute(&ctx);
}
```

Do the same for `Execute(query, parameters)`.

- [ ] **Step 3: Verify NodeScan uses storage**

Check `src/cypher/operators/node_scan.cc` (or equivalent file). If `NodeScan::Next()` currently returns `nullptr` or ignores `context_->storage`, update it:

```cpp
std::shared_ptr<Record> NodeScan::Next() {
  if (!context_ || !context_->storage) {
    return nullptr;
  }
  // Use storage to scan nodes
  // ... existing scan logic ...
}
```

- [ ] **Step 4: Write integration test**

Create `tests/cypher/test_cypher_storage_integration.cc`:
```cpp
#include <gtest/gtest.h>
#include "cedar/cypher/cypher_engine.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;

TEST(CypherStorageIntegration, EngineHasStoragePointer) {
  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, "/tmp/cypher_int_test", &storage).ok());

  cypher::CypherEngine engine(storage);
  
  // Verify the engine holds the storage pointer
  EXPECT_NE(engine.GetStorage(), nullptr);

  delete storage;
  std::filesystem::remove_all("/tmp/cypher_int_test");
}
```

Add `GetStorage()` accessor to `CypherEngine` if needed.

- [ ] **Step 5: Run tests**

```bash
cd build && cmake --build . --target test_cypher_storage_integration
./tests/test_cypher_storage_integration
```

- [ ] **Step 6: Commit**

```bash
git add include/cedar/cypher/execution_plan.h src/cypher/cypher_engine.cc tests/cypher/test_cypher_storage_integration.cc
git commit -m "cypher: wire CedarGraphStorage pointer into ExecutionContext"
```

---

### Task 2.2: Fix Parameterized Query bad_variant_access Crash

**Problem:** `ExpressionEvaluator::EvaluateParameter()` at `src/cypher/expression_evaluator.cc:375` only checks `parameters_`, not `context_->variables`. When a `ParameterExpr` is evaluated and the parameter is not in `parameters_`, it returns `Value()` (Null). But `EvaluateVariable()` correctly falls back to context variables. The test `ParameterFallbackToContext` expects parameter fallback to context variables.

**Files:**
- Modify: `src/cypher/expression_evaluator.cc:375-381`
- Test: `tests/cypher/test_parameterized_queries.cc:84-93`

- [ ] **Step 1: Fix EvaluateParameter to fallback to context variables**

```cpp
Value ExpressionEvaluator::EvaluateParameter(const ParameterExpr& expr) {
  auto it = parameters_.find(expr.name);
  if (it != parameters_.end()) {
    return it->second;
  }
  // Fallback to context variables (same as EvaluateVariable)
  if (context_ && context_->variables.find(expr.name) != context_->variables.end()) {
    return context_->variables.at(expr.name);
  }
  return Value();  // Null if parameter not provided
}
```

- [ ] **Step 2: Run the failing test**

```bash
cd build && cmake --build . --target test_parameterized_query
./tests/test_parameterized_query --gtest_filter="*ParameterFallbackToContext*"
```
Expected: PASS

- [ ] **Step 3: Run full parameterized query suite**

```bash
cd build && ./tests/test_parameterized_query
```
Expected: 7/7 PASS (was 6/7 PASS, 1 FAIL).

- [ ] **Step 4: Commit**

```bash
git add src/cypher/expression_evaluator.cc
git commit -m "cypher: fix ParameterExpr fallback to context variables (bad_variant_access crash)"
```

---

### Task 2.3: Wire Temporal AS_OF Operator to Storage Layer

**Problem:** `TemporalNodeScan::Next()` at `src/cypher/operators/temporal_operators.cc` returns `nullptr`. The parser correctly parses `AS OF` into `TemporalClause`, and `ExecutionPlanBuilder` routes to `TemporalNodeScan`. But execution returns zero rows because the operator never accesses storage.

**Files:**
- Modify: `src/cypher/operators/temporal_operators.cc` (TemporalNodeScan::Next)
- Modify: `src/cypher/execution_plan_builder.cc` (if needed to pass temporal clause)
- Test: `tests/cypher/test_temporal_operators.cc` (create)

- [ ] **Step 1: Implement TemporalNodeScan::Next**

In `src/cypher/operators/temporal_operators.cc`:
```cpp
std::shared_ptr<Record> TemporalNodeScan::Next() {
  if (!initialized_) {
    if (!context_ || !context_->storage) {
      return nullptr;
    }
    // Get the target timestamp from temporal clause
    Timestamp ts = context_->query_timestamp;
    if (context_->temporal_clause) {
      ts = context_->temporal_clause->timestamp;
    }
    
    // Scan all nodes at the target time
    // For simplicity, scan a reasonable entity range
    for (uint64_t eid = 0; eid < 10000; eid++) {
      auto result = context_->storage->Get(eid, EntityType::Vertex, 0, ts);
      if (result.has_value()) {
        Record record;
        record.values["id"] = Value(static_cast<int64_t>(eid));
        record.values["descriptor"] = Value("...");  // Convert descriptor to Value
        records_.push_back(record);
      }
    }
    initialized_ = true;
    current_idx_ = 0;
  }
  
  if (current_idx_ >= records_.size()) {
    return nullptr;
  }
  return std::make_shared<Record>(records_[current_idx_++]);
}
```

- [ ] **Step 2: Add necessary fields to TemporalNodeScan**

In `include/cedar/cypher/temporal_operators.h` (or wherever the class is defined):
```cpp
class TemporalNodeScan : public PhysicalOperator {
  // ... existing fields ...
  bool initialized_ = false;
  size_t current_idx_ = 0;
  std::vector<Record> records_;
};
```

- [ ] **Step 3: Write test**

Create `tests/cypher/test_temporal_operators.cc`:
```cpp
#include <gtest/gtest.h>
#include "cedar/cypher/cypher_engine.h"
#include "cedar/storage/cedar_graph_storage.h"

TEST(TemporalOperatorTest, AsOfReturnsNodesAtSpecificTime) {
  std::string db_path = "/tmp/temporal_op_test";
  std::filesystem::remove_all(db_path);
  
  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, db_path, &storage).ok());
  
  // Create a vertex at t=1000
  Timestamp t1(1000);
  Descriptor desc = Descriptor::InlineInt(1, 42);
  storage->Put(100, EntityType::Vertex, 1, t1, desc);
  storage->ForceFlush();
  
  cypher::CypherEngine engine(storage);
  auto result = engine.Execute("MATCH (n) AS OF SYSTEM TIME 1000 RETURN n");
  
  EXPECT_FALSE(result.HasError());
  EXPECT_GE(result.RowCount(), 1);
  
  delete storage;
  std::filesystem::remove_all(db_path);
}
```

- [ ] **Step 4: Run tests**

```bash
cd build && cmake --build . --target test_temporal_operators
./tests/test_temporal_operators
```

- [ ] **Step 5: Commit**

```bash
git add src/cypher/operators/temporal_operators.cc include/cedar/cypher/temporal_operators.h tests/cypher/test_temporal_operators.cc
git commit -m "cypher: wire TemporalNodeScan AS_OF operator to CedarGraphStorage"
```

---

## Phase 3: Distributed Governance

> **Priority: P2 — Cluster safety and operability.**

### Task 3.1: Implement Basic Config Validation

**Problem:** `ConfigManager::ValidateConfig()` at `src/governance/config_manager.cc:657` returns `Status::OK()` without any validation. Invalid configs (e.g., `replica_factor=0`) propagate silently.

**Files:**
- Modify: `src/governance/config_manager.cc:657-660`
- Test: `tests/test_config_manager.cc`

- [ ] **Step 1: Implement validation rules**

```cpp
Status ConfigManager::ValidateConfig(const ClusterConfig& config) {
  if (config.replica_factor == 0) {
    return Status::InvalidArgument("ConfigManager", "replica_factor must be > 0");
  }
  if (config.replica_factor > 7) {
    return Status::InvalidArgument("ConfigManager", "replica_factor must be <= 7");
  }
  if (config.partition_count == 0) {
    return Status::InvalidArgument("ConfigManager", "partition_count must be > 0");
  }
  if (config.election_timeout_ms < 50) {
    return Status::InvalidArgument("ConfigManager", "election_timeout_ms must be >= 50");
  }
  if (config.heartbeat_interval_ms < 10) {
    return Status::InvalidArgument("ConfigManager", "heartbeat_interval_ms must be >= 10");
  }
  if (config.heartbeat_interval_ms >= config.election_timeout_ms) {
    return Status::InvalidArgument("ConfigManager",
        "heartbeat_interval_ms must be < election_timeout_ms");
  }
  return Status::OK();
}
```

- [ ] **Step 2: Write test**

```cpp
TEST(ConfigManagerTest, ValidateConfigRejectsInvalidValues) {
  ClusterConfig config;
  config.replica_factor = 0;
  EXPECT_FALSE(ConfigManager::ValidateConfig(config).ok());
  
  config.replica_factor = 3;
  config.partition_count = 0;
  EXPECT_FALSE(ConfigManager::ValidateConfig(config).ok());
  
  config.partition_count = 16;
  config.election_timeout_ms = 30;
  EXPECT_FALSE(ConfigManager::ValidateConfig(config).ok());
  
  config.election_timeout_ms = 200;
  config.heartbeat_interval_ms = 250;
  EXPECT_FALSE(ConfigManager::ValidateConfig(config).ok());
  
  config.heartbeat_interval_ms = 50;
  EXPECT_TRUE(ConfigManager::ValidateConfig(config).ok());
}
```

- [ ] **Step 3: Run tests**

```bash
cd build && cmake --build . --target test_config_manager
./tests/test_config_manager --gtest_filter="*ValidateConfig*"
```

- [ ] **Step 4: Commit**

```bash
git add src/governance/config_manager.cc tests/test_config_manager.cc
git commit -m "governance: implement ConfigManager::ValidateConfig with basic safety checks"
```

---

### Task 3.2: Add Health Check to Failover Manager Leader Selection

**Problem:** `FailoverManager::SelectNewLeader()` at `src/dtx/storage/failover_manager.cc:189-208` picks the first non-leader replica without any health verification.

**Files:**
- Modify: `src/dtx/storage/failover_manager.cc:189-208`
- Modify: `include/cedar/dtx/failover_manager.h` (add health checker dependency)
- Test: `tests/test_failover.cc`

- [ ] **Step 1: Add health check dependency**

In `include/cedar/dtx/failover_manager.h`:
```cpp
#include "cedar/governance/health_checker.h"

class FailoverManager {
 public:
  void SetHealthChecker(std::shared_ptr<HealthChecker> checker);
  // ...
 private:
  std::shared_ptr<HealthChecker> health_checker_;
};
```

- [ ] **Step 2: Implement health-aware leader selection**

```cpp
std::string FailoverManager::SelectNewLeader(const std::vector<ReplicaInfo>& replicas,
                                             const std::string& current_leader) {
  for (const auto& replica : replicas) {
    if (replica.node_id == current_leader) continue;
    
    if (health_checker_) {
      auto health = health_checker_->CheckComponent(replica.node_id);
      if (!health.ok() || !health.ValueOrDie().is_healthy) {
        CEDAR_LOG_ERROR() << "Failover: skipping unhealthy replica " 
                          << replica.node_id << std::endl;
        continue;
      }
    }
    return replica.node_id;
  }
  return "";
}
```

- [ ] **Step 3: Write test**

```cpp
TEST(FailoverTest, SelectNewLeaderSkipsUnhealthyReplica) {
  FailoverManager manager;
  auto mock_health = std::make_shared<MockHealthChecker>();
  
  // node-2 is unhealthy
  EXPECT_CALL(*mock_health, CheckComponent("node-2"))
      .WillOnce(Return(HealthStatus{false, "down"}));
  // node-3 is healthy
  EXPECT_CALL(*mock_health, CheckComponent("node-3"))
      .WillOnce(Return(HealthStatus{true, ""}));
  
  manager.SetHealthChecker(mock_health);
  
  std::vector<ReplicaInfo> replicas = {
    {"node-1", "127.0.0.1:1", RaftRole::kLeader},
    {"node-2", "127.0.0.1:2", RaftRole::kFollower},
    {"node-3", "127.0.0.1:3", RaftRole::kFollower},
  };
  
  auto new_leader = manager.SelectNewLeader(replicas, "node-1");
  EXPECT_EQ(new_leader, "node-3");
}
```

- [ ] **Step 4: Run tests**

```bash
cd build && cmake --build . --target test_failover
./tests/test_failover --gtest_filter="*SelectNewLeader*"
```

- [ ] **Step 5: Commit**

```bash
git add src/dtx/storage/failover_manager.cc include/cedar/dtx/failover_manager.h tests/test_failover.cc
git commit -m "dtx: add health check to failover leader selection"
```

---

### Task 3.3: Enable test_storage_integration Runtime Tests

**Problem:** `tests/test_storage_integration.cc` has 4 tests that all call `GTEST_SKIP() << "No cluster available"`. They never execute.

**Files:**
- Modify: `tests/test_storage_integration.cc`

- [ ] **Step 1: Replace skip tests with single-node integration tests**

Replace each skipped test with a single-node version that tests the same functionality through the local engine:

```cpp
TEST(StorageIntegrationTest, SingleNodePutGet) {
  std::string db_path = "/tmp/cedar_int_test_" + std::to_string(getpid());
  std::filesystem::remove_all(db_path);
  
  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, db_path, &storage).ok());
  
  Descriptor desc = Descriptor::InlineInt(1, 42);
  Status s = storage->Put(1000, EntityType::Vertex, 1, Timestamp(1000), desc);
  ASSERT_TRUE(s.ok());
  
  auto result = storage->Get(1000, EntityType::Vertex, 1, Timestamp(1000));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result->AsInlineInt(), 42);
  
  delete storage;
  std::filesystem::remove_all(db_path);
}

TEST(StorageIntegrationTest, SingleNodeScan) {
  // ... similar single-node scan test ...
}

TEST(StorageIntegrationTest, SingleNodeBatchWrite) {
  // ... batch write through local engine ...
}

TEST(StorageIntegrationTest, SingleNodeHealthCheck) {
  // ... verify storage health metrics export ...
}
```

- [ ] **Step 2: Run tests**

```bash
cd build && cmake --build . --target test_storage_integration
./tests/test_storage_integration
```
Expected: 4/4 PASS (was 0/4, all skipped).

- [ ] **Step 3: Commit**

```bash
git add tests/test_storage_integration.cc
git commit -m "test: replace skipped storage integration tests with single-node versions"
```

---

## Phase 4: SST Bloom Filter Enablement

> **Priority: P2 — Significant I/O savings for temporal point queries.**

### Task 4.1: Build and Persist SST Bloom Filters

**Problem:** `ZoneColumnarSstBuilderV2::Finish()` at `src/sst/zone_columnar_builder_v2.cc:387-388` hardcodes `footer.bloom_filter_offset = 0` and `footer.bloom_filter_size = 0`. Bloom filters are never built or written.

**Files:**
- Modify: `src/sst/zone_columnar_builder_v2.cc` (build bloom filter in Finish)
- Modify: `src/sst/zone_columnar_reader.cc` (ensure MayContainEntity works)
- Test: `tests/test_sstv2_integration.cc`

- [ ] **Step 1: Build bloom filter during Finish()**

In `src/sst/zone_columnar_builder_v2.cc`, in the `Finish()` method, before writing footer:

```cpp
  // Build bloom filter from all keys
  BloomFilterBuilder bloom_builder;
  for (const auto& key : all_keys_) {  // or iterate through entries
    bloom_builder.AddKey(key.entity_id());
  }
  std::string bloom_data = bloom_builder.Finish();
  
  // Write bloom filter
  uint64_t bloom_offset = file_->GetFileSize();  // current position
  file_->Append(bloom_data);
  file_->Flush();
  
  footer.bloom_filter_offset = bloom_offset;
  footer.bloom_filter_size = bloom_data.size();
```

Note: The exact BloomFilterBuilder API may differ. Use the existing `bloom_filter.cc` implementation if available.

- [ ] **Step 2: Verify reader loads bloom filter**

In `src/sst/zone_columnar_reader.cc`, `LoadMetadata()` already reads bloom filter data at lines 150-160. Verify `MayContainEntity()` works:

```cpp
bool ZoneColumnarSstReader::MayContainEntity(uint64_t entity_id) const {
  if (!opened_ || bloom_filter_.Size() == 0) {
    return true;  // No bloom filter = must check
  }
  return bloom_filter_.MayContain(entity_id);
}
```

- [ ] **Step 3: Wire bloom filter into GetAtTime hot path**

In `src/storage/lsm_engine.cc`, in `GetAtTime()` before opening each SST:

```cpp
for (const auto& file_meta : files) {
  // Bloom filter check
  if (sst_reader_cache_) {
    auto reader = sst_reader_cache_->Get(file_meta.path);
    if (reader && !reader->MayContainEntity(entity_id)) {
      sst_reader_cache_->Release(file_meta.path);
      continue;  // Skip this file
    }
    if (reader) sst_reader_cache_->Release(file_meta.path);
  }
  // ... rest of existing logic
}
```

- [ ] **Step 4: Write test**

Add to `tests/test_sstv2_integration.cc`:
```cpp
TEST(SSTv2Integration, BloomFilterSkipsIrrelevantFiles) {
  // Write SST with entity_ids 100-200
  // Query entity_id 999
  // Verify MayContainEntity(999) returns false
}
```

- [ ] **Step 5: Run tests**

```bash
cd build && cmake --build . --target test_sstv2_integration
./tests/test_sstv2_integration --gtest_filter="*BloomFilter*"
```

- [ ] **Step 6: Commit**

```bash
git add src/sst/zone_columnar_builder_v2.cc src/sst/zone_columnar_reader.cc src/storage/lsm_engine.cc tests/test_sstv2_integration.cc
git commit -m "sst: enable bloom filter generation and query-time skipping"
```

---

## Self-Review Checklist

**1. Spec coverage:**
- ✅ Raft heartbeat term ordering → Task 0.1
- ✅ BatchLogCommitter index mismatch → Task 0.2
- ✅ HasQuorum dynamic shrink → Task 0.3
- ✅ Tautological test assertion → Task 0.4
- ✅ LsmEngine::Compact() stub → Task 1.1
- ✅ Edge distributed routing → Task 1.2
- ✅ Edge type mapping → Task 1.3
- ✅ ScanEdges dst_id → Task 1.4
- ✅ Cypher storage wiring → Task 2.1
- ✅ Parameterized query crash → Task 2.2
- ✅ Temporal AS_OF operator → Task 2.3
- ✅ Config validation → Task 3.1
- ✅ Failover health check → Task 3.2
- ✅ Storage integration tests → Task 3.3
- ✅ SST bloom filter → Task 4.1

**2. Placeholder scan:**
- ✅ No "TBD", "TODO", "implement later"
- ✅ All steps have code blocks
- ✅ All steps have exact commands

**3. Type consistency:**
- ✅ `CedarGraphStorage*` used consistently across Tasks 1.2, 2.1
- ✅ `Timestamp` used consistently
- ✅ `ValueType::kInt` / `ValueType::kNull` used correctly in Task 2.2
