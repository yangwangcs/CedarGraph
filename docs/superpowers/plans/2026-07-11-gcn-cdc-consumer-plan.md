# GCN CDC Consumer And Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make standalone GCN instances populate and recover TMV from StorageD CDC and snapshots without in-process storage injection.

**Architecture:** `StorageCdcClient` wraps bounded StorageD RPCs, `PartitionConsumer` owns one leased partition's pull/apply loop, and `CheckpointStore` persists progress atomically. A snapshot loader rebuilds an isolated TMV partition before atomically publishing it; readiness and served versions derive from proven consumer state.

**Tech Stack:** C++17, Protobuf/gRPC, Cedar status/TLS helpers, TMVEngine/EventApplier, GoogleTest/CTest.

## Global Constraints

- Apply a record before advancing its checkpoint.
- Duplicate `(partition_id, offset)` records must not change final TMV state twice.
- Invalid or missing checkpoints trigger snapshot recovery, never guessed offsets.
- Consumer loops have bounded queues, deadlines, exponential backoff, and cancellable shutdown.
- A partition in bootstrap/recovery is not query-ready.

---

### Task 1: Implement Atomic Partition Checkpoints

**Files:**
- Create: `include/cedar/gcn/checkpoint_store.h`
- Create: `src/gcn/checkpoint_store.cc`
- Test: `tests/gcn/test_gcn_checkpoint.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces: `PartitionCheckpoint`, `CheckpointStore::Load`, `Save`, and `Remove`.

```cpp
namespace cedar::gcn {
struct PartitionCheckpoint {
  uint32_t partition_id = 0;
  uint64_t partition_epoch = 0;
  uint64_t applied_offset = 0;
  uint64_t applied_version = 0;
  std::string tmv_snapshot_id;
};

class CheckpointStore {
 public:
  explicit CheckpointStore(std::string directory);
  StatusOr<std::optional<PartitionCheckpoint>> Load(uint32_t partition_id) const;
  Status Save(const PartitionCheckpoint& checkpoint);
  Status Remove(uint32_t partition_id);
};
}  // namespace cedar::gcn
```

- [ ] **Step 1: Write failing round-trip, torn-write, checksum, and traversal tests**

```cpp
TEST(CheckpointStoreTest, SavesAndLoadsAtomically) {
  CheckpointStore store(temp_dir());
  PartitionCheckpoint expected{4, 8, 101, 900, "snapshot-900"};
  ASSERT_OK(store.Save(expected));
  ASSERT_OK_AND_ASSIGN(auto loaded, store.Load(4));
  ASSERT_TRUE(loaded.has_value());
  EXPECT_EQ(loaded->applied_offset, 101);
  EXPECT_EQ(loaded->tmv_snapshot_id, "snapshot-900");
}

TEST(CheckpointStoreTest, RejectsChecksumCorruption) {
  CheckpointStore store(temp_dir());
  ASSERT_OK(store.Save({4, 8, 101, 900, "snapshot-900"}));
  CorruptCheckpoint(temp_dir(), 4);
  EXPECT_TRUE(store.Load(4).status().IsCorruption());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_gcn_checkpoint -j4`

Expected: FAIL because `CheckpointStore` is missing.

- [ ] **Step 3: Implement versioned files with CRC and atomic replacement**

Serialize a fixed version header plus checkpoint payload and CRC32C. Write a sibling temporary file, fsync it, rename it over the destination, and fsync the directory. Open the directory without following symlinks and reject partition IDs outside the configured range.

- [ ] **Step 4: Run checkpoint tests**

Run: `cmake --build build --target test_gcn_checkpoint -j4 && ./build/tests/test_gcn_checkpoint`

Expected: all checkpoint tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/gcn/checkpoint_store.h src/gcn/checkpoint_store.cc tests/gcn/test_gcn_checkpoint.cc tests/CMakeLists.txt
git commit -m "feat(gcn): persist partition checkpoints"
```

### Task 2: Add The StorageD CDC Client

**Files:**
- Create: `include/cedar/gcn/storage_cdc_client.h`
- Create: `src/gcn/storage_cdc_client.cc`
- Test: `tests/gcn/test_storage_cdc_client.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: StorageD CDC and snapshot RPCs from the storage plan.
- Produces: `StorageCdcClient::GetState`, `Fetch`, and `StreamSnapshot`.

```cpp
class StorageCdcClient {
 public:
  struct Options {
    std::chrono::milliseconds rpc_timeout{3000};
    uint32_t max_records = 1024;
    uint64_t max_bytes = 4 * 1024 * 1024;
  };
  StorageCdcClient(std::shared_ptr<grpc::Channel> channel, Options options);
  StatusOr<cedar::storage::GetChangeLogStateResponse> GetState(
      uint32_t partition_id, uint64_t expected_epoch);
  StatusOr<cedar::storage::FetchChangesResponse> Fetch(
      uint32_t partition_id, uint64_t after_offset, uint64_t expected_epoch);
  Status StreamSnapshot(uint32_t partition_id, uint64_t snapshot_version,
                        const std::function<Status(const ComputeSnapshotBatch&)>& on_batch);
};
```

- [ ] **Step 1: Write failing fake-server tests for bounds, deadlines, cancellation, and stale epoch**

```cpp
TEST_F(StorageCdcClientTest, SendsConfiguredBoundsAndReturnsRecords) {
  fake_->SetRecords(MakeRecords(6));
  ASSERT_OK_AND_ASSIGN(auto response, client_->Fetch(3, 2, 7));
  EXPECT_EQ(fake_->last_request().limit_records(), 1024);
  EXPECT_EQ(fake_->last_request().limit_bytes(), 4 * 1024 * 1024);
  EXPECT_EQ(response.records(0).offset(), 3);
}

TEST_F(StorageCdcClientTest, MapsStaleEpochWithoutRetryingOldLeader) {
  fake_->ReturnStaleEpoch();
  auto result = client_->Fetch(3, 2, 7);
  EXPECT_TRUE(result.status().IsAborted());
  EXPECT_EQ(fake_->fetch_count(), 1);
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_storage_cdc_client -j4`

Expected: FAIL because `StorageCdcClient` is missing.

- [ ] **Step 3: Implement the TLS-authenticated wrapper**

Create a fresh `grpc::ClientContext` per call, attach existing auth metadata, set a deadline, validate response sizes even if the server misbehaves, and map stale epoch distinctly from retryable availability errors.

- [ ] **Step 4: Run client and TLS tests**

Run: `cmake --build build --target test_storage_cdc_client test_tls_fail_secure -j4 && ctest --test-dir build --output-on-failure -R 'storage_cdc_client|tls_fail_secure'`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/gcn/storage_cdc_client.h src/gcn/storage_cdc_client.cc tests/gcn/test_storage_cdc_client.cc tests/CMakeLists.txt
git commit -m "feat(gcn): add StorageD CDC client"
```

### Task 3: Make Event Application Offset-Idempotent

**Files:**
- Modify: `include/cedar/gcn/event_applier.h`
- Modify: `src/gcn/event_applier.cc`
- Modify: `include/cedar/gcn/tmv_engine.h`
- Modify: `src/gcn/tmv_engine.cc`
- Test: `tests/gcn/test_event_applier_offsets.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: CDC `ChangeRecord`.
- Produces: `EventApplier::ApplyChangeRecord` and per-partition applied offset/version.

```cpp
Status EventApplier::ApplyChangeRecord(const cedar::cdc::ChangeRecord& record);
uint64_t EventApplier::AppliedOffset(uint32_t partition_id) const;
uint64_t EventApplier::AppliedVersion(uint32_t partition_id) const;
```

- [ ] **Step 1: Write failing duplicate, gap, delete, and batch tests**

```cpp
TEST(EventApplierOffsetTest, DuplicateOffsetIsNoOp) {
  EventApplier applier(&engine_);
  auto record = MakeEdgeCreate(2, 1, 10, 20);
  ASSERT_OK(applier.ApplyChangeRecord(record));
  ASSERT_OK(applier.ApplyChangeRecord(record));
  EXPECT_EQ(engine_.ScanAtTime(10, Direction::kOut, 100).size(), 1);
  EXPECT_EQ(applier.AppliedOffset(2), 1);
}

TEST(EventApplierOffsetTest, RejectsOffsetGap) {
  EventApplier applier(&engine_);
  EXPECT_TRUE(applier.ApplyChangeRecord(MakeEdgeCreate(2, 2, 10, 20))
                  .IsAborted());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_event_applier_offsets -j4`

Expected: FAIL because offset-aware APIs do not exist.

- [ ] **Step 3: Implement per-partition sequencing and deterministic mapping**

Reject gaps, ignore duplicates at or below the applied offset, validate a complete transaction batch before mutation, and map create/update/delete to TMV operations using the record's actual target, edge type, and valid-time range. Protect partition progress with partition-scoped synchronization rather than a single global lock.

- [ ] **Step 4: Run old and new EventApplier suites**

Run: `cmake --build build --target test_event_applier test_event_applier_offsets event_applier_backpressure_test -j4 && ctest --test-dir build --output-on-failure -R 'event_applier'`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/gcn/event_applier.h src/gcn/event_applier.cc include/cedar/gcn/tmv_engine.h src/gcn/tmv_engine.cc tests/gcn/test_event_applier_offsets.cc tests/CMakeLists.txt
git commit -m "feat(gcn): apply CDC records idempotently"
```

### Task 4: Implement Per-Partition Consumption And Recovery

**Files:**
- Create: `include/cedar/gcn/partition_consumer.h`
- Create: `src/gcn/partition_consumer.cc`
- Create: `include/cedar/gcn/snapshot_loader.h`
- Create: `src/gcn/snapshot_loader.cc`
- Test: `tests/gcn/test_gcn_cdc_consumer.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `StorageCdcClient`, `CheckpointStore`, and offset-aware `EventApplier`.
- Produces: `PartitionConsumer::Start`, `Stop`, `GetProgress`, and readiness state.

```cpp
enum class ConsumerState { kStarting, kBackfilling, kCatchingUp, kReady, kFailed, kStopped };
struct ConsumerProgress {
  ConsumerState state;
  uint64_t partition_epoch;
  uint64_t applied_offset;
  uint64_t applied_version;
  uint64_t high_watermark;
  std::string last_error;
};

class PartitionConsumer {
 public:
  Status Start(PartitionLease lease);
  Status Stop(std::chrono::milliseconds deadline);
  ConsumerProgress GetProgress() const;
};
```

- [ ] **Step 1: Write failing tests for normal catch-up, restart, duplicate delivery, stale epoch, log expiry, and bounded stop**

```cpp
TEST_F(PartitionConsumerTest, RestartsFromDurableCheckpoint) {
  storage_->Append(MakeRecords(1, 5));
  StartConsumer();
  ASSERT_TRUE(WaitUntilApplied(5));
  StopConsumer();
  storage_->Append(MakeRecords(6, 8));
  StartConsumer();
  ASSERT_TRUE(WaitUntilApplied(8));
  EXPECT_EQ(storage_->first_requested_after_on_restart(), 5);
}

TEST_F(PartitionConsumerTest, ExpiredOffsetTriggersSnapshotThenResumesCdc) {
  checkpoint_store_->Save({3, 4, 2, 20, "old"});
  storage_->SetState(/*earliest=*/10, /*high=*/12, /*version=*/120);
  storage_->SetSnapshot(/*version=*/100, /*resume_offset=*/10, SnapshotEdges());
  StartConsumer();
  ASSERT_TRUE(WaitUntilReady());
  EXPECT_TRUE(storage_->snapshot_requested());
  EXPECT_EQ(consumer_->GetProgress().applied_offset, 12);
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_gcn_cdc_consumer -j4`

Expected: FAIL because consumer and loader are missing.

- [ ] **Step 3: Implement the state machine and isolated snapshot publish**

The loop validates the lease epoch, loads a checkpoint, fetches state, enters backfill if required, applies batches, saves checkpoint, and publishes progress. Use condition-variable cancellation and capped exponential backoff. `SnapshotLoader` builds into a temporary TMV partition, verifies sequence/checksum, then swaps it into the live engine before writing the checkpoint.

- [ ] **Step 4: Run consumer, checkpoint, and TMV tests**

Run: `cmake --build build --target test_gcn_cdc_consumer test_gcn_checkpoint test_tmv_engine -j4 && ctest --test-dir build --output-on-failure -R 'gcn_cdc_consumer|gcn_checkpoint|tmv_engine'`

Expected: all selected tests pass and shutdown completes within the asserted deadline.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/gcn/partition_consumer.h src/gcn/partition_consumer.cc include/cedar/gcn/snapshot_loader.h src/gcn/snapshot_loader.cc tests/gcn/test_gcn_cdc_consumer.cc tests/CMakeLists.txt
git commit -m "feat(gcn): consume CDC with restart recovery"
```

### Task 5: Wire Standalone GcnNode And Versioned Responses

**Files:**
- Modify: `include/cedar/gcn/gcn_node.h`
- Modify: `src/gcn/gcn_node.cc`
- Modify: `tools/graphcomputenode.cc`
- Modify: `proto/gcn_service.proto`
- Modify: `include/cedar/gcn/gcn_service.h`
- Modify: `src/gcn/gcn_service.cc`
- Modify: `src/gcn/query_dispatcher.cc`
- Test: `tests/gcn/test_gcn_node_cdc.cc`
- Test: `tests/gcn/test_gcn_version_gate.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: partition consumers and later MetaD leases.
- Produces: a standalone node with health/readiness and responses containing epoch, served version, and cache status.

- [ ] **Step 1: Write failing node and version-gate tests**

```cpp
TEST(GcnVersionGateTest, RejectsRequestAheadOfAppliedVersion) {
  service_.SetPartitionProgress(3, /*epoch=*/4, /*applied_version=*/99, true);
  auto response = Traverse(/*partition=*/3, /*required_version=*/100);
  EXPECT_FALSE(response.success());
  EXPECT_EQ(response.cache_status(), CACHE_STATUS_VERSION_LAG);
  EXPECT_EQ(response.served_version(), 99);
}

TEST(GcnNodeCdcTest, StandaloneNodeConsumesWithoutSetStorage) {
  StartFakeMetaAndStorage();
  GcnNode node(NodeOptionsForServers());
  ASSERT_OK(node.Initialize());
  ASSERT_OK(node.Start());
  ASSERT_TRUE(WaitForReady(node));
  EXPECT_GT(node.GetPartitionProgress(3).applied_offset, 0);
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_gcn_node_cdc test_gcn_version_gate -j4`

Expected: FAIL because standalone CDC wiring and response metadata are absent.

- [ ] **Step 3: Replace the unfinished polling path with consumer orchestration**

Parse explicit node id, advertise endpoint, data directory, MetaD endpoints, RPC limits, and readiness settings. Remove the co-located storage polling loop from production startup. Start/stop partition consumers as leases change. Do not mark ready until registration, lease acquisition, writable checkpoint storage, and partition catch-up conditions hold.

Extend GCN responses with `partition_epoch`, `served_version`, and `CacheStatus`; have QueryDispatcher validate `required_version` before reading TMV.

- [ ] **Step 4: Run all focused GCN tests**

Run: `cmake --build build --target test_gcn_node_cdc test_gcn_version_gate test_gcn_service test_query_dispatcher -j4 && ctest --test-dir build --output-on-failure -R 'gcn_node_cdc|gcn_version_gate|gcn_service|query_dispatcher'`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/gcn/gcn_node.h src/gcn/gcn_node.cc tools/graphcomputenode.cc proto/gcn_service.proto include/cedar/gcn/gcn_service.h src/gcn/gcn_service.cc src/gcn/query_dispatcher.cc tests/gcn/test_gcn_node_cdc.cc tests/gcn/test_gcn_version_gate.cc tests/CMakeLists.txt
git commit -m "feat(gcn): run CDC consumers in standalone nodes"
```

### Task 6: Persist TMV Snapshots And Derive Safe Watermarks

**Files:**
- Create: `include/cedar/gcn/tmv_snapshot_store.h`
- Create: `src/gcn/tmv_snapshot_store.cc`
- Modify: `include/cedar/gcn/tmv_engine.h`
- Modify: `src/gcn/tmv_engine.cc`
- Modify: `include/cedar/gcn/watermark_gc.h`
- Modify: `src/gcn/watermark_gc.cc`
- Modify: `src/gcn/gcn_node.cc`
- Test: `tests/gcn/test_tmv_snapshot_recovery.cc`
- Test: `tests/gcn/test_gcn_watermark_safety.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: applied partition offsets/versions and active-query minimum snapshots.
- Produces: versioned TMV snapshots, atomic partition restore, safe GC watermark, readiness metrics.

- [ ] **Step 1: Write failing snapshot and watermark tests**

```cpp
TEST(TmvSnapshotRecoveryTest, RestoresEdgesAndCheckpointIdentity) {
  SeedEngine(engine_, /*partition=*/3);
  ASSERT_OK(store_.SavePartition(engine_, 3, 100, 20));
  TMVEngine restored(16);
  ASSERT_OK_AND_ASSIGN(auto metadata, store_.RestorePartition(restored, 3));
  EXPECT_EQ(metadata.applied_version, 100);
  EXPECT_EQ(restored.ScanAtTime(42, Direction::kOut, 100), ExpectedEdges());
}

TEST(GcnWatermarkSafetyTest, NeverPassesActiveQueryOrAppliedVersion) {
  WatermarkInputs inputs{.minimum_applied_version = 120,
                         .minimum_active_query_version = 90,
                         .retention_floor_version = 100};
  EXPECT_EQ(ComputeSafeWatermark(inputs), 90);
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_tmv_snapshot_recovery test_gcn_watermark_safety -j4`

Expected: FAIL because snapshot persistence and CDC-derived watermark logic are missing.

- [ ] **Step 3: Implement versioned snapshots and remove wall-clock-only GC**

Serialize partition TMV state into a temporary snapshot with metadata, per-block checksums, applied version/offset, and format version; fsync and atomically publish it. Restore into an isolated partition and swap only after full validation. Compute the GC watermark as the minimum safe bound from applied CDC, active query snapshots, and retention policy; remove the fixed `now - 60 seconds` production heuristic.

- [ ] **Step 4: Run snapshot, GC, and node restart suites**

Run: `cmake --build build --target test_tmv_snapshot_recovery test_gcn_watermark_safety test_watermark_gc test_gcn_node_cdc -j4 && ctest --test-dir build --output-on-failure -R 'tmv_snapshot_recovery|gcn_watermark_safety|watermark_gc|gcn_node_cdc'`

Expected: all selected tests pass, including corrupt snapshot fallback to StorageD snapshot backfill.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/gcn/tmv_snapshot_store.h src/gcn/tmv_snapshot_store.cc include/cedar/gcn/tmv_engine.h src/gcn/tmv_engine.cc include/cedar/gcn/watermark_gc.h src/gcn/watermark_gc.cc src/gcn/gcn_node.cc tests/gcn/test_tmv_snapshot_recovery.cc tests/gcn/test_gcn_watermark_safety.cc tests/CMakeLists.txt
git commit -m "feat(gcn): persist TMV snapshots and safe watermarks"
```
