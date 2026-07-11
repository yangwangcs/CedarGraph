# StorageD Durable CDC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist committed per-partition CDC records in StorageD and expose bounded pull and snapshot RPCs.

**Architecture:** A focused `PartitionChangeLog` owns append-only segments, manifest recovery, offsets, checksums, and retention. StorageD appends deterministic `ChangeRecord` values only from committed apply paths and serves them through backward-compatible StorageService RPC additions.

**Tech Stack:** C++17, Cedar status/filesystem helpers, Protobuf/gRPC, GoogleTest/CTest, braft state-machine apply paths.

## Global Constraints

- CDC offsets are strictly increasing and gap-free per partition.
- Uncommitted and aborted transactions never advance the visible high watermark.
- Middle-of-log corruption fails recovery; only a partial tail record may be truncated.
- RPC requests enforce record-count and byte-size limits.
- All on-disk replacements use file fsync, atomic rename, and parent-directory fsync.

---

### Task 1: Define The Change Record Wire Contract

**Files:**
- Create: `proto/cdc_service.proto`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Test: `tests/storage/test_change_record.cc`

**Interfaces:**
- Produces: `cedar::cdc::ChangeRecord`, `ChangeOperation`, and compute-snapshot messages.
- Consumes: existing entity and partition scalar encodings; no storage implementation dependency.

- [ ] **Step 1: Write the failing serialization tests**

```cpp
TEST(ChangeRecordTest, RoundTripsRequiredFields) {
  cedar::cdc::ChangeRecord record;
  record.set_partition_id(7);
  record.set_partition_epoch(3);
  record.set_offset(41);
  record.set_commit_version(99);
  record.set_txn_id(12);
  record.set_batch_index(0);
  record.set_batch_size(1);
  record.set_entity_id(100);
  record.set_target_id(200);
  record.set_operation(cedar::cdc::CHANGE_OPERATION_CREATE);
  record.set_checksum(1234);
  std::string bytes;
  ASSERT_TRUE(record.SerializeToString(&bytes));
  cedar::cdc::ChangeRecord decoded;
  ASSERT_TRUE(decoded.ParseFromString(bytes));
  EXPECT_EQ(decoded.offset(), 41);
  EXPECT_EQ(decoded.target_id(), 200);
}

TEST(ChangeRecordTest, OperationZeroIsUnspecified) {
  cedar::cdc::ChangeRecord record;
  EXPECT_EQ(record.operation(), cedar::cdc::CHANGE_OPERATION_UNSPECIFIED);
}
```

- [ ] **Step 2: Run the new target and confirm it fails before generated types exist**

Run: `cmake --build build --target test_change_record -j4`

Expected: FAIL because `cdc_service.pb.h` or `cedar::cdc::ChangeRecord` is missing.

- [ ] **Step 3: Add the proto contract and generation entry**

```proto
syntax = "proto3";
package cedar.cdc;

enum ChangeOperation {
  CHANGE_OPERATION_UNSPECIFIED = 0;
  CHANGE_OPERATION_CREATE = 1;
  CHANGE_OPERATION_UPDATE = 2;
  CHANGE_OPERATION_DELETE = 3;
}

message ChangeRecord {
  uint32 partition_id = 1;
  uint64 partition_epoch = 2;
  uint64 offset = 3;
  uint64 commit_version = 4;
  uint64 txn_id = 5;
  uint32 batch_index = 6;
  uint32 batch_size = 7;
  uint64 entity_id = 8;
  uint64 target_id = 9;
  uint32 entity_type = 10;
  uint32 edge_type = 11;
  uint32 column_id = 12;
  ChangeOperation operation = 13;
  uint64 valid_from = 14;
  uint64 valid_to = 15;
  bytes payload = 16;
  uint32 checksum = 17;
}
```

Add `proto/cdc_service.proto` to the existing protobuf generation lists in `CMakeLists.txt`, then register `test_change_record` with `gtest_discover_tests`.

- [ ] **Step 4: Build and run the test**

Run: `cmake --build build --target test_change_record -j4 && ./build/tests/test_change_record`

Expected: all ChangeRecord tests pass.

- [ ] **Step 5: Commit the wire contract**

```bash
git add proto/cdc_service.proto CMakeLists.txt tests/CMakeLists.txt tests/storage/test_change_record.cc
git commit -m "feat(cdc): define durable change record contract"
```

### Task 2: Implement Segment Persistence And Recovery

**Files:**
- Create: `include/cedar/cdc/partition_change_log.h`
- Create: `src/cdc/partition_change_log.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Test: `tests/storage/test_change_log.cc`

**Interfaces:**
- Consumes: `cedar::cdc::ChangeRecord` from Task 1.
- Produces: `PartitionChangeLog::Open`, `AppendCommittedBatch`, `ReadAfter`, `GetState`, and `Compact`.

```cpp
namespace cedar::cdc {
struct ChangeLogState {
  uint64_t partition_epoch;
  uint64_t earliest_offset;
  uint64_t high_watermark;
  uint64_t committed_version;
};

class PartitionChangeLog {
 public:
  struct Options {
    std::string directory;
    uint32_t partition_id;
    uint64_t partition_epoch;
    size_t max_segment_bytes = 64 * 1024 * 1024;
    size_t max_fetch_records = 4096;
    size_t max_fetch_bytes = 4 * 1024 * 1024;
  };
  static StatusOr<std::unique_ptr<PartitionChangeLog>> Open(Options options);
  Status AppendCommittedBatch(uint64_t commit_version,
                              std::vector<ChangeRecord> records);
  StatusOr<std::vector<ChangeRecord>> ReadAfter(uint64_t offset,
                                                size_t limit_records,
                                                size_t limit_bytes) const;
  ChangeLogState GetState() const;
  Status Compact(uint64_t retain_from_offset);
};
}  // namespace cedar::cdc
```

- [ ] **Step 1: Write failing tests for append, reopen, limits, rollover, and corruption**

```cpp
TEST(PartitionChangeLogTest, ReopensWithContinuousOffsets) {
  auto log = OpenLog(temp_dir(), 5, 9);
  ASSERT_OK(log->AppendCommittedBatch(100, MakeBatch(2)));
  log.reset();
  auto reopened = OpenLog(temp_dir(), 5, 9);
  EXPECT_EQ(reopened->GetState().earliest_offset, 1);
  EXPECT_EQ(reopened->GetState().high_watermark, 2);
  ASSERT_OK_AND_ASSIGN(auto records, reopened->ReadAfter(0, 10, 1 << 20));
  ASSERT_EQ(records.size(), 2);
  EXPECT_EQ(records[0].offset(), 1);
  EXPECT_EQ(records[1].offset(), 2);
}

TEST(PartitionChangeLogTest, RejectsMiddleRecordCorruption) {
  auto log = OpenLog(temp_dir(), 5, 9);
  ASSERT_OK(log->AppendCommittedBatch(100, MakeBatch(3)));
  CorruptSecondRecord(temp_dir());
  log.reset();
  EXPECT_FALSE(PartitionChangeLog::Open(OptionsFor(temp_dir(), 5, 9)).ok());
}
```

- [ ] **Step 2: Run the focused test and confirm failure**

Run: `cmake --build build --target test_change_log -j4`

Expected: FAIL because `PartitionChangeLog` does not exist.

- [ ] **Step 3: Implement framed records and manifest recovery**

Use a fixed frame header containing magic, format version, payload size, and CRC32C. Write records as `[header][serialized ChangeRecord]`; seal segments before rollover. Persist a versioned manifest containing segment ranges and watermarks. Validate every frame on open, truncate only an incomplete final frame, and return `Status::Corruption` for an invalid complete frame or offset gap.

```cpp
Status PartitionChangeLog::AppendCommittedBatch(
    uint64_t commit_version, std::vector<ChangeRecord> records) {
  std::lock_guard<std::mutex> lock(mu_);
  uint64_t next = state_.high_watermark + 1;
  for (size_t i = 0; i < records.size(); ++i) {
    records[i].set_partition_id(options_.partition_id);
    records[i].set_partition_epoch(options_.partition_epoch);
    records[i].set_offset(next + i);
    records[i].set_commit_version(commit_version);
    records[i].set_batch_index(static_cast<uint32_t>(i));
    records[i].set_batch_size(static_cast<uint32_t>(records.size()));
  }
  CEDAR_RETURN_IF_ERROR(AppendFramesAndSync(records));
  state_.high_watermark += records.size();
  state_.committed_version = std::max(state_.committed_version, commit_version);
  return PersistManifestAndSync();
}
```

- [ ] **Step 4: Run persistence tests**

Run: `cmake --build build --target test_change_log -j4 && ./build/tests/test_change_log`

Expected: append, reopen, bounds, rollover, tail recovery, corruption rejection, and compaction tests pass.

- [ ] **Step 5: Commit the durable log**

```bash
git add include/cedar/cdc/partition_change_log.h src/cdc/partition_change_log.cc CMakeLists.txt tests/CMakeLists.txt tests/storage/test_change_log.cc
git commit -m "feat(cdc): persist partition change logs"
```

### Task 3: Integrate CDC With Committed StorageD Apply Paths

**Files:**
- Modify: `include/cedar/dtx/storage/partition_raft_manager.h`
- Modify: `src/dtx/storage/partition_raft_manager.cc`
- Modify: `src/dtx/storage/braft_partition_state_machine.cc`
- Modify: `include/cedar/dtx/storage_service_impl.h`
- Modify: `src/dtx/storage_impl/storage_service_impl.cc`
- Modify: `tools/storaged.cc`
- Test: `tests/dtx/test_storage_cdc_commit.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `PartitionChangeLog::AppendCommittedBatch`.
- Produces: committed writes and CDC advancing as one state-machine apply outcome.

- [ ] **Step 1: Write failing commit-visibility tests**

```cpp
TEST(StorageCdcCommitTest, AbortedPrepareDoesNotAdvanceWatermark) {
  auto fixture = StorageCdcFixture::Create();
  ASSERT_OK(fixture.Prepare(11, MakeWrites(2)));
  ASSERT_OK(fixture.Abort(11));
  EXPECT_EQ(fixture.ChangeLogState().high_watermark, 0);
}

TEST(StorageCdcCommitTest, CommitAdvancesDataAndCdcTogether) {
  auto fixture = StorageCdcFixture::Create();
  ASSERT_OK(fixture.Prepare(12, MakeWrites(2)));
  ASSERT_OK(fixture.Commit(12, 500));
  EXPECT_TRUE(fixture.StorageContainsWrites());
  EXPECT_EQ(fixture.ChangeLogState().high_watermark, 2);
  EXPECT_EQ(fixture.ChangeLogState().committed_version, 500);
}

TEST(StorageCdcCommitTest, ReplayedRaftEntryIsIdempotent) {
  auto fixture = StorageCdcFixture::Create();
  auto entry = fixture.MakeCommittedEntry(13, 501, MakeWrites(1));
  ASSERT_OK(fixture.Apply(entry));
  ASSERT_OK(fixture.Apply(entry));
  EXPECT_EQ(fixture.ChangeLogState().high_watermark, 1);
}
```

- [ ] **Step 2: Run the focused test and confirm failure**

Run: `cmake --build build --target test_storage_cdc_commit -j4 && ./build/tests/test_storage_cdc_commit`

Expected: FAIL because committed apply does not write a change log.

- [ ] **Step 3: Thread ChangeRecord construction through the committed command**

Construct records from the authoritative write batch, include the records in the replicated command, and call `AppendCommittedBatch` only while applying a committed state-machine entry. For non-Raft test mode, route through the same deterministic apply helper rather than duplicating a pre-commit append.

```cpp
Status ApplyCommittedWriteBatch(const ReplicatedWriteBatch& batch) {
  CEDAR_RETURN_IF_ERROR(storage_->ApplyCommitted(batch.writes(), batch.commit_version()));
  return change_log_->AppendCommittedBatch(batch.commit_version(),
                                           BuildChangeRecords(batch));
}
```

If existing storage APIs cannot atomically recover the two durable artifacts, add a small apply-intent marker in the partition manifest and replay it during startup; never publish the CDC high watermark before storage apply is durable.

- [ ] **Step 4: Run commit, Raft replay, abort, and restart tests**

Run: `cmake --build build --target test_storage_cdc_commit test_partition_raft -j4 && ctest --test-dir build --output-on-failure -R 'storage_cdc_commit|partition_raft'`

Expected: all selected tests pass, including restart after each injected persistence boundary.

- [ ] **Step 5: Commit committed-path integration**

```bash
git add include/cedar/dtx/storage/partition_raft_manager.h src/dtx/storage/partition_raft_manager.cc src/dtx/storage/braft_partition_state_machine.cc include/cedar/dtx/storage_service_impl.h src/dtx/storage_impl/storage_service_impl.cc tools/storaged.cc tests/dtx/test_storage_cdc_commit.cc tests/CMakeLists.txt
git commit -m "feat(storage): publish CDC from committed apply"
```

### Task 4: Expose Bounded CDC And Snapshot RPCs

**Files:**
- Modify: `proto/storage_service.proto`
- Modify: `include/cedar/dtx/storage_service_impl.h`
- Modify: `src/dtx/storage_impl/storage_service_impl.cc`
- Modify: `tools/storaged.cc`
- Test: `tests/dtx/test_storage_cdc_rpc.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `PartitionChangeLog::GetState` and `ReadAfter`.
- Produces: `GetChangeLogState`, `FetchChanges`, and streaming `GetComputeSnapshot` RPCs used by the GCN plan.

- [ ] **Step 1: Write failing in-process gRPC tests**

```cpp
TEST_F(StorageCdcRpcTest, FetchChangesReturnsBoundedContinuousBatch) {
  CommitRecords(8, 20);
  cedar::storage::FetchChangesRequest request;
  request.set_partition_id(8);
  request.set_after_offset(5);
  request.set_limit_records(3);
  request.set_limit_bytes(1 << 20);
  request.set_expected_epoch(CurrentEpoch());
  cedar::storage::FetchChangesResponse response;
  ASSERT_TRUE(stub_->FetchChanges(&context_, request, &response).ok());
  ASSERT_EQ(response.records_size(), 3);
  EXPECT_EQ(response.records(0).offset(), 6);
  EXPECT_EQ(response.next_offset(), 8);
}

TEST_F(StorageCdcRpcTest, RejectsStaleEpochAndOversizedLimits) {
  EXPECT_EQ(FetchWithEpoch(CurrentEpoch() - 1).error_code(), STALE_EPOCH);
  EXPECT_EQ(FetchWithLimits(UINT32_MAX, UINT64_MAX).error_code(), INVALID_LIMIT);
}
```

- [ ] **Step 2: Build and run to verify failure**

Run: `cmake --build build --target test_storage_cdc_rpc -j4`

Expected: FAIL because the RPC methods are not defined.

- [ ] **Step 3: Add RPC messages and service implementations**

```proto
rpc GetChangeLogState(GetChangeLogStateRequest) returns (GetChangeLogStateResponse);
rpc FetchChanges(FetchChangesRequest) returns (FetchChangesResponse);
rpc GetComputeSnapshot(GetComputeSnapshotRequest) returns (stream ComputeSnapshotBatch);
```

Implement auth checks, cancellation, deadlines, epoch validation, configured limits, leader-only reads, and response high-watermark metadata. Snapshot batches must carry a stable `snapshot_version`, `resume_offset`, sequence number, final marker, and checksum.

- [ ] **Step 4: Run RPC and auth tests**

Run: `cmake --build build --target test_storage_cdc_rpc test_storage_service_auth -j4 && ctest --test-dir build --output-on-failure -R 'storage_cdc_rpc|storage_service_auth'`

Expected: all selected tests pass; unauthorized CDC/snapshot reads fail closed.

- [ ] **Step 5: Commit the StorageD RPC surface**

```bash
git add proto/storage_service.proto include/cedar/dtx/storage_service_impl.h src/dtx/storage_impl/storage_service_impl.cc tools/storaged.cc tests/dtx/test_storage_cdc_rpc.cc tests/CMakeLists.txt
git commit -m "feat(storage): expose CDC pull and snapshot RPCs"
```

### Task 5: Enforce Retention And Export StorageD CDC Metrics

**Files:**
- Create: `include/cedar/cdc/change_log_maintenance.h`
- Create: `src/cdc/change_log_maintenance.cc`
- Modify: `tools/storaged.cc`
- Modify: `config/cedar.yaml`
- Modify: `include/cedar/dtx/storage/metrics_collector.h`
- Modify: `src/dtx/storage/metrics_collector.cc`
- Test: `tests/storage/test_change_log_maintenance.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: `PartitionChangeLog::Compact` and `GetState`.
- Produces: bounded periodic retention and per-partition CDC metrics.

- [ ] **Step 1: Write failing retention and metric tests**

```cpp
TEST(ChangeLogMaintenanceTest, RetainsBothMinimumAgeAndConfiguredBytes) {
  auto log = CreateSegmentedLog({Segment(1, 10, HoursAgo(48)),
                                 Segment(11, 20, HoursAgo(1))});
  ChangeLogMaintenance maintenance({.min_retention_hours = 24,
                                    .max_retained_bytes = 1024});
  ASSERT_OK(maintenance.RunOnce(*log));
  EXPECT_LE(log.GetState().earliest_offset, 11);
  EXPECT_TRUE(log.ContainsOffset(20));
}

TEST(ChangeLogMaintenanceTest, PublishesWatermarksAndCorruptionCounts) {
  auto metrics = CollectMetricsFromLogWithState(3, 7, 20, 500);
  EXPECT_EQ(metrics.Value("cedar_cdc_high_watermark", {{"partition", "3"}}), 20);
  EXPECT_EQ(metrics.Value("cedar_cdc_earliest_offset", {{"partition", "3"}}), 7);
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_change_log_maintenance -j4`

Expected: FAIL because maintenance and CDC metrics are missing.

- [ ] **Step 3: Implement bounded maintenance and metrics**

Add validated config for minimum retention hours, maximum retained bytes, segment size, maintenance interval, and RPC batch limits. Run maintenance with cancellable waits, never delete the active segment, and export high watermark, earliest offset, committed version, segment bytes/count, append/fetch latency, stale epoch, and checksum failures.

- [ ] **Step 4: Run maintenance and metrics tests**

Run: `cmake --build build --target test_change_log_maintenance test_storage_metrics_collector -j4 && ctest --test-dir build --output-on-failure -R 'change_log_maintenance|storage_metrics_collector'`

Expected: all selected tests pass and invalid zero/overflow config is rejected.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/cdc/change_log_maintenance.h src/cdc/change_log_maintenance.cc tools/storaged.cc config/cedar.yaml include/cedar/dtx/storage/metrics_collector.h src/dtx/storage/metrics_collector.cc tests/storage/test_change_log_maintenance.cc tests/CMakeLists.txt
git commit -m "feat(cdc): enforce retention and expose metrics"
```
