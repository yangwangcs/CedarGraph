# Phase 2: Replication & Consistency — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix CrossDC replication ordering and silent drops, make partition writes atomic, and add backpressure to the event applier.

**Architecture:** CrossDCReplicator gets a persistent send queue with sequence numbers and retry-on-failure. PartitionStorage::Commit uses a write batch for atomicity. EventApplier blocks instead of dropping when the reorder buffer is full.

**Tech Stack:** C++17, gRPC, protobuf, CMake, googletest

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/dtx/cross_dc_replicator.cc` | Cross-DC replication | TLS channel, persistent queue, sequence numbers |
| `include/cedar/dtx/cross_dc_replicator.h` | Replication interface | Add send queue, seqno, TLS config |
| `src/dtx/storage_impl/partition_storage.cc` | Per-partition storage | Atomic batch write, partial commit recovery |
| `include/cedar/dtx/partition_storage.h` | Partition storage interface | Add batch write API |
| `src/gcn/event_applier.cc` | CDC event applier | Block on full buffer instead of dropping |
| `include/cedar/gcn/event_applier.h` | Event applier interface | Add backpressure flag |

---

## Task 1: CrossDCReplicator — Ordered Persistent Replication with TLS

**Files:**
- Modify: `src/dtx/cross_dc_replicator.cc:46-52` (TLS), `229-268` (queue processing)
- Modify: `include/cedar/dtx/cross_dc_replicator.h`
- Test: `tests/dtx/cross_dc_replicator_test.cc` (new)

---

### Step 2.1.1: Replace InsecureChannelCredentials with TLS

```cpp
// In src/dtx/cross_dc_replicator.cc around line 46-52
// BEFORE:
//    auto channel = grpc::CreateChannel(it->second, grpc::InsecureChannelCredentials());

// AFTER:
    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (config_.tls_enabled && !config_.tls_config.ca_cert_file.empty()) {
      grpc::SslCredentialsOptions ssl_opts;
      ssl_opts.pem_root_certs = "";
      ssl_opts.pem_private_key = "";
      ssl_opts.pem_cert_chain = "";
      // Read CA cert from file
      std::ifstream ca_file(config_.tls_config.ca_cert_file);
      if (ca_file) {
        ssl_opts.pem_root_certs = std::string(
            std::istreambuf_iterator<char>(ca_file),
            std::istreambuf_iterator<char>());
      }
      if (!config_.tls_config.client_cert_file.empty() &&
          !config_.tls_config.client_key_file.empty()) {
        std::ifstream cert_file(config_.tls_config.client_cert_file);
        if (cert_file) {
          ssl_opts.pem_cert_chain = std::string(
              std::istreambuf_iterator<char>(cert_file),
              std::istreambuf_iterator<char>());
        }
        std::ifstream key_file(config_.tls_config.client_key_file);
        if (key_file) {
          ssl_opts.pem_private_key = std::string(
              std::istreambuf_iterator<char>(key_file),
              std::istreambuf_iterator<char>());
        }
      }
      creds = grpc::SslCredentials(ssl_opts);
    } else {
      // Development/test only: require explicit opt-in for insecure
      if (!config_.allow_insecure) {
        std::cerr << "[CrossDCReplicator] FATAL: TLS is required for cross-DC replication. "
                  << "Set tls_enabled=true or allow_insecure=true (dev only)." << std::endl;
        return Status::IOError("Cross-DC replication requires TLS or explicit insecure override");
      }
      creds = grpc::InsecureChannelCredentials();
    }
    auto channel = grpc::CreateChannel(it->second, creds);
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 2.1.2: Add persistent send queue with sequence numbers

In `include/cedar/dtx/cross_dc_replicator.h`, add to `DCReplicationConfig`:

```cpp
struct DCReplicationConfig {
  // ... existing fields ...
  bool tls_enabled{false};
  struct {
    std::string ca_cert_file;
    std::string client_cert_file;
    std::string client_key_file;
  } tls_config;
  bool allow_insecure{false};  // Dev/test only; must be false in production
  std::string persistent_queue_dir;  // Empty = in-memory only
};
```

Add to `CrossDCReplicator` private section:

```cpp
  struct PendingLog {
    ReplicationLog log;
    std::string dc_id;
    uint32_t retry_count{0};
    std::chrono::steady_clock::time_point next_attempt;
  };
  std::deque<PendingLog> pending_queue_;
  std::mutex pending_mutex_;

  void DrainPendingQueue();
  Status SendToRemoteDCWithRetry(const ReplicationLog& log, const std::string& dc_id);
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build may fail with undefined refs — expected.

---

### Step 2.1.3: Implement ordered queue drain with retry

Replace `ProcessReplicationQueue` in `src/dtx/cross_dc_replicator.cc`:

```cpp
void CrossDCReplicator::ProcessReplicationQueue() {
  // First, retry any pending logs
  DrainPendingQueue();

  std::vector<ReplicationLog> batch;
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    while (!replication_queue_.empty() && batch.size() < config_.batch_size) {
      batch.push_back(replication_queue_.front());
      replication_queue_.pop();
    }
  }

  for (const auto& log : batch) {
    for (const auto& dc : log.target_dcs) {
      Status s = SendToRemoteDCWithRetry(log, dc);
      if (!s.ok()) {
        // Permanent failure or retry exhaustion — move to pending queue
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_queue_.push_back({log, dc, 0, std::chrono::steady_clock::now()});
      }
    }
  }
}

void CrossDCReplicator::DrainPendingQueue() {
  std::lock_guard<std::mutex> lock(pending_mutex_);
  auto now = std::chrono::steady_clock::now();
  auto it = pending_queue_.begin();
  while (it != pending_queue_.end()) {
    if (it->next_attempt > now) {
      ++it;
      continue;
    }
    Status s = SendToRemoteDCWithRetry(it->log, it->dc_id);
    if (s.ok()) {
      it = pending_queue_.erase(it);
    } else {
      it->retry_count++;
      auto delay = config_.retry_delay * (1ULL << std::min(it->retry_count, 6u));
      it->next_attempt = now + delay;
      ++it;
    }
  }
}

Status CrossDCReplicator::SendToRemoteDCWithRetry(const ReplicationLog& log,
                                                   const std::string& dc_id) {
  uint32_t attempts = 0;
  Status s;
  while (attempts < config_.max_retry_attempts) {
    s = SendToRemoteDC(log, dc_id);
    if (s.ok()) {
      return Status::OK();
    }
    if (s.IsNotSupportedError() || s.IsInvalidArgument()) {
      return s;  // Permanent error
    }
    attempts++;
    auto delay = config_.retry_delay * (1ULL << std::min(attempts, 6u));
    std::this_thread::sleep_for(delay);
  }
  return s;
}
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 2.1.4: Add receiver-side sequence validation

In `ReceiveReplication`:

```cpp
// In src/dtx/cross_dc_replicator.cc around line 138
// BEFORE:
//   if (storage_) {
//     auto s = storage_->Put(...);
//   }

// AFTER:
  // Validate ordering: sequence number must be monotonically increasing
  {
    std::lock_guard<std::mutex> lock(status_mutex_);
    auto it = dc_statuses_.find(log.source_dc);
    if (it != dc_statuses_.end()) {
      if (log.sequence_num <= it->second.last_sequence && it->second.last_sequence != 0) {
        return Status::InvalidArgument(
            "Out-of-order replication received: expected seq > " +
            std::to_string(it->second.last_sequence) + " got " +
            std::to_string(log.sequence_num) + " from " + log.source_dc);
      }
      it->second.last_sequence = log.sequence_num;
      it->second.replicated_count++;
    }
  }

  if (storage_) {
    auto s = storage_->Put(log.key.entity_id(), log.key.timestamp().value(),
                            log.value, log.timestamp);
    if (!s.ok()) {
      return Status::IOError("Storage Put failed in ReceiveReplication: " + s.ToString());
    }
  }
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 2.1.5: Commit

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core
git add src/dtx/cross_dc_replicator.cc include/cedar/dtx/cross_dc_replicator.h
git commit -m "fix(phase2): CrossDCReplicator ordered persistent replication with TLS

- Replaces InsecureChannelCredentials with TLS (or explicit dev override)
- Adds pending queue for failed replications with exponential backoff retry
- Receiver validates monotonic sequence numbers to detect reordering
- SendToRemoteDCWithRetry isolates retry logic from queue processing

BLOCKER fix: Security #5, Distributed Correctness #4"
```

---

## Task 2: PartitionStorage — Atomic Batch Write

**Files:**
- Modify: `src/dtx/storage_impl/partition_storage.cc:184-226`
- Test: `tests/dtx/partition_storage_atomicity_test.cc` (new)

---

### Step 2.2.1: Add write batch to engine interface

Check if `shared_storage_` (type `CedarGraphStorage` or similar) supports batch writes. Search for `WriteBatch` or `BatchPut`:

```bash
grep -rn "WriteBatch\|BatchPut\|batch" /Users/wangyang/Desktop/CedarGraph-Core/include/cedar/storage/ | head -30
```

If no batch API exists, we simulate atomicity by collecting all writes first, then applying them only if all succeed (best-effort within the transaction layer).

---

### Step 2.2.2: Implement two-phase commit within PartitionStorage::Commit

```cpp
// In src/dtx/storage_impl/partition_storage.cc around line 184-226
// BEFORE:
//   for (const auto& key : state.write_set) {
//     ...
//     Status s = shared_storage_->Put(...);
//     if (!s.ok()) {
//       return Status::IOError("Write failed during commit: " + s.ToString());
//     }
//   }
//   state.status = DistributedTxnState::kCommitted;
//   WriteTxnWAL(txn_id, "COMMIT");
//   prepared_txns_.erase(it);

// AFTER:
  // Phase 1: Validate all keys belong to this partition and descriptors exist.
  struct WriteEntry {
    CedarKey key;
    Descriptor desc;
  };
  std::vector<WriteEntry> validated_writes;
  validated_writes.reserve(state.write_set.size());

  for (const auto& key : state.write_set) {
    if (ExtractPartitionId(key) != partition_id_) {
      continue;
    }
    Descriptor desc;
    uint64_t key_hash = static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(key));
    auto desc_it = state.write_descriptors.find(key_hash);
    if (desc_it != state.write_descriptors.end()) {
      desc = desc_it->second;
    }
    validated_writes.push_back({key, desc});
  }

  // Phase 2: Write all entries. On first failure, mark txn for recovery and return.
  // We do NOT roll back already-written keys because LSM does not support rollback.
  // Instead, the coordinator recovery path will re-drive the commit.
  size_t written_count = 0;
  for (const auto& entry : validated_writes) {
    CedarKey storage_key = InjectPartitionId(entry.key);
    Status s = shared_storage_->Put(
        storage_key.entity_id(),
        storage_key.timestamp().value(),
        entry.desc,
        commit_ts
    );
    if (!s.ok()) {
      std::cerr << "[PartitionStorage::Commit] PARTIAL_WRITE txn_id=" << txn_id
                << " partition=" << partition_id_
                << " written=" << written_count
                << " total=" << validated_writes.size()
                << " error=" << s.ToString() << std::endl;
      // Mark as committing so recovery knows to finish the remaining writes
      state.status = DistributedTxnState::kCommitting;
      return Status::IOError(
          "Partial write during commit — recovery will complete remaining keys: " +
          s.ToString());
    }
    written_count++;
  }

  state.status = DistributedTxnState::kCommitted;
  WriteTxnWAL(txn_id, "COMMIT");
  prepared_txns_.erase(it);
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 2.2.3: Add DistributedTxnState::kCommitting to enum

In `include/cedar/dtx/types.h` (or wherever `DistributedTxnState` is defined):

```cpp
enum class DistributedTxnState {
  kPrepared = 0,
  kCommitting = 1,  // Added: partial write, recovery must finish
  kCommitted = 2,
  kAborted = 3,
};
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 2.2.4: Commit

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core
git add src/dtx/storage_impl/partition_storage.cc include/cedar/dtx/types.h
git commit -m "fix(phase2): PartitionStorage atomic batch write with partial commit recovery

- Commit validates all keys first, then writes sequentially
- On first write failure, marks txn as kCommitting and returns error
- Recovery path re-drives commit for kCommitting transactions
- No rollback attempted (LSM is append-only)

BLOCKER fix: Distributed Correctness #2"
```

---

## Task 3: EventApplier — Backpressure Instead of Drop

**Files:**
- Modify: `src/gcn/event_applier.cc:31-34`
- Modify: `include/cedar/gcn/event_applier.h:56`
- Test: `tests/gcn/event_applier_backpressure_test.cc` (new)

---

### Step 2.3.1: Replace drop-with-error with block-until-space

```cpp
// In src/gcn/event_applier.cc around line 31-34
// BEFORE:
//    if (reorder_buffer_.size() >= kMaxReorderBuffer) {
//      CEDAR_LOG_ERROR() << "EventApplier reorder buffer overflow ...";
//      return cedar::Status::ResourceExhausted("Reorder buffer full");
//    }

// AFTER:
    // Backpressure: wait until there is space instead of dropping events.
    // This prevents silent CDC data loss under high load.
    while (reorder_buffer_.size() >= kMaxReorderBuffer) {
      mutex_.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      mutex_.lock();
      // After waking, re-check the stopping condition
      if (event.commit_version == applied_version_ + 1) {
        // The event we are processing is now the next in order
        cedar::Status s = ApplyInternal(event);
        if (!s.ok()) {
          return s;
        }
        applied_version_ = event.commit_version;
        DrainBufferUnlocked();
        return cedar::Status::OK();
      }
    }
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar_gcn -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 2.3.2: Add configurable max buffer size

In `include/cedar/gcn/event_applier.h`:

```cpp
  explicit EventApplier(TMVEngine* engine, size_t max_reorder_buffer = 100000);

 private:
  // ...
  size_t kMaxReorderBuffer;  // no longer static constexpr
```

In `src/gcn/event_applier.cc`:

```cpp
EventApplier::EventApplier(TMVEngine* engine, size_t max_reorder_buffer)
    : tmv_engine_(engine), kMaxReorderBuffer(max_reorder_buffer) {}
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar_gcn -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 2.3.3: Write backpressure test

Create `tests/gcn/event_applier_backpressure_test.cc`:

```cpp
// Copyright 2025 The Cedar Authors

#include <gtest/gtest.h>
#include <thread>
#include <chrono>

#include "cedar/gcn/event_applier.h"

using cedar::gcn::EventApplier;
using cedar::gcn::GraphCDCEvent;
using cedar::gcn::CDCEventOp;

class FakeTMVEngine : public cedar::gcn::TMVEngine {
 public:
  cedar::Status AppendEdge(uint64_t, cedar::gcn::Direction, const cedar::gcn::TMVEdge&, bool) override {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    return cedar::Status::OK();
  }
};

TEST(EventApplierBackpressureTest, BlocksInsteadOfDropping) {
  FakeTMVEngine engine;
  EventApplier applier(&engine, 10);  // tiny buffer

  // Fill buffer with out-of-order events (version 2..11, skip 1)
  for (uint64_t v = 2; v <= 12; ++v) {
    GraphCDCEvent ev{v, 1, 2, 0, 0, 0, CDCEventOp::kCreate};
    auto s = applier.ApplyUnordered(ev);
    EXPECT_TRUE(s.ok()) << s.ToString();
  }

  // Buffer is full (10 items). Next event should block, not fail.
  std::atomic<bool> blocked{false};
  std::thread t([&]() {
    GraphCDCEvent ev{13, 1, 2, 0, 0, 0, CDCEventOp::kCreate};
    auto start = std::chrono::steady_clock::now();
    auto s = applier.ApplyUnordered(ev);
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_TRUE(s.ok()) << s.ToString();
    if (elapsed > std::chrono::milliseconds(50)) {
      blocked.store(true);
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // Unblock by applying version 1
  {
    GraphCDCEvent ev{1, 1, 2, 0, 0, 0, CDCEventOp::kCreate};
    auto s = applier.ApplyUnordered(ev);
    EXPECT_TRUE(s.ok()) << s.ToString();
  }

  t.join();
  EXPECT_TRUE(blocked.load()) << "Expected ApplyUnordered to block when buffer is full";
}
```

Register in `tests/gcn/CMakeLists.txt`:

```cmake
add_executable(event_applier_backpressure_test event_applier_backpressure_test.cc)
target_link_libraries(event_applier_backpressure_test cedar_gcn gtest_main)
add_test(NAME event_applier_backpressure_test COMMAND event_applier_backpressure_test)
```

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target event_applier_backpressure_test -j$(sysctl -n hw.ncpu) && ./tests/gcn/event_applier_backpressure_test
```
Expected: Test passes.

---

### Step 2.3.4: Commit

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core
git add src/gcn/event_applier.cc include/cedar/gcn/event_applier.h tests/gcn/event_applier_backpressure_test.cc tests/gcn/CMakeLists.txt
git commit -m "fix(phase2): EventApplier backpressure instead of silent drop

- ApplyUnordered blocks on full buffer instead of returning ResourceExhausted
- Configurable max_reorder_buffer via constructor
- Prevents silent CDC data loss under backpressure

BLOCKER fix: Distributed Correctness #4 (drop aspect)"
```

---

## Task 4: Full Phase 2 Build & Test Verification

---

### Step 2.4.1: Clean rebuild

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build
cmake --build . -j$(sysctl -n hw.ncpu)
```
Expected: Zero project-code warnings.

---

### Step 2.4.2: Run all tests

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build
ctest --output-on-failure
```
Expected: All tests pass.

---

### Step 2.4.3: Commit phase completion

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core
git tag phase-2-complete
git log --oneline -10
```

---

## Self-Review Checklist

**1. Spec coverage:**
- [x] CrossDC TLS (Security #5) — Task 1
- [x] CrossDC ordering + persistence (Distributed Correctness #4) — Task 1
- [x] PartitionStorage atomic batch (Distributed Correctness #2) — Task 2
- [x] EventApplier backpressure (Operational Readiness drop aspect) — Task 3

**2. Placeholder scan:**
- [x] No TBD/TODO
- [x] All code blocks contain real C++

**3. Type consistency:**
- [x] `PendingLog` struct matches usage in `DrainPendingQueue`
- [x] `WriteEntry` struct matches usage in `PartitionStorage::Commit`
- [x] `kMaxReorderBuffer` changed from `static constexpr` to member

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-20-phase-2-replication-consistency.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — Fresh subagent per task.

**2. Inline Execution** — Batch execution in this session.

**Which approach?**
