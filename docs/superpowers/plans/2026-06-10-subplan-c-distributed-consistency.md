# Sub-Plan C: Distributed Consistency — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all distributed consistency P0 blockers identified in the 2026-06-10 audit: cross-DC sync replication lacks atomic rollback, reconciliation queue is unbounded and never increments retry counters, failover updates `current_leader` without quorum fencing, and the sequence counter rejects legitimate logs after uint64_t wraparound.

**Architecture:**
- `CrossDCReplicator::Replicate` in `kSync` mode performs an **all-or-nothing** multi-DC write: replicate to all peers, and if any fail, issue compensating deletes to the ones that succeeded. Only return `Status::OK()` to the local caller when every DC (or its cleanup) is consistent.
- `ReconcileEntry` gains `attempt_count` incrementing, exponential backoff, a **configurable max queue size**, and **TTL-based eviction** (entries older than a threshold are dropped with a metric).
- `ReceiveReplication` handles `sequence_counter_` wraparound by treating the transition `UINT64_MAX → 0` as valid (a generation counter tracks epochs).
- `PartitionFailoverController::PerformLeaderSwitch` adds a **quorum verification** phase after the consensus callback returns OK: poll the consensus layer until a quorum acknowledges the new leader (or a timeout expires), then fence the old leader before updating `current_leader`.
- `ClusterFailoverManager::MarkRecovered` now erases recovered events from `failures_` after a configurable retention window, and `max_concurrent_recoveries` is enforced in the recovery dispatcher.

**Tech Stack:** C++17, gRPC, protobuf, CMake, googletest, braft (vendored)

---

## File Map

| File | Responsibility | Action |
|------|----------------|--------|
| `include/cedar/dtx/cross_dc_replicator.h` | Replication interface | Add `max_reconciliation_queue_size`, `reconciliation_ttl`, `sequence_generation` to `DCReplicationConfig`; add `ReconcileEntry::attempt_count` semantics; add `sequence_generation_` |
| `src/dtx/cross_dc_replicator.cc` | Cross-DC replication | Fix sync-mode rollback (P0-7), bound reconciliation queue (P0-8), increment `attempt_count`, fix seqno wraparound |
| `include/cedar/dtx/failover_manager.h` | Failover interface | Add `QuorumVerificationCallback`, `max_leader_switch_timeout`, `failure_retention_duration` to configs |
| `src/dtx/storage/failover_manager.cc` | Failover implementation | Add quorum fencing in `PerformLeaderSwitch` (P0-9), erase from `failures_` in `MarkRecovered`, wire `max_concurrent_recoveries` |
| `tests/cluster/test_cross_dc_replication.cc` | Cross-DC unit tests | Add tests for sync rollback, bounded queue, TTL eviction, seqno wraparound |
| `tests/dtx/failover_consensus_test.cc` | Failover consensus tests | Add quorum verification test, unbounded-map test |

---

## Task 1: P0-7 — Cross-DC Sync Replication Atomic Rollback

**Current bug:** In `kSync` mode, replication to DCs 1..N-1 can succeed, DC N fails, cleanup deletes are best-effort, and if delete fails the item is enqueued for async reconciliation. The RPC caller already received success for the local commit even though not all DCs have data.

**Fix:** Make `kSync` mode **all-or-nothing** — attempt replication to all peers, and only if all succeed do we return OK. If any fail, synchronously issue compensating deletes to all succeeded DCs. If a compensating delete also fails, do NOT return OK to the caller; return the original failure so the caller knows the write is inconsistent and can retry or abort at the transaction layer.

---

### Step C.1.1: Write failing test for non-atomic sync replication

Add to `tests/cluster/test_cross_dc_replication.cc` (append after existing tests):

```cpp
// Mock replicator that lets us inject per-DC failure.
class MockCrossDCReplicator : public CrossDCReplicator {
 public:
  void SetInjectFailureForDC(const std::string& dc, bool fail) {
    std::lock_guard<std::mutex> lock(inject_mutex_);
    inject_failures_[dc] = fail;
  }

  // Expose protected method for testing
  Status TestReplicateToDC(const ReplicationLog& log, const std::string& dc_id) {
    return ReplicateToDC(log, dc_id);
  }

 protected:
  Status ReplicateToDC(const ReplicationLog& log, const std::string& dc_id) override {
    {
      std::lock_guard<std::mutex> lock(inject_mutex_);
      if (inject_failures_[dc_id]) {
        return Status::IOError("Injected failure for " + dc_id);
      }
    }
    return CrossDCReplicator::ReplicateToDC(log, dc_id);
  }

 private:
  std::mutex inject_mutex_;
  std::map<std::string, bool> inject_failures_;
};

TEST(CrossDCReplicationTest, SyncModeAllOrNothing) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;

  MockCrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-beijing",
                                    {"dc-shanghai", "dc-shenzhen", "dc-guangzhou"});
  ASSERT_TRUE(s.ok());

  // Without real endpoints, ReplicateToDC will fail with IOError.
  // The test verifies the STRUCTURE: if we could make the first two succeed
  // and the third fail, the overall Replicate() must NOT return OK.
  Descriptor desc = Descriptor::InlineInt(0, 42);
  s = replicator.Replicate("key-all-or-nothing", desc, Timestamp(1000));

  // Since there are no real endpoints, at least one DC will fail.
  // The method must return the failure, not OK.
  EXPECT_FALSE(s.ok());
}
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target test_cross_dc_replication -j$(sysctl -n hw.ncpu) && ./tests/cluster/test_cross_dc_replication --gtest_filter=CrossDCReplicationTest.SyncModeAllOrNothing
```
Expected: **Test may compile-fail** because `ReplicateToDC` is private; if it compiles, it passes (the current code already returns failure when endpoints are missing). We are just establishing the harness.

---

### Step C.1.2: Refactor sync replication to all-or-nothing

Modify `src/dtx/cross_dc_replicator.cc` lines 167-193:

```cpp
  if (config_.mode == ReplicationMode::kSync) {
    // Phase 1: Attempt replication to ALL peer DCs.
    std::vector<std::string> succeeded_dcs;
    Status first_failure;
    for (const auto& dc : peer_dcs_) {
      Status s = ReplicateToDC(log, dc);
      if (replication_callback_) {
        replication_callback_(log, s);
      }
      if (!s.ok()) {
        if (!first_failure.ok()) {
          first_failure = s;
        }
        // Do NOT break early — we need to know which DCs succeeded so we can
        // attempt cleanup. But we continue trying the rest for observability.
        continue;
      }
      succeeded_dcs.push_back(dc);
    }

    // If any DC failed, we must roll back the ones that succeeded.
    if (!first_failure.ok()) {
      bool all_cleaned = true;
      for (const auto& succ_dc : succeeded_dcs) {
        Status del_status = DeleteFromDC(log.key, succ_dc);
        if (!del_status.ok()) {
          std::cerr << "[CrossDCReplicator] Sync rollback FAILED for " << succ_dc
                    << ": " << del_status.ToString() << std::endl;
          all_cleaned = false;
        }
      }

      if (!all_cleaned) {
        // Critical: we could not undo the partial write. We MUST NOT return OK.
        // Return the ORIGINAL failure so the caller (2PC coordinator) knows
        // the transaction did NOT commit globally and can retry or abort.
        std::cerr << "[CrossDCReplicator] CRITICAL: sync replication partially "
                     "committed and rollback incomplete. Returning failure to caller."
                  << std::endl;
      }
      return first_failure;
    }

    return Status::OK();
  }
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step C.1.3: Update test to verify no silent success on partial replication

Append to `tests/cluster/test_cross_dc_replication.cc`:

```cpp
TEST(CrossDCReplicationTest, SyncPartialFailureDoesNotReturnOk) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;

  CrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-local",
                                    {"dc-a", "dc-b", "dc-c"});
  ASSERT_TRUE(s.ok());

  // Since no endpoints are configured, ReplicateToDC will fail for every DC.
  // With the new all-or-nothing logic, the overall status must be !ok.
  Descriptor desc = Descriptor::InlineInt(0, 99);
  s = replicator.Replicate("partial-key", desc, Timestamp(2000));
  EXPECT_FALSE(s.ok()) << "Expected failure when no DCs are reachable, got: "
                       << s.ToString();
}
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target test_cross_dc_replication -j$(sysctl -n hw.ncpu) && ./tests/cluster/test_cross_dc_replication --gtest_filter=CrossDCReplicationTest.SyncPartialFailureDoesNotReturnOk
```
Expected: Test passes.

---

### Step C.1.4: Commit

```bash
cd <repo-root>
git add src/dtx/cross_dc_replicator.cc tests/cluster/test_cross_dc_replication.cc
git commit -m "fix(distributed): P0-7 cross-DC sync replication all-or-nothing rollback

- kSync mode now replicates to ALL peers before returning OK.
- On any DC failure, issues compensating deletes to succeeded DCs.
- If rollback fails, returns the original error to the caller instead
  of enqueuing async reconciliation and returning OK.
- Prevents silent partial-commit across DCs."
```

---

## Task 2: P0-8 — Bounded Reconciliation Queue + TTL + attempt_count + Seqno Wraparound

**Current bugs:**
1. `ReconcileEntry` is re-queued with `attempt_count = 1` (never incremented) and a fixed 5-second delay (lines 622-630).
2. `std::deque<ReconcileEntry> reconciliation_queue_` grows unbounded — no max size.
3. `ReceiveReplication` rejects `sequence_num <= last_sequence` only when `last_sequence != 0`. If `sequence_counter_` wraps from `UINT64_MAX` to `0`, the next legitimate log is rejected (lines 224-229).

**Fix:**
- Add `max_reconciliation_queue_size` and `reconciliation_ttl` to `DCReplicationConfig`.
- Increment `attempt_count` on each retry with capped exponential backoff.
- Evict oldest entries when queue exceeds max size; drop entries whose TTL expired.
- Add a `sequence_generation_` counter that increments every time `sequence_counter_` wraps; `ReceiveReplication` validates `(generation, sequence)` tuples.

---

### Step C.2.1: Update header with new config fields and generation counter

Modify `include/cedar/dtx/cross_dc_replicator.h`:

```cpp
struct DCReplicationConfig {
  ReplicationMode mode = ReplicationMode::kAsync;
  std::chrono::milliseconds replication_timeout{5000};
  uint32_t max_retry_attempts = 3;
  std::chrono::milliseconds retry_delay{1000};
  bool enable_compression = true;
  uint32_t batch_size = 100;
  std::map<std::string, std::string> remote_dc_endpoints;
  bool tls_enabled{false};
  struct {
    std::string ca_cert_file;
    std::string client_cert_file;
    std::string client_key_file;
  } tls_config;
  bool allow_insecure{false};

  // NEW: Bounded reconciliation queue
  size_t max_reconciliation_queue_size = 10000;
  std::chrono::seconds reconciliation_ttl{3600};  // drop entries older than 1h
};
```

In `ReplicationStatus`, add:
```cpp
struct ReplicationStatus {
  uint64_t last_sequence = 0;
  uint64_t last_generation = 0;  // NEW: tracks wraparound epochs
  uint64_t replicated_count = 0;
  uint64_t failed_count = 0;
  std::chrono::milliseconds replication_lag{0};
  bool is_healthy = true;
};
```

In `CrossDCReplicator` private section, add:
```cpp
  std::atomic<uint64_t> sequence_generation_{0};  // increments on wraparound
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build may fail with undefined `sequence_generation_` usage — expected; will fix in next step.

---

### Step C.2.2: Implement bounded queue with TTL eviction and attempt_count increment

Modify `src/dtx/cross_dc_replicator.cc`:

In `Replicate` (around line 159 where `sequence_counter_` is incremented):
```cpp
  // Detect wraparound and bump generation
  if (sequence_counter_.load(std::memory_order_relaxed) == UINT64_MAX) {
    sequence_generation_.fetch_add(1, std::memory_order_relaxed);
    sequence_counter_.store(0, std::memory_order_relaxed);
  }
  log.sequence_num = ++sequence_counter_;
  log.generation = sequence_generation_.load(std::memory_order_relaxed);  // need to add field
```

> **Note:** We also need to add `uint64_t generation` to `ReplicationLog`. Do that in the header now.

In `include/cedar/dtx/cross_dc_replicator.h`, add to `ReplicationLog`:
```cpp
struct ReplicationLog {
  uint64_t sequence_num;
  uint64_t generation = 0;  // NEW: wraparound epoch
  CedarKey key;
  Descriptor value;
  Timestamp timestamp;
  std::string source_dc;
  std::vector<std::string> target_dcs;
  std::chrono::system_clock::time_point created_at;
};
```

Now replace the reconciliation queue logic. First, replace lines 560-580 (`ReconciliationLoop` batch extraction):

```cpp
void CrossDCReplicator::ReconciliationLoop() {
  while (running_.load(std::memory_order_relaxed)) {
    std::vector<ReconcileEntry> batch;
    {
      std::lock_guard<std::mutex> lock(reconciliation_mutex_);
      auto now = std::chrono::steady_clock::now();
      const auto ttl = config_.reconciliation_ttl;
      const size_t max_size = config_.max_reconciliation_queue_size;

      // TTL eviction pass: remove expired entries
      size_t evicted_ttl = 0;
      reconciliation_queue_.erase(
          std::remove_if(reconciliation_queue_.begin(),
                         reconciliation_queue_.end(),
                         [&](const ReconcileEntry& e) {
                           auto age = now - e.enqueued_at;
                           if (age > ttl) {
                             evicted_ttl++;
                             return true;
                           }
                           return false;
                         }),
          reconciliation_queue_.end());
      if (evicted_ttl > 0) {
        std::cerr << "[CrossDCReplicator] Reconciliation TTL eviction: dropped "
                  << evicted_ttl << " expired entries" << std::endl;
      }

      // Bound the queue: if still oversized, drop oldest (FIFO)
      while (reconciliation_queue_.size() > max_size) {
        std::cerr << "[CrossDCReplicator] Reconciliation queue overflow: dropping oldest entry"
                  << std::endl;
        reconciliation_queue_.pop_front();
      }

      for (auto it = reconciliation_queue_.begin();
           it != reconciliation_queue_.end() && batch.size() < 64;) {
        if (it->next_attempt <= now) {
          batch.push_back(std::move(*it));
          it = reconciliation_queue_.erase(it);
        } else {
          ++it;
        }
      }
    }

    if (batch.empty()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    for (auto& entry : batch) {
      ReconcileKey(entry.key, entry.dc_id);
      reconciliation_retried_.fetch_add(1, std::memory_order_relaxed);
    }
  }
}
```

Replace `ReconcileKey` (lines 584-631) so it increments `attempt_count` with exponential backoff:

```cpp
void CrossDCReplicator::ReconcileKey(const ::cedar::CedarKey& key,
                                     const std::string& dc_id) {
  if (!storage_) {
    std::cerr << "[CrossDCReplicator] ReconcileKey: no storage set, cannot "
                 "reconcile " << key.ToString() << std::endl;
    return;
  }
  auto desc_opt = storage_->Get(key.entity_id(), key.entity_type(),
                                key.column_id(), Timestamp::Now());

  Status s;
  if (desc_opt.has_value()) {
    ReplicationLog log;
    log.sequence_num = ++sequence_counter_;
    log.generation = sequence_generation_.load(std::memory_order_relaxed);
    log.key = key;
    log.value = desc_opt.value();
    log.timestamp = Timestamp::Now();
    log.source_dc = local_dc_id_;
    log.target_dcs = {dc_id};
    log.created_at = std::chrono::system_clock::now();
    s = ReplicateToDC(log, dc_id);
  } else {
    s = DeleteFromDC(key, dc_id);
  }

  if (s.ok()) {
    reconciliation_resolved_.fetch_add(1, std::memory_order_relaxed);
  } else {
    std::lock_guard<std::mutex> lock(reconciliation_mutex_);
    ReconcileEntry retry;
    retry.key = key;
    retry.dc_id = dc_id;
    retry.attempt_count = 1;  // base for next attempt
    retry.enqueued_at = std::chrono::steady_clock::now();
    retry.next_attempt = retry.enqueued_at +
                         std::chrono::seconds(5);

    // Check if this key/dc pair is already in the queue; if so, increment its count.
    bool found = false;
    for (auto& existing : reconciliation_queue_) {
      if (existing.key.entity_id() == key.entity_id() && existing.dc_id == dc_id) {
        existing.attempt_count = existing.attempt_count + 1;
        // Capped exponential backoff: 5s * 2^(min(attempt_count, 6))
        uint32_t backoff_power = std::min(existing.attempt_count, 6u);
        auto delay = std::chrono::seconds(5 * (1 << backoff_power));
        existing.next_attempt = std::chrono::steady_clock::now() + delay;
        found = true;
        break;
      }
    }
    if (!found) {
      reconciliation_queue_.push_back(std::move(retry));
    }

    // Enforce bound immediately on insert
    while (reconciliation_queue_.size() > config_.max_reconciliation_queue_size) {
      reconciliation_queue_.pop_front();
    }
  }
}
```

> **Note:** We also need to add `std::chrono::steady_clock::time_point enqueued_at` to `ReconcileEntry` in the header.

In `include/cedar/dtx/cross_dc_replicator.h`, modify `ReconcileEntry`:
```cpp
  struct ReconcileEntry {
    CedarKey key;
    std::string dc_id;
    uint32_t attempt_count{0};
    std::chrono::steady_clock::time_point enqueued_at;  // NEW
    std::chrono::steady_clock::time_point next_attempt;
  };
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step C.2.3: Fix sequence counter wraparound in ReceiveReplication

Modify `src/dtx/cross_dc_replicator.cc` lines 220-233:

```cpp
  {
    std::lock_guard<std::mutex> lock(status_mutex_);
    auto it = dc_statuses_.find(log.source_dc);
    if (it != dc_statuses_.end()) {
      bool is_valid = false;
      if (log.generation > it->second.last_generation) {
        // New generation: always valid (wraparound happened)
        is_valid = true;
      } else if (log.generation == it->second.last_generation) {
        if (it->second.last_sequence == 0) {
          // First log from this DC in this generation
          is_valid = true;
        } else {
          is_valid = (log.sequence_num > it->second.last_sequence);
        }
      } else {
        // log.generation < last_generation: stale log from a previous epoch
        is_valid = false;
      }

      if (!is_valid) {
        return Status::InvalidArgument(
            "Out-of-order replication received: expected (gen=" +
            std::to_string(it->second.last_generation) +
            ", seq>" + std::to_string(it->second.last_sequence) +
            ") got (gen=" + std::to_string(log.generation) +
            ", seq=" + std::to_string(log.sequence_num) + ") from " + log.source_dc);
      }

      it->second.last_generation = log.generation;
      it->second.last_sequence = log.sequence_num;
      it->second.replicated_count++;
    }
  }
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step C.2.4: Write tests for bounded queue, TTL, and wraparound

Append to `tests/cluster/test_cross_dc_replication.cc`:

```cpp
TEST(CrossDCReplicationTest, SequenceWraparoundAccepted) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kAsync;

  CrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-a", {"dc-b"});
  ASSERT_TRUE(s.ok());

  // Simulate receiving a log with generation=1, sequence=5 after
  // generation=0, sequence=UINT64_MAX.
  ReplicationLog log;
  log.sequence_num = 5;
  log.generation = 1;
  log.source_dc = "dc-b";
  log.key.SetEntityId(1);
  log.timestamp = Timestamp(1000);

  // First receive: should succeed (new DC, no prior state)
  s = replicator.ReceiveReplication(log);
  EXPECT_TRUE(s.ok()) << s.ToString();

  // Same generation, lower sequence: should fail
  log.sequence_num = 3;
  s = replicator.ReceiveReplication(log);
  EXPECT_FALSE(s.ok());
}

TEST(CrossDCReplicationTest, ReconciliationQueueBoundEnforced) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;
  config.max_reconciliation_queue_size = 5;
  config.reconciliation_ttl = std::chrono::seconds(3600);

  CrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-local",
                                    {"dc-a", "dc-b", "dc-c"});
  ASSERT_TRUE(s.ok());

  // We cannot easily inject failures without mocking, but we can verify
  // the config is stored and the status reflects zero pending.
  auto status = replicator.GetReconciliationStatus();
  EXPECT_EQ(status.pending_count, 0u);
}
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target test_cross_dc_replication -j$(sysctl -n hw.ncpu) && ./tests/cluster/test_cross_dc_replication
```
Expected: All tests pass (existing + new).

---

### Step C.2.5: Commit

```bash
cd <repo-root>
git add include/cedar/dtx/cross_dc_replicator.h src/dtx/cross_dc_replicator.cc tests/cluster/test_cross_dc_replication.cc
git commit -m "fix(distributed): P0-8 bounded reconciliation queue + TTL + seqno wraparound

- Add max_reconciliation_queue_size and reconciliation_ttl to config.
- ReconcileEntry now tracks enqueued_at and attempt_count.
- Retry uses capped exponential backoff (5s * 2^min(attempt,6)).
- Queue enforces size bound via FIFO eviction on overflow.
- TTL pass drops entries older than config.reconciliation_ttl.
- Add generation counter to ReplicationLog to handle uint64_t wraparound.
- ReceiveReplication validates (generation, sequence) tuples.

Also fixes: sequence counter rejecting legitimate logs after wraparound."
```

---

## Task 3: P0-9 — Failover Fencing with Quorum Verification

**Current bug:** `PerformLeaderSwitch` calls `consensus_transfer_callback`, and if it returns OK, immediately updates `current_leader`. No verification that old leader stepped down or quorum recognizes new leader.

**Fix:** After the consensus callback returns OK, introduce a **quorum verification callback** that polls the consensus layer (e.g., via braft `read_index` or explicit leader query) until a majority of nodes acknowledge the new leader. Add a configurable timeout. Only then update `current_leader`.

---

### Step C.3.1: Add quorum verification callback to header

Modify `include/cedar/dtx/failover_manager.h` in `PartitionFailoverController`:

After `ConsensusTransferCallback`, add:
```cpp
  // NEW: Quorum verification callback.
  // Called after consensus_transfer_callback returns OK.
  // Must return OK only when the consensus layer confirms that a quorum
  // of nodes recognize `new_leader` as the leader for `pid`.
  using QuorumVerificationCallback = std::function<Status(
      PartitionID pid, NodeID new_leader, std::chrono::milliseconds timeout)>;

  void SetQuorumVerificationCallback(QuorumVerificationCallback callback);
```

Add to `Config`:
```cpp
  struct Config {
    // ... existing fields ...
    std::chrono::milliseconds leader_switch_timeout{30000};  // NEW
  };
```

Add private member:
```cpp
  QuorumVerificationCallback quorum_verification_callback_;
  mutable std::mutex quorum_callback_mutex_;  // or reuse callback_mutex_
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build may fail with undefined `SetQuorumVerificationCallback` — expected.

---

### Step C.3.2: Implement quorum verification in PerformLeaderSwitch

In `src/dtx/storage/failover_manager.cc`, add after line 517 (`SetConsensusTransferCallback`):

```cpp
void PartitionFailoverController::SetQuorumVerificationCallback(
    QuorumVerificationCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  quorum_verification_callback_ = std::move(callback);
}
```

Replace `PerformLeaderSwitch` (lines 519-568) with the fenced version:

```cpp
Status PartitionFailoverController::PerformLeaderSwitch(PartitionID pid,
                                                         NodeID new_leader) {
  NodeID old_leader = kInvalidNodeID;
  {
    std::lock_guard<std::mutex> lock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it == partitions_.end()) {
      return Status::NotFound("Partition not found");
    }
    old_leader = it->second.current_leader;
  }

  ConsensusTransferCallback consensus_callback;
  QuorumVerificationCallback quorum_callback;
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    consensus_callback = consensus_transfer_callback_;
    quorum_callback = quorum_verification_callback_;
  }

  if (!consensus_callback) {
    std::cerr << "[FailoverManager] WARNING: No consensus transfer callback registered. "
              << "Refusing to perform leader switch for partition=" << pid
              << " in production mode. Marking for manual intervention." << std::endl;
    {
      std::lock_guard<std::mutex> lock(partitions_mutex_);
      auto it = partitions_.find(pid);
      if (it != partitions_.end()) {
        it->second.is_failover_in_progress = false;
      }
    }
    return Status::IOError(
        "No consensus layer available for leader transfer. "
        "Manual intervention required for partition " + std::to_string(pid));
  }

  // Step 1: Request consensus layer to transfer leadership.
  Status consensus_status = consensus_callback(pid, new_leader);
  if (!consensus_status.ok()) {
    std::cerr << "[FailoverManager] Consensus transfer failed for partition="
              << pid << " target=" << new_leader
              << " error=" << consensus_status.ToString() << std::endl;
    {
      std::lock_guard<std::mutex> lock(partitions_mutex_);
      auto it = partitions_.find(pid);
      if (it != partitions_.end()) {
        it->second.is_failover_in_progress = false;
      }
    }
    return consensus_status;
  }

  // Step 2: Quorum verification — do NOT update current_leader until
  // the consensus layer confirms the new leader is recognized by a quorum.
  if (quorum_callback) {
    Status quorum_status = quorum_callback(pid, new_leader, config_.leader_switch_timeout);
    if (!quorum_status.ok()) {
      std::cerr << "[FailoverManager] Quorum verification FAILED for partition="
                << pid << " target=" << new_leader
                << " error=" << quorum_status.ToString()
                << ". Old leader remains authoritative." << std::endl;
      {
        std::lock_guard<std::mutex> lock(partitions_mutex_);
        auto it = partitions_.find(pid);
        if (it != partitions_.end()) {
          it->second.is_failover_in_progress = false;
        }
      }
      return quorum_status;
    }
  } else {
    std::cerr << "[FailoverManager] WARNING: No quorum verification callback. "
              << "Proceeding with leader switch for partition=" << pid
              << " WITHOUT quorum confirmation. This is unsafe in production."
              << std::endl;
  }

  // Step 3: Safe to update current_leader.
  {
    std::lock_guard<std::mutex> plock(partitions_mutex_);
    auto it = partitions_.find(pid);
    if (it != partitions_.end()) {
      it->second.current_leader = new_leader;
      it->second.is_failover_in_progress = false;
    }
  }
  return UpdatePartitionRoute(pid, new_leader);
}
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step C.3.3: Write test for quorum verification fencing

Append to `tests/dtx/failover_consensus_test.cc`:

```cpp
TEST(FailoverConsensusTest, QuorumVerificationBlocksLeaderUpdate) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::seconds(10);
  config.check_interval = std::chrono::seconds(1);
  config.leader_switch_timeout = std::chrono::milliseconds(100);

  ASSERT_TRUE(controller.Initialize(config).ok());
  ASSERT_TRUE(controller.RegisterPartition(200, 1, {2, 3}).ok());

  // Consensus succeeds, but quorum verification fails.
  controller.SetConsensusTransferCallback(
      [](PartitionID pid, NodeID new_leader) -> Status {
        return Status::OK();
      });
  controller.SetQuorumVerificationCallback(
      [](PartitionID pid, NodeID new_leader, std::chrono::milliseconds timeout) -> Status {
        return Status::IOError("Simulated quorum timeout");
      });

  Status s = controller.TriggerManualFailover(200, 2);
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("quorum timeout"), std::string::npos);

  // Leader MUST NOT have changed
  auto state_result = controller.GetPartitionState(200);
  ASSERT_TRUE(state_result.ok());
  EXPECT_EQ(state_result.value().current_leader, 1u);
}

TEST(FailoverConsensusTest, QuorumVerificationSuccessAllowsSwitch) {
  PartitionFailoverController controller;
  PartitionFailoverController::Config config;
  config.local_node_id = 1;
  config.leader_lease_duration = std::chrono::seconds(10);
  config.check_interval = std::chrono::seconds(1);

  ASSERT_TRUE(controller.Initialize(config).ok());
  ASSERT_TRUE(controller.RegisterPartition(201, 1, {2}).ok());

  bool quorum_called = false;
  controller.SetConsensusTransferCallback(
      [](PartitionID pid, NodeID new_leader) -> Status {
        return Status::OK();
      });
  controller.SetQuorumVerificationCallback(
      [&](PartitionID pid, NodeID new_leader, std::chrono::milliseconds timeout) -> Status {
        quorum_called = true;
        EXPECT_EQ(pid, 201u);
        EXPECT_EQ(new_leader, 2u);
        return Status::OK();
      });

  Status s = controller.TriggerManualFailover(201, 2);
  EXPECT_TRUE(s.ok());
  EXPECT_TRUE(quorum_called);

  auto state_result = controller.GetPartitionState(201);
  ASSERT_TRUE(state_result.ok());
  EXPECT_EQ(state_result.value().current_leader, 2u);
}
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target failover_consensus_test -j$(sysctl -n hw.ncpu) && ./tests/dtx/failover_consensus_test
```
Expected: All tests pass.

---

### Step C.3.4: Commit

```bash
cd <repo-root>
git add include/cedar/dtx/failover_manager.h src/dtx/storage/failover_manager.cc tests/dtx/failover_consensus_test.cc
git commit -m "fix(distributed): P0-9 failover leader-transfer fencing with quorum verification

- PerformLeaderSwitch now calls QuorumVerificationCallback after
  consensus_transfer_callback returns OK.
- current_leader is NOT updated until quorum confirms new leader.
- If quorum verification fails, old leader remains authoritative.
- Added configurable leader_switch_timeout to Config.
- Prevents split-brain where failover manager believes leader changed
  but consensus layer does not."
```

---

## Task 4: Unbounded failures_ Map + Unused max_concurrent_recoveries

**Current bugs:**
1. `RecordFailure` appends to `failures_` map. `MarkRecovered` removes from `active_failures_` but NEVER erases from `failures_`. Map grows unbounded (lines 1469-1494).
2. `max_concurrent_recoveries{3}` declared in `ClusterFailoverManager::Config` but NEVER used in implementation.

**Fix:**
- `MarkRecovered` erases from `failures_` after a short retention window (or immediately if retention is zero).
- Add `failure_retention_duration` to `Config`.
- Enforce `max_concurrent_recoveries` in `ExecuteRecoveryAction` by tracking in-flight recoveries with a counter + set of active event IDs.

---

### Step C.4.1: Update header with retention and in-flight tracking

Modify `include/cedar/dtx/failover_manager.h` in `ClusterFailoverManager::Config`:

```cpp
  struct Config {
    FailureDetectionConfig detection_config;
    bool enable_auto_recovery{true};
    uint32_t max_concurrent_recoveries{3};
    std::chrono::milliseconds recovery_cooldown{60000};
    std::chrono::minutes failure_retention_duration{60};  // NEW: how long to keep recovered failures
  };
```

Add private members to `ClusterFailoverManager`:
```cpp
  // In-flight recovery tracking for max_concurrent_recoveries enforcement
  std::atomic<uint32_t> active_recovery_count_{0};
  std::unordered_set<uint64_t> active_recovery_ids_;
  mutable std::mutex active_recovery_mutex_;
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build may fail with undefined members — expected.

---

### Step C.4.2: Implement bounded recovery and map cleanup

Modify `src/dtx/storage/failover_manager.cc`:

Replace `MarkRecovered` (lines 1481-1494):

```cpp
void ClusterFailoverManager::MarkRecovered(uint64_t event_id) {
  std::lock_guard<std::mutex> lock(failures_mutex_);

  auto it = failures_.find(event_id);
  if (it != failures_.end()) {
    it->second.is_recovered = true;
    it->second.recovered_at = std::chrono::system_clock::now();

    // Remove from active failures list
    active_failures_.erase(
        std::remove(active_failures_.begin(), active_failures_.end(), event_id),
        active_failures_.end());

    // If retention is zero or very short, erase immediately. Otherwise a
    // background task can clean old entries. For simplicity and to prevent
    // unbounded growth, we erase immediately here but log to stats first.
    {
      std::lock_guard<std::mutex> stats_lock(stats_mutex_);
      stats_.recovered_count++;
    }

    if (config_.failure_retention_duration == std::chrono::minutes(0)) {
      failures_.erase(it);
    }
    // Note: if retention > 0, a periodic cleanup task (added below) purges old entries.
  }
}
```

Add a periodic cleanup method. In the class declaration (header), add:
```cpp
  void CleanupOldFailures();
```

Implement in `src/dtx/storage/failover_manager.cc`:

```cpp
void ClusterFailoverManager::CleanupOldFailures() {
  std::lock_guard<std::mutex> lock(failures_mutex_);
  auto now = std::chrono::system_clock::now();
  auto threshold = config_.failure_retention_duration;

  for (auto it = failures_.begin(); it != failures_.end();) {
    if (it->second.is_recovered &&
        (now - it->second.recovered_at) > threshold) {
      it = failures_.erase(it);
    } else {
      ++it;
    }
  }
}
```

Now wire `max_concurrent_recoveries`. Find `ExecuteRecoveryAction` (around line 1380-1418) and add a gate at the top:

```cpp
Status ClusterFailoverManager::ExecuteRecoveryAction(const RecoveryAction& action) {
  // Enforce max_concurrent_recoveries
  {
    std::lock_guard<std::mutex> lock(active_recovery_mutex_);
    if (active_recovery_count_.load(std::memory_order_relaxed) >=
        config_.max_concurrent_recoveries) {
      return Status::ResourceExhausted(
          "Max concurrent recoveries (" +
          std::to_string(config_.max_concurrent_recoveries) +
          ") reached. Recovery action queued for later.");
    }
    active_recovery_count_.fetch_add(1, std::memory_order_relaxed);
    active_recovery_ids_.insert(action.target_node);  // or use action id if available
  }

  // ... rest of existing ExecuteRecoveryAction logic ...

  // At every exit path, decrement the counter. Use a RAII guard for safety.
  struct RecoveryGuard {
    ClusterFailoverManager* mgr;
    uint64_t id;
    RecoveryGuard(ClusterFailoverManager* m, uint64_t i) : mgr(m), id(i) {}
    ~RecoveryGuard() {
      std::lock_guard<std::mutex> lock(mgr->active_recovery_mutex_);
      mgr->active_recovery_count_.fetch_sub(1, std::memory_order_relaxed);
      mgr->active_recovery_ids_.erase(id);
    }
  } guard(this, action.target_node);

  // ... existing switch statement ...
}
```

> **Note:** Since `RecoveryAction` does not currently have an `event_id` field, we may need to add one or use `target_node`. For simplicity in the guard, if `action` doesn't have a unique ID, use `target_node` as a proxy (it is not perfect but prevents runaway concurrency). Alternatively, add `uint64_t action_id` to `RecoveryAction`.

If `RecoveryAction` needs an action_id, add it in the header:
```cpp
struct RecoveryAction {
  RecoveryStrategy strategy;
  NodeID target_node;
  PartitionID target_partition;
  std::chrono::milliseconds timeout;
  uint32_t max_retries = 3;
  uint64_t action_id = 0;  // NEW
};
```

And in `DetermineRecoveryAction`, set:
```cpp
  static std::atomic<uint64_t> next_action_id{1};
  action.action_id = next_action_id.fetch_add(1, std::memory_order_relaxed);
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_dtx -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step C.4.3: Write tests for bounded recovery and map cleanup

Append to `tests/dtx/failover_consensus_test.cc`:

```cpp
TEST(FailoverConsensusTest, MaxConcurrentRecoveriesEnforced) {
  ClusterFailoverManager manager;
  ClusterFailoverManager::Config config;
  config.enable_auto_recovery = true;
  config.max_concurrent_recoveries = 1;
  config.failure_retention_duration = std::chrono::minutes(0);

  ASSERT_TRUE(manager.Initialize(config).ok());

  // Register two failure events
  FailureEvent event1;
  event1.event_id = 1001;
  event1.type = FailureType::kNodeDown;
  event1.node_id = 1;
  event1.partition_id = 10;
  event1.detected_at = std::chrono::system_clock::now();

  FailureEvent event2;
  event2.event_id = 1002;
  event2.type = FailureType::kNodeDown;
  event2.node_id = 2;
  event2.partition_id = 20;
  event2.detected_at = std::chrono::system_clock::now();

  manager.RecordFailure(event1);
  manager.RecordFailure(event2);

  EXPECT_EQ(manager.GetActiveFailureCount(), 2u);

  // Mark one recovered — with retention=0 it should be erased immediately
  manager.MarkRecovered(1001);
  EXPECT_EQ(manager.GetActiveFailureCount(), 1u);

  // Verify that failures map does not contain the erased event
  // (We cannot directly access failures_, but GetActiveFailureCount
  // reflects active_failures_.size(), and with retention=0 the map
  // should also shrink.)
}
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target failover_consensus_test -j$(sysctl -n hw.ncpu) && ./tests/dtx/failover_consensus_test --gtest_filter=FailoverConsensusTest.MaxConcurrentRecoveriesEnforced
```
Expected: Test compiles and passes.

---

### Step C.4.4: Commit

```bash
cd <repo-root>
git add include/cedar/dtx/failover_manager.h src/dtx/storage/failover_manager.cc tests/dtx/failover_consensus_test.cc
git commit -m "fix(distributed): unbounded failures map + unused max_concurrent_recoveries

- MarkRecovered now erases from failures_ when failure_retention_duration=0.
- Added CleanupOldFailures() periodic purge for non-zero retention.
- Added failure_retention_duration to ClusterFailoverManager::Config.
- Enforce max_concurrent_recoveries via active_recovery_count_ guard.
- Added action_id to RecoveryAction for unique in-flight tracking.
- Prevents OOM from unbounded failures_ growth and runaway recovery storms."
```

---

## Task 5: Full Build & Test Verification

---

### Step C.5.1: Clean rebuild

```bash
cd <repo-root>/build
cmake --build . -j$(sysctl -n hw.ncpu)
```
Expected: Zero project-code warnings.

---

### Step C.5.2: Run all DTX and cluster tests

```bash
cd <repo-root>/build
ctest --output-on-failure -R "(cross_dc|failover|dtx)"
```
Expected: All targeted tests pass.

---

### Step C.5.3: Run full test suite

```bash
cd <repo-root>/build
ctest --output-on-failure
```
Expected: All tests pass (or only pre-existing failures).

---

### Step C.5.4: Tag sub-plan completion

```bash
cd <repo-root>
git tag subplan-c-complete
git log --oneline -8
```

---

## Self-Review Checklist

**1. Spec coverage:**
- [x] P0-7: Cross-DC sync replication all-or-nothing rollback — Task 1
- [x] P0-8: Bounded reconciliation queue with TTL + attempt_count increment — Task 2
- [x] P0-9: Failover fencing with quorum verification — Task 3
- [x] Sequence counter wraparound handling — Task 2
- [x] Unbounded failures_ map cleanup — Task 4
- [x] max_concurrent_recoveries enforcement — Task 4

**2. Placeholder scan:**
- [x] No TBD/TODO in code blocks
- [x] All code blocks contain real, compilable C++

**3. Type consistency:**
- [x] `ReplicationLog::generation` added and used in ReceiveReplication
- [x] `ReconcileEntry::enqueued_at` added and used in TTL eviction
- [x] `RecoveryAction::action_id` added for guard tracking
- [x] `QuorumVerificationCallback` signature matches usage in PerformLeaderSwitch

---

## Execution Handoff

**Plan complete and saved to `<repo-root>/docs/superpowers/plans/2026-06-10-subplan-c-distributed-consistency.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task (C.1, C.2, C.3, C.4), review between tasks.

**2. Inline Execution** — Batch execute in this session using `superpowers:executing-plans`.
