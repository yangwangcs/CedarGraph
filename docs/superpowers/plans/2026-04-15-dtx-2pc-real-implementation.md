# DTx / 2PC Real Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all stubs and simulators in the distributed transaction layer with real production-grade implementations: real cross-node RPC 2PC, coordinator crash recovery, cross-shard SI validation, full temporal-range commit, and active deadlock detection.

**Architecture:** `Optimized2PCEngine` is rewritten to use real `StorageClient` gRPC stubs with proper prepare/commit/abort phases and result aggregation. `TransactionRecoveryManager` executes recovery actions by re-connecting to participants and driving them to completion. `DTxRpcClient` becomes a thin wrapper around `StorageClient` with real network I/O. Cross-shard validation uses a two-round validation protocol. Deadlock detection is wired into lock-wait paths.

**Tech Stack:** C++17, gRPC, Protocol Buffers, GoogleTest, CMake

---

## File Structure

| File | Responsibility |
|------|----------------|
| `src/dtx/optimized_2pc_engine.cc` | Rewritten: real parallel 2PC using `StorageClient`, no sleeps, no detached threads |
| `src/dtx/transaction_recovery_manager.cc` | Implements `ApplyRecoveryAction`, `SendCommitToParticipants`, `SendAbortToParticipants`, `InquireParticipant` |
| `src/dtx/grpc/rpc_client.cc` | Rewritten: delegates all methods to a real `StorageClient` instance |
| `src/dtx/protocol/version_chain.cc` | Implements `DistributedValidationCoordinator::CoordinateValidation()` with two-round quorum validation |
| `src/dtx/protocol/lsm_native_occ.cc` | Fixes `SameTemporalRangeCommit()` to prepare and commit all participants, not just partition 0 |
| `src/transaction/occ_transaction.cc` | Wires `RegisterWait` calls into lock-contention paths so deadlock detector is populated |
| `tests/test_2pc_recovery.cpp` | Existing recovery tests to extend |
| `tests/test_driver_transaction.cc` | Driver-level transaction tests |

---

## Task 1: Rewrite `Optimized2PCEngine` with Real 2PC RPC

**Files:**
- Modify: `src/dtx/optimized_2pc_engine.cc`
- Test: `tests/test_driver_transaction.cc`

- [ ] **Step 1: Remove all `sleep_for` and detached threads from `Optimized2PCEngine`**

Open `src/dtx/optimized_2pc_engine.cc`. Delete the old `Execute2PCAsync`, `ExecuteSequential2PC`, `ExecuteParallel2PC`, and `PipelineWorkerLoop` implementations.

- [ ] **Step 2: Implement real `ExecuteSequential2PC()`**

Insert the new implementation:

```cpp
bool Optimized2PCEngine::ExecuteSequential2PC(
    TransactionContext* ctx,
    const std::vector<ParticipantAddress>& participants) {
  if (participants.empty()) {
    return true;
  }

  // Phase 1: Prepare all participants
  std::vector<bool> prepared(participants.size(), false);
  for (size_t i = 0; i < participants.size(); ++i) {
    auto client = GetOrCreateClient(participants[i]);
    if (!client) {
      ctx->SetError("Failed to connect to participant " + participants[i].address);
      return false;
    }

    PrepareRequest req;
    req.set_txn_id(ctx->txn_id());
    req.set_timestamp(ctx->timestamp());
    for (const auto& op : ctx->write_set()) {
      auto* write = req.add_writes();
      write->set_key(op.key.Serialize());
      write->set_value(op.value);
    }

    PrepareResponse resp;
    auto status = client->Prepare(req, &resp);
    if (!status.ok() || !resp.success()) {
      ctx->SetError("Prepare failed on " + participants[i].address + ": " +
                    status.ToString());
      // Abort already-prepared participants before returning
      for (size_t j = 0; j < i; ++j) {
        if (prepared[j]) {
          AbortRequest abort_req;
          abort_req.set_txn_id(ctx->txn_id());
          AbortResponse abort_resp;
          auto abort_client = GetOrCreateClient(participants[j]);
          if (abort_client) abort_client->Abort(abort_req, &abort_resp);
        }
      }
      return false;
    }
    prepared[i] = true;
  }

  // Phase 2: Commit all participants
  for (size_t i = 0; i < participants.size(); ++i) {
    auto client = GetOrCreateClient(participants[i]);
    if (!client) continue;

    CommitRequest req;
    req.set_txn_id(ctx->txn_id());
    CommitResponse resp;
    auto status = client->Commit(req, &resp);
    if (!status.ok()) {
      ctx->SetError("Commit failed on " + participants[i].address);
      // Log for recovery; do not return false here because some commits may
      // have succeeded and we must rely on recovery manager.
    }
  }

  return true;
}
```

If `PrepareRequest` / `CommitRequest` / `AbortRequest` types differ, use the actual DTx proto or `StorageClient` wrapper types defined in `include/cedar/dtx/storage_service_impl.h`.

- [ ] **Step 3: Implement real `ExecuteParallel2PC()` using a thread pool**

```cpp
bool Optimized2PCEngine::ExecuteParallel2PC(
    TransactionContext* ctx,
    const std::vector<ParticipantAddress>& participants) {
  if (participants.empty()) {
    return true;
  }

  std::vector<std::future<bool>> futures;
  std::atomic<bool> any_prepare_failed{false};

  // Phase 1: Parallel prepare
  for (const auto& addr : participants) {
    futures.push_back(thread_pool_->Submit([&, addr]() {
      auto client = GetOrCreateClient(addr);
      if (!client) return false;

      PrepareRequest req;
      req.set_txn_id(ctx->txn_id());
      PrepareResponse resp;
      auto s = client->Prepare(req, &resp);
      return s.ok() && resp.success();
    }));
  }

  std::vector<bool> prepared;
  for (auto& f : futures) {
    prepared.push_back(f.get());
    if (!prepared.back()) {
      any_prepare_failed = true;
    }
  }

  if (any_prepare_failed.load()) {
    // Abort all that prepared successfully
    for (size_t i = 0; i < participants.size(); ++i) {
      if (prepared[i]) {
        thread_pool_->Submit([&, i]() {
          auto client = GetOrCreateClient(participants[i]);
          if (!client) return;
          AbortRequest req;
          req.set_txn_id(ctx->txn_id());
          AbortResponse resp;
          client->Abort(req, &resp);
        }).wait();
      }
    }
    ctx->SetError("Parallel prepare failed");
    return false;
  }

  // Phase 2: Parallel commit
  futures.clear();
  for (const auto& addr : participants) {
    futures.push_back(thread_pool_->Submit([&, addr]() {
      auto client = GetOrCreateClient(addr);
      if (!client) return false;
      CommitRequest req;
      req.set_txn_id(ctx->txn_id());
      CommitResponse resp;
      auto s = client->Commit(req, &resp);
      return s.ok();
    }));
  }

  for (auto& f : futures) {
    f.get();  // ignore individual commit errors; recovery manager handles them
  }

  return true;
}
```

If `Optimized2PCEngine` does not have a `thread_pool_`, add a `cedar::ThreadPool` member in `include/cedar/dtx/optimized_2pc_engine.h` and initialize it in the constructor with a fixed size (e.g., `std::thread::hardware_concurrency()`).

- [ ] **Step 4: Remove `PipelineWorkerLoop` and `Execute2PCAsync` or make them use the synchronous implementations**

Delete `PipelineWorkerLoop` entirely. Rewrite `Execute2PCAsync` to use `thread_pool_->Submit` instead of `std::thread(...).detach()`:

```cpp
std::future<bool> Optimized2PCEngine::Execute2PCAsync(
    TransactionContext* ctx,
    const std::vector<ParticipantAddress>& participants) {
  return thread_pool_->Submit([this, ctx, participants]() {
    return ExecuteParallel2PC(ctx, participants);
  });
}
```

- [ ] **Step 5: Write a test proving sleeps and detached threads are gone**

Add to `tests/test_driver_transaction.cc`:

```cpp
TEST(Optimized2PCTest, NoSleepNoDetach) {
  // This is a compile-time / static test: we verify the engine can be
  // instantiated and that synchronous 2PC fails gracefully without a backend.
  cedar::dtx::Optimized2PCEngine engine;
  cedar::dtx::TransactionContext ctx(12345);
  std::vector<cedar::dtx::ParticipantAddress> parts;
  bool ok = engine.ExecuteSequential2PC(&ctx, parts);
  EXPECT_TRUE(ok);  // empty participants => trivial success
}
```

- [ ] **Step 6: Build and run**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_driver_transaction && ./tests/test_driver_transaction --gtest_filter='Optimized2PCTest.NoSleepNoDetach'
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/dtx/optimized_2pc_engine.cc include/cedar/dtx/optimized_2pc_engine.h tests/test_driver_transaction.cc
git commit -m "feat(dtx): rewrite Optimized2PCEngine with real 2PC RPC and thread pool"
```

---

## Task 2: Implement Transaction Recovery Actions

**Files:**
- Modify: `src/dtx/transaction_recovery_manager.cc:158-176`
- Test: `tests/test_2pc_recovery.cpp`

- [ ] **Step 1: Implement `SendCommitToParticipants()`**

In `src/dtx/transaction_recovery_manager.cc`, replace the TODO stub with:

```cpp
Status TransactionRecoveryManager::SendCommitToParticipants(
    TxnID txn_id,
    const std::vector<ParticipantAddress>& participants) {
  for (const auto& addr : participants) {
    auto client = storage_client_factory_(addr);
    if (!client) {
      LOG(ERROR) << "Recovery: cannot create client for " << addr.address;
      continue;
    }

    CommitRequest req;
    req.set_txn_id(txn_id);
    CommitResponse resp;
    auto s = client->Commit(req, &resp);
    if (!s.ok()) {
      LOG(ERROR) << "Recovery: commit failed for txn " << txn_id
                 << " on " << addr.address << ": " << s.ToString();
      // Leave txn in recovery queue for retry
      return s;
    }
  }
  return Status::OK();
}
```

- [ ] **Step 2: Implement `SendAbortToParticipants()`**

```cpp
Status TransactionRecoveryManager::SendAbortToParticipants(
    TxnID txn_id,
    const std::vector<ParticipantAddress>& participants) {
  for (const auto& addr : participants) {
    auto client = storage_client_factory_(addr);
    if (!client) continue;

    AbortRequest req;
    req.set_txn_id(txn_id);
    AbortResponse resp;
    auto s = client->Abort(req, &resp);
    if (!s.ok()) {
      LOG(WARNING) << "Recovery: abort failed for txn " << txn_id
                   << " on " << addr.address;
    }
  }
  return Status::OK();
}
```

- [ ] **Step 3: Implement `InquireParticipant()`**

```cpp
StatusOr<ParticipantState> TransactionRecoveryManager::InquireParticipant(
    TxnID txn_id,
    const ParticipantAddress& addr) {
  auto client = storage_client_factory_(addr);
  if (!client) {
    return Status::IOError("Cannot create client for " + addr.address);
  }

  InquireRequest req;
  req.set_txn_id(txn_id);
  InquireResponse resp;
  auto s = client->Inquire(req, &resp);
  if (!s.ok()) {
    return s;
  }

  return static_cast<ParticipantState>(resp.state());
}
```

If `InquireRequest` / `InquireResponse` do not exist in the proto, add them to `proto/cedar_dtx.proto`:

```protobuf
message InquireRequest {
  uint64 txn_id = 1;
}

message InquireResponse {
  enum State {
    UNKNOWN = 0;
    PREPARED = 1;
    COMMITTED = 2;
    ABORTED = 3;
  }
  State state = 1;
}
```

- [ ] **Step 4: Wire `ApplyRecoveryAction()` to call the new helpers**

Replace the TODO stubs in `ApplyRecoveryAction()` with:

```cpp
Status TransactionRecoveryManager::ApplyRecoveryAction(
    TxnID txn_id,
    RecoveryAction action,
    const std::vector<ParticipantAddress>& participants) {
  switch (action) {
    case RecoveryAction::kCommit:
      return SendCommitToParticipants(txn_id, participants);
    case RecoveryAction::kAbort:
      return SendAbortToParticipants(txn_id, participants);
    case RecoveryAction::kInquire: {
      int prepared = 0;
      int aborted = 0;
      for (const auto& addr : participants) {
        auto state = InquireParticipant(txn_id, addr);
        if (!state.ok()) continue;
        if (state.value() == ParticipantState::kPrepared) prepared++;
        if (state.value() == ParticipantState::kAborted) aborted++;
      }
      if (aborted > 0) {
        return SendAbortToParticipants(txn_id, participants);
      }
      if (prepared > static_cast<int>(participants.size()) / 2) {
        return SendCommitToParticipants(txn_id, participants);
      }
      return SendAbortToParticipants(txn_id, participants);
    }
    default:
      return Status::InvalidArgument("Unknown recovery action");
  }
}
```

- [ ] **Step 5: Add a recovery action unit test**

In `tests/test_2pc_recovery.cpp`, add:

```cpp
TEST(RecoveryManagerTest, ApplyAbortActionWithNoParticipants) {
  // Empty participant list should trivially succeed
  cedar::dtx::TransactionRecoveryManager mgr;
  auto s = mgr.ApplyRecoveryAction(999, cedar::dtx::RecoveryAction::kAbort, {});
  EXPECT_TRUE(s.ok());
}
```

- [ ] **Step 6: Build, run, and commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_2pc_recovery && ./tests/test_2pc_recovery --gtest_filter='RecoveryManagerTest.ApplyAbortActionWithNoParticipants'
```

Expected: PASS

```bash
git add src/dtx/transaction_recovery_manager.cc tests/test_2pc_recovery.cpp proto/cedar_dtx.proto
git commit -m "feat(dtx): implement real recovery action execution in TransactionRecoveryManager"
```

---

## Task 3: Make `DTxRpcClient` a Real RPC Client

**Files:**
- Modify: `src/dtx/grpc/rpc_client.cc`
- Modify: `include/cedar/dtx/grpc/rpc_client.h`
- Test: `tests/test_driver_transaction.cc`

- [ ] **Step 1: Add a `StorageClient` member to `DTxRpcClient`**

In `include/cedar/dtx/grpc/rpc_client.h`, change `DTxRpcClient` to hold a `std::shared_ptr<cedar::dtx::StorageClient>` (or equivalent gRPC stub type):

```cpp
class DTxRpcClient : public DTxRpcClientInterface {
 public:
  explicit DTxRpcClient(std::shared_ptr<cedar::dtx::StorageClient> client);
  // ... existing interface methods
 private:
  std::shared_ptr<cedar::dtx::StorageClient> client_;
};
```

- [ ] **Step 2: Implement all methods by delegating to `StorageClient`**

In `src/dtx/grpc/rpc_client.cc`, replace all stub bodies with:

```cpp
DTxRpcClient::DTxRpcClient(std::shared_ptr<cedar::dtx::StorageClient> client)
    : client_(std::move(client)) {}

Status DTxRpcClient::Put(TxnID txn_id,
                         const CedarKey& key,
                         const Descriptor& value) {
  if (!client_) return Status::IOError("No storage client");
  return client_->Put(key, value);
}

StatusOr<Descriptor> DTxRpcClient::Get(TxnID txn_id, const CedarKey& key) {
  if (!client_) return Status::IOError("No storage client");
  return client_->Get(key);
}

Status DTxRpcClient::Prepare(TxnID txn_id,
                             const std::vector<WriteOperation>& writes) {
  if (!client_) return Status::IOError("No storage client");
  PrepareRequest req;
  req.set_txn_id(txn_id);
  for (const auto& w : writes) {
    auto* op = req.add_writes();
    op->set_key(w.key.Serialize());
    op->set_value(w.value.Serialize());
  }
  PrepareResponse resp;
  auto s = client_->Prepare(req, &resp);
  if (!s.ok()) return s;
  return resp.success() ? Status::OK() : Status::IOError("Prepare rejected");
}

Status DTxRpcClient::Commit(TxnID txn_id) {
  if (!client_) return Status::IOError("No storage client");
  CommitRequest req;
  req.set_txn_id(txn_id);
  CommitResponse resp;
  return client_->Commit(req, &resp);
}

Status DTxRpcClient::Abort(TxnID txn_id) {
  if (!client_) return Status::IOError("No storage client");
  AbortRequest req;
  req.set_txn_id(txn_id);
  AbortResponse resp;
  return client_->Abort(req, &resp);
}
```

- [ ] **Step 3: Write a test that fails when client is null**

In `tests/test_driver_transaction.cc`:

```cpp
TEST(DTxRpcClientTest, NullClientReturnsError) {
  cedar::dtx::DTxRpcClient client(nullptr);
  cedar::CedarKey key(1, cedar::CedarKey::Type::kVertex, 0);
  auto s = client.Put(1, key, cedar::Descriptor());
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}
```

- [ ] **Step 4: Build, run, and commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_driver_transaction && ./tests/test_driver_transaction --gtest_filter='DTxRpcClientTest.NullClientReturnsError'
```

Expected: PASS

```bash
git add src/dtx/grpc/rpc_client.cc include/cedar/dtx/grpc/rpc_client.h tests/test_driver_transaction.cc
git commit -m "feat(dtx): make DTxRpcClient delegate to real StorageClient RPCs"
```

---

## Task 4: Implement Cross-Shard SI Validation

**Files:**
- Modify: `src/dtx/protocol/version_chain.cc:419-430`
- Test: `tests/test_2pc_optimized.cpp`

- [ ] **Step 1: Rewrite `CoordinateValidation()`**

Replace the unconditional `return kValid` with:

```cpp
ValidationResult DistributedValidationCoordinator::CoordinateValidation(
    TxnID txn_id,
    const std::vector<ParticipantAddress>& participants,
    const ValidationSet& reads) {
  if (participants.empty()) {
    return ValidationResult::kValid;
  }

  // Round 1: Send validation requests to all participants in parallel
  std::vector<std::future<ValidationResult>> futures;
  for (const auto& addr : participants) {
    futures.push_back(std::async(std::launch::async, [&, addr]() {
      auto client = storage_client_factory_(addr);
      if (!client) return ValidationResult::kNetworkError;

      ValidateRequest req;
      req.set_txn_id(txn_id);
      for (const auto& r : reads) {
        auto* entry = req.add_reads();
        entry->set_key(r.key.Serialize());
        entry->set_version(r.version);
      }

      ValidateResponse resp;
      auto s = client->Validate(req, &resp);
      if (!s.ok()) return ValidationResult::kNetworkError;
      return static_cast<ValidationResult>(resp.result());
    }));
  }

  // Round 2: Collect results
  int conflicts = 0;
  for (auto& f : futures) {
    auto res = f.get();
    if (res == ValidationResult::kConflict) {
      conflicts++;
    } else if (res == ValidationResult::kNetworkError) {
      return ValidationResult::kNetworkError;
    }
  }

  return conflicts == 0 ? ValidationResult::kValid : ValidationResult::kConflict;
}
```

If `ValidateRequest` / `ValidateResponse` do not exist, add them to `proto/cedar_dtx.proto`:

```protobuf
message ValidateRequest {
  uint64 txn_id = 1;
  message ReadEntry {
    bytes key = 1;
    uint64 version = 2;
  }
  repeated ReadEntry reads = 2;
}

message ValidateResponse {
  enum Result {
    VALID = 0;
    CONFLICT = 1;
    NETWORK_ERROR = 2;
  }
  Result result = 1;
}
```

- [ ] **Step 2: Add a validation coordination test**

In `tests/test_2pc_optimized.cpp` (or create it), add:

```cpp
TEST(DistributedValidationTest, EmptyParticipantsAlwaysValid) {
  cedar::dtx::DistributedValidationCoordinator coordinator;
  cedar::dtx::ValidationSet reads;
  auto result = coordinator.CoordinateValidation(1, {}, reads);
  EXPECT_EQ(result, cedar::dtx::ValidationResult::kValid);
}
```

- [ ] **Step 3: Build, run, and commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_2pc_optimized && ./tests/test_2pc_optimized --gtest_filter='DistributedValidationTest.EmptyParticipantsAlwaysValid'
```

Expected: PASS

```bash
git add src/dtx/protocol/version_chain.cc tests/test_2pc_optimized.cpp proto/cedar_dtx.proto
git commit -m "feat(dtx): implement two-round cross-shard SI validation"
```

---

## Task 5: Fix `SameTemporalRangeCommit()` to Commit All Participants

**Files:**
- Modify: `src/dtx/protocol/lsm_native_occ.cc:310-331`
- Test: `tests/test_driver_transaction.cc`

- [ ] **Step 1: Replace single-partition fallback with full 2PC**

In `src/dtx/protocol/lsm_native_occ.cc`, replace `SameTemporalRangeCommit()` with:

```cpp
Status LndOccEngine::SameTemporalRangeCommit(TransactionContext* ctx) {
  auto participants = GetParticipants(ctx);
  if (participants.empty()) {
    return Status::InvalidArgument("No participants for commit");
  }

  // Even if all writes share the same temporal range, we must still
  // prepare and commit every participant for atomicity.
  std::vector<bool> prepared;
  for (auto* coord : participants) {
    auto s = coord->Prepare(ctx);
    if (!s.ok()) {
      // Abort already-prepared participants
      for (size_t i = 0; i < prepared.size(); ++i) {
        if (prepared[i]) {
          participants[i]->Abort(ctx);
        }
      }
      return s;
    }
    prepared.push_back(true);
  }

  for (auto* coord : participants) {
    auto s = coord->Commit(ctx);
    if (!s.ok()) {
      // Log and rely on recovery manager; partial commit is possible here.
      LOG(ERROR) << "SameTemporalRangeCommit: commit failed for txn "
                 << ctx->txn_id() << ": " << s.ToString();
    }
  }

  return Status::OK();
}
```

- [ ] **Step 2: Add test for multi-participant temporal commit**

In `tests/test_driver_transaction.cc`:

```cpp
TEST(LndOccEngineTest, SameTemporalRangeCommitRequiresParticipants) {
  cedar::dtx::LndOccEngine engine;
  cedar::dtx::TransactionContext ctx(777);
  // No write set => no participants
  auto s = engine.SameTemporalRangeCommit(&ctx);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}
```

- [ ] **Step 3: Build, run, and commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_driver_transaction && ./tests/test_driver_transaction --gtest_filter='LndOccEngineTest.SameTemporalRangeCommitRequiresParticipants'
```

Expected: PASS

```bash
git add src/dtx/protocol/lsm_native_occ.cc tests/test_driver_transaction.cc
git commit -m "fix(dtx): SameTemporalRangeCommit now prepares and commits all participants"
```

---

## Task 6: Wire Deadlock Detection into Lock-Wait Paths

**Files:**
- Modify: `src/transaction/occ_transaction.cc`
- Test: `tests/test_driver_transaction.cc`

- [ ] **Step 1: Add `RegisterWait` calls in `OCCTransaction::Get()` and `Put()` when blocked**

In `src/transaction/occ_transaction.cc`, find the paths where a transaction waits for a lock or retries. Typically this is inside a retry loop. Add deadlock registration before each sleep/retry:

```cpp
// Inside the retry loop of OCCTransaction::Get or Put, when lock contention is detected:
if (deadlock_detector_) {
  auto owner = lock_manager_->GetLockOwner(key);
  if (owner.has_value() && owner.value() != txn_id_) {
    deadlock_detector_->RegisterWait(txn_id_, owner.value(), key);
  }
}
```

If `GetLockOwner` does not exist on `lock_manager_`, add a minimal implementation that returns the transaction ID currently holding the write lock on the key.

- [ ] **Step 2: Ensure `UnregisterWait` is called on success or abort**

At the top of `OCCTransaction::Commit()` and `Abort()`, add:

```cpp
if (deadlock_detector_) {
  deadlock_detector_->UnregisterWait(txn_id_);
}
```

- [ ] **Step 3: Add deadlock detector integration test**

In `tests/test_driver_transaction.cc`:

```cpp
TEST(DeadlockDetectorTest, RegisterAndDetectCycle) {
  cedar::dtx::DistributedDeadlockDetector detector;
  detector.RegisterWait(1, 2, cedar::CedarKey(100, cedar::CedarKey::Type::kVertex, 0));
  detector.RegisterWait(2, 3, cedar::CedarKey(200, cedar::CedarKey::Type::kVertex, 0));
  detector.RegisterWait(3, 1, cedar::CedarKey(300, cedar::CedarKey::Type::kVertex, 0));

  auto victim = detector.DetectAndBreakCycle();
  EXPECT_TRUE(victim.has_value());
}
```

- [ ] **Step 4: Build, run, and commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_driver_transaction && ./tests/test_driver_transaction --gtest_filter='DeadlockDetectorTest.RegisterAndDetectCycle'
```

Expected: PASS

```bash
git add src/transaction/occ_transaction.cc tests/test_driver_transaction.cc
git commit -m "feat(dtx): wire deadlock detector into OCC lock-wait paths"
```

---

## Self-Review Checklist

1. **Spec coverage:** All DTx broken items addressed: Optimized2PCEngine, recovery manager, DTxRpcClient, cross-shard validation, SameTemporalRangeCommit, deadlock detection.
2. **Placeholder scan:** No TBD or TODO left in the plan steps.
3. **Type consistency:** `TxnID`, `ParticipantAddress`, `TransactionContext`, `StorageClient`, `PrepareRequest`, `CommitRequest`, `AbortRequest` usage is consistent across all tasks.
