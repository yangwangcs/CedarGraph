# P1 Fix: Auth for Internal Services + Leader Lease Check + Raft Rollback

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Add `CheckAuth()` to GCN/MetaD/DTX services, add `IsLeaseValid()` to all write paths in StorageD, and fix Raft state machine to rollback on apply errors.

**Architecture:** Reuse the existing `CheckAuth()` helper pattern from `storage_service_impl.cc`. Add a `CheckWriteLeader()` helper that combines `IsLeader() + IsLeaseValid()`. In the state machine, call `iter.set_error_and_rollback()` on any storage apply failure.

**Tech Stack:** C++17, gRPC, braft, brpc, gtest

---

## File Map

| File | Responsibility |
|------|----------------|
| `src/gcn/gcn_service.cc` | Add CheckAuth to 4 GCN handlers |
| `src/dtx/grpc/meta_service_grpc.cc` | Add CheckAuth to 15 MetaD handlers |
| `src/dtx/dtx_service_impl.cc` | Add CheckAuth to 7 DTX handlers |
| `src/dtx/storage_impl/storage_service_impl.cc` | Add IsLeaseValid to 6 write paths; add CheckWriteLeader helper |
| `src/dtx/storage/braft_partition_state_machine.cc` | Rollback on ApplyPut/ApplyDelete failure |
| `tests/dtx/test_storage_service_auth.cc` | Existing auth tests — extend coverage |

---

### Task 1: Add CheckAuth to GCN Service

**Files:**
- Modify: `src/gcn/gcn_service.cc:1-30` (add include + helper)
- Modify: `src/gcn/gcn_service.cc:66,84,108,122` (add auth gate)

- [ ] **Step 1: Add CheckAuth helper at top of gcn_service.cc**

```cpp
#include "cedar/dtx/security/security_manager.h"

namespace {
grpc::Status CheckAuth(grpc::ServerContext* context,
                       cedar::dtx::security::Permission perm) {
  auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
  if (!sm || !sm->IsAuthEnabled()) return grpc::Status::OK;
  auto meta = context->client_metadata();
  auto it = meta.find("authorization");
  if (it == meta.end()) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Missing auth token");
  }
  auto st = sm->AuthenticateAndAuthorize(
      std::string(it->second.data(), it->second.size()), perm, "");
  if (!st.ok()) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, st.ToString());
  }
  return grpc::Status::OK;
}
}  // namespace
```

- [ ] **Step 2: Add auth gate to Traverse handler (line ~66)**

Replace the first line of each handler body with:
```cpp
  if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
```

Handlers and their permissions:
- `Traverse` → `kRead`
- `SubQuery` → `kRead`
- `OnCacheInvalidate` → `kWrite`
- `OnEventStream` → `kRead`

- [ ] **Step 3: Build cedar target**

```bash
cd <repo-root>/build && cmake --build . --target cedar -j$(sysctl -n hw.ncpu)
```
Expected: compiles without errors.

- [ ] **Step 4: Commit**

```bash
cd <repo-root> && git add src/gcn/gcn_service.cc && git commit -m "fix(gcn): add CheckAuth to all GCN service handlers"
```

---

### Task 2: Add CheckAuth to MetaD gRPC Service

**Files:**
- Modify: `src/dtx/grpc/meta_service_grpc.cc:1-50` (add include + helper)
- Modify: `src/dtx/grpc/meta_service_grpc.cc:50,78,95,117,141,171,188,216,233,297,350,378,403,423,439` (add auth gate)

- [ ] **Step 1: Add CheckAuth helper at top of meta_service_grpc.cc**

```cpp
#include "cedar/dtx/security/security_manager.h"

namespace {
grpc::Status CheckAuth(grpc::ServerContext* context,
                       cedar::dtx::security::Permission perm) {
  auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
  if (!sm || !sm->IsAuthEnabled()) return grpc::Status::OK;
  auto meta = context->client_metadata();
  auto it = meta.find("authorization");
  if (it == meta.end()) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Missing auth token");
  }
  auto st = sm->AuthenticateAndAuthorize(
      std::string(it->second.data(), it->second.size()), perm, "");
  if (!st.ok()) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, st.ToString());
  }
  return grpc::Status::OK;
}
}  // namespace
```

- [ ] **Step 2: Add auth gate to every handler**

Permissions per handler:
- `CreateSpace` → `kWrite`
- `GetSpace` → `kRead`
- `GetPartitionAssignment` → `kRead`
- `UpdatePartitionAssignment` → `kWrite`
- `GetSpacePartitionMap` → `kRead`
- `RegisterNode` → `kWrite`
- `Heartbeat` → `kMonitor`
- `GetNode` → `kRead`
- `GetAliveNodes` → `kRead`
- `WatchPartitionMap` → `kRead`
- `CreateLabelSchema` → `kWrite`
- `GetSchema` → `kRead`
- `LocateCache` → `kRead`
- `ReportCache` → `kWrite`
- `GcnHeartbeat` → `kMonitor`

- [ ] **Step 3: Build cedar target**

```bash
cd <repo-root>/build && cmake --build . --target cedar -j$(sysctl -n hw.ncpu)
```

- [ ] **Step 4: Commit**

```bash
cd <repo-root> && git add src/dtx/grpc/meta_service_grpc.cc && git commit -m "fix(metad): add CheckAuth to all MetaD gRPC handlers"
```

---

### Task 3: Add CheckAuth to DTX Service

**Files:**
- Modify: `src/dtx/dtx_service_impl.cc:1-40` (add include + helper)
- Modify: `src/dtx/dtx_service_impl.cc:70,116,198,233,265,296,338` (add auth gate)

- [ ] **Step 1: Add CheckAuth helper at top of dtx_service_impl.cc**

Same helper as Task 2 (copy-paste identical code).

- [ ] **Step 2: Add auth gate to every handler**

Permissions:
- `Replicate` → `kWrite`
- `ApplyReplication` → `kWrite`
- `Prepare` → `kWrite`
- `Commit` → `kWrite`
- `Abort` → `kWrite`
- `Inquire` → `kRead`
- `RegisterParticipant` → `kWrite`

- [ ] **Step 3: Build cedar target**

```bash
cd <repo-root>/build && cmake --build . --target cedar -j$(sysctl -n hw.ncpu)
```

- [ ] **Step 4: Commit**

```bash
cd <repo-root> && git add src/dtx/dtx_service_impl.cc && git commit -m "fix(dtx): add CheckAuth to all DTX service handlers"
```

---

### Task 4: Add IsLeaseValid to Write Paths

**Files:**
- Modify: `src/dtx/storage_impl/storage_service_impl.cc:291,446,738,935,1070,1223`

- [ ] **Step 1: Add CheckWriteLeader helper near CheckReadLeader**

Find `CheckReadLeader` (around line 340) and add after it:

```cpp
grpc::Status CheckWriteLeader(braft::PartitionRaftGroup* raft_group) {
  if (!raft_group->IsLeader()) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "Not leader for this partition");
  }
  if (!raft_group->IsLeaseValid()) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "Leader lease expired");
  }
  return grpc::Status::OK;
}
```

- [ ] **Step 2: Replace IsLeader checks with CheckWriteLeader in write handlers**

Replace these patterns:
```cpp
if (!raft_group->IsLeader()) { return Status::NotLeader(...); }
```

With:
```cpp
if (auto st = CheckWriteLeader(raft_group); !st.ok()) { return st; }
```

Locations:
- `Put()` around line 291
- `Delete()` around line 446
- `BatchPut()` around line 738
- `Prepare()` around line 935
- `Commit()` around line 1070
- `Abort()` around line 1223

Note: Some locations use `grpc::Status`, others use internal `Status`. Use the correct return type for each function (`grpc::Status` for gRPC handlers, `Status` for internal helpers).

- [ ] **Step 3: Build and run storage service auth tests**

```bash
cd <repo-root>/build && cmake --build . --target test_storage_service_auth -j$(sysctl -n hw.ncpu) && ./tests/test_storage_service_auth
```
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
cd <repo-root> && git add src/dtx/storage_impl/storage_service_impl.cc && git commit -m "fix(storage): enforce IsLeaseValid on all write paths"
```

---

### Task 5: Fix Raft State Machine Rollback on Apply Error

**Files:**
- Modify: `src/dtx/storage/braft_partition_state_machine.cc:100-112`

- [ ] **Step 1: Add rollback on ApplyPut failure**

At line 100-105, replace:
```cpp
    if (cmd.type == StorageRaftCommand::Type::kPut) {
      auto status = storage_->Put(cmd.key, cmd.descriptor, cmd.txn_version, 0);
      if (!status.ok()) {
        LOG(ERROR) << "Apply PUT failed at index=" << iter.index()
                   << ": " << status.ToString();
      }
```

With:
```cpp
    if (cmd.type == StorageRaftCommand::Type::kPut) {
      auto status = storage_->Put(cmd.key, cmd.descriptor, cmd.txn_version, 0);
      if (!status.ok()) {
        LOG(ERROR) << "Apply PUT failed at index=" << iter.index()
                   << ": " << status.ToString();
        iter.set_error_and_rollback();
        return;
      }
```

- [ ] **Step 2: Add rollback on ApplyDelete failure**

At line 106-111, replace:
```cpp
    } else if (cmd.type == StorageRaftCommand::Type::kDelete) {
      auto status = storage_->Put(cmd.key, Descriptor(), cmd.txn_version, 0);
      if (!status.ok()) {
        LOG(ERROR) << "Apply DELETE failed at index=" << iter.index()
                   << ": " << status.ToString();
      }
```

With:
```cpp
    } else if (cmd.type == StorageRaftCommand::Type::kDelete) {
      auto status = storage_->Put(cmd.key, Descriptor(), cmd.txn_version, 0);
      if (!status.ok()) {
        LOG(ERROR) << "Apply DELETE failed at index=" << iter.index()
                   << ": " << status.ToString();
        iter.set_error_and_rollback();
        return;
      }
```

- [ ] **Step 3: Build cedar target**

```bash
cd <repo-root>/build && cmake --build . --target cedar -j$(sysctl -n hw.ncpu)
```

- [ ] **Step 4: Commit**

```bash
cd <repo-root> && git add src/dtx/storage/braft_partition_state_machine.cc && git commit -m "fix(raft): rollback on storage apply error to prevent replica divergence"
```

---

### Task 6: Full Regression Test

- [ ] **Step 1: Run full test suite**

```bash
cd <repo-root>/build && ctest --output-on-failure -j$(sysctl -n hw.ncpu)
```
Expected: 1285/1285 passed, 0 failed.

- [ ] **Step 2: Commit any final fixes**

If tests fail, fix and commit. Otherwise, no additional commit needed.
