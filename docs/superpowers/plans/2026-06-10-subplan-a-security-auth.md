# CedarGraph-Core Sub-Plan A: Security Authentication Enforcement

**Date:** 2026-06-10  
**Author:** Agent (sub-plan)  
**Parent:** Production Readiness Audit 2026-05-26  
**Scope:** P0 security blockers only — auth injection + TLS fail-hard  
**Estimated Duration:** 2.5 hours  
**Risk Level:** High ( touches every gRPC entry point )

---

## Goal

Close all P0 security gaps identified in the 2026-05-26 production readiness audit:

1. **P0-1** — Every `StorageService::Service` gRPC handler validates the caller via `SecurityManager::AuthenticateAndAuthorize` before processing.
2. **P0-2** — `GraphServiceRouter::ExecuteQuery` and `GraphServiceRouter::BeginTransaction` enforce the same auth gate.
3. **P0-3** — Remove all "insecure fallback" paths in `TlsCredentialFactory`. If TLS credentials cannot be loaded, the factory **must** return an error, never `InsecureServerCredentials()` or `InsecureChannelCredentials()`.
4. **P0-4** — Eliminate hard-coded `grpc::InsecureChannelCredentials()` in `meta_client.cpp` and `query_storage_client.cpp`; wire them through `TlsCredentialFactory`.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        gRPC Client                               │
│  (graphd / queryd / meta_client / storage_client)               │
└──────────────────────────┬──────────────────────────────────────┘
                           │ TLS (mandatory)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     gRPC Server                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Auth Interceptor (inline — no middleware abstraction)   │   │
│  │  1. Extract "authorization" metadata                     │   │
│  │  2. Validate Bearer <token> via SecurityManager          │   │
│  │  3. Reject with UNAUTHENTICATED / PERMISSION_DENIED      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│     StorageService        │        GraphServiceRouter            │
│     (Put/Get/Delete/...)  │        (ExecuteQuery/BeginTxn/...)   │
└───────────────────────────┴──────────────────────────────────────┘
```

**Decision:** Inline auth checks inside every handler rather than a gRPC interceptor.  
*Rationale:* Interceptors require regenerating proto stubs with a specific plugin flag that the current CMake build does not enable. Inline checks are zero-build-system-risk and audit-log friendly (each handler knows the exact `Permission` enum to check).

---

## Tech Stack

| Component | Version / Binding |
|-----------|-------------------|
| C++ Standard | C++17 |
| gRPC | 1.65+ (system or submodule) |
| Test Framework | GoogleTest (linked via `gtest_main`) |
| Security Module | `cedar::dtx::security::SecurityManager` (singleton) |
| Permission Model | RBAC via `cedar::dtx::security::Permission` bitmask |

---

## File Map

| # | File | Action | P0 |
|---|------|--------|-----|
| 1 | `src/dtx/storage/storage_server_with_grpc.cc` | Add auth helper + inject into all 12 handlers | P0-1 |
| 2 | `src/dtx/storage_impl/storage_service_impl.cc` | Same auth helper + inject into all handlers (same pattern as #1) | P0-1 |
| 3 | `src/service/graph_service_router.cc` | Add `CheckAuth()` private helper; call in `ExecuteQuery`, `BeginTransaction`, and all other entry points | P0-2 |
| 4 | `include/cedar/service/graph_service_router.h` | Declare `CheckAuth()` private method | P0-2 |
| 5 | `src/dtx/raft/grpc_tls.cc` | Replace insecure fallbacks with hard errors | P0-3 |
| 6 | `src/queryd/meta_client.cpp` | Replace `InsecureChannelCredentials()` with `CreateClientCredentialsFromEnv()` | P0-4 |
| 7 | `src/queryd/query_storage_client.cpp` | Replace `InsecureChannelCredentials()` with `CreateClientCredentialsFromEnv()` | P0-4 |
| 8 | `tests/dtx/test_tls_fail_secure.cc` | Update existing tests to expect failure on disabled TLS | P0-3 |
| 9 | `tests/security/test_storage_service_auth.cc` | **New** — unit test verifying auth rejection & success | P0-1 |
| 10 | `tests/service/test_graph_service_router_auth.cc` | **New** — unit test verifying auth on `ExecuteQuery` / `BeginTransaction` | P0-2 |
| 11 | `tests/CMakeLists.txt` | Register new test targets | — |

---

## Phase P0-1: StorageService Auth Injection

### Step 1.1 — Add auth helper to `storage_server_with_grpc.cc`  
**Time:** 3 min  
**File:** `src/dtx/storage/storage_server_with_grpc.cc`

- [ ] Add the `#include` for the security header after the existing includes (around line 38).
- [ ] Insert the `CheckAuth` free function immediately before `class StorageServiceImpl` (around line 229).

```cpp
// Insert after line 38 (after #include "cedar/storage/storage_interface.h")
#include "cedar/dtx/security.h"
```

```cpp
// Insert before line 229 (before class StorageServiceImpl)

namespace {

// Auth gate — called first thing in every gRPC handler
grpc::Status CheckAuth(grpc::ServerContext* context,
                       cedar::dtx::security::Permission required_perm,
                       const std::string& resource = "") {
  auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
  if (!sm) {
    return grpc::Status(grpc::StatusCode::INTERNAL,
                        "SecurityManager not initialized");
  }

  auto meta = context->client_metadata();
  auto it = meta.find("authorization");
  if (it == meta.end()) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                        "Missing authorization header");
  }

  std::string auth_hdr(it->second.data(), it->second.size());
  const std::string kBearer = "Bearer ";
  if (auth_hdr.rfind(kBearer, 0) != 0) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                        "Malformed authorization header; expected Bearer token");
  }

  std::string token = auth_hdr.substr(kBearer.length());
  auto status = sm->AuthenticateAndAuthorize(token, required_perm, resource);
  if (!status.ok()) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                        std::string("Auth failed: ") + status.ToString());
  }
  return grpc::Status::OK;
}

}  // namespace
```

**Verify compile (do NOT link yet):**
```bash
cd <repo-root>/build && \
  cmake --build . --target storaged -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected output:**
```
[100%] Linking CXX executable storaged
[100%] Built target storaged
```
*(Link may fail until we inject the calls; that is fine for this step.)*

---

### Step 1.2 — Inject auth into data-plane handlers  
**Time:** 4 min  
**File:** `src/dtx/storage/storage_server_with_grpc.cc`

- [ ] Modify **Put**, **Get**, **Delete**, **Scan**, **ScanNodeV2**, **ScanEdgeV2** so the first statement after `IsCancelled()` is the auth check.

**Put** (line 240):
```cpp
  grpc::Status Put(grpc::ServerContext* context,
                   const cedar::storage::PutRequest* request,
                   cedar::storage::PutResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Get** (line 288):
```cpp
  grpc::Status Get(grpc::ServerContext* context,
                   const cedar::storage::GetRequest* request,
                   cedar::storage::GetResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Delete** (line 323):
```cpp
  grpc::Status Delete(grpc::ServerContext* context,
                      const cedar::storage::DeleteRequest* request,
                      cedar::storage::DeleteResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kDelete);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Scan** (line 354):
```cpp
  grpc::Status Scan(grpc::ServerContext* context,
                    const cedar::storage::ScanRequest* request,
                    cedar::storage::ScanResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**ScanNodeV2** (line 367):
```cpp
  grpc::Status ScanNodeV2(grpc::ServerContext* context,
                          const cedar::storage::ScanNodeRequestV2* request,
                          cedar::storage::ScanResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**ScanEdgeV2** (line 403):
```cpp
  grpc::Status ScanEdgeV2(grpc::ServerContext* context,
                          const cedar::storage::ScanEdgeRequestV2* request,
                          cedar::storage::ScanResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Compile check:**
```bash
cd <repo-root>/build && \
  cmake --build . --target storaged -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target storaged`

---

### Step 1.3 — Inject auth into batch & 2PC handlers  
**Time:** 4 min  
**File:** `src/dtx/storage/storage_server_with_grpc.cc`

- [ ] Modify **BatchPut**, **BatchGet**, **Prepare**, **Commit**, **Abort**.

**BatchPut** (line 460):
```cpp
  grpc::Status BatchPut(grpc::ServerContext* context,
                        const cedar::storage::BatchPutRequest* request,
                        cedar::storage::BatchPutResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**BatchGet** (line 507):
```cpp
  grpc::Status BatchGet(grpc::ServerContext* context,
                        const cedar::storage::BatchGetRequest* request,
                        cedar::storage::BatchGetResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Prepare** (line 541):
```cpp
  grpc::Status Prepare(grpc::ServerContext* context,
                       const cedar::storage::PrepareRequest* request,
                       cedar::storage::PrepareResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Commit** (line 652):
```cpp
  grpc::Status Commit(grpc::ServerContext* context,
                      const cedar::storage::CommitRequest* request,
                      cedar::storage::CommitResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Abort** (line 717):
```cpp
  grpc::Status Abort(grpc::ServerContext* context,
                     const cedar::storage::AbortRequest* request,
                     cedar::storage::AbortResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Compile check:**
```bash
cd <repo-root>/build && \
  cmake --build . --target storaged -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target storaged`

---

### Step 1.4 — Inject auth into admin & heartbeat handlers  
**Time:** 3 min  
**File:** `src/dtx/storage/storage_server_with_grpc.cc`

- [ ] Modify **GetPartitionInfo** and **Heartbeat**.

**GetPartitionInfo** (line 780):
```cpp
  grpc::Status GetPartitionInfo(grpc::ServerContext* context,
                                const cedar::storage::GetPartitionInfoRequest* request,
                                cedar::storage::GetPartitionInfoResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kMonitor);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Heartbeat** (line 809):
```cpp
  grpc::Status Heartbeat(grpc::ServerContext* context,
                         grpc::ServerReaderWriter<cedar::storage::HeartbeatResponse,
                                                  cedar::storage::HeartbeatRequest>* stream) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kMonitor);
        !st.ok()) {
      return st;
    }
    // ... existing body unchanged ...
```

**Commit:**
```bash
cd <repo-root> && \
  git add src/dtx/storage/storage_server_with_grpc.cc && \
  git commit -m "P0-1: wire auth into all StorageService handlers (storage_server_with_grpc)"
```

---

### Step 1.5 — Apply identical auth pattern to `storage_service_impl.cc`  
**Time:** 5 min  
**File:** `src/dtx/storage_impl/storage_service_impl.cc`

- [ ] Add `#include "cedar/dtx/security.h"` after existing includes (line ~18).
- [ ] Add `CheckAuth` free function in the `cedar::dtx` namespace before `StorageServiceImpl` method definitions.
- [ ] Inject the same `if (auto st = CheckAuth(...))` block into **every** overridden handler in this file: `Put`, `Get`, `Delete`, `Scan`, `ScanNodeV2`, `ScanEdgeV2`, `BatchPut`, `BatchGet`, `Prepare`, `Commit`, `Abort`, `Inquire`, `GetRangeForCompute`, `GetCommittedVersion`, `GetPartitionInfo`, `Flush`, `Heartbeat`, `ExecuteSubQuery`.

The helper code is identical to Step 1.1 (just change the namespace from anonymous to `cedar::dtx`).  
The permission mapping is identical to Steps 1.2–1.4:

| Handler | Permission |
|---------|------------|
| Put, Delete, BatchPut, Prepare, Commit, Abort, Inquire, Flush, ExecuteSubQuery | `kWrite` |
| Get, Scan, ScanNodeV2, ScanEdgeV2, BatchGet, GetRangeForCompute, GetCommittedVersion | `kRead` |
| GetPartitionInfo, Heartbeat | `kMonitor` |

**Compile check:**
```bash
cd <repo-root>/build && \
  cmake --build . --target cedar -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target cedar`

**Commit:**
```bash
cd <repo-root> && \
  git add src/dtx/storage_impl/storage_service_impl.cc && \
  git commit -m "P0-1: wire auth into all StorageService handlers (storage_service_impl)"
```

---

## Phase P0-2: GraphServiceRouter Auth Injection

### Step 2.1 — Declare `CheckAuth` in header  
**Time:** 2 min  
**File:** `include/cedar/service/graph_service_router.h`

- [ ] Add `#include "cedar/dtx/security.h"` after the existing includes (after line 33).
- [ ] Add a private method declaration inside `class GraphServiceRouter` (in the `private:` section, after line 327).

```cpp
// Insert after line 33
#include "cedar/dtx/security.h"
```

```cpp
// Insert in private section, after line ~327 (after ExecuteDistributedWrite declaration)
  // Auth gate
  grpc::Status CheckAuth(grpc::ServerContext* context,
                         cedar::dtx::security::Permission perm,
                         const std::string& resource = "") const;
```

---

### Step 2.2 — Implement `CheckAuth` and wire into `ExecuteQuery` + `BeginTransaction`  
**Time:** 4 min  
**File:** `src/service/graph_service_router.cc`

- [ ] Add the implementation of `CheckAuth` at the bottom of the file, before the closing namespace brace (line 2322).

```cpp
// Insert before line 2322 (before }  // namespace service)

grpc::Status GraphServiceRouter::CheckAuth(
    grpc::ServerContext* context,
    cedar::dtx::security::Permission perm,
    const std::string& resource) const {
  auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
  if (!sm) {
    return grpc::Status(grpc::StatusCode::INTERNAL,
                        "SecurityManager not initialized");
  }

  auto meta = context->client_metadata();
  auto it = meta.find("authorization");
  if (it == meta.end()) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                        "Missing authorization header");
  }

  std::string auth_hdr(it->second.data(), it->second.size());
  const std::string kBearer = "Bearer ";
  if (auth_hdr.rfind(kBearer, 0) != 0) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                        "Malformed authorization header; expected Bearer token");
  }

  std::string token = auth_hdr.substr(kBearer.length());
  auto status = sm->AuthenticateAndAuthorize(token, perm, resource);
  if (!status.ok()) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                        std::string("Auth failed: ") + status.ToString());
  }
  return grpc::Status::OK;
}
```

- [ ] Modify `ExecuteQuery` (line 261):

```cpp
grpc::Status GraphServiceRouter::ExecuteQuery(grpc::ServerContext* context,
                                              const ExecuteQueryRequest* request,
                                              ExecuteQueryResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  // Auth: read permission minimum; upgraded to write inside if needed
  if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead);
      !st.ok()) {
    return st;
  }
  // ... existing body unchanged ...
```

- [ ] Modify `BeginTransaction` (line 2034):

```cpp
grpc::Status GraphServiceRouter::BeginTransaction(grpc::ServerContext* context,
                                                  const cedargrpc::BeginTransactionRequest* request,
                                                  cedargrpc::Transaction* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite);
      !st.ok()) {
    return st;
  }
  // ... existing body unchanged ...
```

**Compile check:**
```bash
cd <repo-root>/build && \
  cmake --build . --target graphd -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target graphd`

---

### Step 2.3 — Wire auth into remaining GraphServiceRouter entry points  
**Time:** 4 min  
**File:** `src/service/graph_service_router.cc`

- [ ] Add auth checks to the remaining handlers using the same one-liner pattern.

| Handler | Permission | Line |
|---------|------------|------|
| `Traverse` | `kRead` | ~678 |
| `TemporalQuery` | `kRead` | ~770 |
| `Health` | `kMonitor` | ~848 |
| `GetStats` | `kMonitor` | ~874 |
| `StreamQuery` | `kRead` | ~900 |
| `BatchQuery` | `kRead` | ~1030 |
| `GetSchema` | `kRead` | ~1064 |
| `Commit` | `kWrite` | ~2073 |
| `Rollback` | `kWrite` | ~2121 |

Example pattern for each:
```cpp
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead);
      !st.ok()) {
    return st;
  }
```

**Compile check:**
```bash
cd <repo-root>/build && \
  cmake --build . --target graphd -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target graphd`

**Commit:**
```bash
cd <repo-root> && \
  git add include/cedar/service/graph_service_router.h src/service/graph_service_router.cc && \
  git commit -m "P0-2: wire auth into GraphServiceRouter entry points"
```

---

## Phase P0-3: Remove gRPC Insecure Fallback

### Step 3.1 — Modify `CreateServerCredentials` & `CreateClientCredentials`  
**Time:** 3 min  
**File:** `src/dtx/raft/grpc_tls.cc`

- [ ] Replace the early `return InsecureServerCredentials()` in `CreateServerCredentials` with a hard error.

**Before** (lines 47–51):
```cpp
StatusOr<std::shared_ptr<ServerCredentials>> TlsCredentialFactory::CreateServerCredentials(
    const TlsConfig& config) {
  if (!config.enabled) {
    return InsecureServerCredentials();
  }
```

**After:**
```cpp
StatusOr<std::shared_ptr<ServerCredentials>> TlsCredentialFactory::CreateServerCredentials(
    const TlsConfig& config) {
  if (!config.enabled) {
    return Status::InvalidArgument(
        "TLS is mandatory. Set enabled=true and provide certificate files. "
        "Insecure credentials are not allowed.");
  }
```

- [ ] Replace the early `return InsecureChannelCredentials()` in `CreateClientCredentials` with a hard error.

**Before** (lines 85–89):
```cpp
StatusOr<std::shared_ptr<ChannelCredentials>> TlsCredentialFactory::CreateClientCredentials(
    const TlsConfig& config) {
  if (!config.enabled) {
    return InsecureChannelCredentials();
  }
```

**After:**
```cpp
StatusOr<std::shared_ptr<ChannelCredentials>> TlsCredentialFactory::CreateClientCredentials(
    const TlsConfig& config) {
  if (!config.enabled) {
    return Status::InvalidArgument(
        "TLS is mandatory. Set enabled=true and provide certificate files. "
        "Insecure credentials are not allowed.");
  }
```

---

### Step 3.2 — Modify `CreateServerCredentialsFromEnv` & `CreateClientCredentialsFromEnv`  
**Time:** 3 min  
**File:** `src/dtx/raft/grpc_tls.cc`

- [ ] Replace the fallback `return InsecureServerCredentials()` at the end of `CreateServerCredentialsFromEnv` (line 169).

**Before:**
```cpp
  return InsecureServerCredentials();
}
```

**After:**
```cpp
  return Status::InvalidArgument(
      "CEDAR_GRPC_TLS_ENABLED is not set to 1. TLS is mandatory. "
      "Insecure credentials are not allowed.");
}
```

- [ ] Replace the fallback `return InsecureChannelCredentials()` at the end of `CreateClientCredentialsFromEnv` (line 189).

**Before:**
```cpp
  return InsecureChannelCredentials();
}
```

**After:**
```cpp
  return Status::InvalidArgument(
      "CEDAR_GRPC_TLS_ENABLED is not set to 1. TLS is mandatory. "
      "Insecure credentials are not allowed.");
}
```

- [ ] Update `ValidateConfig` to reject disabled TLS (line 123–127).

**Before:**
```cpp
  if (!config.enabled) {
    return true;  // TLS disabled is valid
  }
```

**After:**
```cpp
  if (!config.enabled) {
    if (error_msg) {
      *error_msg = "TLS must be enabled; insecure mode is not allowed";
    }
    return false;
  }
```

**Compile check:**
```bash
cd <repo-root>/build && \
  cmake --build . --target cedar -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target cedar`

---

### Step 3.3 — Update existing TLS tests to expect hard failure  
**Time:** 4 min  
**File:** `tests/dtx/test_tls_fail_secure.cc`

- [ ] Rewrite the two tests that currently expect insecure success to expect failure.

**Replace the entire file contents** with:

```cpp
// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include <gtest/gtest.h>
#include "cedar/dtx/raft/grpc_tls.h"

using cedar::dtx::raft::TlsConfig;
using cedar::dtx::raft::TlsCredentialFactory;

TEST(TlsCredentialFactory, MissingServerCertReturnsError) {
  TlsConfig config;
  config.enabled = true;
  config.server_cert_file = "/nonexistent/cert.pem";
  config.server_key_file = "/nonexistent/key.pem";
  auto result = TlsCredentialFactory::CreateServerCredentials(config);
  EXPECT_FALSE(result.ok());
}

TEST(TlsCredentialFactory, DisabledReturnsError) {
  TlsConfig config;
  config.enabled = false;
  auto result = TlsCredentialFactory::CreateServerCredentials(config);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.status().ToString().find("mandatory"), std::string::npos);
}

TEST(TlsCredentialFactory, MissingClientCertReturnsError) {
  TlsConfig config;
  config.enabled = true;
  config.mtls_enabled = true;
  config.client_cert_file = "/nonexistent/client_cert.pem";
  config.client_key_file = "/nonexistent/client_key.pem";
  auto result = TlsCredentialFactory::CreateClientCredentials(config);
  EXPECT_FALSE(result.ok());
}

TEST(TlsCredentialFactory, DisabledClientReturnsError) {
  TlsConfig config;
  config.enabled = false;
  auto result = TlsCredentialFactory::CreateClientCredentials(config);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.status().ToString().find("mandatory"), std::string::npos);
}

TEST(TlsCredentialFactory, ValidateConfigRejectsDisabled) {
  TlsConfig config;
  config.enabled = false;
  std::string err;
  EXPECT_FALSE(TlsCredentialFactory::ValidateConfig(config, &err));
  EXPECT_NE(err.find("insecure"), std::string::npos);
}

TEST(TlsCredentialFactory, EnvUnsetReturnsError) {
  unsetenv("CEDAR_GRPC_TLS_ENABLED");
  auto srv = TlsCredentialFactory::CreateServerCredentialsFromEnv();
  EXPECT_FALSE(srv.ok());
  EXPECT_NE(srv.status().ToString().find("CEDAR_GRPC_TLS_ENABLED"), std::string::npos);

  auto cli = TlsCredentialFactory::CreateClientCredentialsFromEnv();
  EXPECT_FALSE(cli.ok());
  EXPECT_NE(cli.status().ToString().find("CEDAR_GRPC_TLS_ENABLED"), std::string::npos);
}
```

**Run the test (TDD — expect pass after the impl change):**
```bash
cd <repo-root>/build && \
  cmake --build . --target test_tls_fail_secure -- -j$(sysctl -n hw.ncpu) && \
  ./tests/test_tls_fail_secure
```
**Expected output:**
```
[==========] Running 6 tests from 1 test suite
[----------] Global test environment set-up.
[----------] 6 tests from TlsCredentialFactory
[ RUN      ] TlsCredentialFactory.MissingServerCertReturnsError
[       OK ] TlsCredentialFactory.MissingServerCertReturnsError (0 ms)
[ RUN      ] TlsCredentialFactory.DisabledReturnsError
[       OK ] TlsCredentialFactory.DisabledReturnsError (0 ms)
[ RUN      ] TlsCredentialFactory.MissingClientCertReturnsError
[       OK ] TlsCredentialFactory.MissingClientCertReturnsError (0 ms)
[ RUN      ] TlsCredentialFactory.DisabledClientReturnsError
[       OK ] TlsCredentialFactory.DisabledClientReturnsError (0 ms)
[ RUN      ] TlsCredentialFactory.ValidateConfigRejectsDisabled
[       OK ] TlsCredentialFactory.ValidateConfigRejectsDisabled (0 ms)
[ RUN      ] TlsCredentialFactory.EnvUnsetReturnsError
[       OK ] TlsCredentialFactory.EnvUnsetReturnsError (0 ms)
[----------] 6 tests from TlsCredentialFactory (2 ms total)

[==========] 6 tests from 1 test suite ran. (2 ms total)
[  PASSED  ] 6 tests.
```

**Commit:**
```bash
cd <repo-root> && \
  git add src/dtx/raft/grpc_tls.cc tests/dtx/test_tls_fail_secure.cc && \
  git commit -m "P0-3: remove insecure TLS fallback; fail hard on missing credentials"
```

---

## Phase P0-4: Client-Side TLS Enforcement

### Step 4.1 — Replace hard-coded insecure creds in `meta_client.cpp`  
**Time:** 3 min  
**File:** `src/queryd/meta_client.cpp`

- [ ] Replace the `Init()` body (lines 76–82).

**Before:**
```cpp
Status QueryMetaClient::Init() {
  // Create gRPC channel
  channel_ = grpc::CreateChannel(
      options_.meta_service_address,
      grpc::InsecureChannelCredentials());
```

**After:**
```cpp
Status QueryMetaClient::Init() {
  // Create gRPC channel with mandatory TLS
  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
  if (!creds.ok()) {
    return Status::IOError(
        "Failed to create TLS credentials for MetaD: " + creds.status().ToString());
  }
  channel_ = grpc::CreateChannel(options_.meta_service_address, creds.ValueOrDie());
```

**Compile check:**
```bash
cd <repo-root>/build && \
  cmake --build . --target cedar_queryd -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target cedar_queryd`

---

### Step 4.2 — Replace hard-coded insecure creds in `query_storage_client.cpp`  
**Time:** 3 min  
**File:** `src/queryd/query_storage_client.cpp`

- [ ] Replace the channel creation in `GetOrCreateChannel` (lines 829).

**Before:**
```cpp
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
```

**After:**
```cpp
  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
  if (!creds.ok()) {
    std::cerr << "[QueryStorageClient] TLS credential error: "
              << creds.status().ToString() << std::endl;
    return nullptr;
  }
  auto channel = grpc::CreateChannel(address, creds.ValueOrDie());
```

**Compile check:**
```bash
cd <repo-root>/build && \
  cmake --build . --target cedar_queryd -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target cedar_queryd`

**Commit:**
```bash
cd <repo-root> && \
  git add src/queryd/meta_client.cpp src/queryd/query_storage_client.cpp && \
  git commit -m "P0-4: replace hard-coded insecure channels with mandatory TLS factory"
```

---

## Phase P0-5: Tests & Validation

### Step 5.1 — Create `test_storage_service_auth.cc`  
**Time:** 5 min  
**File:** `tests/security/test_storage_service_auth.cc` (new)

- [ ] Write the complete test file:

```cpp
// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "cedar/dtx/security.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::dtx;
using namespace cedar::dtx::security;

class StorageServiceAuthTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize SecurityManager with a test account
    auto* sm = SecurityManager::GetInstance();
    SecurityManager::Config cfg;
    cfg.enable_auth = true;
    cfg.auth.jwt_secret = "test-secret-key-with-at-least-32-bytes!!";
    cfg.auth.accounts.push_back({"admin", "adminpass", {"admin"}});
    auto s = sm->Initialize(cfg);
    EXPECT_TRUE(s.ok()) << s.ToString();

    // Initialize a minimal partition manager
    pm_ = std::make_unique<StoragePartitionManager>();
    StoragePartitionManager::PartitionConfig pcfg;
    pcfg.data_root = "/tmp/cedar_test_storage_auth";
    pcfg.max_partitions = 4;
    s = pm_->Initialize(pcfg);
    EXPECT_TRUE(s.ok()) << s.ToString();
    pm_->AddPartition(0);

    service_ = std::make_unique<StorageServiceImpl>(pm_.get());
  }

  void TearDown() override {
    service_.reset();
    pm_->Shutdown();
    pm_.reset();
    SecurityManager::GetInstance()->Shutdown();
  }

  // Helper to build a ServerContext with (or without) an auth header
  grpc::ServerContext MakeContext(const std::string& bearer_token = "") {
    grpc::ServerContext ctx;
    if (!bearer_token.empty()) {
      ctx.AddInitialMetadata("authorization", "Bearer " + bearer_token);
    }
    return ctx;
  }

  std::string GetValidToken() {
    auto* auth = SecurityManager::GetInstance()->GetAuthenticator();
    auto result = auth->Authenticate("admin", "adminpass");
    EXPECT_TRUE(result.ok());
    return SecurityManager::GetInstance()->GetAuthenticator()->GenerateJWT(result.value());
  }

  std::unique_ptr<StoragePartitionManager> pm_;
  std::unique_ptr<StorageServiceImpl> service_;
};

TEST_F(StorageServiceAuthTest, PutMissingTokenRejected) {
  auto ctx = MakeContext();  // no token
  cedar::storage::PutRequest req;
  cedar::storage::PutResponse resp;
  auto status = service_->Put(&ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, PutInvalidTokenRejected) {
  auto ctx = MakeContext("bad-token");
  cedar::storage::PutRequest req;
  cedar::storage::PutResponse resp;
  auto status = service_->Put(&ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_F(StorageServiceAuthTest, PutValidTokenAccepted) {
  auto ctx = MakeContext(GetValidToken());
  cedar::storage::PutRequest req;
  auto* key = req.mutable_key();
  key->set_entity_id(1);
  key->set_partition_id(0);
  key->set_timestamp(1);
  req.mutable_txn_version()->set_value(1);
  req.set_txn_id(1);
  cedar::storage::PutResponse resp;
  auto status = service_->Put(&ctx, &req, &resp);
  // Auth passes; request may still fail for storage reasons, but NOT auth reasons
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_F(StorageServiceAuthTest, GetMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::GetRequest req;
  cedar::storage::GetResponse resp;
  auto status = service_->Get(&ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, HeartbeatMissingTokenRejected) {
  auto ctx = MakeContext();
  grpc::ServerReaderWriter<cedar::storage::HeartbeatResponse,
                           cedar::storage::HeartbeatRequest> stream(&ctx);
  auto status = service_->Heartbeat(&ctx, &stream);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}
```

---

### Step 5.2 — Create `test_graph_service_router_auth.cc`  
**Time:** 5 min  
**File:** `tests/service/test_graph_service_router_auth.cc` (new)

- [ ] Write the complete test file:

```cpp
// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "cedar/service/graph_service_router.h"
#include "cedar/dtx/security.h"

using namespace cedar;
using cedar::service::GraphServiceRouter;
using cedar::dtx::security::SecurityManager;

class GraphServiceRouterAuthTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto* sm = SecurityManager::GetInstance();
    cedar::dtx::security::SecurityManager::Config cfg;
    cfg.enable_auth = true;
    cfg.auth.jwt_secret = "test-secret-key-with-at-least-32-bytes!!";
    cfg.auth.accounts.push_back({"reader", "readerpass", {"readonly"}});
    auto s = sm->Initialize(cfg);
    EXPECT_TRUE(s.ok()) << s.ToString();

    router_ = std::make_unique<GraphServiceRouter>();
  }

  void TearDown() override {
    router_.reset();
    SecurityManager::GetInstance()->Shutdown();
  }

  grpc::ServerContext MakeContext(const std::string& bearer_token = "") {
    grpc::ServerContext ctx;
    if (!bearer_token.empty()) {
      ctx.AddInitialMetadata("authorization", "Bearer " + bearer_token);
    }
    return ctx;
  }

  std::string GetValidToken() {
    auto* auth = SecurityManager::GetInstance()->GetAuthenticator();
    auto result = auth->Authenticate("reader", "readerpass");
    EXPECT_TRUE(result.ok());
    return auth->GenerateJWT(result.value());
  }

  std::unique_ptr<GraphServiceRouter> router_;
};

TEST_F(GraphServiceRouterAuthTest, ExecuteQueryMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::query::ExecuteQueryRequest req;
  req.set_query("MATCH (n) RETURN n LIMIT 1");
  cedar::query::ExecuteQueryResponse resp;
  auto status = router_->ExecuteQuery(&ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(GraphServiceRouterAuthTest, ExecuteQueryValidTokenAccepted) {
  auto ctx = MakeContext(GetValidToken());
  cedar::query::ExecuteQueryRequest req;
  req.set_query("MATCH (n) RETURN n LIMIT 1");
  cedar::query::ExecuteQueryResponse resp;
  auto status = router_->ExecuteQuery(&ctx, &req, &resp);
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_F(GraphServiceRouterAuthTest, BeginTransactionMissingTokenRejected) {
  auto ctx = MakeContext();
  cedargrpc::BeginTransactionRequest req;
  cedargrpc::Transaction resp;
  auto status = router_->BeginTransaction(&ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(GraphServiceRouterAuthTest, BeginTransactionValidTokenAccepted) {
  auto ctx = MakeContext(GetValidToken());
  cedargrpc::BeginTransactionRequest req;
  cedargrpc::Transaction resp;
  auto status = router_->BeginTransaction(&ctx, &req, &resp);
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}
```

---

### Step 5.3 — Register new tests in `tests/CMakeLists.txt`  
**Time:** 2 min  
**File:** `tests/CMakeLists.txt`

- [ ] Append the following two targets near the other security/service tests (e.g., after `test_security_blockers` and `test_graph_service_router`).

```cmake
# Storage Service Auth Tests
add_executable(test_storage_service_auth
    security/test_storage_service_auth.cc
    ../src/dtx/storage_impl/storage_service_impl.cc
    ${CMAKE_BINARY_DIR}/generated_proto/storage_service.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/storage_service.grpc.pb.cc
)
target_link_libraries(test_storage_service_auth ${CEDAR_TEST_LIBS} cedar_queryd)
gtest_discover_tests(test_storage_service_auth)

# Graph Service Router Auth Tests
add_executable(test_graph_service_router_auth
    service/test_graph_service_router_auth.cc
    ../src/service/graph_service_router.cc
    ${CMAKE_BINARY_DIR}/generated_proto/cedar_graph.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/cedar_graph.grpc.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/query_service.pb.cc
    ${CMAKE_BINARY_DIR}/generated_proto/query_service.grpc.pb.cc
)
target_link_libraries(test_graph_service_router_auth cedar cedar_cypher cedar_queryd gRPC::grpc++ ${GTEST_MAIN_TARGET})
target_link_directories(test_graph_service_router_auth PRIVATE /opt/homebrew/lib)
gtest_discover_tests(test_graph_service_router_auth)
```

**Configure & build:**
```bash
cd <repo-root>/build && \
  cmake .. && \
  cmake --build . --target test_storage_service_auth -- -j$(sysctl -n hw.ncpu) && \
  cmake --build . --target test_graph_service_router_auth -- -j$(sysctl -n hw.ncpu)
```
**Expected output:**
```
[100%] Built target test_storage_service_auth
[100%] Built target test_graph_service_router_auth
```

---

### Step 5.4 — Run all new tests  
**Time:** 3 min

- [ ] Run the auth tests.

```bash
cd <repo-root>/build && \
  ./tests/test_storage_service_auth && \
  ./tests/test_graph_service_router_auth
```

**Expected output:**
```
[==========] Running 5 tests from 1 test suite
[----------] 5 tests from StorageServiceAuthTest
[ RUN      ] StorageServiceAuthTest.PutMissingTokenRejected
[       OK ] StorageServiceAuthTest.PutMissingTokenRejected (0 ms)
[ RUN      ] StorageServiceAuthTest.PutInvalidTokenRejected
[       OK ] StorageServiceAuthTest.PutInvalidTokenRejected (0 ms)
[ RUN      ] StorageServiceAuthTest.PutValidTokenAccepted
[       OK ] StorageServiceAuthTest.PutValidTokenAccepted (1 ms)
[ RUN      ] StorageServiceAuthTest.GetMissingTokenRejected
[       OK ] StorageServiceAuthTest.GetMissingTokenRejected (0 ms)
[ RUN      ] StorageServiceAuthTest.HeartbeatMissingTokenRejected
[       OK ] StorageServiceAuthTest.HeartbeatMissingTokenRejected (0 ms)
[----------] 5 tests from StorageServiceAuthTest (2 ms total)
[==========] 5 tests ran. (2 ms total)
[  PASSED  ] 5 tests.

[==========] Running 4 tests from 1 test suite
[----------] 4 tests from GraphServiceRouterAuthTest
[ RUN      ] GraphServiceRouterAuthTest.ExecuteQueryMissingTokenRejected
[       OK ] GraphServiceRouterAuthTest.ExecuteQueryMissingTokenRejected (0 ms)
[ RUN      ] GraphServiceRouterAuthTest.ExecuteQueryValidTokenAccepted
[       OK ] GraphServiceRouterAuthTest.ExecuteQueryValidTokenAccepted (1 ms)
[ RUN      ] GraphServiceRouterAuthTest.BeginTransactionMissingTokenRejected
[       OK ] GraphServiceRouterAuthTest.BeginTransactionMissingTokenRejected (0 ms)
[ RUN      ] GraphServiceRouterAuthTest.BeginTransactionValidTokenAccepted
[       OK ] GraphServiceRouterAuthTest.BeginTransactionValidTokenAccepted (0 ms)
[----------] 4 tests from GraphServiceRouterAuthTest (2 ms total)
[==========] 4 tests ran. (2 ms total)
[  PASSED  ] 4 tests.
```

**Final commit:**
```bash
cd <repo-root> && \
  git add tests/security/test_storage_service_auth.cc \
          tests/service/test_graph_service_router_auth.cc \
          tests/CMakeLists.txt && \
  git commit -m "P0-5: add auth unit tests for StorageService and GraphServiceRouter"
```

---

## Rollout Checklist

- [ ] All 12+ handlers in `storage_server_with_grpc.cc` have `CheckAuth` as the second statement.
- [ ] All 18+ handlers in `storage_service_impl.cc` have the same `CheckAuth` gate.
- [ ] `GraphServiceRouter::ExecuteQuery` and `BeginTransaction` enforce auth.
- [ ] All other `GraphServiceRouter` handlers (`Traverse`, `TemporalQuery`, `StreamQuery`, `BatchQuery`, `GetSchema`, `Health`, `GetStats`, `Commit`, `Rollback`) enforce auth.
- [ ] `TlsCredentialFactory` never returns insecure credentials.
- [ ] `meta_client.cpp` and `query_storage_client.cpp` never call `InsecureChannelCredentials()`.
- [ ] `test_tls_fail_secure` passes with 6/6 tests.
- [ ] `test_storage_service_auth` passes with 5/5 tests.
- [ ] `test_graph_service_router_auth` passes with 4/4 tests.
- [ ] `storaged`, `graphd`, `cedar`, and `cedar_queryd` targets build cleanly.

---

## Post-Implementation Notes

1. **Performance:** The auth check touches a singleton `SecurityManager` and parses a JWT on every request. If profiling shows >50 µs overhead, cache the parsed `AuthToken` in a thread-local or add a gRPC interceptor with a per-channel auth context. Do **not** optimize prematurely.
2. **Health Endpoints:** `Health` and `Heartbeat` currently require `kMonitor`. If external load balancers cannot pass tokens, downgrade them to optional auth behind a `CEDAR_HEALTH_NO_AUTH` env flag in a follow-up PR.
3. **mTLS vs Token Auth:** This plan adds token-based application auth. mTLS (client certificate verification) is still handled at the TLS layer by `TlsCredentialFactory` and remains independent of the `authorization` header check.
4. **Integration Tests:** The existing `test_storaged_tls_enforced` integration test should be run manually after this plan to confirm the server still starts correctly when `CEDAR_GRPC_TLS_ENABLED=1` and valid cert files are provided.
