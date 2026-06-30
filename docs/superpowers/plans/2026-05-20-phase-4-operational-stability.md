# Phase 4: Operational Readiness & Stability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix signal safety, health-check DoS, config churn, unsafe defaults, and LSM/WAL data races.

**Architecture:** Replace non-async-signal-safe `std::cerr` in signal handlers with atomic flags and `write(2)`. Add a bounded thread pool to the HTTP health endpoint. Add mtime checks to ConfigManager hot-reload. Fix `/tmp` default data directory and query timeout. Patch WAL `current_file_` data race and LSM `ForceFlush`/`Close` race.

**Tech Stack:** C++17, POSIX APIs, CMake, googletest

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/dtx/storage/storage_server_with_grpc.cc` | Storage server signals | Replace std::cerr with write(2) + atomic flag |
| `src/query/cedar_queryd_full.cpp` | QueryD signals | Same |
| `src/governance/health_checker.cc` | Health HTTP server | Thread pool + connection limit |
| `src/governance/config_manager.cc` | Config hot-reload | mtime check before Reload |
| `tools/storaged.cc` | StorageD defaults | `/tmp` → `/var/lib/cedar` |
| `src/query/cedar_queryd_full.cpp` | QueryD defaults | Default timeout 5min → 30s |
| `src/transaction/wal.cc` | WAL writer | Mutex-protect current_file_ access |
| `src/storage/lsm_engine.cc` | LSM engine | Fix ForceFlush/Close race |
| `k8s/*.yaml` | K8s manifests | HTTP probes, NetworkPolicy, PDB |

---

## Task 1: Signal Safety — Replace std::cerr with write(2)

**Files:**
- Modify: `src/dtx/storage/storage_server_with_grpc.cc:51-56`
- Modify: `src/query/cedar_queryd_full.cpp` (find SignalHandler)

---

### Step 4.1.1: Add async-signal-safe signal handler

```cpp
// In src/dtx/storage/storage_server_with_grpc.cc
// BEFORE:
// void SignalHandler(int signal) {
//   if (signal == SIGINT || signal == SIGTERM) {
//     g_running = false;
//   }
// }

// AFTER:
#include <unistd.h>  // for write()

static volatile sig_atomic_t g_signal_received = 0;

void SignalHandler(int signal) {
  // Async-signal-safe: only write to volatile sig_atomic_t.
  // std::cerr, printf, malloc, etc. are NOT async-signal-safe.
  if (signal == SIGINT || signal == SIGTERM) {
    g_signal_received = signal;
    g_running = false;
  }
}
```

In `main()`, after signal handler setup, add a polling loop that logs using `write(2)`:

```cpp
// In main(), after server start, replace any std::cerr inside signal handling loops:
// BEFORE:
//   while (g_running) { ... }

// AFTER:
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (g_signal_received != 0) {
      const char msg[] = "[StorageServer] Shutdown signal received\n";
      write(STDERR_FILENO, msg, sizeof(msg) - 1);
      g_signal_received = 0;
    }
  }
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target storaged -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 4.1.2: Apply same fix to queryd

Find `SignalHandler` in `src/query/cedar_queryd_full.cpp` and apply the same pattern:

```cpp
// Add at file scope:
static volatile sig_atomic_t g_queryd_signal = 0;

// In SignalHandler:
void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    g_queryd_signal = signal;
    g_running = false;
  }
}

// In main loop, log via write(2):
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (g_queryd_signal != 0) {
      const char msg[] = "[QueryD] Shutdown signal received\n";
      write(STDERR_FILENO, msg, sizeof(msg) - 1);
      g_queryd_signal = 0;
    }
  }
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target queryd -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 4.1.3: Commit

```bash
cd <repo-root>
git add src/dtx/storage/storage_server_with_grpc.cc src/query/cedar_queryd_full.cpp
git commit -m "fix(phase4): async-signal-safe signal handlers

- Replaced std::cerr in signal handlers with volatile sig_atomic_t flags
- Logging moved to main loop using POSIX write(2)
- Prevents deadlock/UB inside signal handlers

CRITICAL fix: Cross-cutting signal safety"
```

---

## Task 2: HealthChecker — Bounded Thread Pool

**Files:**
- Modify: `src/governance/health_checker.cc:712-741`
- Modify: `include/cedar/governance/health_checker.h`

---

### Step 4.2.1: Add thread pool and connection limit to health checker

```cpp
// In include/cedar/governance/health_checker.h, add to private members:
  std::unique_ptr<cedar::ThreadPool> http_thread_pool_;
  std::atomic<int> active_http_connections_{0};
  static constexpr int kMaxHttpConnections = 100;
  static constexpr int kHttpThreadPoolSize = 4;
```

In the constructor or initialization:

```cpp
// In health_checker.cc initialization:
  http_thread_pool_ = std::make_unique<cedar::ThreadPool>(kHttpThreadPoolSize);
```

Replace `HttpServerLoop`:

```cpp
// BEFORE:
//  void HttpServerLoop() {
//    while (true) {
//      ...
//      int client_socket = accept(...);
//      std::thread([this, client_socket]() {
//        HandleHttpRequest(client_socket);
//        close(client_socket);
//      }).detach();
//    }
//  }

// AFTER:
  void HttpServerLoop() {
    while (true) {
      {
        std::lock_guard<std::mutex> lock(http_mutex_);
        if (!http_running_) {
          break;
        }
      }

      struct sockaddr_in client_addr;
      socklen_t client_len = sizeof(client_addr);
      int client_socket = accept(http_socket_,
                                 reinterpret_cast<struct sockaddr*>(&client_addr),
                                 &client_len);
      if (client_socket < 0) {
        std::lock_guard<std::mutex> lock(http_mutex_);
        if (!http_running_) break;
        continue;
      }

      // Connection limit: reject if too many active
      int current = active_http_connections_.fetch_add(1);
      if (current >= kMaxHttpConnections) {
        active_http_connections_.fetch_sub(1);
        close(client_socket);
        continue;
      }

      // Set socket timeout to prevent slowloris
      struct timeval tv;
      tv.tv_sec = 5;
      tv.tv_usec = 0;
      setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(client_socket, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

      // Use thread pool instead of unbounded detached threads
      http_thread_pool_->Schedule([this, client_socket]() {
        HandleHttpRequest(client_socket);
        close(client_socket);
        active_http_connections_.fetch_sub(1);
      });
    }
  }
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_governance -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 4.2.2: Commit

```bash
cd <repo-root>
git add src/governance/health_checker.cc include/cedar/governance/health_checker.h
git commit -m "fix(phase4): HealthChecker bounded thread pool + connection limit

- Replaced unbounded detached threads with ThreadPool (size 4)
- Max 100 concurrent HTTP connections
- 5-second socket timeout on each connection
- Prevents DoS via health probe exhaustion

BLOCKER fix: Operational Readiness #1"
```

---

## Task 3: ConfigManager — mtime-Based Hot Reload

**Files:**
- Modify: `src/governance/config_manager.cc:820-844`

---

### Step 4.3.1: Add mtime check to hot-reload loop

```cpp
// In src/governance/config_manager.cc around line 820-844
// BEFORE:
//  impl_->hot_reload_thread_ = std::make_unique<std::thread>([this]() {
//      while (impl_->hot_reload_enabled_) {
//        std::this_thread::sleep_for(...);
//        if (!impl_->source_file_.empty()) {
//          Reload();
//        }
//      }
//  });

// AFTER:
  impl_->hot_reload_last_mtime_ = 0;
  {
    struct stat st;
    if (stat(impl_->source_file_.c_str(), &st) == 0) {
      impl_->hot_reload_last_mtime_ = st.st_mtime;
    }
  }

  impl_->hot_reload_thread_ = std::make_unique<std::thread>([this]() {
    while (impl_->hot_reload_enabled_) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(impl_->hot_reload_interval_ms_));

      if (!impl_->hot_reload_enabled_) {
        break;
      }

      if (!impl_->source_file_.empty()) {
        struct stat st;
        if (stat(impl_->source_file_.c_str(), &st) == 0) {
          if (st.st_mtime != impl_->hot_reload_last_mtime_) {
            impl_->hot_reload_last_mtime_ = st.st_mtime;
            Reload();
          }
        }
      }
    }
  });
```

Add `hot_reload_last_mtime_` to `ConfigManager::Impl` in `include/cedar/governance/config_manager.h`:

```cpp
  time_t hot_reload_last_mtime_{0};
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_governance -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 4.3.2: Commit

```bash
cd <repo-root>
git add src/governance/config_manager.cc include/cedar/governance/config_manager.h
git commit -m "fix(phase4): ConfigManager mtime-based hot reload

- Reload only fires when source file modification time changes
- Eliminates continuous config-churn every 5 seconds
- Falls back to no-op if stat fails

BLOCKER fix: Operational Readiness #2"
```

---

## Task 4: Fix Unsafe Defaults

**Files:**
- Modify: `tools/storaged.cc:77`
- Modify: `src/query/cedar_queryd_full.cpp` (search for timeout default)

---

### Step 4.4.1: Change StorageD default data directory

```cpp
// In tools/storaged.cc around line 77
// BEFORE:
// std::string data_dir = "/tmp/cedar/storaged";

// AFTER:
std::string data_dir = "/var/lib/cedar/storaged";
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target storaged -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 4.4.2: Change query timeout default

Search for query timeout in queryd:

```bash
grep -n "timeout" <repo-root>/src/query/cedar_queryd_full.cpp | head -20
```

If a 5-minute default exists, reduce to 30 seconds:

```cpp
// In src/query/cedar_queryd_full.cpp
// BEFORE:
// constexpr int kDefaultQueryTimeoutMs = 300000;  // 5 minutes

// AFTER:
constexpr int kDefaultQueryTimeoutMs = 30000;  // 30 seconds
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target queryd -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 4.4.3: Commit

```bash
cd <repo-root>
git add tools/storaged.cc src/query/cedar_queryd_full.cpp
git commit -m "fix(phase4): safe defaults

- StorageD default data dir: /tmp/cedar/storaged -> /var/lib/cedar/storaged
- QueryD default timeout: 5min -> 30s

BLOCKER fix: Operational Readiness #5, #10"
```

---

## Task 5: WAL & LSM Race Fixes

**Files:**
- Modify: `src/transaction/wal.cc:304-320`
- Modify: `src/storage/lsm_engine.cc:1292, 1344, 148`

---

### Step 4.5.1: WAL WriteBatch — Synchronize current_file_ access

```cpp
// In src/transaction/wal.cc around line 304-320
// BEFORE:
// Status WalWriter::WriteBatch(const WalBatch& batch) {
//   if (batch.empty()) return Status::OK();
//   if (options_.group_commit_timeout_us > 0) {
//     uint64_t seq;
//     Status s = WriteBatchAsync(batch, &seq);
//     CEDAR_RETURN_IF_ERROR(s);
//     return WaitForSequence(seq);
//   }
//   return WriteInternal(batch);
// }

// AFTER:
Status WalWriter::WriteBatch(const WalBatch& batch) {
  if (batch.empty()) {
    return Status::OK();
  }

  std::lock_guard<std::mutex> lock(file_mutex_);
  if (!current_file_) {
    return Status::IOError("WalWriter", "not opened");
  }

  if (options_.group_commit_timeout_us > 0) {
    uint64_t seq;
    Status s = WriteBatchAsyncLocked(batch, &seq);
    CEDAR_RETURN_IF_ERROR(s);
    return WaitForSequence(seq);
  }
  return WriteInternalLocked(batch);
}
```

Rename `WriteInternal` to `WriteInternalLocked` and `WriteBatchAsync` to `WriteBatchAsyncLocked`, ensuring they are only called while holding `file_mutex_`. Add `file_mutex_` to `WalWriter` in `include/cedar/transaction/wal.h`.

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_transaction -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 4.5.2: LSM ForceFlush — Check shutdown_ before scheduling

```cpp
// In src/storage/lsm_engine.cc around line 1292 (ForceFlush or similar)
// Add at the start of the function:
  if (shutdown_.load()) {
    return Status::InvalidArgument("LsmEngine", "engine is shutting down");
  }
```

Also in `MaybeScheduleFlush`, check `shutdown_` before incrementing counter:

```cpp
// Around line 1336, before active_flush_count_.fetch_add(1):
  if (shutdown_.load() || !opened_) {
    return Status::InvalidArgument("LsmEngine", "engine not open or shutting down");
  }
```

Run:
```bash
cd <repo-root>/build && cmake --build . --target cedar_storage -j$(sysctl -n hw.ncpu)
```
Expected: Build succeeds.

---

### Step 4.5.3: Commit

```bash
cd <repo-root>
git add src/transaction/wal.cc include/cedar/transaction/wal.h src/storage/lsm_engine.cc
git commit -m "fix(phase4): WAL/LSM race fixes

- WalWriter::WriteBatch holds file_mutex_ while accessing current_file_
- LSM ForceFlush and MaybeScheduleFlush check shutdown_ flag
- Prevents UAF and data races during shutdown

CRITICAL fix: Stability #4, #5, #6, #7"
```

---

## Task 6: K8s Manifests — HTTP Probes, NetworkPolicy, PDB

**Files:**
- Modify: `k8s/graphd.yaml`, `k8s/storaged.yaml`, `k8s/metad.yaml`
- Create: `k8s/network-policy.yaml`
- Create: `k8s/pod-disruption-budget.yaml`

---

### Step 4.6.1: Replace TCP probes with HTTP probes

In `k8s/storaged.yaml`, find `livenessProbe` and `readinessProbe`:

```yaml
# BEFORE:
# livenessProbe:
#   tcpSocket:
#     port: 9559

# AFTER:
        livenessProbe:
          httpGet:
            path: /healthz
            port: 9559
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /readyz
            port: 9559
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 3
```

Apply the same pattern to `k8s/graphd.yaml` and `k8s/metad.yaml` (adjust ports).

---

### Step 4.6.2: Add NetworkPolicy

Create `k8s/network-policy.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: cedargraph-network-policy
  namespace: cedargraph
spec:
  podSelector:
    matchLabels:
      app: cedargraph
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: cedargraph
      ports:
        - protocol: TCP
          port: 9559
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: cedargraph
```

---

### Step 4.6.3: Add PodDisruptionBudget

Create `k8s/pod-disruption-budget.yaml`:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cedargraph-pdb
  namespace: cedargraph
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: cedargraph
```

---

### Step 4.6.4: Commit

```bash
cd <repo-root>
git add k8s/
git commit -m "fix(phase4): K8s manifests — HTTP probes, NetworkPolicy, PDB

- Replaced TCP socket probes with HTTP /healthz and /readyz
- Added NetworkPolicy restricting ingress/egress to cedargraph namespace
- Added PodDisruptionBudget with minAvailable: 2

CRITICAL fix: Operational Readiness (K8s defaults)"
```

---

## Task 7: Full Phase 4 Build & Test Verification

---

### Step 4.7.1: Clean rebuild and test

```bash
cd <repo-root>/build
cmake --build . -j$(sysctl -n hw.ncpu)
ctest --output-on-failure
```
Expected: All tests pass.

---

### Step 4.7.2: Commit phase completion

```bash
cd <repo-root>
git tag phase-4-complete
git log --oneline -10
```

---

## Self-Review Checklist

**1. Spec coverage:**
- [x] Signal safety (cross-cutting) — Task 1
- [x] HealthChecker DoS (Operational Readiness #1) — Task 2
- [x] ConfigManager churn (Operational Readiness #2) — Task 3
- [x] Default data dir /tmp (Operational Readiness #5) — Task 4
- [x] Query timeout (CRITICAL #10) — Task 4
- [x] WAL current_file_ race (Stability #4) — Task 5
- [x] LSM ForceFlush race (Stability #5) — Task 5
- [x] K8s probes (CRITICAL #9) — Task 6
- [x] NetworkPolicy/PDB (Operational Readiness helm defaults) — Task 6

**2. Placeholder scan:**
- [x] No TBD/TODO
- [x] All code blocks contain real C++

**3. Type consistency:**
- [x] `g_signal_received` is `volatile sig_atomic_t`
- [x] `active_http_connections_` is `std::atomic<int>`
- [x] `hot_reload_last_mtime_` is `time_t`

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-20-phase-4-operational-stability.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — Fresh subagent per task.

**2. Inline Execution** — Batch execution in this session.

**Which approach?**
