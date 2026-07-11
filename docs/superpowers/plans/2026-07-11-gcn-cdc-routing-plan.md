# MetaD GCN Leases And GraphD Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Assign GCN partition leases through MetaD and route GraphD queries only to healthy, version-eligible GCN owners with a semantically complete StorageD fallback.

**Architecture:** MetaD persists GCN registrations and lease epochs through its replicated metadata state machine. GCN heartbeats renew leases and publish per-partition progress; GraphD caches `LocateGcn` results, validates response epoch/version, and falls back immediately when no eligible GCN exists.

**Tech Stack:** C++17, MetaD braft state machine, Protobuf/gRPC, GraphD router, GoogleTest/CTest.

## Global Constraints

- One active GCN owner per partition in the first production version.
- Lease epoch is monotonic; expired owners must stop serving and consuming.
- GraphD starts and remains usable when no GCN is registered.
- GraphD fallback preserves direction, depth, edge-type filters, paths, and served-version semantics.
- GCN lookup and call deadlines remain below the total StorageD fallback budget.

---

### Task 1: Define Replicated GCN Registration And Lease State

**Files:**
- Modify: `include/cedar/dtx/meta_service.h`
- Modify: `src/dtx/meta/meta_service.cc`
- Test: `tests/dtx/test_meta_gcn_lease.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces: `GcnRegistration`, `GcnPartitionProgress`, `GcnLease`, and MetadataService registration/renew/locate methods.

```cpp
struct GcnRegistration {
  uint64_t gcn_id;
  std::string endpoint;
  uint64_t incarnation;
  uint64_t last_heartbeat_ms;
};

struct GcnLease {
  uint32_t partition_id;
  uint64_t gcn_id;
  uint64_t lease_epoch;
  uint64_t expires_at_ms;
};

Status RegisterGcn(const GcnRegistration& registration);
StatusOr<std::vector<GcnLease>> RenewGcnLeases(
    uint64_t gcn_id, uint64_t incarnation,
    const std::vector<GcnPartitionProgress>& progress);
StatusOr<GcnRoute> LocateGcn(uint32_t partition_id, uint64_t required_version) const;
```

- [ ] **Step 1: Write failing state-machine tests**

```cpp
TEST(MetaGcnLeaseTest, AssignsSingleOwnerAndMonotonicEpoch) {
  auto meta = CreateTestMetadataService();
  ASSERT_OK(meta.RegisterGcn({1, "gcn-a:9780", 10, NowMs()}));
  ASSERT_OK(meta.RegisterGcn({2, "gcn-b:9780", 20, NowMs()}));
  ASSERT_OK_AND_ASSIGN(auto first, meta.RenewGcnLeases(1, 10, {}));
  ASSERT_EQ(OwnerFor(first, 3), 1);
  meta.ExpireGcnForTest(1);
  ASSERT_OK_AND_ASSIGN(auto second, meta.RenewGcnLeases(2, 20, {}));
  EXPECT_EQ(OwnerFor(second, 3), 2);
  EXPECT_GT(EpochFor(second, 3), EpochFor(first, 3));
}

TEST(MetaGcnLeaseTest, LocateRequiresAppliedVersionAndLiveLease) {
  auto meta = ReadyMetaWithLease(/*partition=*/3, /*version=*/99);
  EXPECT_TRUE(meta.LocateGcn(3, 99).ok());
  EXPECT_TRUE(meta.LocateGcn(3, 100).status().IsNotFound());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_meta_gcn_lease -j4`

Expected: FAIL because replicated GCN leases are missing.

- [ ] **Step 3: Implement state and deterministic lease assignment**

Add versioned serialization for GCN registrations, progress, leases, and per-partition next epoch. Mutations go through the existing MetaD Raft command path. Use deterministic rendezvous hashing over live GCN ids for initial assignment, retain a live owner to avoid churn, and increment the partition's lease epoch on owner change.

- [ ] **Step 4: Run state-machine and restart tests**

Run: `cmake --build build --target test_meta_gcn_lease test_meta_service_gcn_cache -j4 && ctest --test-dir build --output-on-failure -R 'meta_gcn_lease|meta_service_gcn_cache'`

Expected: all selected tests pass, including metadata serialization/restart.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/dtx/meta_service.h src/dtx/meta/meta_service.cc tests/dtx/test_meta_gcn_lease.cc tests/CMakeLists.txt
git commit -m "feat(meta): persist GCN partition leases"
```

### Task 2: Expose GCN Control-Plane RPCs

**Files:**
- Modify: `proto/meta_service.proto`
- Modify: `include/cedar/dtx/meta_service_grpc.h`
- Modify: `src/dtx/grpc/meta_service_grpc.cc`
- Modify: `include/cedar/gcn/coordinator_client.h`
- Modify: `src/gcn/coordinator_client.cc`
- Test: `tests/dtx/test_meta_gcn_rpc.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: MetadataService APIs from Task 1.
- Produces: `RegisterGcn`, `RenewGcnLeases`, and `LocateGcn` RPCs plus CoordinatorClient wrappers.

- [ ] **Step 1: Write failing loopback gRPC tests**

```cpp
TEST_F(MetaGcnRpcTest, RegistrationAndRenewalReturnLeaseToken) {
  ASSERT_OK(client_->RegisterGcn(7, "gcn-7:9780", 42));
  ASSERT_OK_AND_ASSIGN(auto leases, client_->RenewGcnLeases(7, 42, {}));
  ASSERT_FALSE(leases.empty());
  EXPECT_GT(leases.front().lease_epoch, 0);
  EXPECT_FALSE(leases.front().lease_token.empty());
}

TEST_F(MetaGcnRpcTest, OldIncarnationCannotRenew) {
  ASSERT_OK(client_->RegisterGcn(7, "gcn-7:9780", 43));
  EXPECT_TRUE(client_->RenewGcnLeases(7, 42, {}).status().IsAborted());
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_meta_gcn_rpc -j4`

Expected: FAIL because the new RPCs are missing.

- [ ] **Step 3: Add authenticated RPCs and client wrappers**

Define explicit status enums for stale incarnation, stale lease, no eligible GCN, and not-leader redirect. Reuse existing MetaD client leader retry logic for registration and renewal; do not retry stale-incarnation responses.

- [ ] **Step 4: Run RPC, auth, and shutdown tests**

Run: `cmake --build build --target test_meta_gcn_rpc test_meta_service_node_client_shutdown -j4 && ctest --test-dir build --output-on-failure -R 'meta_gcn_rpc|meta_service_node_client_shutdown'`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add proto/meta_service.proto include/cedar/dtx/meta_service_grpc.h src/dtx/grpc/meta_service_grpc.cc include/cedar/gcn/coordinator_client.h src/gcn/coordinator_client.cc tests/dtx/test_meta_gcn_rpc.cc tests/CMakeLists.txt
git commit -m "feat(meta): expose GCN lease RPCs"
```

### Task 3: Drive GcnNode Consumers From Leases

**Files:**
- Modify: `include/cedar/gcn/gcn_node.h`
- Modify: `src/gcn/gcn_node.cc`
- Modify: `tools/graphcomputenode.cc`
- Test: `tests/gcn/test_gcn_lease_lifecycle.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: CoordinatorClient lease APIs and `PartitionConsumer`.
- Produces: lease-driven consumer start/stop and heartbeat progress.

- [ ] **Step 1: Write failing acquisition, expiry, reassignment, and shutdown tests**

```cpp
TEST_F(GcnLeaseLifecycleTest, StartsAndStopsConsumersWithLeaseSet) {
  meta_->SetLeases({Lease(3, 1), Lease(4, 1)});
  StartNode();
  ASSERT_TRUE(WaitForConsumers({3, 4}));
  meta_->SetLeases({Lease(4, 1)});
  ASSERT_TRUE(WaitForConsumers({4}));
  EXPECT_TRUE(node_->GetPartitionProgress(3).state == ConsumerState::kStopped);
}

TEST_F(GcnLeaseLifecycleTest, StopsServingAfterRenewalDeadline) {
  StartReadyNode();
  meta_->FailRenewals();
  ASSERT_TRUE(WaitForLeaseExpiry());
  EXPECT_FALSE(node_->IsPartitionReady(3));
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_gcn_lease_lifecycle -j4`

Expected: FAIL because GcnNode does not manage leases.

- [ ] **Step 3: Implement registration and lease reconciliation**

Register with a stable node id and fresh process incarnation. Reconcile the returned lease set: start missing consumers, update changed epochs/endpoints, and stop removed consumers before the lease deadline. Heartbeats include each partition's applied offset/version and consumer state.

- [ ] **Step 4: Run lifecycle and consumer tests**

Run: `cmake --build build --target test_gcn_lease_lifecycle test_gcn_cdc_consumer -j4 && ctest --test-dir build --output-on-failure -R 'gcn_lease_lifecycle|gcn_cdc_consumer'`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/gcn/gcn_node.h src/gcn/gcn_node.cc tools/graphcomputenode.cc tests/gcn/test_gcn_lease_lifecycle.cc tests/CMakeLists.txt
git commit -m "feat(gcn): reconcile consumers from MetaD leases"
```

### Task 4: Add Dynamic GraphD Discovery And Version Gating

**Files:**
- Create: `include/cedar/service/gcn_route_cache.h`
- Create: `src/service/gcn_route_cache.cc`
- Modify: `include/cedar/service/graph_service_router.h`
- Modify: `src/service/graph_service_router.cc`
- Modify: `tools/graphd.cc`
- Test: `tests/service/test_graphd_gcn_routing.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: MetaD `LocateGcn` and versioned GCN responses.
- Produces: dynamic route cache, no-GCN fast path, and response validation.

- [ ] **Step 1: Write failing routing tests**

```cpp
TEST_F(GraphdGcnRoutingTest, NoEligibleGcnSkipsGcnRpc) {
  meta_->ReturnNoEligibleGcn();
  storage_->SetTraversalResult(ExpectedPaths());
  auto response = TraverseAtVersion(100);
  EXPECT_TRUE(response.success());
  EXPECT_EQ(gcn_->call_count(), 0);
  EXPECT_EQ(storage_->call_count(), 1);
}

TEST_F(GraphdGcnRoutingTest, RejectsStaleEpochOrVersionAndFallsBack) {
  meta_->ReturnRoute(Route(/*epoch=*/8, /*version=*/100));
  gcn_->ReturnSuccess(/*epoch=*/7, /*served_version=*/99, WrongPaths());
  storage_->SetTraversalResult(ExpectedPaths());
  auto response = TraverseAtVersion(100);
  EXPECT_EQ(response.paths(), ExpectedPaths());
  EXPECT_EQ(storage_->call_count(), 1);
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_graphd_gcn_routing -j4`

Expected: FAIL because GraphD still registers a static GCN address.

- [ ] **Step 3: Implement dynamic lookup and remove the fake default dependency**

Change GraphD's default GCN endpoint to empty. `GcnRouteCache` caches positive routes only until lease expiry and negative lookups for a short bounded interval. Before GCN RPC, validate route version; after RPC, validate response epoch, served version, and cache status. Use a short GCN deadline and preserve remaining time for StorageD fallback.

- [ ] **Step 4: Run GraphD routing tests**

Run: `cmake --build build --target test_graphd_gcn_routing test_graph_service_router -j4 && ctest --test-dir build --output-on-failure -R 'graphd_gcn_routing|graph_service_router'`

Expected: all selected tests pass and no-GCN requests do not incur a GCN timeout.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/service/gcn_route_cache.h src/service/gcn_route_cache.cc include/cedar/service/graph_service_router.h src/service/graph_service_router.cc tools/graphd.cc tests/service/test_graphd_gcn_routing.cc tests/CMakeLists.txt
git commit -m "feat(graphd): discover version-eligible GCN routes"
```

### Task 5: Preserve Complete StorageD Fallback Semantics

**Files:**
- Modify: `proto/storage_service.proto`
- Modify: `tools/storaged.cc`
- Modify: `src/service/graph_service_router.cc`
- Test: `tests/service/test_graphd_storage_traversal_fallback.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: existing query traversal request.
- Produces: StorageD fallback results equivalent to the GCN path for supported traversal semantics.

- [ ] **Step 1: Write failing equivalence tests**

```cpp
TEST_F(GraphdStorageFallbackTest, PreservesDepthDirectionTypesAndPaths) {
  SeedGraphWithIncomingAndOutgoingEdges();
  DisableGcn();
  auto response = Traverse(/*root=*/42, /*depth=*/2,
                           /*direction=*/OUTGOING,
                           /*edge_types=*/{3, 5},
                           /*required_version=*/100);
  ASSERT_TRUE(response.success());
  EXPECT_EQ(Normalize(response.paths()), ExpectedTwoHopPaths());
  EXPECT_EQ(response.served_version(), 100);
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_graphd_storage_traversal_fallback -j4 && ./build/tests/test_graphd_storage_traversal_fallback`

Expected: FAIL because the current fallback returns a count and empty paths.

- [ ] **Step 3: Add a storage traversal RPC or route through distributed execution**

Prefer a dedicated bounded StorageService traversal RPC that accepts root, snapshot/required version, direction, depth, edge types, branch/filter limits, and returns full paths plus served version. Do not emulate multi-hop traversal with one Scan call and discarded items.

- [ ] **Step 4: Run equivalence, query, and auth tests**

Run: `cmake --build build --target test_graphd_storage_traversal_fallback test_graph_service_router_auth -j4 && ctest --test-dir build --output-on-failure -R 'graphd_storage_traversal_fallback|graph_service_router_auth'`

Expected: all selected tests pass for GCN success, no-GCN, GCN RPC failure, version lag, and cache miss.

- [ ] **Step 5: Commit**

```bash
git add proto/storage_service.proto tools/storaged.cc src/service/graph_service_router.cc tests/service/test_graphd_storage_traversal_fallback.cc tests/CMakeLists.txt
git commit -m "fix(graphd): preserve traversal fallback semantics"
```

### Task 6: Export Control-Plane And Routing Metrics

**Files:**
- Modify: `include/cedar/dtx/monitoring.h`
- Modify: `src/dtx/monitoring.cc`
- Modify: `src/dtx/meta/meta_service.cc`
- Modify: `src/gcn/gcn_node.cc`
- Modify: `src/service/graph_service_router.cc`
- Test: `tests/metrics/test_gcn_cdc_metrics.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces: lease, lag, backfill, TMV hit, and GraphD fallback metrics used by deployment alerts.

- [ ] **Step 1: Write failing metric registration and reason-label tests**

```cpp
TEST(GcnCdcMetricsTest, GraphdFallbackReasonsAreDistinctAndBounded) {
  MetricsFixture metrics;
  metrics.RecordGcnFallback(GcnFallbackReason::kVersionLag);
  metrics.RecordGcnFallback(GcnFallbackReason::kRpcFailure);
  EXPECT_EQ(metrics.Counter("cedar_graphd_gcn_fallback_total", "version_lag"), 1);
  EXPECT_EQ(metrics.Counter("cedar_graphd_gcn_fallback_total", "rpc_failure"), 1);
  EXPECT_EQ(metrics.RegisteredFallbackLabelCount(), 6);
}
```

- [ ] **Step 2: Run and verify failure**

Run: `cmake --build build --target test_gcn_cdc_metrics -j4`

Expected: FAIL because metrics are not registered.

- [ ] **Step 3: Register bounded-cardinality metrics**

Export GCN partition lag/checkpoint/applied version/backfill state/duplicates/TMV hit rate; MetaD live GCN and lease reassignment counts; GraphD GCN requests, successes, and fallback totals by a fixed enum reason set. Do not use entity id, transaction id, endpoint, or error text as labels.

- [ ] **Step 4: Run metrics and monitoring tests**

Run: `cmake --build build --target test_gcn_cdc_metrics test_monitoring_shutdown -j4 && ctest --test-dir build --output-on-failure -R 'gcn_cdc_metrics|monitoring_shutdown'`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/dtx/monitoring.h src/dtx/monitoring.cc src/dtx/meta/meta_service.cc src/gcn/gcn_node.cc src/service/graph_service_router.cc tests/metrics/test_gcn_cdc_metrics.cc tests/CMakeLists.txt
git commit -m "feat(metrics): expose GCN CDC health"
```
