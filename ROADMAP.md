# CedarGraph Roadmap

> This document consolidates the historical development notes previously scattered across `docs/history/` into a single, actionable roadmap.
> 
> **Version**: v2.0.0  
> **Last Updated**: 2026-04-09

---

## 1. Project Overview

CedarGraph is a distributed temporal graph database built in C++17. It combines an LSM-Tree storage engine with native time-indexing, Multi-Raft consensus, and a Cypher-compatible distributed query layer.

### Key Capabilities
- **Temporal Graph Storage**: Every entity is versioned; queries can target `AS OF` or `BETWEEN` specific timestamps.
- **Multi-Raft Consensus**: Each partition runs its own Raft group for strong consistency.
- **Distributed Cypher Query Engine**: `cedar-queryd` parses Cypher, plans distributed execution, and routes requests to storage nodes.
- **Event-Driven Graph Traversal**: Leverages CedarKey physical clustering for efficient neighbor scans.
- **Production Operations**: Prometheus metrics, Grafana dashboards, health checks, and automatic failover.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  cedar-queryd  (Stateless Query Layer)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Cypher Parser│  │ Query Planner│  │ gRPC Service │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
│         └──────────────────┼──────────────────┘                  │
│                            ▼                                    │
│              ┌─────────────────────────┐                        │
│              │   Distributed Executor  │                        │
│              └────────────┬────────────┘                        │
│                           │                                     │
│              ┌────────────┴────────────┐                        │
│              │    Storage Client       │                        │
│              │  (Routing + Conn Pool)  │                        │
│              └─────────────────────────┘                        │
└─────────────────────────────────────────────────────────────────┘
                                    │
                   ┌────────────────┼────────────────┐
                   ▼                ▼                ▼
            ┌──────────┐     ┌──────────┐     ┌──────────┐
            │ cedar-st │     │ cedar-st │     │ cedar-st │
            │ (Shard 0)│     │ (Shard 1)│     │ (Shard N)│
            │ + Raft   │     │ + Raft   │     │ + Raft   │
            └──────────┘     └──────────┘     └──────────┘
                   │                │                │
                   └────────────────┼────────────────┘
                                    ▼
                           ┌──────────────┐
                           │   cedar-md   │
                           │ (Metadata)   │
                           └──────────────┘
```

### Core Components

| Component | Directory / File | Status |
|-----------|------------------|--------|
| Storage Engine | `src/dtx/storage_impl/` | ✅ Complete |
| Distributed Txn | `src/dtx/`, `include/cedar/dtx/` | ✅ Complete |
| Multi-Raft | `src/dtx/storage/raft_replication.cc` | ✅ Complete |
| Query Engine | `src/queryd/`, `src/cypher/` | ✅ Compiles |
| gRPC Services | `*_service.grpc.pb.{h,cc}` | ✅ Complete |
| Metrics & Ops | `src/dtx/storage/metrics_collector.cc` | ✅ Complete |

---

## 3. Completed Milestones

### 3.1 Query Layer (`cedar-queryd`)
- **Cypher Parser** (`cypher::CypherParser`) with AST generation.
- **Distributed Executor** supporting single-partition optimization, cross-partition parallel execution, result merging, and query-plan caching.
- **Storage Client Bridge** (`QueryStorageClient::NodeClient`) enabling per-partition `ScanEntity` calls.
- **gRPC Service** thin wrapper that delegates `ExecuteQuery`, `StreamQuery`, `Traverse`, `TemporalQuery`, `BatchQuery`, and `GetStats` to the executor.

### 3.2 Storage & Raft Fixes
- **CedarKey Consistency**: `Put` and `Get` now consistently use `timestamp().value()` instead of mixing `txn_version`.
- **Column ID Consistency**: `DeserializeDescriptor` accepts and propagates the correct `column_id`.
- **Memtable Flush**: Threshold reduced to 256 KB with WAL enabled; automatic flush every 5 seconds.
- **Batch Neighbor Reads**: `BatchGet` optimization delivering ~10× throughput improvement for neighbor queries.

### 3.3 Bug Fixes (Code Review)
- Removed undefined `FLAGS` references in `cedar_queryd_main.cpp`.
- Fixed `MetaClient` → `QueryMetaClient` class-name mismatch in `meta_client.cpp`.
- Made `SignalHandler` async-signal-safe (only sets a flag).
- Corrected `QueryStorageClient::HealthCheck` logic and `Init` validation.
- Added mutex protection for `CircuitBreaker` concurrent access.

### 3.4 Deployment & Operations
- Docker Compose files for single-node and cluster modes.
- Kubernetes manifests under `k8s/`.
- Prometheus metrics exporter and Grafana dashboards.
- Automated recovery manager and health-monitoring scripts.

---

## 4. Known Issues & Technical Debt

| Issue | Severity | Notes |
|-------|----------|-------|
| `Value::operator+`/`-`/`*`/`/` are stubs | Low | Minimal arithmetic operator implementations in `src/cypher/value.cc`; sufficient for current query coverage but need full Cypher semantics later. |
| `Date::FromYMD` is a stub | Low | Returns `Date(0)`; replace with a proper calendar conversion before supporting date literals extensively. |
| `meta_client.cpp` protobuf drift | Medium | Original code referenced `RegisterServiceRequest` and `ServiceType::QUERYD`; actual generated headers use `RegisterNodeRequest` with no `ServiceType` enum. Needs alignment if dynamic service registration is required. |
| QueryD executable duplicate lib warning | Low | Linker warns about duplicate `libcedar.a`; harmless but should be cleaned up in CMake. |

---

## 5. Next Steps

### Near Term (0–2 months)
1. **End-to-End Query Tests**: Run Cypher queries through `cedar-queryd` against a live storage cluster and validate result correctness.
2. **Protobuf Cleanup**: Reconcile `meta_client.cpp` with the actual `meta_service.pb.h` definitions or regenerate protobufs from a canonical `.proto` source.
3. **Value Operators**: Implement real temporal arithmetic and list/map concatenation per OpenCypher spec.
4. **CMake Hygiene**: Remove duplicate library linkage warnings.

### Medium Term (3–6 months)
5. **Query Optimizer Cost Model**: Add cardinality estimation for time-range predicates and CedarKey clustering.
6. **Prepared Statements**: Extend `QueryPlanCache` to support parameterized Cypher plans.
7. **CI/CD Pipeline**: Complete the GitHub Actions workflow for building, testing, and publishing Docker images.

### Long Term (6+ months)
8. **Federation**: Cross-cluster graph queries via a federated query planner.
9. **Graph Algorithms**: Native implementation of PageRank, shortest-path, and community-detection operators inside the query engine.
10. **Cloud-Native Autoscaling**: Horizontal auto-scaling of `cedar-queryd` based on query latency and queue depth.

---

## 6. Quick Start

```bash
# Build
mkdir build && cd build
cmake ..
make -j$(sysctl -n hw.ncpu)

# Run tests
ctest --output-on-failure

# Docker quick-start (cluster mode)
docker-compose up -d
```

---

## 7. References

- Original design docs: `DISTRIBUTED_ANALYSIS.md`, `MULTI_RAFT_ARCHITECTURE.md`, `MULTI_RAFT_INTEGRATION_GUIDE.md`
- Deployment guide: `DEPLOYMENT_GUIDE.md`
- Temporal index design: `TIME_INDEX_PRODUCTION_READINESS_v2.md`
