# CedarGraph Distributed System — Code Review & Architecture Audit

> **For agentic workers:** This document is a comprehensive production-readiness audit of the CedarGraph distributed layer. It maps architecture, traces read/write paths through every layer, and catalogs findings with exact file paths and line references.

**Goal:** Establish a complete understanding of CedarGraph's distributed architecture, identify production-critical code quality issues, and provide exact file/function references for every subsystem.

**Architecture:** CedarGraph is a distributed temporal graph database using a NebulaGraph-style separation of concerns: stateless query routers (graphd/queryd), consensus metadata (metad), stateful storage nodes (storaged) with Raft replication, a Graph Compute Node (gcn) for analytics, and cross-DC replication.

**Tech Stack:** C++17, gRPC/Protobuf, braft/brpc (Raft), LSM-Tree storage, LZ4 compression, OpenSSL/TLS, CMake

---

## Table of Contents

1. [Overall Architecture](#1-overall-architecture)
2. [Service Topology & Binaries](#2-service-topology--binaries)
3. [Module Decomposition](#3-module-decomposition)
4. [Read Path — End-to-End Trace](#4-read-path--end-to-end-trace)
5. [Write Path & 2PC — End-to-End Trace](#5-write-path--2pc--end-to-end-trace)
6. [Key Data Structures](#6-key-data-structures)
7. [Threading & Concurrency Model](#7-threading--concurrency-model)
8. [Failure Handling Mechanisms](#8-failure-handling-mechanisms)
9. [Performance Characteristics](#9-performance-characteristics)
10. [Code Quality Findings](#10-code-quality-findings)

---

## 1. Overall Architecture

CedarGraph is organized into seven horizontal layers, with clear separation between stateless routing, graph semantics, storage engine, distributed transactions, consensus, and governance.

```
┌─────────────────────────────────────────────────────────┐
│  GraphD / QueryD  (stateless query routers)             │
│  ├─ Cypher parser, planner, executor                    │
│  ├─ DistributedExecutor (partition routing, parallel)   │
│  ├─ Optimized2PCEngine (parallel/pipelined/batched 2PC) │
│  └─ GCN scatter-gather router                           │
├─────────────────────────────────────────────────────────┤
│  Graph Layer (CedarGraph)                               │
│  ├─ Semantic graph API (neighbors, BFS, temporal)       │
│  └─ Cypher integration & TMV engine                     │
├─────────────────────────────────────────────────────────┤
│  DB Layer (CedarGraphDB)                                │
│  ├─ Manifest management                                 │
│  └─ Event bus / backfill                                │
├─────────────────────────────────────────────────────────┤
│  Storage Layer (CedarGraphStorage / LsmEngine)          │
│  ├─ MemTable (CedarMemTable — VSL skiplist)             │
│  ├─ Immutable MemTable                                  │
│  ├─ SST (zone-columnar v1/v2)                           │
│  ├─ Compaction (size-tiered, parallel)                  │
│  ├─ Block Cache / Query Cache / SST Reader Cache        │
│  └─ Blob Storage for large values                       │
├─────────────────────────────────────────────────────────┤
│  Transaction Layer (local)                              │
│  ├─ OCCTransaction                                      │
│  ├─ WAL / WALBatchWriter (group commit)                 │
│  └─ TransactionPool                                     │
├─────────────────────────────────────────────────────────┤
│  DTX Layer (distributed)                                │
│  ├─ Optimized2PCEngine (coordinator)                    │
│  ├─ StorageServiceImpl (participant)                    │
│  ├─ PartitionRaftStateMachine (braft)                   │
│  ├─ Partition Manager / Migrator                        │
│  ├─ CrossDCReplicator (sync/async + reconciliation)     │
│  └─ Failover Manager (Phi Accrual, health checks)       │
├─────────────────────────────────────────────────────────┤
│  Governance                                             │
│  ├─ ServiceRegistry (name-based discovery)              │
│  ├─ ConfigManager (hot reload)                          │
│  ├─ HealthChecker (HTTP /healthz, /readyz)              │
│  └─ MetricsRegistry (Prometheus counters/histograms)    │
├─────────────────────────────────────────────────────────┤
│  Core / Types                                           │
│  ├─ CedarKey (32B fixed, cache-line aligned)            │
│  ├─ Descriptor (inline or blob-backed value)            │
│  ├─ Timestamp (microsecond, descending storage order)   │
│  ├─ Status, Slice, Env, CRC32C                          │
│  └─ ThreadPool, BackgroundWorker                        │
└─────────────────────────────────────────────────────────┘
```

---

## 2. Service Topology & Binaries

### Production Binaries

| Binary | Source | Output | Role | Default Port |
|--------|--------|--------|------|-------------|
| **graphd** | `tools/graphd.cc` | `cedar-graphd` | Stateless Cypher query router | 9669 |
| **queryd** | `src/queryd/cedar_queryd_full.cpp` | `cedar-queryd` | Distributed query execution layer | 9669 |
| **storaged** | `tools/storaged.cc` | `cedar-storaged` | Stateful storage node + 2PC participant | 9779 |
| **metad** | `tools/metad.cc` | `cedar-metad` | Metadata service with braft consensus | 9559 |
| **gcn** | `tools/graphcomputenode.cc` | `graphcomputenode` | Graph compute node (TMV engine) | — |

### Service Interaction

```
[Client] ──gRPC──► [GraphD / QueryD]
                          │
                          ├──gRPC──► [MetaD] (partition map, schema, node registry)
                          │
                          ├──gRPC──► [StorageD-1] ──Raft──► [StorageD-2,3] (replication)
                          ├──gRPC──► [StorageD-N]
                          │
                          └──gRPC──► [GCN] ──scatter/gather──► [GCN peers]
```

- **GraphD** parses Cypher, plans execution, routes to StorageD via cached partition map from MetaD.
- **QueryD** is the newer distributed query layer with `PartitionRouter`, `ParallelExecutor`, `ResultMerger`, and explicit transaction API.
- **StorageD** runs `CedarGraphStorage` (LSM engine), participates in 2PC, replicates partitions via Raft.
- **MetaD** uses braft for consensus over partition assignments, node registration, heartbeat collection.
- **GCN** runs `TMVEngine` with CDC listeners, watermark-based GC, scatter-gather routing.

---

## 3. Module Decomposition

### 3.1 Core & Types (`src/core/`, `src/types/`)

| File | Responsibility |
|------|---------------|
| `include/cedar/types/cedar_key.h` | 32-byte fixed-length key. Sort order: `entity_id ASC → entity_type ASC → column_id ASC → target_id ASC → timestamp DESC → sequence ASC` |
| `include/cedar/types/descriptor.h` | Value payload. Inline ≤4 bytes; larger values go to blob storage |
| `include/cedar/types/timestamp.h` | Microsecond temporal ordering, descending encoding for storage |
| `src/core/status.cc` | Status codes with message handling |
| `src/core/env.cc` | Platform abstraction (files, mmap, threads) |

### 3.2 Storage Engine (`src/storage/`, `src/sst/`)

| File | Responsibility |
|------|---------------|
| `src/storage/cedar_graph_storage.cc` | Public storage API: `Put`, `Get`, `Scan`, `BatchWrite`, `BatchGet`, edge APIs |
| `src/storage/lsm_engine.cc` | LSM-Tree: memtable → imm → SST levels L0–L6, flush, compaction orchestration |
| `src/storage/cedar_memtable.cc` | In-memory write buffer. Dual structure: `std::map<InternalKey, vector<MemTableEntry>>` + version chain doubly-linked list |
| `src/storage/vsl_memtable.h` | Vectorized skiplist wrapper (evolution of CedarMemTable) |
| `src/storage/compaction_engine.cc` | Size-tiered compaction scheduler |
| `src/storage/compaction_merger.cc` | K-way merge with min-heap, deduplication, tombstone dropping |
| `src/sst/zone_columnar_builder.cc` | SST V2 builder: entity-aligned blocks, zone encoding (RLE, delta-delta, dictionary, LZ4) |
| `src/sst/zone_columnar_reader.cc` | SST V2 reader: block cache, predicate pushdown, temporal bloom filter |

### 3.3 Transaction Layer (`src/transaction/`)

| File | Responsibility |
|------|---------------|
| `src/transaction/wal.cc` | Write-ahead log: group commit, fsync, file rotation |
| `src/transaction/occ_transaction.cc` | Optimistic concurrency control: read-set validation, write-set buffering |
| `src/transaction/transaction_pool.cc` | Pooled transaction context reuse |

### 3.4 Distributed Transactions (`src/dtx/`)

| File | Responsibility |
|------|---------------|
| `src/dtx/optimized_2pc_engine.cc` | Coordinator: `Execute2PC`, `ExecuteParallel2PC`, `ExecutePipelined2PC`, `ExecuteBatched2PC`, decision log persistence |
| `src/dtx/storage_impl/storage_service_impl.cc` | Participant gRPC service: `Prepare`, `Commit`, `Abort` handlers |
| `src/dtx/storage_impl/partition_storage.cc` | Per-partition 2PC state machine: OCC validation, locking, WAL |
| `src/dtx/raft/braft_node.cc` | Metadata Raft state machine: snapshot save/load, leadership callbacks |
| `src/dtx/storage/braft_partition_state_machine.cc` | Partition Raft state machine: log apply, snapshot copy |
| `src/dtx/storage/partition_migrator.cc` | Online partition migration: CopyData → CatchUp → SwitchTraffic → Verify |
| `src/dtx/cross_dc_replicator.cc` | Cross-DC replication: sync/async modes, cleanup, reconciliation loop |
| `src/dtx/storage/failover_manager.cc` | Failover controller: health checks, Phi Accrual, leader switching |
| `src/dtx/phi_accrual.cc` | Phi Accrual failure detection: sliding window, normal CDF |

### 3.5 Query & Cypher (`src/cypher/`, `src/queryd/`, `src/query/`)

| File | Responsibility |
|------|---------------|
| `src/cypher/parser.cc` | Recursive-descent Cypher parser |
| `src/cypher/execution_plan.cc` | Physical operator tree builder: NodeScan, Expand, Filter, Sort, Limit, Project |
| `src/cypher/cypher_engine.cc` | Query execution engine with plan cache |
| `src/queryd/distributed_executor.cpp` | PartitionRouter, ParallelExecutor, ResultMerger, single-partition fast path |
| `src/queryd/query_storage_client.cpp` | Storage client with circuit breaker, local vs remote node selection |
| `src/queryd/query_service_full.cpp` | QueryService gRPC implementation |
| `src/query/cedar_scan.cc` | Snapshot-based temporal scan engine |

### 3.6 Graph Layer (`src/graph/`)

| File | Responsibility |
|------|---------------|
| `src/graph/cedar_graph.cc` | Semantic graph API: temporal neighbors, BFS, Allen relations, Cypher integration |
| `src/graph/tmv_engine.cc` | Temporal Multi-Version engine for GCN |

### 3.7 Partition Management (`src/partition/`)

| File | Responsibility |
|------|---------------|
| `src/partition/partition_strategy_manager.cc` | AUTO mode switching between StaticHash and MTHStream |
| `src/partition/static_hash_strategy.cc` | `entity_id % num_partitions` |
| `src/partition/mth_stream_strategy.cc` | Count-Min Sketch based streaming hash |

### 3.8 Governance (`src/governance/`)

| File | Responsibility |
|------|---------------|
| `src/governance/service_registry.cc` | Name-based service discovery with watchers |
| `src/governance/health_checker.cc` | HTTP /healthz /readyz endpoints (raw socket) |
| `src/governance/config_manager.cc` | Hot-reload configuration |

### 3.9 GCN (`src/gcn/`)

| File | Responsibility |
|------|---------------|
| `src/gcn/gcn_node.cc` | GCN process lifecycle |
| `src/gcn/scatter_gather_router.cc` | Sub-query dispatch and result gathering |
| `src/gcn/local_compute_thread.cc` | BFS/DFS traversal execution |
| `src/gcn/event_applier.cc` | Ordered CDC event apply with reorder buffer |
| `src/gcn/watermark_gc.cc` | Watermark-based chunk garbage collection |

---

## 4. Read Path — End-to-End Trace

### 4.1 Client Entry

**File:** `tools/graphd.cc` → `src/service/graph_service_router.cc:137`

```cpp
grpc::Status GraphServiceRouter::ExecuteQuery(
    grpc::ServerContext* context,
    const ExecuteQueryRequest* request,
    ExecuteQueryResponse* response)
```

1. GraphD starts gRPC server on port 9669.
2. Client sends `ExecuteQuery` with Cypher string.
3. Router checks query cache (`query_cache_->Get(cache_key)`). On hit, returns immediately.
4. On miss, calls `ParseQueryForRouting()` (line 178).

### 4.2 Query Parsing & Planning

**File:** `src/service/graph_service_router.cc:870-1083`

5. `ParseQueryForRouting()` invokes `cypher::CypherParser parser(query); auto stmt = parser.ParseStatement();`

**File:** `src/cypher/parser.cc`

6. `CypherParser::ParseStatement()` performs recursive-descent parsing:
   - Extracts temporal clauses.
   - Parses `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `LIMIT`, `CREATE`, `SET`, `DELETE`.
   - Builds AST: `MatchClause`, `WhereClause`, `ReturnClause`, etc.

7. Router classifies query type:
   - `POINT_LOOKUP`: `WHERE id(n) = xxx`
   - `NEIGHBOR_TRAVERSAL`: `()-[]->()` pattern
   - `AGGREGATE`: `count(*)`, `sum()`, etc.
   - `SCAN`: everything else

**File:** `src/cypher/execution_plan.cc:615-748`

8. `ExecutionPlanBuilder::Build()` constructs physical operator tree bottom-up:
   - `MATCH` → `NodeScan` / `TemporalNodeScan`
   - Relationships → `Expand` operators
   - `WHERE` → `Filter`
   - `ORDER BY` → `Sort`
   - `LIMIT` / `SKIP` → `Limit` / `Skip`
   - `RETURN` → `Project` → `Distinct` (if needed) → `ProduceResults`

### 4.3 Partition Routing

**File:** `src/service/graph_service_router.cc:1085-1143`

9. `CalculatePartition(entity_id)`:
   - If `partition_strategy_` active, calls `PartitionStrategyManager::ComputePartition(key, kNumPartitions)`.
   - Fallback: `entity_id % 32768`.

**File:** `src/partition/partition_strategy_manager.cc`

10. `PartitionStrategyManager::RouteVertex(entity_id)` delegates to active `IPartitionStrategy`:
    - `STATIC_HASH`: `entity_id % num_partitions`
    - `MTH_STREAM`: Count-Min Sketch based routing
    - `AUTO`: switches based on temporal query ratio (> 0.5) and locality ratio (> 0.7)

**File:** `src/queryd/meta_client.cpp:257-323`

11. `QueryMetaClient::FetchClusterStateFromMeta()`:
    - gRPC: `MetaService::GetSpacePartitionMap("default")`
    - Also `GetAliveNodes()` for address resolution.
    - Cached in `cached_cluster_state_`, refreshed every 30s.

**File:** `src/service/graph_service_router.cc:1101-1143`

12. `GetPartitionRoute()`:
    - Checks `partition_cache_` under `partition_map_mutex_` (shared_lock).
    - On miss: `meta_client_->GetPartitionAssignment("default", partition_id)`.
    - Resolves leader/replica addresses.
    - Caches `PartitionRoute`.

### 4.4 Distributed Execution

**File:** `src/queryd/distributed_executor.cpp:505-598`

13. `DistributedExecutor::Execute()`:
    - Parses and validates query.
    - **Single-Partition Check:** `IsSinglePartitionQuery()` extracts entity IDs. If all map to same partition, calls `ExecuteSinglePartition()`.
    - Otherwise, `ExecuteCrossPartition()`.

**File:** `src/queryd/distributed_executor.cpp:930-963`

14. `ExecuteSinglePartition()`:
    - Leader check: `router_->CheckIsLeader(partition_id, leader_address)`.
    - Gets `NodeClient` via `storage_client_->GetNodeClient(partition_id)`.
    - `node_client->ExecuteSubQuery(query, parameters, result)`.

**File:** `src/queryd/distributed_executor.cpp:965-1006`

15. `ExecuteCrossPartition()`:
    - `SplitQuery()` creates one `SubQueryTask` per partition.
    - `parallel_executor_->ExecuteParallel(tasks, storage_client_, ctx)`.

**File:** `src/queryd/distributed_executor.cpp:170-225`

16. `ParallelExecutor::ExecuteParallel()`:
    - Submits each task to worker thread pool.
    - Each: `storage_client_->GetNodeClient(t.partition_id)->ExecuteSubQuery(...)`.
    - Collects results via `std::promise<void>` / `std::future`.

**File:** `src/queryd/distributed_executor.cpp:308-468`

17. `ResultMerger::Merge()`:
    - Concatenates records from all `SubQueryResult`s.
    - Stable sort if `sort_keys` provided.
    - `MergeAggregate()` handles `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with group-by.

### 4.5 Storage Layer Read

**File:** `src/storage/cedar_graph_storage.cc:389-445`

18. `CedarGraphStorage::Get(entity_id, entity_type, column_id, timestamp)`:
    - Distributed mode: `rep_->dtx_client->Get(key, timestamp)` (RPC).
    - Local mode: `rep_->engine->GetAtTime(entity_id, entity_type, column_id, timestamp)`.

**File:** `src/storage/lsm_engine.cc:459-618`

19. `LsmEngine::GetAtTime()`:
    1. **Query Cache Check:** `query_cache_->Get(entity_id, column_id, timestamp.value())`.
    2. **MemTable (hot):** `mem_->GetAtTime(...)`.
    3. **Immutable MemTable:** `imm_->GetAtTime(...)` if exists.
    4. **Accumulated Buffer:** `QueryAccumulatedBuffer(...)`.
    5. **SST Files (cold):**
       - `compaction_engine_->GetFilesForEntity()` → candidate files.
       - **Temporal Bloom Filter** check to skip irrelevant files.
       - `SstReaderCache` reuse.
       - `reader->GetRange(...)` → scan matching entries.
       - Select **latest version with `ts <= timestamp`**.

**Temporal Version Resolution:**
- CedarKey stores timestamps in **descending order**.
- `GetAtTime`: scans all versions, picks `max(ts) where ts <= query_timestamp`.
- `GetRecordAtTime`: sorts descending, picks first `<= timestamp`.
- Tombstones (`Descriptor::IsTombstone()`) return `std::nullopt`.

### 4.6 Response Path

**File:** `src/queryd/query_service_full.cpp:189-226`

20. `QueryServiceImpl::ExecuteQuery()` builds gRPC response:
    - Converts `cypher::ResultSet` → proto `ResultSet` (columns + rows).
    - Populates stats: `execution_time_us`, `rows_scanned`, `rows_returned`, `storage_nodes_accessed`, `network_roundtrips`.

**File:** `src/service/graph_service_router.cc:137-483`

21. `GraphServiceRouter::ExecuteQuery()` post-processing:
    - Aggregation across partition results if `has_aggregate`.
    - ORDER BY, LIMIT, SKIP slicing.
    - Query cache insertion: `query_cache_->Put(cache_key, response->result_set())`.
    - Returns `ExecuteQueryResponse`.

---

## 5. Write Path & 2PC — End-to-End Trace

### 5.1 Write Entry

**File:** `src/service/graph_service_router.cc:137-242`

1. `GraphServiceRouter::ExecuteQuery()` detects write via `IsWriteQuery()`.
   - **Explicit transaction:** accumulates keys into `active_transactions_[txn_id].write_set`, defers commit.
   - **Autocommit:** allocates fresh `TxnID`, immediately calls `two_pc_engine_->Execute2PC(txn_id, read_set, write_set, Timestamp(now_ts))`.

**File:** `src/queryd/query_service_full.cpp:68-227`

2. `QueryServiceImpl::ExecuteQuery()` dispatches to `DistributedExecutor::Execute()`.
   - Write fragments reach `StorageServiceImpl` gRPC handlers.

### 5.2 Distributed Transaction Coordinator

**File:** `src/dtx/optimized_2pc_engine.cc:196-281`

3. `Optimized2PCEngine::Execute2PC(txn_id, read_set, write_set, commit_ts)`:
   - Creates `TransactionContext` with atomic state machine:
     ```cpp
     enum class State { kInit, kPreparing, kPrepared, kCommitting,
                        kCommitted, kAborting, kAborted };
     ```
   - Registers with `TransactionTimeoutManager`.
   - Persists participant list: `state_manager_->CreateTransaction(txn_id, pids)`.
   - Selects strategy: `Sequential`, `Parallel`, `Pipelined`, `Batched`, `Hybrid`.
   - Default hot path: `ExecuteParallel2PC()`.

**File:** `src/dtx/optimized_2pc_engine.cc:1148-1215`

4. Participant determination:
   - `GetParticipants(write_set)`: reads `key.part_id()` for each key.
   - Fallback: `entity_id % clients_.size()` if `part_id == 0`.
   - Deduplicates and sorts `PartitionID`s.

### 5.3 Phase 1: Prepare

**File:** `src/dtx/optimized_2pc_engine.cc:619-701`

5. `ExecuteParallel2PC()`:
   - State → `kPreparing`.
   - Spawns one `thread_pool_->Schedule` task per participant.
   - Each: `client->Prepare(ctx->txn_id, ctx->read_set, ctx->write_set, ctx->commit_ts)`.
   - `prepare_acks` / `prepare_nacks` updated atomically.
   - `WaitForPrepareQuorum()`: **requires ALL participants** (`required_successes = total`).
   - Any NACK → abort path.

**CAS Timeout Protection** (pipelined/batched variants, lines 386-388, 831-832):
```cpp
auto expected = TransactionContext::State::kInit;
if (ctx->state.compare_exchange_strong(expected,
                                       TransactionContext::State::kAborted)) {
    state_manager_->UpdateState(ctx->txn_id, TxnState::kAborted);
}
```

**File:** `src/dtx/storage_impl/storage_service_impl.cc:793-908`

6. `StorageServiceImpl::Prepare()`:
   - Deserializes read/write sets.
   - Per-partition:
     - **Raft path:** verifies `raft_group->IsLeader()`, proposes `StorageLogEntry::Type::kPrepare`.
     - **Direct path:** `partition->Prepare(...)`.
   - Any failure → rolls back already-prepared partitions.

**File:** `src/dtx/storage_impl/partition_storage.cc:117-188`

7. `PartitionStorage::Prepare()`:
   - Duplicate check: rejects if `prepared_txns_` already contains `txn_id`.
   - **Write-write conflict detection:** iterates existing `PreparedTxnState`; same `(entity_id, column_id)` → `Status::Busy`.
   - **Read-set validation:** queries `LsmEngine` for read key versions. If version with `txn_version > commit_ts` exists → `Status::Busy`.
   - Registers `PreparedTxnState` with `status = kPrepared`.
   - WAL durability: `WriteTxnWAL(txn_id, "PREPARE")`.

### 5.4 Decision Log Persistence

**File:** `src/dtx/optimized_2pc_engine.cc:409-456`

8. `PersistCommitDecision(decision)`:
   - Called **before** broadcasting Commit.
   - Writes binary decision log to `<decision_log_dir>/txn_{txn_id}.decision`:
     - Magic `0x44454301`, version `1`
     - `txn_id`, `commit_ts`, participant count, participant list
   - `fsync` for crash-safe durability.
   - If persistence fails → aborts all participants.

### 5.5 Phase 2: Commit

**File:** `src/dtx/optimized_2pc_engine.cc:731-797`

9. Parallel commit broadcast:
   - State → `kCommitting`.
   - Spawns parallel `Commit` RPC tasks.
   - `WaitForCommitQuorum()`: again requires ALL commits.

**Success path:**
- State → `kCommitted`.
- `state_manager_->UpdateState(txn_id, TxnState::kCommitted)`.

**Incomplete commit path:**
- State stays `kCommitting`.
- `recovery_manager_->StartRecovery(txn_id)` reads decision log and drives remaining participants.

**File:** `src/dtx/storage_impl/storage_service_impl.cc:923-1030`

10. `StorageServiceImpl::Commit()`:
    - Looks up involved partitions.
    - Per-partition: proposes `kCommit` through Raft or calls `partition->Commit()` directly.
    - Partial failure → rolls back already-committed partitions.

**File:** `src/dtx/storage_impl/partition_storage.cc:191-257`

11. `PartitionStorage::Commit()`:
    - Validates txn is `kPrepared`.
    - For each write key: `shared_storage_->Put(entity_id, timestamp, desc, commit_ts)`.
    - Partial write → `kCommitting` + `IOError`.
    - Full success → `kCommitted`, `WriteTxnWAL(txn_id, "COMMIT")`, erases from `prepared_txns_`.

### 5.6 Storage Layer Write

**File:** `src/storage/cedar_graph_storage.cc:312-343`

12. `CedarGraphStorage::Put()`:
    - Computes partition ID.
    - Constructs `CedarKey`.
    - Distributed: RPC via `dtx_client->Put()`.
    - Local: `rep_->engine->Put(key, descriptor, txn_version)`.

**File:** `src/storage/lsm_engine.cc:228-263`

13. `LsmEngine::Put()`:
    1. Acquire `unique_lock` on `mutex_`.
    2. **WAL write first:** `wal_writer_->WritePut(key, descriptor, txn_version)`.
    3. **MemTable insert:** `mem_->Put(key, descriptor, txn_version)`.
    4. Cache invalidation.
    5. **Auto-flush:** if `mem_->ApproximateMemoryUsage() >= threshold`, unlock, `MaybeScheduleFlush()`.

**File:** `src/transaction/wal.cc`

14. `WalWriter::WritePut()`:
    - Builds `WalBatch` with `WalRecordType::kPut`.
    - Group commit: enqueues to `commit_queue_`, waits on `future`.
    - `WriteInternal()`: encodes `WalRecordHeader` (crc32, type, flags, length, sequence), appends batch.
    - `GroupCommitThread`: drains queue, writes batches, **`fsync`** once per group.

**File:** `src/storage/lsm_engine.cc:1344-1409`

15. `MaybeScheduleFlush()`:
    - Moves `mem_` → `imm_`, creates new `mem_`.
    - `std::async` launches `FlushMemTable(imm)`.

**File:** `src/storage/lsm_engine.cc:2162-2312`

16. `FlushMemTable()` / `FlushEntityGroup()`:
    - Traverses immutable memtable.
    - Sorts entries by `LessForSorting` (timestamp descending).
    - Creates `.sst` via `SstBuilderFactory::Create`.
    - Builds `SSTFileMeta`, inserts into `levels_[0]`.
    - Notifies compaction engine.

### 5.7 Raft Replication

**File:** `src/dtx/raft/braft_node.cc:68-292`

17. **Metadata Raft — `MetaRaftStateMachine`:**
    - `on_apply`: deserializes `RaftCommand`, calls `meta_service_->ApplyRaftCommand()`.
    - `on_snapshot_save`: atomic write (temp → fsync → rename).
    - `on_snapshot_load`: reads `meta_snapshot.bin`, deserializes.
    - `on_leader_start` / `on_leader_stop`: notifies metadata service.

**File:** `src/dtx/storage/braft_partition_state_machine.cc:74-250`

18. **Partition Raft — `PartitionRaftStateMachine`:**
    - `on_apply`: deserializes `StorageRaftCommand` (49-byte: `[type:1][key:32][desc:8][txn_version:8]`).
      - `kPut` → `storage_->Put(key, desc, txn_version)`
      - `kDelete` → `storage_->Put(key, Descriptor(), txn_version)`
    - `on_snapshot_save`: force-flush, copy data directory, serialize prepared txns.
    - `on_snapshot_load`: restore data + prepared txns.
    - Leader enforcement: non-leaders return `UNAVAILABLE` with redirect.

### 5.8 Cross-DC Replication

**File:** `src/dtx/cross_dc_replicator.cc:155-202`

19. `CrossDCReplicator::Replicate()`:
    - Builds `ReplicationLog`.
    - **Sync mode:** iterates `peer_dcs_` sequentially. On first failure:
      - Best-effort cleanup: `DeleteFromDC(key, succ_dc)` for succeeded DCs.
      - Cleanup failure → enqueue to `reconciliation_queue_`.
    - **Async mode:** enqueues to `replication_queue_`, returns immediately.

**File:** `src/dtx/cross_dc_replicator.cc:555-631`

20. `ReconciliationLoop()`:
    - Drains up to 64 entries per second.
    - `ReconcileKey(key, dc_id)`:
      - Queries local authoritative storage for latest value.
      - If exists → replicates current value.
      - If deleted → sends tombstone.
      - Failure → re-queues with 5s backoff.

### 5.9 Response Path

**File:** `src/service/graph_service_router.cc:233-242`

21. On `Execute2PC` success:
    - `response->set_success(true)`.
    - `result_set.set_total_rows(...)`.
    - On failure: `stats_.failed_queries++`, `response->set_success(false)`, `response->set_error_msg(...)`.

---

## 6. Key Data Structures

### 6.1 CedarKey
**File:** `include/cedar/types/cedar_key.h`

32-byte fixed-length, cache-line aligned (64B):
```
Offset 0-7:   entity_id (big-endian)
Offset 8-15:  timestamp_be (descending big-endian)
Offset 16-23: target_id
Offset 24-25: column_id / edge_type
Offset 26-27: sequence
Offset 28:     entity_type (0=Vertex, 1=EdgeOut, 2=EdgeIn)
Offset 29:     flags (op type, distributed, compressed, tombstone, lock)
Offset 30-31:  part_id
```

### 6.2 Descriptor
**File:** `include/cedar/types/descriptor.h`

Value payload. Inline small values (≤4 bytes) directly; larger values go to auto blob storage.

### 6.3 TransactionContext
**File:** `include/cedar/dtx/optimized_2pc_engine.h:55`

```cpp
enum class State { kInit, kPreparing, kPrepared, kCommitting,
                   kCommitted, kAborting, kAborted };
std::atomic<State> state{State::kInit};
std::atomic<int> prepare_acks{0};
std::atomic<int> prepare_nacks{0};
std::atomic<int> commit_acks{0};
std::atomic<int> abort_acks{0};
std::shared_ptr<std::promise<void>> done_promise;
```

### 6.4 PartitionState
**File:** `include/cedar/dtx/failover_manager.h`

```cpp
struct PartitionState {
  PartitionID pid;
  NodeID current_leader;
  std::vector<NodeID> replicas;
  bool is_failover_in_progress = false;
  NodeID failover_target = 0;
  std::chrono::steady_clock::time_point last_failover;
};
```

### 6.5 HealthScore
**File:** `include/cedar/dtx/failover_manager.h`

Six-dimensional weighted score:
- TCP latency: 25%
- Raft lag: 20%
- Disk I/O: 15%
- Memory: 15%
- CPU: 15%
- Error rate: 10%

---

## 7. Threading & Concurrency Model

### 7.1 Storage Engine
- `LsmEngine`: single `std::shared_mutex`. Writes take `unique_lock`; reads take `shared_lock`.
- `CedarMemTable`: single `std::shared_mutex`. `Put` takes `unique_lock` for entire duration.
- Background threads: `bg_thread_` (manual delete), `auto_compaction_thread_` (manual delete).
- `SizeTieredCompactionEngine`: own `levels_mutex_`, task queue + `condition_variable`, 2 compaction threads.

### 7.2 2PC Engine
- `Optimized2PCEngine`: `pipeline_thread_`, `batch_thread_`, `tuning_thread_`, worker thread pool.
- `thread_pool_->Schedule()` dispatches parallel Prepare/Commit RPCs.
- Timeout manager: background thread scanning registered transactions.

### 7.3 Failover Controller
- Two dedicated threads: `lease_thread_`, `health_thread_`.
- Bounded worker pool (`cedar::ThreadPool`, max 16) for failover execution.
- **11 mutexes** in `PartitionFailoverController`:
  `partitions_mutex_`, `callbacks_mutex_`, `route_mutex_`, `consensus_callback_mutex_`,
  `replica_health_mutex_`, `health_scores_mutex_`, `score_history_mutex_`,
  `degraded_nodes_mutex_`, `phi_detectors_mutex_`, `collectors_mutex_`,
  `health_probe_callback_mutex_`, `node_addresses_mutex_`.

### 7.4 Cross-DC Replicator
- `ReplicationLoop` thread (async mode).
- `ReconciliationLoop` thread (always running).
- `reconciliation_mutex_` protects queue.

### 7.5 QueryD
- `ParallelExecutor`: submits sub-queries to worker thread pool.
- `ResultMerger`: merges results from multiple threads.

---

## 8. Failure Handling Mechanisms

### 8.1 Defensive Coding
- Extensive `try { ... } catch (...) { std::cerr << ... }` at thread boundaries. Prevents crashes but masks bugs.

### 8.2 2PC Failure Modes
- **Prepare failure:** Abort all participants, state → `kAborted`.
- **Decision log failure:** Abort all participants, return `IOError`.
- **Partial commit:** State stays `kCommitting`, recovery manager completes remaining participants.
- **Timeout:** CAS from `kInit` → `kAborted`. If CAS fails (worker already started), wait for accurate result.

### 8.3 Raft Failure Modes
- Non-leader storage nodes return `UNAVAILABLE` with redirect address.
- `on_apply` errors call `iter.set_error_and_rollback()` and step down.
- Snapshot I/O uses EINTR retry loops, fsync, atomic rename.

### 8.4 Failover
- **Phi Accrual:** `phi = -log10(1 - CDF(silence))`. Threshold-based suspicion.
- **Trend detection:** Last 3 scores monotonically decreasing + overall < 60 → degraded.
- **Leader lease expiry:** triggers automatic failover.
- **Maintenance mode:** nodes in `maintenance_nodes_` skipped by recovery.
- **Bounded worker pool:** prevents unbounded thread spawning during cascading failures.

### 8.5 Cross-DC
- Sync replication: best-effort cleanup on failure → reconciliation queue.
- Async replication: exponential backoff retry (max 2^6 = 64x).
- Reconciliation: queries authoritative local state, replicates current value or tombstone.

### 8.6 Health Checking
- Raw BSD socket HTTP endpoint with 5s timeout.
- Bounded thread pool (4 threads) for HTTP requests.
- Connection limit: 100 active connections.

---

## 9. Performance Characteristics

### 9.1 Read Path
- **Cache layers:** Query result cache → Plan cache → LSM query cache → Cross-query cache → SST reader cache.
- **Temporal resolution:** Descending timestamp encoding enables efficient latest-version selection without full sort in some paths.
- **Read amplification:** LSM queries may open multiple SST files. No block-level bloom filter in current reader — point queries can degrade to scanning many blocks.
- **MemTable iterator:** Copies entire `map_` under `shared_lock`. O(N) memory spike at creation.

### 9.2 Write Path
- **WAL first:** Durability before memtable modification.
- **Group commit:** `fsync` once per batch. Reduces write amplification.
- **Write amplification:** Size-tiered compaction with 4× growth ratio yields ~3–5×.
- **MemTable flush:** Creates many small files if not accumulated.

### 9.3 2PC
- **All-or-nothing quorum:** CedarGraph requires ALL participants to prepare and commit. No partial acceptance.
- **Parallel RPC:** Both prepare and commit phases spawn parallel tasks via thread pool.
- **Pipelined mode:** Batches transactions through a pipeline queue with worker loop.

### 9.4 Partition Migration
- **Blocking drain:** `SwitchTraffic` sleeps for `2 × rpc_timeout_ms` rather than active drain protocol.
- **WAL replay:** One RPC per WAL entry. No batching.
- **Snapshot streaming:** 64KB chunks, each as separate gRPC Write() call without flow control.

### 9.5 Failover
- Health check loop iterates all unique replicas every `check_interval` (default 1s).
- TCP probe: non-blocking connect with 500ms `select()` timeout.

---

## 10. Code Quality Findings

### 10.1 Critical (P0)

| # | Issue | Location | Impact | Recommendation |
|---|-------|----------|--------|----------------|
| 1 | **MemTable iterator copies entire map** | `src/storage/cedar_memtable.h` | O(N) memory spike per iterator; read-heavy workloads will experience allocation storms and GC-like pauses | Implement snapshot iterator that holds shared_lock and references map in-place without copy |
| 2 | **PartitionStrategyManager single mutex** | `src/partition/partition_strategy_manager.cc` | Every query acquires same mutex for routing; hotspot under high concurrency | Shard counters with atomics; use read-copy-update for strategy switching |
| 3 | **Failover controller 11 mutexes, no lock ordering** | `src/dtx/storage/failover_manager.cc` | High deadlock risk; `HealthCheckLoop` acquires multiple mutexes and may schedule work that re-enters | Document and enforce global lock order; consolidate related state under fewer mutexes |
| 4 | **HTTP health endpoint is raw socket parser** | `src/governance/health_checker.cc` | No header validation, vulnerable to request smuggling; unsuitable for production exposure | Replace with a minimal HTTP library (e.g., llhttp) or brpc HTTP server |
| 5 | **CompactionMergerV2::ShouldFilter always false** | `src/storage/compaction_merger_v2.cc` | No tombstones are ever filtered in V2 compaction path; storage bloat | Implement actual filter logic or disable V2 path until fixed |

### 10.2 High (P1)

| # | Issue | Location | Impact | Recommendation |
|---|-------|----------|--------|----------------|
| 6 | **WAL replay: one RPC per entry** | `src/dtx/storage/partition_migrator.cc` | Network overhead during migration catch-up; slows recovery | Batch WAL entries into chunked RPCs |
| 7 | **SST builder CalculateCRC64 always returns 0** | `src/sst/zone_columnar_builder.cc` | No data integrity verification on SST writes | Implement CRC64 over data region |
| 8 | **ScanWithLateMaterialization declared but not implemented** | `include/cedar/sst/zone_columnar_reader.h` | Advertised optimization missing; full materialization on every scan | Implement late materialization or remove declaration |
| 9 | **PartitionMigrator SwitchTraffic sleeps fixed duration** | `src/dtx/storage/partition_migrator.cc` | Deterministic migration latency; no active drain | Implement fencing token or in-flight request counter |
| 10 | **MemTable node_pool_ never shrinks** | `src/storage/cedar_memtable.cc` | Memory retained after flush until MemTable destruction | Trim node_pool_ after flush or use arena allocator |
| 11 | **LsmEngine mixes raw pointers and unique_ptr** | `src/storage/lsm_engine.cc` | `bg_thread_`, `auto_compaction_thread_` manually deleted in `Close()` | Convert to `std::unique_ptr<std::thread>` |
| 12 | **Block cache is FIFO, not LRU** | `src/sst/zone_columnar_reader.cc` | 16-block FIFO cache ineffective for skewed access | Replace with LRU or clock eviction |

### 10.3 Medium (P2)

| # | Issue | Location | Impact | Recommendation |
|---|-------|----------|--------|----------------|
| 13 | **AUTO mode no hysteresis** | `src/partition/partition_strategy_manager.cc` | Can oscillate between strategies under mixed workload | Add cooldown period and downgrade threshold |
| 14 | **Phi Accrual recomputes mean/variance from scratch** | `src/dtx/phi_accrual.cc` | O(N) per `Phi()` call; unnecessary for N=1000 but adds up | Use Welford's online algorithm for incremental stats |
| 15 | **GetLeaderCandidates copies replicas under lock** | `src/dtx/storage/failover_manager.cc` | Health score may change between copy and use | Check health scores while holding partition lock, or re-verify after |
| 16 | **PartitionMigrator StreamSnapshotToTarget offset unused** | `src/dtx/storage/partition_migrator.cc` | `request.set_offset(0)` for every chunk; offset field meaningless | Track and set actual byte offset |
| 17 | **SST builder buffers entire file in memory** | `src/sst/zone_columnar_builder.cc` | Large heap spikes for big SSTs | Stream directly to file descriptor |
| 18 | **Health check serial, not parallel** | `src/governance/health_checker.cc` | One component at a time; slow with many components | Parallelize health checks with thread pool |
| 19 | **VersionChainIterator copies entire chain** | `src/storage/cedar_memtable.h` | Large copy for keys with many versions | Implement lazy iterator over linked list |
| 20 | **Query cache invalidation on every write** | `src/storage/lsm_engine.cc` | All writes invalidate query cache; write-heavy workloads starve reads | Use versioning or selective invalidation |

### 10.4 Low (P3)

| # | Issue | Location | Impact | Recommendation |
|---|-------|----------|--------|----------------|
| 21 | **Header guards mix FERN_ and CEDAR_ prefixes** | Multiple headers | Indicates incomplete codebase renaming | Standardize to CEDAR_ |
| 22 | **EscapeJsonString hex state pollution** | `src/governance/health_checker.cc` | `std::hex` without reset may affect subsequent numeric insertions | Reset stream flags after hex insertion |
| 23 | **PartitionMigrator CalculateChecksum walks all SSTs** | `src/dtx/storage/partition_migrator.cc` | Extremely expensive for large partitions | Use SST metadata checksums instead |
| 24 | **Get(uint64_t) hardcodes column_id=0** | `src/storage/cedar_memtable.cc` | Generic interface unsuitable for multi-column lookups | Remove or generalize |
| 25 | **RestartViaSystemd strncpy non-null termination** | `src/dtx/storage/failover_manager.cc` | Potential non-null-terminated path | Use `strlcpy` or `std::string` + `.c_str()` |

---

*Document compiled from direct source code analysis. All file paths and line references correspond to commit `641118a` on branch `main`.*
