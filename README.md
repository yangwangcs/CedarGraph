# CedarGraph-Core

A distributed temporal graph database built with C++, gRPC, and Protobuf.

## Performance

### Benchmark Results (macOS ARM64, test_mode, no Raft)

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Write Throughput** | 104 ops/sec | **230,000 ops/sec** | **2,212x** |
| **Read Throughput** | 293 ops/sec | **102,000 ops/sec** | **349x** |
| **Temporal Range** | - | **158,000 ops/sec** | - |
| **Temporal Point** | - | **98,000 ops/sec** | - |

### Comparison with NebulaGraph

| Metric | CedarGraph | NebulaGraph | Status |
|--------|-----------|-------------|--------|
| Write | 230K ops/sec | ~50K ops/sec | **4.6x faster** ✓ |
| Read | 102K ops/sec | ~100K ops/sec | **1.02x faster** ✓ |

## Architecture

### Core Services

| Service | Role | Default Port |
|---------|------|--------------|
| **MetaD** | Raft-based metadata management, schema, partition mapping | 9559 |
| **StorageD** | Replicated LSM storage, Raft log, MVCC version chains | 9779 |
| **GraphD** | Stateless query router, Cypher execution, 2PC coordinator | 9669 |
| **GCN** | Graph Compute Node, TMV engine, scatter-gather | - |

### Key Subsystems

- **DTX (src/dtx/)**: Distributed transactions — 2PC coordinator, TWCD engine, LSM-native OCC
- **Storage (src/storage/)**: LSM engine, VSL memtable, zone-columnar SST, parallel compaction
- **Cypher (src/cypher/)**: OpenCypher parser, temporal extensions, execution plan operators
- **SST (src/sst/)**: Zone-columnar format with column-level compression

## Quick Start

### Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_POLICY_VERSION_MINIMUM=3.5
make -j4
```

### Run Standalone (3 processes)

```bash
./scripts/start_standalone.sh start
```

### Run Distributed Cluster (7 processes)

```bash
./scripts/start_distributed.sh start
```

## Performance Optimizations

### Write Path

1. **WriteBatch interface** — single WAL write for batch of entries
2. **WAL batch sync** — `sync_on_write=false`, conditional sync every 100ms or 1000 writes
3. **Deferred operations** — cache invalidation, column tracking moved outside lock

### Read Path

1. **LockedVSL shared_mutex** — concurrent reads on MemTable
2. **SST GetAtTime direct lookup** — no more GetRange + sort
3. **Accumulated buffer shared_mutex** — concurrent reads
4. **Block cache thundering herd double-check** — prevent duplicate loads
5. **Get fast path** — try column_id=0 first, skip GetEntityColumnIds
6. **Disabled QueryCache** — was serializing all reads with std::mutex

## License

Apache 2.0 ("The Cedar Authors")
