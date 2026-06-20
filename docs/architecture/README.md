# CedarGraph Architecture Documentation

## Overview

CedarGraph is a distributed temporal graph database designed for high-performance graph queries with full temporal versioning support. The system is built in C++ with gRPC for inter-service communication and Raft for distributed consensus.

## Core Architecture

### Four-Service Architecture

CedarGraph consists of four core services:

| Service | Port | Role |
|---------|------|------|
| **MetaD** | 9559 (Raft), 10559 (gRPC) | Metadata management, schema, partition mapping |
| **StorageD** | 9779 | Replicated LSM storage, Raft log, MVCC version chains |
| **GraphD** | 9669 | Stateless query router, Cypher execution, 2PC coordinator |
| **GCN** | - | Graph Compute Node, TMV engine, scatter-gather |

### Design Principles

1. **Temporal MVCC**: Every entity has a version chain with timestamps, enabling time-travel queries
2. **Partitioned Storage**: Each partition has its own LSM-Tree for isolation and independent compaction
3. **Multi-Raft Consensus**: Each partition has its own Raft group for replication
4. **Separation of Concerns**: Query routing, transaction management, and storage are cleanly separated

## Data Model

### Entity Types
- **Vertex**: Nodes in the graph with properties
- **EdgeOut**: Outgoing edges from a vertex
- **EdgeIn**: Incoming edges to a vertex (reverse index)

### Key Structure (CedarKey - 32 bytes)
```
entity_id (8 bytes) | entity_type (1 byte) | column_id (2 bytes) | 
target_id (8 bytes) | timestamp (8 bytes) | sequence (2 bytes) | 
flags (1 byte) | part_id (2 bytes)
```

### Descriptor (8 bytes)
```
Kind (4 bits) | ColumnID (12 bits) | Payload (32 bits) | 
Length (8 bits) | Compression (2 bits) | SchemaVersion (6 bits)
```

## Storage Engine

### LSM-Tree Architecture
- **MemTable**: In-memory sorted data structure (skip list)
- **Immutable MemTable**: Flush-ready memtable
- **SST Files**: Sorted String Tables on disk
- **WAL**: Write-Ahead Log for durability

### Compaction Strategy
- **Level 0**: No compression
- **Level 1-2**: LZ4 compression
- **Level 3+**: Zstd compression
- **Zone-Columnar Format**: Column-level compression within SST blocks

### Column Tracking
- `entity_column_map_`: Maps entity_id to set of column_ids
- Rebuilt from SST files on startup
- Used for efficient scan operations

## Transaction System

### OCC (Optimistic Concurrency Control)
1. **Begin**: Allocate transaction ID and read timestamp
2. **Read/Write**: Buffer changes locally
3. **Validate**: Check for conflicts with concurrent transactions
4. **Commit**: Write to WAL and MemTable atomically

### 2PC (Two-Phase Commit)
- **Prepare**: Validate and acquire locks on all participants
- **Commit/Abort**: Broadcast decision to all participants
- **Stripe-based Locks**: 64 lock stripes for per-entity commit serialization

### WAL (Write-Ahead Log)
- **Group Commit**: Batch multiple transactions for efficiency
- **Sync Policy**: Configurable sync-on-write or batch sync
- **Recovery**: Replay WAL on startup to restore state

## Query Engine

### Cypher Parser
- OpenCypher compatible
- Temporal extensions: `FOR SYSTEM_TIME AS OF`, `BETWEEN`
- Parameterized queries with plan caching

### Execution Plan
- **NodeScan**: Scan vertices from storage
- **Expand**: Traverse edges
- **Filter**: Apply predicates
- **ProduceResults**: Collect and return results

### Plan Caching
- Fingerprint-based cache key
- `Clone()` for thread-safe execution
- LRU eviction (when implemented)

## Distributed System

### Partition Routing
- Hash-based partition assignment
- Partition map cached in MetaClient
- Automatic leader election on failure

### Consistency Levels
- **Strong**: Read from leader (default)
- **Eventual**: Read from follower (lower latency)
- **ReadYourWrites**: Read from leader with lease check

### Failure Detection
- Health checker with configurable intervals
- Circuit breaker for failing nodes
- Automatic failover with Raft leader transfer

## Performance Characteristics

### Write Path
- MemTable write: ~1μs
- WAL write: ~10μs (with group commit)
- SST flush: ~100ms (background)

### Read Path
- MemTable read: ~1μs
- SST read: ~10-100μs (depending on level)
- Cache hit: ~0.1μs

### Scalability
- Horizontal: Add more StorageD nodes
- Vertical: Increase partition count
- Compaction: Independent per partition

## Configuration

### Key Parameters
- `storage_mode`: "partitioned" (default) or "shared"
- `enable_wal`: true (default)
- `sync_on_write`: false (default, use batch sync)
- `max_open_partitions`: 256 (default)

### Environment Variables
- `CEDAR_SCAN_MAX_ENTITIES`: Max entities to scan (default 50)
- `CEDAR_LOG_LEVEL`: Logging level

## Monitoring

### Metrics
- Query latency (P50/P95/P99)
- Raft leader changes
- Compaction statistics
- Storage size and growth

### Health Checks
- HTTP endpoints: `/health`, `/ready`
- Component-level health tracking
- Automatic unhealthy node detection

## Security

### Authentication
- JWT token support
- TLS mutual authentication (planned)
- Shared secret for inter-service communication

### Authorization
- Role-based access control (planned)
- Per-query permissions (planned)

## Deployment

### Single Node
```bash
scripts/start_standalone.sh start
```

### Docker
```bash
docker-compose -f docker-compose.minimal.yml up -d
```

### Kubernetes
```bash
kubectl apply -f k8s/
```

## Future Work

1. **CBO Optimizer**: Cost-based query optimization
2. **Backup/Restore**: Snapshot-based backup
3. **Client SDKs**: Python, Java, Go
4. **Monitoring**: Prometheus metrics export
5. **Security**: Full RBAC implementation
