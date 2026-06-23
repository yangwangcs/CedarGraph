# CedarGraph User Manual

## Getting Started

### Installation

#### Prerequisites
- C++17 compiler (GCC 11+ or Clang 14+)
- CMake 3.16+
- gRPC 1.50+
- Protobuf 3.21+

#### Build from Source
```bash
git clone https://github.com/cedar-graph/CedarGraph-Core.git
cd CedarGraph-Core
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Quick Start

#### 1. Start MetaD
```bash
./cedar-metad --listen 127.0.0.1:9559 --grpc_port 10559
```

#### 2. Start StorageD
```bash
./cedar-storaged --port 9779 --meta 127.0.0.1:10559
```

#### 3. Start GraphD
```bash
./cedar-graphd --port 9669 --meta 127.0.0.1:10559
```

#### 4. Connect and Query
```bash
./cedargraph shell --host 127.0.0.1 --port 9669
> CREATE (n:Person {name: 'Alice', age: 30})
> MATCH (n:Person) RETURN n
```

## Data Model

### Vertices
```cypher
CREATE (n:Person {name: 'Alice', age: 30, city: 'Beijing'})
```

### Edges
```cypher
CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
```

### Properties
- Integer: `42`
- Float: `3.14`
- String: `'hello'`
- Boolean: `true`/`false`
- NULL: `null`

## Query Language

### MATCH
```cypher
-- Find all persons
MATCH (n:Person) RETURN n

-- Find persons with specific property
MATCH (n:Person {name: 'Alice'}) RETURN n

-- Find relationships
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name
```

### WHERE
```cypher
-- Filter by property
MATCH (n:Person) WHERE n.age > 25 RETURN n

-- Multiple conditions
MATCH (n:Person) WHERE n.age > 25 AND n.city = 'Beijing' RETURN n
```

### CREATE
```cypher
-- Create vertex
CREATE (n:Person {name: 'Bob', age: 25})

-- Create edge
CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
```

### SET
```cypher
-- Update property
MATCH (n:Person {name: 'Alice'}) SET n.age = 31
```

### DELETE
```cypher
-- Delete vertex
MATCH (n:Person {name: 'Alice'}) DELETE n

-- Delete edge
MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r
```

## Temporal Queries

### Time Travel
```cypher
-- Query as of specific time
MATCH (n:Person) FOR SYSTEM_TIME AS OF timestamp('2024-01-01') RETURN n

-- Query time range
MATCH (n:Person) FOR SYSTEM_TIME BETWEEN timestamp('2024-01-01') 
  AND timestamp('2024-12-31') RETURN n
```

## Administration

### Cluster Management
```bash
# Check cluster status
cedargraph status

# Start cluster
cedargraph start

# Stop cluster
cedargraph stop
```

### Backup and Restore
```bash
# Create backup
cedargraph backup --output /path/to/backup

# Restore from backup
cedargraph restore --input /path/to/backup
```

### Monitoring
```bash
# View metrics
curl http://localhost:9669/metrics

# Health check
curl http://localhost:9669/health
```

## Configuration

### MetaD Configuration
```yaml
metad:
  listen_address: "0.0.0.0:9559"
  grpc_port: 10559
  data_dir: "/data/metad"
```

### StorageD Configuration
```yaml
storaged:
  port: 9779
  meta_server: "127.0.0.1:10559"
  data_dir: "/data/storage"
  storage_mode: "partitioned"
  max_open_partitions: 256
```

### GraphD Configuration
```yaml
graphd:
  port: 9669
  meta_server: "127.0.0.1:10559"
  query_timeout_ms: 30000
```

## Performance Tuning

### Memory
- `memtable_size_mb`: MemTable size (default: 64MB)
- `block_cache_mb`: Block cache size (default: 256MB)
- `row_cache_mb`: Row cache size (default: 64MB)

### Storage
- `l0_max_files`: L0 compaction trigger (default: 4)
- `max_bytes_for_level_base_mb`: Level size base (default: 256MB)
- `max_bytes_for_level_multiplier`: Level size multiplier (default: 10)

### Concurrency
- `max_open_partitions`: Max open partitions (default: 256)
- `compaction_threads`: Compaction thread count (default: 1)

## Troubleshooting

### Common Issues

#### MetaD won't start
- Check if port 9559 is available
- Verify data directory permissions
- Check logs for errors

#### StorageD connection failed
- Verify MetaD is running
- Check network connectivity
- Verify port 9779 is available

#### Query timeout
- Increase `query_timeout_ms`
- Check storage performance
- Optimize query with indexes

### Logs
- MetaD: `/var/log/cedar/metad.log`
- StorageD: `/var/log/cedar/storaged.log`
- GraphD: `/var/log/cedar/graphd.log`

### Metrics
- Query latency: `cypher_query_latency_us`
- Write throughput: `cedar_storage_writes_total`
- Cache hit rate: `cedar_cache_hit_rate`
