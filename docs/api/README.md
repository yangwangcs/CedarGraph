# CedarGraph API Documentation

## Overview

CedarGraph is a distributed temporal graph database that provides:
- Full ACID transactions
- Temporal versioning (time-travel queries)
- Horizontal scalability
- High availability with Raft consensus

## Core APIs

### 1. Graph Operations

#### Create Vertex
```cpp
Status CreateVertex(uint64_t vertex_id, 
                    const std::map<std::string, Value>& properties);
```

#### Create Edge
```cpp
Status CreateEdge(uint64_t src_id, 
                  uint64_t dst_id,
                  uint16_t edge_type,
                  const std::map<std::string, Value>& properties);
```

#### Get Vertex
```cpp
StatusOr<std::map<std::string, Value>> GetVertex(uint64_t vertex_id);
```

#### Get Edge
```cpp
StatusOr<std::map<std::string, Value>> GetEdge(uint64_t src_id,
                                                uint64_t dst_id,
                                                uint16_t edge_type);
```

### 2. Query Operations

#### Execute Cypher Query
```cpp
ResultSet ExecuteQuery(const std::string& query);
```

#### Execute Parameterized Query
```cpp
ResultSet ExecuteQuery(const std::string& query,
                       const std::map<std::string, Value>& parameters);
```

### 3. Transaction Operations

#### Begin Transaction
```cpp
Transaction* BeginTransaction();
```

#### Commit Transaction
```cpp
Status CommitTransaction(Transaction* txn);
```

#### Abort Transaction
```cpp
Status AbortTransaction(Transaction* txn);
```

### 4. Temporal Operations

#### Get Historical Version
```cpp
StatusOr<Value> GetHistoricalVersion(uint64_t entity_id,
                                     uint16_t column_id,
                                     Timestamp timestamp);
```

#### Get Version Range
```cpp
StatusOr<std::vector<Value>> GetVersionRange(uint64_t entity_id,
                                              uint16_t column_id,
                                              Timestamp start,
                                              Timestamp end);
```

## Configuration

### CedarOptions
```cpp
struct CedarOptions {
  bool create_if_missing = true;
  bool enable_wal = true;
  bool enable_accumulated_flush = true;
  size_t memtable_threshold = 64 * 1024 * 1024;  // 64MB
  std::string storage_mode = "partitioned";
  size_t max_open_partitions = 256;
  uint32_t partition_count = 65536;
};
```

## Error Handling

All operations return `Status` or `StatusOr<T>` objects:
- `Status::OK()` - Success
- `Status::NotFound()` - Entity not found
- `Status::InvalidArgument()` - Invalid parameters
- `Status::IOError()` - I/O error
- `Status::Busy()` - Resource busy

## Thread Safety

- All public APIs are thread-safe
- Multiple readers can access concurrently
- Writers are serialized per entity
- Transactions provide snapshot isolation

## Performance Tips

1. **Use WriteBatch** for bulk operations
2. **Use indexes** for frequent queries
3. **Partition data** by access patterns
4. **Monitor metrics** for bottlenecks
5. **Tune configuration** for workload
