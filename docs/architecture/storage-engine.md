# Storage Engine Documentation

## Overview

The storage engine is the foundation of CedarGraph, responsible for persisting graph data with full temporal versioning support. It uses a Log-Structured Merge (LSM) tree architecture optimized for write-heavy workloads.

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────┐
│                    CedarGraphStorage                     │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   MemTable   │  │   WAL       │  │   Cache     │     │
│  │  (Skip List) │  │ (Write-Ahead│  │  (LRU)      │     │
│  │              │  │    Log)     │  │             │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │              LSM Engine                          │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐        │   │
│  │  │ Level 0 │  │ Level 1 │  │ Level N │        │   │
│  │  │  SST    │  │  SST    │  │  SST    │        │   │
│  │  └─────────┘  └─────────┘  └─────────┘        │   │
│  └─────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │           Compaction Engine                      │   │
│  │  - Size-Tiered Compaction                       │   │
│  │  - Zone-Columnar Format                         │   │
│  │  - Per-Zone Compression (LZ4/Zstd)              │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## MemTable

### Implementation: Versioned Skip List (VSL)

The MemTable uses a lock-free skip list that maintains multiple versions per entity.

**Key Features:**
- Thread-safe concurrent reads and writes
- MVCC version chains per entity
- Efficient range scans
- Memory-bounded with configurable size threshold

**Data Structure:**
```cpp
class LockedVSL {
  LFNode* head_;
  LFNode* tail_;
  std::atomic<int> max_height_;
  std::shared_mutex mutex_;  // For thread safety
};
```

**Operations:**
- `Put(key, descriptor, txn_version)`: Insert a new version
- `Get(entity_id, entity_type, column_id, timestamp)`: Get version at timestamp
- `GetRange(entity_id, entity_type, column_id, start, end)`: Get version range
- `ScanRange(entity_id, entity_type, column_id, start, end)`: Scan with filters

### Version Chain Structure

Each entity maintains a version chain:
```
Entity 100, Column 1:
  Version 3 (ts=1000, txn=2000) → Version 2 (ts=500, txn=1500) → Version 1 (ts=100, txn=1000)
```

**Version Chain Rules:**
1. New versions are prepended to the chain
2. Versions are ordered by timestamp (newest first)
3. Tombstone versions mark deletion
4. GC removes versions older than watermark

## Write-Ahead Log (WAL)

### Purpose
The WAL ensures durability by logging all mutations before they are applied to the MemTable.

### WAL Entry Format
```
┌─────────────┬─────────────┬─────────────┐
│   Header    │    Data     │    CRC      │
│  (16 bytes) │  (variable) │  (4 bytes)  │
└─────────────┴─────────────┴─────────────┘
```

**Header Fields:**
- `type`: PUT, COMMIT, ABORT
- `data_length`: Length of data section
- `sequence`: Monotonic sequence number
- `crc32`: CRC32 checksum of data

### Group Commit

To improve throughput, the WAL uses group commit:
1. Multiple transactions queue their WAL entries
2. A background thread batches entries
3. Single `fsync()` per batch
4. Promises fulfilled after sync

**Configuration:**
- `group_commit_timeout_us`: Max wait time (default 1000μs)
- `sync_on_write`: Sync every write (default false)
- `sync_interval_ms`: Batch sync interval (default 100ms)
- `sync_threshold`: Max unsynced writes (default 1000)

### Recovery

On startup, the WAL is replayed:
1. Read all WAL files in order
2. For each entry, apply to MemTable
3. Skip entries with timestamps older than SST files
4. Rebuild `entity_column_map_` from SST files

## SST Files

### Zone-Columnar Format

SST files use a zone-columnar format for efficient column-level compression:

```
┌─────────────────────────────────────────────────────────┐
│                    SST File                              │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │              Zone 0 (Entity Range)              │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐        │   │
│  │  │ Column 0│  │ Column 1│  │ Column N│        │   │
│  │  │ (LZ4)   │  │ (Zstd)  │  │ (None)  │        │   │
│  │  └─────────┘  └─────────┘  └─────────┘        │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Zone 1 (Entity Range)              │   │
│  │  ...                                            │   │
│  └─────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │              Sparse Index                        │   │
│  │  - Entity ID range per zone                     │   │
│  │  - Timestamp range per zone                     │   │
│  │  - Column IDs per zone                          │   │
│  └─────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │              Footer                              │   │
│  │  - Magic number                                 │   │
│  │  - Version                                      │   │
│  │  - Index offset                                 │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Compression Strategy

| Level | Compression | Threshold |
|-------|-------------|-----------|
| L0 | None | - |
| L1-L2 | LZ4 | 64 bytes |
| L3+ | Zstd | 64 bytes |

**Per-Zone Compression:**
Each zone can have different compression based on data characteristics:
- Integer columns: LZ4 (fast)
- String columns: Zstd (high ratio)
- Blob columns: Zstd level 1

### SST File Metadata

```cpp
struct SSTFileMeta {
  uint64_t file_number;
  uint64_t file_size;
  uint64_t num_entries;
  uint64_t min_entity_id;
  uint64_t max_entity_id;
  Timestamp min_tx_time;
  Timestamp max_tx_time;
  uint16_t column_id;
  uint8_t entity_type;
  int level;
};
```

## Compaction

### Size-Tiered Compaction

CedarGraph uses size-tiered compaction with leveled promotion:

**L0 → L1 Promotion:**
- When L0 has >1 file, merge ALL L0 files + overlapping L1 files
- Output to L1
- This prevents L0 infinite loop (previous bug)

**Higher Level Compaction:**
- Triggered when level exceeds size threshold
- Merge overlapping files from level N to level N+1
- Configurable size ratio (default 10x)

**Compaction Triggers:**
```cpp
struct CompactionConfig {
  size_t l0_max_files = 4;
  size_t max_bytes_for_level_base = 256 * 1024 * 1024;  // 256MB
  size_t max_bytes_for_level_multiplier = 10;
  size_t max_merge_width = 10;
};
```

### Compaction Process

1. **Pick Files**: Select files for compaction based on strategy
2. **Merge**: Combine files using min-heap merge
3. **Write Output**: Write merged data to new SST file
4. **Update Manifest**: Record new file, remove old files
5. **Delete Old Files**: Remove obsolete SST files

**Thread Safety:**
- Compaction runs in background thread
- Pause during snapshot creation
- Check pause flag before each task

### GC Safe Point

The GC safe point determines which versions can be garbage collected:
- Based on Raft apply index (conservative)
- Wall-clock time for temporal queries
- Configurable retention period

## Column Tracking

### entity_column_map_

The `entity_column_map_` tracks which columns have data for each entity:

```cpp
std::unordered_map<uint64_t, RoaringBitmap> entity_column_map_;
```

**Purpose:**
- Efficient scan operations (only scan columns with data)
- Skip non-existent columns
- Optimize `GetEntityColumnIds()` calls

**Rebuild on Startup:**
```cpp
// In LoadSstFiles()
for (const auto& entry : sst_entries) {
  TrackColumnId(entry.entity_id(), entry.column_id());
}
```

### Property Name Mapping

Property names are mapped to column IDs using a 12-bit hash:
```cpp
uint16_t PropertyNameToColumnId(const std::string& name) {
  return std::hash<std::string>{}(name) & 0x0FFF;
}
```

**Reserved Column IDs:**
- `0xFFF`: Label
- `0xFFE`: Lifecycle
- `0xFFD`: IntervalAnchor
- `0xFFC`: StateAnchor

## Query Cache

### Cross-Query Cache

Caches query results for repeated queries:

```cpp
struct CacheEntry {
  std::vector<MemTableEntry> data;
  std::chrono::steady_clock::time_point timestamp;
  size_t hit_count;
};
```

**Cache Key:**
- Entity ID + Column ID
- TTL-based expiration (configurable)

**Thread Safety:**
- `shared_mutex` for concurrent reads
- `unique_lock` for writes
- TOCTOU-safe operations

### Plan Cache

Caches parsed and planned queries:

```cpp
std::unordered_map<std::string, std::shared_ptr<ExecutionPlan>> plan_cache_;
```

**Fingerprint:**
- Query string hash
- Normalized form (strip whitespace, parameters)

**Eviction:**
- Random eviction when cache full (should be LRU)
- `ClearCache()` for manual invalidation

## Performance Tuning

### MemTable Size
- Default: 64MB
- Larger: Fewer flushes, more memory
- Smaller: More flushes, less memory

### Compaction Threads
- Default: 1 background thread
- More: Faster compaction, more I/O
- Configurable per deployment

### Cache Sizes
- Block Cache: LRU with memory budget
- Row Cache: Entity-level caching
- Query Cache: Cross-query result caching

### I/O Tuning
- `RateLimiter`: Token bucket for compaction I/O
- Direct I/O: Bypass page cache for SST files
- Async I/O: Background flush and compaction

## Monitoring

### Key Metrics
- `flush_count`: Number of flushes
- `compaction_count`: Number of compactions
- `sst_file_count`: Number of SST files
- `memtable_size`: Current MemTable size
- `cache_hit_rate`: Query cache hit rate

### Health Checks
- MemTable not full
- Compaction not stuck
- WAL sync working
- Cache not evicting too fast

## Troubleshooting

### Common Issues

**High Write Latency:**
- Check MemTable size (too small → frequent flushes)
- Check compaction backlog
- Check WAL sync policy

**High Read Latency:**
- Check cache hit rate
- Check SST file count (too many → slow scans)
- Check compaction level distribution

**Data Loss:**
- Check WAL sync policy
- Check crash recovery logs
- Verify SST file integrity

**Memory Usage:**
- Check MemTable size
- Check cache sizes
- Check for memory leaks in version chains

## Best Practices

1. **Partition Design**: Use meaningful partition keys
2. **Column Design**: Group related properties
3. **Index Design**: Create indexes for common queries
4. **Monitoring**: Set up alerts for key metrics
5. **Backup**: Regular snapshots for disaster recovery
6. **Testing**: Load test before production deployment
