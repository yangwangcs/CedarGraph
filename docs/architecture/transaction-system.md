# Transaction System Documentation

## Overview

CedarGraph implements a hybrid transaction system combining Optimistic Concurrency Control (OCC) for single-partition transactions and Two-Phase Commit (2PC) for cross-partition transactions. The system provides snapshot isolation with full ACID guarantees.

## Transaction Types

### 1. Single-Partition Transaction (OCC)

Used when all operations are within a single partition.

**Flow:**
```
Client → Begin → Read/Write → Validate → Commit/Abort
```

**Characteristics:**
- No distributed coordination
- Fast path for single-partition operations
- Snapshot isolation
- Automatic rollback on conflict

### 2. Cross-Partition Transaction (2PC)

Used when operations span multiple partitions.

**Flow:**
```
Coordinator → Prepare (all participants) → 
  Wait for all ACKs →
  Persist decision →
  Commit/Abort (all participants)
```

**Characteristics:**
- Distributed coordination via 2PC
- Atomic commit across partitions
- Deadlock detection
- Timeout handling

## OCC Transaction

### Transaction States

```
┌─────────┐     ┌─────────────┐     ┌─────────────┐
│  Active  │────▶│  Validating │────▶│  Committing │
└─────────┘     └─────────────┘     └─────────────┘
     │                │                    │
     │                ▼                    ▼
     │          ┌──────────┐         ┌──────────┐
     └─────────▶│  Aborted │         │Committed │
                └──────────┘         └──────────┘
```

**State Transitions:**
- `Active → Validating`: Begin validation phase
- `Validating → Committing`: Validation passed
- `Committing → Committed`: Write to WAL and MemTable
- `Active → Aborted`: Validation failed or explicit abort
- `Validating → Aborted`: Conflict detected

### Transaction Context

```cpp
struct OCCTransaction {
  TxnID txn_id_;
  Timestamp read_timestamp_;    // Snapshot timestamp
  Timestamp commit_timestamp_;  // Assigned at commit
  
  // Buffered changes
  std::vector<ReadSetEntry> read_set_;
  std::vector<WriteSetEntry> write_set_;
  
  // State
  std::atomic<TransactionState> state_;
};
```

### Read Set

Records all reads for conflict detection:
```cpp
struct ReadSetEntry {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  Timestamp read_txn_version;  // Version at read time
};
```

### Write Set

Records all writes for commit:
```cpp
struct WriteSetEntry {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  Descriptor descriptor;
  CedarKey key;
  Timestamp user_timestamp;
  Timestamp txn_version;  // Assigned at commit
  uint64_t target_id;
};
```

### Validation Phase

The validation phase checks for conflicts:

1. **Read-Write Conflict**: Check if any read item was modified after `read_timestamp_`
2. **Write-Write Conflict**: Check if any write item was modified by concurrent transaction

**Read-Write Validation:**
```cpp
for (const auto& read_entry : read_set_) {
  auto versions = memtable_->GetVersionChain(
      read_entry.entity_id, read_entry.entity_type, read_entry.column_id);
  
  for (const auto& version : versions) {
    if (version.txn_version > read_timestamp_) {
      // Conflict: item modified after our read
      return Status::Busy("Read-write conflict");
    }
  }
}
```

**Write-Write Validation:**
```cpp
for (const auto& write_entry : write_set_) {
  auto chain = memtable_->GetVersionChain(
      write_entry.entity_id, write_entry.entity_type, write_entry.column_id);
  
  if (!chain.empty()) {
    // Check for blind write conflict
    const auto& latest = chain.front();
    if (latest.txn_version > read_timestamp_) {
      return Status::Busy("Write-write conflict");
    }
  }
}
```

### Commit Process

1. **Allocate Commit Timestamp**: Global monotonic timestamp
2. **Update Write Set**: Set `txn_version` to commit timestamp
3. **Write to WAL**: Log commit record
4. **Write to MemTable**: Apply all writes
5. **Update State**: Mark as committed

**Atomicity Guarantee:**
- WAL write before MemTable write
- If MemTable write fails, WAL entry will be replayed on recovery
- Commit is idempotent (can be retried)

### Abort Process

1. **Update State**: Mark as aborted
2. **Write to WAL**: Log abort record (optional)
3. **Release Resources**: Clear read/write sets
4. **Unregister**: Remove from active transactions

## 2PC Transaction

### Coordinator Role

The coordinator manages the 2PC protocol:

1. **Begin**: Create transaction context
2. **Execute**: Send operations to participants
3. **Prepare**: Send prepare to all participants
4. **Decide**: Commit if all prepare, abort otherwise
5. **Commit/Abort**: Send decision to all participants

### Participant Role

Each participant manages its local transaction:

1. **Begin**: Start local transaction
2. **Execute**: Apply operations locally
3. **Prepare**: Validate and acquire locks
4. **Commit/Abort**: Apply or rollback

### Prepare Phase

```cpp
Status Prepare(TxnID txn_id, const WriteSet& write_set) {
  // 1. Validate local transaction
  Status s = local_txn_->Validate();
  if (!s.ok()) return s;
  
  // 2. Acquire locks (stripe-based)
  for (const auto& entry : write_set) {
    AcquireCommitLock(entry.entity_id);
  }
  
  // 3. Write prepare record to WAL
  WritePrepareWAL(txn_id);
  
  return Status::OK();
}
```

### Commit Phase

```cpp
Status Commit(TxnID txn_id, Timestamp commit_ts) {
  // 1. Write to MemTable
  for (const auto& entry : write_set_) {
    memtable_->Put(entry.key, entry.descriptor, commit_ts);
  }
  
  // 2. Write commit record to WAL
  WriteCommitWAL(txn_id, commit_ts);
  
  // 3. Release locks
  ReleaseCommitLocks();
  
  return Status::OK();
}
```

### Abort Phase

```cpp
Status Abort(TxnID txn_id) {
  // 1. Write abort record to WAL
  WriteAbortWAL(txn_id);
  
  // 2. Release locks
  ReleaseCommitLocks();
  
  // 3. Rollback local changes
  local_txn_->Abort();
  
  return Status::OK();
}
```

## Lock Management

### Stripe-Based Commit Locks

To prevent deadlocks, commit locks are striped:

```cpp
static constexpr size_t kCommitLockStripes = 64;
std::array<std::mutex, kCommitLockStripes> commit_lock_stripes_;

std::mutex& GetCommitLock(uint64_t entity_id) const {
  return commit_lock_stripes_[entity_id & (kCommitLockStripes - 1)];
}
```

**Lock Ordering:**
- Lock stripes in sorted order
- Deduplicate stripe indices (prevent self-deadlock)
- Release locks after commit/abort

### Deadlock Detection

The system detects deadlocks using a wait-for graph:

```cpp
class DeadlockDetector {
  std::unordered_map<TxnID, std::set<TxnID>> wait_for_graph_;
  
  bool DetectCycle(TxnID start_txn);
  TxnID SelectVictim(const std::vector<TxnID>& cycle);
};
```

**Victim Selection:**
- Choose transaction with fewest locks
- Or transaction that started most recently
- Abort victim and retry

## Write-Ahead Log (WAL)

### WAL Entry Types

1. **PUT**: Single key-value write
2. **COMMIT**: Transaction commit record
3. **ABORT**: Transaction abort record
4. **CHECKPOINT**: Periodic checkpoint

### WAL Entry Format

```cpp
struct WalRecordHeader {
  uint32_t crc32;        // CRC32 of data
  uint16_t type;         // PUT, COMMIT, ABORT
  uint16_t flags;        // Reserved
  uint32_t data_length;  // Length of data
  uint64_t sequence;     // Monotonic sequence
};
```

### Group Commit

Multiple transactions batch their WAL entries:

```
Txn1 ──┐
Txn2 ──┼──▶ [Batch] ──▶ WAL ──▶ fsync ──▶ Notify
Txn3 ──┘
```

**Benefits:**
- Fewer fsync calls
- Higher throughput
- Lower latency per transaction

### Recovery Process

On startup, replay WAL entries:

1. Read all WAL files in sequence order
2. For each entry:
   - PUT: Apply to MemTable
   - COMMIT: Mark transaction as committed
   - ABORT: Mark transaction as aborted
3. Skip entries older than SST files
4. Rebuild state from SST files + WAL

## Consistency Levels

### Snapshot Isolation

Default isolation level:
- Each transaction sees a consistent snapshot
- Snapshot taken at `read_timestamp_`
- No dirty reads, no non-repeatable reads
- Phantom reads possible (but rare in graph queries)

### Read-Your-Writes

Guarantees that a transaction sees its own writes:
- Read from leader (has latest writes)
- Use same `read_timestamp_` for all reads

### Eventual Consistency

For read-only queries that can tolerate stale data:
- Read from follower (lower latency)
- May see slightly stale data
- Configurable lag threshold

## Performance Optimization

### Transaction Pool

Reuse transaction objects to avoid allocation:

```cpp
class TransactionPool {
  std::vector<std::unique_ptr<OCCTransaction>> pool_;
  std::mutex mutex_;
  
  OCCTransaction* Acquire();
  void Release(OCCTransaction* txn);
};
```

### Write Batching

Batch multiple writes into single transaction:

```cpp
Status BatchWrite(const std::vector<WriteEntry>& entries) {
  auto txn = BeginTransaction();
  for (const auto& entry : entries) {
    txn->Put(entry.entity_id, entry.entity_type, 
             entry.column_id, entry.descriptor, entry.timestamp);
  }
  return txn->Commit();
}
```

### Parallel Execution

Execute independent operations in parallel:

```cpp
Status ParallelBatchWrite(const std::vector<WriteEntry>& entries) {
  // Split into chunks
  auto chunks = SplitIntoChunks(entries, num_threads);
  
  // Execute in parallel
  std::vector<std::future<Status>> futures;
  for (const auto& chunk : chunks) {
    futures.push_back(std::async(std::launch::async, [&]() {
      return BatchWrite(chunk);
    }));
  }
  
  // Wait for all
  for (auto& f : futures) {
    if (!f.get().ok()) return f.get();
  }
  return Status::OK();
}
```

## Error Handling

### Conflict Resolution

**Read-Write Conflict:**
- Retry with new snapshot
- Exponential backoff
- Max retry count

**Write-Write Conflict:**
- Abort and retry
- Or use pessimistic locking for hot keys

**Timeout:**
- Configurable timeout per transaction
- Automatic abort on timeout
- Return error to client

### Recovery Scenarios

**Crash During Commit:**
- WAL entry exists but MemTable not updated
- On recovery, replay WAL entry
- Commit is idempotent

**Crash During Abort:**
- WAL entry may or may not exist
- On recovery, mark as aborted
- Release any held locks

**Network Partition:**
- 2PC timeout triggers abort
- Coordinator decides commit/abort
- Participants retry on reconnect

## Monitoring

### Key Metrics

- `active_transactions`: Number of active transactions
- `committed_transactions`: Number of committed transactions
- `aborted_transactions`: Number of aborted transactions
- `validation_failures`: Number of validation failures
- `deadlock_detections`: Number of deadlocks detected
- `avg_commit_time`: Average commit time
- `p99_commit_time`: 99th percentile commit time

### Health Checks

- Transaction pool not exhausted
- No stuck transactions
- Deadlock detector working
- WAL sync working

## Best Practices

1. **Keep Transactions Short**: Reduce conflict window
2. **Use Appropriate Isolation**: Snapshot for most cases
3. **Avoid Hot Keys**: Distribute writes across entities
4. **Monitor Conflicts**: Track validation failures
5. **Tune Timepoints**: Set appropriate timeouts
6. **Batch Writes**: Use batch operations when possible
7. **Handle Retries**: Implement exponential backoff
8. **Test Under Load**: Verify behavior under contention
