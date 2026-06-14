# CedarGraph-Core Changelog

## [Unreleased] - 2026-06-14

### Production Readiness Sprint — 45+ Fixes

Comprehensive code review and production hardening across all modules.

---

### Added

#### DDL & Query Layer
- **SHOW commands**: `SHOW SPACES`, `SHOW TAGS`, `SHOW EDGES`, `SHOW LABELS`, `SHOW PARTS`, `SHOW HOSTS`, `SHOW INDEXES`, `SHOW HOTSPOTS`
- **USE SPACE**: Session-level space context via `USE <space_name>`
- **Index DDL**: `CREATE INDEX`, `DROP INDEX`, `SHOW INDEXES` with MetaD persistence
- **ListSpaces/ListLabels RPC**: New gRPC endpoints for metadata enumeration
- **Slow query logging**: Queries exceeding 500ms logged with `[SLOW_QUERY]` prefix
- **QPS sliding window**: Accurate QPS calculation using 10K-entry circular buffer

#### Storage Engine
- **DeltaVersionEncoder**: XOR-based delta compression for version chains (50-90% memory savings)
- **RateLimiter**: Token bucket I/O throttling for background compaction
- **Blob GC integration**: `BlobFileManager::OnSSTDeleted()` connects to `BlobGCManager`
- **Blob pending writes buffer**: Write visibility before flush with `pending_writes_` map
- **Blob header checksum verification**: CRC64 validation on `BlobFileReader::Open()`

#### Backup & Monitoring
- **CreateBackup/RestoreFromBackup/ListBackups**: gRPC API for file-level backup
- **GetHotSpots/ResetHotSpotStats**: gRPC API for hot entity detection
- **GetStorageCapacity**: gRPC API for disk usage monitoring
- **Health check disk capacity component**: Warning at 80%, critical at 90%

#### Security & RBAC
- **Role assignment API**: `AssignRole`/`RevokeRole`/`GetUserRoles` via MetaD gRPC
- **ListRoles RPC**: Enumerate all defined roles

#### Raft & Distributed
- **Raft write path**: `Put`/`Delete`/`Commit` through `RaftGroup::Propose()` in StorageD
- **Lease Read**: `Get` uses leader lease check for linearizable reads without RPC
- **PartitionRaftManager**: Real initialization in StorageD (replaces stub mode)
- **braft/brpc version locking**: `third_party/VERSIONS` file + `setup_deps.sh` script

#### Testing
- **test_batch_neighbor_query**: Standalone GTest replacing server-dependent `test_debug_batch`

---

### Fixed

#### Critical (Data Corruption / Deadlock / UB)
- **CopyCedarKey**: Now copies all 7 key fields (was only 2) — `dtx_service_impl.cc`
- **AutoBatchExecutor deadlock**: `Flush()` called while holding `mutex_` — refactored to `FlushInternal()` pattern
- **Iterator invalidation**: `lsm_engine.cc` cache erase used invalidated iterator after unlock — now uses key-based erase
- **Slice null UB**: `Slice(const char* s)` called `strlen(nullptr)` — added null guard
- **WriteBatch Iterate format mismatch**: Read extra byte not present in `Put()` format — fixed to match
- **Parallel lambda UAF**: `ExecuteParallel2PC` captured promises by reference — changed to `shared_ptr`
- **Parallel batch thread safety**: `ExecuteVertexBatch` returned before joining threads — rewritten to join-first

#### High (Correctness / Availability)
- **FERN_CONFIG macros**: 6 macros referenced undefined `FERN_CONFIG` — changed to `CEDAR_CONFIG`
- **Config limits not assigned**: `cedar_config.cc` `get_size_t()` return values discarded — now assigned
- **FullTwoPhaseCommit state**: Partial commit set `kUnknown` — changed to `kCommitting` for recovery
- **Decision log durability**: `PersistCommitDecision` didn't fsync parent directory — added
- **Recovery infinite retry**: No backoff or max retries — added exponential backoff (100ms→6.4s, max 10)
- **Destructor exception safety**: `TransactionWalBatch::~TransactionWalBatch` auto-committed without try-catch
- **OCC write-write conflict**: Checked `latest.txn_version != read_txn_version` (too strict) — now checks within snapshot window
- **OCC GetAllColumns SST fallback**: Used `Timestamp::Max()` skipping validation — now uses `read_timestamp_`
- **OCC ValidateReadEntry empty chain**: Couldn't distinguish flush vs delete — now queries SST with `read_timestamp_`
- **Sequential Prepare**: Continued after first failure — now breaks immediately
- **Sequential Abort**: Sent to all participants including unprepared — now only to prepared
- **GetParticipants dedup**: Deduplicated by pointer, not partition ID — now deduplicates by `PartitionID`
- **Commit state update order**: In-memory before persistent — reversed for crash safety
- **Query cache**: Cached write queries and transactions — now skips both
- **Write timestamp**: Used `steady_clock` (not synchronized) — changed to `system_clock`
- **Rollback**: Didn't send Abort to StorageD — now sends via `storage_clients_`
- **Read-set accumulation**: Built from query text, not actual reads — now accumulates during execution
- **Prepare read-set validation**: Used `commit_ts` instead of `read_timestamp` — now uses snapshot timestamp
- **Snapshot isolation**: Read path didn't pass transaction context — now passes `read_timestamp`

#### Medium (Robustness / Performance)
- **MergeZones linear scan**: O(K×N) minimum key finding — replaced with min-heap O(N×logK)
- **VersionCache linear scan**: O(N) lookup in 16-entry vector — replaced with `unordered_map` O(1)
- **kUserKeySize**: Incorrectly set to 27 — corrected to 19 bytes
- **SST reader cache EvictLRU**: Could infinite-loop when all entries pinned — added attempt limit
- **AnchorCache eviction**: "LRU" was actually random (middle element) — changed to LFU by access count
- **Block-level compaction OOB**: Accessed `inputs[0]` when empty — added empty check
- **IsWriteQuery false positives**: Substring matching ("set" in "offset") — changed to word-boundary matching
- **ConfigManager Merge deadlock**: Two configs merging each other — changed to `std::scoped_lock`
- **consistent_hash_ring EVP_MD_CTX_new**: Unchecked null return — added fallback to `std::hash`
- **GetDouble error message**: Said "Invalid integer" — corrected to "Invalid double"
- **Include guard mismatches**: 35+ files had `FERN_` prefix — all corrected to `CEDAR_`

#### GC & Concurrency
- **WatermarkGc::Start() race**: Two concurrent calls could both create threads — added `start_stop_mutex_`
- **VersionChainIndex::RunGC**: Held unique lock blocking all reads — changed to per-chain locking
- **BlobGCManager polling**: 60-second sleep loop — changed to `condition_variable::wait_for`
- **GetInlineString null truncation**: Used `std::string(buf)` with embedded nulls — now uses `std::string(buf, len)`
- **Blob write/read mutex**: Single mutex blocked reads during writes — split into `write_mutex_` and `read_mutex_`
- **MetaD HeartbeatCheckLoop**: `sleep_for` caused shutdown delay — changed to `condition_variable`
- **MetaD NotifyPartitionChange**: Called callbacks under lock (deadlock risk) — now copies then calls outside lock
- **ConfigManager hot reload**: `sleep_for` polling — changed to `condition_variable`

#### MetaD
- **Snapshot missing indexes**: `Serialize()` didn't persist `indexes_` — added serialization (version 2→3)
- **UpdatePartitionLeader/Assignment duplication**: Extracted `NotifyPartitionLeaderChange` helper

#### Storage Engine
- **WriteBlob flush**: Flushed after every write (slow) — batch flush with pending buffer for visibility

---

### Changed

- **third_party/VERSIONS**: Locks brpc=1.16.0, braft=v1.3.0
- **CMakeLists.txt**: Always uses vendored braft/brpc (removed `CEDAR_VENDOR_BRAFT` option)
- **scripts/setup_deps.sh**: New script for dependency management with version verification
- **DeltaVersionChain**: Now integrated into `CedarMemTable` with `kCompressionThreshold=16`

---

### Test Results

| Category | Tests | Status |
|----------|-------|--------|
| Core (status, key, descriptor, env) | 44 | ✅ PASSED |
| Storage (graph, edge, update, memtable) | 34 | ✅ PASSED |
| Transaction (pool, WAL, OCC, version chain) | 23 | ✅ PASSED |
| DTX (integration, deadlock, recovery) | 40 | ✅ PASSED |
| Governance (config, registry, health) | 46 | ✅ PASSED |
| Security (blockers, JWT, RBAC) | 4 | ✅ PASSED |
| Batch neighbor query (new) | 5 | ✅ PASSED |
| **Total** | **196** | **✅ ALL PASSED** |

---

### Migration Notes

- **Snapshot format**: Version 3 (backward compatible with version 2)
- **No data migration required**: All changes are backward compatible
- **Config macros**: `CEDAR_DB()`, `CEDAR_LSM()`, etc. now work correctly (were broken before)
