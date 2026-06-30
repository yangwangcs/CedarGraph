# CedarGraph-Core Sub-Plan H: Replace Hand-Rolled Config Parser with rapidjson

**Date:** 2026-06-11  
**Author:** Agent (sub-plan)  
**Parent:** Production Readiness Audit 2026-05-26  
**Scope:** `CedarConfig::LoadFromFile` full rewrite + `SaveToFile` rapidjson parity + round-trip tests  
**Estimated Duration:** 1.5 hours  
**Risk Level:** Medium (touches config persistence layer; breaks undocumented YAML-like input)  

---

## Goal

Replace the hand-rolled line parser in `CedarConfig::LoadFromFile` with a proper rapidjson parser so that:

1. `LoadFromFile` can read **every section** emitted by `SaveToFile` (all 11 sections: `db`, `lsm`, `wal`, `memtable`, `mvcc`, `transaction`, `cache`, `filesystem`, `security`, `tls`, `debug`).
2. Nested objects are handled natively — no more regex/trim logic.
3. Arrays are handled gracefully (skipped with a warning today, ready for future use).
4. `SaveToFile` is rewritten with `rapidjson::PrettyWriter` to eliminate hand-rolled JSON escaping.
5. A comprehensive round-trip test proves that **every scalar field** survives `Save → Load`.
6. Existing tests that feed YAML-like text into `LoadFromFile` are updated to use canonical JSON (the only format `SaveToFile` has ever produced).

---

## Architecture

```
┌─────────────────────────────────────────┐
│  CedarConfig::SaveToFile(path)          │
│  ├─ PrettyWriter → StringBuffer         │
│  ├─ Atomic write (tmp → fsync → rename) │
│  └─ Valid JSON with ALL 11 sections     │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  CedarConfig::LoadFromFile(path)        │
│  ├─ FileReadStream → Document           │
│  ├─ Section-level object iteration      │
│  ├─ Type-safe helper lambdas            │
│  │   (bool, uint32, size_t, int,        │
│  │    double, string, enum)             │
│  └─ Merged into CedarConfig fields      │
└─────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Version / Binding |
|-----------|-------------------|
| C++ Standard | C++17 |
| JSON Library | rapidjson 1.0.2 (vendored inside brpc at `third_party/brpc/src/butil/third_party/rapidjson`) |
| Namespace | `butil::rapidjson` |
| Test Framework | GoogleTest (`gtest_main`) |
| Build Target | `test_cedar_config` |

---

## File Map

| # | File | Action |
|---|------|--------|
| 1 | `src/storage/cedar_config.cc` | Add rapidjson includes; replace `LoadFromFile`; rewrite `SaveToFile`; delete dead helpers |
| 2 | `tests/storage/test_cedar_config.cc` | Update YAML-based tests to JSON; add comprehensive round-trip test |
| 3 | `tests/CMakeLists.txt` | No change required (test target already registered) |

---

## Phase 1: Fix Existing Tests to Use Canonical JSON

### Step 1.1 — Update `ReloadConfigDoesNotDeadlock` to valid JSON
**Time:** 2 min  
**File:** `tests/storage/test_cedar_config.cc`

- [ ] Change the YAML-like write in `ReloadConfigDoesNotDeadlock` (around line 13-15) to strict JSON.

```cpp
// Replace lines 13-15
  {
    std::ofstream ofs(tmp_path);
    ofs << R"({"db":{"memtable_threshold":65536}})";
  }
```

**Why:** rapidjson cannot parse unquoted keys or missing commas. The test only cares about deadlock avoidance, not config values.

---

### Step 1.2 — Update `LoadFromFileParsesYamlSecurityAndTls` to valid JSON
**Time:** 2 min  
**File:** `tests/storage/test_cedar_config.cc`

- [ ] Replace the YAML block in `LoadFromFileParsesYamlSecurityAndTls` (around lines 68-83) with the equivalent JSON.

```cpp
// Replace the ofs << R"( ... )"; block (lines 68-83)
    ofs << R"({
  "db": {
    "memtable_threshold": 2097152,
    "write_buffer_size": 4194304
  },
  "security": {
    "enable_auth": true,
    "enable_tls": true,
    "jwt_secret": "super-secret-key"
  },
  "tls": {
    "enabled": true,
    "ca_cert": "/etc/certs/ca.crt",
    "server_cert": "/etc/certs/server.crt",
    "server_key": "/etc/certs/server.key",
    "client_cert": "/etc/certs/client.crt",
    "client_key": "/etc/certs/client.key"
  }
})";
```

**Run existing tests (expect some failures because LoadFromFile still skips most sections):**
```bash
cd <repo-root>/build && \
  cmake --build . --target test_cedar_config -- -j$(sysctl -n hw.ncpu) && \
  ./tests/test_cedar_config --gtest_filter='CedarConfigTest.LoadFromFileParsesYamlSecurityAndTls:CedarConfigTest.LoadFromFileParsesJsonSecurityAndTls:CedarConfigTest.SaveAndLoadRoundTripSecurityTls:CedarConfigManagerTest.ReloadConfigDoesNotDeadlock'
```
**Expected:** `SaveAndLoadRoundTripSecurityTls` passes (security+tls are in the old 5 sections). `LoadFromFileParsesYamlSecurityAndTls` and `LoadFromFileParsesJsonSecurityAndTls` pass (db+security+tls are in the old 5 sections). `ReloadConfigDoesNotDeadlock` passes.

---

## Phase 2: Add Comprehensive Round-Trip Test (Failing First)

### Step 2.1 — Append `FullRoundTripAllSections` test
**Time:** 5 min  
**File:** `tests/storage/test_cedar_config.cc`

- [ ] Append the following test after the last existing test (after line 226).

```cpp
TEST(CedarConfigTest, FullRoundTripAllSections) {
  CedarConfig original;
  // Set every field to a non-default value so we can detect if Load missed it.
  original.db.create_if_missing = true;
  original.db.error_if_exists = true;
  original.db.paranoid_checks = true;
  original.db.memtable_threshold = 111111;
  original.db.write_buffer_size = 222222;
  original.db.column_id = 42;
  original.db.enable_bloom_filter = false;
  original.db.bloom_bits_per_key = 7;
  original.db.verify_checksums = false;

  original.lsm.min_files_for_compaction = 3;
  original.lsm.min_size_for_compaction = 333333;
  original.lsm.target_file_size = 444444;
  original.lsm.max_levels = 3;
  original.lsm.level_size_multiplier = 5.5;
  original.lsm.level0_file_num_compaction_trigger = 6;
  original.lsm.level0_slowdown_writes_trigger = 22;
  original.lsm.level0_stop_writes_trigger = 40;

  original.wal.max_file_size = 555555;
  original.wal.group_commit_timeout_us = 2000;
  original.wal.group_commit_max_batch = 2000;
  original.wal.use_fsync = true;
  original.wal.preallocate_size = 666666;
  original.wal.enable_sharded_wal = false;
  original.wal.num_shards = 99;
  original.wal.bind_by_thread_id = false;
  original.wal.max_file_size_per_shard = 777777;
  original.wal.batch_timeout_us = 200;
  original.wal.batch_max_size = 200;
  original.wal.enable_background_merger = false;
  original.wal.merge_interval_ms = 200;

  original.memtable.type = CedarConfig::MemTableType::kMutexBased;
  original.memtable.enable_lockfree_memtable = false;
  original.memtable.initial_capacity = 888888;
  original.memtable.rehash_threshold = 0.5;
  original.memtable.enable_preallocation = false;
  original.memtable.preallocation_pool_size = 5000;
  original.memtable.gc_interval_ms = 500;
  original.memtable.gc_batch_size = 500;

  original.mvcc.enable_sharded_timestamp_allocator = false;
  original.mvcc.timestamp_shard_count = 77;
  original.mvcc.timestamp_batch_size = 777;
  original.mvcc.enable_version_chain_index = false;
  original.mvcc.version_chain_index_threshold = 77;
  original.mvcc.version_chain_max_level = 2;
  original.mvcc.enable_delta_encoding = true;
  original.mvcc.delta_max_per_group = 32;
  original.mvcc.enable_temporal_bloom_filter = false;
  original.mvcc.temporal_filter_false_positive_rate = 0.02;
  original.mvcc.temporal_filter_hours_per_bucket = 48;
  original.mvcc.enable_sharded_wal = false;
  original.mvcc.enable_lockfree_memtable = false;
  original.mvcc.enable_async_index_builder = false;
  original.mvcc.index_builder_worker_threads = 8;
  original.mvcc.index_builder_max_concurrent = 32;
  original.mvcc.index_builder_batch_size = 20;
  original.mvcc.index_builder_batch_timeout_ms = 20;
  original.mvcc.enable_build_cache = false;
  original.mvcc.build_cache_size = 500;
  original.mvcc.enable_deep_integration = false;

  original.transaction.enable_transaction = false;
  original.transaction.default_isolation_level = 1;
  original.transaction.timeout_ms = 60000;
  original.transaction.max_retries = 5;
  original.transaction.parallel_validation = false;
  original.transaction.validation_threads = 8;
  original.transaction.enable_occ = false;
  original.transaction.max_write_set_size = 5000;
  original.transaction.max_read_set_size = 50000;

  original.cache.block_cache_size = 11111111;
  original.cache.table_cache_size = 2048;
  original.cache.block_restart_interval = 32;
  original.cache.block_size = 8192;
  original.cache.version_chain_cache_size = 50000;
  original.cache.enable_version_chain_cache = false;

  original.filesystem.max_open_files = 500;
  original.filesystem.use_direct_io = true;
  original.filesystem.advise_random_access = false;
  original.filesystem.prefetch_buffer_size = 512000;

  original.security.enable_auth = true;
  original.security.enable_tls = true;
  original.security.jwt_secret = "test-jwt-secret-123";

  original.tls.enabled = true;
  original.tls.ca_cert = "/test/ca.crt";
  original.tls.server_cert = "/test/server.crt";
  original.tls.server_key = "/test/server.key";
  original.tls.client_cert = "/test/client.crt";
  original.tls.client_key = "/test/client.key";

  original.debug.enable_stats = false;
  original.debug.stats_dump_interval_sec = 300;
  original.debug.enable_slow_log = false;
  original.debug.slow_log_threshold_ms = 50;
  original.debug.enable_trace = true;

  std::string tmp_path = "/tmp/cedar_test_full_roundtrip_" + std::to_string(getpid()) + ".json";
  Status s = original.SaveToFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "SaveToFile failed: " << s.ToString();

  CedarConfig loaded;
  s = loaded.LoadFromFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "LoadFromFile failed: " << s.ToString();

  // db
  EXPECT_EQ(loaded.db.create_if_missing, original.db.create_if_missing);
  EXPECT_EQ(loaded.db.error_if_exists, original.db.error_if_exists);
  EXPECT_EQ(loaded.db.paranoid_checks, original.db.paranoid_checks);
  EXPECT_EQ(loaded.db.memtable_threshold, original.db.memtable_threshold);
  EXPECT_EQ(loaded.db.write_buffer_size, original.db.write_buffer_size);
  EXPECT_EQ(loaded.db.column_id, original.db.column_id);
  EXPECT_EQ(loaded.db.enable_bloom_filter, original.db.enable_bloom_filter);
  EXPECT_EQ(loaded.db.bloom_bits_per_key, original.db.bloom_bits_per_key);
  EXPECT_EQ(loaded.db.verify_checksums, original.db.verify_checksums);

  // lsm
  EXPECT_EQ(loaded.lsm.min_files_for_compaction, original.lsm.min_files_for_compaction);
  EXPECT_EQ(loaded.lsm.min_size_for_compaction, original.lsm.min_size_for_compaction);
  EXPECT_EQ(loaded.lsm.target_file_size, original.lsm.target_file_size);
  EXPECT_EQ(loaded.lsm.max_levels, original.lsm.max_levels);
  EXPECT_DOUBLE_EQ(loaded.lsm.level_size_multiplier, original.lsm.level_size_multiplier);
  EXPECT_EQ(loaded.lsm.level0_file_num_compaction_trigger,
            original.lsm.level0_file_num_compaction_trigger);
  EXPECT_EQ(loaded.lsm.level0_slowdown_writes_trigger,
            original.lsm.level0_slowdown_writes_trigger);
  EXPECT_EQ(loaded.lsm.level0_stop_writes_trigger,
            original.lsm.level0_stop_writes_trigger);

  // wal
  EXPECT_EQ(loaded.wal.max_file_size, original.wal.max_file_size);
  EXPECT_EQ(loaded.wal.group_commit_timeout_us, original.wal.group_commit_timeout_us);
  EXPECT_EQ(loaded.wal.group_commit_max_batch, original.wal.group_commit_max_batch);
  EXPECT_EQ(loaded.wal.use_fsync, original.wal.use_fsync);
  EXPECT_EQ(loaded.wal.preallocate_size, original.wal.preallocate_size);
  EXPECT_EQ(loaded.wal.enable_sharded_wal, original.wal.enable_sharded_wal);
  EXPECT_EQ(loaded.wal.num_shards, original.wal.num_shards);
  EXPECT_EQ(loaded.wal.bind_by_thread_id, original.wal.bind_by_thread_id);
  EXPECT_EQ(loaded.wal.max_file_size_per_shard, original.wal.max_file_size_per_shard);
  EXPECT_EQ(loaded.wal.batch_timeout_us, original.wal.batch_timeout_us);
  EXPECT_EQ(loaded.wal.batch_max_size, original.wal.batch_max_size);
  EXPECT_EQ(loaded.wal.enable_background_merger, original.wal.enable_background_merger);
  EXPECT_EQ(loaded.wal.merge_interval_ms, original.wal.merge_interval_ms);

  // memtable
  EXPECT_EQ(loaded.memtable.type, original.memtable.type);
  EXPECT_EQ(loaded.memtable.enable_lockfree_memtable,
            original.memtable.enable_lockfree_memtable);
  EXPECT_EQ(loaded.memtable.initial_capacity, original.memtable.initial_capacity);
  EXPECT_DOUBLE_EQ(loaded.memtable.rehash_threshold, original.memtable.rehash_threshold);
  EXPECT_EQ(loaded.memtable.enable_preallocation, original.memtable.enable_preallocation);
  EXPECT_EQ(loaded.memtable.preallocation_pool_size,
            original.memtable.preallocation_pool_size);
  EXPECT_EQ(loaded.memtable.gc_interval_ms, original.memtable.gc_interval_ms);
  EXPECT_EQ(loaded.memtable.gc_batch_size, original.memtable.gc_batch_size);

  // mvcc
  EXPECT_EQ(loaded.mvcc.enable_sharded_timestamp_allocator,
            original.mvcc.enable_sharded_timestamp_allocator);
  EXPECT_EQ(loaded.mvcc.timestamp_shard_count, original.mvcc.timestamp_shard_count);
  EXPECT_EQ(loaded.mvcc.timestamp_batch_size, original.mvcc.timestamp_batch_size);
  EXPECT_EQ(loaded.mvcc.enable_version_chain_index,
            original.mvcc.enable_version_chain_index);
  EXPECT_EQ(loaded.mvcc.version_chain_index_threshold,
            original.mvcc.version_chain_index_threshold);
  EXPECT_EQ(loaded.mvcc.version_chain_max_level, original.mvcc.version_chain_max_level);
  EXPECT_EQ(loaded.mvcc.enable_delta_encoding, original.mvcc.enable_delta_encoding);
  EXPECT_EQ(loaded.mvcc.delta_max_per_group, original.mvcc.delta_max_per_group);
  EXPECT_EQ(loaded.mvcc.enable_temporal_bloom_filter,
            original.mvcc.enable_temporal_bloom_filter);
  EXPECT_DOUBLE_EQ(loaded.mvcc.temporal_filter_false_positive_rate,
                   original.mvcc.temporal_filter_false_positive_rate);
  EXPECT_EQ(loaded.mvcc.temporal_filter_hours_per_bucket,
            original.mvcc.temporal_filter_hours_per_bucket);
  EXPECT_EQ(loaded.mvcc.enable_sharded_wal, original.mvcc.enable_sharded_wal);
  EXPECT_EQ(loaded.mvcc.enable_lockfree_memtable,
            original.mvcc.enable_lockfree_memtable);
  EXPECT_EQ(loaded.mvcc.enable_async_index_builder,
            original.mvcc.enable_async_index_builder);
  EXPECT_EQ(loaded.mvcc.index_builder_worker_threads,
            original.mvcc.index_builder_worker_threads);
  EXPECT_EQ(loaded.mvcc.index_builder_max_concurrent,
            original.mvcc.index_builder_max_concurrent);
  EXPECT_EQ(loaded.mvcc.index_builder_batch_size,
            original.mvcc.index_builder_batch_size);
  EXPECT_EQ(loaded.mvcc.index_builder_batch_timeout_ms,
            original.mvcc.index_builder_batch_timeout_ms);
  EXPECT_EQ(loaded.mvcc.enable_build_cache, original.mvcc.enable_build_cache);
  EXPECT_EQ(loaded.mvcc.build_cache_size, original.mvcc.build_cache_size);
  EXPECT_EQ(loaded.mvcc.enable_deep_integration, original.mvcc.enable_deep_integration);

  // transaction
  EXPECT_EQ(loaded.transaction.enable_transaction, original.transaction.enable_transaction);
  EXPECT_EQ(loaded.transaction.default_isolation_level,
            original.transaction.default_isolation_level);
  EXPECT_EQ(loaded.transaction.timeout_ms, original.transaction.timeout_ms);
  EXPECT_EQ(loaded.transaction.max_retries, original.transaction.max_retries);
  EXPECT_EQ(loaded.transaction.parallel_validation,
            original.transaction.parallel_validation);
  EXPECT_EQ(loaded.transaction.validation_threads, original.transaction.validation_threads);
  EXPECT_EQ(loaded.transaction.enable_occ, original.transaction.enable_occ);
  EXPECT_EQ(loaded.transaction.max_write_set_size, original.transaction.max_write_set_size);
  EXPECT_EQ(loaded.transaction.max_read_set_size, original.transaction.max_read_set_size);

  // cache
  EXPECT_EQ(loaded.cache.block_cache_size, original.cache.block_cache_size);
  EXPECT_EQ(loaded.cache.table_cache_size, original.cache.table_cache_size);
  EXPECT_EQ(loaded.cache.block_restart_interval, original.cache.block_restart_interval);
  EXPECT_EQ(loaded.cache.block_size, original.cache.block_size);
  EXPECT_EQ(loaded.cache.version_chain_cache_size,
            original.cache.version_chain_cache_size);
  EXPECT_EQ(loaded.cache.enable_version_chain_cache,
            original.cache.enable_version_chain_cache);

  // filesystem
  EXPECT_EQ(loaded.filesystem.max_open_files, original.filesystem.max_open_files);
  EXPECT_EQ(loaded.filesystem.use_direct_io, original.filesystem.use_direct_io);
  EXPECT_EQ(loaded.filesystem.advise_random_access, original.filesystem.advise_random_access);
  EXPECT_EQ(loaded.filesystem.prefetch_buffer_size,
            original.filesystem.prefetch_buffer_size);

  // security
  EXPECT_EQ(loaded.security.enable_auth, original.security.enable_auth);
  EXPECT_EQ(loaded.security.enable_tls, original.security.enable_tls);
  EXPECT_EQ(loaded.security.jwt_secret, original.security.jwt_secret);

  // tls
  EXPECT_EQ(loaded.tls.enabled, original.tls.enabled);
  EXPECT_EQ(loaded.tls.ca_cert, original.tls.ca_cert);
  EXPECT_EQ(loaded.tls.server_cert, original.tls.server_cert);
  EXPECT_EQ(loaded.tls.server_key, original.tls.server_key);
  EXPECT_EQ(loaded.tls.client_cert, original.tls.client_cert);
  EXPECT_EQ(loaded.tls.client_key, original.tls.client_key);

  // debug
  EXPECT_EQ(loaded.debug.enable_stats, original.debug.enable_stats);
  EXPECT_EQ(loaded.debug.stats_dump_interval_sec, original.debug.stats_dump_interval_sec);
  EXPECT_EQ(loaded.debug.enable_slow_log, original.debug.enable_slow_log);
  EXPECT_EQ(loaded.debug.slow_log_threshold_ms, original.debug.slow_log_threshold_ms);
  EXPECT_EQ(loaded.debug.enable_trace, original.debug.enable_trace);

  s = loaded.Validate();
  EXPECT_TRUE(s.ok()) << "Validation failed: " << s.ToString();

  std::filesystem::remove(tmp_path);
}
```

**Run the test (expect FAILURE because LoadFromFile currently skips 6 sections):**
```bash
cd <repo-root>/build && \
  cmake --build . --target test_cedar_config -- -j$(sysctl -n hw.ncpu) && \
  ./tests/test_cedar_config --gtest_filter='CedarConfigTest.FullRoundTripAllSections'
```
**Expected:** Multiple assertion failures on memtable, mvcc, transaction, cache, filesystem, and debug fields (they remain at defaults because the old parser ignores those sections).

---

## Phase 3: Rewrite LoadFromFile with rapidjson

### Step 3.1 — Add rapidjson includes
**Time:** 2 min  
**File:** `src/storage/cedar_config.cc`

- [ ] Add rapidjson headers and `<cstdio>` after existing includes (around line 22).

```cpp
// After line 22 (#include <unistd.h>)
#include <cstdio>
#include "butil/third_party/rapidjson/document.h"
#include "butil/third_party/rapidjson/filereadstream.h"
```

**Verify compile:**
```bash
cd <repo-root>/build && \
  cmake --build . --target cedar -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target cedar` (no rapidjson-related errors).

---

### Step 3.2 — Replace `LoadFromFile` body
**Time:** 5 min  
**File:** `src/storage/cedar_config.cc`

- [ ] Delete lines 558-738 (the entire old `LoadFromFile` method).
- [ ] Replace with the new rapidjson-based implementation.

```cpp
Status CedarConfig::LoadFromFile(const std::string& path) {
  FILE* fp = std::fopen(path.c_str(), "rb");
  if (!fp) {
    return Status::IOError("Cannot open config file: " + path);
  }

  char readBuffer[65536];
  butil::rapidjson::FileReadStream is(fp, readBuffer, sizeof(readBuffer));
  butil::rapidjson::Document doc;
  doc.ParseStream(is);
  std::fclose(fp);

  if (doc.HasParseError()) {
    return Status::InvalidArgument(
        std::string("JSON parse error at offset ") +
        std::to_string(doc.GetErrorOffset()));
  }
  if (!doc.IsObject()) {
    return Status::InvalidArgument("Config file root must be a JSON object");
  }

  // ---------------------------------------------------------------------------
  // Type-safe helper lambdas (local to this function)
  // ---------------------------------------------------------------------------
  auto get_bool = [&](const butil::rapidjson::Value& obj, const char* key,
                      bool def) -> bool {
    if (!obj.HasMember(key)) return def;
    const auto& v = obj[key];
    if (v.IsBool()) return v.GetBool();
    if (v.IsInt()) return v.GetInt() != 0;
    if (v.IsUint()) return v.GetUint() != 0;
    if (v.IsString()) {
      std::string s(v.GetString(), v.GetStringLength());
      for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      return s == "true" || s == "yes" || s == "1" || s == "on";
    }
    return def;
  };

  auto get_uint32 = [&](const butil::rapidjson::Value& obj, const char* key,
                        uint32_t def) -> uint32_t {
    if (!obj.HasMember(key)) return def;
    const auto& v = obj[key];
    if (v.IsUint()) return v.GetUint();
    if (v.IsInt()) return static_cast<uint32_t>(v.GetInt());
    if (v.IsUint64()) return static_cast<uint32_t>(v.GetUint64());
    if (v.IsInt64()) return static_cast<uint32_t>(v.GetInt64());
    if (v.IsString()) {
      try {
        return static_cast<uint32_t>(
            std::stoul(std::string(v.GetString(), v.GetStringLength())));
      } catch (...) {
        return def;
      }
    }
    return def;
  };

  auto get_size_t = [&](const butil::rapidjson::Value& obj, const char* key,
                        size_t def) -> size_t {
    if (!obj.HasMember(key)) return def;
    const auto& v = obj[key];
    if (v.IsUint64()) return static_cast<size_t>(v.GetUint64());
    if (v.IsUint()) return static_cast<size_t>(v.GetUint());
    if (v.IsInt64()) return static_cast<size_t>(v.GetInt64());
    if (v.IsInt()) return static_cast<size_t>(v.GetInt());
    if (v.IsString()) {
      try {
        return static_cast<size_t>(
            std::stoull(std::string(v.GetString(), v.GetStringLength())));
      } catch (...) {
        return def;
      }
    }
    return def;
  };

  auto get_int = [&](const butil::rapidjson::Value& obj, const char* key,
                     int def) -> int {
    if (!obj.HasMember(key)) return def;
    const auto& v = obj[key];
    if (v.IsInt()) return v.GetInt();
    if (v.IsUint()) return static_cast<int>(v.GetUint());
    if (v.IsString()) {
      try {
        return std::stoi(std::string(v.GetString(), v.GetStringLength()));
      } catch (...) {
        return def;
      }
    }
    return def;
  };

  auto get_double = [&](const butil::rapidjson::Value& obj, const char* key,
                        double def) -> double {
    if (!obj.HasMember(key)) return def;
    const auto& v = obj[key];
    if (v.IsDouble()) return v.GetDouble();
    if (v.IsInt()) return static_cast<double>(v.GetInt());
    if (v.IsUint()) return static_cast<double>(v.GetUint());
    if (v.IsInt64()) return static_cast<double>(v.GetInt64());
    if (v.IsUint64()) return static_cast<double>(v.GetUint64());
    if (v.IsString()) {
      try {
        return std::stod(std::string(v.GetString(), v.GetStringLength()));
      } catch (...) {
        return def;
      }
    }
    return def;
  };

  auto get_string = [&](const butil::rapidjson::Value& obj, const char* key,
                        const std::string& def) -> std::string {
    if (!obj.HasMember(key)) return def;
    const auto& v = obj[key];
    if (v.IsString()) return std::string(v.GetString(), v.GetStringLength());
    return def;
  };

  // ---------------------------------------------------------------------------
  // Optional version check
  // ---------------------------------------------------------------------------
  if (doc.HasMember("version") && doc["version"].IsUint()) {
    uint32_t file_version = doc["version"].GetUint();
    (void)file_version;  // Reserved for future migration logic
  }

  // ---------------------------------------------------------------------------
  // 1. db
  // ---------------------------------------------------------------------------
  if (doc.HasMember("db") && doc["db"].IsObject()) {
    const auto& o = doc["db"];
    db.create_if_missing = get_bool(o, "create_if_missing", db.create_if_missing);
    db.error_if_exists = get_bool(o, "error_if_exists", db.error_if_exists);
    db.paranoid_checks = get_bool(o, "paranoid_checks", db.paranoid_checks);
    db.memtable_threshold = get_size_t(o, "memtable_threshold", db.memtable_threshold);
    db.write_buffer_size = get_size_t(o, "write_buffer_size", db.write_buffer_size);
    db.column_id = static_cast<uint16_t>(get_uint32(o, "column_id", db.column_id));
    db.enable_bloom_filter = get_bool(o, "enable_bloom_filter", db.enable_bloom_filter);
    db.bloom_bits_per_key = get_int(o, "bloom_bits_per_key", db.bloom_bits_per_key);
    db.verify_checksums = get_bool(o, "verify_checksums", db.verify_checksums);
  }

  // ---------------------------------------------------------------------------
  // 2. lsm
  // ---------------------------------------------------------------------------
  if (doc.HasMember("lsm") && doc["lsm"].IsObject()) {
    const auto& o = doc["lsm"];
    lsm.min_files_for_compaction =
        get_size_t(o, "min_files_for_compaction", lsm.min_files_for_compaction);
    lsm.min_size_for_compaction =
        get_size_t(o, "min_size_for_compaction", lsm.min_size_for_compaction);
    lsm.target_file_size = get_size_t(o, "target_file_size", lsm.target_file_size);
    lsm.max_levels = get_int(o, "max_levels", lsm.max_levels);
    lsm.level_size_multiplier =
        get_double(o, "level_size_multiplier", lsm.level_size_multiplier);
    lsm.level0_file_num_compaction_trigger =
        get_size_t(o, "level0_file_num_compaction_trigger",
                   lsm.level0_file_num_compaction_trigger);
    lsm.level0_slowdown_writes_trigger =
        get_size_t(o, "level0_slowdown_writes_trigger",
                   lsm.level0_slowdown_writes_trigger);
    lsm.level0_stop_writes_trigger =
        get_size_t(o, "level0_stop_writes_trigger",
                   lsm.level0_stop_writes_trigger);
  }

  // ---------------------------------------------------------------------------
  // 3. wal
  // ---------------------------------------------------------------------------
  if (doc.HasMember("wal") && doc["wal"].IsObject()) {
    const auto& o = doc["wal"];
    wal.max_file_size = get_size_t(o, "max_file_size", wal.max_file_size);
    wal.group_commit_timeout_us =
        get_uint32(o, "group_commit_timeout_us", wal.group_commit_timeout_us);
    wal.group_commit_max_batch =
        get_size_t(o, "group_commit_max_batch", wal.group_commit_max_batch);
    wal.use_fsync = get_bool(o, "use_fsync", wal.use_fsync);
    wal.preallocate_size = get_size_t(o, "preallocate_size", wal.preallocate_size);
    wal.enable_sharded_wal = get_bool(o, "enable_sharded_wal", wal.enable_sharded_wal);
    wal.num_shards = get_uint32(o, "num_shards", wal.num_shards);
    wal.bind_by_thread_id = get_bool(o, "bind_by_thread_id", wal.bind_by_thread_id);
    wal.max_file_size_per_shard =
        get_size_t(o, "max_file_size_per_shard", wal.max_file_size_per_shard);
    wal.batch_timeout_us = get_uint32(o, "batch_timeout_us", wal.batch_timeout_us);
    wal.batch_max_size = get_size_t(o, "batch_max_size", wal.batch_max_size);
    wal.enable_background_merger =
        get_bool(o, "enable_background_merger", wal.enable_background_merger);
    wal.merge_interval_ms = get_uint32(o, "merge_interval_ms", wal.merge_interval_ms);
  }

  // ---------------------------------------------------------------------------
  // 4. memtable
  // ---------------------------------------------------------------------------
  if (doc.HasMember("memtable") && doc["memtable"].IsObject()) {
    const auto& o = doc["memtable"];
    if (o.HasMember("type")) {
      const auto& v = o["type"];
      int t = -1;
      if (v.IsInt()) t = v.GetInt();
      else if (v.IsUint()) t = static_cast<int>(v.GetUint());
      if (t == 0 || t == 1) memtable.type = static_cast<decltype(memtable.type)>(t);
    }
    memtable.enable_lockfree_memtable =
        get_bool(o, "enable_lockfree_memtable", memtable.enable_lockfree_memtable);
    memtable.initial_capacity =
        get_size_t(o, "initial_capacity", memtable.initial_capacity);
    memtable.rehash_threshold =
        get_double(o, "rehash_threshold", memtable.rehash_threshold);
    memtable.enable_preallocation =
        get_bool(o, "enable_preallocation", memtable.enable_preallocation);
    memtable.preallocation_pool_size =
        get_size_t(o, "preallocation_pool_size", memtable.preallocation_pool_size);
    memtable.gc_interval_ms =
        get_uint32(o, "gc_interval_ms", memtable.gc_interval_ms);
    memtable.gc_batch_size =
        get_size_t(o, "gc_batch_size", memtable.gc_batch_size);
  }

  // ---------------------------------------------------------------------------
  // 5. mvcc
  // ---------------------------------------------------------------------------
  if (doc.HasMember("mvcc") && doc["mvcc"].IsObject()) {
    const auto& o = doc["mvcc"];
    mvcc.enable_sharded_timestamp_allocator =
        get_bool(o, "enable_sharded_timestamp_allocator",
                 mvcc.enable_sharded_timestamp_allocator);
    mvcc.timestamp_shard_count =
        get_uint32(o, "timestamp_shard_count", mvcc.timestamp_shard_count);
    mvcc.timestamp_batch_size =
        get_uint32(o, "timestamp_batch_size", mvcc.timestamp_batch_size);
    mvcc.enable_version_chain_index =
        get_bool(o, "enable_version_chain_index",
                 mvcc.enable_version_chain_index);
    mvcc.version_chain_index_threshold =
        get_size_t(o, "version_chain_index_threshold",
                   mvcc.version_chain_index_threshold);
    mvcc.version_chain_max_level =
        get_int(o, "version_chain_max_level", mvcc.version_chain_max_level);
    mvcc.enable_delta_encoding =
        get_bool(o, "enable_delta_encoding", mvcc.enable_delta_encoding);
    mvcc.delta_max_per_group =
        get_size_t(o, "delta_max_per_group", mvcc.delta_max_per_group);
    mvcc.enable_temporal_bloom_filter =
        get_bool(o, "enable_temporal_bloom_filter",
                 mvcc.enable_temporal_bloom_filter);
    mvcc.temporal_filter_false_positive_rate =
        get_double(o, "temporal_filter_false_positive_rate",
                   mvcc.temporal_filter_false_positive_rate);
    mvcc.temporal_filter_hours_per_bucket =
        get_uint32(o, "temporal_filter_hours_per_bucket",
                   mvcc.temporal_filter_hours_per_bucket);
    mvcc.enable_sharded_wal =
        get_bool(o, "enable_sharded_wal", mvcc.enable_sharded_wal);
    mvcc.enable_lockfree_memtable =
        get_bool(o, "enable_lockfree_memtable",
                 mvcc.enable_lockfree_memtable);
    mvcc.enable_async_index_builder =
        get_bool(o, "enable_async_index_builder",
                 mvcc.enable_async_index_builder);
    mvcc.index_builder_worker_threads =
        get_uint32(o, "index_builder_worker_threads",
                   mvcc.index_builder_worker_threads);
    mvcc.index_builder_max_concurrent =
        get_uint32(o, "index_builder_max_concurrent",
                   mvcc.index_builder_max_concurrent);
    mvcc.index_builder_batch_size =
        get_size_t(o, "index_builder_batch_size",
                   mvcc.index_builder_batch_size);
    mvcc.index_builder_batch_timeout_ms =
        get_uint32(o, "index_builder_batch_timeout_ms",
                   mvcc.index_builder_batch_timeout_ms);
    mvcc.enable_build_cache =
        get_bool(o, "enable_build_cache", mvcc.enable_build_cache);
    mvcc.build_cache_size =
        get_size_t(o, "build_cache_size", mvcc.build_cache_size);
    mvcc.enable_deep_integration =
        get_bool(o, "enable_deep_integration", mvcc.enable_deep_integration);
  }

  // ---------------------------------------------------------------------------
  // 6. transaction
  // ---------------------------------------------------------------------------
  if (doc.HasMember("transaction") && doc["transaction"].IsObject()) {
    const auto& o = doc["transaction"];
    transaction.enable_transaction =
        get_bool(o, "enable_transaction", transaction.enable_transaction);
    transaction.default_isolation_level =
        get_int(o, "default_isolation_level",
                transaction.default_isolation_level);
    transaction.timeout_ms =
        get_size_t(o, "timeout_ms", transaction.timeout_ms);
    transaction.max_retries =
        get_uint32(o, "max_retries", transaction.max_retries);
    transaction.parallel_validation =
        get_bool(o, "parallel_validation", transaction.parallel_validation);
    transaction.validation_threads =
        get_uint32(o, "validation_threads", transaction.validation_threads);
    transaction.enable_occ =
        get_bool(o, "enable_occ", transaction.enable_occ);
    transaction.max_write_set_size =
        get_size_t(o, "max_write_set_size", transaction.max_write_set_size);
    transaction.max_read_set_size =
        get_size_t(o, "max_read_set_size", transaction.max_read_set_size);
  }

  // ---------------------------------------------------------------------------
  // 7. cache
  // ---------------------------------------------------------------------------
  if (doc.HasMember("cache") && doc["cache"].IsObject()) {
    const auto& o = doc["cache"];
    cache.block_cache_size =
        get_size_t(o, "block_cache_size", cache.block_cache_size);
    cache.table_cache_size =
        get_size_t(o, "table_cache_size", cache.table_cache_size);
    cache.block_restart_interval =
        get_int(o, "block_restart_interval", cache.block_restart_interval);
    cache.block_size = get_size_t(o, "block_size", cache.block_size);
    cache.version_chain_cache_size =
        get_size_t(o, "version_chain_cache_size",
                   cache.version_chain_cache_size);
    cache.enable_version_chain_cache =
        get_bool(o, "enable_version_chain_cache",
                 cache.enable_version_chain_cache);
  }

  // ---------------------------------------------------------------------------
  // 8. filesystem
  // ---------------------------------------------------------------------------
  if (doc.HasMember("filesystem") && doc["filesystem"].IsObject()) {
    const auto& o = doc["filesystem"];
    filesystem.max_open_files =
        get_int(o, "max_open_files", filesystem.max_open_files);
    filesystem.use_direct_io =
        get_bool(o, "use_direct_io", filesystem.use_direct_io);
    filesystem.advise_random_access =
        get_bool(o, "advise_random_access",
                 filesystem.advise_random_access);
    filesystem.prefetch_buffer_size =
        get_size_t(o, "prefetch_buffer_size",
                   filesystem.prefetch_buffer_size);
  }

  // ---------------------------------------------------------------------------
  // 9. security
  // ---------------------------------------------------------------------------
  if (doc.HasMember("security") && doc["security"].IsObject()) {
    const auto& o = doc["security"];
    security.enable_auth = get_bool(o, "enable_auth", security.enable_auth);
    security.enable_tls = get_bool(o, "enable_tls", security.enable_tls);
    security.jwt_secret = get_string(o, "jwt_secret", security.jwt_secret);
  }

  // ---------------------------------------------------------------------------
  // 10. tls
  // ---------------------------------------------------------------------------
  if (doc.HasMember("tls") && doc["tls"].IsObject()) {
    const auto& o = doc["tls"];
    tls.enabled = get_bool(o, "enabled", tls.enabled);
    tls.ca_cert = get_string(o, "ca_cert", tls.ca_cert);
    tls.server_cert = get_string(o, "server_cert", tls.server_cert);
    tls.server_key = get_string(o, "server_key", tls.server_key);
    tls.client_cert = get_string(o, "client_cert", tls.client_cert);
    tls.client_key = get_string(o, "client_key", tls.client_key);
  }

  // ---------------------------------------------------------------------------
  // 11. debug
  // ---------------------------------------------------------------------------
  if (doc.HasMember("debug") && doc["debug"].IsObject()) {
    const auto& o = doc["debug"];
    debug.enable_stats = get_bool(o, "enable_stats", debug.enable_stats);
    debug.stats_dump_interval_sec =
        get_uint32(o, "stats_dump_interval_sec",
                   debug.stats_dump_interval_sec);
    debug.enable_slow_log =
        get_bool(o, "enable_slow_log", debug.enable_slow_log);
    debug.slow_log_threshold_ms =
        get_uint32(o, "slow_log_threshold_ms",
                   debug.slow_log_threshold_ms);
    debug.enable_trace = get_bool(o, "enable_trace", debug.enable_trace);
  }

  return Status::OK();
}
```

**Run the full round-trip test:**
```bash
cd <repo-root>/build && \
  cmake --build . --target test_cedar_config -- -j$(sysctl -n hw.ncpu) && \
  ./tests/test_cedar_config --gtest_filter='CedarConfigTest.FullRoundTripAllSections'
```
**Expected:** `PASSED`.

---

## Phase 4: Rewrite SaveToFile with rapidjson PrettyWriter

### Step 4.1 — Add PrettyWriter include
**Time:** 1 min  
**File:** `src/storage/cedar_config.cc`

- [ ] Add PrettyWriter header alongside the other rapidjson includes (line 23-24).

```cpp
#include "butil/third_party/rapidjson/stringbuffer.h"
#include "butil/third_party/rapidjson/prettywriter.h"
```

---

### Step 4.2 — Replace `SaveToFile` body and delete dead helpers
**Time:** 5 min  
**File:** `src/storage/cedar_config.cc`

- [ ] Delete lines 740-783 (`JsonEscape`, `JsonKV`, `ToStr`, `BoolStr` helpers) and lines 785-979 (old `SaveToFile` body).
- [ ] Replace with the rapidjson-based implementation below.

```cpp
Status CedarConfig::SaveToFile(const std::string& path) const {
  butil::rapidjson::StringBuffer buffer;
  butil::rapidjson::PrettyWriter<butil::rapidjson::StringBuffer> writer(buffer);
  writer.SetIndent(' ', 2);

  writer.StartObject();

  writer.Key("version");
  writer.Uint(kVersion);

  // -------------------------------------------------------------------------
  // db
  // -------------------------------------------------------------------------
  writer.Key("db");
  writer.StartObject();
  writer.Key("create_if_missing"); writer.Bool(db.create_if_missing);
  writer.Key("error_if_exists"); writer.Bool(db.error_if_exists);
  writer.Key("paranoid_checks"); writer.Bool(db.paranoid_checks);
  writer.Key("memtable_threshold");
  writer.Uint64(static_cast<uint64_t>(db.memtable_threshold));
  writer.Key("write_buffer_size");
  writer.Uint64(static_cast<uint64_t>(db.write_buffer_size));
  writer.Key("column_id"); writer.Uint(db.column_id);
  writer.Key("enable_bloom_filter"); writer.Bool(db.enable_bloom_filter);
  writer.Key("bloom_bits_per_key"); writer.Int(db.bloom_bits_per_key);
  writer.Key("verify_checksums"); writer.Bool(db.verify_checksums);
  writer.EndObject();

  // -------------------------------------------------------------------------
  // lsm
  // -------------------------------------------------------------------------
  writer.Key("lsm");
  writer.StartObject();
  writer.Key("min_files_for_compaction");
  writer.Uint64(static_cast<uint64_t>(lsm.min_files_for_compaction));
  writer.Key("min_size_for_compaction");
  writer.Uint64(static_cast<uint64_t>(lsm.min_size_for_compaction));
  writer.Key("target_file_size");
  writer.Uint64(static_cast<uint64_t>(lsm.target_file_size));
  writer.Key("max_levels"); writer.Int(lsm.max_levels);
  writer.Key("level_size_multiplier"); writer.Double(lsm.level_size_multiplier);
  writer.Key("level0_file_num_compaction_trigger");
  writer.Uint64(static_cast<uint64_t>(lsm.level0_file_num_compaction_trigger));
  writer.Key("level0_slowdown_writes_trigger");
  writer.Uint64(static_cast<uint64_t>(lsm.level0_slowdown_writes_trigger));
  writer.Key("level0_stop_writes_trigger");
  writer.Uint64(static_cast<uint64_t>(lsm.level0_stop_writes_trigger));
  writer.EndObject();

  // -------------------------------------------------------------------------
  // wal
  // -------------------------------------------------------------------------
  writer.Key("wal");
  writer.StartObject();
  writer.Key("max_file_size");
  writer.Uint64(static_cast<uint64_t>(wal.max_file_size));
  writer.Key("group_commit_timeout_us"); writer.Uint(wal.group_commit_timeout_us);
  writer.Key("group_commit_max_batch");
  writer.Uint64(static_cast<uint64_t>(wal.group_commit_max_batch));
  writer.Key("use_fsync"); writer.Bool(wal.use_fsync);
  writer.Key("preallocate_size");
  writer.Uint64(static_cast<uint64_t>(wal.preallocate_size));
  writer.Key("enable_sharded_wal"); writer.Bool(wal.enable_sharded_wal);
  writer.Key("num_shards"); writer.Uint(wal.num_shards);
  writer.Key("bind_by_thread_id"); writer.Bool(wal.bind_by_thread_id);
  writer.Key("max_file_size_per_shard");
  writer.Uint64(static_cast<uint64_t>(wal.max_file_size_per_shard));
  writer.Key("batch_timeout_us"); writer.Uint(wal.batch_timeout_us);
  writer.Key("batch_max_size");
  writer.Uint64(static_cast<uint64_t>(wal.batch_max_size));
  writer.Key("enable_background_merger");
  writer.Bool(wal.enable_background_merger);
  writer.Key("merge_interval_ms"); writer.Uint(wal.merge_interval_ms);
  writer.EndObject();

  // -------------------------------------------------------------------------
  // memtable
  // -------------------------------------------------------------------------
  writer.Key("memtable");
  writer.StartObject();
  writer.Key("type"); writer.Int(static_cast<int>(memtable.type));
  writer.Key("enable_lockfree_memtable");
  writer.Bool(memtable.enable_lockfree_memtable);
  writer.Key("initial_capacity");
  writer.Uint64(static_cast<uint64_t>(memtable.initial_capacity));
  writer.Key("rehash_threshold"); writer.Double(memtable.rehash_threshold);
  writer.Key("enable_preallocation");
  writer.Bool(memtable.enable_preallocation);
  writer.Key("preallocation_pool_size");
  writer.Uint64(static_cast<uint64_t>(memtable.preallocation_pool_size));
  writer.Key("gc_interval_ms"); writer.Uint(memtable.gc_interval_ms);
  writer.Key("gc_batch_size");
  writer.Uint64(static_cast<uint64_t>(memtable.gc_batch_size));
  writer.EndObject();

  // -------------------------------------------------------------------------
  // mvcc
  // -------------------------------------------------------------------------
  writer.Key("mvcc");
  writer.StartObject();
  writer.Key("enable_sharded_timestamp_allocator");
  writer.Bool(mvcc.enable_sharded_timestamp_allocator);
  writer.Key("timestamp_shard_count"); writer.Uint(mvcc.timestamp_shard_count);
  writer.Key("timestamp_batch_size"); writer.Uint(mvcc.timestamp_batch_size);
  writer.Key("enable_version_chain_index");
  writer.Bool(mvcc.enable_version_chain_index);
  writer.Key("version_chain_index_threshold");
  writer.Uint64(static_cast<uint64_t>(mvcc.version_chain_index_threshold));
  writer.Key("version_chain_max_level"); writer.Int(mvcc.version_chain_max_level);
  writer.Key("enable_delta_encoding"); writer.Bool(mvcc.enable_delta_encoding);
  writer.Key("delta_max_per_group");
  writer.Uint64(static_cast<uint64_t>(mvcc.delta_max_per_group));
  writer.Key("enable_temporal_bloom_filter");
  writer.Bool(mvcc.enable_temporal_bloom_filter);
  writer.Key("temporal_filter_false_positive_rate");
  writer.Double(mvcc.temporal_filter_false_positive_rate);
  writer.Key("temporal_filter_hours_per_bucket");
  writer.Uint(mvcc.temporal_filter_hours_per_bucket);
  writer.Key("enable_sharded_wal"); writer.Bool(mvcc.enable_sharded_wal);
  writer.Key("enable_lockfree_memtable");
  writer.Bool(mvcc.enable_lockfree_memtable);
  writer.Key("enable_async_index_builder");
  writer.Bool(mvcc.enable_async_index_builder);
  writer.Key("index_builder_worker_threads");
  writer.Uint(mvcc.index_builder_worker_threads);
  writer.Key("index_builder_max_concurrent");
  writer.Uint(mvcc.index_builder_max_concurrent);
  writer.Key("index_builder_batch_size");
  writer.Uint64(static_cast<uint64_t>(mvcc.index_builder_batch_size));
  writer.Key("index_builder_batch_timeout_ms");
  writer.Uint(mvcc.index_builder_batch_timeout_ms);
  writer.Key("enable_build_cache"); writer.Bool(mvcc.enable_build_cache);
  writer.Key("build_cache_size");
  writer.Uint64(static_cast<uint64_t>(mvcc.build_cache_size));
  writer.Key("enable_deep_integration");
  writer.Bool(mvcc.enable_deep_integration);
  writer.EndObject();

  // -------------------------------------------------------------------------
  // transaction
  // -------------------------------------------------------------------------
  writer.Key("transaction");
  writer.StartObject();
  writer.Key("enable_transaction"); writer.Bool(transaction.enable_transaction);
  writer.Key("default_isolation_level");
  writer.Int(transaction.default_isolation_level);
  writer.Key("timeout_ms");
  writer.Uint64(static_cast<uint64_t>(transaction.timeout_ms));
  writer.Key("max_retries"); writer.Uint(transaction.max_retries);
  writer.Key("parallel_validation");
  writer.Bool(transaction.parallel_validation);
  writer.Key("validation_threads"); writer.Uint(transaction.validation_threads);
  writer.Key("enable_occ"); writer.Bool(transaction.enable_occ);
  writer.Key("max_write_set_size");
  writer.Uint64(static_cast<uint64_t>(transaction.max_write_set_size));
  writer.Key("max_read_set_size");
  writer.Uint64(static_cast<uint64_t>(transaction.max_read_set_size));
  writer.EndObject();

  // -------------------------------------------------------------------------
  // cache
  // -------------------------------------------------------------------------
  writer.Key("cache");
  writer.StartObject();
  writer.Key("block_cache_size");
  writer.Uint64(static_cast<uint64_t>(cache.block_cache_size));
  writer.Key("table_cache_size");
  writer.Uint64(static_cast<uint64_t>(cache.table_cache_size));
  writer.Key("block_restart_interval");
  writer.Int(cache.block_restart_interval);
  writer.Key("block_size");
  writer.Uint64(static_cast<uint64_t>(cache.block_size));
  writer.Key("version_chain_cache_size");
  writer.Uint64(static_cast<uint64_t>(cache.version_chain_cache_size));
  writer.Key("enable_version_chain_cache");
  writer.Bool(cache.enable_version_chain_cache);
  writer.EndObject();

  // -------------------------------------------------------------------------
  // filesystem
  // -------------------------------------------------------------------------
  writer.Key("filesystem");
  writer.StartObject();
  writer.Key("max_open_files"); writer.Int(filesystem.max_open_files);
  writer.Key("use_direct_io"); writer.Bool(filesystem.use_direct_io);
  writer.Key("advise_random_access");
  writer.Bool(filesystem.advise_random_access);
  writer.Key("prefetch_buffer_size");
  writer.Uint64(static_cast<uint64_t>(filesystem.prefetch_buffer_size));
  writer.EndObject();

  // -------------------------------------------------------------------------
  // security
  // -------------------------------------------------------------------------
  writer.Key("security");
  writer.StartObject();
  writer.Key("enable_auth"); writer.Bool(security.enable_auth);
  writer.Key("enable_tls"); writer.Bool(security.enable_tls);
  writer.Key("jwt_secret"); writer.String(security.jwt_secret.c_str());
  writer.EndObject();

  // -------------------------------------------------------------------------
  // tls
  // -------------------------------------------------------------------------
  writer.Key("tls");
  writer.StartObject();
  writer.Key("enabled"); writer.Bool(tls.enabled);
  writer.Key("ca_cert"); writer.String(tls.ca_cert.c_str());
  writer.Key("server_cert"); writer.String(tls.server_cert.c_str());
  writer.Key("server_key"); writer.String(tls.server_key.c_str());
  writer.Key("client_cert"); writer.String(tls.client_cert.c_str());
  writer.Key("client_key"); writer.String(tls.client_key.c_str());
  writer.EndObject();

  // -------------------------------------------------------------------------
  // debug
  // -------------------------------------------------------------------------
  writer.Key("debug");
  writer.StartObject();
  writer.Key("enable_stats"); writer.Bool(debug.enable_stats);
  writer.Key("stats_dump_interval_sec");
  writer.Uint(debug.stats_dump_interval_sec);
  writer.Key("enable_slow_log"); writer.Bool(debug.enable_slow_log);
  writer.Key("slow_log_threshold_ms");
  writer.Uint(debug.slow_log_threshold_ms);
  writer.Key("enable_trace"); writer.Bool(debug.enable_trace);
  writer.EndObject();

  writer.EndObject();

  std::string json(buffer.GetString(), buffer.GetSize());

  // Atomic write: temp file -> fsync -> rename
  std::string tmp_path = path + ".tmp";
  std::ofstream ofs(tmp_path, std::ios::binary);
  if (!ofs.is_open()) {
    return Status::IOError("SaveToFile", "Cannot open temp file: " + tmp_path);
  }
  ofs.write(json.data(), static_cast<std::streamsize>(json.size()));
  ofs.flush();
  ofs.close();

  if (!ofs.good()) {
    std::filesystem::remove(tmp_path);
    return Status::IOError("SaveToFile", "Failed to write temp file: " + tmp_path);
  }

  int fd = ::open(tmp_path.c_str(), O_RDONLY);
  if (fd >= 0) {
#if defined(__APPLE__)
    ::fcntl(fd, F_FULLFSYNC);
#else
    ::fsync(fd);
#endif
    ::close(fd);
  }

  try {
    std::filesystem::rename(tmp_path, path);
  } catch (const std::exception& e) {
    std::filesystem::remove(tmp_path);
    return Status::IOError("SaveToFile",
                           std::string("Failed to rename temp file: ") + e.what());
  }

  std::string dir_path = ".";
  auto last_slash = path.rfind('/');
  if (last_slash != std::string::npos) {
    dir_path = path.substr(0, last_slash);
  }
  int dir_fd = ::open(dir_path.c_str(), O_RDONLY);
  if (dir_fd >= 0) {
#if defined(__APPLE__)
    ::fcntl(dir_fd, F_FULLFSYNC);
#else
    ::fsync(dir_fd);
#endif
    ::close(dir_fd);
  }

  return Status::OK();
}
```

**Run all config tests:**
```bash
cd <repo-root>/build && \
  cmake --build . --target test_cedar_config -- -j$(sysctl -n hw.ncpu) && \
  ./tests/test_cedar_config
```
**Expected:** All tests pass.

---

## Phase 5: Cleanup Dead Code

### Step 5.1 — Remove obsolete helpers
**Time:** 2 min  
**File:** `src/storage/cedar_config.cc`

- [ ] Delete the old anonymous-namespace helpers that are no longer referenced:
  - `Trim` (lines 521-532)
  - `Unquote` (lines 534-541)
  - `ParseBool` (lines 543-554)

*(The `JsonEscape`, `JsonKV`, `ToStr`, `BoolStr` helpers were already removed in Step 4.2.)*

**Verify compile:**
```bash
cd <repo-root>/build && \
  cmake --build . --target cedar -- -j$(sysctl -n hw.ncpu) 2>&1 | tail -5
```
**Expected:** `[100%] Built target cedar` with zero warnings about unused functions.

---

## Phase 6: Final Verification

### Step 6.1 — Run full test suite
**Time:** 3 min  

- [ ] Run all `test_cedar_config` tests plus the config-manager test.

```bash
cd <repo-root>/build && \
  cmake --build . --target test_cedar_config -- -j$(sysctl -n hw.ncpu) && \
  ./tests/test_cedar_config && \
  cmake --build . --target test_config_manager -- -j$(sysctl -n hw.ncpu) && \
  ./tests/test_config_manager
```

**Expected output:**
```
[==========] Running N tests from M test suites
[==========] N tests from M test suites ran
[  PASSED  ] N tests.
```

---

### Step 6.2 — Commit
**Time:** 1 min  

```bash
cd <repo-root> && \
  git add src/storage/cedar_config.cc tests/storage/test_cedar_config.cc && \
  git commit -m "feat(config): replace hand-rolled parser with rapidjson

- LoadFromFile now parses all 11 sections via rapidjson Document
- SaveToFile rewritten with rapidjson PrettyWriter for correctness
- Full round-trip test covers every scalar field
- Removes 200+ lines of ad-hoc Trim/Unquote/ParseBool/JsonEscape helpers
- Updates existing tests to use canonical JSON (the only format SaveToFile emits)"
```

---

## Appendix A: rapidjson Type Mapping Reference

| C++ Type | rapidjson Check | rapidjson Getter | Helper |
|----------|-----------------|------------------|--------|
| `bool` | `IsBool()` | `GetBool()` | `get_bool` |
| `uint32_t` | `IsUint()` | `GetUint()` | `get_uint32` |
| `int` | `IsInt()` | `GetInt()` | `get_int` |
| `size_t` | `IsUint64()` | `GetUint64()` | `get_size_t` |
| `double` | `IsDouble()` | `GetDouble()` | `get_double` |
| `std::string` | `IsString()` | `GetString()` + `GetStringLength()` | `get_string` |
| enum | `IsInt()` / `IsUint()` | `GetInt()` / `GetUint()` | cast after range check |

**Coercion rules in helpers:** If the JSON value has the wrong type but can be safely coerced (e.g., integer stored as string), the helpers attempt `std::stoi` / `std::stoul` / `std::stoull` / `std::stod`. If coercion fails, the default value is preserved.

---

## Appendix B: Backwards Compatibility Notes

| Aspect | Before | After |
|--------|--------|-------|
| Accepted input format | Loose YAML-like + limited JSON | Strict JSON only |
| Sections parsed by `LoadFromFile` | 5 (`db`, `lsm`, `wal`, `security`, `tls`) | All 11 |
| Nested objects | Ignored / broken | Fully supported |
| Arrays | Not handled | Gracefully skipped |
| JSON escaping in `SaveToFile` | Hand-rolled (`JsonEscape`) | `rapidjson::PrettyWriter` |
| Atomic write | Preserved | Preserved |

**Migration path:** Any existing hand-written config files in YAML format must be converted to JSON. This is acceptable because `SaveToFile` is the canonical producer and has always emitted JSON. The two tests that previously fed YAML into `LoadFromFile` are updated to use JSON in Step 1.1 and Step 1.2.
