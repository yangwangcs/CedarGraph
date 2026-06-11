#include <gtest/gtest.h>
#include <fstream>
#include <future>
#include <chrono>
#include <filesystem>
#include "cedar/storage/cedar_config.h"

using namespace cedar;

TEST(CedarConfigManagerTest, ReloadConfigDoesNotDeadlock) {
  auto* mgr = cedar::CedarConfigManager::Instance();
  std::string tmp_path = "/tmp/cedar_test_config_" + std::to_string(getpid()) + ".json";
  {
    std::ofstream ofs(tmp_path);
    ofs << R"({"db":{"memtable_threshold":65536}})";
  }
  auto s = mgr->LoadConfig(tmp_path);
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::future<cedar::Status> fut = std::async(std::launch::async, [mgr]() {
    return mgr->ReloadConfig();
  });
  auto status = fut.wait_for(std::chrono::seconds(2));
  EXPECT_NE(status, std::future_status::timeout) << "ReloadConfig deadlocked!";
  if (status == std::future_status::ready) {
    auto result = fut.get();
    EXPECT_TRUE(result.ok()) << result.ToString();
  }
  std::remove(tmp_path.c_str());
}

TEST(CedarConfigTest, SaveToFileCreatesValidJson) {
  CedarConfig config;
  config.db.memtable_threshold = 8 * 1024 * 1024;
  config.db.write_buffer_size = 16 * 1024 * 1024;
  config.lsm.max_levels = 5;
  config.wal.num_shards = 8;
  config.mvcc.enable_delta_encoding = true;

  std::string tmp_path = "/tmp/cedar_test_save_config_" + std::to_string(getpid()) + ".json";
  Status s = config.SaveToFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "SaveToFile failed: " << s.ToString();

  // Verify file exists
  EXPECT_TRUE(std::filesystem::exists(tmp_path));

  // Verify it contains expected keys
  std::ifstream ifs(tmp_path);
  ASSERT_TRUE(ifs.is_open());
  std::string content((std::istreambuf_iterator<char>(ifs)),
                       std::istreambuf_iterator<char>());
  ifs.close();

  EXPECT_NE(content.find("\"memtable_threshold\": 8388608"), std::string::npos);
  EXPECT_NE(content.find("\"write_buffer_size\": 16777216"), std::string::npos);
  EXPECT_NE(content.find("\"max_levels\": 5"), std::string::npos);
  EXPECT_NE(content.find("\"num_shards\": 8"), std::string::npos);
  EXPECT_NE(content.find("\"enable_delta_encoding\": true"), std::string::npos);

  // Cleanup
  std::filesystem::remove(tmp_path);
}

TEST(CedarConfigTest, LoadFromFileParsesYamlSecurityAndTls) {
  std::string tmp_path = "/tmp/cedar_test_yaml_security_" + std::to_string(getpid()) + ".json";
  {
    std::ofstream ofs(tmp_path);
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
  }

  CedarConfig config;
  Status s = config.LoadFromFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "LoadFromFile failed: " << s.ToString();

  EXPECT_EQ(config.db.memtable_threshold, 2097152u);
  EXPECT_EQ(config.db.write_buffer_size, 4194304u);
  EXPECT_TRUE(config.security.enable_auth);
  EXPECT_TRUE(config.security.enable_tls);
  EXPECT_EQ(config.security.jwt_secret, "super-secret-key");
  EXPECT_TRUE(config.tls.enabled);
  EXPECT_EQ(config.tls.ca_cert, "/etc/certs/ca.crt");
  EXPECT_EQ(config.tls.server_cert, "/etc/certs/server.crt");
  EXPECT_EQ(config.tls.server_key, "/etc/certs/server.key");
  EXPECT_EQ(config.tls.client_cert, "/etc/certs/client.crt");
  EXPECT_EQ(config.tls.client_key, "/etc/certs/client.key");

  s = config.Validate();
  EXPECT_TRUE(s.ok()) << "Validation failed: " << s.ToString();

  std::filesystem::remove(tmp_path);
}

TEST(CedarConfigTest, LoadFromFileParsesJsonSecurityAndTls) {
  std::string tmp_path = "/tmp/cedar_test_json_security_" + std::to_string(getpid()) + ".json";
  {
    std::ofstream ofs(tmp_path);
    ofs << R"({
  "db": {
    "memtable_threshold": 1048576,
    "write_buffer_size": 2097152
  },
  "security": {
    "enable_auth": true,
    "enable_tls": false,
    "jwt_secret": "json-secret"
  },
  "tls": {
    "enabled": false,
    "ca_cert": "",
    "server_cert": "",
    "server_key": "",
    "client_cert": "",
    "client_key": ""
  }
})";
  }

  CedarConfig config;
  Status s = config.LoadFromFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "LoadFromFile failed: " << s.ToString();

  EXPECT_EQ(config.db.memtable_threshold, 1048576u);
  EXPECT_EQ(config.db.write_buffer_size, 2097152u);
  EXPECT_TRUE(config.security.enable_auth);
  EXPECT_FALSE(config.security.enable_tls);
  EXPECT_EQ(config.security.jwt_secret, "json-secret");
  EXPECT_FALSE(config.tls.enabled);

  s = config.Validate();
  EXPECT_TRUE(s.ok()) << "Validation failed: " << s.ToString();

  std::filesystem::remove(tmp_path);
}

TEST(CedarConfigTest, ValidateRejectsAuthWithoutJwtSecret) {
  CedarConfig config;
  config.security.enable_auth = true;
  config.security.jwt_secret = "";

  Status s = config.Validate();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(CedarConfigTest, ValidateRejectsTlsWithoutCerts) {
  CedarConfig config;
  config.tls.enabled = true;
  config.tls.server_cert = "";
  config.tls.server_key = "";

  Status s = config.Validate();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(CedarConfigTest, SaveAndLoadRoundTripSecurityTls) {
  CedarConfig original;
  original.security.enable_auth = true;
  original.security.enable_tls = true;
  original.security.jwt_secret = "round-trip-secret";
  original.tls.enabled = true;
  original.tls.ca_cert = "/certs/ca.crt";
  original.tls.server_cert = "/certs/server.crt";
  original.tls.server_key = "/certs/server.key";
  original.tls.client_cert = "/certs/client.crt";
  original.tls.client_key = "/certs/client.key";

  std::string tmp_path = "/tmp/cedar_test_roundtrip_" + std::to_string(getpid()) + ".json";
  Status s = original.SaveToFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "SaveToFile failed: " << s.ToString();

  CedarConfig loaded;
  s = loaded.LoadFromFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "LoadFromFile failed: " << s.ToString();

  EXPECT_EQ(loaded.security.enable_auth, original.security.enable_auth);
  EXPECT_EQ(loaded.security.enable_tls, original.security.enable_tls);
  EXPECT_EQ(loaded.security.jwt_secret, original.security.jwt_secret);
  EXPECT_EQ(loaded.tls.enabled, original.tls.enabled);
  EXPECT_EQ(loaded.tls.ca_cert, original.tls.ca_cert);
  EXPECT_EQ(loaded.tls.server_cert, original.tls.server_cert);
  EXPECT_EQ(loaded.tls.server_key, original.tls.server_key);
  EXPECT_EQ(loaded.tls.client_cert, original.tls.client_cert);
  EXPECT_EQ(loaded.tls.client_key, original.tls.client_key);

  s = loaded.Validate();
  EXPECT_TRUE(s.ok()) << "Validation failed: " << s.ToString();

  std::filesystem::remove(tmp_path);
}

TEST(CedarConfigTest, SaveToFileIsAtomic) {
  CedarConfig config;
  std::string tmp_path = "/tmp/cedar_test_atomic_config_" + std::to_string(getpid()) + ".json";

  // Ensure no stale tmp file exists
  std::filesystem::remove(tmp_path + ".tmp");
  std::filesystem::remove(tmp_path);

  Status s = config.SaveToFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "SaveToFile failed: " << s.ToString();

  // The .tmp file must not exist after successful atomic rename
  EXPECT_FALSE(std::filesystem::exists(tmp_path + ".tmp"))
      << "Temp file should be removed after atomic rename";

  // The final file must exist
  EXPECT_TRUE(std::filesystem::exists(tmp_path));

  std::filesystem::remove(tmp_path);
}

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

  original.memtable.type = static_cast<decltype(original.memtable.type)>(1);
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
