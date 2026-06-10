// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cedar/storage/cedar_config.h"

#include <fcntl.h>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <thread>
#include <unistd.h>

namespace cedar {

// ============================================================================
// CedarConfig 静态方法
// ============================================================================

CedarConfig CedarConfig::Default() {
  return CedarConfig();
}

CedarConfig CedarConfig::ForWorkload(WorkloadType type) {
  switch (type) {
    case WorkloadType::kReadHeavy:
      return ForReadHeavy();
    case WorkloadType::kWriteHeavy:
      return ForWriteHeavy();
    case WorkloadType::kHighConcurrency:
      return ForHighConcurrency();
    case WorkloadType::kLowLatency:
      return ForLowLatency();
    case WorkloadType::kHighThroughput:
      return ForHighThroughput();
    case WorkloadType::kMemoryConstrained:
      return ForMemoryConstrained();
    default:
      return Default();
  }
}

CedarConfig CedarConfig::ForReadHeavy() {
  CedarConfig config;
  
  // 大缓存
  config.cache.block_cache_size = 1024 * 1024 * 1024;  // 1GB
  config.cache.enable_version_chain_cache = true;
  config.cache.version_chain_cache_size = 100000;
  
  // 启用所有索引优化
  config.mvcc.enable_version_chain_index = true;
  config.mvcc.version_chain_index_threshold = 50;  // 更早构建索引
  config.mvcc.enable_temporal_bloom_filter = true;
  config.mvcc.temporal_filter_false_positive_rate = 0.005;  // 更低假阳性
  
  // 更大的 MemTable 减少刷盘
  config.db.memtable_threshold = 64 * 1024 * 1024;  // 64MB
  config.db.write_buffer_size = 64 * 1024 * 1024;
  
  // 布隆过滤器
  config.db.enable_bloom_filter = true;
  config.db.bloom_bits_per_key = 16;  // 更多位数
  
  return config;
}

CedarConfig CedarConfig::ForBalanced() {
  CedarConfig config;
  
  // 使用默认配置，但做一些平衡调整
  // 中等大小的 MemTable
  config.db.memtable_threshold = 32 * 1024 * 1024;   // 32MB
  config.db.write_buffer_size = 32 * 1024 * 1024;
  
  // 启用主要优化，但不过度
  config.mvcc.enable_sharded_timestamp_allocator = true;
  config.mvcc.enable_version_chain_index = true;
  config.mvcc.version_chain_index_threshold = 100;
  
  // Phase 3 优化选择性启用
  unsigned int cores = std::thread::hardware_concurrency();
  if (cores >= 16) {
    config.mvcc.enable_sharded_wal = true;
    config.mvcc.enable_lockfree_memtable = true;
    config.mvcc.enable_async_index_builder = true;
    config.wal.num_shards = cores;
  } else {
    config.mvcc.enable_sharded_wal = false;
    config.mvcc.enable_lockfree_memtable = false;
    config.mvcc.enable_async_index_builder = false;
  }
  
  // 平衡的缓存
  config.cache.block_cache_size = 64 * 1024 * 1024;  // 64MB
  
  return config;
}

CedarConfig CedarConfig::ForWriteHeavy() {
  CedarConfig config;
  
  // 大 MemTable 和写缓冲
  config.db.memtable_threshold = 128 * 1024 * 1024;  // 128MB
  config.db.write_buffer_size = 128 * 1024 * 1024;
  
  // 基于文件数量的阈值触发压缩
  // 当 Level 0 有 16 个文件 (~72MB @ 4.5MB each) 就触发
  config.lsm.min_files_for_compaction = 16;
  config.lsm.min_size_for_compaction = 32 * 1024 * 1024;  // 32MB (较低，主要用文件数触发)
  config.lsm.level0_file_num_compaction_trigger = 16;
  
  // 目标文件 64MB
  config.lsm.target_file_size = 64 * 1024 * 1024;
  
  // WAL 优化
  config.wal.group_commit_max_batch = 5000;
  config.wal.group_commit_timeout_us = 5000;
  
  // 禁用读优化相关
  config.db.enable_bloom_filter = false;
  config.mvcc.enable_temporal_bloom_filter = false;
  
  return config;
}

CedarConfig CedarConfig::ForHighConcurrency() {
  CedarConfig config;
  
  // 启用所有 Phase 3 优化
  config.mvcc.enable_deep_integration = true;
  config.mvcc.enable_sharded_timestamp_allocator = true;
  config.wal.enable_sharded_wal = true;
  config.memtable.enable_lockfree_memtable = true;
  config.mvcc.enable_async_index_builder = true;
  
  // 分片配置
  unsigned int cores = std::thread::hardware_concurrency();
  if (cores == 0) cores = 16;
  
  config.wal.num_shards = cores * 2;  // 2x CPU 核心
  config.mvcc.timestamp_shard_count = cores;
  config.mvcc.index_builder_worker_threads = cores / 2;
  
  // 减少锁竞争
  config.memtable.initial_capacity = 4 * 1024 * 1024;  // 4M entries
  config.memtable.enable_preallocation = true;
  
  // 事务优化
  config.transaction.parallel_validation = true;
  config.transaction.validation_threads = cores;
  
  return config;
}

CedarConfig CedarConfig::ForLowLatency() {
  CedarConfig config;
  
  // 小缓冲区减少延迟
  config.db.memtable_threshold = 1 * 1024 * 1024;  // 1MB
  config.db.write_buffer_size = 1 * 1024 * 1024;
  
  // 小批次快速提交
  config.wal.group_commit_max_batch = 10;
  config.wal.group_commit_timeout_us = 100;  // 100us
  
  // 异步索引构建避免阻塞
  config.mvcc.enable_async_index_builder = true;
  config.mvcc.index_builder_batch_timeout_ms = 1;  // 1ms
  
  // 立即刷盘
  config.wal.use_fsync = true;
  
  // 禁用可能增加延迟的功能
  config.mvcc.enable_delta_encoding = false;
  
  return config;
}

CedarConfig CedarConfig::ForHighThroughput() {
  CedarConfig config;
  
  // 大缓冲区
  config.db.memtable_threshold = 256 * 1024 * 1024;  // 256MB
  config.db.write_buffer_size = 256 * 1024 * 1024;
  
  // 大批量提交
  config.wal.group_commit_max_batch = 10000;
  config.wal.group_commit_timeout_us = 10000;  // 10ms
  
  // 高并发优化
  config.mvcc.enable_sharded_timestamp_allocator = true;
  config.mvcc.timestamp_batch_size = 10000;  // 大批量分配
  
  // 大 SST 文件减少文件数量
  config.lsm.target_file_size = 1024 * 1024 * 1024;  // 1GB
  
  // 延迟压缩
  config.lsm.level0_slowdown_writes_trigger = 100;
  config.lsm.level0_stop_writes_trigger = 200;
  
  return config;
}

CedarConfig CedarConfig::ForMemoryConstrained() {
  CedarConfig config;
  
  // 小内存占用
  config.db.memtable_threshold = 512 * 1024;  // 512KB
  config.db.write_buffer_size = 512 * 1024;
  
  // 小缓存
  config.cache.block_cache_size = 1 * 1024 * 1024;  // 1MB
  config.cache.enable_version_chain_cache = false;
  
  // 禁用内存密集型功能
  config.mvcc.enable_version_chain_index = false;
  config.mvcc.enable_async_index_builder = false;
  config.mvcc.enable_temporal_bloom_filter = false;
  config.db.enable_bloom_filter = false;
  
  // 小 SST 文件
  config.lsm.target_file_size = 16 * 1024 * 1024;  // 16MB
  
  // 积极压缩
  config.lsm.min_files_for_compaction = 2;
  config.lsm.level0_file_num_compaction_trigger = 2;
  
  return config;
}

// ============================================================================
// 转换为旧版 Options
// ============================================================================

CedarOptions CedarConfig::ToCedarOptions() const {
  CedarOptions options;
  
  options.create_if_missing = db.create_if_missing;
  options.error_if_exists = db.error_if_exists;
  options.paranoid_checks = db.paranoid_checks;
  options.memtable_threshold = db.memtable_threshold;
  options.write_buffer_size = db.write_buffer_size;
  options.column_id = db.column_id;
  options.enable_bloom_filter = db.enable_bloom_filter;
  options.bloom_bits_per_key = db.bloom_bits_per_key;
  options.verify_checksums = db.verify_checksums;
  
  // LSM 压缩配置
  options.compaction_config.min_files = lsm.min_files_for_compaction;
  options.compaction_config.min_size = lsm.min_size_for_compaction;
  options.compaction_config.target_file_size = lsm.target_file_size;
  options.compaction_config.max_levels = lsm.max_levels;
  
  return options;
}

MVCCOptimizationConfig CedarConfig::ToMVCCConfig() const {
  MVCCOptimizationConfig config;
  
  // Phase 1/2
  config.enable_sharded_timestamp_allocator = mvcc.enable_sharded_timestamp_allocator;
  config.timestamp_shard_count = mvcc.timestamp_shard_count;
  config.timestamp_batch_size = mvcc.timestamp_batch_size;
  
  config.enable_version_chain_index = mvcc.enable_version_chain_index;
  config.version_chain_index_threshold = mvcc.version_chain_index_threshold;
  config.version_chain_max_level = mvcc.version_chain_max_level;
  
  config.enable_delta_encoding = mvcc.enable_delta_encoding;
  config.delta_max_per_group = mvcc.delta_max_per_group;
  
  config.enable_temporal_bloom_filter = mvcc.enable_temporal_bloom_filter;
  config.temporal_filter_false_positive_rate = mvcc.temporal_filter_false_positive_rate;
  config.temporal_filter_hours_per_bucket = mvcc.temporal_filter_hours_per_bucket;
  
  // Phase 3
  config.enable_sharded_wal = wal.enable_sharded_wal;
  config.wal_shard_count = wal.num_shards;
  config.wal_batch_timeout_us = wal.batch_timeout_us;
  config.wal_batch_max_size = wal.batch_max_size;
  config.wal_enable_background_merger = wal.enable_background_merger;
  config.wal_merge_interval_ms = wal.merge_interval_ms;
  
  config.enable_lockfree_memtable = memtable.enable_lockfree_memtable;
  config.memtable_initial_capacity = memtable.initial_capacity;
  config.memtable_gc_interval_ms = memtable.gc_interval_ms;
  config.memtable_enable_preallocation = memtable.enable_preallocation;
  
  config.enable_async_index_builder = mvcc.enable_async_index_builder;
  config.index_builder_worker_threads = mvcc.index_builder_worker_threads;
  config.index_builder_max_concurrent = mvcc.index_builder_max_concurrent;
  config.index_builder_enable_batch = true;  // 默认启用
  config.index_builder_batch_size = mvcc.index_builder_batch_size;
  
  return config;
}

ShardedWalOptions CedarConfig::ToShardedWalOptions() const {
  ShardedWalOptions options;
  
  options.num_shards = wal.num_shards;
  if (options.num_shards == 0) {
    options.num_shards = std::thread::hardware_concurrency();
    if (options.num_shards == 0) options.num_shards = 16;
  }
  
  options.bind_by_thread_id = wal.bind_by_thread_id;
  options.max_file_size_per_shard = wal.max_file_size_per_shard;
  options.batch_timeout_us = wal.batch_timeout_us;
  options.batch_max_size = wal.batch_max_size;
  options.enable_background_merger = wal.enable_background_merger;
  options.merge_interval_ms = wal.merge_interval_ms;
  
  return options;
}

LockFreeMemTableOptions CedarConfig::ToLockFreeMemTableOptions() const {
  LockFreeMemTableOptions options;
  
  options.initial_capacity = memtable.initial_capacity;
  options.rehash_threshold = memtable.rehash_threshold;
  options.enable_version_chain_index = mvcc.enable_version_chain_index;
  options.index_build_threshold = mvcc.version_chain_index_threshold;
  options.gc_interval_ms = memtable.gc_interval_ms;
  options.gc_batch_size = memtable.gc_batch_size;
  options.enable_preallocation = memtable.enable_preallocation;
  options.preallocation_pool_size = memtable.preallocation_pool_size;
  
  return options;
}

AsyncIndexBuilderOptions CedarConfig::ToAsyncIndexBuilderOptions() const {
  AsyncIndexBuilderOptions options;
  
  options.num_worker_threads = mvcc.index_builder_worker_threads;
  options.build_threshold = mvcc.version_chain_index_threshold;
  options.max_concurrent_builds = mvcc.index_builder_max_concurrent;
  options.enable_batch_building = true;
  options.batch_max_size = mvcc.index_builder_batch_size;
  options.batch_timeout_ms = mvcc.index_builder_batch_timeout_ms;
  options.enable_build_cache = mvcc.enable_build_cache;
  options.build_cache_size = mvcc.build_cache_size;
  
  return options;
}

// ============================================================================
// 配置验证
// ============================================================================

Status CedarConfig::Validate() const {
  // 检查基本约束
  if (db.memtable_threshold < 1024) {
    return Status::InvalidArgument("memtable_threshold too small (< 1KB)");
  }
  
  if (db.write_buffer_size < 1024) {
    return Status::InvalidArgument("write_buffer_size too small (< 1KB)");
  }
  
  if (lsm.max_levels < 2 || lsm.max_levels > 10) {
    return Status::InvalidArgument("max_levels should be in [2, 10]");
  }
  
  if (wal.num_shards > 256) {
    return Status::InvalidArgument("wal.num_shards too large (> 256)");
  }
  
  if (mvcc.timestamp_shard_count > 256) {
    return Status::InvalidArgument("timestamp_shard_count too large (> 256)");
  }
  
  if (cache.block_cache_size < 1024) {
    return Status::InvalidArgument("block_cache_size too small (< 1KB)");
  }

  // Security validation
  if (security.enable_auth && security.jwt_secret.empty()) {
    return Status::InvalidArgument(
        "security.enable_auth is true but security.jwt_secret is empty");
  }
  if (security.enable_tls && !tls.enabled) {
    return Status::InvalidArgument(
        "security.enable_tls is true but tls.enabled is false");
  }
  if (tls.enabled) {
    if (tls.server_cert.empty() || tls.server_key.empty()) {
      return Status::InvalidArgument(
          "tls.enabled is true but server_cert or server_key is empty");
    }
  }
  
  return Status::OK();
}

// ============================================================================
// 配置打印
// ============================================================================

void CedarConfig::Dump() const {
  std::cout << "\n╔══════════════════════════════════════════════════════════════════════════════╗\n";
  std::cout << "║                         Cedar Configuration                                   ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════════════════════╝\n";
  
  // DB
  std::cout << "\n[Database]\n";
  if (db.memtable_threshold >= 1024 * 1024) {
    std::cout << "  memtable_threshold: " << db.memtable_threshold / 1024 / 1024 << " MB\n";
  } else {
    std::cout << "  memtable_threshold: " << db.memtable_threshold / 1024 << " KB\n";
  }
  if (db.write_buffer_size >= 1024 * 1024) {
    std::cout << "  write_buffer_size: " << db.write_buffer_size / 1024 / 1024 << " MB\n";
  } else {
    std::cout << "  write_buffer_size: " << db.write_buffer_size / 1024 << " KB\n";
  }
  std::cout << "  enable_bloom_filter: " << (db.enable_bloom_filter ? "true" : "false") << "\n";
  
  // LSM
  std::cout << "\n[LSM Tree]\n";
  std::cout << "  max_levels: " << lsm.max_levels << "\n";
  std::cout << "  target_file_size: " << lsm.target_file_size / 1024 / 1024 << " MB\n";
  
  // WAL
  std::cout << "\n[WAL]\n";
  std::cout << "  enable_sharded_wal: " << (wal.enable_sharded_wal ? "true" : "false") << "\n";
  std::cout << "  num_shards: " << (wal.num_shards == 0 ? "auto" : std::to_string(wal.num_shards)) << "\n";
  
  // MVCC
  std::cout << "\n[MVCC Optimization]\n";
  std::cout << "  enable_sharded_timestamp_allocator: " << (mvcc.enable_sharded_timestamp_allocator ? "true" : "false") << "\n";
  std::cout << "  enable_version_chain_index: " << (mvcc.enable_version_chain_index ? "true" : "false") << "\n";
  std::cout << "  enable_sharded_wal: " << (wal.enable_sharded_wal ? "true" : "false") << "\n";
  std::cout << "  enable_lockfree_memtable: " << (memtable.enable_lockfree_memtable ? "true" : "false") << "\n";
  std::cout << "  enable_async_index_builder: " << (mvcc.enable_async_index_builder ? "true" : "false") << "\n";
  
  // Cache
  std::cout << "\n[Cache]\n";
  std::cout << "  block_cache_size: " << cache.block_cache_size / 1024 / 1024 << " MB\n";

  // Security
  std::cout << "\n[Security]\n";
  std::cout << "  enable_auth: " << (security.enable_auth ? "true" : "false") << "\n";
  std::cout << "  enable_tls: " << (security.enable_tls ? "true" : "false") << "\n";
  std::cout << "  jwt_secret: " << (security.jwt_secret.empty() ? "<not set>" : "<set>") << "\n";

  // TLS
  std::cout << "\n[TLS]\n";
  std::cout << "  enabled: " << (tls.enabled ? "true" : "false") << "\n";
  if (tls.enabled) {
    std::cout << "  ca_cert: " << tls.ca_cert << "\n";
    std::cout << "  server_cert: " << tls.server_cert << "\n";
    std::cout << "  server_key: " << tls.server_key << "\n";
    std::cout << "  client_cert: " << tls.client_cert << "\n";
    std::cout << "  client_key: " << tls.client_key << "\n";
  }
  
  std::cout << "\n";
}

// ============================================================================
// 配置合并
// ============================================================================

void CedarConfig::MergeFrom(const CedarConfig& other) {
  // Merge non-default values from other into this config.
  CedarConfig defaults;
  
  // DB options
  if (other.db.create_if_missing != defaults.db.create_if_missing) db.create_if_missing = other.db.create_if_missing;
  if (other.db.error_if_exists != defaults.db.error_if_exists) db.error_if_exists = other.db.error_if_exists;
  if (other.db.paranoid_checks != defaults.db.paranoid_checks) db.paranoid_checks = other.db.paranoid_checks;
  if (other.db.memtable_threshold != defaults.db.memtable_threshold) db.memtable_threshold = other.db.memtable_threshold;
  if (other.db.write_buffer_size != defaults.db.write_buffer_size) db.write_buffer_size = other.db.write_buffer_size;
  if (other.db.column_id != defaults.db.column_id) db.column_id = other.db.column_id;
  if (other.db.enable_bloom_filter != defaults.db.enable_bloom_filter) db.enable_bloom_filter = other.db.enable_bloom_filter;
  if (other.db.bloom_bits_per_key != defaults.db.bloom_bits_per_key) db.bloom_bits_per_key = other.db.bloom_bits_per_key;
  if (other.db.verify_checksums != defaults.db.verify_checksums) db.verify_checksums = other.db.verify_checksums;
  
  // LSM options
  if (other.lsm.min_files_for_compaction != defaults.lsm.min_files_for_compaction) lsm.min_files_for_compaction = other.lsm.min_files_for_compaction;
  if (other.lsm.min_size_for_compaction != defaults.lsm.min_size_for_compaction) lsm.min_size_for_compaction = other.lsm.min_size_for_compaction;
  if (other.lsm.target_file_size != defaults.lsm.target_file_size) lsm.target_file_size = other.lsm.target_file_size;
  if (other.lsm.max_levels != defaults.lsm.max_levels) lsm.max_levels = other.lsm.max_levels;
  if (other.lsm.level_size_multiplier != defaults.lsm.level_size_multiplier) lsm.level_size_multiplier = other.lsm.level_size_multiplier;
  if (other.lsm.level0_file_num_compaction_trigger != defaults.lsm.level0_file_num_compaction_trigger) lsm.level0_file_num_compaction_trigger = other.lsm.level0_file_num_compaction_trigger;
  if (other.lsm.level0_slowdown_writes_trigger != defaults.lsm.level0_slowdown_writes_trigger) lsm.level0_slowdown_writes_trigger = other.lsm.level0_slowdown_writes_trigger;
  if (other.lsm.level0_stop_writes_trigger != defaults.lsm.level0_stop_writes_trigger) lsm.level0_stop_writes_trigger = other.lsm.level0_stop_writes_trigger;

  // Security options
  if (other.security.enable_auth != defaults.security.enable_auth) security.enable_auth = other.security.enable_auth;
  if (other.security.enable_tls != defaults.security.enable_tls) security.enable_tls = other.security.enable_tls;
  if (other.security.jwt_secret != defaults.security.jwt_secret) security.jwt_secret = other.security.jwt_secret;

  // TLS options
  if (other.tls.enabled != defaults.tls.enabled) tls.enabled = other.tls.enabled;
  if (other.tls.ca_cert != defaults.tls.ca_cert) tls.ca_cert = other.tls.ca_cert;
  if (other.tls.server_cert != defaults.tls.server_cert) tls.server_cert = other.tls.server_cert;
  if (other.tls.server_key != defaults.tls.server_key) tls.server_key = other.tls.server_key;
  if (other.tls.client_cert != defaults.tls.client_cert) tls.client_cert = other.tls.client_cert;
  if (other.tls.client_key != defaults.tls.client_key) tls.client_key = other.tls.client_key;
}

// ============================================================================
// 文件加载/保存（简化实现）
// ============================================================================

namespace {

// Trim leading and trailing whitespace from a string.
std::string Trim(const std::string& s) {
  size_t start = 0;
  while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
    ++start;
  }
  size_t end = s.size();
  while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
    --end;
  }
  return s.substr(start, end - start);
}

// Remove surrounding quotes from a string value.
std::string Unquote(const std::string& s) {
  if (s.size() >= 2 && ((s.front() == '"' && s.back() == '"') ||
                        (s.front() == '\'' && s.back() == '\''))) {
    return s.substr(1, s.size() - 2);
  }
  return s;
}

// Parse a boolean from various string representations.
bool ParseBool(const std::string& value) {
  std::string lower;
  lower.reserve(value.size());
  for (char c : value) {
    lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  if (lower == "true" || lower == "yes" || lower == "1" || lower == "on") {
    return true;
  }
  return false;
}

}  // namespace

Status CedarConfig::LoadFromFile(const std::string& path) {
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    return Status::IOError("Cannot open config file: " + path);
  }

  std::string line;
  std::string current_section;

  while (std::getline(ifs, line)) {
    std::string trimmed = Trim(line);

    // Skip empty lines and comments.
    if (trimmed.empty() || trimmed.front() == '#') {
      continue;
    }

    // Detect section headers (YAML: "section:" or JSON: "section": {).
    // A section is a top-level key without leading whitespace or with minimal indent.
    size_t first_non_space = line.find_first_not_of(" \t");
    if (first_non_space == std::string::npos) {
      continue;
    }

    bool is_top_level = (first_non_space == 0);
    // In YAML, nested keys are indented; in JSON, braces may appear.
    // Treat a line ending with ':' (possibly followed by '{' in JSON) as a section.
    size_t colon_pos = trimmed.find(':');
    if (colon_pos != std::string::npos && colon_pos == trimmed.find_last_of(':')) {
      // Check if the next char after colon is space, end of string, or '{'
      size_t after_colon = colon_pos + 1;
      while (after_colon < trimmed.size() &&
             std::isspace(static_cast<unsigned char>(trimmed[after_colon]))) {
        ++after_colon;
      }
      char next_char = (after_colon < trimmed.size()) ? trimmed[after_colon] : '\0';

      if (next_char == '\0' || next_char == '{' || next_char == '#') {
        std::string section_name = Trim(trimmed.substr(0, colon_pos));
        // Remove quotes around section name for JSON.
        section_name = Unquote(section_name);
        if (section_name == "db" || section_name == "lsm" || section_name == "wal" ||
            section_name == "security" || section_name == "tls") {
          current_section = section_name;
          continue;
        }
      }
    }

    // If we are inside a target section, look for key-value pairs.
    if (!current_section.empty()) {
      size_t kv_colon = trimmed.find(':');
      if (kv_colon != std::string::npos) {
        std::string key = Trim(trimmed.substr(0, kv_colon));
        key = Unquote(key);
        std::string value = Trim(trimmed.substr(kv_colon + 1));
        // Strip trailing comma (JSON) and comments.
        size_t comment_pos = value.find('#');
        if (comment_pos != std::string::npos) {
          value = Trim(value.substr(0, comment_pos));
        }
        if (!value.empty() && value.back() == ',') {
          value.pop_back();
          value = Trim(value);
        }
        value = Unquote(value);

        // -------------------------------------------------------------------
        // db.*
        // -------------------------------------------------------------------
        if (current_section == "db") {
          if (key == "create_if_missing") {
            db.create_if_missing = ParseBool(value);
          } else if (key == "error_if_exists") {
            db.error_if_exists = ParseBool(value);
          } else if (key == "paranoid_checks") {
            db.paranoid_checks = ParseBool(value);
          } else if (key == "memtable_threshold") {
            db.memtable_threshold = static_cast<size_t>(std::stoull(value));
          } else if (key == "write_buffer_size") {
            db.write_buffer_size = static_cast<size_t>(std::stoull(value));
          } else if (key == "column_id") {
            db.column_id = static_cast<uint16_t>(std::stoul(value));
          } else if (key == "enable_bloom_filter") {
            db.enable_bloom_filter = ParseBool(value);
          } else if (key == "bloom_bits_per_key") {
            db.bloom_bits_per_key = std::stoi(value);
          } else if (key == "verify_checksums") {
            db.verify_checksums = ParseBool(value);
          }
        }
        // -------------------------------------------------------------------
        // lsm.*
        // -------------------------------------------------------------------
        else if (current_section == "lsm") {
          if (key == "min_files_for_compaction") {
            lsm.min_files_for_compaction = static_cast<size_t>(std::stoull(value));
          } else if (key == "min_size_for_compaction") {
            lsm.min_size_for_compaction = static_cast<size_t>(std::stoull(value));
          } else if (key == "target_file_size") {
            lsm.target_file_size = static_cast<size_t>(std::stoull(value));
          } else if (key == "max_levels") {
            lsm.max_levels = std::stoi(value);
          } else if (key == "level_size_multiplier") {
            lsm.level_size_multiplier = std::stod(value);
          } else if (key == "level0_file_num_compaction_trigger") {
            lsm.level0_file_num_compaction_trigger = static_cast<size_t>(std::stoull(value));
          } else if (key == "level0_slowdown_writes_trigger") {
            lsm.level0_slowdown_writes_trigger = static_cast<size_t>(std::stoull(value));
          } else if (key == "level0_stop_writes_trigger") {
            lsm.level0_stop_writes_trigger = static_cast<size_t>(std::stoull(value));
          }
        }
        // -------------------------------------------------------------------
        // wal.*
        // -------------------------------------------------------------------
        else if (current_section == "wal") {
          if (key == "max_file_size") {
            wal.max_file_size = static_cast<size_t>(std::stoull(value));
          } else if (key == "group_commit_timeout_us") {
            wal.group_commit_timeout_us = static_cast<uint32_t>(std::stoul(value));
          } else if (key == "group_commit_max_batch") {
            wal.group_commit_max_batch = static_cast<size_t>(std::stoull(value));
          } else if (key == "use_fsync") {
            wal.use_fsync = ParseBool(value);
          } else if (key == "preallocate_size") {
            wal.preallocate_size = static_cast<size_t>(std::stoull(value));
          } else if (key == "enable_sharded_wal") {
            wal.enable_sharded_wal = ParseBool(value);
          } else if (key == "num_shards") {
            wal.num_shards = static_cast<uint32_t>(std::stoul(value));
          } else if (key == "bind_by_thread_id") {
            wal.bind_by_thread_id = ParseBool(value);
          } else if (key == "max_file_size_per_shard") {
            wal.max_file_size_per_shard = static_cast<size_t>(std::stoull(value));
          } else if (key == "batch_timeout_us") {
            wal.batch_timeout_us = static_cast<uint32_t>(std::stoul(value));
          } else if (key == "batch_max_size") {
            wal.batch_max_size = static_cast<size_t>(std::stoull(value));
          } else if (key == "enable_background_merger") {
            wal.enable_background_merger = ParseBool(value);
          } else if (key == "merge_interval_ms") {
            wal.merge_interval_ms = static_cast<uint32_t>(std::stoul(value));
          }
        }
        // -------------------------------------------------------------------
        // security.*
        // -------------------------------------------------------------------
        else if (current_section == "security") {
          if (key == "enable_auth") {
            security.enable_auth = ParseBool(value);
          } else if (key == "enable_tls") {
            security.enable_tls = ParseBool(value);
          } else if (key == "jwt_secret") {
            security.jwt_secret = value;
          }
        }
        // -------------------------------------------------------------------
        // tls.*
        // -------------------------------------------------------------------
        else if (current_section == "tls") {
          if (key == "enabled") {
            tls.enabled = ParseBool(value);
          } else if (key == "ca_cert") {
            tls.ca_cert = value;
          } else if (key == "server_cert") {
            tls.server_cert = value;
          } else if (key == "server_key") {
            tls.server_key = value;
          } else if (key == "client_cert") {
            tls.client_cert = value;
          } else if (key == "client_key") {
            tls.client_key = value;
          }
        }
      }
    }
  }

  return Status::OK();
}

namespace {

// Simple JSON value escaper for strings
std::string JsonEscape(const std::string& s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\b': out += "\\b"; break;
      case '\f': out += "\\f"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default: out += c; break;
    }
  }
  return out;
}

// Append a JSON key-value pair to a string buffer
void JsonKV(std::string* out, const std::string& key, const std::string& value,
            bool is_string, bool last) {
  *out += "    \"" + JsonEscape(key) + "\": ";
  if (is_string) {
    *out += "\"" + JsonEscape(value) + "\"";
  } else {
    *out += value;
  }
  if (!last) *out += ",";
  *out += "\n";
}

template <typename T>
std::string ToStr(T v) {
  return std::to_string(v);
}

std::string BoolStr(bool b) {
  return b ? "true" : "false";
}

}  // namespace

Status CedarConfig::SaveToFile(const std::string& path) const {
  std::string json;
  json += "{\n";
  json += "  \"version\": " + std::to_string(kVersion) + ",\n";

  // db section
  json += "  \"db\": {\n";
  JsonKV(&json, "create_if_missing", BoolStr(db.create_if_missing), false, false);
  JsonKV(&json, "error_if_exists", BoolStr(db.error_if_exists), false, false);
  JsonKV(&json, "paranoid_checks", BoolStr(db.paranoid_checks), false, false);
  JsonKV(&json, "memtable_threshold", ToStr(db.memtable_threshold), false, false);
  JsonKV(&json, "write_buffer_size", ToStr(db.write_buffer_size), false, false);
  JsonKV(&json, "column_id", ToStr(db.column_id), false, false);
  JsonKV(&json, "enable_bloom_filter", BoolStr(db.enable_bloom_filter), false, false);
  JsonKV(&json, "bloom_bits_per_key", ToStr(db.bloom_bits_per_key), false, false);
  JsonKV(&json, "verify_checksums", BoolStr(db.verify_checksums), false, true);
  json += "  },\n";

  // lsm section
  json += "  \"lsm\": {\n";
  JsonKV(&json, "min_files_for_compaction", ToStr(lsm.min_files_for_compaction), false, false);
  JsonKV(&json, "min_size_for_compaction", ToStr(lsm.min_size_for_compaction), false, false);
  JsonKV(&json, "target_file_size", ToStr(lsm.target_file_size), false, false);
  JsonKV(&json, "max_levels", ToStr(lsm.max_levels), false, false);
  JsonKV(&json, "level_size_multiplier", ToStr(lsm.level_size_multiplier), false, false);
  JsonKV(&json, "level0_file_num_compaction_trigger", ToStr(lsm.level0_file_num_compaction_trigger), false, false);
  JsonKV(&json, "level0_slowdown_writes_trigger", ToStr(lsm.level0_slowdown_writes_trigger), false, false);
  JsonKV(&json, "level0_stop_writes_trigger", ToStr(lsm.level0_stop_writes_trigger), false, true);
  json += "  },\n";

  // wal section
  json += "  \"wal\": {\n";
  JsonKV(&json, "max_file_size", ToStr(wal.max_file_size), false, false);
  JsonKV(&json, "group_commit_timeout_us", ToStr(wal.group_commit_timeout_us), false, false);
  JsonKV(&json, "group_commit_max_batch", ToStr(wal.group_commit_max_batch), false, false);
  JsonKV(&json, "use_fsync", BoolStr(wal.use_fsync), false, false);
  JsonKV(&json, "preallocate_size", ToStr(wal.preallocate_size), false, false);
  JsonKV(&json, "enable_sharded_wal", BoolStr(wal.enable_sharded_wal), false, false);
  JsonKV(&json, "num_shards", ToStr(wal.num_shards), false, false);
  JsonKV(&json, "bind_by_thread_id", BoolStr(wal.bind_by_thread_id), false, false);
  JsonKV(&json, "max_file_size_per_shard", ToStr(wal.max_file_size_per_shard), false, false);
  JsonKV(&json, "batch_timeout_us", ToStr(wal.batch_timeout_us), false, false);
  JsonKV(&json, "batch_max_size", ToStr(wal.batch_max_size), false, false);
  JsonKV(&json, "enable_background_merger", BoolStr(wal.enable_background_merger), false, false);
  JsonKV(&json, "merge_interval_ms", ToStr(wal.merge_interval_ms), false, true);
  json += "  },\n";

  // memtable section
  json += "  \"memtable\": {\n";
  JsonKV(&json, "type", ToStr(static_cast<int>(memtable.type)), false, false);
  JsonKV(&json, "enable_lockfree_memtable", BoolStr(memtable.enable_lockfree_memtable), false, false);
  JsonKV(&json, "initial_capacity", ToStr(memtable.initial_capacity), false, false);
  JsonKV(&json, "rehash_threshold", ToStr(memtable.rehash_threshold), false, false);
  JsonKV(&json, "enable_preallocation", BoolStr(memtable.enable_preallocation), false, false);
  JsonKV(&json, "preallocation_pool_size", ToStr(memtable.preallocation_pool_size), false, false);
  JsonKV(&json, "gc_interval_ms", ToStr(memtable.gc_interval_ms), false, false);
  JsonKV(&json, "gc_batch_size", ToStr(memtable.gc_batch_size), false, true);
  json += "  },\n";

  // mvcc section
  json += "  \"mvcc\": {\n";
  JsonKV(&json, "enable_sharded_timestamp_allocator", BoolStr(mvcc.enable_sharded_timestamp_allocator), false, false);
  JsonKV(&json, "timestamp_shard_count", ToStr(mvcc.timestamp_shard_count), false, false);
  JsonKV(&json, "timestamp_batch_size", ToStr(mvcc.timestamp_batch_size), false, false);
  JsonKV(&json, "enable_version_chain_index", BoolStr(mvcc.enable_version_chain_index), false, false);
  JsonKV(&json, "version_chain_index_threshold", ToStr(mvcc.version_chain_index_threshold), false, false);
  JsonKV(&json, "version_chain_max_level", ToStr(mvcc.version_chain_max_level), false, false);
  JsonKV(&json, "enable_delta_encoding", BoolStr(mvcc.enable_delta_encoding), false, false);
  JsonKV(&json, "delta_max_per_group", ToStr(mvcc.delta_max_per_group), false, false);
  JsonKV(&json, "enable_temporal_bloom_filter", BoolStr(mvcc.enable_temporal_bloom_filter), false, false);
  JsonKV(&json, "temporal_filter_false_positive_rate", ToStr(mvcc.temporal_filter_false_positive_rate), false, false);
  JsonKV(&json, "temporal_filter_hours_per_bucket", ToStr(mvcc.temporal_filter_hours_per_bucket), false, false);
  JsonKV(&json, "enable_sharded_wal", BoolStr(mvcc.enable_sharded_wal), false, false);
  JsonKV(&json, "enable_lockfree_memtable", BoolStr(mvcc.enable_lockfree_memtable), false, false);
  JsonKV(&json, "enable_async_index_builder", BoolStr(mvcc.enable_async_index_builder), false, false);
  JsonKV(&json, "index_builder_worker_threads", ToStr(mvcc.index_builder_worker_threads), false, false);
  JsonKV(&json, "index_builder_max_concurrent", ToStr(mvcc.index_builder_max_concurrent), false, false);
  JsonKV(&json, "index_builder_batch_size", ToStr(mvcc.index_builder_batch_size), false, false);
  JsonKV(&json, "index_builder_batch_timeout_ms", ToStr(mvcc.index_builder_batch_timeout_ms), false, false);
  JsonKV(&json, "enable_build_cache", BoolStr(mvcc.enable_build_cache), false, false);
  JsonKV(&json, "build_cache_size", ToStr(mvcc.build_cache_size), false, false);
  JsonKV(&json, "enable_deep_integration", BoolStr(mvcc.enable_deep_integration), false, true);
  json += "  },\n";

  // transaction section
  json += "  \"transaction\": {\n";
  JsonKV(&json, "enable_transaction", BoolStr(transaction.enable_transaction), false, false);
  JsonKV(&json, "default_isolation_level", ToStr(transaction.default_isolation_level), false, false);
  JsonKV(&json, "timeout_ms", ToStr(transaction.timeout_ms), false, false);
  JsonKV(&json, "max_retries", ToStr(transaction.max_retries), false, false);
  JsonKV(&json, "parallel_validation", BoolStr(transaction.parallel_validation), false, false);
  JsonKV(&json, "validation_threads", ToStr(transaction.validation_threads), false, false);
  JsonKV(&json, "enable_occ", BoolStr(transaction.enable_occ), false, false);
  JsonKV(&json, "max_write_set_size", ToStr(transaction.max_write_set_size), false, false);
  JsonKV(&json, "max_read_set_size", ToStr(transaction.max_read_set_size), false, true);
  json += "  },\n";

  // cache section
  json += "  \"cache\": {\n";
  JsonKV(&json, "block_cache_size", ToStr(cache.block_cache_size), false, false);
  JsonKV(&json, "table_cache_size", ToStr(cache.table_cache_size), false, false);
  JsonKV(&json, "block_restart_interval", ToStr(cache.block_restart_interval), false, false);
  JsonKV(&json, "block_size", ToStr(cache.block_size), false, false);
  JsonKV(&json, "version_chain_cache_size", ToStr(cache.version_chain_cache_size), false, false);
  JsonKV(&json, "enable_version_chain_cache", BoolStr(cache.enable_version_chain_cache), false, true);
  json += "  },\n";

  // filesystem section
  json += "  \"filesystem\": {\n";
  JsonKV(&json, "max_open_files", ToStr(filesystem.max_open_files), false, false);
  JsonKV(&json, "use_direct_io", BoolStr(filesystem.use_direct_io), false, false);
  JsonKV(&json, "advise_random_access", BoolStr(filesystem.advise_random_access), false, false);
  JsonKV(&json, "prefetch_buffer_size", ToStr(filesystem.prefetch_buffer_size), false, true);
  json += "  },\n";

  // security section
  json += "  \"security\": {\n";
  JsonKV(&json, "enable_auth", BoolStr(security.enable_auth), false, false);
  JsonKV(&json, "enable_tls", BoolStr(security.enable_tls), false, false);
  JsonKV(&json, "jwt_secret", security.jwt_secret, true, true);
  json += "  },\n";

  // tls section
  json += "  \"tls\": {\n";
  JsonKV(&json, "enabled", BoolStr(tls.enabled), false, false);
  JsonKV(&json, "ca_cert", tls.ca_cert, true, false);
  JsonKV(&json, "server_cert", tls.server_cert, true, false);
  JsonKV(&json, "server_key", tls.server_key, true, false);
  JsonKV(&json, "client_cert", tls.client_cert, true, false);
  JsonKV(&json, "client_key", tls.client_key, true, true);
  json += "  },\n";

  // debug section
  json += "  \"debug\": {\n";
  JsonKV(&json, "enable_stats", BoolStr(debug.enable_stats), false, false);
  JsonKV(&json, "stats_dump_interval_sec", ToStr(debug.stats_dump_interval_sec), false, false);
  JsonKV(&json, "enable_slow_log", BoolStr(debug.enable_slow_log), false, false);
  JsonKV(&json, "slow_log_threshold_ms", ToStr(debug.slow_log_threshold_ms), false, false);
  JsonKV(&json, "enable_trace", BoolStr(debug.enable_trace), false, true);
  json += "  }\n";

  json += "}\n";

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

  // fsync the temp file for durability
  int fd = ::open(tmp_path.c_str(), O_RDONLY);
  if (fd >= 0) {
#if defined(__APPLE__)
    ::fcntl(fd, F_FULLFSYNC);
#else
    ::fsync(fd);
#endif
    ::close(fd);
  }

  // fsync the directory to ensure the rename is durable
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

// ============================================================================
// CedarConfigManager 单例实现
// ============================================================================

CedarConfigManager* CedarConfigManager::Instance() {
  static CedarConfigManager instance;
  return &instance;
}

Status CedarConfigManager::LoadConfig(const std::string& path) {
  std::unique_lock<std::mutex> lock(mutex_);

  Status s = config_.LoadFromFile(path);
  CEDAR_RETURN_IF_ERROR(s);

  config_path_ = path;

  s = config_.Validate();
  CEDAR_RETURN_IF_ERROR(s);

  auto callbacks_copy = callbacks_;
  lock.unlock();

  for (const auto& callback : callbacks_copy) {
    callback(config_);
  }

  return Status::OK();
}

Status CedarConfigManager::LoadDefaultConfig() {
  std::lock_guard<std::mutex> lock(mutex_);
  config_ = CedarConfig::Default();
  return Status::OK();
}

Status CedarConfigManager::ReloadConfig() {
  if (config_path_.empty()) {
    return Status::InvalidArgument("No config file loaded");
  }
  return LoadConfig(config_path_);
}

void CedarConfigManager::RegisterCallback(ConfigChangeCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  callbacks_.push_back(callback);
}

Status CedarConfigManager::ApplyToEngine(class LsmEngine* engine) {
  // Apply runtime config changes to LsmEngine.
  // LsmEngine currently does not support dynamic reconfiguration;
  // changes require engine restart.
  (void)engine;
  return Status::OK();
}

}  // namespace cedar
