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
#include <cctype>
#include "butil/third_party/rapidjson/document.h"
#include "butil/third_party/rapidjson/stringbuffer.h"
#include "butil/third_party/rapidjson/prettywriter.h"

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

Status CedarConfig::LoadFromFile(const std::string& path) {
  std::ifstream ifs(path, std::ios::binary);
  if (!ifs.is_open()) {
    return Status::IOError("Cannot open config file: " + path);
  }

  std::string content((std::istreambuf_iterator<char>(ifs)),
                       std::istreambuf_iterator<char>());
  ifs.close();

  butil::rapidjson::Document doc;
  doc.Parse(content.c_str());

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

  // ---------------------------------------------------------------------------
  // 12. limits
  // ---------------------------------------------------------------------------
  if (doc.HasMember("limits") && doc["limits"].IsObject()) {
    const auto& lim = doc["limits"];
    limits.max_query_length = get_size_t(lim, "max_query_length", limits.max_query_length);
    limits.max_batch_items = get_size_t(lim, "max_batch_items", limits.max_batch_items);
    limits.max_parameter_count = get_size_t(lim, "max_parameter_count", limits.max_parameter_count);
    limits.max_parameter_value_length = get_size_t(lim, "max_parameter_value_length", limits.max_parameter_value_length);
    limits.max_timeout_ms = get_uint32(lim, "max_timeout_ms", limits.max_timeout_ms);
    limits.max_heartbeat_partitions = get_size_t(lim, "max_heartbeat_partitions", limits.max_heartbeat_partitions);
  }

  return Status::OK();
}

Status CedarConfig::SaveToFile(const std::string& path) const {
  butil::rapidjson::StringBuffer buffer;
  butil::rapidjson::PrettyWriter<butil::rapidjson::StringBuffer> writer(buffer);
  writer.SetIndent(' ', 2);

  writer.StartObject();

  writer.Key("version");
  writer.AddUint(kVersion);

  // -------------------------------------------------------------------------
  // db
  // -------------------------------------------------------------------------
  writer.Key("db");
  writer.StartObject();
  writer.Key("create_if_missing"); writer.Bool(db.create_if_missing);
  writer.Key("error_if_exists"); writer.Bool(db.error_if_exists);
  writer.Key("paranoid_checks"); writer.Bool(db.paranoid_checks);
  writer.Key("memtable_threshold");
  writer.AddUint64(static_cast<uint64_t>(db.memtable_threshold));
  writer.Key("write_buffer_size");
  writer.AddUint64(static_cast<uint64_t>(db.write_buffer_size));
  writer.Key("column_id"); writer.AddUint(db.column_id);
  writer.Key("enable_bloom_filter"); writer.Bool(db.enable_bloom_filter);
  writer.Key("bloom_bits_per_key"); writer.AddInt(db.bloom_bits_per_key);
  writer.Key("verify_checksums"); writer.Bool(db.verify_checksums);
  writer.EndObject();

  // -------------------------------------------------------------------------
  // lsm
  // -------------------------------------------------------------------------
  writer.Key("lsm");
  writer.StartObject();
  writer.Key("min_files_for_compaction");
  writer.AddUint64(static_cast<uint64_t>(lsm.min_files_for_compaction));
  writer.Key("min_size_for_compaction");
  writer.AddUint64(static_cast<uint64_t>(lsm.min_size_for_compaction));
  writer.Key("target_file_size");
  writer.AddUint64(static_cast<uint64_t>(lsm.target_file_size));
  writer.Key("max_levels"); writer.AddInt(lsm.max_levels);
  writer.Key("level_size_multiplier"); writer.Double(lsm.level_size_multiplier);
  writer.Key("level0_file_num_compaction_trigger");
  writer.AddUint64(static_cast<uint64_t>(lsm.level0_file_num_compaction_trigger));
  writer.Key("level0_slowdown_writes_trigger");
  writer.AddUint64(static_cast<uint64_t>(lsm.level0_slowdown_writes_trigger));
  writer.Key("level0_stop_writes_trigger");
  writer.AddUint64(static_cast<uint64_t>(lsm.level0_stop_writes_trigger));
  writer.EndObject();

  // -------------------------------------------------------------------------
  // wal
  // -------------------------------------------------------------------------
  writer.Key("wal");
  writer.StartObject();
  writer.Key("max_file_size");
  writer.AddUint64(static_cast<uint64_t>(wal.max_file_size));
  writer.Key("group_commit_timeout_us"); writer.AddUint(wal.group_commit_timeout_us);
  writer.Key("group_commit_max_batch");
  writer.AddUint64(static_cast<uint64_t>(wal.group_commit_max_batch));
  writer.Key("use_fsync"); writer.Bool(wal.use_fsync);
  writer.Key("preallocate_size");
  writer.AddUint64(static_cast<uint64_t>(wal.preallocate_size));
  writer.Key("enable_sharded_wal"); writer.Bool(wal.enable_sharded_wal);
  writer.Key("num_shards"); writer.AddUint(wal.num_shards);
  writer.Key("bind_by_thread_id"); writer.Bool(wal.bind_by_thread_id);
  writer.Key("max_file_size_per_shard");
  writer.AddUint64(static_cast<uint64_t>(wal.max_file_size_per_shard));
  writer.Key("batch_timeout_us"); writer.AddUint(wal.batch_timeout_us);
  writer.Key("batch_max_size");
  writer.AddUint64(static_cast<uint64_t>(wal.batch_max_size));
  writer.Key("enable_background_merger");
  writer.Bool(wal.enable_background_merger);
  writer.Key("merge_interval_ms"); writer.AddUint(wal.merge_interval_ms);
  writer.EndObject();

  // -------------------------------------------------------------------------
  // memtable
  // -------------------------------------------------------------------------
  writer.Key("memtable");
  writer.StartObject();
  writer.Key("type"); writer.AddInt(static_cast<int>(memtable.type));
  writer.Key("enable_lockfree_memtable");
  writer.Bool(memtable.enable_lockfree_memtable);
  writer.Key("initial_capacity");
  writer.AddUint64(static_cast<uint64_t>(memtable.initial_capacity));
  writer.Key("rehash_threshold"); writer.Double(memtable.rehash_threshold);
  writer.Key("enable_preallocation");
  writer.Bool(memtable.enable_preallocation);
  writer.Key("preallocation_pool_size");
  writer.AddUint64(static_cast<uint64_t>(memtable.preallocation_pool_size));
  writer.Key("gc_interval_ms"); writer.AddUint(memtable.gc_interval_ms);
  writer.Key("gc_batch_size");
  writer.AddUint64(static_cast<uint64_t>(memtable.gc_batch_size));
  writer.EndObject();

  // -------------------------------------------------------------------------
  // mvcc
  // -------------------------------------------------------------------------
  writer.Key("mvcc");
  writer.StartObject();
  writer.Key("enable_sharded_timestamp_allocator");
  writer.Bool(mvcc.enable_sharded_timestamp_allocator);
  writer.Key("timestamp_shard_count"); writer.AddUint(mvcc.timestamp_shard_count);
  writer.Key("timestamp_batch_size"); writer.AddUint(mvcc.timestamp_batch_size);
  writer.Key("enable_version_chain_index");
  writer.Bool(mvcc.enable_version_chain_index);
  writer.Key("version_chain_index_threshold");
  writer.AddUint64(static_cast<uint64_t>(mvcc.version_chain_index_threshold));
  writer.Key("version_chain_max_level"); writer.AddInt(mvcc.version_chain_max_level);
  writer.Key("enable_delta_encoding"); writer.Bool(mvcc.enable_delta_encoding);
  writer.Key("delta_max_per_group");
  writer.AddUint64(static_cast<uint64_t>(mvcc.delta_max_per_group));
  writer.Key("enable_temporal_bloom_filter");
  writer.Bool(mvcc.enable_temporal_bloom_filter);
  writer.Key("temporal_filter_false_positive_rate");
  writer.Double(mvcc.temporal_filter_false_positive_rate);
  writer.Key("temporal_filter_hours_per_bucket");
  writer.AddUint(mvcc.temporal_filter_hours_per_bucket);
  writer.Key("enable_sharded_wal"); writer.Bool(mvcc.enable_sharded_wal);
  writer.Key("enable_lockfree_memtable");
  writer.Bool(mvcc.enable_lockfree_memtable);
  writer.Key("enable_async_index_builder");
  writer.Bool(mvcc.enable_async_index_builder);
  writer.Key("index_builder_worker_threads");
  writer.AddUint(mvcc.index_builder_worker_threads);
  writer.Key("index_builder_max_concurrent");
  writer.AddUint(mvcc.index_builder_max_concurrent);
  writer.Key("index_builder_batch_size");
  writer.AddUint64(static_cast<uint64_t>(mvcc.index_builder_batch_size));
  writer.Key("index_builder_batch_timeout_ms");
  writer.AddUint(mvcc.index_builder_batch_timeout_ms);
  writer.Key("enable_build_cache"); writer.Bool(mvcc.enable_build_cache);
  writer.Key("build_cache_size");
  writer.AddUint64(static_cast<uint64_t>(mvcc.build_cache_size));
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
  writer.AddInt(transaction.default_isolation_level);
  writer.Key("timeout_ms");
  writer.AddUint64(static_cast<uint64_t>(transaction.timeout_ms));
  writer.Key("max_retries"); writer.AddUint(transaction.max_retries);
  writer.Key("parallel_validation");
  writer.Bool(transaction.parallel_validation);
  writer.Key("validation_threads"); writer.AddUint(transaction.validation_threads);
  writer.Key("enable_occ"); writer.Bool(transaction.enable_occ);
  writer.Key("max_write_set_size");
  writer.AddUint64(static_cast<uint64_t>(transaction.max_write_set_size));
  writer.Key("max_read_set_size");
  writer.AddUint64(static_cast<uint64_t>(transaction.max_read_set_size));
  writer.EndObject();

  // -------------------------------------------------------------------------
  // cache
  // -------------------------------------------------------------------------
  writer.Key("cache");
  writer.StartObject();
  writer.Key("block_cache_size");
  writer.AddUint64(static_cast<uint64_t>(cache.block_cache_size));
  writer.Key("table_cache_size");
  writer.AddUint64(static_cast<uint64_t>(cache.table_cache_size));
  writer.Key("block_restart_interval");
  writer.AddInt(cache.block_restart_interval);
  writer.Key("block_size");
  writer.AddUint64(static_cast<uint64_t>(cache.block_size));
  writer.Key("version_chain_cache_size");
  writer.AddUint64(static_cast<uint64_t>(cache.version_chain_cache_size));
  writer.Key("enable_version_chain_cache");
  writer.Bool(cache.enable_version_chain_cache);
  writer.EndObject();

  // -------------------------------------------------------------------------
  // filesystem
  // -------------------------------------------------------------------------
  writer.Key("filesystem");
  writer.StartObject();
  writer.Key("max_open_files"); writer.AddInt(filesystem.max_open_files);
  writer.Key("use_direct_io"); writer.Bool(filesystem.use_direct_io);
  writer.Key("advise_random_access");
  writer.Bool(filesystem.advise_random_access);
  writer.Key("prefetch_buffer_size");
  writer.AddUint64(static_cast<uint64_t>(filesystem.prefetch_buffer_size));
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
  writer.AddUint(debug.stats_dump_interval_sec);
  writer.Key("enable_slow_log"); writer.Bool(debug.enable_slow_log);
  writer.Key("slow_log_threshold_ms");
  writer.AddUint(debug.slow_log_threshold_ms);
  writer.Key("enable_trace"); writer.Bool(debug.enable_trace);
  writer.EndObject();

  // -------------------------------------------------------------------------
  // limits
  // -------------------------------------------------------------------------
  writer.Key("limits");
  writer.StartObject();
  writer.Key("max_query_length");
  writer.AddUint64(static_cast<uint64_t>(limits.max_query_length));
  writer.Key("max_batch_items");
  writer.AddUint64(static_cast<uint64_t>(limits.max_batch_items));
  writer.Key("max_parameter_count");
  writer.AddUint64(static_cast<uint64_t>(limits.max_parameter_count));
  writer.Key("max_parameter_value_length");
  writer.AddUint64(static_cast<uint64_t>(limits.max_parameter_value_length));
  writer.Key("max_timeout_ms"); writer.AddUint(limits.max_timeout_ms);
  writer.Key("max_heartbeat_partitions");
  writer.AddUint64(static_cast<uint64_t>(limits.max_heartbeat_partitions));
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
