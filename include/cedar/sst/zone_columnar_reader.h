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

// =============================================================================
// Zone-Columnar SST Reader - 谓词下推与延迟物化
// =============================================================================
// 针对 Entity-Aligned Block 设计优化的读取器：
// 
// 1. 谓词下推 (Predicate Pushdown)：
//    - Zone Map 过滤 (文件级)
//    - Bloom Filter 过滤 (Block 级)  
//    - Zone 0 范围探测 (Row 级)
//    - Zone 3 语义过滤 (Column/Type 级)
//
// 2. 延迟物化 (Late Materialization)：
//    - 先读取 Zone 0/3 过滤
//    - 只在需要时读取 Zone 4 (Values)
//    - 减少 60-80% 的 I/O
//
// 3. Block 级别缓存：
//    - 每个 Block 独立解压
//    - Block 内随机访问 O(1)
// =============================================================================

#ifndef FERN_ZONE_COLUMNAR_READER_H_
#define FERN_ZONE_COLUMNAR_READER_H_

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <optional>
#include <unordered_map>
#include <shared_mutex>

#include "cedar/core/status.h"
#include "cedar/core/slice.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/sst/zone_columnar_format_v2.h"
#include "cedar/sst/bloom_filter.h"

// V2 Block Header (44 bytes)
struct BlockHeader {
  uint32_t row_count;              // 行数
  uint32_t zone_sizes[6];          // 6 个 Zone 的大小
  uint64_t min_entity_id;          // 本 Block 最小 Entity
  uint64_t max_entity_id;          // 本 Block 最大 Entity
  
  static constexpr size_t kSize = 4 + 24 + 8 + 8;  // 44 bytes
};
#include "cedar/sst/zone_encoder.h"

namespace cedar {

// 前向声明
class RandomAccessFile;
class SimpleSSTBlobManager;

// =============================================================================
// Block 缓存条目
// =============================================================================
struct BlockCacheEntry {
  uint32_t block_id = 0;
  uint32_t start_row = 0;
  uint32_t num_rows = 0;
  uint64_t first_entity_id = 0;
  uint64_t last_entity_id = 0;
  uint64_t min_entity_id = 0;
  uint64_t max_entity_id = 0;
  
  // Zone 解码器（延迟初始化）- 使用 unique_ptr 存储，支持移动语义 (V1 legacy)
  std::unique_ptr<EntityIdZoneEncoder::Decoder> entity_decoder;
  std::unique_ptr<TimestampZoneEncoder::Decoder> timestamp_decoder;
  std::unique_ptr<TargetIdZoneEncoder::Decoder> target_decoder;
  std::unique_ptr<KeyMetadataZoneEncoder::Decoder> metadata_decoder;
  std::unique_ptr<ValueZoneEncoder::Decoder> value_decoder;
  
  // Zone 原始数据 (V1 legacy)
  std::string zone0_data;
  std::string zone1_data;
  std::string zone2_data;
  std::string zone3_column_rle;
  std::string zone3_seq_rle;
  std::string zone3_type_bitmap;
  std::string zone3_flags_bitmap;
  std::string zone3_part_rle;
  std::string zone4_data;
  
  // V2 format: raw block data with zone offsets
  std::string data;
  size_t zone_offsets[6] = {0, 0, 0, 0, 0, 0};
  size_t zone_sizes[6] = {0, 0, 0, 0, 0, 0};
  
  // 禁用拷贝，支持移动
  BlockCacheEntry() = default;
  BlockCacheEntry(const BlockCacheEntry&) = delete;
  BlockCacheEntry& operator=(const BlockCacheEntry&) = delete;
  BlockCacheEntry(BlockCacheEntry&&) = default;
  BlockCacheEntry& operator=(BlockCacheEntry&&) = default;
  
  bool IsEntityInBlock(uint64_t entity_id) const {
    return entity_id >= min_entity_id && entity_id <= max_entity_id;
  }
  
  ~BlockCacheEntry() = default;
};

// =============================================================================
// 谓词定义
// =============================================================================
struct ReadPredicate {
  // Entity 过滤
  std::optional<uint64_t> entity_id;
  std::optional<uint8_t> entity_type;
  
  // 属性/边类型过滤
  std::optional<uint16_t> column_id;
  
  // 分区过滤 (用于 partition migration / scan)
  std::optional<uint16_t> part_id;
  
  // 时间戳范围
  std::optional<uint64_t> min_timestamp;
  std::optional<uint64_t> max_timestamp;
  
  // Flags 过滤
  std::optional<uint8_t> op_type;  // 0=CREATE, 1=UPDATE, 2=DELETE
  bool skip_tombstones = true;
  
  // 检查 Key 是否匹配谓词
  bool Matches(const CedarKey& key) const {
    if (entity_id && key.entity_id() != *entity_id) return false;
    if (entity_type && static_cast<uint8_t>(key.entity_type()) != *entity_type) return false;
    if (column_id && key.column_id() != *column_id) return false;
    if (part_id && key.part_id() != *part_id) return false;
    if (min_timestamp && key.timestamp().value() < *min_timestamp) return false;
    if (max_timestamp && key.timestamp().value() > *max_timestamp) return false;
    if (op_type && key.GetOpType() != *op_type) return false;
    if (skip_tombstones && key.IsTombstone()) return false;
    return true;
  }
  
  // 检查是否只关心特定 entity
  bool IsSingleEntityQuery() const {
    return entity_id.has_value() && !column_id.has_value();
  }
  
  // 检查是否点查
  bool IsPointQuery() const {
    return entity_id.has_value() && column_id.has_value() && 
           min_timestamp.has_value() && max_timestamp.has_value() &&
           *min_timestamp == *max_timestamp;
  }
};

// =============================================================================
// 扫描结果回调
// =============================================================================
using ScanCallback = std::function<void(const CedarKey& key, const Descriptor& desc)>;
using LateMaterializationCallback = std::function<void(uint32_t row_idx, const CedarKey& key)>;

// =============================================================================
// Zone-Columnar SST Reader
// =============================================================================
class ZoneColumnarSstReader {
 public:
  // 打开现有 SST 文件
  // 从文件打开
  explicit ZoneColumnarSstReader(const std::string& file_path);
  
  // 从内存缓冲区打开（用于测试）
  ZoneColumnarSstReader(const char* data, size_t size);
  
  ~ZoneColumnarSstReader();

  ZoneColumnarSstReader(const ZoneColumnarSstReader&) = delete;
  ZoneColumnarSstReader& operator=(const ZoneColumnarSstReader&) = delete;

  // 打开并解析文件
  Status Open();
  void Close();
  
  // ==================== 迭代器 ====================
  
  // 创建迭代器（调用者负责 delete）
  class Iterator;
  Iterator* NewIterator() const;
  
  // ==================== 谓词下推查询接口 ====================
  
  // 点查：获取指定 Key 的 Value（全谓词匹配）
  std::optional<Descriptor> Get(const CedarKey& key) const;
  
  // 获取时间范围
  std::vector<std::tuple<CedarKey, Descriptor, Timestamp>> GetRange(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      Timestamp start,
      Timestamp end) const;
  
  // 带谓词的范围扫描
  void Scan(const ReadPredicate& predicate, ScanCallback callback) const;
  
  // 获取序列化的 TemporalBloomFilter 数据（如果 SST 包含）
  const std::string& GetTemporalFilterData() const { return temporal_filter_data_; }
  
  // 延迟物化扫描：先过滤，再物化匹配的 Value
  // 1. 先扫描 Zone 0/3，收集匹配的行号
  // 2. 再批量读取 Zone 4
  void ScanWithLateMaterialization(
      const ReadPredicate& predicate,
      LateMaterializationCallback row_callback,
      std::vector<std::pair<CedarKey, Descriptor>>* results) const;

  // 获取指定时间的最新版本（高效点查）
  std::optional<Descriptor> GetAtTime(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      Timestamp timestamp) const;
  
  // 从 Block 中重建 TxnVersion
  Timestamp GetTxnVersionFromBlock(
      const BlockCacheEntry& block, uint32_t row_idx) const;
  
  // 批量时间范围查询（针对多个 entity）
  std::unordered_map<uint64_t, std::vector<std::tuple<CedarKey, Descriptor, Timestamp>>>
  BatchGetRange(const std::vector<uint64_t>& entity_ids,
                EntityType entity_type,
                uint16_t column_id,
                Timestamp start,
                Timestamp end) const;

  // ==================== 高效随机访问 (V1 兼容) ====================
  
  // 获取列 ID (V1 兼容)
  uint16_t ColumnId() const { return header_.column_id; }
  
  // 获取实体类型 (V1 兼容)
  uint8_t GetEntityType() const { return header_.entity_type; }
  
  // 获取行数
  uint64_t NumEntries() const { return footer_.row_count; }
  
  // 获取 Block 数量
  uint32_t NumBlocks() const { return footer_.block_count; }
  
  // 根据行号重建完整的 CedarKey（O(1)）
  CedarKey ReconstructKey(uint32_t row_idx) const;
  
  // 根据行号获取 Value（O(1)，需要已加载 Block）
  // 从 block 获取值（高效，已知 block）
  std::optional<Descriptor> GetValueByRow(const BlockCacheEntry& block, uint32_t local_idx) const;
  
  // 从全局行号获取值（自动加载 block）(V1 兼容)
  std::optional<Descriptor> GetValueByRow(uint32_t row_idx) const;
  
  // 批量获取 Values（优化顺序读取）
  std::vector<Descriptor> GetValuesByRows(const std::vector<uint32_t>& row_indices) const;

  // ==================== Block 级别过滤 ====================
  
  // Zone Map 过滤：快速排除不相关的 Block
  bool MayContainEntity(uint64_t entity_id) const;
  bool MayContainTimeRange(uint64_t start_ts, uint64_t end_ts) const;
  bool MayMatchPredicate(const ReadPredicate& predicate) const;

  // ==================== 统计与元数据 ====================
  
  struct Stats {
    uint64_t total_blocks = 0;
    uint64_t total_rows = 0;
    uint64_t unique_entities = 0;
    uint64_t avg_rows_per_block = 0;
    double avg_block_compression_ratio = 0.0;
  };
  Stats GetStats() const;
  
  // 读取性能统计
  struct ReadStats {
    uint64_t zone_map_filtered_blocks = 0;    // Zone Map 过滤的 Block 数
    uint64_t bloom_filtered_blocks = 0;       // Bloom Filter 过滤的 Block 数
    uint64_t predicate_filtered_rows = 0;     // 谓词过滤的行数
    uint64_t materialized_rows = 0;           // 实际物化的行数
    uint64_t blocks_loaded = 0;               // 加载的 Block 数
    uint64_t bytes_read_from_disk = 0;        // 磁盘读取字节数
  };
  mutable ReadStats read_stats_;
  ReadStats GetReadStats() const { return read_stats_; }
  void ResetReadStats() const { read_stats_ = ReadStats(); }

  // ==================== 元数据访问 ====================
  
  uint64_t MinTimestamp() const { return header_.min_timestamp; }
  uint64_t MaxTimestamp() const { return header_.max_timestamp; }
  uint64_t MinEntityId() const { return header_.min_entity_id; }
  uint64_t MaxEntityId() const { return header_.max_entity_id; }
  
  const ZoneColumnarHeader& Header() const { return header_; }
  const ZoneColumnarFooter& Footer() const { return footer_; }

 private:
  // 加载文件元数据
  Status LoadMetadata();
  
  // 加载指定 Block（按需解压）
  std::shared_ptr<BlockCacheEntry> LoadBlock(uint32_t block_id) const;
  
  // 查找包含指定 entity 的 Block
  std::vector<uint32_t> FindBlocksForEntity(uint64_t entity_id) const;
  
  // 使用 Entity Index 快速定位行号
  std::vector<uint32_t> FindRowsForEntity(uint64_t entity_id) const;
  
  // 二分查找 Restart Points
  size_t BinarySearchRestartPoint(uint64_t entity_id) const;
  
  // 在 Block 内二分查找行号范围
  std::pair<uint32_t, uint32_t> FindRowRangeInBlock(
      const BlockCacheEntry& block, uint64_t entity_id) const;
  
  // 谓词匹配（不物化 Value）
  bool RowMatchesPredicate(const BlockCacheEntry& block, 
                           uint32_t row_idx, 
                           const ReadPredicate& predicate) const;
  
  // 从 Block 重建 Key
  CedarKey ReconstructKeyFromBlock(const BlockCacheEntry& block, uint32_t row_idx) const;

  std::string file_path_;
  std::unique_ptr<RandomAccessFile> file_;
  
  // 文件元数据 (V2 格式)
  ZoneColumnarHeader header_;
  ZoneColumnarFooter footer_;
  std::vector<BlockIndexEntry> block_index_;
  std::vector<ZoneRestartPoint> restart_points_;
  
  // Block 偏移表
  struct BlockInfo {
    uint64_t offset;
    uint64_t size;
    uint32_t start_row;
    uint32_t row_count;
    uint64_t first_entity_id;
  };
  std::vector<BlockInfo> block_infos_;
  
  // Entity Index (entity_id -> [row_indices])
  std::unordered_map<uint64_t, std::vector<uint32_t>> entity_index_;
  bool has_entity_index_ = false;
  
  // Bloom Filter
  BloomFilter bloom_filter_;
  
  // Temporal Bloom Filter 序列化数据
  std::string temporal_filter_data_;
  
  // Block 缓存（LRU，可配置大小）
  mutable std::unordered_map<uint32_t, std::shared_ptr<BlockCacheEntry>> block_cache_;
  mutable std::shared_mutex block_cache_mutex_;  // Changed from std::mutex for read concurrency
  static constexpr size_t kMaxCachedBlocks = 256;  // Increased from 16 for better cache hit rate
  
  // 内存缓冲区模式（用于测试）
  const char* buffer_data_ = nullptr;
  size_t buffer_size_ = 0;
  bool owns_buffer_ = false;
  
  bool opened_ = false;
  
  // 从内存缓冲区加载元数据
  Status LoadMetadataFromBuffer();
  
  // 友元声明：允许 Iterator 访问私有成员
  friend class Iterator;
};

// =============================================================================
// ZoneColumnarSstReader::Iterator - 迭代器实现
// =============================================================================
class ZoneColumnarSstReader::Iterator {
 public:
  explicit Iterator(const ZoneColumnarSstReader* reader);

  // 定位到第一行
  void SeekToFirst();
  
  // 定位到指定 Key
  void Seek(const CedarKey& key);
  
  // 移动到下一行
  void Next();
  
  // 是否有效
  bool Valid() const { return valid_ && current_idx_ < total_count_; }
  
  // 获取当前 Key
  CedarKey Key() const;
  
  // 获取当前 Value
  Descriptor Value() const;
  
  // 获取当前 TxnVersion (MVCC)
  Timestamp TxnVersion() const;
  
  // 获取当前行号
  size_t RowIndex() const { return current_idx_; }

 private:
  const ZoneColumnarSstReader* reader_;
  size_t current_idx_ = 0;
  size_t total_count_ = 0;
  bool valid_ = false;
  
  // 当前加载的 block
  mutable std::shared_ptr<BlockCacheEntry> current_block_;
  mutable uint32_t current_block_id_ = UINT32_MAX;
  mutable uint32_t current_block_start_row_ = 0;
  
  // 确保指定行所在的 block 已加载
  Status EnsureBlockLoaded(uint32_t row_idx) const;
};

// 简化类型别名 - ZoneColumnarSstReader 是标准的 SST Reader
using SstReader = ZoneColumnarSstReader;

}  // namespace cedar

#endif  // FERN_ZONE_COLUMNAR_READER_H_
