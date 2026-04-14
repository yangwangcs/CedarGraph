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
// Zone-Columnar SST Builder - Entity-Aligned Block Partitioning
// =============================================================================
// 实现"逻辑行排序，物理列拆分"的混合存储结构：
// 
// 全局排序契约：
// 1. entity_id ASC (首要聚簇关键字)
// 2. entity_type ASC
// 3. column_id ASC
// 4. target_id ASC
// 5. timestamp_be DESC (降序 - 最新版本在前)
// 6. sequence ASC
//
// 实体对齐的块设计：
// - 块大小默认 64KB
// - 块切割时尽量不分割同一个 entity_id 的记录
// - 利用 Zone 0 RLE 压缩连续相同的 entity_id
// =============================================================================

#ifndef FERN_ZONE_COLUMNAR_BUILDER_H_
#define FERN_ZONE_COLUMNAR_BUILDER_H_

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <functional>

#include "cedar/core/status.h"
#include "cedar/core/slice.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/sst/zone_columnar_format.h"
#include "cedar/sst/blob_file_manager.h"

namespace cedar {

// 前向声明
class WritableFile;

// =============================================================================
// Entity-Aligned Block 定义
// =============================================================================
// 每个 Block 包含连续的行，尽量保持同一个 entity_id 的数据在同一个 Block 中
struct EntityAlignedBlock {
  uint32_t start_row;           // 全局起始行号
  uint32_t row_count;           // 行数
  uint64_t first_entity_id;     // 首个 entity_id
  uint64_t last_entity_id;      // 最后一个 entity_id
  
  // 各 Zone 的编码数据
  std::string zone0_data;       // Entity IDs (RLE)
  std::string zone1_data;       // Timestamps (Delta-Delta)
  std::string zone2_data;       // Target IDs (Delta/RLE)
  // Zone 3: Key Metadata (8B) - 5 个独立字段
  std::string zone3_column_rle;
  std::string zone3_seq_rle;
  std::string zone3_type_bitmap;
  std::string zone3_flags_bitmap;
  std::string zone3_part_rle;
  std::string zone4_data;       // Values (Dictionary/LZ4)
  
  // 统计信息
  uint64_t min_timestamp = UINT64_MAX;
  uint64_t max_timestamp = 0;
  uint32_t tombstone_count = 0;
  
  size_t TotalSize() const {
    return zone0_data.size() + zone1_data.size() + zone2_data.size() +
           zone3_column_rle.size() + zone3_seq_rle.size() +
           zone3_type_bitmap.size() + zone3_flags_bitmap.size() +
           zone3_part_rle.size() + zone4_data.size();
  }
};

// =============================================================================
// Zone-Columnar SST Builder
// =============================================================================
class ZoneColumnarSstBuilder {
 public:
  struct Options {
    size_t block_size = 64 * 1024;        // 块大小 (默认 64KB)
    size_t block_row_limit = 4096;        // 每块最大行数
    bool entity_aligned = true;           // 启用实体对齐
    size_t entity_align_threshold = 256;  // 实体对齐阈值 (同一 entity 超过此数量强制切割)
    bool enable_blob = true;              // 启用 Blob 存储
    size_t blob_threshold = 256;          // Blob 阈值 (超过此大小的 value 存 Blob)
    std::string db_path;                  // 数据库路径 (用于 Blob 文件)
  };
  
  ZoneColumnarSstBuilder(const Options& options, WritableFile* file);
  
  ~ZoneColumnarSstBuilder();

  ZoneColumnarSstBuilder(const ZoneColumnarSstBuilder&) = delete;
  ZoneColumnarSstBuilder& operator=(const ZoneColumnarSstBuilder&) = delete;

  // 添加一个条目 (Key 必须按全局排序契约有序)
  // 要求: 添加的 Key 必须满足 CompareForSorting 的升序
  void Add(const CedarKey& key, const Descriptor& descriptor);
  
  // 批量添加 (内部会排序)
  void AddBatch(std::vector<std::pair<CedarKey, Descriptor>>& batch);
  
  // 添加原始数据（自动决定内联或 Blob 存储）
  void AddValue(const CedarKey& key, const Slice& raw_value);
  
  // 完成构建
  Status Finish();
  
  // 放弃构建
  void Abandon();
  
  // 获取当前文件大小
  uint64_t FileSize() const { return file_size_; }
  
  // 获取条目数
  uint64_t NumEntries() const { return total_entries_; }
  
  // 获取块数
  uint32_t NumBlocks() const { return blocks_.size(); }
  
  // 设置文件号（用于版本链）
  void SetFileNumber(uint64_t file_number) { file_number_ = file_number; }
  void SetPrevFileNumber(uint64_t file_number) { prev_file_number_ = file_number; }
  
  // 设置层级
  void SetLevel(uint32_t level) { level_ = level; }
  
  // 获取列 ID (兼容 V1 接口)
  uint16_t ColumnId() const { return column_id_; }

  // 检查状态
  bool ok() const { return status_.ok(); }
  Status status() const { return status_; }
  
  // 获取统计信息
  struct Stats {
    uint64_t total_entries = 0;
    uint64_t total_blocks = 0;
    uint64_t total_entities = 0;      // 不同 entity_id 的数量
    uint64_t avg_rows_per_block = 0;
    uint64_t entity_span_crossings = 0;  // 跨块切割的 entity 数量
    double compression_ratio = 0.0;
  };
  Stats GetStats() const;

 private:
  // 内部行缓冲区
  struct RowBuffer {
    std::vector<uint64_t> entity_ids;
    std::vector<uint64_t> timestamps;
    std::vector<uint64_t> target_ids;
    std::vector<uint16_t> column_ids;
    std::vector<uint16_t> sequences;
    std::vector<uint8_t>  entity_types;
    std::vector<uint8_t>  flags;
    std::vector<uint16_t> part_ids;
    std::vector<Descriptor> values;
    
    size_t Size() const { return entity_ids.size(); }
    void Clear() {
      entity_ids.clear(); timestamps.clear(); target_ids.clear();
      column_ids.clear(); sequences.clear(); entity_types.clear();
      flags.clear(); part_ids.clear(); values.clear();
    }
    void Reserve(size_t n) {
      entity_ids.reserve(n); timestamps.reserve(n); target_ids.reserve(n);
      column_ids.reserve(n); sequences.reserve(n); entity_types.reserve(n);
      flags.reserve(n); part_ids.reserve(n); values.reserve(n);
    }
  };
  
  // 块切割决策
  bool ShouldCutBlock() const;
  
  // 刷新当前缓冲区为 Block
  Status FlushBlock();
  
  // 编码单个 Block
  EntityAlignedBlock EncodeBlock(const RowBuffer& buffer, uint32_t start_row);
  
  // 写入所有 Blocks
  Status WriteBlocks();
  
  // 写入 Zone Maps
  Status WriteZoneMaps();
  
  // 写入 Restart Points
  Status WriteRestartPoints();
  
  // 写入 Bloom Filter
  Status WriteBloomFilter();
  
  // 写入 Entity Index
  Status WriteEntityIndex();
  
  // 写入 Footer
  Status WriteFooter();
  
  // 计算校验和
  uint64_t CalculateCRC64(const Slice& data) const;
  
  // 检查 Value 是否需要存 Blob
  bool ShouldStoreInBlob(const Descriptor& desc) const;
  
  // 写入 Blob
  Status WriteBlob(uint64_t entity_id, uint16_t column_id, uint64_t timestamp,
                   const Descriptor& desc, std::string* ref_out);

  Options options_;
  WritableFile* file_;
  
  // 当前缓冲区
  RowBuffer buffer_;
  
  // 已完成的 Blocks
  std::vector<EntityAlignedBlock> blocks_;
  
  // 当前 block 的起始行号
  uint32_t current_block_start_row_ = 0;
  
  // 统计
  uint64_t total_entries_ = 0;
  uint64_t total_entities_ = 0;
  uint64_t file_size_ = 0;
  uint64_t last_entity_id_ = 0;
  bool has_last_entity_ = false;
  
  // 全局统计
  uint64_t min_timestamp_ = UINT64_MAX;
  uint64_t max_timestamp_ = 0;
  uint64_t min_entity_id_ = UINT64_MAX;
  uint64_t max_entity_id_ = 0;
  uint32_t tombstone_count_ = 0;
  
  // Bloom Filter
  BloomFilter bloom_filter_;
  
  // Restart Points
  std::vector<ZoneRestartPoint> restart_points_;
  
  // Entity Index
  std::unordered_map<uint64_t, std::vector<uint32_t>> entity_index_;
  
  // Blob 管理器 (TODO: 实现)
  // std::unique_ptr<SimpleSSTBlobManager> blob_manager_;
  
  // 列 ID 和实体类型 (兼容 V1)
  uint16_t column_id_ = 0;
  uint8_t entity_type_ = 0;
  
  // 状态
  Status status_;
  bool closed_ = false;
  
  // 版本链
  uint64_t file_number_ = 0;
  uint64_t prev_file_number_ = 0;
  uint32_t level_ = 0;
  
  // Header (延迟写入)
  ZoneColumnarHeader header_;
};

// 简化类型别名 - ZoneColumnarSstBuilder 是标准的 SST Builder
using SstBuilder = ZoneColumnarSstBuilder;

}  // namespace cedar

#endif  // FERN_ZONE_COLUMNAR_BUILDER_H_
