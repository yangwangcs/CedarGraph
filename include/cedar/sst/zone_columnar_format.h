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
// Zone-Columnar SST Format (SST v2) - 文件格式定义
// =============================================================================
// 文件结构：
// ┌──────────────────────────────────────────────────────────────┐
// │ Header (256 bytes)                                           │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone 0: Entity IDs (RLE 编码)                                │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone 1: Timestamps (Delta-of-Delta)                          │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone 2: Target IDs (Delta/RLE)                               │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone 3: Values (Dictionary/LZ4)                              │
// ├──────────────────────────────────────────────────────────────┤
// │ Zone Maps (每 Zone 统计信息)                                 │
// ├──────────────────────────────────────────────────────────────┤
// │ Restart Points (稀疏索引)                                    │
// ├──────────────────────────────────────────────────────────────┤
// │ Bloom Filter (可选)                                          │
// ├──────────────────────────────────────────────────────────────┤
// │ Footer (128 bytes)                                           │
// └──────────────────────────────────────────────────────────────┘
// =============================================================================

#ifndef FERN_ZONE_COLUMNAR_FORMAT_H_
#define FERN_ZONE_COLUMNAR_FORMAT_H_

#include <cstdint>
#include <string>
#include <vector>
#include <array>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/sst/bloom_filter.h"
#include "cedar/sst/zone_encoder.h"
#include "cedar/storage/block_cache.h"
#include "cedar/storage/cedar_options.h"
// WritableFile is in env.h which is included by cedar_options.h

namespace cedar {

// 前向声明：SST 专用的 Blob 管理器（定义在 zone_columnar_format.cc 中）
class SimpleSSTBlobManager;

// Zone-Columnar 文件魔数
static constexpr uint32_t kZoneColumnarMagic = 0x5A434F4C;  // "ZCOL"
static constexpr uint32_t kZoneColumnarVersion = 1;  // 版本 1

// =============================================================================
// Zone-Columnar Header (256 bytes)
// =============================================================================
#pragma pack(push, 1)
struct ZoneColumnarHeader {
  // ===== Magic & Version (8 bytes) =====
  uint32_t magic = kZoneColumnarMagic;
  uint32_t version = kZoneColumnarVersion;

  // ===== 文件元信息 (16 bytes) =====
  uint32_t flags = 0;               // 文件标志
  uint16_t column_id = 0;           // 列 ID / 边类型 ID
  uint8_t  entity_type = 0;         // 0=Vertex, 1=EdgeOut, 2=EdgeIn
  uint8_t  reserved1 = 0;
  uint32_t row_count = 0;           // 总行数
  uint32_t block_size = 65536;      // 块大小（默认 64KB）

  // ===== Zone 信息 (64 bytes = 4 zones * 16 bytes) =====
  // 每个 Zone 的编码类型和压缩类型
  struct ZoneInfo {
    uint8_t encoding_type = 0;      // 编码类型
    uint8_t compression_type = 0;   // 压缩类型
    uint16_t reserved = 0;
    uint32_t data_offset = 0;       // 数据偏移（相对于文件头）
    uint32_t data_size = 0;         // 压缩后数据大小
    uint32_t uncompressed_size = 0; // 解压后大小
  } zone0;  // EntityIds
  
  struct ZoneInfo1 {
    uint8_t encoding_type = 0;
    uint8_t compression_type = 0;
    uint16_t reserved = 0;
    uint32_t data_offset = 0;
    uint32_t data_size = 0;
    uint32_t uncompressed_size = 0;
  } zone1;  // Timestamps
  
  struct ZoneInfo2 {
    uint8_t encoding_type = 0;
    uint8_t compression_type = 0;
    uint16_t reserved = 0;
    uint32_t data_offset = 0;
    uint32_t data_size = 0;
    uint32_t uncompressed_size = 0;
  } zone2;  // TargetIds
  
  struct ZoneInfo3 {
    // Zone 3 包含 5 个独立编码的字段，对应 CedarKey 8B 元数据区
    uint8_t encoding_type = 0;        // 主编码类型
    uint8_t compression_type = 0;     // 压缩类型
    uint16_t reserved = 0;
    
    // Column ID (2B) - φ: Predicate ID
    uint32_t column_rle_offset = 0;
    uint32_t column_rle_size = 0;
    
    // Sequence (2B) - κ: intra-microsecond ordering
    uint32_t seq_rle_offset = 0;
    uint32_t seq_rle_size = 0;
    
    // Entity Type (1B) - τ: Vertex/EdgeOut/EdgeIn
    uint32_t type_bitmap_offset = 0;
    uint32_t type_bitmap_size = 0;
    
    // Flags (1B) - δ: OpType + 分布式状态
    uint32_t flags_bitmap_offset = 0;
    uint32_t flags_bitmap_size = 0;
    
    // Part ID (2B) - 分布式分区 ID
    uint32_t part_rle_offset = 0;
    uint32_t part_rle_size = 0;
  } zone3;  // Key Metadata (8B: φ+κ+τ+δ+part_id)
  
  struct ZoneInfo4 {
    uint8_t encoding_type = 0;
    uint8_t compression_type = 0;
    uint16_t reserved = 0;
    uint32_t data_offset = 0;         // Value 数据偏移
    uint32_t data_size = 0;           // Value 数据大小
    uint32_t uncompressed_size = 0;   // 解压后大小
  } zone4;  // Values

  // ===== Block Info 偏移 (8 bytes) =====
  uint32_t block_info_offset = 0;   // Block Info 表偏移
  uint32_t block_count = 0;         // Block 数量

  // ===== Zone Maps 偏移 (16 bytes) =====
  uint32_t zone_maps_offset = 0;    // Zone Maps 起始偏移
  uint32_t zone_maps_size = 0;      // Zone Maps 大小
  uint32_t restart_points_offset = 0;  // 重启点偏移
  uint32_t restart_points_count = 0;   // 重启点数量

  // ===== Bloom Filter 偏移 (8 bytes) =====
  uint32_t bloom_filter_offset = 0;
  uint32_t bloom_filter_size = 0;

  // ===== Footer 偏移 (8 bytes) =====
  uint32_t footer_offset = 0;
  uint32_t reserved2 = 0;
  
  // ===== Entity Index 偏移 (8 bytes) - 持久化倒排索引 =====
  uint32_t entity_index_offset = 0;
  uint32_t entity_index_size = 0;

  // ===== 时间戳范围（用于快速过滤）(16 bytes) =====
  uint64_t min_timestamp = 0;
  uint64_t max_timestamp = 0;

  // ===== Entity ID 范围（用于快速过滤）(16 bytes) =====
  uint64_t min_entity_id = 0;
  uint64_t max_entity_id = 0;

  // ===== 保留字段 (44 bytes) =====
  uint8_t reserved[44] = {};

  static constexpr size_t kEncodedSize = 256;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(ZoneColumnarHeader) == 256, "ZoneColumnarHeader must be 256 bytes");
#pragma pack(pop)

// =============================================================================
// Zone Map 条目 (32 bytes per zone)
// =============================================================================
#pragma pack(push, 1)
struct ZoneMapEntry {
  uint64_t min_value = 0;
  uint64_t max_value = 0;
  uint64_t count = 0;
  uint64_t distinct_count = 0;

  static constexpr size_t kEncodedSize = 32;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(ZoneMapEntry) == 32, "ZoneMapEntry must be 32 bytes");
#pragma pack(pop)

// =============================================================================
// Block Info 条目 (32 bytes) - 描述每个 Block 的位置和大小
// =============================================================================
#pragma pack(push, 1)
struct BlockInfoEntry {
  uint64_t offset = 0;              // Block 在文件中的偏移量 (8 bytes)
  uint64_t size = 0;                // Block 的总大小（压缩后）(8 bytes)
  uint32_t start_row = 0;           // Block 起始行号 (4 bytes)
  uint32_t row_count = 0;           // Block 行数 (4 bytes)
  uint64_t first_entity_id = 0;     // Block 首个 entity_id (8 bytes)

  static constexpr size_t kEncodedSize = 32;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(BlockInfoEntry) == 32, "BlockInfoEntry must be 32 bytes");
#pragma pack(pop)

// =============================================================================
// Restart Point 条目 (16 bytes)
// =============================================================================
// 每 N 行记录一个，支持二分查找
#pragma pack(push, 1)
struct ZoneRestartPoint {
  uint64_t entity_id;               // 该重启点的首个 entity_id
  uint32_t timestamp_hi;            // 时间戳高 32 位
  uint32_t row_index;               // 行索引（相对于块起始）

  static constexpr size_t kEncodedSize = 16;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(ZoneRestartPoint) == 16, "ZoneRestartPoint must be 16 bytes");
#pragma pack(pop)

// =============================================================================
// Zone-Columnar Footer (128 bytes)
// =============================================================================
#pragma pack(push, 1)
struct ZoneColumnarFooter {
  // ===== 校验和 (16 bytes) =====
  uint64_t data_checksum = 0;       // 数据区 CRC64
  uint64_t header_checksum = 0;     // Header CRC64

  // ===== 统计信息 (32 bytes) =====
  uint64_t entry_count = 0;         // 总条目数
  uint64_t uncompressed_size = 0;   // 解压前总大小
  uint64_t compressed_size = 0;     // 压缩后总大小
  uint64_t index_size = 0;          // 索引总大小

  // ===== 版本链 (16 bytes) =====
  uint64_t file_number = 0;         // 当前文件号
  uint64_t prev_file_number = 0;    // 前一个文件号（版本链）

  // ===== 层级信息 (8 bytes) =====
  uint32_t level = 0;               // SST 层级
  uint32_t sequence = 0;            // 文件序列号

  // ===== 编码统计 (16 bytes) =====
  float compression_ratio = 0.0f;   // 压缩率
  uint32_t encoding_time_us = 0;    // 编码耗时（微秒）
  uint32_t reserved1 = 0;
  uint32_t reserved2 = 0;

  // ===== 保留字段 (40 bytes) =====
  uint8_t reserved[40] = {};

  static constexpr size_t kEncodedSize = 128;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};
static_assert(sizeof(ZoneColumnarFooter) == 128, "ZoneColumnarFooter must be 128 bytes");
#pragma pack(pop)

// =============================================================================

}  // namespace cedar

#endif  // FERN_ZONE_COLUMNAR_FORMAT_H_
