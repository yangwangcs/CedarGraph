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
// Zone-Columnar SST Format V2 - 生产级设计
// =============================================================================
// 针对用户的 3 个核心质疑进行重构：
// 1. 采用稀疏索引（Block-level）替代行级索引
// 2. Block 大小提升至 256KB（压缩前）
// 3. SST 文件大小目标 8MB-64MB
// =============================================================================

#ifndef FERN_ZONE_COLUMNAR_FORMAT_V2_H_
#define FERN_ZONE_COLUMNAR_FORMAT_V2_H_

#include <cstdint>
#include <vector>
#include <string>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// =============================================================================
// 常量定义
// =============================================================================
namespace sstv2 {

// Block 大小（压缩前目标）
constexpr size_t kTargetBlockSize = 256 * 1024;     // 256KB
constexpr size_t kMaxBlockSize = 1024 * 1024;       // 1MB 上限
constexpr size_t kBlockRowLimit = 16384;            // 每 Block 最多 16K 行

// SST 文件大小目标
constexpr size_t kMinSSTSize = 8 * 1024 * 1024;     // 8MB 最小
constexpr size_t kTargetSSTSize = 64 * 1024 * 1024; // 64MB 目标
constexpr size_t kMaxSSTSize = 256 * 1024 * 1024;   // 256MB 最大

// 索引稀疏度：每 N 个 Block 创建一个索引项
constexpr size_t kIndexStride = 1;  // 每个 Block 一个索引项（稀疏）

// 魔数和版本 - 与 V1 兼容
constexpr uint32_t kMagic = 0x5A434F4C;  // "ZCOL" - 小端序编码后为 0x5A434F4C
constexpr uint32_t kVersion = 1;         // V1 兼容版本

}  // namespace sstv2

// =============================================================================
// Block 级稀疏索引（替代行级索引）
// =============================================================================
struct BlockIndexEntry {
  uint64_t min_entity_id;    // Block 内最小 Entity ID
  uint64_t max_entity_id;    // Block 内最大 Entity ID
  uint64_t min_timestamp;    // Block 内最小 Timestamp
  uint64_t max_timestamp;    // Block 内最大 Timestamp
  uint32_t block_offset;     // Block 在文件中的偏移
  uint32_t block_size;       // Block 压缩后大小
  uint32_t row_count;        // Block 内行数
  
  // 编码/解码
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
  
  static constexpr size_t kEncodedSize = 48;  // 8+8+8+8+4+4+4 = 44 bytes (padding to 48)
};

// =============================================================================
// 文件级 Footer（精简版）
// =============================================================================
struct ZoneColumnarFooter {
  uint32_t block_index_offset;       // Block 索引偏移
  uint32_t block_index_size;         // Block 索引大小
  uint32_t bloom_filter_offset;      // Bloom Filter 偏移（可选）
  uint32_t bloom_filter_size;        // Bloom Filter 大小
  uint64_t row_count;                // 总行数
  uint32_t block_count;              // Block 数量
  uint32_t footer_magic;             // Footer 魔数校验
  uint32_t temporal_filter_offset;   // Temporal Bloom Filter 偏移（可选，V2+）
  uint32_t temporal_filter_size;     // Temporal Bloom Filter 大小（V2+）
  uint32_t entity_index_offset;      // Entity Hash Index 偏移（V2+, 0=不存在）
  uint32_t entity_index_size;        // Entity Hash Index 大小（V2+, 0=不存在）
  uint64_t data_checksum;            // CRC64 of all data between header and footer
  
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
  
  static constexpr size_t kEncodedSize = 64;  // 56 bytes payload + 8 bytes padding
  static constexpr uint32_t kFooterMagic = 0x464F4F54;  // "FOOT"
};

// =============================================================================
// Header（精简版）
// =============================================================================
struct ZoneColumnarHeader {
  uint32_t magic = sstv2::kMagic;
  uint32_t version = sstv2::kVersion;
  uint64_t file_size = 0;           // 文件总大小
  uint64_t min_entity_id = 0;       // 全局最小 Entity ID
  uint64_t max_entity_id = 0;       // 全局最大 Entity ID
  uint64_t min_timestamp = 0;       // 全局最小 Timestamp
  uint64_t max_timestamp = 0;       // 全局最大 Timestamp
  uint32_t column_id = 0;           // Column ID
  uint8_t entity_type = 0;          // Entity Type
  uint8_t reserved[3] = {0};        // Reserved
  uint32_t header_checksum = 0;     // CRC32C of header (excluding this field)
  uint32_t padding = 0;             // Padding to 64 bytes
  
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
  
  static constexpr size_t kEncodedSize = 64;
};

// =============================================================================
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

// SST 统计信息
// =============================================================================
struct SSTStats {
  size_t file_size = 0;
  size_t block_count = 0;
  size_t row_count = 0;
  size_t index_size = 0;      // Block 索引大小
  size_t data_size = 0;       // 纯数据大小（不含索引）
  double compression_ratio = 0.0;
  
  double IndexOverhead() const {
    return file_size > 0 ? static_cast<double>(index_size) / file_size : 0.0;
  }
  
  double AvgRowSize() const {
    return row_count > 0 ? static_cast<double>(file_size) / row_count : 0.0;
  }
  
  double AvgBlockSize() const {
    return block_count > 0 ? static_cast<double>(data_size) / block_count : 0.0;
  }
};

}  // namespace cedar

#endif  // FERN_ZONE_COLUMNAR_FORMAT_V2_H_
