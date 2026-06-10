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
// Zone-Columnar SST Builder V2 - 声明
// =============================================================================

#ifndef FERN_ZONE_COLUMNAR_BUILDER_V2_H_
#define FERN_ZONE_COLUMNAR_BUILDER_V2_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/env.h"
#include "cedar/core/status.h"
#include "cedar/sst/zone_columnar_format_v2.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// =============================================================================
// Row Buffer（批量收集行数据）
// =============================================================================
class RowBuffer {
 public:
  void Clear();
  void Reserve(size_t n);
  void Add(const CedarKey& key, const Descriptor& desc, Timestamp txn_version);
  bool Empty() const;
  size_t Size() const;

  std::vector<uint64_t> entity_ids;
  std::vector<uint64_t> timestamps;
  std::vector<uint64_t> target_ids;
  std::vector<uint16_t> column_ids;
  std::vector<uint16_t> sequences;
  std::vector<uint8_t> entity_types;
  std::vector<uint8_t> flags;
  std::vector<uint16_t> part_ids;
  std::vector<Descriptor> values;
  std::vector<uint64_t> txn_versions;
};

// =============================================================================
// ZoneColumnarSstBuilderV2
// =============================================================================
class ZoneColumnarSstBuilderV2 {
 public:
  struct Options {
    size_t target_block_size = sstv2::kTargetBlockSize;  // 256KB
    size_t target_sst_size = sstv2::kTargetSSTSize;      // 64MB
    size_t block_row_limit = sstv2::kBlockRowLimit;      // 16K 行
    bool enable_compression = true;
  };

  explicit ZoneColumnarSstBuilderV2(const Options& options, WritableFile* file);

  // 添加条目（要求：按 CedarKey 排序）
  void Add(const CedarKey& key, const Descriptor& desc, Timestamp txn_version);

  // 完成构建
  Status Finish();

  // 获取统计
  SSTStats GetStats() const;

  uint64_t FileSize() const;
  uint64_t NumEntries() const;
  const std::string& GetTemporalFilterData() const;

 private:
  // Block 数据结构
  struct Block {
    std::string zone0_data;  // Entity IDs
    std::string zone1_data;  // Timestamps
    std::string zone2_data;  // Target IDs
    std::string zone3_data;  // Metadata
    std::string zone4_data;  // Values
    std::string zone5_data;  // Txn Versions (MVCC)

    uint64_t min_entity_id = 0;
    uint64_t max_entity_id = 0;
    uint64_t min_timestamp = 0;
    uint64_t max_timestamp = 0;
    uint32_t row_count = 0;
    uint32_t compressed_size = 0;
  };

  // 判断是否应该切割 Block
  bool ShouldCutBlock() const;

  // 刷新 Block
  Status FlushBlock();

  // 写入完整文件
  Status WriteFile();

  Options options_;
  WritableFile* file_;
  Status status_;
  bool closed_ = false;

  RowBuffer buffer_;
  std::vector<Block> blocks_;
  std::vector<BlockIndexEntry> block_index_;

  std::vector<CedarKey> all_keys_;
  std::string temporal_filter_data_;

  uint64_t total_rows_ = 0;
  uint64_t min_entity_id_ = 0;
  uint64_t max_entity_id_ = 0;
  uint64_t min_timestamp_ = 0;
  uint64_t max_timestamp_ = 0;
  uint64_t file_size_ = 0;
};

}  // namespace cedar

#endif  // FERN_ZONE_COLUMNAR_BUILDER_V2_H_
