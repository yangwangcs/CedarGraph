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
// Zone-Columnar SST Builder V2 - 生产级实现 (Checksum 版)
// =============================================================================
// 核心特性：
// 1. 稀疏索引（Block-level）替代行级索引
// 2. 256KB Block 大小
// 3. SST 大小控制 8MB-64MB
// 4. Header CRC32C checksum
// 5. Data CRC64 checksum (footer)
// =============================================================================

#include "cedar/sst/zone_columnar_format_v2.h"
#include "cedar/sst/zone_encoder.h"
#include "cedar/storage/sst_temporal_filter.h"
#include "cedar/core/env.h"
#include "cedar/core/crc32c.h"

#include <algorithm>
#include <cstring>
#include <vector>
#include <memory>

// =============================================================================
// 编码/解码辅助函数 + CRC64 helper
// =============================================================================
namespace {

static void EncodeFixed16(char* buf, uint16_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
}

static void EncodeFixed32(char* buf, uint32_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
}

static void EncodeFixed64(char* buf, uint64_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  buf[4] = (value >> 32) & 0xff;
  buf[5] = (value >> 40) & 0xff;
  buf[6] = (value >> 48) & 0xff;
  buf[7] = (value >> 56) & 0xff;
}

static uint16_t DecodeFixed16(const char* ptr) {
  return static_cast<uint16_t>(static_cast<unsigned char>(ptr[0]))
       | (static_cast<uint16_t>(static_cast<unsigned char>(ptr[1])) << 8);
}

static uint32_t DecodeFixed32(const char* ptr) {
  return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
}

static uint64_t DecodeFixed64(const char* ptr) {
  uint64_t lo = DecodeFixed32(ptr);
  uint64_t hi = DecodeFixed32(ptr + 4);
  return (hi << 32) | lo;
}

static void PutFixed32(std::string* dst, uint32_t value) {
  char buf[4];
  EncodeFixed32(buf, value);
  dst->append(buf, 4);
}

static void PutFixed64(std::string* dst, uint64_t value) {
  char buf[8];
  EncodeFixed64(buf, value);
  dst->append(buf, 8);
}

// CRC64: combine two CRC32C with different seeds for 64-bit integrity check
static uint64_t ComputeCRC64(const char* data, size_t n) {
  uint32_t crc_lo = cedar::crc32c::Value(data, n);
  uint32_t crc_hi = cedar::crc32c::Extend(0xa5a5a5a5u, data, n);
  return (static_cast<uint64_t>(crc_hi) << 32) | static_cast<uint64_t>(crc_lo);
}

}  // namespace

namespace cedar {

// =============================================================================
// Row Buffer（批量收集行数据）
// =============================================================================
class RowBuffer {
 public:
  void Clear() {
    entity_ids.clear();
    timestamps.clear();
    target_ids.clear();
    column_ids.clear();
    sequences.clear();
    entity_types.clear();
    flags.clear();
    part_ids.clear();
    values.clear();
  }

  void Reserve(size_t n) {
    entity_ids.reserve(n);
    timestamps.reserve(n);
    target_ids.reserve(n);
    column_ids.reserve(n);
    sequences.reserve(n);
    entity_types.reserve(n);
    flags.reserve(n);
    part_ids.reserve(n);
    values.reserve(n);
  }

  void Add(const CedarKey& key, const Descriptor& desc) {
    entity_ids.push_back(key.entity_id());
    timestamps.push_back(key.timestamp().value());
    target_ids.push_back(key.target_id());
    column_ids.push_back(key.column_id());
    sequences.push_back(key.sequence());
    entity_types.push_back(static_cast<uint8_t>(key.entity_type()));
    flags.push_back(key.flags());
    part_ids.push_back(key.part_id());
    values.push_back(desc);
  }

  bool Empty() const { return entity_ids.empty(); }
  size_t Size() const { return entity_ids.size(); }

  std::vector<uint64_t> entity_ids;
  std::vector<uint64_t> timestamps;
  std::vector<uint64_t> target_ids;
  std::vector<uint16_t> column_ids;
  std::vector<uint16_t> sequences;
  std::vector<uint8_t> entity_types;
  std::vector<uint8_t> flags;
  std::vector<uint16_t> part_ids;
  std::vector<Descriptor> values;
};

// =============================================================================
// ZoneColumnarSstBuilderV2 实现
// =============================================================================
class ZoneColumnarSstBuilderV2 {
 public:
  struct Options {
    size_t target_block_size = sstv2::kTargetBlockSize;  // 256KB
    size_t target_sst_size = sstv2::kTargetSSTSize;      // 64MB
    size_t block_row_limit = sstv2::kBlockRowLimit;      // 16K 行
    bool enable_compression = true;
  };

  explicit ZoneColumnarSstBuilderV2(const Options& options, WritableFile* file)
      : options_(options), file_(file) {
    buffer_.Reserve(options.block_row_limit);
  }

  // 添加条目（要求：按 CedarKey 排序）
  void Add(const CedarKey& key, const Descriptor& desc) {
    if (!status_.ok()) return;

    // 更新全局统计
    if (total_rows_ == 0) {
      min_entity_id_ = max_entity_id_ = key.entity_id();
      min_timestamp_ = max_timestamp_ = key.timestamp().value();
    } else {
      min_entity_id_ = std::min(min_entity_id_, key.entity_id());
      max_entity_id_ = std::max(max_entity_id_, key.entity_id());
      min_timestamp_ = std::min(min_timestamp_, key.timestamp().value());
      max_timestamp_ = std::max(max_timestamp_, key.timestamp().value());
    }

    // 添加到 buffer
    buffer_.Add(key, desc);
    total_rows_++;

    // 收集 key 用于构建 TemporalBloomFilter
    all_keys_.push_back(key);

    // 检查是否需要切割 Block
    if (ShouldCutBlock()) {
      status_ = FlushBlock();
    }
  }

  // 完成构建
  Status Finish() {
    if (!status_.ok()) return status_;
    if (closed_) return Status::OK();

    // 刷新最后的 block
    if (!buffer_.Empty()) {
      status_ = FlushBlock();
      if (!status_.ok()) return status_;
    }

    // 构建文件
    return WriteFile();
  }

  // 获取统计
  SSTStats GetStats() const {
    SSTStats stats;
    stats.block_count = block_index_.size();
    stats.row_count = total_rows_;
    stats.index_size = block_index_.size() * BlockIndexEntry::kEncodedSize;
    for (const auto& block : blocks_) {
      stats.data_size += block.compressed_size;
    }
    stats.file_size = stats.data_size + stats.index_size +
                      ZoneColumnarHeaderV2::kEncodedSize +
                      ZoneColumnarFooterV2::kEncodedSize;
    if (total_rows_ > 0) {
      stats.compression_ratio = static_cast<double>(stats.file_size) /
                                (total_rows_ * 40);  // 假设原始 40B/行
    }
    return stats;
  }

  uint64_t FileSize() const { return file_size_; }
  uint64_t NumEntries() const { return total_rows_; }
  const std::string& GetTemporalFilterData() const { return temporal_filter_data_; }

 private:
  // Block 数据结构
  struct Block {
    std::string zone0_data;  // Entity IDs
    std::string zone1_data;  // Timestamps
    std::string zone2_data;  // Target IDs
    std::string zone3_data;  // Metadata
    std::string zone4_data;  // Values

    uint64_t min_entity_id = 0;
    uint64_t max_entity_id = 0;
    uint64_t min_timestamp = 0;
    uint64_t max_timestamp = 0;
    uint32_t row_count = 0;
    uint32_t compressed_size = 0;
  };

  // 判断是否应该切割 Block
  bool ShouldCutBlock() const {
    if (buffer_.Empty()) return false;

    // 条件 1: 行数超过限制
    if (buffer_.Size() >= options_.block_row_limit) {
      return true;
    }

    // 条件 2: 估算大小超过目标 Block 大小
    size_t estimated_size = buffer_.Size() * 40;
    if (estimated_size >= options_.target_block_size) {
      return true;
    }

    return false;
  }

  // 刷新 Block
  Status FlushBlock() {
    if (buffer_.Empty()) return Status::OK();

    Block block;
    block.row_count = static_cast<uint32_t>(buffer_.Size());
    block.min_entity_id = *std::min_element(buffer_.entity_ids.begin(), buffer_.entity_ids.end());
    block.max_entity_id = *std::max_element(buffer_.entity_ids.begin(), buffer_.entity_ids.end());
    block.min_timestamp = *std::min_element(buffer_.timestamps.begin(), buffer_.timestamps.end());
    block.max_timestamp = *std::max_element(buffer_.timestamps.begin(), buffer_.timestamps.end());

    // 编码 5 个 Zone - 直接存储原始数据
    block.zone0_data.resize(buffer_.entity_ids.size() * 8);
    memcpy(&block.zone0_data[0], buffer_.entity_ids.data(), block.zone0_data.size());
    block.zone1_data.resize(buffer_.timestamps.size() * 8);
    memcpy(&block.zone1_data[0], buffer_.timestamps.data(), block.zone1_data.size());
    block.zone2_data.resize(buffer_.target_ids.size() * 8);
    memcpy(&block.zone2_data[0], buffer_.target_ids.data(), block.zone2_data.size());

    // Zone 3: Metadata (raw 8 bytes per row)
    block.zone3_data.reserve(buffer_.Size() * 8);
    for (size_t i = 0; i < buffer_.Size(); i++) {
      block.zone3_data.append(reinterpret_cast<const char*>(&buffer_.column_ids[i]), 2);
      block.zone3_data.append(reinterpret_cast<const char*>(&buffer_.sequences[i]), 2);
      block.zone3_data.push_back(buffer_.entity_types[i]);
      block.zone3_data.push_back(buffer_.flags[i]);
      block.zone3_data.append(reinterpret_cast<const char*>(&buffer_.part_ids[i]), 2);
    }

    // Zone 4: Values（Descriptor 是 64-bit 值，直接存储）
    std::string value_data;
    value_data.reserve(buffer_.values.size() * 8);
    for (const auto& desc : buffer_.values) {
      uint64_t raw = desc.AsRaw();
      value_data.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
    }
    block.zone4_data = std::move(value_data);

    // 计算压缩后大小
    block.compressed_size = static_cast<uint32_t>(
        40 +  // BlockHeader
        block.zone0_data.size() +
        block.zone1_data.size() +
        block.zone2_data.size() +
        block.zone3_data.size() +
        block.zone4_data.size()
    );

    blocks_.push_back(std::move(block));

    // 清空 buffer
    buffer_.Clear();
    buffer_.Reserve(options_.block_row_limit);

    return Status::OK();
  }

  // 写入完整文件
  Status WriteFile() {
    std::string file_data;
    file_data.reserve(options_.target_sst_size);

    // 1. Header（占位，稍后填充）
    size_t header_offset = file_data.size();
    file_data.append(ZoneColumnarHeaderV2::kEncodedSize, '\0');

    // 2. 写入所有 Blocks
    block_index_.clear();
    for (auto& block : blocks_) {
      BlockIndexEntry entry;
      entry.block_offset = static_cast<uint32_t>(file_data.size());
      entry.block_size = block.compressed_size;
      entry.row_count = block.row_count;
      entry.min_entity_id = block.min_entity_id;
      entry.max_entity_id = block.max_entity_id;
      entry.min_timestamp = block.min_timestamp;
      entry.max_timestamp = block.max_timestamp;
      block_index_.push_back(entry);

      // 写入 Block Header（40 bytes）
      char bh_data[40];
      EncodeFixed32(bh_data, block.row_count);
      EncodeFixed32(bh_data + 4, static_cast<uint32_t>(block.zone0_data.size()));
      EncodeFixed32(bh_data + 8, static_cast<uint32_t>(block.zone1_data.size()));
      EncodeFixed32(bh_data + 12, static_cast<uint32_t>(block.zone2_data.size()));
      EncodeFixed32(bh_data + 16, static_cast<uint32_t>(block.zone3_data.size()));
      EncodeFixed32(bh_data + 20, static_cast<uint32_t>(block.zone4_data.size()));
      EncodeFixed64(bh_data + 24, block.min_entity_id);
      EncodeFixed64(bh_data + 32, block.max_entity_id);
      file_data.append(bh_data, 40);

      // 写入 5 个 Zone
      file_data.append(block.zone0_data);
      file_data.append(block.zone1_data);
      file_data.append(block.zone2_data);
      file_data.append(block.zone3_data);
      file_data.append(block.zone4_data);
    }

    // 3. Block 索引
    size_t index_offset = file_data.size();
    for (const auto& entry : block_index_) {
      std::string encoded;
      entry.EncodeTo(&encoded);
      file_data.append(encoded);
    }
    size_t index_size = file_data.size() - index_offset;

    // 3.5 Temporal Bloom Filter（如果存在数据）
    size_t temporal_filter_offset = 0;
    size_t temporal_filter_size = 0;
    if (!all_keys_.empty()) {
      auto tbf = SSTTemporalBloomFilter::CreateForSST(
          Timestamp(min_timestamp_), Timestamp(max_timestamp_), all_keys_);
      if (tbf) {
        temporal_filter_data_ = tbf->Serialize();
        temporal_filter_offset = file_data.size();
        file_data.append(temporal_filter_data_);
        temporal_filter_size = temporal_filter_data_.size();
      }
    }

    // 4. Footer
    ZoneColumnarFooterV2 footer;
    footer.block_index_offset = static_cast<uint32_t>(index_offset);
    footer.block_index_size = static_cast<uint32_t>(index_size);
    footer.bloom_filter_offset = 0;
    footer.bloom_filter_size = 0;
    footer.row_count = total_rows_;
    footer.block_count = static_cast<uint32_t>(blocks_.size());
    footer.footer_magic = ZoneColumnarFooterV2::kFooterMagic;
    footer.temporal_filter_offset = static_cast<uint32_t>(temporal_filter_offset);
    footer.temporal_filter_size = static_cast<uint32_t>(temporal_filter_size);
    footer.reserved = 0;

    // 计算 data_checksum: header 之后到 footer 之前的所有数据
    size_t data_start = ZoneColumnarHeaderV2::kEncodedSize;
    size_t data_end = file_data.size();
    size_t data_len = data_end - data_start;
    footer.data_checksum = ComputeCRC64(file_data.data() + data_start, data_len);

    std::string footer_data;
    footer.EncodeTo(&footer_data);
    file_data.append(footer_data);

    // 5. 编码 Header（含 CRC32C checksum）
    ZoneColumnarHeaderV2 header;
    header.file_size = file_data.size();
    header.min_entity_id = min_entity_id_;
    header.max_entity_id = max_entity_id_;
    header.min_timestamp = min_timestamp_;
    header.max_timestamp = max_timestamp_;
    header.column_id = UINT16_MAX;
    header.entity_type = 0;

    std::string header_data;
    header.EncodeTo(&header_data);
    memcpy(&file_data[header_offset], header_data.data(), header_data.size());

    // 6. 写入文件
    status_ = file_->Append(file_data);
    if (!status_.ok()) return status_;

    status_ = file_->Sync();
    if (!status_.ok()) return status_;

    file_size_ = file_data.size();
    closed_ = true;

    return Status::OK();
  }

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

// =============================================================================
// BlockIndexEntry 实现
// =============================================================================
void BlockIndexEntry::EncodeTo(std::string* dst) const {
  char buf[48];
  EncodeFixed64(buf, min_entity_id);
  EncodeFixed64(buf + 8, max_entity_id);
  EncodeFixed64(buf + 16, min_timestamp);
  EncodeFixed64(buf + 24, max_timestamp);
  EncodeFixed32(buf + 32, block_offset);
  EncodeFixed32(buf + 36, block_size);
  EncodeFixed32(buf + 40, row_count);
  // padding to 48 bytes
  memset(buf + 44, 0, 4);
  dst->append(buf, 48);
}

Status BlockIndexEntry::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("BlockIndexEntry too small");
  }
  const char* p = input->data();
  min_entity_id = DecodeFixed64(p);
  max_entity_id = DecodeFixed64(p + 8);
  min_timestamp = DecodeFixed64(p + 16);
  max_timestamp = DecodeFixed64(p + 24);
  block_offset = DecodeFixed32(p + 32);
  block_size = DecodeFixed32(p + 36);
  row_count = DecodeFixed32(p + 40);
  *input = Slice(input->data() + kEncodedSize, input->size() - kEncodedSize);
  return Status::OK();
}

// =============================================================================
// Header/Footer 编码/解码
// =============================================================================
void ZoneColumnarHeaderV2::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  memset(buf, 0, kEncodedSize);

  EncodeFixed32(buf, magic);
  EncodeFixed32(buf + 4, version);
  EncodeFixed64(buf + 8, file_size);
  EncodeFixed64(buf + 16, min_entity_id);
  EncodeFixed64(buf + 24, max_entity_id);
  EncodeFixed64(buf + 32, min_timestamp);
  EncodeFixed64(buf + 40, max_timestamp);
  EncodeFixed32(buf + 48, column_id);
  buf[52] = entity_type;
  // reserved[3] at 53-55, already zero
  // header_checksum at 56 - computed below
  // padding at 60-63, already zero

  // Compute CRC32C of first 56 bytes (excluding checksum + padding)
  uint32_t checksum = crc32c::Value(buf, 56);
  EncodeFixed32(buf + 56, checksum);

  dst->append(buf, kEncodedSize);
}

Status ZoneColumnarHeaderV2::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("Header too small");
  }
  const char* p = input->data();
  magic = DecodeFixed32(p);
  version = DecodeFixed32(p + 4);
  file_size = DecodeFixed64(p + 8);
  min_entity_id = DecodeFixed64(p + 16);
  max_entity_id = DecodeFixed64(p + 24);
  min_timestamp = DecodeFixed64(p + 32);
  max_timestamp = DecodeFixed64(p + 40);
  column_id = DecodeFixed32(p + 48);
  entity_type = static_cast<uint8_t>(p[52]);
  uint32_t stored_checksum = DecodeFixed32(p + 56);

  // Verify CRC32C of first 56 bytes
  uint32_t computed_checksum = crc32c::Value(p, 56);
  if (computed_checksum != stored_checksum) {
    return Status::Corruption("SST header checksum mismatch");
  }
  header_checksum = stored_checksum;

  *input = Slice(input->data() + kEncodedSize, input->size() - kEncodedSize);
  return Status::OK();
}

void ZoneColumnarFooterV2::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  memset(buf, 0, kEncodedSize);

  EncodeFixed32(buf, block_index_offset);
  EncodeFixed32(buf + 4, block_index_size);
  EncodeFixed32(buf + 8, bloom_filter_offset);
  EncodeFixed32(buf + 12, bloom_filter_size);
  EncodeFixed64(buf + 16, row_count);
  EncodeFixed32(buf + 24, block_count);
  EncodeFixed32(buf + 28, footer_magic);
  EncodeFixed32(buf + 32, temporal_filter_offset);
  EncodeFixed32(buf + 36, temporal_filter_size);
  EncodeFixed32(buf + 40, reserved);
  EncodeFixed64(buf + 44, data_checksum);
  // padding 52-63 already zeroed by memset

  dst->append(buf, kEncodedSize);
}

Status ZoneColumnarFooterV2::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("Footer too small");
  }
  const char* p = input->data();
  block_index_offset = DecodeFixed32(p);
  block_index_size = DecodeFixed32(p + 4);
  bloom_filter_offset = DecodeFixed32(p + 8);
  bloom_filter_size = DecodeFixed32(p + 12);
  row_count = DecodeFixed64(p + 16);
  block_count = DecodeFixed32(p + 24);
  footer_magic = DecodeFixed32(p + 28);
  temporal_filter_offset = DecodeFixed32(p + 32);
  temporal_filter_size = DecodeFixed32(p + 36);
  reserved = DecodeFixed32(p + 40);
  data_checksum = DecodeFixed64(p + 44);

  if (footer_magic != kFooterMagic) {
    return Status::Corruption("Invalid SST footer magic");
  }

  *input = Slice(input->data() + kEncodedSize, input->size() - kEncodedSize);
  return Status::OK();
}

}  // namespace cedar
