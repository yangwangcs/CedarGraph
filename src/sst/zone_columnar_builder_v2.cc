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

#include "cedar/sst/zone_columnar_builder_v2.h"

#include "cedar/core/crc32c.h"
#include "cedar/core/env.h"
#include "cedar/sst/bloom_filter.h"
#include "cedar/sst/zone_encoder.h"
#include "cedar/sst/compression.h"
#include "cedar/storage/sst_temporal_filter.h"
#include "cedar/common/logging.h"

#include <algorithm>
#include <cstring>
#include <unordered_set>

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
// RowBuffer 实现
// =============================================================================
void RowBuffer::Clear() {
  entity_ids.clear();
  timestamps.clear();
  target_ids.clear();
  column_ids.clear();
  sequences.clear();
  entity_types.clear();
  flags.clear();
  part_ids.clear();
  txn_versions.clear();
  values.clear();
}

void RowBuffer::Reserve(size_t n) {
  entity_ids.reserve(n);
  timestamps.reserve(n);
  target_ids.reserve(n);
  column_ids.reserve(n);
  sequences.reserve(n);
  txn_versions.reserve(n);
  entity_types.reserve(n);
  flags.reserve(n);
  part_ids.reserve(n);
  values.reserve(n);
}

void RowBuffer::Add(const CedarKey& key, const Descriptor& desc, Timestamp txn_version) {
  entity_ids.push_back(key.entity_id());
  timestamps.push_back(key.timestamp().value());
  target_ids.push_back(key.target_id());
  column_ids.push_back(key.column_id());
  sequences.push_back(key.sequence());
  entity_types.push_back(static_cast<uint8_t>(key.entity_type()));
  flags.push_back(key.flags());
  part_ids.push_back(key.part_id());
  txn_versions.push_back(txn_version.value());
  values.push_back(desc);
}

bool RowBuffer::Empty() const { return entity_ids.empty(); }
size_t RowBuffer::Size() const { return entity_ids.size(); }

// =============================================================================
// ZoneColumnarSstBuilder 实现
// =============================================================================
ZoneColumnarSstBuilder::ZoneColumnarSstBuilder(const Options& options,
                                                    WritableFile* file)
    : options_(options), file_(file) {
  buffer_.Reserve(options.block_row_limit);
}

void ZoneColumnarSstBuilder::Add(const CedarKey& key, const Descriptor& desc,
                                        Timestamp txn_version) {
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
  buffer_.Add(key, desc, txn_version);
  total_rows_++;

  // 收集 key 用于构建 TemporalBloomFilter
  all_keys_.push_back(key);
  
  // 跟踪 entity_id → block 映射 (当前 block 还在 buffer 中，索引 = blocks_.size())
  entity_blocks_[key.entity_id()].push_back(static_cast<uint32_t>(blocks_.size()));
  
  // 检查是否需要切割 Block
  if (ShouldCutBlock()) {
    status_ = FlushBlock();
  }
}

Status ZoneColumnarSstBuilder::Finish() {
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

SSTStats ZoneColumnarSstBuilder::GetStats() const {
  SSTStats stats;
  stats.block_count = block_index_.size();
  stats.row_count = total_rows_;
  stats.index_size = block_index_.size() * BlockIndexEntry::kEncodedSize;
  for (const auto& block : blocks_) {
    stats.data_size += block.compressed_size;
  }
  stats.file_size = stats.data_size + stats.index_size +
                    ZoneColumnarHeader::kEncodedSize +
                    ZoneColumnarFooter::kEncodedSize;
  if (total_rows_ > 0) {
    stats.compression_ratio = static_cast<double>(stats.file_size) /
                              (total_rows_ * 40);  // 假设原始 40B/行
  }
  return stats;
}

uint64_t ZoneColumnarSstBuilder::FileSize() const { return file_size_; }
uint64_t ZoneColumnarSstBuilder::NumEntries() const { return total_rows_; }
const std::string& ZoneColumnarSstBuilder::GetTemporalFilterData() const {
  return temporal_filter_data_;
}

bool ZoneColumnarSstBuilder::ShouldCutBlock() const {
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

Status ZoneColumnarSstBuilder::FlushBlock() {
  if (buffer_.Empty()) return Status::OK();

  Block block;
  block.row_count = static_cast<uint32_t>(buffer_.Size());
  block.min_entity_id = *std::min_element(buffer_.entity_ids.begin(),
                                          buffer_.entity_ids.end());
  block.max_entity_id = *std::max_element(buffer_.entity_ids.begin(),
                                          buffer_.entity_ids.end());
  block.min_timestamp = *std::min_element(buffer_.timestamps.begin(),
                                          buffer_.timestamps.end());
  block.max_timestamp = *std::max_element(buffer_.timestamps.begin(),
                                          buffer_.timestamps.end());

  // 编码 6 个 Zone - 直接存储原始数据
  block.zone0_data.resize(buffer_.entity_ids.size() * 8);
  memcpy(&block.zone0_data[0], buffer_.entity_ids.data(),
         block.zone0_data.size());
  block.zone1_data.resize(buffer_.timestamps.size() * 8);
  memcpy(&block.zone1_data[0], buffer_.timestamps.data(),
         block.zone1_data.size());
  block.zone2_data.resize(buffer_.target_ids.size() * 8);
  memcpy(&block.zone2_data[0], buffer_.target_ids.data(),
         block.zone2_data.size());

  // Zone 3: Metadata (raw 8 bytes per row)
  block.zone3_data.reserve(buffer_.Size() * 8);
  for (size_t i = 0; i < buffer_.Size(); i++) {
    block.zone3_data.append(
        reinterpret_cast<const char*>(&buffer_.column_ids[i]), 2);
    block.zone3_data.append(
        reinterpret_cast<const char*>(&buffer_.sequences[i]), 2);
    block.zone3_data.push_back(buffer_.entity_types[i]);
    block.zone3_data.push_back(buffer_.flags[i]);
    block.zone3_data.append(
        reinterpret_cast<const char*>(&buffer_.part_ids[i]), 2);
  }

  // Zone 4: Values（Descriptor 是 64-bit 值，直接存储）
  std::string value_data;
  value_data.reserve(buffer_.values.size() * 8);
  for (const auto& desc : buffer_.values) {
    uint64_t raw = desc.AsRaw();
    value_data.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
  }
  block.zone4_data = std::move(value_data);

  // Zone 5: Txn Versions (MVCC)
  block.zone5_data.resize(buffer_.txn_versions.size() * 8);
  memcpy(&block.zone5_data[0], buffer_.txn_versions.data(),
         block.zone5_data.size());

  // 分级压缩: L0=不压缩, L1-2=LZ4, L3+=Zstd
  if (options_.enable_compression && options_.output_level > 0) {
    CedarCompressionType compress_type = CedarCompressionType::LZ4;
    if (options_.output_level >= 3 && Compression::IsSupported(CedarCompressionType::Zstd)) {
      compress_type = CedarCompressionType::Zstd;
    }
    
    std::string* zones[] = {&block.zone0_data, &block.zone1_data, &block.zone2_data,
                            &block.zone3_data, &block.zone4_data, &block.zone5_data};
    for (int i = 0; i < 6; ++i) {
      if (zones[i]->size() >= 64) {  // 小数据不压缩
        std::string compressed;
        CedarCompressionType actual_type;
        Status s = Compression::Compress(compress_type, Slice(*zones[i]),
                                        &compressed, &actual_type);
        if (s.ok() && actual_type != CedarCompressionType::None && 
            compressed.size() < zones[i]->size()) {
          *zones[i] = std::move(compressed);
          block.compression_types[i] = static_cast<uint8_t>(actual_type);
        }
      }
    }
  }

  // 计算压缩后大小 (Block Header 50 bytes + 6 zones)
  block.compressed_size = static_cast<uint32_t>(
      50 +  // BlockHeader (50 bytes: 44 + 6 compression types)
      block.zone0_data.size() + block.zone1_data.size() +
      block.zone2_data.size() + block.zone3_data.size() +
      block.zone4_data.size() + block.zone5_data.size());

  blocks_.push_back(std::move(block));

  // 清空 buffer
  buffer_.Clear();
  buffer_.Reserve(options_.block_row_limit);

  return Status::OK();
}

Status ZoneColumnarSstBuilder::WriteFile() {
  std::string file_data;
  file_data.reserve(options_.target_sst_size);

  // 1. Header（占位，稍后填充）
  size_t header_offset = file_data.size();
  file_data.append(ZoneColumnarHeader::kEncodedSize, '\0');

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

    // 写入 Block Header（50 bytes = 44 + 6 compression types）
    char bh_data[50];
    EncodeFixed32(bh_data, block.row_count);
    EncodeFixed32(bh_data + 4, static_cast<uint32_t>(block.zone0_data.size()));
    EncodeFixed32(bh_data + 8, static_cast<uint32_t>(block.zone1_data.size()));
    EncodeFixed32(bh_data + 12, static_cast<uint32_t>(block.zone2_data.size()));
    EncodeFixed32(bh_data + 16, static_cast<uint32_t>(block.zone3_data.size()));
    EncodeFixed32(bh_data + 20, static_cast<uint32_t>(block.zone4_data.size()));
    EncodeFixed32(bh_data + 24, static_cast<uint32_t>(block.zone5_data.size()));
    EncodeFixed64(bh_data + 28, block.min_entity_id);
    EncodeFixed64(bh_data + 36, block.max_entity_id);
    // Per-zone compression type (6 bytes)
    for (int i = 0; i < 6; ++i) {
      bh_data[44 + i] = static_cast<char>(block.compression_types[i]);
    }
    file_data.append(bh_data, 50);

    // 写入 6 个 Zone
    file_data.append(block.zone0_data);
    file_data.append(block.zone1_data);
    file_data.append(block.zone2_data);
    file_data.append(block.zone3_data);
    file_data.append(block.zone4_data);
    file_data.append(block.zone5_data);
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

  // 3.6 Entity-level Bloom Filter（用于快速跳过不包含目标 entity 的 SST 文件）
  size_t bloom_filter_offset = 0;
  size_t bloom_filter_size = 0;
  if (!all_keys_.empty()) {
    // 10 bits per key ≈ 1% false positive rate
    BloomFilter bf(10, all_keys_.size());
    // Use set to deduplicate entity_ids (same entity may have multiple versions)
    std::unordered_set<uint64_t> seen_entities;
    for (const auto& key : all_keys_) {
      uint64_t eid = key.entity_id();
      if (seen_entities.insert(eid).second) {
        bf.Add(eid);
      }
    }
    auto bf_data = bf.Finish();
    bloom_filter_data_ = std::string(bf_data.begin(), bf_data.end());
    if (!bloom_filter_data_.empty()) {
      bloom_filter_offset = file_data.size();
      file_data.append(bloom_filter_data_);
      bloom_filter_size = bloom_filter_data_.size();
    }
  }

  // 3.7 Entity Hash Index (entity_id → block_ids, O(1) lookup)
  size_t entity_index_offset = 0;
  size_t entity_index_size = 0;
  if (!entity_blocks_.empty()) {
    // Deduplicate block_ids per entity (same entity may appear in same block multiple times)
    for (auto& [eid, block_ids] : entity_blocks_) {
      std::sort(block_ids.begin(), block_ids.end());
      block_ids.erase(std::unique(block_ids.begin(), block_ids.end()), block_ids.end());
    }
    // Serialize: [entry_count:4] [(entity_id:8, block_count:2, block_ids:2*block_count) ...]
    std::string idx_data;
    uint32_t entry_count = static_cast<uint32_t>(entity_blocks_.size());
    idx_data.append(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));
    for (const auto& [eid, block_ids] : entity_blocks_) {
      idx_data.append(reinterpret_cast<const char*>(&eid), sizeof(eid));
      uint16_t bc = static_cast<uint16_t>(block_ids.size());
      idx_data.append(reinterpret_cast<const char*>(&bc), sizeof(bc));
      for (uint32_t bid : block_ids) {
        uint16_t bid16 = static_cast<uint16_t>(bid);
        idx_data.append(reinterpret_cast<const char*>(&bid16), sizeof(bid16));
      }
    }
    entity_index_data_ = std::move(idx_data);
    entity_index_offset = file_data.size();
    file_data.append(entity_index_data_);
    entity_index_size = entity_index_data_.size();
  }

  // 4. Footer
  ZoneColumnarFooter footer;
  footer.block_index_offset = static_cast<uint32_t>(index_offset);
  footer.block_index_size = static_cast<uint32_t>(index_size);
  footer.bloom_filter_offset = static_cast<uint32_t>(bloom_filter_offset);
  footer.bloom_filter_size = static_cast<uint32_t>(bloom_filter_size);
  footer.row_count = total_rows_;
  footer.block_count = static_cast<uint32_t>(blocks_.size());
  footer.footer_magic = ZoneColumnarFooter::kFooterMagic;
  footer.temporal_filter_offset = static_cast<uint32_t>(temporal_filter_offset);
  footer.temporal_filter_size = static_cast<uint32_t>(temporal_filter_size);
  footer.entity_index_offset = static_cast<uint32_t>(entity_index_offset);
  footer.entity_index_size = static_cast<uint32_t>(entity_index_size);

  // 计算 data_checksum: header 之后到 footer 之前的所有数据
  size_t data_start = ZoneColumnarHeader::kEncodedSize;
  size_t data_end = file_data.size();
  size_t data_len = data_end - data_start;
  footer.data_checksum = ComputeCRC64(file_data.data() + data_start, data_len);

  std::string footer_data;
  footer.EncodeTo(&footer_data);
  file_data.append(footer_data);

  // 5. 编码 Header（含 CRC32C checksum）
  ZoneColumnarHeader header;
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

}  // namespace cedar
