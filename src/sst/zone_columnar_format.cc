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

#include "cedar/sst/zone_columnar_format.h"

#include <cstring>
#include <algorithm>
#include <fstream>
#include <unordered_set>
#include <unordered_map>

#include "cedar/core/crc32c.h"
#include "cedar/sst/blob_file.h"
#include "cedar/types/descriptor.h"

#include <optional>

namespace cedar {

// =============================================================================
// SimpleSSTBlobManager - 每个 SST 对应一个 Blob 文件 (1:1 映射)
// =============================================================================
class SimpleSSTBlobManager {
 public:
  SimpleSSTBlobManager(const std::string& db_path, uint32_t sst_id)
      : db_path_(db_path), sst_id_(sst_id) {}
  
  ~SimpleSSTBlobManager() { Close(); }

  Status OpenForWrite() {
    if (writer_) return Status::OK();
    writer_ = std::make_unique<BlobFileWriter>(GetBlobPath(), sst_id_);
    return writer_->Open();
  }
  
  Status OpenForRead() {
    if (reader_) return Status::OK();
    reader_ = std::make_unique<BlobFileReader>(GetBlobPath());
    return reader_->Open();
  }
  
  Status Close() {
    if (writer_) {
      writer_->Close();
      writer_.reset();
    }
    if (reader_) {
      reader_->Close();
      reader_.reset();
    }
    return Status::OK();
  }
  
  std::optional<Descriptor::BlobRef> WriteBlob(const Slice& data) {
    if (!writer_) return std::nullopt;
    if (data.size() <= 6) return std::nullopt;  // 小值内联
    
    uint32_t offset = 0;
    Status s = writer_->Append(data, &offset);
    if (!s.ok()) return std::nullopt;
    
    uint32_t aligned_size = ((4 + data.size() + 4095) / 4096) * 4096;
    uint16_t size_kb = static_cast<uint16_t>(aligned_size / 1024);
    uint8_t checksum = ComputeChecksum(data);
    
    return Descriptor::BlobRef{offset, size_kb, checksum};
  }
  
  Status ReadBlob(uint32_t offset, uint16_t size_kb, std::string* out_data) {
    if (!reader_) return Status::IOError("SimpleSSTBlobManager", "not opened for read");
    
    uint32_t read_size = size_kb * 1024;
    std::vector<char> buffer(read_size);
    
    Status s = reader_->Read(offset, read_size, buffer.data());
    CEDAR_RETURN_IF_ERROR(s);
    
    uint32_t actual_size = *reinterpret_cast<uint32_t*>(buffer.data());
    out_data->assign(buffer.data() + 4, actual_size);
    return Status::OK();
  }
  
  Status ReadBlobs(const std::vector<std::pair<uint32_t, uint16_t>>& offsets_sizes,
                   std::vector<std::string>* out_datas) {
    if (!reader_) return Status::IOError("SimpleSSTBlobManager", "not opened for read");
    
    std::vector<uint32_t> offsets;
    offsets.reserve(offsets_sizes.size());
    for (const auto& [offset, _] : offsets_sizes) {
      offsets.push_back(offset);
    }
    reader_->Prefetch(offsets);
    
    out_datas->clear();
    out_datas->reserve(offsets_sizes.size());
    
    for (const auto& [offset, size_kb] : offsets_sizes) {
      std::string data;
      Status s = ReadBlob(offset, size_kb, &data);
      CEDAR_RETURN_IF_ERROR(s);
      out_datas->push_back(std::move(data));
    }
    return Status::OK();
  }
  
  std::string GetBlobPath() const {
    return db_path_ + "/sst_" + std::to_string(sst_id_) + ".blob";
  }

 private:
  static uint8_t ComputeChecksum(const Slice& data) {
    uint8_t checksum = 0;
    for (size_t i = 0; i < data.size(); ++i) {
      checksum ^= static_cast<uint8_t>(data[i]);
      checksum = (checksum << 1) | (checksum >> 7);
    }
    return checksum;
  }

  std::string db_path_;
  uint32_t sst_id_;
  std::unique_ptr<BlobFileWriter> writer_;
  std::unique_ptr<BlobFileReader> reader_;
};

// =============================================================================
// Helper functions for encoding/decoding
// =============================================================================

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

// =============================================================================
// ZoneColumnarHeader 实现
// =============================================================================

void ZoneColumnarHeader::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  size_t pos = 0;

  // Magic & Version (8 bytes)
  EncodeFixed32(buf + pos, magic);
  pos += 4;
  EncodeFixed32(buf + pos, version);
  pos += 4;

  // 文件元信息 (16 bytes)
  EncodeFixed32(buf + pos, flags);
  pos += 4;
  EncodeFixed16(buf + pos, column_id);
  pos += 2;
  buf[pos++] = entity_type;
  buf[pos++] = reserved1;
  EncodeFixed32(buf + pos, row_count);
  pos += 4;
  EncodeFixed32(buf + pos, block_size);
  pos += 4;

  // Zone 0: EntityIds (16 bytes)
  buf[pos++] = zone0.encoding_type;
  buf[pos++] = zone0.compression_type;
  EncodeFixed16(buf + pos, zone0.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone0.data_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone0.data_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone0.uncompressed_size);
  pos += 4;

  // Zone 1: Timestamps (16 bytes)
  buf[pos++] = zone1.encoding_type;
  buf[pos++] = zone1.compression_type;
  EncodeFixed16(buf + pos, zone1.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone1.data_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone1.data_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone1.uncompressed_size);
  pos += 4;

  // Zone 2: TargetIds (16 bytes)
  buf[pos++] = zone2.encoding_type;
  buf[pos++] = zone2.compression_type;
  EncodeFixed16(buf + pos, zone2.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone2.data_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone2.data_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone2.uncompressed_size);
  pos += 4;

  // Zone 3: Key Metadata (48 bytes) - 5 个独立编码字段
  buf[pos++] = zone3.encoding_type;
  buf[pos++] = zone3.compression_type;
  EncodeFixed16(buf + pos, zone3.reserved);
  pos += 2;
  // Column ID (φ)
  EncodeFixed32(buf + pos, zone3.column_rle_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone3.column_rle_size);
  pos += 4;
  // Sequence (κ)
  EncodeFixed32(buf + pos, zone3.seq_rle_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone3.seq_rle_size);
  pos += 4;
  // Entity Type (τ)
  EncodeFixed32(buf + pos, zone3.type_bitmap_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone3.type_bitmap_size);
  pos += 4;
  // Flags (δ)
  EncodeFixed32(buf + pos, zone3.flags_bitmap_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone3.flags_bitmap_size);
  pos += 4;
  // Part ID
  EncodeFixed32(buf + pos, zone3.part_rle_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone3.part_rle_size);
  pos += 4;

  // Zone 4: Values (16 bytes)
  buf[pos++] = zone4.encoding_type;
  buf[pos++] = zone4.compression_type;
  EncodeFixed16(buf + pos, zone4.reserved);
  pos += 2;
  EncodeFixed32(buf + pos, zone4.data_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone4.data_size);
  pos += 4;
  EncodeFixed32(buf + pos, zone4.uncompressed_size);
  pos += 4;

  // Block Info 偏移 (8 bytes)
  EncodeFixed32(buf + pos, block_info_offset);
  pos += 4;
  EncodeFixed32(buf + pos, block_count);
  pos += 4;

  // Zone Maps 偏移 (16 bytes)
  EncodeFixed32(buf + pos, zone_maps_offset);
  pos += 4;
  EncodeFixed32(buf + pos, zone_maps_size);
  pos += 4;
  EncodeFixed32(buf + pos, restart_points_offset);
  pos += 4;
  EncodeFixed32(buf + pos, restart_points_count);
  pos += 4;

  // Bloom Filter 偏移 (8 bytes)
  EncodeFixed32(buf + pos, bloom_filter_offset);
  pos += 4;
  EncodeFixed32(buf + pos, bloom_filter_size);
  pos += 4;

  // Footer 偏移 (8 bytes)
  EncodeFixed32(buf + pos, footer_offset);
  pos += 4;
  EncodeFixed32(buf + pos, reserved2);
  pos += 4;
  
  // Entity Index 偏移 (8 bytes) - OPTIMIZATION: 持久化倒排索引
  EncodeFixed32(buf + pos, entity_index_offset);
  pos += 4;
  EncodeFixed32(buf + pos, entity_index_size);
  pos += 4;

  // 时间戳范围 (16 bytes)
  EncodeFixed64(buf + pos, min_timestamp);
  pos += 8;
  EncodeFixed64(buf + pos, max_timestamp);
  pos += 8;

  // Entity ID 范围 (16 bytes)
  EncodeFixed64(buf + pos, min_entity_id);
  pos += 8;
  EncodeFixed64(buf + pos, max_entity_id);
  pos += 8;

  // 保留字段 (44 bytes)
  memset(buf + pos, 0, 44);
  pos += 44;

  dst->append(buf, kEncodedSize);
}

Status ZoneColumnarHeader::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneColumnarHeader", "truncated header");
  }

  const char* p = input->data();
  size_t pos = 0;

  magic = DecodeFixed32(p + pos);
  pos += 4;
  version = DecodeFixed32(p + pos);
  pos += 4;
  flags = DecodeFixed32(p + pos);
  pos += 4;
  column_id = DecodeFixed16(p + pos);
  pos += 2;
  entity_type = p[pos++];
  reserved1 = p[pos++];
  row_count = DecodeFixed32(p + pos);
  pos += 4;
  block_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 0
  zone0.encoding_type = p[pos++];
  zone0.compression_type = p[pos++];
  zone0.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone0.data_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone0.data_size = DecodeFixed32(p + pos);
  pos += 4;
  zone0.uncompressed_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 1
  zone1.encoding_type = p[pos++];
  zone1.compression_type = p[pos++];
  zone1.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone1.data_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone1.data_size = DecodeFixed32(p + pos);
  pos += 4;
  zone1.uncompressed_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 2
  zone2.encoding_type = p[pos++];
  zone2.compression_type = p[pos++];
  zone2.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone2.data_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone2.data_size = DecodeFixed32(p + pos);
  pos += 4;
  zone2.uncompressed_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 3: Key Metadata (48 bytes)
  zone3.encoding_type = p[pos++];
  zone3.compression_type = p[pos++];
  zone3.reserved = DecodeFixed16(p + pos);
  pos += 2;
  // Column ID (φ)
  zone3.column_rle_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone3.column_rle_size = DecodeFixed32(p + pos);
  pos += 4;
  // Sequence (κ)
  zone3.seq_rle_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone3.seq_rle_size = DecodeFixed32(p + pos);
  pos += 4;
  // Entity Type (τ)
  zone3.type_bitmap_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone3.type_bitmap_size = DecodeFixed32(p + pos);
  pos += 4;
  // Flags (δ)
  zone3.flags_bitmap_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone3.flags_bitmap_size = DecodeFixed32(p + pos);
  pos += 4;
  // Part ID
  zone3.part_rle_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone3.part_rle_size = DecodeFixed32(p + pos);
  pos += 4;

  // Zone 4: Values
  zone4.encoding_type = p[pos++];
  zone4.compression_type = p[pos++];
  zone4.reserved = DecodeFixed16(p + pos);
  pos += 2;
  zone4.data_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone4.data_size = DecodeFixed32(p + pos);
  pos += 4;
  zone4.uncompressed_size = DecodeFixed32(p + pos);
  pos += 4;

  // Block Info 偏移
  block_info_offset = DecodeFixed32(p + pos);
  pos += 4;
  block_count = DecodeFixed32(p + pos);
  pos += 4;

  zone_maps_offset = DecodeFixed32(p + pos);
  pos += 4;
  zone_maps_size = DecodeFixed32(p + pos);
  pos += 4;
  restart_points_offset = DecodeFixed32(p + pos);
  pos += 4;
  restart_points_count = DecodeFixed32(p + pos);
  pos += 4;

  bloom_filter_offset = DecodeFixed32(p + pos);
  pos += 4;
  bloom_filter_size = DecodeFixed32(p + pos);
  pos += 4;

  footer_offset = DecodeFixed32(p + pos);
  pos += 4;
  reserved2 = DecodeFixed32(p + pos);
  pos += 4;
  
  // Entity Index 偏移 - OPTIMIZATION
  entity_index_offset = DecodeFixed32(p + pos);
  pos += 4;
  entity_index_size = DecodeFixed32(p + pos);
  pos += 4;

  min_timestamp = DecodeFixed64(p + pos);
  pos += 8;
  max_timestamp = DecodeFixed64(p + pos);
  pos += 8;

  min_entity_id = DecodeFixed64(p + pos);
  pos += 8;
  max_entity_id = DecodeFixed64(p + pos);
  pos += 8;

  pos += 44;  // Skip reserved (44 bytes)

  input->remove_prefix(kEncodedSize);

  if (magic != kZoneColumnarMagic) {
    return Status::Corruption("ZoneColumnarHeader", "invalid magic number");
  }

  return Status::OK();
}

// =============================================================================
// BlockInfoEntry 实现
// =============================================================================

void BlockInfoEntry::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  EncodeFixed64(buf, offset);
  EncodeFixed64(buf + 8, size);
  EncodeFixed32(buf + 16, start_row);
  EncodeFixed32(buf + 20, row_count);
  EncodeFixed64(buf + 24, first_entity_id);
  dst->append(buf, kEncodedSize);
}

Status BlockInfoEntry::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("BlockInfoEntry", "truncated");
  }

  const char* p = input->data();
  offset = DecodeFixed64(p);
  size = DecodeFixed64(p + 8);
  start_row = DecodeFixed32(p + 16);
  row_count = DecodeFixed32(p + 20);
  first_entity_id = DecodeFixed64(p + 24);

  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneMapEntry 实现
// =============================================================================

void ZoneMapEntry::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  EncodeFixed64(buf, min_value);
  EncodeFixed64(buf + 8, max_value);
  EncodeFixed64(buf + 16, count);
  EncodeFixed64(buf + 24, distinct_count);
  dst->append(buf, kEncodedSize);
}

Status ZoneMapEntry::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneMapEntry", "truncated");
  }

  const char* p = input->data();
  min_value = DecodeFixed64(p);
  max_value = DecodeFixed64(p + 8);
  count = DecodeFixed64(p + 16);
  distinct_count = DecodeFixed64(p + 24);

  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneRestartPoint 实现
// =============================================================================

void ZoneRestartPoint::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  
  EncodeFixed64(buf, entity_id);
  EncodeFixed32(buf + 8, timestamp_hi);
  EncodeFixed32(buf + 12, row_index);
  
  dst->append(buf, kEncodedSize);
}

Status ZoneRestartPoint::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneRestartPoint", "truncated");
  }

  const char* p = input->data();
  entity_id = DecodeFixed64(p);
  timestamp_hi = DecodeFixed32(p + 8);
  row_index = DecodeFixed32(p + 12);

  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneColumnarFooter 实现
// =============================================================================

void ZoneColumnarFooter::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  size_t pos = 0;

  EncodeFixed64(buf + pos, data_checksum);
  pos += 8;
  EncodeFixed64(buf + pos, header_checksum);
  pos += 8;

  EncodeFixed64(buf + pos, entry_count);
  pos += 8;
  EncodeFixed64(buf + pos, uncompressed_size);
  pos += 8;
  EncodeFixed64(buf + pos, compressed_size);
  pos += 8;
  EncodeFixed64(buf + pos, index_size);
  pos += 8;

  EncodeFixed64(buf + pos, file_number);
  pos += 8;
  EncodeFixed64(buf + pos, prev_file_number);
  pos += 8;

  EncodeFixed32(buf + pos, level);
  pos += 4;
  EncodeFixed32(buf + pos, sequence);
  pos += 4;

  memcpy(buf + pos, &compression_ratio, sizeof(float));
  pos += 4;
  EncodeFixed32(buf + pos, encoding_time_us);
  pos += 4;
  EncodeFixed32(buf + pos, reserved1);
  pos += 4;
  EncodeFixed32(buf + pos, reserved2);
  pos += 4;

  memset(buf + pos, 0, 40);
  pos += 40;

  dst->append(buf, kEncodedSize);
}

Status ZoneColumnarFooter::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneColumnarFooter", "truncated footer");
  }

  const char* p = input->data();
  size_t pos = 0;

  data_checksum = DecodeFixed64(p + pos);
  pos += 8;
  header_checksum = DecodeFixed64(p + pos);
  pos += 8;

  entry_count = DecodeFixed64(p + pos);
  pos += 8;
  uncompressed_size = DecodeFixed64(p + pos);
  pos += 8;
  compressed_size = DecodeFixed64(p + pos);
  pos += 8;
  index_size = DecodeFixed64(p + pos);
  pos += 8;

  file_number = DecodeFixed64(p + pos);
  pos += 8;
  prev_file_number = DecodeFixed64(p + pos);
  pos += 8;

  level = DecodeFixed32(p + pos);
  pos += 4;
  sequence = DecodeFixed32(p + pos);
  pos += 4;

  memcpy(&compression_ratio, p + pos, sizeof(float));
  pos += 4;
  encoding_time_us = DecodeFixed32(p + pos);
  pos += 4;
  reserved1 = DecodeFixed32(p + pos);
  pos += 4;
  reserved2 = DecodeFixed32(p + pos);
  pos += 4;

  pos += 40;  // Skip reserved

  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================

}  // namespace cedar
