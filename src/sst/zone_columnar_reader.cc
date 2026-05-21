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

#include "cedar/sst/zone_columnar_reader.h"

#include <algorithm>
#include <cstring>
#include <unordered_set>
#include "cedar/core/env.h"
#include "cedar/core/crc32c.h"
#include "cedar/sst/blob_file_manager.h"

namespace {
// CRC64: combine two CRC32C with different seeds for 64-bit integrity check
static uint64_t ComputeCRC64(const char* data, size_t n) {
  uint32_t crc_lo = cedar::crc32c::Value(data, n);
  uint32_t crc_hi = cedar::crc32c::Extend(0xa5a5a5a5u, data, n);
  return (static_cast<uint64_t>(crc_hi) << 32) | static_cast<uint64_t>(crc_lo);
}
}  // namespace

namespace cedar {

// =============================================================================
// ZoneColumnarSstReader Implementation - V2 Format
// =============================================================================

ZoneColumnarSstReader::ZoneColumnarSstReader(const std::string& file_path)
    : file_path_(file_path), buffer_data_(nullptr), buffer_size_(0), owns_buffer_(false) {
}

ZoneColumnarSstReader::ZoneColumnarSstReader(const char* data, size_t size)
    : file_path_(""), buffer_data_(data), buffer_size_(size), owns_buffer_(false) {
}

ZoneColumnarSstReader::~ZoneColumnarSstReader() {
  Close();
}

Status ZoneColumnarSstReader::Open() {
  if (opened_) return Status::OK();
  
  // 如果是内存缓冲区模式，不需要打开文件
  if (buffer_data_ != nullptr) {
    Status s = LoadMetadataFromBuffer();
    CEDAR_RETURN_IF_ERROR(s);
    opened_ = true;
    return Status::OK();
  }
  
  // 打开文件
  auto env = Env::Default();
  RandomAccessFile* file = nullptr;
  Status s = env->NewRandomAccessFile(file_path_, &file);
  if (!s.ok()) {
    return s;
  }
  file_.reset(file);
  
  // 加载元数据
  s = LoadMetadata();
  CEDAR_RETURN_IF_ERROR(s);
  
  opened_ = true;
  return Status::OK();
}

void ZoneColumnarSstReader::Close() {
  if (!opened_) return;
  
  file_.reset();
  block_cache_.clear();
  opened_ = false;
}

Status ZoneColumnarSstReader::LoadMetadata() {
  // 读取文件大小
  auto env = Env::Default();
  uint64_t file_size = 0;
  Status s = env->GetFileSize(file_path_, &file_size);
  CEDAR_RETURN_IF_ERROR(s);
  
  // V2: Header 64 bytes + Footer 64 bytes = 128 bytes minimum
  if (file_size < ZoneColumnarHeaderV2::kEncodedSize + ZoneColumnarFooterV2::kEncodedSize) {
    return Status::Corruption("SST file too small");
  }
  
  // 读取 Header (64 bytes)
  char header_buf[ZoneColumnarHeaderV2::kEncodedSize];
  Slice header_result;
  s = file_->Read(0, sizeof(header_buf), &header_result, header_buf);
  CEDAR_RETURN_IF_ERROR(s);
  
  Slice header_input(header_result.data(), header_result.size());
  s = header_.DecodeFrom(&header_input);
  CEDAR_RETURN_IF_ERROR(s);
  
  // 验证魔数 (V2 uses sstv2::kMagic = 0x5A434F4C)
  if (header_.magic != sstv2::kMagic) {
    return Status::Corruption("Invalid SST magic number");
  }
  
  // 验证版本 (V2 uses version = 1 for compatibility)
  if (header_.version != sstv2::kVersion) {
    return Status::Corruption("Invalid SST version");
  }
  
  // 读取 Footer (64 bytes at end of file; backward compat for old 48-byte footers)
  char footer_buf[ZoneColumnarFooterV2::kEncodedSize];
  Slice footer_result;
  s = file_->Read(file_size - sizeof(footer_buf), sizeof(footer_buf), 
                  &footer_result, footer_buf);
  CEDAR_RETURN_IF_ERROR(s);
  
  Slice footer_input(footer_result.data(), footer_result.size());
  s = footer_.DecodeFrom(&footer_input);
  CEDAR_RETURN_IF_ERROR(s);
  
  // 加载 Block Index
  if (footer_.block_count > 0 && footer_.block_index_size > 0) {
    std::string index_buf;
    index_buf.resize(footer_.block_index_size);
    Slice index_result;
    s = file_->Read(footer_.block_index_offset, footer_.block_index_size, 
                    &index_result, &index_buf[0]);
    CEDAR_RETURN_IF_ERROR(s);
    
    Slice index_input(index_result.data(), index_result.size());
    for (uint32_t i = 0; i < footer_.block_count; i++) {
      BlockIndexEntry entry;
      s = entry.DecodeFrom(&index_input);
      CEDAR_RETURN_IF_ERROR(s);
      block_index_.push_back(entry);
    }
  }
  
  // 加载 Bloom Filter (if present)
  if (footer_.bloom_filter_size > 0) {
    std::string bloom_buf;
    bloom_buf.resize(footer_.bloom_filter_size);
    Slice bloom_result;
    s = file_->Read(footer_.bloom_filter_offset, footer_.bloom_filter_size, 
                    &bloom_result, &bloom_buf[0]);
    CEDAR_RETURN_IF_ERROR(s);
    
    // Bloom filter data is raw bitmap
    bloom_filter_.Init(bloom_result.data(), bloom_result.size());
  }
  
  // 加载 Temporal Bloom Filter (if present)
  if (footer_.temporal_filter_size > 0) {
    temporal_filter_data_.resize(footer_.temporal_filter_size);
    Slice tf_result;
    s = file_->Read(footer_.temporal_filter_offset, footer_.temporal_filter_size,
                    &tf_result, &temporal_filter_data_[0]);
    CEDAR_RETURN_IF_ERROR(s);
    temporal_filter_data_.assign(tf_result.data(), tf_result.size());
  }

  // 验证 data_checksum: header 之后到 footer 之前的所有数据
  size_t data_start = ZoneColumnarHeaderV2::kEncodedSize;
  size_t data_end = file_size - ZoneColumnarFooterV2::kEncodedSize;
  if (data_end > data_start) {
    size_t data_len = data_end - data_start;
    std::string data_buf;
    data_buf.resize(data_len);
    Slice data_result;
    s = file_->Read(data_start, data_len, &data_result, &data_buf[0]);
    CEDAR_RETURN_IF_ERROR(s);
    uint64_t computed = ComputeCRC64(data_result.data(), data_result.size());
    if (computed != footer_.data_checksum) {
      return Status::Corruption("SST data checksum mismatch");
    }
  }

  return Status::OK();
}

std::optional<Descriptor> ZoneColumnarSstReader::Get(const CedarKey& key) const {
  if (!opened_) return std::nullopt;
  
  // Step 1: Bloom Filter 过滤
  if (!MayContainEntity(key.entity_id())) {
    return std::nullopt;
  }
  
  // Step 2: 在 Block Index 中查找可能包含该 entity 的 Block
  for (size_t i = 0; i < block_index_.size(); ++i) {
    const auto& entry = block_index_[i];
    if (key.entity_id() < entry.min_entity_id || key.entity_id() > entry.max_entity_id) {
      continue;  // Skip blocks that don't contain this entity
    }
    
    // Load the block and search for the key
    auto block = LoadBlock(i);
    if (!block) continue;
    
    // Search within block
    for (uint32_t row = 0; row < entry.row_count; ++row) {
      CedarKey candidate = ReconstructKeyFromBlock(*block, row);
      if (candidate == key) {
        auto desc = GetValueByRow(*block, row);
        if (desc.has_value()) {
          return desc.value();
        }
      }
    }
  }
  
  return std::nullopt;
}

std::optional<Descriptor> ZoneColumnarSstReader::GetAtTime(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    Timestamp timestamp) const {
  if (!opened_) return std::nullopt;
  
  // Get all versions and find the one at specified time
  auto range_results = GetRange(entity_id, entity_type, column_id, 
                                Timestamp(0), timestamp);
  
  for (const auto& [key, desc] : range_results) {
    if (key.timestamp().value() <= timestamp.value()) {
      return desc;  // First match (newest due to descending order)
    }
  }
  
  return std::nullopt;
}

std::vector<std::pair<CedarKey, Descriptor>> ZoneColumnarSstReader::GetRange(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    Timestamp start,
    Timestamp end) const {
  std::vector<std::pair<CedarKey, Descriptor>> results;
  
  if (!opened_) return results;
  
  // Find blocks that may contain this entity
  for (size_t block_idx = 0; block_idx < block_index_.size(); ++block_idx) {
    const auto& entry = block_index_[block_idx];
    
    // Skip if entity not in this block's range
    if (entity_id < entry.min_entity_id || entity_id > entry.max_entity_id) {
      continue;
    }
    
    // Skip if timestamp not in range
    if (end.value() < entry.min_timestamp || start.value() > entry.max_timestamp) {
      continue;
    }
    
    // Load block
    auto block = LoadBlock(block_idx);
    if (!block) continue;
    
    // Scan block for matching entries
    for (uint32_t row = 0; row < entry.row_count; ++row) {
      CedarKey key = ReconstructKeyFromBlock(*block, row);
      
      if (key.entity_id() != entity_id) continue;
      // Entity type 检查: 如果查询指定了特定类型，始终过滤
      if (static_cast<uint8_t>(entity_type) != 0 &&
          static_cast<uint8_t>(key.entity_type()) != static_cast<uint8_t>(entity_type)) continue;
      // Column ID 检查: 如果查询指定了特定 column_id，始终过滤（即使 SST 是跨列存储）
      if (column_id != UINT16_MAX && key.column_id() != column_id) continue;
      if (key.timestamp().value() < start.value() || key.timestamp().value() > end.value()) continue;
      
      auto desc = GetValueByRow(*block, row);
      if (desc.has_value()) {
        results.emplace_back(key, desc.value());
      }
    }
  }
  
  // Sort by timestamp descending (CedarKey default sort)
  std::sort(results.begin(), results.end(), 
    [](const auto& a, const auto& b) { return a.first.LessForSorting(b.first); });
  
  return results;
}

bool ZoneColumnarSstReader::MayContainEntity(uint64_t entity_id) const {
  // Header range check
  if (entity_id < header_.min_entity_id || entity_id > header_.max_entity_id) {
    return false;
  }
  
  // Bloom filter check (if available)
  return bloom_filter_.MayContain(entity_id);
}

// Helper to load a block from file
std::shared_ptr<BlockCacheEntry> ZoneColumnarSstReader::LoadBlock(uint32_t block_id) const {
  if (block_id >= block_index_.size()) return nullptr;
  
  // Check cache first
  {
    std::lock_guard<std::mutex> lock(block_cache_mutex_);
    auto it = block_cache_.find(block_id);
    if (it != block_cache_.end()) {
      return it->second;
    }
  }
  
  const auto& entry = block_index_[block_id];
  
  // Read block data from file
  std::string block_data;
  block_data.resize(entry.block_size);
  Slice result;
  Status s = file_->Read(entry.block_offset, entry.block_size, &result, &block_data[0]);
  if (!s.ok()) return nullptr;
  
  // Parse block header and data
  auto cache_entry = std::make_shared<BlockCacheEntry>();
  cache_entry->start_row = 0;  // Will be computed
  cache_entry->num_rows = entry.row_count;
  cache_entry->min_entity_id = entry.min_entity_id;
  cache_entry->max_entity_id = entry.max_entity_id;
  cache_entry->data.assign(result.data(), result.size());
  
  // Parse block header (40 bytes)
  if (result.size() < BlockHeader::kSize) return nullptr;
  const char* p = result.data();
  uint32_t row_count = *reinterpret_cast<const uint32_t*>(p);
  p += 4;
  
  // Zone sizes
  uint32_t zone_sizes[5];
  for (int i = 0; i < 5; i++) {
    zone_sizes[i] = *reinterpret_cast<const uint32_t*>(p);
    p += 4;
  }
  
  uint64_t min_entity = *reinterpret_cast<const uint64_t*>(p);
  p += 8;
  uint64_t max_entity = *reinterpret_cast<const uint64_t*>(p);
  
  (void)row_count;  // Unused for now
  (void)min_entity;
  (void)max_entity;
  
  // Store zone offsets for later parsing
  size_t offset = BlockHeader::kSize;
  for (int i = 0; i < 5; i++) {
    cache_entry->zone_offsets[i] = offset;
    cache_entry->zone_sizes[i] = zone_sizes[i];
    offset += zone_sizes[i];
  }
  
  // Add to cache
  {
    std::lock_guard<std::mutex> lock(block_cache_mutex_);
    if (block_cache_.size() >= kMaxCachedBlocks) {
      block_cache_.erase(block_cache_.begin());  // Simple eviction
    }
    block_cache_[block_id] = cache_entry;
  }
  
  return cache_entry;
}

CedarKey ZoneColumnarSstReader::ReconstructKeyFromBlock(
    const BlockCacheEntry& block, uint32_t row_idx) const {
  // Parse entity_id from zone 0
  uint64_t entity_id = 0;
  if (block.zone_sizes[0] >= (row_idx + 1) * 8) {
    entity_id = *reinterpret_cast<const uint64_t*>(
        block.data.data() + block.zone_offsets[0] + row_idx * 8);
  }
  
  // Parse timestamp from zone 1 (uint64_t)
  uint64_t timestamp_val = 0;
  if (block.zone_sizes[1] >= (row_idx + 1) * 8) {
    timestamp_val = *reinterpret_cast<const uint64_t*>(
        block.data.data() + block.zone_offsets[1] + row_idx * 8);
  }
  
  // Parse target_id from zone 2 (uint64_t)
  uint64_t target_id = 0;
  if (block.zone_sizes[2] >= (row_idx + 1) * 8) {
    target_id = *reinterpret_cast<const uint64_t*>(
        block.data.data() + block.zone_offsets[2] + row_idx * 8);
  }
  
  // Parse metadata from zone 3 (raw 8 bytes per row)
  uint16_t column_id = static_cast<uint16_t>(header_.column_id);
  uint8_t entity_type = header_.entity_type != 0 ? header_.entity_type : 0;
  uint16_t sequence = 0;
  uint8_t flags = 0;
  uint16_t part_id = 0;
  
  size_t meta_offset = block.zone_offsets[3] + row_idx * 8;
  if (block.zone_sizes[3] >= (row_idx + 1) * 8) {
    column_id = *reinterpret_cast<const uint16_t*>(
        block.data.data() + meta_offset);
    sequence = *reinterpret_cast<const uint16_t*>(
        block.data.data() + meta_offset + 2);
    entity_type = *reinterpret_cast<const uint8_t*>(
        block.data.data() + meta_offset + 4);
    flags = *reinterpret_cast<const uint8_t*>(
        block.data.data() + meta_offset + 5);
    part_id = *reinterpret_cast<const uint16_t*>(
        block.data.data() + meta_offset + 6);
  }
  
  return CedarKey(entity_id, 
                  static_cast<EntityType>(entity_type),
                  column_id,
                  Timestamp(timestamp_val),
                  sequence,
                  target_id,
                  flags,
                  part_id);
}

std::optional<Descriptor> ZoneColumnarSstReader::GetValueByRow(
    const BlockCacheEntry& block, uint32_t row_idx) const {
  // Zone 4 contains descriptor data
  Descriptor desc;
  
  if (block.zone_sizes[4] > row_idx * sizeof(uint64_t)) {
    // Parse descriptor from zone 4
    const char* data = block.data.data() + block.zone_offsets[4] + row_idx * sizeof(uint64_t);
    uint64_t desc_data;
    std::memcpy(&desc_data, data, sizeof(desc_data));
    desc = Descriptor(desc_data);
  }
  
  return desc;
}

Status ZoneColumnarSstReader::LoadMetadataFromBuffer() {
  // Similar to LoadMetadata but from memory buffer
  if (buffer_size_ < ZoneColumnarHeaderV2::kEncodedSize + ZoneColumnarFooterV2::kEncodedSize) {
    return Status::Corruption("Buffer too small");
  }
  
  // Parse header from buffer
  Slice header_input(buffer_data_, ZoneColumnarHeaderV2::kEncodedSize);
  Status s = header_.DecodeFrom(&header_input);
  CEDAR_RETURN_IF_ERROR(s);
  
  if (header_.magic != sstv2::kMagic) {
    return Status::Corruption("Invalid magic");
  }
  
  // Parse footer from end of buffer
  const char* footer_data = buffer_data_ + buffer_size_ - ZoneColumnarFooterV2::kEncodedSize;
  Slice footer_input(footer_data, ZoneColumnarFooterV2::kEncodedSize);
  s = footer_.DecodeFrom(&footer_input);
  CEDAR_RETURN_IF_ERROR(s);
  
  // Parse block index from buffer
  if (footer_.block_count > 0 && footer_.block_index_size > 0) {
    const char* index_data = buffer_data_ + footer_.block_index_offset;
    Slice index_input(index_data, footer_.block_index_size);
    for (uint32_t i = 0; i < footer_.block_count; i++) {
      BlockIndexEntry entry;
      s = entry.DecodeFrom(&index_input);
      CEDAR_RETURN_IF_ERROR(s);
      block_index_.push_back(entry);
    }
  }
  
  // Load temporal filter from buffer (if present)
  if (footer_.temporal_filter_size > 0) {
    const char* tf_data = buffer_data_ + footer_.temporal_filter_offset;
    temporal_filter_data_.assign(tf_data, footer_.temporal_filter_size);
  }

  // 验证 data_checksum from buffer
  size_t data_start = ZoneColumnarHeaderV2::kEncodedSize;
  size_t data_end = buffer_size_ - ZoneColumnarFooterV2::kEncodedSize;
  if (data_end > data_start) {
    size_t data_len = data_end - data_start;
    uint64_t computed = ComputeCRC64(buffer_data_ + data_start, data_len);
    if (computed != footer_.data_checksum) {
      return Status::Corruption("SST data checksum mismatch (buffer)");
    }
  }

  return Status::OK();
}

// Iterator implementation
ZoneColumnarSstReader::Iterator::Iterator(const ZoneColumnarSstReader* reader)
    : reader_(reader), current_idx_(0), total_count_(0), valid_(false) {
}

void ZoneColumnarSstReader::Iterator::SeekToFirst() {
  current_idx_ = 0;
  total_count_ = reader_->footer_.row_count;
  valid_ = total_count_ > 0;
  if (valid_) {
    EnsureBlockLoaded(static_cast<uint32_t>(current_idx_));
  }
}

void ZoneColumnarSstReader::Iterator::Seek(const CedarKey& key) {
  SeekToFirst();
  while (valid_) {
    if (Key() == key) return;
    Next();
  }
}

void ZoneColumnarSstReader::Iterator::Next() {
  if (!valid_) return;
  current_idx_++;
  valid_ = current_idx_ < total_count_;
  if (valid_) {
    EnsureBlockLoaded(static_cast<uint32_t>(current_idx_));
  }
}

CedarKey ZoneColumnarSstReader::Iterator::Key() const {
  if (!valid_ || !current_block_) return CedarKey();
  uint32_t local_idx = static_cast<uint32_t>(current_idx_ - current_block_start_row_);
  return reader_->ReconstructKeyFromBlock(*current_block_, local_idx);
}

Descriptor ZoneColumnarSstReader::Iterator::Value() const {
  if (!valid_ || !current_block_) return Descriptor();
  uint32_t local_idx = static_cast<uint32_t>(current_idx_ - current_block_start_row_);
  auto opt = reader_->GetValueByRow(*current_block_, local_idx);
  return opt.value_or(Descriptor());
}

Status ZoneColumnarSstReader::Iterator::EnsureBlockLoaded(uint32_t row_idx) const {
  // Find which block contains this row
  uint32_t block_id = 0;
  uint32_t start_row = 0;
  for (size_t i = 0; i < reader_->block_index_.size(); ++i) {
    const auto& entry = reader_->block_index_[i];
    if (row_idx < start_row + entry.row_count) {
      block_id = static_cast<uint32_t>(i);
      break;
    }
    start_row += entry.row_count;
  }
  
  if (block_id >= reader_->block_index_.size()) {
    return Status::Corruption("Iterator", "row index out of range");
  }
  
  if (current_block_id_ != block_id) {
    current_block_ = reader_->LoadBlock(block_id);
    current_block_id_ = block_id;
    current_block_start_row_ = start_row;
    if (!current_block_) {
      return Status::Corruption("Iterator", "failed to load block");
    }
  }
  return Status::OK();
}

// Other methods stubs
bool ZoneColumnarSstReader::MayContainTimeRange(uint64_t start_ts, uint64_t end_ts) const {
  return !(end_ts < header_.min_timestamp || start_ts > header_.max_timestamp);
}

bool ZoneColumnarSstReader::MayMatchPredicate(const ReadPredicate& predicate) const {
  // Simplified implementation
  (void)predicate;
  return true;
}

ZoneColumnarSstReader::Stats ZoneColumnarSstReader::GetStats() const {
  Stats stats;
  stats.total_blocks = footer_.block_count;
  stats.total_rows = footer_.row_count;
  return stats;
}

void ZoneColumnarSstReader::Scan(
    const ReadPredicate& predicate,
    std::function<void(const CedarKey&, const Descriptor&)> callback) const {
  if (!opened_) return;
  
  for (size_t block_idx = 0; block_idx < block_index_.size(); ++block_idx) {
    const auto& entry = block_index_[block_idx];
    
    // Skip blocks based on predicate
    if (predicate.entity_id.has_value() && 
        (entry.max_entity_id < predicate.entity_id.value() || 
         entry.min_entity_id > predicate.entity_id.value())) {
      continue;
    }
    
    auto block = LoadBlock(block_idx);
    if (!block) continue;
    
    for (uint32_t row = 0; row < entry.row_count; ++row) {
      CedarKey key = ReconstructKeyFromBlock(*block, row);
      
      // Apply predicate
      if (predicate.entity_id.has_value() && key.entity_id() != predicate.entity_id.value()) continue;
      if (predicate.entity_type.has_value() && 
          static_cast<uint8_t>(key.entity_type()) != predicate.entity_type.value()) continue;
      if (predicate.column_id.has_value() && key.column_id() != predicate.column_id.value()) continue;
      if (predicate.part_id.has_value() && key.part_id() != predicate.part_id.value()) continue;
      if (predicate.min_timestamp.has_value() && key.timestamp().value() < predicate.min_timestamp.value()) continue;
      if (predicate.max_timestamp.has_value() && key.timestamp().value() > predicate.max_timestamp.value()) continue;
      if (predicate.skip_tombstones && key.IsTombstone()) continue;
      
      auto desc = GetValueByRow(*block, row);
      if (desc.has_value()) {
        callback(key, desc.value());
      }
    }
  }
}

std::unordered_map<uint64_t, std::vector<std::pair<CedarKey, Descriptor>>>
ZoneColumnarSstReader::BatchGetRange(
    const std::vector<uint64_t>& entity_ids,
    EntityType entity_type,
    uint16_t column_id,
    Timestamp start,
    Timestamp end) const {
  std::unordered_map<uint64_t, std::vector<std::pair<CedarKey, Descriptor>>> results;
  
  for (uint64_t entity_id : entity_ids) {
    results[entity_id] = GetRange(entity_id, entity_type, column_id, start, end);
  }
  
  return results;
}

// For compatibility with old code - not implemented in V2
CedarKey ZoneColumnarSstReader::ReconstructKey(uint32_t row_idx) const {
  (void)row_idx;
  return CedarKey();
}

std::optional<Descriptor> ZoneColumnarSstReader::GetValueByRow(uint32_t row_idx) const {
  (void)row_idx;
  return std::nullopt;
}

std::vector<Descriptor> ZoneColumnarSstReader::GetValuesByRows(
    const std::vector<uint32_t>& row_indices) const {
  (void)row_indices;
  return {};
}

ZoneColumnarSstReader::Iterator* ZoneColumnarSstReader::NewIterator() const {
  return new Iterator(this);
}

}  // namespace cedar
