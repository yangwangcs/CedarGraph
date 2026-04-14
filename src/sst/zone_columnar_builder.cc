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

#include "cedar/sst/zone_columnar_builder.h"

#include <algorithm>
#include <cstring>
#include "cedar/core/env.h"
#include "cedar/sst/zone_encoder.h"
#include "cedar/sst/blob_file_manager.h"

namespace cedar {

// =============================================================================
// ZoneColumnarSstBuilder Implementation
// =============================================================================

ZoneColumnarSstBuilder::ZoneColumnarSstBuilder(const Options& options, 
                                                    WritableFile* file)
    : options_(options), file_(file), column_id_(0) {
  buffer_.Reserve(options_.block_row_limit);
  
  // TODO: Initialize blob manager if needed
  // if (options_.enable_blob && !options_.db_path.empty()) {
  //   blob_manager_ = std::make_unique<SimpleSSTBlobManager>(...);
  // }
}

ZoneColumnarSstBuilder::~ZoneColumnarSstBuilder() {
  if (!closed_) {
    Abandon();
  }
}

void ZoneColumnarSstBuilder::Add(const CedarKey& key, const Descriptor& descriptor) {
  if (!status_.ok() || closed_) return;
  
  // 检查排序顺序 (调试模式)
  // assert(!has_last_entity_ || key.LessForSorting(last_key_added_));
  
  // 提取 Key 的各个字段
  uint64_t entity_id = key.entity_id();
  uint64_t timestamp = key.timestamp().value();
  uint64_t target_id = key.target_id();
  uint16_t column_id = key.column_id();
  uint16_t sequence = key.sequence();
  uint8_t entity_type = static_cast<uint8_t>(key.entity_type());
  uint8_t flags = key.flags();
  uint16_t part_id = key.part_id();
  
  // 添加到缓冲区
  buffer_.entity_ids.push_back(entity_id);
  buffer_.timestamps.push_back(timestamp);
  buffer_.target_ids.push_back(target_id);
  buffer_.column_ids.push_back(column_id);
  buffer_.sequences.push_back(sequence);
  buffer_.entity_types.push_back(entity_type);
  buffer_.flags.push_back(flags);
  buffer_.part_ids.push_back(part_id);
  buffer_.values.push_back(descriptor);
  
  // 更新 entity_type_ 和 column_id_ (从第一个添加的条目获取)
  if (total_entries_ == 0) {
    entity_type_ = entity_type;
    column_id_ = column_id;
  }
  
  // 更新统计
  total_entries_++;
  if (!has_last_entity_ || entity_id != last_entity_id_) {
    total_entities_++;
    last_entity_id_ = entity_id;
    has_last_entity_ = true;
  }
  
  // 更新全局范围
  if (entity_id < min_entity_id_) min_entity_id_ = entity_id;
  if (entity_id > max_entity_id_) max_entity_id_ = entity_id;
  if (timestamp < min_timestamp_) min_timestamp_ = timestamp;
  if (timestamp > max_timestamp_) max_timestamp_ = timestamp;
  if (key.IsTombstone()) tombstone_count_++;
  
  // 更新 Bloom Filter
  bloom_filter_.Add(key.entity_id());
  
  // 更新 Entity Index
  entity_index_[entity_id].push_back(static_cast<uint32_t>(total_entries_ - 1));
  
  // 检查是否需要切割 Block
  if (ShouldCutBlock()) {
    status_ = FlushBlock();
  }
}

void ZoneColumnarSstBuilder::AddBatch(
    std::vector<std::pair<CedarKey, Descriptor>>& batch) {
  // 按全局排序契约排序
  std::sort(batch.begin(), batch.end(),
    [](const auto& a, const auto& b) {
      return a.first.LessForSorting(b.first);
    });
  
  // 逐个添加
  for (auto& [key, desc] : batch) {
    Add(key, desc);
  }
}

void ZoneColumnarSstBuilder::AddValue(const CedarKey& key, const Slice& raw_value) {
  // 将 raw_value 转换为 Descriptor
  // 简化为内联字符串存储
  Descriptor desc;
  if (raw_value.size() <= 12) {
    // 小值：内联存储
    desc = Descriptor::InlineShortStr(key.column_id(), std::string(raw_value.data(), raw_value.size()))
               .value_or(Descriptor::InlineInt(key.column_id(), 0));
  } else {
    // 大值：截断为内联存储（实际应该存 Blob）
    desc = Descriptor::InlineShortStr(key.column_id(), std::string(raw_value.data(), 12))
               .value_or(Descriptor::InlineInt(key.column_id(), 0));
  }
  Add(key, desc);
}

bool ZoneColumnarSstBuilder::ShouldCutBlock() const {
  if (buffer_.Size() == 0) return false;
  
  // 检查行数限制
  if (buffer_.Size() >= options_.block_row_limit) return true;
  
  // 检查大小限制
  size_t estimated_size = buffer_.Size() * 32;  // 每行约 32B Key + Value
  if (estimated_size >= options_.block_size) {
    if (!options_.entity_aligned) return true;
    
    // 实体对齐：检查当前 entity 是否已积累足够多
    uint64_t current_entity = buffer_.entity_ids.back();
    size_t same_entity_count = 0;
    for (auto it = buffer_.entity_ids.rbegin(); 
         it != buffer_.entity_ids.rend() && *it == current_entity; 
         ++it) {
      same_entity_count++;
    }
    
    // 如果同一 entity 数量超过阈值，允许切割
    if (same_entity_count >= options_.entity_align_threshold) return true;
    
    // 否则继续积累，除非总大小超过 2 倍块大小
    if (estimated_size >= options_.block_size * 2) return true;
  }
  
  return false;
}

Status ZoneColumnarSstBuilder::FlushBlock() {
  if (buffer_.Size() == 0) return Status::OK();
  
  // 编码 Block
  EntityAlignedBlock block = EncodeBlock(buffer_, current_block_start_row_);
  
  // 添加到 blocks 列表
  blocks_.push_back(std::move(block));
  
  // 更新起始行号
  current_block_start_row_ += static_cast<uint32_t>(buffer_.Size());
  
  // 清空缓冲区
  buffer_.Clear();
  buffer_.Reserve(options_.block_row_limit);
  
  return Status::OK();
}

EntityAlignedBlock ZoneColumnarSstBuilder::EncodeBlock(
    const RowBuffer& buffer, uint32_t start_row) {
  EntityAlignedBlock block;
  block.start_row = start_row;
  block.row_count = static_cast<uint32_t>(buffer.Size());
  block.first_entity_id = buffer.entity_ids.front();
  block.last_entity_id = buffer.entity_ids.back();
  
  // Zone 0: Entity IDs - RLE 编码
  block.zone0_data = EntityIdZoneEncoder::Encode(buffer.entity_ids);
  
  // Zone 1: Timestamps - Delta-Delta 编码
  // 注意： timestamps 已经是降序排列
  block.zone1_data = TimestampZoneEncoder::Encode(buffer.timestamps);
  
  // Zone 2: Target IDs - Delta 或 RLE
  block.zone2_data = TargetIdZoneEncoder::Encode(buffer.target_ids, buffer.entity_ids);
  
  // Zone 3: Key Metadata - 5 个字段独立编码
  std::vector<KeyMetadataZoneEncoder::MetadataEntry> metadata_entries;
  metadata_entries.reserve(buffer.Size());
  for (size_t i = 0; i < buffer.Size(); ++i) {
    KeyMetadataZoneEncoder::MetadataEntry entry;
    entry.column_id = buffer.column_ids[i];
    entry.sequence = buffer.sequences[i];
    entry.entity_type = buffer.entity_types[i];
    entry.flags = buffer.flags[i];
    entry.part_id = buffer.part_ids[i];
    metadata_entries.push_back(entry);
  }
  
  auto metadata_result = KeyMetadataZoneEncoder::EncodeWithResult(metadata_entries);
  block.zone3_column_rle = std::move(metadata_result.column_rle);
  block.zone3_seq_rle = std::move(metadata_result.sequence_rle);
  block.zone3_type_bitmap = std::move(metadata_result.type_bitmap);
  block.zone3_flags_bitmap = std::move(metadata_result.flags_bitmap);
  block.zone3_part_rle = std::move(metadata_result.part_rle);
  
  // Zone 4: Values - Dictionary 或 LZ4
  auto value_result = ValueZoneEncoder::EncodeWithType(buffer.values);
  block.zone4_data = std::move(value_result.data);
  
  // 统计
  for (uint64_t ts : buffer.timestamps) {
    if (ts < block.min_timestamp) block.min_timestamp = ts;
    if (ts > block.max_timestamp) block.max_timestamp = ts;
  }
  for (uint8_t f : buffer.flags) {
    if (f & key_flags::kTombstone) block.tombstone_count++;
  }
  
  return block;
}

Status ZoneColumnarSstBuilder::Finish() {
  if (!status_.ok() || closed_) return status_;
  
  // 刷新最后的 block
  if (buffer_.Size() > 0) {
    status_ = FlushBlock();
    if (!status_.ok()) return status_;
  }
  
  // 构建所有数据到内存缓冲区，计算偏移量
  std::string file_data;
  file_data.reserve(64 * 1024);  // 预分配 64KB
  
  // 1. 占位 Header (256 bytes)
  size_t header_offset = file_data.size();
  file_data.append(ZoneColumnarHeader::kEncodedSize, '\0');
  
  // 2. 写入所有 Blocks，并记录 Block Info
  std::vector<BlockInfoEntry> block_infos;
  for (const auto& block : blocks_) {
    BlockInfoEntry info;
    info.offset = file_data.size();
    info.start_row = block.start_row;
    info.row_count = block.row_count;
    info.first_entity_id = block.first_entity_id;
    
    // 计算 Zone 大小并写入 header
    uint32_t zone_sizes[5];
    zone_sizes[0] = static_cast<uint32_t>(block.zone0_data.size());
    zone_sizes[1] = static_cast<uint32_t>(block.zone1_data.size());
    zone_sizes[2] = static_cast<uint32_t>(block.zone2_data.size());
    // Zone 3: 5 个字段，每个字段前有 4 字节长度前缀
    zone_sizes[3] = static_cast<uint32_t>(block.zone3_column_rle.size() + block.zone3_seq_rle.size() + 
                                           block.zone3_type_bitmap.size() + block.zone3_flags_bitmap.size() + 
                                           block.zone3_part_rle.size() + 5 * 4);
    zone_sizes[4] = static_cast<uint32_t>(block.zone4_data.size());
    
    // 写入 Zone 大小 header (20 bytes)
    file_data.append(reinterpret_cast<const char*>(zone_sizes), sizeof(zone_sizes));
    
    // Zone 0
    file_data.append(block.zone0_data);
    // Zone 1
    file_data.append(block.zone1_data);
    // Zone 2
    file_data.append(block.zone2_data);
    // Zone 3 (5 个字段) - 每个字段前加 4 字节大小
    auto append_with_size = [&file_data](const std::string& data) {
      uint32_t size = static_cast<uint32_t>(data.size());
      file_data.append(reinterpret_cast<const char*>(&size), 4);
      file_data.append(data);
    };
    append_with_size(block.zone3_column_rle);
    append_with_size(block.zone3_seq_rle);
    append_with_size(block.zone3_type_bitmap);
    append_with_size(block.zone3_flags_bitmap);
    append_with_size(block.zone3_part_rle);
    // Zone 4
    file_data.append(block.zone4_data);
    
    info.size = file_data.size() - info.offset;
    block_infos.push_back(info);
  }
  
  // 3. 写入 Block Info 表
  size_t block_info_offset = file_data.size();
  for (const auto& info : block_infos) {
    info.EncodeTo(&file_data);
  }
  
  // 4. 准备 Zone Maps
  ZoneMapEntry entity_zone_map;
  ZoneMapEntry timestamp_zone_map;
  entity_zone_map.min_value = min_entity_id_;
  entity_zone_map.max_value = max_entity_id_;
  entity_zone_map.count = total_entries_;
  entity_zone_map.distinct_count = total_entities_;
  timestamp_zone_map.min_value = min_timestamp_;
  timestamp_zone_map.max_value = max_timestamp_;
  timestamp_zone_map.count = total_entries_;
  
  size_t zone_maps_offset = file_data.size();
  entity_zone_map.EncodeTo(&file_data);
  timestamp_zone_map.EncodeTo(&file_data);
  size_t zone_maps_size = file_data.size() - zone_maps_offset;
  
  // 4. 准备 Restart Points
  size_t restart_points_offset = file_data.size();
  uint32_t restart_points_count = 0;
  for (const auto& block : blocks_) {
    ZoneRestartPoint rp;
    rp.entity_id = block.first_entity_id;
    rp.timestamp_hi = static_cast<uint32_t>(block.max_timestamp >> 32);
    rp.row_index = block.start_row;
    rp.EncodeTo(&file_data);
    restart_points_count++;
  }
  size_t restart_points_size = file_data.size() - restart_points_offset;
  
  // 5. Bloom Filter (占位，暂不写入实际数据)
  size_t bloom_filter_offset = file_data.size();
  size_t bloom_filter_size = 0;
  
  // 6. 准备 Entity Index
  size_t entity_index_offset = file_data.size();
  uint32_t num_entries = static_cast<uint32_t>(entity_index_.size());
  file_data.append(reinterpret_cast<const char*>(&num_entries), 4);
  for (const auto& [entity_id, positions] : entity_index_) {
    file_data.append(reinterpret_cast<const char*>(&entity_id), 8);
    uint32_t num_pos = static_cast<uint32_t>(positions.size());
    file_data.append(reinterpret_cast<const char*>(&num_pos), 4);
    for (uint32_t pos : positions) {
      file_data.append(reinterpret_cast<const char*>(&pos), 4);
    }
  }
  size_t entity_index_size = file_data.size() - entity_index_offset;
  
  // 7. 准备 Footer
  size_t footer_offset = file_data.size();
  ZoneColumnarFooter footer;
  footer.entry_count = total_entries_;
  footer.file_number = file_number_;
  footer.prev_file_number = prev_file_number_;
  footer.level = level_;
  footer.uncompressed_size = total_entries_ * 32;  // 32B per key
  footer.compressed_size = file_data.size() + ZoneColumnarFooter::kEncodedSize;
  if (footer.uncompressed_size > 0) {
    footer.compression_ratio = static_cast<float>(footer.compressed_size) / footer.uncompressed_size;
  }
  footer.index_size = static_cast<uint32_t>(entity_index_size);
  
  // 填充 Header
  ZoneColumnarHeader header;
  header.magic = kZoneColumnarMagic;
  header.version = kZoneColumnarVersion;
  header.flags = options_.enable_blob ? 1 : 0;
  header.column_id = column_id_;
  header.entity_type = entity_type_;
  header.row_count = static_cast<uint32_t>(total_entries_);
  header.block_size = options_.block_size;
  header.min_timestamp = min_timestamp_;
  header.max_timestamp = max_timestamp_;
  header.min_entity_id = min_entity_id_;
  header.max_entity_id = max_entity_id_;
  header.block_info_offset = static_cast<uint32_t>(block_info_offset);
  header.block_count = static_cast<uint32_t>(block_infos.size());
  header.zone_maps_offset = static_cast<uint32_t>(zone_maps_offset);
  header.zone_maps_size = static_cast<uint32_t>(zone_maps_size);
  header.restart_points_offset = static_cast<uint32_t>(restart_points_offset);
  header.restart_points_count = restart_points_count;
  header.bloom_filter_offset = static_cast<uint32_t>(bloom_filter_offset);
  header.bloom_filter_size = static_cast<uint32_t>(bloom_filter_size);
  header.entity_index_offset = static_cast<uint32_t>(entity_index_offset);
  header.entity_index_size = static_cast<uint32_t>(entity_index_size);
  header.footer_offset = static_cast<uint32_t>(footer_offset);
  
  // 编码 Header 到文件数据的开头
  std::string header_data;
  header.EncodeTo(&header_data);
  if (header_data.size() != ZoneColumnarHeader::kEncodedSize) {
    return Status::Corruption("Header encoding size mismatch");
  }
  memcpy(&file_data[header_offset], header_data.data(), header_data.size());
  
  // 编码 Footer 并追加
  footer.EncodeTo(&file_data);
  
  // 一次性写入文件
  status_ = file_->Append(file_data);
  if (!status_.ok()) return status_;
  file_size_ = file_data.size();
  
  // 同步文件
  if (file_) {
    status_ = file_->Sync();
  }
  if (!status_.ok()) return status_;
  
  closed_ = true;
  return Status::OK();
}

void ZoneColumnarSstBuilder::Abandon() {
  closed_ = true;
  status_ = Status::IOError("Builder abandoned");
}

Status ZoneColumnarSstBuilder::WriteBlocks() {
  // 注意：此函数已弃用，请使用 Finish() 函数
  // 为了兼容性，调用 Finish()
  return Finish();
}

Status ZoneColumnarSstBuilder::WriteZoneMaps() {
  // 计算每个 Zone 的全局统计
  ZoneMapEntry entity_zone_map;
  ZoneMapEntry timestamp_zone_map;
  
  entity_zone_map.min_value = min_entity_id_;
  entity_zone_map.max_value = max_entity_id_;
  entity_zone_map.count = total_entries_;
  entity_zone_map.distinct_count = total_entities_;
  
  timestamp_zone_map.min_value = min_timestamp_;
  timestamp_zone_map.max_value = max_timestamp_;
  timestamp_zone_map.count = total_entries_;
  
  // 编码并写入
  std::string zone_maps_data;
  entity_zone_map.EncodeTo(&zone_maps_data);
  timestamp_zone_map.EncodeTo(&zone_maps_data);
  
  status_ = file_->Append(zone_maps_data);
  if (!status_.ok()) return status_;
  file_size_ += zone_maps_data.size();
  
  return Status::OK();
}

Status ZoneColumnarSstBuilder::WriteRestartPoints() {
  // 从每个 block 创建 restart point
  for (const auto& block : blocks_) {
    ZoneRestartPoint rp;
    rp.entity_id = block.first_entity_id;
    rp.timestamp_hi = static_cast<uint32_t>(block.max_timestamp >> 32);
    rp.row_index = block.start_row;
    restart_points_.push_back(rp);
  }
  
  // 编码并写入
  std::string restart_data;
  for (const auto& rp : restart_points_) {
    rp.EncodeTo(&restart_data);
  }
  
  status_ = file_->Append(restart_data);
  if (!status_.ok()) return status_;
  file_size_ += restart_data.size();
  
  return Status::OK();
}

Status ZoneColumnarSstBuilder::WriteBloomFilter() {
  // TODO: Implement Bloom Filter serialization
  // std::string bloom_data = bloom_filter_.Serialize();
  // status_ = file_->Append(bloom_data);
  // if (!status_.ok()) return status_;
  // file_size_ += bloom_data.size();
  return Status::OK();
}

Status ZoneColumnarSstBuilder::WriteEntityIndex() {
  // 编码 entity index
  // 格式: [num_entries:4B] [entity_id:8B][num_positions:4B][positions:4B...]...
  std::string index_data;
  
  uint32_t num_entries = static_cast<uint32_t>(entity_index_.size());
  index_data.append(reinterpret_cast<const char*>(&num_entries), 4);
  
  for (const auto& [entity_id, positions] : entity_index_) {
    index_data.append(reinterpret_cast<const char*>(&entity_id), 8);
    uint32_t num_pos = static_cast<uint32_t>(positions.size());
    index_data.append(reinterpret_cast<const char*>(&num_pos), 4);
    for (uint32_t pos : positions) {
      index_data.append(reinterpret_cast<const char*>(&pos), 4);
    }
  }
  
  status_ = file_->Append(index_data);
  if (!status_.ok()) return status_;
  file_size_ += index_data.size();
  
  return Status::OK();
}

Status ZoneColumnarSstBuilder::WriteFooter() {
  ZoneColumnarFooter footer;
  footer.entry_count = total_entries_;
  footer.file_number = file_number_;
  footer.prev_file_number = prev_file_number_;
  footer.level = level_;
  
  // 计算压缩率
  size_t uncompressed_size = total_entries_ * 32;  // 32B per key
  footer.uncompressed_size = uncompressed_size;
  footer.compressed_size = file_size_;
  if (uncompressed_size > 0) {
    footer.compression_ratio = static_cast<float>(file_size_) / uncompressed_size;
  }
  
  std::string footer_data(ZoneColumnarFooter::kEncodedSize, '\0');
  footer.EncodeTo(&footer_data);
  
  status_ = file_->Append(footer_data);
  if (!status_.ok()) return status_;
  file_size_ += footer_data.size();
  
  return Status::OK();
}

ZoneColumnarSstBuilder::Stats ZoneColumnarSstBuilder::GetStats() const {
  Stats stats;
  stats.total_entries = total_entries_;
  stats.total_blocks = blocks_.size();
  stats.total_entities = total_entities_;
  if (!blocks_.empty()) {
    stats.avg_rows_per_block = total_entries_ / blocks_.size();
  }
  
  // 计算跨块切割的 entity 数量
  uint64_t crossings = 0;
  uint64_t last_entity = 0;
  bool first = true;
  for (const auto& block : blocks_) {
    if (!first && block.first_entity_id == last_entity) {
      crossings++;
    }
    last_entity = block.last_entity_id;
    first = false;
  }
  stats.entity_span_crossings = crossings;
  
  // Calculate compression ratio
  size_t uncompressed_size = total_entries_ * 32;  // 32B per key
  if (uncompressed_size > 0 && file_size_ > 0) {
    stats.compression_ratio = static_cast<double>(file_size_) / uncompressed_size;
  }
  
  return stats;
}

uint64_t ZoneColumnarSstBuilder::CalculateCRC64(const Slice& data) const {
  // TODO: Implement CRC64
  (void)data;
  return 0;
}

}  // namespace cedar
