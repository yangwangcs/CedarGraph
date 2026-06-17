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

#include "cedar/sst/zone_columnar_format_v2.h"

#include <cstring>
#include <algorithm>

#include "cedar/core/crc32c.h"

namespace cedar {

// =============================================================================
// BlockIndexEntry
// =============================================================================

void BlockIndexEntry::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  std::memset(buf, 0, kEncodedSize);
  
  uint64_t* u64 = reinterpret_cast<uint64_t*>(buf);
  u64[0] = min_entity_id;
  u64[1] = max_entity_id;
  u64[2] = min_timestamp;
  u64[3] = max_timestamp;
  
  uint32_t* u32 = reinterpret_cast<uint32_t*>(buf + 32);
  u32[0] = block_offset;
  u32[1] = block_size;
  u32[2] = row_count;
  
  dst->append(buf, kEncodedSize);
}

Status BlockIndexEntry::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("BlockIndexEntry", "too short");
  }
  
  const char* buf = input->data();
  const uint64_t* u64 = reinterpret_cast<const uint64_t*>(buf);
  min_entity_id = u64[0];
  max_entity_id = u64[1];
  min_timestamp = u64[2];
  max_timestamp = u64[3];
  
  const uint32_t* u32 = reinterpret_cast<const uint32_t*>(buf + 32);
  block_offset = u32[0];
  block_size = u32[1];
  row_count = u32[2];
  
  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneColumnarHeader
// =============================================================================

void ZoneColumnarHeader::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  std::memset(buf, 0, kEncodedSize);
  
  uint32_t* u32 = reinterpret_cast<uint32_t*>(buf);
  u32[0] = magic;
  u32[1] = version;
  
  uint64_t* u64 = reinterpret_cast<uint64_t*>(buf + 8);
  u64[0] = file_size;
  u64[1] = min_entity_id;
  u64[2] = max_entity_id;
  u64[3] = min_timestamp;
  u64[4] = max_timestamp;
  
  u32 = reinterpret_cast<uint32_t*>(buf + 48);
  u32[0] = column_id;
  
  buf[52] = entity_type;
  
  dst->append(buf, kEncodedSize);
}

Status ZoneColumnarHeader::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneColumnarHeader", "too short");
  }
  
  const char* buf = input->data();
  const uint32_t* u32 = reinterpret_cast<const uint32_t*>(buf);
  magic = u32[0];
  version = u32[1];
  
  if (magic != sstv2::kMagic) {
    return Status::Corruption("ZoneColumnarHeader", "bad magic");
  }
  
  const uint64_t* u64 = reinterpret_cast<const uint64_t*>(buf + 8);
  file_size = u64[0];
  min_entity_id = u64[1];
  max_entity_id = u64[2];
  min_timestamp = u64[3];
  max_timestamp = u64[4];
  
  u32 = reinterpret_cast<const uint32_t*>(buf + 48);
  column_id = u32[0];
  
  entity_type = buf[52];
  
  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneColumnarFooter
// =============================================================================

void ZoneColumnarFooter::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  std::memset(buf, 0, kEncodedSize);
  
  uint32_t* u32 = reinterpret_cast<uint32_t*>(buf);
  u32[0] = block_index_offset;
  u32[1] = block_index_size;
  u32[2] = bloom_filter_offset;
  u32[3] = bloom_filter_size;
  
  uint64_t* u64 = reinterpret_cast<uint64_t*>(buf + 16);
  u64[0] = row_count;
  
  u32 = reinterpret_cast<uint32_t*>(buf + 24);
  u32[0] = block_count;
  u32[1] = footer_magic;
  u32[2] = temporal_filter_offset;
  u32[3] = temporal_filter_size;
  u32[4] = reserved;
  
  u64 = reinterpret_cast<uint64_t*>(buf + 44);
  u64[0] = data_checksum;
  
  dst->append(buf, kEncodedSize);
}

Status ZoneColumnarFooter::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneColumnarFooter", "too short");
  }
  
  const char* buf = input->data();
  const uint32_t* u32 = reinterpret_cast<const uint32_t*>(buf);
  block_index_offset = u32[0];
  block_index_size = u32[1];
  bloom_filter_offset = u32[2];
  bloom_filter_size = u32[3];
  
  const uint64_t* u64 = reinterpret_cast<const uint64_t*>(buf + 16);
  row_count = u64[0];
  
  u32 = reinterpret_cast<const uint32_t*>(buf + 24);
  block_count = u32[0];
  footer_magic = u32[1];
  temporal_filter_offset = u32[2];
  temporal_filter_size = u32[3];
  reserved = u32[4];
  
  u64 = reinterpret_cast<const uint64_t*>(buf + 44);
  data_checksum = u64[0];
  
  if (footer_magic != kFooterMagic) {
    return Status::Corruption("ZoneColumnarFooter", "bad magic");
  }
  
  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

// =============================================================================
// ZoneRestartPoint
// =============================================================================

void ZoneRestartPoint::EncodeTo(std::string* dst) const {
  char buf[kEncodedSize];
  std::memset(buf, 0, kEncodedSize);
  
  uint64_t* u64 = reinterpret_cast<uint64_t*>(buf);
  u64[0] = entity_id;
  
  uint32_t* u32 = reinterpret_cast<uint32_t*>(buf + 8);
  u32[0] = timestamp_hi;
  u32[1] = row_index;
  
  dst->append(buf, kEncodedSize);
}

Status ZoneRestartPoint::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedSize) {
    return Status::Corruption("ZoneRestartPoint", "too short");
  }
  
  const char* buf = input->data();
  const uint64_t* u64 = reinterpret_cast<const uint64_t*>(buf);
  entity_id = u64[0];
  
  const uint32_t* u32 = reinterpret_cast<const uint32_t*>(buf + 8);
  timestamp_hi = u32[0];
  row_index = u32[1];
  
  input->remove_prefix(kEncodedSize);
  return Status::OK();
}

}  // namespace cedar
