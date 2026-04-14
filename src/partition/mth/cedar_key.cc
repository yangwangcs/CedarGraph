// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#include "partition/mth/cedar_key.h"
#include "partition/mth/op_type.h"
#include <cstring>

namespace cedar {
namespace partition {

uint64_t CedarKey::EncodeTimestamp(uint64_t micros) {
  // Big-endian encoding for lexicographical ordering
  return __builtin_bswap64(micros);
}

uint64_t CedarKey::DecodeTimestamp(uint64_t be) {
  return __builtin_bswap64(be);
}

CedarKey CedarKey::Vertex(uint64_t vertex_id, uint16_t col,
                           uint64_t ts_micros, uint16_t seq,
                           uint16_t part_id, uint64_t extension,
                           uint8_t flags) {
  (void)extension;  // Reserved for future use
  CedarKey key;
  key.entity_id = vertex_id;
  key.timestamp_be = EncodeTimestamp(ts_micros);
  key.target_id = 0;
  key.column_id = col;
  key.sequence = seq;
  key.entity_type = 0;  // kVertex
  key.flags = flags;
  key.part_id = part_id;
  return key;
}

CedarKey CedarKey::EdgeOut(uint64_t src_id, uint64_t dst_id,
                            uint16_t edge_type, uint64_t ts_micros,
                            uint16_t seq, uint16_t part_id,
                            uint8_t flags) {
  CedarKey key;
  key.entity_id = src_id;
  key.timestamp_be = EncodeTimestamp(ts_micros);
  key.target_id = dst_id;
  key.column_id = edge_type;
  key.sequence = seq;
  key.entity_type = 1;  // kEdgeOut
  key.flags = flags;
  key.part_id = part_id;
  return key;
}

CedarKey CedarKey::EdgeIn(uint64_t dst_id, uint64_t src_id,
                           uint16_t edge_type, uint64_t ts_micros,
                           uint16_t seq, uint16_t part_id,
                           uint8_t flags) {
  CedarKey key;
  key.entity_id = dst_id;
  key.timestamp_be = EncodeTimestamp(ts_micros);
  key.target_id = src_id;
  key.column_id = edge_type;
  key.sequence = seq;
  key.entity_type = 2;  // kEdgeIn
  key.flags = flags;
  key.part_id = part_id;
  return key;
}

CedarKey CedarKey::WithFlags(uint8_t new_flags) const {
  CedarKey copy = *this;
  copy.flags = new_flags;
  return copy;
}

CedarKey CedarKey::WithPartId(uint16_t new_part_id) const {
  CedarKey copy = *this;
  copy.part_id = new_part_id;
  return copy;
}

std::string CedarKey::Encode() const {
  return std::string(reinterpret_cast<const char*>(this), sizeof(CedarKey));
}

std::optional<CedarKey> CedarKey::Decode(std::string_view data) {
  if (data.size() != sizeof(CedarKey)) {
    return std::nullopt;
  }
  CedarKey key;
  std::memcpy(&key, data.data(), sizeof(CedarKey));
  return key;
}

} // namespace partition
} // namespace cedar
