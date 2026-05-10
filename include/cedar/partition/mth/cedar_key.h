// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#ifndef CEDAR_PARTITION_MTH_CEDAR_KEY_H_
#define CEDAR_PARTITION_MTH_CEDAR_KEY_H_

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace cedar {
namespace partition {

#pragma pack(push, 1)
struct CedarKey {
  uint64_t entity_id;
  uint64_t timestamp_be;
  uint64_t target_id;
  uint16_t column_id;
  uint16_t sequence;
  uint8_t  entity_type;
  uint8_t  flags;
  uint16_t part_id;

  static uint64_t EncodeTimestamp(uint64_t micros);
  static uint64_t DecodeTimestamp(uint64_t be);

  static CedarKey Vertex(uint64_t vertex_id, uint16_t col,
                         uint64_t ts_micros, uint16_t seq = 0,
                         uint16_t part_id = 0, uint64_t extension = 0,
                         uint8_t flags = 0);
  static CedarKey EdgeOut(uint64_t src_id, uint64_t dst_id,
                          uint16_t edge_type, uint64_t ts_micros,
                          uint16_t seq = 0, uint16_t part_id = 0,
                          uint8_t flags = 0);
  static CedarKey EdgeIn(uint64_t dst_id, uint64_t src_id,
                         uint16_t edge_type, uint64_t ts_micros,
                         uint16_t seq = 0, uint16_t part_id = 0,
                         uint8_t flags = 0);

  CedarKey WithFlags(uint8_t new_flags) const;
  CedarKey WithPartId(uint16_t new_part_id) const;

  std::string Encode() const;
  static std::optional<CedarKey> Decode(std::string_view data);
};
#pragma pack(pop)

static_assert(sizeof(CedarKey) == 32, "CedarKey must be 32 bytes");

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_MTH_CEDAR_KEY_H_
