// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#ifndef CEDAR_PARTITION_MTH_OP_TYPE_H_
#define CEDAR_PARTITION_MTH_OP_TYPE_H_

#include <cstdint>

namespace cedar {
namespace partition {

struct OpType {
  static constexpr uint8_t kCreate = 0x00;
  static constexpr uint8_t kUpdate = 0x01;
  static constexpr uint8_t kDelete = 0x02;
};

struct KeyFlags {
  static constexpr uint8_t kOpTypeMask = 0x03;
  static constexpr uint8_t kIsDistributed = 1 << 2;
  static constexpr uint8_t kHasVInline = 1 << 3;
  static constexpr uint8_t kIsCompressed = 1 << 4;
  static constexpr uint8_t kIsLocked = 1 << 5;
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_MTH_OP_TYPE_H_
