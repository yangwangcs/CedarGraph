// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
// 
// Ported from subgraph2

#ifndef CEDAR_PARTITION_MTH_ENTITY_TYPE_H_
#define CEDAR_PARTITION_MTH_ENTITY_TYPE_H_

#include <cstdint>

namespace cedar {
namespace partition {

struct EntityType {
  static constexpr uint8_t kVertex = 0;
  static constexpr uint8_t kEdgeOut = 1;
  static constexpr uint8_t kEdgeIn = 2;
};

} // namespace partition
} // namespace cedar

#endif // CEDAR_PARTITION_MTH_ENTITY_TYPE_H_
