#pragma once

#include <cstdint>

namespace cedar {
namespace gcn {

enum class EdgeOp : uint8_t {
  kCreate = 0,
  kDelete = 1,
};

struct alignas(32) TMVEdge {
  uint64_t target_id;
  uint32_t valid_from;
  uint32_t valid_to;
  uint64_t attr_offset;
  uint32_t edge_type;
  uint32_t reserved;
};

static_assert(sizeof(TMVEdge) == 32, "TMVEdge must be exactly 32 bytes");
static_assert(alignof(TMVEdge) == 32, "TMVEdge must be 32-byte aligned");

}  // namespace gcn
}  // namespace cedar
