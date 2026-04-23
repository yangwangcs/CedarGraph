#pragma once

#include <cstdint>
#include <map>
#include <memory>

#include "cedar/gcn/tmv_engine.h"

namespace cedar {
namespace gcn {

enum class CDCEventOp : uint8_t {
  kCreate = 0,
  kDelete = 1,
};

struct GraphCDCEvent {
  uint64_t commit_version;
  uint64_t entity_id;
  uint64_t target_id;
  uint32_t valid_from;
  uint32_t valid_to;
  uint32_t edge_type;
  CDCEventOp op;
};

class EventApplier {
 public:
  explicit EventApplier(TMVEngine* engine);

  // Non-copyable, non-movable
  EventApplier(const EventApplier&) = delete;
  EventApplier& operator=(const EventApplier&) = delete;

  // Apply events that are already in commit_version order.
  void ApplyOrdered(const GraphCDCEvent& event);

  // Apply events that may arrive out of order.  Buffers events until
  // commit_version == applied_version_ + 1, then applies them and
  // drains the reorder buffer for any contiguous next versions.
  void ApplyUnordered(const GraphCDCEvent& event);

  uint64_t applied_version() const { return applied_version_; }

 private:
  TMVEngine* tmv_engine_;
  uint64_t applied_version_ = 0;
  std::map<uint64_t, GraphCDCEvent> reorder_buffer_;

  void ApplyInternal(const GraphCDCEvent& event);
  void DrainBuffer();
};

}  // namespace gcn
}  // namespace cedar
