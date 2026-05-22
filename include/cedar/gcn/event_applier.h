#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <condition_variable>
#include <mutex>

#include "cedar/core/status.h"
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

/// Applies CDC events to the TMVEngine in commit-order.
/// Thread-safe; buffers out-of-order events until the gap is filled.
class EventApplier {
 public:
  explicit EventApplier(TMVEngine* engine, size_t max_reorder_buffer = 100000);

  // Non-copyable, non-movable
  EventApplier(const EventApplier&) = delete;
  EventApplier& operator=(const EventApplier&) = delete;

  // Apply events that are already in commit_version order.
  cedar::Status ApplyOrdered(const GraphCDCEvent& event);

  // Apply events that may arrive out of order.  Buffers events until
  // commit_version == applied_version_ + 1, then applies them and
  // drains the reorder buffer for any contiguous next versions.
  cedar::Status ApplyUnordered(const GraphCDCEvent& event);

  uint64_t applied_version() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return applied_version_;
  }

 private:
  TMVEngine* tmv_engine_;
  uint64_t applied_version_ = 0;
  std::map<uint64_t, GraphCDCEvent> reorder_buffer_;
  size_t max_reorder_buffer_;
  mutable std::mutex mutex_;
  std::condition_variable buffer_drained_cv_;

  cedar::Status ApplyInternal(const GraphCDCEvent& event);
  void DrainBufferUnlocked();
};

}  // namespace gcn
}  // namespace cedar
