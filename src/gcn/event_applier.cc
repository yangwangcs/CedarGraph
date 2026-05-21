#include "cedar/gcn/event_applier.h"

#include <limits>

#include "cedar/core/logging.h"

namespace cedar {
namespace gcn {

EventApplier::EventApplier(TMVEngine* engine) : tmv_engine_(engine) {}

cedar::Status EventApplier::ApplyOrdered(const GraphCDCEvent& event) {
  cedar::Status s = ApplyInternal(event);
  if (s.ok()) {
    applied_version_ = event.commit_version;
  }
  return s;
}

cedar::Status EventApplier::ApplyUnordered(const GraphCDCEvent& event) {
  if (event.commit_version == applied_version_ + 1) {
    cedar::Status s = ApplyInternal(event);
    if (!s.ok()) {
      return s;
    }
    applied_version_ = event.commit_version;
    DrainBuffer();
  } else {
    if (reorder_buffer_.size() >= kMaxReorderBuffer) {
      CEDAR_LOG_ERROR() << "EventApplier reorder buffer overflow (size="
                        << reorder_buffer_.size() << ", max=" << kMaxReorderBuffer << ")\n";
      return cedar::Status::ResourceExhausted("Reorder buffer full");
    }
    reorder_buffer_[event.commit_version] = event;
  }
  return cedar::Status::OK();
}

cedar::Status EventApplier::ApplyInternal(const GraphCDCEvent& event) {
  TMVEdge edge;
  edge.target_id = event.target_id;
  edge.attr_offset = 0;
  edge.edge_type = event.edge_type;
  edge.reserved = 0;

  if (event.op == CDCEventOp::kCreate) {
    edge.valid_from = event.valid_from;
    edge.valid_to = std::numeric_limits<uint32_t>::max();
  } else {  // kDelete
    edge.valid_from = event.valid_from;
    edge.valid_to = event.valid_from;
  }

  if (tmv_engine_) {
    return tmv_engine_->AppendEdge(event.entity_id, Direction::kOut, edge, true);
  }
  return cedar::Status::OK();
}

void EventApplier::DrainBuffer() {
  while (!reorder_buffer_.empty()) {
    auto it = reorder_buffer_.begin();
    if (it->first == applied_version_ + 1) {
      cedar::Status s = ApplyInternal(it->second);
      if (!s.ok()) {
        CEDAR_LOG_ERROR() << "EventApplier failed to drain buffered event version "
                          << it->first << ": " << s.ToString() << "\n";
        break;
      }
      applied_version_ = it->first;
      reorder_buffer_.erase(it);
    } else {
      break;
    }
  }
}

}  // namespace gcn
}  // namespace cedar
