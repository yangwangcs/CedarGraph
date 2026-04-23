#include "cedar/gcn/event_applier.h"

#include <limits>

namespace cedar {
namespace gcn {

EventApplier::EventApplier(TMVEngine* engine) : tmv_engine_(engine) {}

void EventApplier::ApplyOrdered(const GraphCDCEvent& event) {
  ApplyInternal(event);
  applied_version_ = event.commit_version;
}

void EventApplier::ApplyUnordered(const GraphCDCEvent& event) {
  if (event.commit_version == applied_version_ + 1) {
    ApplyInternal(event);
    applied_version_ = event.commit_version;
    DrainBuffer();
  } else {
    reorder_buffer_[event.commit_version] = event;
  }
}

void EventApplier::ApplyInternal(const GraphCDCEvent& event) {
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

  tmv_engine_->AppendEdge(event.entity_id, Direction::kOut, edge, true);
}

void EventApplier::DrainBuffer() {
  while (!reorder_buffer_.empty()) {
    auto it = reorder_buffer_.begin();
    if (it->first == applied_version_ + 1) {
      ApplyInternal(it->second);
      applied_version_ = it->first;
      reorder_buffer_.erase(it);
    } else {
      break;
    }
  }
}

}  // namespace gcn
}  // namespace cedar
