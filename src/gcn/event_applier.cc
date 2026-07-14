#include "cedar/gcn/event_applier.h"

#include <chrono>
#include <iostream>
#include <limits>
#include <thread>

namespace cedar {
namespace gcn {
namespace {

uint32_t ClampValidTime(uint64_t value) {
  return value > std::numeric_limits<uint32_t>::max()
             ? std::numeric_limits<uint32_t>::max()
             : static_cast<uint32_t>(value);
}

}  // namespace

EventApplier::EventApplier(TMVEngine* engine, size_t max_reorder_buffer)
    : tmv_engine_(engine), max_reorder_buffer_(max_reorder_buffer) {}

cedar::Status EventApplier::ApplyOrdered(const GraphCDCEvent& event) {
  std::lock_guard<std::mutex> lock(mutex_);
  cedar::Status s = ApplyInternal(event);
  if (s.ok()) {
    applied_version_ = event.commit_version;
  }
  return s;
}

cedar::Status EventApplier::ApplyUnordered(const GraphCDCEvent& event) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (event.commit_version == applied_version_ + 1) {
    cedar::Status s = ApplyInternal(event);
    if (!s.ok()) {
      return s;
    }
    applied_version_ = event.commit_version;
    DrainBufferUnlocked();
  } else {
    while (reorder_buffer_.size() >= max_reorder_buffer_ &&
           event.commit_version != applied_version_ + 1) {
      buffer_drained_cv_.wait(lock);
    }
    if (event.commit_version == applied_version_ + 1) {
      cedar::Status s = ApplyInternal(event);
      if (!s.ok()) {
        return s;
      }
      applied_version_ = event.commit_version;
      DrainBufferUnlocked();
      return cedar::Status::OK();
    }
    reorder_buffer_[event.commit_version] = event;
  }
  return cedar::Status::OK();
}

cedar::Status EventApplier::ApplyChangeRecord(
    const cedar::cdc::ChangeRecord& record) {
  if (record.batch_size() == 0 || record.batch_index() >= record.batch_size()) {
    return cedar::Status::InvalidArgument("CDC record has invalid batch metadata");
  }

  PartitionApplyState* state = GetOrCreatePartitionState(record.partition_id());
  std::lock_guard<std::mutex> lock(state->mutex);
  auto& progress = state->progress;
  if (record.offset() <= progress.applied_offset) {
    return cedar::Status::OK();
  }
  if (record.batch_size() == 1) {
    if (record.offset() != progress.applied_offset + 1) {
      return cedar::Status::Conflict("CDC record offset gap");
    }
    CEDAR_RETURN_IF_ERROR(ValidateChangeRecordForApply(record));
    if (tmv_engine_) {
      CEDAR_RETURN_IF_ERROR(tmv_engine_->AppendEdgesAtomic({BuildEdgeAppend(record)}));
    }
    progress.applied_offset = record.offset();
    progress.applied_version = record.commit_version();
    return cedar::Status::OK();
  }

  const uint64_t first_offset = record.offset() - record.batch_index();
  if (first_offset != progress.applied_offset + 1) {
    return cedar::Status::Conflict("CDC record offset gap");
  }

  auto& batch = state->pending_batch;
  if (batch.batch_size == 0) {
    batch.txn_id = record.txn_id();
    batch.partition_id = record.partition_id();
    batch.first_offset = first_offset;
    batch.batch_size = record.batch_size();
    batch.commit_version = record.commit_version();
    batch.records.resize(record.batch_size());
    batch.present.assign(record.batch_size(), false);
  }
  if (batch.txn_id != record.txn_id() ||
      batch.first_offset != first_offset ||
      batch.batch_size != record.batch_size() ||
      batch.commit_version != record.commit_version()) {
    return cedar::Status::InvalidArgument("CDC record batch metadata mismatch");
  }

  const uint32_t index = record.batch_index();
  if (!batch.present[index]) {
    batch.records[index] = record;
    batch.present[index] = true;
  }
  for (bool present : batch.present) {
    if (!present) {
      return cedar::Status::OK();
    }
  }

  cedar::Status status = ApplyCompleteBatch(&batch, &progress);
  state->pending_batch = PendingBatch{};
  if (!status.ok()) {
    return status;
  }
  return cedar::Status::OK();
}

uint64_t EventApplier::AppliedOffset(uint32_t partition_id) const {
  const PartitionApplyState* state = FindPartitionState(partition_id);
  if (!state) {
    return 0;
  }
  std::lock_guard<std::mutex> lock(state->mutex);
  return state->progress.applied_offset;
}

uint64_t EventApplier::AppliedVersion(uint32_t partition_id) const {
  const PartitionApplyState* state = FindPartitionState(partition_id);
  if (!state) {
    return 0;
  }
  std::lock_guard<std::mutex> lock(state->mutex);
  return state->progress.applied_version;
}

cedar::Status EventApplier::SeedPartitionProgress(uint32_t partition_id,
                                                  uint64_t applied_offset,
                                                  uint64_t applied_version) {
  PartitionApplyState* state = GetOrCreatePartitionState(partition_id);
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->pending_batch.batch_size != 0) {
    return cedar::Status::Conflict(
        "cannot seed CDC progress while a batch is pending");
  }
  if (applied_offset < state->progress.applied_offset) {
    return cedar::Status::Conflict("cannot move CDC applied offset backward");
  }
  state->progress.applied_offset = applied_offset;
  state->progress.applied_version = applied_version;
  return cedar::Status::OK();
}

cedar::Status EventApplier::ApplySnapshotRecordsAtomically(
    uint32_t partition_id, uint64_t applied_offset,
    uint64_t applied_version,
    const std::vector<cedar::cdc::ChangeRecord>& records) {
  PartitionApplyState* state = GetOrCreatePartitionState(partition_id);
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->pending_batch.batch_size != 0) {
    return cedar::Status::Conflict(
        "cannot publish snapshot while a batch is pending");
  }
  if (applied_offset < state->progress.applied_offset) {
    return cedar::Status::Conflict("cannot move CDC applied offset backward");
  }

  std::vector<TMVEngine::EdgeAppend> appends;
  appends.reserve(records.size());
  for (const auto& record : records) {
    if (record.partition_id() != partition_id) {
      return cedar::Status::InvalidArgument(
          "snapshot record partition mismatch");
    }
    CEDAR_RETURN_IF_ERROR(ValidateChangeRecordForApply(record));
    appends.push_back(BuildEdgeAppend(record));
  }
  if (tmv_engine_) {
    CEDAR_RETURN_IF_ERROR(
        tmv_engine_->ReplacePartitionEdgesAtomic(partition_id, appends));
  }
  state->progress.applied_offset = applied_offset;
  state->progress.applied_version = applied_version;
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

TMVEngine::EdgeAppend EventApplier::BuildEdgeAppend(
    const cedar::cdc::ChangeRecord& record) {
  TMVEdge edge;
  edge.target_id = record.target_id();
  edge.attr_offset = 0;
  edge.edge_type = record.edge_type();
  edge.reserved = record.partition_id();
  const uint32_t valid_from = record.valid_from() == 0
                                  ? ClampValidTime(record.commit_version())
                                  : ClampValidTime(record.valid_from());
  edge.valid_from = valid_from;
  if (record.operation() == cedar::cdc::CHANGE_OPERATION_DELETE) {
    edge.valid_to = valid_from;
  } else {
    edge.valid_to = record.valid_to() == 0
                        ? std::numeric_limits<uint32_t>::max()
                        : ClampValidTime(record.valid_to());
  }

  return TMVEngine::EdgeAppend{record.entity_id(), Direction::kOut, edge,
                               true};
}

cedar::Status EventApplier::ValidateChangeRecordForApply(
    const cedar::cdc::ChangeRecord& record) const {
  if (record.operation() != cedar::cdc::CHANGE_OPERATION_CREATE &&
      record.operation() != cedar::cdc::CHANGE_OPERATION_UPDATE &&
      record.operation() != cedar::cdc::CHANGE_OPERATION_DELETE) {
    return cedar::Status::InvalidArgument("unsupported CDC operation");
  }
  if (record.batch_size() == 0 || record.batch_index() >= record.batch_size()) {
    return cedar::Status::InvalidArgument("CDC record has invalid batch metadata");
  }
  return cedar::Status::OK();
}

EventApplier::PartitionApplyState* EventApplier::GetOrCreatePartitionState(
    uint32_t partition_id) {
  {
    std::shared_lock<std::shared_mutex> read_lock(partition_states_mutex_);
    auto it = partition_states_.find(partition_id);
    if (it != partition_states_.end()) {
      return it->second.get();
    }
  }

  std::unique_lock<std::shared_mutex> write_lock(partition_states_mutex_);
  auto& state = partition_states_[partition_id];
  if (!state) {
    state = std::make_unique<PartitionApplyState>();
  }
  return state.get();
}

const EventApplier::PartitionApplyState* EventApplier::FindPartitionState(
    uint32_t partition_id) const {
  std::shared_lock<std::shared_mutex> read_lock(partition_states_mutex_);
  auto it = partition_states_.find(partition_id);
  return it == partition_states_.end() ? nullptr : it->second.get();
}

cedar::Status EventApplier::ApplyCompleteBatch(PendingBatch* batch,
                                               PartitionProgress* progress) {
  std::vector<TMVEngine::EdgeAppend> appends;
  appends.reserve(batch->batch_size);
  for (uint32_t i = 0; i < batch->batch_size; ++i) {
    const auto& record = batch->records[i];
    if (record.offset() != batch->first_offset + i ||
        record.batch_index() != i ||
        record.partition_id() != batch->partition_id ||
        record.txn_id() != batch->txn_id ||
        record.batch_size() != batch->batch_size ||
        record.commit_version() != batch->commit_version) {
      return cedar::Status::InvalidArgument("CDC batch is not contiguous");
    }
    CEDAR_RETURN_IF_ERROR(ValidateChangeRecordForApply(record));
    appends.push_back(BuildEdgeAppend(record));
  }

  if (tmv_engine_) {
    CEDAR_RETURN_IF_ERROR(tmv_engine_->AppendEdgesAtomic(appends));
  }
  progress->applied_offset =
      batch->first_offset + static_cast<uint64_t>(batch->batch_size) - 1;
  progress->applied_version = batch->commit_version;
  return cedar::Status::OK();
}

void EventApplier::DrainBufferUnlocked() {
  while (!reorder_buffer_.empty()) {
    auto it = reorder_buffer_.begin();
    if (it->first == applied_version_ + 1) {
      cedar::Status s = ApplyInternal(it->second);
      if (!s.ok()) {
        std::cerr << "EventApplier failed to drain buffered event version "
                  << it->first << ": " << s.ToString() << "\n";
        break;
      }
      applied_version_ = it->first;
      reorder_buffer_.erase(it);
      buffer_drained_cv_.notify_all();
    } else {
      break;
    }
  }
}

}  // namespace gcn
}  // namespace cedar
