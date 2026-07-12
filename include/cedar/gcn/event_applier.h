#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/gcn/tmv_engine.h"
#include "cdc_service.pb.h"

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

  // Apply a durable StorageD CDC record exactly once per partition offset.
  // Duplicate offsets at or below the applied offset are no-ops; gaps are
  // rejected so consumers can re-fetch from the durable checkpoint.
  cedar::Status ApplyChangeRecord(const cedar::cdc::ChangeRecord& record);

  uint64_t AppliedOffset(uint32_t partition_id) const;
  uint64_t AppliedVersion(uint32_t partition_id) const;

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
  struct PartitionProgress {
    uint64_t applied_offset = 0;
    uint64_t applied_version = 0;
  };
  struct PendingBatch {
    uint64_t txn_id = 0;
    uint32_t partition_id = 0;
    uint64_t first_offset = 0;
    uint32_t batch_size = 0;
    uint64_t commit_version = 0;
    std::vector<cedar::cdc::ChangeRecord> records;
    std::vector<bool> present;
  };
  struct PartitionApplyState {
    mutable std::mutex mutex;
    PartitionProgress progress;
    PendingBatch pending_batch;
  };
  mutable std::shared_mutex partition_states_mutex_;
  std::unordered_map<uint32_t, std::unique_ptr<PartitionApplyState>>
      partition_states_;

  cedar::Status ApplyInternal(const GraphCDCEvent& event);
  TMVEngine::EdgeAppend BuildEdgeAppend(const cedar::cdc::ChangeRecord& record);
  cedar::Status ValidateChangeRecordForApply(
      const cedar::cdc::ChangeRecord& record) const;
  PartitionApplyState* GetOrCreatePartitionState(uint32_t partition_id);
  const PartitionApplyState* FindPartitionState(uint32_t partition_id) const;
  cedar::Status ApplyCompleteBatch(PendingBatch* batch,
                                   PartitionProgress* progress);
  void DrainBufferUnlocked();
};

}  // namespace gcn
}  // namespace cedar
