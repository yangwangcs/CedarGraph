#include "cedar/gcn/partition_consumer.h"

#include <algorithm>
#include <optional>

#include "cedar/gcn/snapshot_loader.h"

namespace cedar::gcn {

PartitionConsumer::PartitionConsumer(StorageCdcSource* source,
                                     CheckpointStore* checkpoints,
                                     EventApplier* applier, TMVEngine* engine,
                                     Options options)
    : source_(source),
      checkpoints_(checkpoints),
      applier_(applier),
      engine_(engine),
      options_(options) {}

PartitionConsumer::~PartitionConsumer() {
  Stop(std::chrono::milliseconds(1000)).IgnoreError();
}

Status PartitionConsumer::Start(PartitionLease lease) {
  if (!source_ || !checkpoints_ || !applier_ || !engine_) {
    return Status::InvalidArgument("partition consumer dependencies are missing");
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) {
      return Status::Busy("partition consumer already running");
    }
  }

  auto state_or = source_->GetState(lease.partition_id, lease.partition_epoch);
  if (!state_or.ok()) {
    return state_or.status();
  }
  if (state_or.ValueOrDie().partition_epoch() != lease.partition_epoch) {
    return Status::Conflict("StorageD partition epoch does not match lease");
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = true;
    stop_requested_ = false;
    lease_ = lease;
    progress_ = PartitionConsumerProgress{};
    progress_.partition_id = lease.partition_id;
    progress_.partition_epoch = lease.partition_epoch;
    progress_.lease_epoch = lease.lease_epoch;
    progress_.state = PartitionConsumerState::kStarting;
    progress_.query_ready = false;
  }

  worker_ = std::thread([this, lease] { WorkerLoop(lease); });
  return Status::OK();
}

Status PartitionConsumer::Stop(std::chrono::milliseconds deadline) {
  std::unique_lock<std::mutex> lock(mutex_);
  stop_requested_ = true;
  cv_.notify_all();
  if (source_) {
    source_->Cancel();
  }
  if (running_) {
    if (!cv_.wait_for(lock, deadline, [&] { return !running_; })) {
      return Status::Unavailable("partition consumer stop deadline exceeded");
    }
  }
  std::thread worker = std::move(worker_);
  if (progress_.state != PartitionConsumerState::kFailed) {
    progress_.state = PartitionConsumerState::kStopped;
    progress_.query_ready = false;
  }
  lock.unlock();
  if (worker.joinable()) {
    worker.join();
  }
  return Status::OK();
}

PartitionConsumerProgress PartitionConsumer::GetProgress() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return progress_;
}

void PartitionConsumer::WorkerLoop(PartitionLease lease) {
  std::chrono::milliseconds backoff = options_.initial_backoff;
  while (!StopRequested()) {
    Status status = RunOnce(lease);
    if (!status.ok()) {
      if (status.IsCancelled() && StopRequested()) {
        break;
      }
      if (status.IsUnavailable()) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, backoff, [&] { return stop_requested_; });
        backoff = std::min(options_.max_backoff, backoff * 2);
        continue;
      }
      std::lock_guard<std::mutex> lock(mutex_);
      progress_.state = PartitionConsumerState::kFailed;
      progress_.query_ready = false;
      progress_.error = status.ToString();
      break;
    }
    backoff = options_.initial_backoff;
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, options_.poll_interval, [&] { return stop_requested_; });
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
  }
  cv_.notify_all();
}

Status PartitionConsumer::RunOnce(PartitionLease lease) {
  auto state_or = source_->GetState(lease.partition_id, lease.partition_epoch);
  if (!state_or.ok()) {
    return state_or.status();
  }
  const auto& state = state_or.ValueOrDie();
  if (state.partition_epoch() != lease.partition_epoch) {
    return Status::Conflict("StorageD partition epoch does not match lease");
  }
  const bool was_query_ready = GetProgress().query_ready;
  PartitionLease observed_lease = lease;
  observed_lease.partition_epoch = state.partition_epoch();
  if (!was_query_ready) {
    SetProgress(PartitionConsumerState::kStarting, observed_lease, 0, 0,
                state.high_watermark(), false);
  }

  auto checkpoint_or = checkpoints_->Load(lease.partition_id);
  std::optional<PartitionCheckpoint> checkpoint;
  bool needs_snapshot = false;
  if (!checkpoint_or.ok()) {
    needs_snapshot = true;
  } else {
    checkpoint = checkpoint_or.ValueOrDie();
  }

  uint64_t after_offset = 0;
  uint64_t applied_version = 0;
  if (checkpoint.has_value() &&
      checkpoint->partition_epoch == state.partition_epoch() &&
      checkpoint->applied_offset + 1 >= state.earliest_offset()) {
    after_offset = checkpoint->applied_offset;
    applied_version = checkpoint->applied_version;
    if (applier_->AppliedOffset(lease.partition_id) != after_offset ||
        applier_->AppliedVersion(lease.partition_id) != applied_version) {
      CEDAR_RETURN_IF_ERROR(applier_->SeedPartitionProgress(
          lease.partition_id, after_offset, applied_version));
    }
  } else {
    needs_snapshot = true;
  }

  if (needs_snapshot) {
    SetProgress(PartitionConsumerState::kBackfilling, observed_lease, 0, 0,
                state.high_watermark(), false);
    SnapshotLoader loader(source_, engine_, applier_,
                          SnapshotLoader::Options{options_.temp_snapshot_chunks});
    auto snapshot_or =
        loader.Load(lease.partition_id, state.partition_epoch(),
                    state.committed_version(), state.high_watermark());
    if (!snapshot_or.ok()) {
      return snapshot_or.status();
    }
    const auto& snapshot = snapshot_or.ValueOrDie();
    CEDAR_RETURN_IF_ERROR(checkpoints_->Save(
        {lease.partition_id, state.partition_epoch(), snapshot.applied_offset,
         snapshot.applied_version, snapshot.snapshot_id}));
    after_offset = snapshot.applied_offset;
    applied_version = snapshot.applied_version;
  }

  if (!was_query_ready || after_offset < state.high_watermark()) {
    SetProgress(PartitionConsumerState::kCatchingUp, observed_lease,
                after_offset, applied_version, state.high_watermark(), false);
  }

  uint64_t high_watermark = state.high_watermark();
  uint64_t fetch_after_offset = after_offset;
  while (!StopRequested() && fetch_after_offset < high_watermark) {
    auto fetch_or =
        source_->Fetch(lease.partition_id, fetch_after_offset,
                       state.partition_epoch());
    if (!fetch_or.ok()) {
      return fetch_or.status();
    }
    const auto& response = fetch_or.ValueOrDie();
    if (response.partition_id() != lease.partition_id) {
      return Status::Conflict("CDC response partition changed");
    }
    if (response.partition_epoch() != state.partition_epoch()) {
      return Status::Conflict("CDC response epoch changed");
    }
    high_watermark = std::max(high_watermark, response.high_watermark());
    if (response.records().empty()) {
      return Status::Unavailable("CDC fetch returned no records below high watermark");
    }
    if (response.next_offset() <= fetch_after_offset) {
      return Status::InvalidArgument("CDC response did not advance next offset");
    }
    for (const auto& record : response.records()) {
      if (record.partition_id() != lease.partition_id) {
        return Status::Conflict("CDC record partition changed");
      }
      if (record.partition_epoch() != state.partition_epoch()) {
        return Status::Conflict("CDC record epoch changed");
      }
      CEDAR_RETURN_IF_ERROR(applier_->ApplyChangeRecord(record));
      const uint64_t durable_offset = applier_->AppliedOffset(lease.partition_id);
      const uint64_t durable_version =
          applier_->AppliedVersion(lease.partition_id);
      if (durable_offset > after_offset) {
        after_offset = durable_offset;
        applied_version = durable_version;
        CEDAR_RETURN_IF_ERROR(checkpoints_->Save(
            {lease.partition_id, state.partition_epoch(), after_offset,
             applied_version, ""}));
      }
    }
    fetch_after_offset = response.next_offset();
    SetProgress(PartitionConsumerState::kCatchingUp, observed_lease,
                after_offset, applied_version, high_watermark,
                false);
  }

  SetProgress(PartitionConsumerState::kReady, observed_lease, after_offset,
              applied_version, high_watermark, true);
  return Status::OK();
}

void PartitionConsumer::SetProgress(PartitionConsumerState state,
                                    PartitionLease lease,
                                    uint64_t applied_offset,
                                    uint64_t applied_version,
                                    uint64_t high_watermark,
                                    bool query_ready,
                                    std::string error) {
  std::lock_guard<std::mutex> lock(mutex_);
  progress_.partition_id = lease.partition_id;
  progress_.partition_epoch = lease.partition_epoch;
  progress_.lease_epoch = lease.lease_epoch;
  progress_.applied_offset = applied_offset;
  progress_.applied_version = applied_version;
  progress_.high_watermark = std::max(progress_.high_watermark, high_watermark);
  progress_.state = state;
  progress_.query_ready = query_ready;
  progress_.error = std::move(error);
}

bool PartitionConsumer::StopRequested() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stop_requested_;
}

}  // namespace cedar::gcn
