// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_GCN_PARTITION_CONSUMER_H_
#define CEDAR_GCN_PARTITION_CONSUMER_H_

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include "cedar/core/status.h"
#include "cedar/gcn/checkpoint_store.h"
#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/storage_cdc_client.h"
#include "cedar/gcn/tmv_engine.h"
#include "cedar/gcn/tmv_snapshot_store.h"

namespace cedar::gcn {

struct PartitionLease {
  uint32_t partition_id = 0;
  uint64_t partition_epoch = 0;
  uint64_t lease_epoch = 0;
};

enum class PartitionConsumerState {
  kStarting,
  kBackfilling,
  kCatchingUp,
  kReady,
  kFailed,
  kStopped,
};

struct PartitionConsumerProgress {
  uint32_t partition_id = 0;
  uint64_t partition_epoch = 0;
  uint64_t lease_epoch = 0;
  uint64_t applied_offset = 0;
  uint64_t applied_version = 0;
  uint64_t high_watermark = 0;
  PartitionConsumerState state = PartitionConsumerState::kStopped;
  bool query_ready = false;
  std::string error;
};

class PartitionConsumer {
 public:
  struct Options {
    std::chrono::milliseconds poll_interval{50};
    std::chrono::milliseconds initial_backoff{10};
    std::chrono::milliseconds max_backoff{1000};
    size_t temp_snapshot_chunks = 1024;
  };

  PartitionConsumer(StorageCdcSource* source, CheckpointStore* checkpoints,
                    EventApplier* applier, TMVEngine* engine,
                    Options options,
                    TmvSnapshotStore* snapshot_store = nullptr);
  ~PartitionConsumer();

  PartitionConsumer(const PartitionConsumer&) = delete;
  PartitionConsumer& operator=(const PartitionConsumer&) = delete;

  Status Start(PartitionLease lease);
  Status Stop(std::chrono::milliseconds deadline);
  PartitionConsumerProgress GetProgress() const;

 private:
  Status RunOnce(PartitionLease lease);
  void WorkerLoop(PartitionLease lease);
  void SetProgress(PartitionConsumerState state, PartitionLease lease,
                   uint64_t applied_offset, uint64_t applied_version,
                   uint64_t high_watermark, bool query_ready,
                   std::string error = "");
  bool StopRequested() const;

  StorageCdcSource* source_;
  CheckpointStore* checkpoints_;
  EventApplier* applier_;
  TMVEngine* engine_;
  Options options_;
  TmvSnapshotStore* snapshot_store_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  PartitionLease lease_;
  PartitionConsumerProgress progress_;
  bool running_ = false;
  bool stop_requested_ = false;
  std::thread worker_;
};

}  // namespace cedar::gcn

#endif  // CEDAR_GCN_PARTITION_CONSUMER_H_
