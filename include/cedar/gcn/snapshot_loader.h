// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_GCN_SNAPSHOT_LOADER_H_
#define CEDAR_GCN_SNAPSHOT_LOADER_H_

#include <cstdint>
#include <string>

#include "cedar/core/status.h"
#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/storage_cdc_client.h"
#include "cedar/gcn/tmv_engine.h"

namespace cedar::gcn {

struct SnapshotLoadResult {
  uint32_t partition_id = 0;
  uint64_t partition_epoch = 0;
  uint64_t applied_offset = 0;
  uint64_t applied_version = 0;
  std::string snapshot_id;
};

class SnapshotLoader {
 public:
  struct Options {
    size_t temp_tmv_chunks = 1024;
  };

  SnapshotLoader(StorageCdcSource* source, TMVEngine* live_engine,
                 EventApplier* live_applier, Options options);

  StatusOr<SnapshotLoadResult> Load(uint32_t partition_id,
                                    uint64_t expected_epoch,
                                    uint64_t snapshot_version,
                                    uint64_t high_watermark);

 private:
  StorageCdcSource* source_;
  TMVEngine* live_engine_;
  EventApplier* live_applier_;
  Options options_;
};

}  // namespace cedar::gcn

#endif  // CEDAR_GCN_SNAPSHOT_LOADER_H_
