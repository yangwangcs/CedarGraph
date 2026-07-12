// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_GCN_TMV_SNAPSHOT_STORE_H_
#define CEDAR_GCN_TMV_SNAPSHOT_STORE_H_

#include <cstdint>
#include <string>

#include "cedar/core/status.h"
#include "cedar/gcn/tmv_engine.h"

namespace cedar::gcn {

struct TmvSnapshotMetadata {
  uint32_t partition_id = 0;
  uint64_t applied_version = 0;
  uint64_t applied_offset = 0;
  uint64_t edge_count = 0;
};

class TmvSnapshotStore {
 public:
  explicit TmvSnapshotStore(std::string directory);

  Status SavePartition(const TMVEngine& engine,
                       uint32_t partition_id,
                       uint64_t applied_version,
                       uint64_t applied_offset) const;

  StatusOr<TmvSnapshotMetadata> RestorePartition(
      TMVEngine* engine,
      uint32_t partition_id) const;

  std::string SnapshotPathForTest(uint32_t partition_id) const;

 private:
  std::string SnapshotPath(uint32_t partition_id) const;
  std::string TempPath(uint32_t partition_id) const;

  std::string directory_;
};

}  // namespace cedar::gcn

#endif  // CEDAR_GCN_TMV_SNAPSHOT_STORE_H_
