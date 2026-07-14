// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_GCN_CHECKPOINT_STORE_H_
#define CEDAR_GCN_CHECKPOINT_STORE_H_

#include <cstdint>
#include <optional>
#include <string>

#include "cedar/core/status.h"

namespace cedar::gcn {

struct PartitionCheckpoint {
  uint32_t partition_id = 0;
  uint64_t partition_epoch = 0;
  uint64_t applied_offset = 0;
  uint64_t applied_version = 0;
  std::string tmv_snapshot_id;
};

class CheckpointStore {
 public:
  explicit CheckpointStore(std::string directory);

  StatusOr<std::optional<PartitionCheckpoint>> Load(
      uint32_t partition_id) const;
  Status Save(const PartitionCheckpoint& checkpoint);
  Status Remove(uint32_t partition_id);

 private:
  std::string CheckpointPath(uint32_t partition_id) const;
  std::string TempPath(uint32_t partition_id) const;

  std::string directory_;
};

}  // namespace cedar::gcn

#endif  // CEDAR_GCN_CHECKPOINT_STORE_H_
