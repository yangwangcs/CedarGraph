// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

// =============================================================================
// Storage Partition braft State Machine
// =============================================================================
// Each partition has its own braft::StateMachine that applies committed
// log entries to the local LSM-Tree via PartitionStorage.
//
// Log entry format:
//   [type:1][key:32][descriptor_raw:8][txn_version:8]  = 49 bytes per entry
// =============================================================================

#ifndef CEDAR_DTX_STORAGE_BRAFT_PARTITION_STATE_MACHINE_H_
#define CEDAR_DTX_STORAGE_BRAFT_PARTITION_STATE_MACHINE_H_

#include <braft/raft.h>
#include <braft/storage.h>

#include <memory>
#include <string>

#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {
namespace storage {

// Forward declaration
class PartitionStorage;

// =============================================================================
// Serialized Raft Command for Storage Layer
// =============================================================================

struct StorageRaftCommand {
  enum class Type : uint8_t {
    kPut = 0,
    kDelete = 1,
  };

  Type type = Type::kPut;
  CedarKey key;
  Descriptor descriptor;
  Timestamp txn_version;

  std::string Serialize() const;
  static StatusOr<StorageRaftCommand> Deserialize(const std::string& data);
};

// =============================================================================
// braft State Machine for a Single Partition
// =============================================================================

class PartitionRaftStateMachine : public braft::StateMachine {
 public:
  explicit PartitionRaftStateMachine(PartitionStorage* storage);
  ~PartitionRaftStateMachine() override = default;

  // Apply committed Raft log entries to LSM-Tree
  void on_apply(braft::Iterator& iter) override;

  // Snapshot support
  void on_snapshot_save(braft::SnapshotWriter* writer,
                        braft::Closure* done) override;
  int on_snapshot_load(braft::SnapshotReader* reader) override;

  // Leadership changes
  void on_leader_start(int64_t term) override;
  void on_leader_stop(const butil::Status& status) override;
  void on_shutdown() override;
  void on_error(const braft::Error& e) override;
  void on_configuration_committed(const braft::Configuration& conf) override;
  void on_stop_following(const braft::LeaderChangeContext& ctx) override;
  void on_start_following(const braft::LeaderChangeContext& ctx) override;

  // Get last applied index (for read-index)
  int64_t last_applied_index() const { return last_applied_index_.load(); }

 private:
  PartitionStorage* storage_;
  std::atomic<int64_t> last_applied_index_{0};
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_BRAFT_PARTITION_STATE_MACHINE_H_
