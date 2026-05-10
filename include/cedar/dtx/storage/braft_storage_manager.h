// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

// =============================================================================
// Storage braft Manager - One Raft Group Per Partition
// =============================================================================
// Manages braft::Node instances for each partition on this storage node.
// Uses third_party/braft for consensus and third_party/brpc for internode RPC.
// =============================================================================

#ifndef CEDAR_DTX_STORAGE_BRAFT_STORAGE_MANAGER_H_
#define CEDAR_DTX_STORAGE_BRAFT_STORAGE_MANAGER_H_

#include <braft/raft.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/storage/braft_partition_state_machine.h"

namespace cedar {
namespace dtx {
namespace storage {

// Forward declaration
class PartitionStorage;
class StoragePartitionManager;

// =============================================================================
// Per-Partition Raft Group Info
// =============================================================================

struct PartitionRaftGroup {
  PartitionID partition_id;
  std::unique_ptr<braft::Node> node;
  std::unique_ptr<PartitionRaftStateMachine> state_machine;
  std::string data_path;
};

// =============================================================================
// Storage Braft Manager
// =============================================================================

class StorageBraftManager {
 public:
  struct Config {
    std::string base_data_dir;                        // e.g. /data/storaged
    std::string listen_address;                       // e.g. 0.0.0.0:9779
    uint64_t election_timeout_ms = 1000;
    uint64_t snapshot_interval_s = 3600;
  };

  StorageBraftManager();
  ~StorageBraftManager();

  // Initialize manager (does not create any groups yet)
  Status Initialize(const Config& config, StoragePartitionManager* partition_manager);
  void Shutdown();

  // Create a braft group for a partition.
  // peers: list of (node_id, address) for all replicas of this partition.
  Status CreatePartitionGroup(PartitionID pid,
                               const std::vector<std::pair<NodeID, std::string>>& peers);

  // Get the braft node for a partition (nullptr if not created)
  braft::Node* GetPartitionNode(PartitionID pid);

  // Check if this node is the leader for a partition
  bool IsPartitionLeader(PartitionID pid);

  // Get leader address for a partition
  std::optional<std::string> GetPartitionLeaderAddress(PartitionID pid);

  // Propose a command to the partition's Raft group.
  // Returns OK if the command was successfully proposed and committed.
  // Returns NotLeader if this node is not the leader.
  Status Propose(PartitionID pid, const StorageRaftCommand& cmd);

  // Remove a partition group
  void RemovePartitionGroup(PartitionID pid);

  // Get all managed partition IDs
  std::vector<PartitionID> GetAllPartitionIDs() const;

 private:
  Config config_;
  StoragePartitionManager* partition_manager_{nullptr};
  std::atomic<bool> initialized_{false};

  mutable std::shared_mutex groups_mutex_;
  std::unordered_map<PartitionID, std::unique_ptr<PartitionRaftGroup>> groups_;
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_BRAFT_STORAGE_MANAGER_H_
