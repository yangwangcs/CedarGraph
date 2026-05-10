// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CEDAR_DTX_STORAGE_PARTITION_RAFT_MANAGER_H_
#define CEDAR_DTX_STORAGE_PARTITION_RAFT_MANAGER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/storage/braft_partition_raft.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// Forward declaration
class PartitionStorage;

// =============================================================================
// PartitionRaftManager - Manages all partition braft nodes (Multi-Raft)
// Each PartitionID maps to one BraftPartitionNode
// =============================================================================
class PartitionRaftManager {
 public:
  PartitionRaftManager();
  ~PartitionRaftManager();

  Status Initialize(NodeID node_id, const std::string& base_data_dir,
                    const std::string& listen_address);
  void Shutdown();

  Status CreateRaftGroup(PartitionID pid,
                         const std::vector<std::string>& peers,
                         PartitionStorage* storage,
                         int election_timeout_ms = 1000,
                         const std::unordered_map<std::string, NodeID>& peer_node_ids = {});

  BraftPartitionNode* GetRaftGroup(PartitionID pid);
  void RemoveRaftGroup(PartitionID pid);

  std::vector<PartitionID> GetAllPartitionIDs() const;

  struct Stats {
    size_t num_groups;
    size_t num_leaders;
    size_t num_followers;
    size_t num_candidates;
  };
  Stats GetStats() const;

 private:
  NodeID node_id_{0};
  std::string base_data_dir_;
  std::string listen_address_;
  std::atomic<bool> initialized_{false};

  mutable std::shared_mutex groups_mutex_;
  std::unordered_map<PartitionID, std::unique_ptr<BraftPartitionNode>> groups_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_PARTITION_RAFT_MANAGER_H_
