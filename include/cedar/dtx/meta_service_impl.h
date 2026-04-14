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

// =============================================================================
// MetaService Implementation
// =============================================================================

#ifndef CEDAR_DTX_META_SERVICE_IMPL_H_
#define CEDAR_DTX_META_SERVICE_IMPL_H_

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <chrono>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/raft/embedded_raft.h"
#include "cedar/dtx/meta_service.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Metadata Command Types (for Raft replication)
// =============================================================================

enum class MetaCommandType : uint8_t {
  kCreateSpace = 1,
  kDropSpace = 2,
  kRegisterNode = 3,
  kUpdateNodeStatus = 4,
  kUpdatePartitionLeader = 5,
  kUpdatePartitionAssignment = 6,
};

struct MetaCommand {
  MetaCommandType type;
  std::string data;  // Serialized command data
  
  std::string Serialize() const;
  static StatusOr<MetaCommand> Deserialize(const std::string& data);
};

// =============================================================================
// In-Memory Metadata Store
// =============================================================================

class MetadataStore {
 public:
  MetadataStore() = default;
  
  // Schema operations
  Status CreateSpace(const SpaceDef& space);
  Status DropSpace(const std::string& space_name);
  StatusOr<SpaceDef> GetSpace(const std::string& space_name) const;
  std::vector<std::string> ListSpaces() const;
  
  // Partition operations
  Status UpdatePartitionAssignment(const std::string& space_name,
                                    PartitionID partition_id,
                                    const PartitionAssignment& assignment);
  StatusOr<PartitionAssignment> GetPartitionAssignment(
      const std::string& space_name, PartitionID partition_id) const;
  StatusOr<SpacePartitionMap> GetSpacePartitionMap(
      const std::string& space_name) const;
  
  // Node operations
  Status RegisterNode(const NodeInfo& info);
  Status UpdateNodeStatus(const NodeStatus& status);
  StatusOr<NodeInfo> GetNode(NodeID node_id) const;
  std::vector<NodeInfo> GetAliveNodes(uint64_t timeout_sec) const;
  std::vector<NodeInfo> GetAllNodes() const;
  
  // Serialization for snapshot
  std::string Serialize() const;
  Status Deserialize(const std::string& data);
  
  // Get current version
  uint64_t GetVersion() const { return version_; }

 private:
  mutable std::shared_mutex mutex_;
  
  // Schema
  std::unordered_map<std::string, SpaceDef> spaces_;
  
  // Partition mappings
  std::unordered_map<std::string, SpacePartitionMap> partition_maps_;
  
  // Node information
  std::unordered_map<NodeID, NodeInfo> nodes_;
  std::unordered_map<NodeID, NodeStatus> node_statuses_;
  
  // Version counter for optimistic concurrency
  std::atomic<uint64_t> version_{0};
};

// =============================================================================
// Raft-aware Metadata Service
// =============================================================================

class RaftMetaService : public raft::StateMachine {
 public:
  RaftMetaService();
  ~RaftMetaService() override = default;
  
  // Initialize with Raft node
  void Initialize(raft::EmbeddedRaftNode* raft_node);
  
  // StateMachine interface
  void Apply(const raft::LogEntry& entry) override;
  raft::Snapshot CreateSnapshot() override;
  Status RestoreSnapshot(const raft::Snapshot& snapshot) override;
  raft::LogIndex GetLastAppliedIndex() const override;
  
  // High-level API (proposes through Raft)
  Status CreateSpace(const SpaceDef& space);
  Status DropSpace(const std::string& space_name);
  Status RegisterNode(const NodeInfo& info);
  Status UpdateNodeStatus(const NodeStatus& status);
  Status UpdatePartitionLeader(const std::string& space_name,
                                PartitionID partition_id,
                                NodeID new_leader);
  
  // Read-only queries (direct from store)
  StatusOr<SpaceDef> GetSpace(const std::string& name) const;
  StatusOr<PartitionAssignment> GetPartitionAssignment(
      const std::string& space_name, PartitionID pid) const;
  StatusOr<SpacePartitionMap> GetSpacePartitionMap(
      const std::string& space_name) const;
  StatusOr<NodeInfo> GetNode(NodeID node_id) const;
  std::vector<NodeInfo> GetAliveNodes() const;
  std::vector<NodeInfo> GetAllNodes() const;
  
  // Check if this is leader
  bool IsLeader() const;
  NodeID GetLeaderId() const;
  
  // Get store reference (for snapshots)
  const MetadataStore& GetStore() const { return store_; }

 private:
  // Apply specific command types
  void ApplyCreateSpace(const std::string& data);
  void ApplyDropSpace(const std::string& data);
  void ApplyRegisterNode(const std::string& data);
  void ApplyUpdateNodeStatus(const std::string& data);
  void ApplyUpdatePartitionLeader(const std::string& data);
  
  // Propose command to Raft
  Status ProposeCommand(const MetaCommand& cmd);
  
  raft::EmbeddedRaftNode* raft_node_{nullptr};
  MetadataStore store_;
  std::atomic<raft::LogIndex> last_applied_{0};
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_META_SERVICE_IMPL_H_
