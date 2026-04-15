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

#include "cedar/dtx/raft/raft_node_factory.h"

#include <memory>

#include "cedar/dtx/raft/embedded_raft.h"
#include "cedar/dtx/raft/raft_interface.h"

#ifdef CEDAR_WITH_BRAFT
#include "cedar/dtx/raft/braft_node.h"
#endif

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// Embedded Raft Wrapper
// =============================================================================
// Wraps EmbeddedRaftNode to implement the RaftNode interface

class EmbeddedRaftNodeWrapper : public RaftNode {
 public:
  EmbeddedRaftNodeWrapper() = default;
  ~EmbeddedRaftNodeWrapper() override = default;
  
  Status Initialize(const RaftConfig& config, StateMachine* state_machine) override {
    config_ = config;
    state_machine_ = state_machine;
    // Note: Full initialization requires transport and storage setup
    // This is simplified for now
    return Status::OK();
  }
  
  Status Shutdown() override {
    if (node_) {
      node_->Shutdown();
    }
    return Status::OK();
  }
  
  StatusOr<LogIndex> Propose(const std::string& data) override {
    if (!node_) {
      return Status::InvalidArgument("Node not initialized");
    }
    
    auto status = node_->Propose(data);
    if (!status.ok()) {
      return status;
    }
    
    // Return the last log index
    return node_->GetCommitIndex();
  }
  
  bool IsLeader() const override {
    return node_ && node_->IsLeader();
  }
  
  NodeID GetLeader() const override {
    if (!node_) return 0;
    auto leader = node_->GetLeaderId();
    return leader.has_value() ? static_cast<NodeID>(leader.value()) : 0;
  }
  
  RaftState GetState() const override {
    if (!node_) return RaftState::kFollower;
    return node_->IsLeader() ? RaftState::kLeader : RaftState::kFollower;
  }
  
  LogTerm GetTerm() const override {
    if (!node_) return 0;
    return node_->GetCurrentTerm();
  }
  
  Status AddNode(NodeID node_id, const std::string& address) override {
    if (!node_) {
      return Status::InvalidArgument("Node not initialized");
    }
    return node_->AddPeer(node_id, address);
  }
  
  Status RemoveNode(NodeID node_id) override {
    if (!node_) {
      return Status::InvalidArgument("Node not initialized");
    }
    return node_->RemovePeer(node_id);
  }
  
  std::vector<std::pair<NodeID, std::string>> GetMembers() const override {
    // TODO: Return actual members from node
    return config_.peers;
  }
  
  Status TriggerSnapshot() override {
    if (!node_) {
      return Status::InvalidArgument("Node not initialized");
    }
    return node_->TriggerSnapshot();
  }
  
  void RegisterStateCallback(
      std::function<void(RaftState old_state, RaftState new_state)> callback) override {
    state_callbacks_.push_back(callback);
  }
  
  void RegisterLeaderCallback(
      std::function<void(NodeID old_leader, NodeID new_leader)> callback) override {
    leader_callbacks_.push_back(callback);
  }
  
  // Set the underlying node (used during initialization)
  void SetNode(std::unique_ptr<EmbeddedRaftNode> node) {
    node_ = std::move(node);
  }

 private:
  std::unique_ptr<EmbeddedRaftNode> node_;
  RaftConfig config_;
  StateMachine* state_machine_{nullptr};
  
  std::vector<std::function<void(RaftState, RaftState)>> state_callbacks_;
  std::vector<std::function<void(NodeID, NodeID)>> leader_callbacks_;
};

// =============================================================================
// Factory Implementation
// =============================================================================

RaftNodeType g_default_node_type = RaftNodeType::kEmbedded;

void SetDefaultRaftNodeType(RaftNodeType type) {
  g_default_node_type = type;
}

std::unique_ptr<RaftNode> CreateRaftNode(const RaftConfig& config, 
                                          RaftNodeType type) {
  // Use default type if not specified
  if (type == RaftNodeType::kDefault) {
    type = g_default_node_type;
  }
  
  std::unique_ptr<RaftNode> node;
  
  switch (type) {
    case RaftNodeType::kEmbedded: {
      // Create EmbeddedRaftNode (self-contained, no external deps)
      auto wrapper = std::make_unique<EmbeddedRaftNodeWrapper>();
      // Note: Full initialization requires transport, state_machine, storage
      // For now, we just call Initialize with config
      wrapper->Initialize(config, nullptr);
      node = std::move(wrapper);
      break;
    }
    
#ifdef CEDAR_WITH_BRAFT
    case RaftNodeType::kBraft: {
      // TODO: Create braft-based wrapper
      // node = std::make_unique<BraftRaftNode>();
      // node->Initialize(config, nullptr);
      break;
    }
#endif
    

    
    default: {
      break;
    }
  }
  
  return node;
}

}  // namespace raft
}  // namespace dtx
}  // namespace cedar
