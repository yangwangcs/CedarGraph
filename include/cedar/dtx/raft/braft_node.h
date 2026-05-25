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
// braft Integration for MetaD
// =============================================================================
// Production-grade Raft consensus using braft (Baidu Raft)
// 
// Prerequisites:
//   sudo apt-get install libbraft-dev  # Ubuntu/Debian
//   brew install braft                 # macOS (may need manual build)
//
// Build from source if package not available:
//   git clone https://github.com/brpc/braft.git
//   cd braft && mkdir build && cd build
//   cmake .. && make -j4 && sudo make install
// =============================================================================

#ifndef CEDAR_DTX_RAFT_BRAFT_NODE_H_
#define CEDAR_DTX_RAFT_BRAFT_NODE_H_

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

// braft headers
#include <braft/raft.h>
#include <braft/storage.h>

namespace cedar {
namespace dtx {

// Forward declarations
class MetadataService;

// =============================================================================
// Raft Command Types
// =============================================================================

enum class RaftCommandType : uint8_t {
  kCreateSpace = 1,
  kDropSpace = 2,
  kUpdatePartition = 3,
  kUpdateNode = 4,
  kUpdateAssignment = 5,
  kBatch = 6,  // Multiple commands in one log entry
};

// Serialized Raft command
struct RaftCommand {
  RaftCommandType type;
  std::string payload;  // Protobuf or JSON serialized data
  uint64_t term;
  uint64_t index;
};

// =============================================================================
// braft State Machine for MetaD
// =============================================================================

class MetaRaftStateMachine : public braft::StateMachine {
 public:
  explicit MetaRaftStateMachine(MetadataService* meta_service);
  ~MetaRaftStateMachine() override = default;

  // Apply Raft log entry to state machine
  void on_apply(braft::Iterator& iter) override;
  
  // Save snapshot for fast recovery and new node bootstrap
  void on_snapshot_save(braft::SnapshotWriter* writer, 
                        braft::Closure* done) override;
  
  // Load snapshot
  int on_snapshot_load(braft::SnapshotReader* reader) override;
  
  // Called when leadership changes
  void on_leader_start(int64_t term) override;
  void on_leader_stop(const butil::Status& status) override;
  void on_shutdown() override;
  void on_error(const braft::Error& e) override;
  void on_configuration_committed(const braft::Configuration& conf) override;
  void on_stop_following(const braft::LeaderChangeContext& ctx) override;
  void on_start_following(const braft::LeaderChangeContext& ctx) override;

 private:
  MetadataService* meta_service_;
  std::atomic<int64_t> last_term_{0};
  mutable std::mutex sm_mutex_;
};

// =============================================================================
// braft Node Wrapper
// =============================================================================

class BRaftNode {
 public:
  struct Options {
    NodeID node_id;
    std::string listen_address;  // e.g., "0.0.0.0:9090"
    std::string data_path;       // Raft log storage path
    std::vector<std::string> initial_peers;  // Other nodes in cluster
    int election_timeout_ms = 5000;
    int snapshot_interval_s = 3600;  // 1 hour
  };

  BRaftNode();
  ~BRaftNode();

  // Initialize and start Raft node
  Status Init(const Options& options, MetadataService* meta_service);
  
  // Shutdown node
  void Shutdown();
  
  // Check if this node is the leader
  bool IsLeader() const;
  
  // Get current leader node ID
  std::optional<NodeID> GetLeaderId() const;
  
  // Propose command to Raft (only leader can propose)
  Status Propose(const RaftCommand& command);
  
  // Add/remove peers (cluster membership change)
  Status AddPeer(const std::string& peer_address);
  Status RemovePeer(const std::string& peer_address);
  
  // Get node status
  struct NodeStatus {
    bool is_leader;
    int64_t term;
    int64_t committed_index;
    int64_t applied_index;
    std::string leader_address;
    size_t peer_count;
  };
  NodeStatus GetStatus() const;
  
  // Set callback for leadership changes
  void SetLeaderChangeCallback(
      std::function<void(bool is_leader, int64_t term)> callback);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// =============================================================================
// Factory for creating Raft nodes
// =============================================================================

class RaftNodeFactory {
 public:
  // Create appropriate Raft implementation based on config
  static std::unique_ptr<BRaftNode> Create(
      const BRaftNode::Options& options,
      MetadataService* meta_service);
  
  // Check if braft is available
  static bool IsBraftAvailable();
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_RAFT_BRAFT_NODE_H_
