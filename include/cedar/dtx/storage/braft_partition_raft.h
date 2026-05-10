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

#ifndef CEDAR_DTX_STORAGE_BRAFT_PARTITION_RAFT_H_
#define CEDAR_DTX_STORAGE_BRAFT_PARTITION_RAFT_H_

#include <braft/raft.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <butil/iobuf.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// Forward declaration
class PartitionStorage;

// =============================================================================
// StorageLogEntry - Serialized log entry for storage operations
// Format: [term:8][index:8][type:1][key_len:4][key][desc:8][ts:8]
// =============================================================================
struct StorageLogEntry {
  enum class Type : uint8_t {
    kPut = 1,
    kDelete = 2,
    kBatch = 3,
    kPrepare = 4,
    kCommit = 5,
    kAbort = 6,
  };

  Type type = Type::kPut;
  CedarKey key;
  std::optional<Descriptor> descriptor;
  Timestamp txn_version;
  std::vector<std::pair<CedarKey, Descriptor>> batch_data;

  // 2PC fields
  TxnID txn_id = 0;
  std::vector<CedarKey> read_set;
  std::vector<CedarKey> write_set;
  std::unordered_map<uint64_t, Descriptor> write_descriptors;
  Timestamp commit_ts;

  std::string Serialize() const;
  static StatusOr<StorageLogEntry> Deserialize(const std::string& data);
};

// =============================================================================
// StoragePartitionStateMachine - braft StateMachine for a single partition
// Each partition has its own independent Raft group (Multi-Raft)
// =============================================================================
using LeaderLeaseCallback = std::function<void(int64_t term, bool is_leader)>;

class StoragePartitionStateMachine : public braft::StateMachine {
 public:
  explicit StoragePartitionStateMachine(PartitionStorage* storage);

  void SetLeaderLeaseCallback(LeaderLeaseCallback cb) { lease_callback_ = std::move(cb); }

  void on_apply(braft::Iterator& iter) override;
  void on_snapshot_save(braft::SnapshotWriter* writer,
                         braft::Closure* done) override;
  int on_snapshot_load(braft::SnapshotReader* reader) override;
  void on_leader_start(int64_t term) override;
  void on_leader_stop(const butil::Status& status) override;
  void on_shutdown() override;
  void on_error(const braft::Error& e) override;
  void on_configuration_committed(const braft::Configuration& conf) override;

 private:
  PartitionStorage* storage_;
  std::atomic<int64_t> last_term_{0};
  LeaderLeaseCallback lease_callback_;
};

// =============================================================================
// BraftPartitionNode - Wrapper around braft::Node for a single partition
// =============================================================================
class BraftPartitionNode {
 public:
  struct Options {
    PartitionID partition_id;
    NodeID node_id;
    std::string listen_address;
    std::string data_path;
    std::vector<std::string> initial_peers;
    std::unordered_map<std::string, NodeID> peer_node_ids;
    int election_timeout_ms = 1000;
  };

  BraftPartitionNode();
  ~BraftPartitionNode();

  Status Init(const Options& options, PartitionStorage* storage);
  void Shutdown();

  bool IsLeader() const;
  bool IsLeaseValid() const;  // Leader lease check for linearizable reads
  std::optional<NodeID> GetLeaderId() const;
  std::optional<std::string> GetLeaderAddress() const;

  Status Propose(const StorageLogEntry& entry);
  StatusOr<uint64_t> ReadIndex(std::chrono::milliseconds timeout);
  Status WaitForApplied(uint64_t index, std::chrono::milliseconds timeout);

  struct NodeStatus {
    bool is_leader;
    int64_t term;
    int64_t committed_index;
    int64_t applied_index;
    std::string leader_address;
    size_t peer_count;
  };
  NodeStatus GetStatus() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_BRAFT_PARTITION_RAFT_H_
