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

#ifndef CEDAR_RAFT_PARTITION_RAFT_SERVICE_H_
#define CEDAR_RAFT_PARTITION_RAFT_SERVICE_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "cedar/core/status.h"
#include "cedar/raft/partition_raft_group.h"
#include "raft_service.grpc.pb.h"
#include "batch_log_committer.h"

namespace cedar {
namespace raft {

// Forward declaration
class PartitionLogStore;

// Per-partition Raft state tracking
struct PartitionState {
  uint32_t partition_id;
  uint64_t current_term = 0;
  std::string voted_for;
  uint64_t commit_index = 0;
  uint64_t last_applied = 0;
  RaftRole current_role = RaftRole::kFollower;
  std::string leader_id;
  std::unique_ptr<PartitionLogStore> log_store;
  mutable std::mutex mutex;

  explicit PartitionState(uint32_t id) : partition_id(id) {}
};

// PartitionRaftServiceImpl implements the gRPC service for partition-level Raft consensus
class PartitionRaftServiceImpl : public cedar::raft::internal::PartitionRaftService::Service {
 public:
  PartitionRaftServiceImpl();
  ~PartitionRaftServiceImpl() override;

  // Disable copy and assignment
  PartitionRaftServiceImpl(const PartitionRaftServiceImpl&) = delete;
  PartitionRaftServiceImpl& operator=(const PartitionRaftServiceImpl&) = delete;

  // Initialize a partition with the given ID and data directory
  Status InitializePartition(uint32_t partition_id, const std::string& data_dir);

  // Check if the given partition is initialized
  bool IsPartitionInitialized(uint32_t partition_id) const;

  // Check if this node is the leader for the given partition
  bool IsPartitionLeader(uint32_t partition_id) const;

  // Get the current leader ID for a partition
  StatusOr<std::string> GetPartitionLeader(uint32_t partition_id) const;

  // Get the current term for a partition
  StatusOr<uint64_t> GetPartitionTerm(uint32_t partition_id) const;

  // gRPC service methods
  ::grpc::Status RequestVote(::grpc::ServerContext* context,
                              const ::cedar::raft::internal::VoteRequest* request,
                              ::cedar::raft::internal::VoteResponse* response) override;

  ::grpc::Status AppendEntries(::grpc::ServerContext* context,
                                const ::cedar::raft::internal::AppendEntriesRequest* request,
                                ::cedar::raft::internal::AppendEntriesResponse* response) override;

  ::grpc::Status InstallSnapshot(::grpc::ServerContext* context,
                                  const ::cedar::raft::internal::SnapshotRequest* request,
                                  ::cedar::raft::internal::SnapshotResponse* response) override;

  ::grpc::Status SendHeartbeat(::grpc::ServerContext* context,
                                const ::cedar::raft::internal::HeartbeatRequest* request,
                                ::cedar::raft::internal::HeartbeatResponse* response) override;

 private:
  // Get or create partition state
  std::shared_ptr<PartitionState> GetPartitionState(uint32_t partition_id);
  std::shared_ptr<PartitionState> GetPartitionState(uint32_t partition_id) const;

  // Raft logic helpers
  void BecomeFollower(std::shared_ptr<PartitionState> state, uint64_t term);
  void BecomeLeader(std::shared_ptr<PartitionState> state);
  void UpdateTerm(std::shared_ptr<PartitionState> state, uint64_t new_term);

  // Check if candidate's log is at least as up-to-date as ours
  bool IsLogUpToDate(std::shared_ptr<PartitionState> state, uint64_t last_log_index,
                     uint64_t last_log_term) const;

  // Apply committed entries to state machine
  Status ApplyCommittedEntries(std::shared_ptr<PartitionState> state);

  mutable std::mutex partitions_mutex_;
  std::unordered_map<uint32_t, std::shared_ptr<PartitionState>> partitions_;
  std::unordered_map<uint32_t, std::unique_ptr<BatchLogCommitter>> batch_committers_;
};

}  // namespace raft
}  // namespace cedar

#endif  // CEDAR_RAFT_PARTITION_RAFT_SERVICE_H_
