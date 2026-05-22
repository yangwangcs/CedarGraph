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

#include "cedar/dtx/storage/partition_raft_manager.h"
#include "cedar/dtx/storage_service_impl.h"


namespace cedar {
namespace dtx {

PartitionRaftManager::PartitionRaftManager() = default;

PartitionRaftManager::~PartitionRaftManager() {
  Shutdown();
}

Status PartitionRaftManager::Initialize(NodeID node_id,
                                        const std::string& base_data_dir,
                                        const std::string& listen_address) {
  node_id_ = node_id;
  base_data_dir_ = base_data_dir;
  listen_address_ = listen_address;
  initialized_.store(true);
  return Status::OK();
}

void PartitionRaftManager::Shutdown() {
  if (!initialized_.exchange(false)) {
    return;
  }

  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  for (auto& [pid, node] : groups_) {
    node->Shutdown();
  }
  groups_.clear();
}

Status PartitionRaftManager::CreateRaftGroup(
    PartitionID pid, const std::vector<std::string>& peers,
    PartitionStorage* storage, int election_timeout_ms,
    const std::unordered_map<std::string, NodeID>& peer_node_ids) {
  if (!initialized_.load()) {
    return Status::IOError("PartitionRaftManager not initialized");
  }

  BraftPartitionNode::Options options;
  options.partition_id = pid;
  options.node_id = node_id_;
  options.data_path = base_data_dir_ + "/raft/partition_" + std::to_string(pid);
  options.listen_address = listen_address_;
  options.initial_peers = peers;
  options.election_timeout_ms = election_timeout_ms;
  options.peer_node_ids = peer_node_ids;

  auto node = std::make_unique<BraftPartitionNode>();
  auto status = node->Init(options, storage);
  if (!status.ok()) {
    return status;
  }

  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  groups_[pid] = std::move(node);
  return Status::OK();
}

BraftPartitionNode* PartitionRaftManager::GetRaftGroup(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  auto it = groups_.find(pid);
  return (it != groups_.end()) ? it->second.get() : nullptr;
}

void PartitionRaftManager::RemoveRaftGroup(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  auto it = groups_.find(pid);
  if (it != groups_.end()) {
    it->second->Shutdown();
    groups_.erase(it);
  }
}

std::vector<PartitionID> PartitionRaftManager::GetAllPartitionIDs() const {
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  std::vector<PartitionID> ids;
  ids.reserve(groups_.size());
  for (const auto& [pid, _] : groups_) {
    ids.push_back(pid);
  }
  return ids;
}

PartitionRaftManager::Stats PartitionRaftManager::GetStats() const {
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  Stats stats{};
  stats.num_groups = groups_.size();
  for (const auto& [_, node] : groups_) {
    auto status = node->GetStatus();
    if (status.is_leader) {
      stats.num_leaders++;
    } else {
      // braft doesn't expose candidate state directly in NodeStatus
      stats.num_followers++;
    }
  }
  return stats;
}

}  // namespace dtx
}  // namespace cedar
