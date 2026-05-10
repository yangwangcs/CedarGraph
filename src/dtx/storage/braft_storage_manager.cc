// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include "cedar/dtx/storage/braft_storage_manager.h"
#include "cedar/dtx/storage_impl/partition_manager.h"
#include "cedar/dtx/storage_impl/partition_storage.h"

#include <butil/logging.h>
#include <braft/raft.h>

#include <atomic>
#include <chrono>
#include <future>

namespace cedar {
namespace dtx {
namespace storage {

StorageBraftManager::StorageBraftManager() = default;

StorageBraftManager::~StorageBraftManager() {
  Shutdown();
}

Status StorageBraftManager::Initialize(const Config& config,
                                        StoragePartitionManager* partition_manager) {
  config_ = config;
  partition_manager_ = partition_manager;
  initialized_.store(true);
  return Status::OK();
}

void StorageBraftManager::Shutdown() {
  if (!initialized_.exchange(false)) return;
  
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  for (auto& [pid, group] : groups_) {
    if (group && group->node) {
      group->node->shutdown(nullptr);
      group->node->join();
    }
  }
  groups_.clear();
}

Status StorageBraftManager::CreatePartitionGroup(
    PartitionID pid,
    const std::vector<std::pair<NodeID, std::string>>& peers) {
  if (!initialized_.load()) {
    return Status::IOError("StorageBraftManager", "not initialized");
  }
  
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  if (groups_.find(pid) != groups_.end()) {
    return Status::OK();  // Already exists
  }
  
  // Get the partition storage for this partition
  PartitionStorage* storage = nullptr;
  if (partition_manager_) {
    storage = partition_manager_->GetPartition(pid);
  }
  if (!storage) {
    return Status::NotFound("StorageBraftManager", "partition not found: " + std::to_string(pid));
  }
  
  auto group = std::make_unique<PartitionRaftGroup>();
  group->partition_id = pid;
  group->data_path = config_.base_data_dir + "/raft/partition_" + std::to_string(pid);
  
  // Create state machine
  group->state_machine = std::make_unique<PartitionRaftStateMachine>(storage);
  
  // Configure braft node
  braft::NodeOptions node_options;
  node_options.election_timeout_ms = static_cast<int>(config_.election_timeout_ms);
  node_options.snapshot_interval_s = static_cast<int>(config_.snapshot_interval_s);
  node_options.fsm = group->state_machine.get();
  node_options.node_owns_fsm = false;
  
  for (const auto& [node_id, address] : peers) {
    (void)node_id;
    node_options.initial_conf.add_peer(braft::PeerId(address));
  }
  
  node_options.log_uri = "local://" + group->data_path + "/log";
  node_options.raft_meta_uri = "local://" + group->data_path + "/meta";
  node_options.snapshot_uri = "local://" + group->data_path + "/snapshot";
  
  braft::PeerId self(config_.listen_address);
  group->node = std::unique_ptr<braft::Node>(
      new braft::Node("partition_" + std::to_string(pid), self));
  
  int ret = group->node->init(node_options);
  if (ret != 0) {
    LOG(ERROR) << "Failed to init braft node for partition " << pid;
    group->node.reset();
    group->state_machine.reset();
    return Status::IOError("Failed to init braft node for partition " + std::to_string(pid));
  }
  
  groups_[pid] = std::move(group);
  LOG(INFO) << "Created braft group for partition " << pid << " with " << peers.size() << " peers";
  return Status::OK();
}

braft::Node* StorageBraftManager::GetPartitionNode(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  auto it = groups_.find(pid);
  if (it == groups_.end()) return nullptr;
  return it->second->node.get();
}

bool StorageBraftManager::IsPartitionLeader(PartitionID pid) {
  auto* node = GetPartitionNode(pid);
  return node && node->is_leader();
}

std::optional<std::string> StorageBraftManager::GetPartitionLeaderAddress(PartitionID pid) {
  auto* node = GetPartitionNode(pid);
  if (!node) return std::nullopt;
  braft::PeerId leader = node->leader_id();
  if (leader.is_empty()) return std::nullopt;
  return leader.to_string();
}

// Closure for async propose
class ProposeClosure : public braft::Closure {
 public:
  explicit ProposeClosure(std::shared_ptr<std::promise<Status>> promise)
      : promise_(std::move(promise)) {}
  
  void Run() override {
    bool expected = false;
    if (!done_.compare_exchange_strong(expected, true)) {
      delete this;
      return;
    }
    if (status().ok()) {
      promise_->set_value(Status::OK());
    } else {
      promise_->set_value(Status::IOError("BRaft propose failed", status().error_cstr()));
    }
    delete this;
  }
  
 private:
  std::shared_ptr<std::promise<Status>> promise_;
  std::atomic<bool> done_{false};
};

Status StorageBraftManager::Propose(PartitionID pid, const StorageRaftCommand& cmd) {
  auto* node = GetPartitionNode(pid);
  if (!node) {
    return Status::NotFound("StorageBraftManager", "no raft group for partition " + std::to_string(pid));
  }
  if (!node->is_leader()) {
    return Status::NotLeader("Not leader for partition " + std::to_string(pid));
  }
  
  butil::IOBuf data;
  std::string serialized = cmd.Serialize();
  data.append(serialized);
  
  braft::Task task;
  task.data = &data;
  
  auto promise = std::make_shared<std::promise<Status>>();
  task.done = new ProposeClosure(promise);
  
  node->apply(task);
  
  auto future = promise->get_future();
  if (future.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
    return Status::IOError("BRaft propose timeout for partition " + std::to_string(pid));
  }
  
  return future.get();
}

void StorageBraftManager::RemovePartitionGroup(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(groups_mutex_);
  auto it = groups_.find(pid);
  if (it == groups_.end()) return;
  
  if (it->second && it->second->node) {
    it->second->node->shutdown(nullptr);
    it->second->node->join();
  }
  groups_.erase(it);
}

std::vector<PartitionID> StorageBraftManager::GetAllPartitionIDs() const {
  std::shared_lock<std::shared_mutex> lock(groups_mutex_);
  std::vector<PartitionID> result;
  result.reserve(groups_.size());
  for (const auto& [pid, _] : groups_) {
    result.push_back(pid);
  }
  return result;
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
