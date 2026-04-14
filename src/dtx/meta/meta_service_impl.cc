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

#include "cedar/dtx/meta_service_impl.h"

#include <iostream>
#include <sstream>
#include <chrono>

namespace cedar {
namespace dtx {

// =============================================================================
// MetaCommand Serialization
// =============================================================================

std::string MetaCommand::Serialize() const {
  // Simple binary format: [1 byte type][4 bytes data length][data]
  std::string result;
  result.push_back(static_cast<char>(type));
  
  uint32_t len = static_cast<uint32_t>(data.size());
  result.append(reinterpret_cast<const char*>(&len), sizeof(len));
  result.append(data);
  
  return result;
}

StatusOr<MetaCommand> MetaCommand::Deserialize(const std::string& data) {
  if (data.size() < 5) {
    return Status::InvalidArgument("Command data too short");
  }
  
  MetaCommand cmd;
  cmd.type = static_cast<MetaCommandType>(data[0]);
  
  uint32_t len;
  memcpy(&len, data.data() + 1, sizeof(len));
  
  if (data.size() < 5 + len) {
    return Status::InvalidArgument("Command data length mismatch");
  }
  
  cmd.data = data.substr(5, len);
  return cmd;
}

// =============================================================================
// MetadataStore Implementation
// =============================================================================

Status MetadataStore::CreateSpace(const SpaceDef& space) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  if (spaces_.find(space.name) != spaces_.end()) {
    return Status::Conflict("Space already exists: " + space.name);
  }
  
  spaces_[space.name] = space;
  
  // Initialize partition map
  SpacePartitionMap partition_map;
  partition_map.space_name = space.name;
  partition_map.num_partitions = space.partition_num;
  partition_map.replication_factor = space.replica_factor;
  partition_maps_[space.name] = partition_map;
  
  version_++;
  std::cout << "[MetaStore] Created space: " << space.name 
            << " with " << space.partition_num << " partitions" << std::endl;
  
  return Status::OK();
}

Status MetadataStore::DropSpace(const std::string& space_name) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  auto it = spaces_.find(space_name);
  if (it == spaces_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  spaces_.erase(it);
  partition_maps_.erase(space_name);
  version_++;
  
  std::cout << "[MetaStore] Dropped space: " << space_name << std::endl;
  return Status::OK();
}

StatusOr<SpaceDef> MetadataStore::GetSpace(const std::string& space_name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = spaces_.find(space_name);
  if (it == spaces_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  return it->second;
}

std::vector<std::string> MetadataStore::ListSpaces() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<std::string> result;
  for (const auto& [name, _] : spaces_) {
    result.push_back(name);
  }
  return result;
}

Status MetadataStore::UpdatePartitionAssignment(
    const std::string& space_name,
    PartitionID partition_id,
    const PartitionAssignment& assignment) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  auto it = partition_maps_.find(space_name);
  if (it == partition_maps_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  it->second.assignments[partition_id] = assignment;
  it->second.version = version_++;
  it->second.updated_at = std::chrono::system_clock::now();
  
  return Status::OK();
}

StatusOr<PartitionAssignment> MetadataStore::GetPartitionAssignment(
    const std::string& space_name, PartitionID partition_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = partition_maps_.find(space_name);
  if (it == partition_maps_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  auto assign_it = it->second.assignments.find(partition_id);
  if (assign_it == it->second.assignments.end()) {
    return Status::NotFound("Partition not found: " + std::to_string(partition_id));
  }
  
  return assign_it->second;
}

StatusOr<SpacePartitionMap> MetadataStore::GetSpacePartitionMap(
    const std::string& space_name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = partition_maps_.find(space_name);
  if (it == partition_maps_.end()) {
    return Status::NotFound("Space not found: " + space_name);
  }
  
  return it->second;
}

Status MetadataStore::RegisterNode(const NodeInfo& info) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  nodes_[info.node_id] = info;
  version_++;
  
  std::cout << "[MetaStore] Registered node: " << info.node_id 
            << " at " << info.address << std::endl;
  
  return Status::OK();
}

Status MetadataStore::UpdateNodeStatus(const NodeStatus& status) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  node_statuses_[status.node_id] = status;
  
  // Update node state based on heartbeat
  auto it = nodes_.find(status.node_id);
  if (it != nodes_.end()) {
    it->second.last_heartbeat = std::chrono::system_clock::now();
    it->second.state = NodeInfo::State::kOnline;
  }
  
  return Status::OK();
}

StatusOr<NodeInfo> MetadataStore::GetNode(NodeID node_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("Node not found: " + std::to_string(node_id));
  }
  
  return it->second;
}

std::vector<NodeInfo> MetadataStore::GetAliveNodes(uint64_t timeout_sec) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<NodeInfo> result;
  auto now = std::chrono::system_clock::now();
  
  for (const auto& [id, info] : nodes_) {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        now - info.last_heartbeat).count();
    if (elapsed < static_cast<int64_t>(timeout_sec)) {
      result.push_back(info);
    }
  }
  
  return result;
}

std::vector<NodeInfo> MetadataStore::GetAllNodes() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::vector<NodeInfo> result;
  for (const auto& [id, info] : nodes_) {
    result.push_back(info);
  }
  return result;
}

// Simple JSON-like serialization
std::string MetadataStore::Serialize() const {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  std::ostringstream oss;
  oss << "{";
  oss << "\"version\":" << version_ << ",";
  
  // Serialize spaces
  oss << "\"spaces\":[";
  bool first = true;
  for (const auto& [name, space] : spaces_) {
    if (!first) oss << ",";
    first = false;
    oss << "{\"name\":\"" << name << "\","
        << "\"partition_num\":" << space.partition_num << "}";
  }
  oss << "],";
  
  // Serialize nodes
  oss << "\"nodes\":[";
  first = true;
  for (const auto& [id, info] : nodes_) {
    if (!first) oss << ",";
    first = false;
    oss << "{\"id\":" << id << ",\"addr\":\"" << info.address << "\"}";
  }
  oss << "]";
  
  oss << "}";
  return oss.str();
}

Status MetadataStore::Deserialize(const std::string& data) {
  // Simplified - in production use proper JSON/Binary serialization
  std::lock_guard<std::shared_mutex> lock(mutex_);
  
  // TODO: Implement proper deserialization
  std::cout << "[MetaStore] Deserializing data: " << data.substr(0, 100) << "..." << std::endl;
  
  return Status::OK();
}

// =============================================================================
// RaftMetaService Implementation
// =============================================================================

RaftMetaService::RaftMetaService() = default;

void RaftMetaService::Initialize(raft::EmbeddedRaftNode* raft_node) {
  raft_node_ = raft_node;
}

void RaftMetaService::Apply(const raft::LogEntry& entry) {
  // Deserialize command
  auto cmd_result = MetaCommand::Deserialize(entry.data);
  if (!cmd_result.ok()) {
    std::cerr << "[RaftMetaService] Failed to deserialize command: " 
              << cmd_result.status().ToString() << std::endl;
    return;
  }
  
  const auto& cmd = cmd_result.value();
  
  std::cout << "[RaftMetaService] Applying command type=" 
            << static_cast<int>(cmd.type) << " at index=" << entry.index << std::endl;
  
  // Apply based on type
  switch (cmd.type) {
    case MetaCommandType::kCreateSpace:
      ApplyCreateSpace(cmd.data);
      break;
    case MetaCommandType::kDropSpace:
      ApplyDropSpace(cmd.data);
      break;
    case MetaCommandType::kRegisterNode:
      ApplyRegisterNode(cmd.data);
      break;
    case MetaCommandType::kUpdateNodeStatus:
      ApplyUpdateNodeStatus(cmd.data);
      break;
    case MetaCommandType::kUpdatePartitionLeader:
      ApplyUpdatePartitionLeader(cmd.data);
      break;
    default:
      std::cerr << "[RaftMetaService] Unknown command type: " 
                << static_cast<int>(cmd.type) << std::endl;
  }
  
  last_applied_ = entry.index;
}

raft::Snapshot RaftMetaService::CreateSnapshot() {
  raft::Snapshot snapshot;
  snapshot.last_included_index = last_applied_.load();
  snapshot.last_included_term = 1;  // TODO: Get from Raft
  snapshot.data = store_.Serialize();
  
  std::cout << "[RaftMetaService] Created snapshot up to index " 
            << snapshot.last_included_index << std::endl;
  
  return snapshot;
}

Status RaftMetaService::RestoreSnapshot(const raft::Snapshot& snapshot) {
  std::cout << "[RaftMetaService] Restoring snapshot up to index " 
            << snapshot.last_included_index << std::endl;
  
  auto status = store_.Deserialize(snapshot.data);
  if (status.ok()) {
    last_applied_ = snapshot.last_included_index;
  }
  return status;
}

raft::LogIndex RaftMetaService::GetLastAppliedIndex() const {
  return last_applied_.load();
}

Status RaftMetaService::CreateSpace(const SpaceDef& space) {
  if (!IsLeader()) {
    return Status::NotLeader("Only leader can create space");
  }
  
  // Serialize space and propose
  MetaCommand cmd;
  cmd.type = MetaCommandType::kCreateSpace;
  cmd.data = space.Serialize();
  
  return ProposeCommand(cmd);
}

Status RaftMetaService::DropSpace(const std::string& space_name) {
  if (!IsLeader()) {
    return Status::NotLeader("Only leader can drop space");
  }
  
  MetaCommand cmd;
  cmd.type = MetaCommandType::kDropSpace;
  cmd.data = space_name;
  
  return ProposeCommand(cmd);
}

Status RaftMetaService::RegisterNode(const NodeInfo& info) {
  if (!IsLeader()) {
    return Status::NotLeader("Only leader can register node");
  }
  
  MetaCommand cmd;
  cmd.type = MetaCommandType::kRegisterNode;
  cmd.data = info.Serialize();
  
  return ProposeCommand(cmd);
}

Status RaftMetaService::UpdateNodeStatus(const NodeStatus& status) {
  // Node status updates can be sent to any node
  // They will be forwarded to leader if needed
  MetaCommand cmd;
  cmd.type = MetaCommandType::kUpdateNodeStatus;
  cmd.data = status.Serialize();
  
  return ProposeCommand(cmd);
}

Status RaftMetaService::UpdatePartitionLeader(const std::string& space_name,
                                               PartitionID partition_id,
                                               NodeID new_leader) {
  if (!IsLeader()) {
    return Status::NotLeader("Only leader can update partition leader");
  }
  
  MetaCommand cmd;
  cmd.type = MetaCommandType::kUpdatePartitionLeader;
  
  // Simple serialization: space_name|partition_id|new_leader
  std::ostringstream oss;
  oss << space_name << "|" << partition_id << "|" << new_leader;
  cmd.data = oss.str();
  
  return ProposeCommand(cmd);
}

// Read-only queries go directly to store
StatusOr<SpaceDef> RaftMetaService::GetSpace(const std::string& name) const {
  return store_.GetSpace(name);
}

StatusOr<PartitionAssignment> RaftMetaService::GetPartitionAssignment(
    const std::string& space_name, PartitionID pid) const {
  return store_.GetPartitionAssignment(space_name, pid);
}

StatusOr<SpacePartitionMap> RaftMetaService::GetSpacePartitionMap(
    const std::string& space_name) const {
  return store_.GetSpacePartitionMap(space_name);
}

StatusOr<NodeInfo> RaftMetaService::GetNode(NodeID node_id) const {
  return store_.GetNode(node_id);
}

std::vector<NodeInfo> RaftMetaService::GetAliveNodes() const {
  return store_.GetAliveNodes(30);  // 30 second timeout
}

std::vector<NodeInfo> RaftMetaService::GetAllNodes() const {
  return store_.GetAllNodes();
}

bool RaftMetaService::IsLeader() const {
  if (!raft_node_) return false;
  return raft_node_->IsLeader();
}

NodeID RaftMetaService::GetLeaderId() const {
  if (!raft_node_) return 0;
  auto leader = raft_node_->GetLeaderId();
  return leader.has_value() ? leader.value() : 0;
}

Status RaftMetaService::ProposeCommand(const MetaCommand& cmd) {
  if (!raft_node_) {
    return Status::InvalidArgument("Raft node not initialized");
  }
  
  return raft_node_->Propose(cmd.Serialize());
}

// Apply implementations
void RaftMetaService::ApplyCreateSpace(const std::string& data) {
  auto space_result = SpaceDef::Deserialize(data);
  if (space_result.ok()) {
    store_.CreateSpace(space_result.value());
  }
}

void RaftMetaService::ApplyDropSpace(const std::string& data) {
  store_.DropSpace(data);
}

void RaftMetaService::ApplyRegisterNode(const std::string& data) {
  auto info_result = NodeInfo::Deserialize(data);
  if (info_result.ok()) {
    store_.RegisterNode(info_result.value());
  }
}

void RaftMetaService::ApplyUpdateNodeStatus(const std::string& data) {
  auto status_result = NodeStatus::Deserialize(data);
  if (status_result.ok()) {
    store_.UpdateNodeStatus(status_result.value());
  }
}

void RaftMetaService::ApplyUpdatePartitionLeader(const std::string& data) {
  // Parse: space_name|partition_id|new_leader
  size_t pos1 = data.find('|');
  size_t pos2 = data.find('|', pos1 + 1);
  
  if (pos1 == std::string::npos || pos2 == std::string::npos) return;
  
  std::string space_name = data.substr(0, pos1);
  PartitionID pid = std::stoul(data.substr(pos1 + 1, pos2 - pos1 - 1));
  NodeID leader = std::stoul(data.substr(pos2 + 1));
  
  auto assign_result = store_.GetPartitionAssignment(space_name, pid);
  if (assign_result.ok()) {
    auto assign = assign_result.value();
    assign.leader_node = leader;
    assign.last_updated = std::chrono::system_clock::now();
    assign.version++;
    store_.UpdatePartitionAssignment(space_name, pid, assign);
  }
}

}  // namespace dtx
}  // namespace cedar
