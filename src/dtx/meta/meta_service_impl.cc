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


}  // namespace dtx
}  // namespace cedar
