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

#include "cedar/dtx/meta_service.h"

#include <sstream>
#include <iomanip>

namespace cedar {
namespace dtx {

// =============================================================================
// SpaceDef Serialization
// =============================================================================

std::string SpaceDef::Serialize() const {
  std::ostringstream oss;
  oss << name << "|" 
      << partition_num << "|" 
      << replica_factor << "|"
      << std::chrono::system_clock::to_time_t(created_at);
  return oss.str();
}

StatusOr<SpaceDef> SpaceDef::Deserialize(const std::string& data) {
  SpaceDef space;
  std::istringstream iss(data);
  
  std::string name_str, part_str, rep_str, time_str;
  if (!std::getline(iss, name_str, '|') ||
      !std::getline(iss, part_str, '|') ||
      !std::getline(iss, rep_str, '|') ||
      !std::getline(iss, time_str, '|')) {
    return Status::InvalidArgument("Invalid SpaceDef serialization");
  }
  
  space.name = name_str;
  space.partition_num = std::stoul(part_str);
  space.replica_factor = std::stoul(rep_str);
  space.created_at = std::chrono::system_clock::from_time_t(std::stoll(time_str));
  
  return space;
}

// =============================================================================
// PartitionAssignment Serialization
// =============================================================================

std::string PartitionAssignment::Serialize() const {
  std::ostringstream oss;
  oss << partition_id << "|"
      << space_name << "|"
      << leader_node << "|";
  
  // Serialize follower nodes
  for (size_t i = 0; i < follower_nodes.size(); ++i) {
    if (i > 0) oss << ",";
    oss << follower_nodes[i];
  }
  oss << "|" << version << "|"
      << static_cast<int>(state) << "|"
      << std::chrono::system_clock::to_time_t(last_updated);
  
  return oss.str();
}

StatusOr<PartitionAssignment> PartitionAssignment::Deserialize(const std::string& data) {
  PartitionAssignment assign;
  std::istringstream iss(data);
  
  std::string pid_str, space, leader, followers, ver, state_str, time_str;
  if (!std::getline(iss, pid_str, '|') ||
      !std::getline(iss, space, '|') ||
      !std::getline(iss, leader, '|') ||
      !std::getline(iss, followers, '|') ||
      !std::getline(iss, ver, '|') ||
      !std::getline(iss, state_str, '|') ||
      !std::getline(iss, time_str, '|')) {
    return Status::InvalidArgument("Invalid PartitionAssignment serialization");
  }
  
  assign.partition_id = std::stoul(pid_str);
  assign.space_name = space;
  assign.leader_node = std::stoul(leader);
  assign.version = std::stoull(ver);
  assign.state = static_cast<State>(std::stoi(state_str));
  assign.last_updated = std::chrono::system_clock::from_time_t(std::stoll(time_str));
  
  // Parse follower nodes
  std::istringstream follower_iss(followers);
  std::string node_str;
  while (std::getline(follower_iss, node_str, ',')) {
    if (!node_str.empty()) {
      assign.follower_nodes.push_back(std::stoul(node_str));
    }
  }
  
  return assign;
}

// =============================================================================
// SpacePartitionMap Serialization
// =============================================================================

PartitionID SpacePartitionMap::GetPartitionForKey(const CedarKey& key) const {
  // Simple hash-based partitioning
  // In production, use consistent hashing
  std::hash<std::string> hasher;
  return hasher(key.ToString()) % num_partitions;
}

NodeID SpacePartitionMap::GetLeader(PartitionID pid) const {
  auto it = assignments.find(pid);
  if (it != assignments.end()) {
    return it->second.leader_node;
  }
  return kInvalidNodeID;
}

StatusOr<PartitionAssignment> SpacePartitionMap::GetAssignment(PartitionID pid) const {
  auto it = assignments.find(pid);
  if (it != assignments.end()) {
    return it->second;
  }
  return Status::NotFound("Partition not found: " + std::to_string(pid));
}

std::string SpacePartitionMap::Serialize() const {
  std::ostringstream oss;
  oss << space_name << "|"
      << num_partitions << "|"
      << replication_factor << "|"
      << version << "|"
      << std::chrono::system_clock::to_time_t(updated_at);
  return oss.str();
}

StatusOr<SpacePartitionMap> SpacePartitionMap::Deserialize(const std::string& data) {
  SpacePartitionMap map;
  std::istringstream iss(data);
  
  std::string name, parts, rep, ver, time;
  if (!std::getline(iss, name, '|') ||
      !std::getline(iss, parts, '|') ||
      !std::getline(iss, rep, '|') ||
      !std::getline(iss, ver, '|') ||
      !std::getline(iss, time, '|')) {
    return Status::InvalidArgument("Invalid SpacePartitionMap serialization");
  }
  
  map.space_name = name;
  map.num_partitions = std::stoul(parts);
  map.replication_factor = std::stoul(rep);
  map.version = std::stoull(ver);
  map.updated_at = std::chrono::system_clock::from_time_t(std::stoll(time));
  
  return map;
}

// =============================================================================
// NodeInfo Serialization
// =============================================================================

std::string NodeInfo::Serialize() const {
  std::ostringstream oss;
  oss << node_id << "|"
      << address << "|"
      << data_path << "|"
      << num_cpu_cores << "|"
      << total_memory_bytes << "|"
      << total_disk_bytes << "|"
      << std::chrono::system_clock::to_time_t(registered_at) << "|"
      << std::chrono::system_clock::to_time_t(last_heartbeat) << "|"
      << static_cast<int>(state);
  return oss.str();
}

StatusOr<NodeInfo> NodeInfo::Deserialize(const std::string& data) {
  NodeInfo info;
  std::istringstream iss(data);
  
  std::string id, addr, path, cpu, mem, disk, reg, hb, st;
  if (!std::getline(iss, id, '|') ||
      !std::getline(iss, addr, '|') ||
      !std::getline(iss, path, '|') ||
      !std::getline(iss, cpu, '|') ||
      !std::getline(iss, mem, '|') ||
      !std::getline(iss, disk, '|') ||
      !std::getline(iss, reg, '|') ||
      !std::getline(iss, hb, '|') ||
      !std::getline(iss, st, '|')) {
    return Status::InvalidArgument("Invalid NodeInfo serialization");
  }
  
  info.node_id = std::stoul(id);
  info.address = addr;
  info.data_path = path;
  info.num_cpu_cores = std::stoul(cpu);
  info.total_memory_bytes = std::stoull(mem);
  info.total_disk_bytes = std::stoull(disk);
  info.registered_at = std::chrono::system_clock::from_time_t(std::stoll(reg));
  info.last_heartbeat = std::chrono::system_clock::from_time_t(std::stoll(hb));
  info.state = static_cast<State>(std::stoi(st));
  
  return info;
}

// =============================================================================
// NodeStatus Serialization
// =============================================================================

std::string NodeStatus::Serialize() const {
  std::ostringstream oss;
  oss << node_id << "|"
      << cpu_usage_percent << "|"
      << memory_usage_percent << "|"
      << disk_usage_percent << "|"
      << qps << "|"
      << latency_ms << "|";
  
  // Serialize leader partitions
  for (size_t i = 0; i < leader_partitions.size(); ++i) {
    if (i > 0) oss << ",";
    oss << leader_partitions[i];
  }
  oss << "|";
  
  // Serialize follower partitions
  for (size_t i = 0; i < follower_partitions.size(); ++i) {
    if (i > 0) oss << ",";
    oss << follower_partitions[i];
  }
  oss << "|";
  
  oss << std::chrono::system_clock::to_time_t(timestamp);
  
  return oss.str();
}

StatusOr<NodeStatus> NodeStatus::Deserialize(const std::string& data) {
  NodeStatus status;
  std::istringstream iss(data);
  
  std::string id, cpu, mem, disk, qps_str, lat, leaders, followers, time;
  if (!std::getline(iss, id, '|') ||
      !std::getline(iss, cpu, '|') ||
      !std::getline(iss, mem, '|') ||
      !std::getline(iss, disk, '|') ||
      !std::getline(iss, qps_str, '|') ||
      !std::getline(iss, lat, '|') ||
      !std::getline(iss, leaders, '|') ||
      !std::getline(iss, followers, '|') ||
      !std::getline(iss, time, '|')) {
    return Status::InvalidArgument("Invalid NodeStatus serialization");
  }
  
  status.node_id = std::stoul(id);
  status.cpu_usage_percent = std::stod(cpu);
  status.memory_usage_percent = std::stod(mem);
  status.disk_usage_percent = std::stod(disk);
  status.qps = std::stoull(qps_str);
  status.latency_ms = std::stoull(lat);
  status.timestamp = std::chrono::system_clock::from_time_t(std::stoll(time));
  
  // Parse leader partitions
  std::istringstream leader_iss(leaders);
  std::string part_str;
  while (std::getline(leader_iss, part_str, ',')) {
    if (!part_str.empty()) {
      status.leader_partitions.push_back(std::stoul(part_str));
    }
  }
  
  // Parse follower partitions
  std::istringstream follower_iss(followers);
  while (std::getline(follower_iss, part_str, ',')) {
    if (!part_str.empty()) {
      status.follower_partitions.push_back(std::stoul(part_str));
    }
  }
  
  return status;
}

}  // namespace dtx
}  // namespace cedar
