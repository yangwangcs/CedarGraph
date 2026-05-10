#include "cedar/dtx/meta_service.h"
#include <chrono>
#include <thread>
#include <sstream>

namespace cedar {
namespace dtx {

// =============================================================================
// MetaServiceClient base class
// =============================================================================

MetaServiceClient::MetaServiceClient() = default;
MetaServiceClient::~MetaServiceClient() = default;

// =============================================================================
// Binary Serialization Helpers
// =============================================================================

static void AppendString(std::string& out, const std::string& s) {
    uint32_t len = static_cast<uint32_t>(s.size());
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(s);
}

static StatusOr<std::string> ReadString(const std::string& data, size_t& pos) {
    if (pos + sizeof(uint32_t) > data.size()) {
        return Status::InvalidArgument("Corrupt data: string length");
    }
    uint32_t len;
    std::memcpy(&len, &data[pos], sizeof(len));
    pos += sizeof(len);
    if (pos + len > data.size()) {
        return Status::InvalidArgument("Corrupt data: string data");
    }
    std::string result = data.substr(pos, len);
    pos += len;
    return result;
}

// Serialize implementations
std::string SpaceDef::Serialize() const {
    std::string result;
    AppendString(result, name);
    result.append(reinterpret_cast<const char*>(&partition_num), sizeof(partition_num));
    result.append(reinterpret_cast<const char*>(&replica_factor), sizeof(replica_factor));
    return result;
}

StatusOr<SpaceDef> SpaceDef::Deserialize(const std::string& data) {
    SpaceDef space;
    size_t pos = 0;
    auto name_result = ReadString(data, pos);
    if (!name_result.ok()) return name_result.status();
    space.name = name_result.value();
    if (pos + sizeof(partition_num) > data.size()) return Status::InvalidArgument("Corrupt SpaceDef");
    std::memcpy(&space.partition_num, &data[pos], sizeof(partition_num));
    pos += sizeof(partition_num);
    if (pos + sizeof(replica_factor) > data.size()) return Status::InvalidArgument("Corrupt SpaceDef");
    std::memcpy(&space.replica_factor, &data[pos], sizeof(replica_factor));
    return space;
}

std::string PartitionAssignment::Serialize() const {
    std::string result;
    result.append(reinterpret_cast<const char*>(&partition_id), sizeof(partition_id));
    AppendString(result, space_name);
    result.append(reinterpret_cast<const char*>(&leader_node), sizeof(leader_node));
    uint32_t follower_count = static_cast<uint32_t>(follower_nodes.size());
    result.append(reinterpret_cast<const char*>(&follower_count), sizeof(follower_count));
    for (auto nid : follower_nodes) {
        result.append(reinterpret_cast<const char*>(&nid), sizeof(nid));
    }
    result.append(reinterpret_cast<const char*>(&version), sizeof(version));
    uint8_t state_val = static_cast<uint8_t>(state);
    result.append(reinterpret_cast<const char*>(&state_val), sizeof(state_val));
    return result;
}

StatusOr<PartitionAssignment> PartitionAssignment::Deserialize(const std::string& data) {
    PartitionAssignment assign;
    size_t pos = 0;
    if (pos + sizeof(partition_id) > data.size()) return Status::InvalidArgument("Corrupt PartitionAssignment");
    std::memcpy(&assign.partition_id, &data[pos], sizeof(partition_id));
    pos += sizeof(partition_id);
    auto space_result = ReadString(data, pos);
    if (!space_result.ok()) return space_result.status();
    assign.space_name = space_result.value();
    if (pos + sizeof(leader_node) > data.size()) return Status::InvalidArgument("Corrupt PartitionAssignment");
    std::memcpy(&assign.leader_node, &data[pos], sizeof(leader_node));
    pos += sizeof(leader_node);
    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt PartitionAssignment");
    uint32_t follower_count;
    std::memcpy(&follower_count, &data[pos], sizeof(follower_count));
    pos += sizeof(follower_count);
    for (uint32_t i = 0; i < follower_count; ++i) {
        if (pos + sizeof(NodeID) > data.size()) return Status::InvalidArgument("Corrupt PartitionAssignment");
        NodeID nid;
        std::memcpy(&nid, &data[pos], sizeof(nid));
        pos += sizeof(nid);
        assign.follower_nodes.push_back(nid);
    }
    if (pos + sizeof(version) > data.size()) return Status::InvalidArgument("Corrupt PartitionAssignment");
    std::memcpy(&assign.version, &data[pos], sizeof(version));
    pos += sizeof(version);
    if (pos + sizeof(uint8_t) > data.size()) return Status::InvalidArgument("Corrupt PartitionAssignment");
    uint8_t state_val;
    std::memcpy(&state_val, &data[pos], sizeof(state_val));
    assign.state = static_cast<State>(state_val);
    return assign;
}

std::string SpacePartitionMap::Serialize() const {
    std::string result;
    AppendString(result, space_name);
    result.append(reinterpret_cast<const char*>(&num_partitions), sizeof(num_partitions));
    result.append(reinterpret_cast<const char*>(&replication_factor), sizeof(replication_factor));
    uint32_t assign_count = static_cast<uint32_t>(assignments.size());
    result.append(reinterpret_cast<const char*>(&assign_count), sizeof(assign_count));
    for (const auto& [pid, assign] : assignments) {
        std::string assign_data = assign.Serialize();
        AppendString(result, assign_data);
    }
    result.append(reinterpret_cast<const char*>(&version), sizeof(version));
    return result;
}

StatusOr<SpacePartitionMap> SpacePartitionMap::Deserialize(const std::string& data) {
    SpacePartitionMap map;
    size_t pos = 0;
    auto name_result = ReadString(data, pos);
    if (!name_result.ok()) return name_result.status();
    map.space_name = name_result.value();
    if (pos + sizeof(num_partitions) > data.size()) return Status::InvalidArgument("Corrupt SpacePartitionMap");
    std::memcpy(&map.num_partitions, &data[pos], sizeof(num_partitions));
    pos += sizeof(num_partitions);
    if (pos + sizeof(replication_factor) > data.size()) return Status::InvalidArgument("Corrupt SpacePartitionMap");
    std::memcpy(&map.replication_factor, &data[pos], sizeof(replication_factor));
    pos += sizeof(replication_factor);
    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt SpacePartitionMap");
    uint32_t assign_count;
    std::memcpy(&assign_count, &data[pos], sizeof(assign_count));
    pos += sizeof(assign_count);
    for (uint32_t i = 0; i < assign_count; ++i) {
        auto assign_data = ReadString(data, pos);
        if (!assign_data.ok()) return assign_data.status();
        auto assign_result = PartitionAssignment::Deserialize(assign_data.value());
        if (!assign_result.ok()) return assign_result.status();
        map.assignments[assign_result.value().partition_id] = assign_result.value();
    }
    if (pos + sizeof(version) > data.size()) return Status::InvalidArgument("Corrupt SpacePartitionMap");
    std::memcpy(&map.version, &data[pos], sizeof(version));
    return map;
}

PartitionID SpacePartitionMap::GetPartitionForKey(const CedarKey& key) const {
    return HashToPartition(key, num_partitions);
}

NodeID SpacePartitionMap::GetLeader(PartitionID pid) const {
    auto it = assignments.find(pid);
    if (it != assignments.end()) return it->second.leader_node;
    return kInvalidNodeID;
}

StatusOr<PartitionAssignment> SpacePartitionMap::GetAssignment(PartitionID pid) const {
    auto it = assignments.find(pid);
    if (it != assignments.end()) return it->second;
    return Status::NotFound("");
}

std::string NodeInfo::Serialize() const {
    std::string result;
    result.append(reinterpret_cast<const char*>(&node_id), sizeof(node_id));
    AppendString(result, address);
    AppendString(result, data_path);
    result.append(reinterpret_cast<const char*>(&num_cpu_cores), sizeof(num_cpu_cores));
    result.append(reinterpret_cast<const char*>(&total_memory_bytes), sizeof(total_memory_bytes));
    result.append(reinterpret_cast<const char*>(&total_disk_bytes), sizeof(total_disk_bytes));
    uint8_t state_val = static_cast<uint8_t>(state);
    result.append(reinterpret_cast<const char*>(&state_val), sizeof(state_val));
    return result;
}

StatusOr<NodeInfo> NodeInfo::Deserialize(const std::string& data) {
    NodeInfo info;
    size_t pos = 0;
    if (pos + sizeof(node_id) > data.size()) return Status::InvalidArgument("Corrupt NodeInfo");
    std::memcpy(&info.node_id, &data[pos], sizeof(node_id));
    pos += sizeof(node_id);
    auto addr_result = ReadString(data, pos);
    if (!addr_result.ok()) return addr_result.status();
    info.address = addr_result.value();
    auto path_result = ReadString(data, pos);
    if (!path_result.ok()) return path_result.status();
    info.data_path = path_result.value();
    if (pos + sizeof(num_cpu_cores) > data.size()) return Status::InvalidArgument("Corrupt NodeInfo");
    std::memcpy(&info.num_cpu_cores, &data[pos], sizeof(num_cpu_cores));
    pos += sizeof(num_cpu_cores);
    if (pos + sizeof(total_memory_bytes) > data.size()) return Status::InvalidArgument("Corrupt NodeInfo");
    std::memcpy(&info.total_memory_bytes, &data[pos], sizeof(total_memory_bytes));
    pos += sizeof(total_memory_bytes);
    if (pos + sizeof(total_disk_bytes) > data.size()) return Status::InvalidArgument("Corrupt NodeInfo");
    std::memcpy(&info.total_disk_bytes, &data[pos], sizeof(total_disk_bytes));
    pos += sizeof(total_disk_bytes);
    if (pos + sizeof(uint8_t) > data.size()) return Status::InvalidArgument("Corrupt NodeInfo");
    uint8_t state_val;
    std::memcpy(&state_val, &data[pos], sizeof(state_val));
    info.state = static_cast<State>(state_val);
    return info;
}

std::string NodeStatus::Serialize() const {
    std::string result;
    result.append(reinterpret_cast<const char*>(&node_id), sizeof(node_id));
    result.append(reinterpret_cast<const char*>(&cpu_usage_percent), sizeof(cpu_usage_percent));
    result.append(reinterpret_cast<const char*>(&memory_usage_percent), sizeof(memory_usage_percent));
    result.append(reinterpret_cast<const char*>(&disk_usage_percent), sizeof(disk_usage_percent));
    result.append(reinterpret_cast<const char*>(&qps), sizeof(qps));
    result.append(reinterpret_cast<const char*>(&latency_ms), sizeof(latency_ms));
    auto timestamp_sec = std::chrono::system_clock::to_time_t(timestamp);
    result.append(reinterpret_cast<const char*>(&timestamp_sec), sizeof(timestamp_sec));
    uint32_t leader_count = static_cast<uint32_t>(leader_partitions.size());
    result.append(reinterpret_cast<const char*>(&leader_count), sizeof(leader_count));
    for (auto pid : leader_partitions) {
        result.append(reinterpret_cast<const char*>(&pid), sizeof(pid));
    }
    uint32_t follower_count = static_cast<uint32_t>(follower_partitions.size());
    result.append(reinterpret_cast<const char*>(&follower_count), sizeof(follower_count));
    for (auto pid : follower_partitions) {
        result.append(reinterpret_cast<const char*>(&pid), sizeof(pid));
    }
    return result;
}

StatusOr<NodeStatus> NodeStatus::Deserialize(const std::string& data) {
    NodeStatus status;
    size_t pos = 0;
    if (pos + sizeof(node_id) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    std::memcpy(&status.node_id, &data[pos], sizeof(node_id));
    pos += sizeof(node_id);
    if (pos + sizeof(cpu_usage_percent) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    std::memcpy(&status.cpu_usage_percent, &data[pos], sizeof(cpu_usage_percent));
    pos += sizeof(cpu_usage_percent);
    if (pos + sizeof(memory_usage_percent) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    std::memcpy(&status.memory_usage_percent, &data[pos], sizeof(memory_usage_percent));
    pos += sizeof(memory_usage_percent);
    if (pos + sizeof(disk_usage_percent) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    std::memcpy(&status.disk_usage_percent, &data[pos], sizeof(disk_usage_percent));
    pos += sizeof(disk_usage_percent);
    if (pos + sizeof(qps) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    std::memcpy(&status.qps, &data[pos], sizeof(qps));
    pos += sizeof(qps);
    if (pos + sizeof(latency_ms) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    std::memcpy(&status.latency_ms, &data[pos], sizeof(latency_ms));
    pos += sizeof(latency_ms);
    if (pos + sizeof(std::time_t) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    std::time_t timestamp_sec;
    std::memcpy(&timestamp_sec, &data[pos], sizeof(timestamp_sec));
    status.timestamp = std::chrono::system_clock::from_time_t(timestamp_sec);
    pos += sizeof(timestamp_sec);
    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    uint32_t leader_count;
    std::memcpy(&leader_count, &data[pos], sizeof(leader_count));
    pos += sizeof(leader_count);
    for (uint32_t i = 0; i < leader_count; ++i) {
        if (pos + sizeof(PartitionID) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
        PartitionID pid;
        std::memcpy(&pid, &data[pos], sizeof(pid));
        pos += sizeof(pid);
        status.leader_partitions.push_back(pid);
    }
    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
    uint32_t follower_count;
    std::memcpy(&follower_count, &data[pos], sizeof(follower_count));
    pos += sizeof(follower_count);
    for (uint32_t i = 0; i < follower_count; ++i) {
        if (pos + sizeof(PartitionID) > data.size()) return Status::InvalidArgument("Corrupt NodeStatus");
        PartitionID pid;
        std::memcpy(&pid, &data[pos], sizeof(pid));
        pos += sizeof(pid);
        status.follower_partitions.push_back(pid);
    }
    return status;
}

std::string PropertyDef::Serialize() const {
    std::string result;
    AppendString(result, name);
    AppendString(result, type);
    result.push_back(nullable ? 1 : 0);
    result.push_back(indexed ? 1 : 0);
    return result;
}

StatusOr<PropertyDef> PropertyDef::Deserialize(const std::string& data) {
    size_t pos = 0;
    PropertyDef def;
    auto name_data = ReadString(data, pos);
    if (!name_data.ok()) return Status::InvalidArgument("Corrupt PropertyDef name");
    def.name = name_data.value();

    auto type_data = ReadString(data, pos);
    if (!type_data.ok()) return Status::InvalidArgument("Corrupt PropertyDef type");
    def.type = type_data.value();

    if (pos + 2 > data.size()) return Status::InvalidArgument("Corrupt PropertyDef flags");
    def.nullable = data[pos++] != 0;
    def.indexed = data[pos++] != 0;
    return def;
}

std::string LabelSchema::Serialize() const {
    std::string result;
    AppendString(result, name);
    uint32_t prop_count = static_cast<uint32_t>(properties.size());
    result.append(reinterpret_cast<const char*>(&prop_count), sizeof(prop_count));
    for (const auto& prop : properties) {
        std::string prop_data = prop.Serialize();
        AppendString(result, prop_data);
    }
    uint32_t idx_count = static_cast<uint32_t>(indexes.size());
    result.append(reinterpret_cast<const char*>(&idx_count), sizeof(idx_count));
    for (const auto& idx : indexes) {
        AppendString(result, idx);
    }
    return result;
}

StatusOr<LabelSchema> LabelSchema::Deserialize(const std::string& data) {
    size_t pos = 0;
    LabelSchema schema;
    auto name_data = ReadString(data, pos);
    if (!name_data.ok()) return Status::InvalidArgument("Corrupt LabelSchema name");
    schema.name = name_data.value();

    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt LabelSchema prop count");
    uint32_t prop_count;
    std::memcpy(&prop_count, &data[pos], sizeof(prop_count));
    pos += sizeof(uint32_t);

    for (uint32_t i = 0; i < prop_count; ++i) {
        auto prop_data = ReadString(data, pos);
        if (!prop_data.ok()) return Status::InvalidArgument("Corrupt LabelSchema property");
        auto prop = PropertyDef::Deserialize(prop_data.value());
        if (!prop.ok()) return prop.status();
        schema.properties.push_back(prop.value());
    }

    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt LabelSchema idx count");
    uint32_t idx_count;
    std::memcpy(&idx_count, &data[pos], sizeof(idx_count));
    pos += sizeof(uint32_t);

    for (uint32_t i = 0; i < idx_count; ++i) {
        auto idx_data = ReadString(data, pos);
        if (!idx_data.ok()) return Status::InvalidArgument("Corrupt LabelSchema index");
        schema.indexes.push_back(idx_data.value());
    }
    return schema;
}

// MetadataStateMachine implementation
void MetadataService::MetadataStateMachine::Apply(const raft::LogEntry& entry) {
    // In real implementation, deserialize entry.data and apply the command
    // For now, just track the index
    last_applied_index_.store(entry.index);
}

raft::Snapshot MetadataService::MetadataStateMachine::CreateSnapshot() {
    raft::Snapshot snapshot;
    snapshot.last_included_index = last_applied_index_.load();
    snapshot.data = Serialize();
    return snapshot;
}

Status MetadataService::MetadataStateMachine::RestoreSnapshot(const raft::Snapshot& snapshot) {
    return Deserialize(snapshot.data);
}

raft::LogIndex MetadataService::MetadataStateMachine::GetLastAppliedIndex() const {
    return last_applied_index_.load();
}

void MetadataService::MetadataStateMachine::ApplyCreateSpace(const SpaceDef& space) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    spaces_[space.name] = space;
    
    // Create partition map for this space
    SpacePartitionMap partition_map;
    partition_map.space_name = space.name;
    partition_map.num_partitions = space.partition_num;
    partition_map.replication_factor = space.replica_factor;
    partition_map.version = 1;
    
    // Get online nodes for partition assignment
    std::vector<NodeID> online_nodes;
    for (const auto& [id, node] : nodes_) {
        if (node.state == NodeInfo::State::kOnline) {
            online_nodes.push_back(id);
        }
    }
    
    // If no nodes are online, we can't assign partitions yet
    // The assignments will be empty and need to be filled later via rebalancing
    if (!online_nodes.empty()) {
        // Sort nodes for deterministic assignment
        std::sort(online_nodes.begin(), online_nodes.end());
        
        // Ensure we have enough nodes for replication factor
        uint32_t effective_replicas = std::min(space.replica_factor,
                                               static_cast<uint32_t>(online_nodes.size()));
        
        // Assign partitions using round-robin
        for (PartitionID pid = 0; pid < space.partition_num; ++pid) {
            PartitionAssignment assign;
            assign.partition_id = pid;
            assign.space_name = space.name;
            assign.version = 1;
            assign.state = PartitionAssignment::State::kNormal;
            
            // Leader: round-robin across nodes
            size_t leader_idx = pid % online_nodes.size();
            assign.leader_node = online_nodes[leader_idx];
            
            // Followers: next (replica_factor - 1) nodes
            for (uint32_t r = 1; r < effective_replicas; ++r) {
                size_t follower_idx = (leader_idx + r) % online_nodes.size();
                assign.follower_nodes.push_back(online_nodes[follower_idx]);
            }
            
            partition_map.assignments[pid] = std::move(assign);
        }
    }
    
    partition_maps_[space.name] = std::move(partition_map);
}

void MetadataService::MetadataStateMachine::ApplyDropSpace(const std::string& space_name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    spaces_.erase(space_name);
    partition_maps_.erase(space_name);
}

void MetadataService::MetadataStateMachine::ApplyRegisterNode(const NodeInfo& info) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    nodes_[info.node_id] = info;
}

void MetadataService::MetadataStateMachine::ApplyUpdateNodeStatus(const NodeStatus& status) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    node_statuses_[status.node_id] = status;
    auto it = nodes_.find(status.node_id);
    if (it != nodes_.end()) {
        it->second.last_heartbeat = status.timestamp;
        it->second.state = NodeInfo::State::kOnline;
    }
}

void MetadataService::MetadataStateMachine::ApplyUpdatePartitionLeader(
    const std::string& space_name, PartitionID partition_id, NodeID new_leader) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = partition_maps_.find(space_name);
    if (it != partition_maps_.end()) {
        auto& partition_map = it->second;
        auto assign_it = partition_map.assignments.find(partition_id);
        if (assign_it != partition_map.assignments.end()) {
            assign_it->second.leader_node = new_leader;
            assign_it->second.version++;
        }
    }
}

StatusOr<SpaceDef> MetadataService::MetadataStateMachine::GetSpace(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = spaces_.find(name);
    if (it != spaces_.end()) return it->second;
    return Status::NotFound("Space not found");
}

StatusOr<PartitionAssignment> MetadataService::MetadataStateMachine::GetPartitionAssignment(
    const std::string& space_name, PartitionID pid) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = partition_maps_.find(space_name);
    if (it != partition_maps_.end()) {
        const auto& partition_map = it->second;
        auto assign_it = partition_map.assignments.find(pid);
        if (assign_it != partition_map.assignments.end()) return assign_it->second;
    }
    return Status::NotFound("Partition not found");
}

StatusOr<SpacePartitionMap> MetadataService::MetadataStateMachine::GetSpacePartitionMap(
    const std::string& space_name) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = partition_maps_.find(space_name);
    if (it != partition_maps_.end()) return it->second;
    return Status::NotFound("Space partition map not found");
}

StatusOr<NodeInfo> MetadataService::MetadataStateMachine::GetNode(NodeID node_id) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = nodes_.find(node_id);
    if (it != nodes_.end()) return it->second;
    return Status::NotFound("Node not found");
}

std::vector<NodeInfo> MetadataService::MetadataStateMachine::GetAliveNodes(uint64_t timeout_sec) const {
    std::vector<NodeInfo> alive_nodes;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto now = std::chrono::system_clock::now();
    for (const auto& [id, node] : nodes_) {
        if (node.state == NodeInfo::State::kOnline) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - node.last_heartbeat).count();
            if (elapsed <= static_cast<int64_t>(timeout_sec)) {
                alive_nodes.push_back(node);
            }
        }
    }
    return alive_nodes;
}

std::vector<std::string> MetadataService::MetadataStateMachine::ListSpaces() const {
    std::vector<std::string> result;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto& [name, space] : spaces_) {
        (void)space;
        result.push_back(name);
    }
    return result;
}

std::vector<NodeInfo> MetadataService::MetadataStateMachine::GetAllNodes() const {
    std::vector<NodeInfo> result;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto& [id, node] : nodes_) {
        (void)id;
        result.push_back(node);
    }
    return result;
}

Status MetadataService::MetadataStateMachine::CreateLabelSchema(
    const std::string& space_name, const LabelSchema& schema) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    schemas_[space_name][schema.name] = schema;
    return Status::OK();
}

std::vector<LabelSchema> MetadataService::MetadataStateMachine::GetSchema(
    const std::string& space_name, const std::vector<std::string>& labels) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<LabelSchema> result;
    auto space_it = schemas_.find(space_name);
    if (space_it == schemas_.end()) return result;

    if (labels.empty()) {
        for (const auto& [_, schema] : space_it->second) {
            result.push_back(schema);
        }
    } else {
        for (const auto& label : labels) {
            auto it = space_it->second.find(label);
            if (it != space_it->second.end()) {
                result.push_back(it->second);
            }
        }
    }
    return result;
}

std::vector<NodeID> MetadataService::MetadataStateMachine::CheckNodeHeartbeats(uint64_t timeout_sec) const {
    std::vector<NodeID> failed_nodes;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto now = std::chrono::system_clock::now();
    for (const auto& [id, node] : nodes_) {
        if (node.state == NodeInfo::State::kOnline) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - node.last_heartbeat).count();
            if (elapsed > static_cast<int64_t>(timeout_sec)) {
                failed_nodes.push_back(id);
            }
        }
    }
    return failed_nodes;
}

bool MetadataService::MetadataStateMachine::MarkNodeOffline(NodeID node_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = nodes_.find(node_id);
    if (it != nodes_.end() && it->second.state == NodeInfo::State::kOnline) {
        it->second.state = NodeInfo::State::kOffline;
        return true;
    }
    return false;
}

std::string MetadataService::MetadataStateMachine::Serialize() const {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::string result;
    // Magic + version
    result.append("CMSN", 4);  // Cedar Meta Snapshot
    uint32_t version = 1;
    result.append(reinterpret_cast<const char*>(&version), sizeof(version));
    
    // spaces
    uint32_t space_count = static_cast<uint32_t>(spaces_.size());
    result.append(reinterpret_cast<const char*>(&space_count), sizeof(space_count));
    for (const auto& [name, space] : spaces_) {
        (void)name;
        std::string space_data = space.Serialize();
        AppendString(result, space_data);
    }
    
    // nodes
    uint32_t node_count = static_cast<uint32_t>(nodes_.size());
    result.append(reinterpret_cast<const char*>(&node_count), sizeof(node_count));
    for (const auto& [id, node] : nodes_) {
        (void)id;
        std::string node_data = node.Serialize();
        AppendString(result, node_data);
    }
    
    // node_statuses
    uint32_t status_count = static_cast<uint32_t>(node_statuses_.size());
    result.append(reinterpret_cast<const char*>(&status_count), sizeof(status_count));
    for (const auto& [id, status] : node_statuses_) {
        (void)id;
        std::string status_data = status.Serialize();
        AppendString(result, status_data);
    }
    
    // partition_maps
    uint32_t map_count = static_cast<uint32_t>(partition_maps_.size());
    result.append(reinterpret_cast<const char*>(&map_count), sizeof(map_count));
    for (const auto& [name, pmap] : partition_maps_) {
        (void)name;
        std::string map_data = pmap.Serialize();
        AppendString(result, map_data);
    }
    
    // schemas
    uint32_t schema_space_count = static_cast<uint32_t>(schemas_.size());
    result.append(reinterpret_cast<const char*>(&schema_space_count), sizeof(schema_space_count));
    for (const auto& [space_name, label_map] : schemas_) {
        AppendString(result, space_name);
        uint32_t label_count = static_cast<uint32_t>(label_map.size());
        result.append(reinterpret_cast<const char*>(&label_count), sizeof(label_count));
        for (const auto& [_, schema] : label_map) {
            std::string schema_data = schema.Serialize();
            AppendString(result, schema_data);
        }
    }
    
    return result;
}

Status MetadataService::MetadataStateMachine::Deserialize(const std::string& data) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    spaces_.clear();
    nodes_.clear();
    node_statuses_.clear();
    partition_maps_.clear();
    schemas_.clear();
    
    size_t pos = 0;
    if (data.size() < 8) return Status::InvalidArgument("Snapshot data too short");
    
    // Magic
    if (std::memcmp(data.data(), "CMSN", 4) != 0) {
        return Status::InvalidArgument("Invalid snapshot magic");
    }
    pos += 4;
    
    // Version
    uint32_t version;
    std::memcpy(&version, &data[pos], sizeof(version));
    pos += sizeof(version);
    if (version != 1) {
        return Status::InvalidArgument("Unsupported snapshot version");
    }
    
    // spaces
    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt snapshot: space count");
    uint32_t space_count;
    std::memcpy(&space_count, &data[pos], sizeof(space_count));
    pos += sizeof(space_count);
    for (uint32_t i = 0; i < space_count; ++i) {
        auto space_data = ReadString(data, pos);
        if (!space_data.ok()) return space_data.status();
        auto space_result = SpaceDef::Deserialize(space_data.value());
        if (!space_result.ok()) return space_result.status();
        spaces_[space_result.value().name] = space_result.value();
    }
    
    // nodes
    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt snapshot: node count");
    uint32_t node_count;
    std::memcpy(&node_count, &data[pos], sizeof(node_count));
    pos += sizeof(node_count);
    for (uint32_t i = 0; i < node_count; ++i) {
        auto node_data = ReadString(data, pos);
        if (!node_data.ok()) return node_data.status();
        auto node_result = NodeInfo::Deserialize(node_data.value());
        if (!node_result.ok()) return node_result.status();
        nodes_[node_result.value().node_id] = node_result.value();
    }
    
    // node_statuses
    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt snapshot: status count");
    uint32_t status_count;
    std::memcpy(&status_count, &data[pos], sizeof(status_count));
    pos += sizeof(status_count);
    for (uint32_t i = 0; i < status_count; ++i) {
        auto status_data = ReadString(data, pos);
        if (!status_data.ok()) return status_data.status();
        auto status_result = NodeStatus::Deserialize(status_data.value());
        if (!status_result.ok()) return status_result.status();
        node_statuses_[status_result.value().node_id] = status_result.value();
    }
    
    // partition_maps
    if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt snapshot: map count");
    uint32_t map_count;
    std::memcpy(&map_count, &data[pos], sizeof(map_count));
    pos += sizeof(map_count);
    for (uint32_t i = 0; i < map_count; ++i) {
        auto map_data = ReadString(data, pos);
        if (!map_data.ok()) return map_data.status();
        auto map_result = SpacePartitionMap::Deserialize(map_data.value());
        if (!map_result.ok()) return map_result.status();
        partition_maps_[map_result.value().space_name] = map_result.value();
    }
    
    // schemas (optional - may not exist in older snapshots)
    if (pos + sizeof(uint32_t) <= data.size()) {
        uint32_t schema_space_count;
        std::memcpy(&schema_space_count, &data[pos], sizeof(schema_space_count));
        pos += sizeof(uint32_t);
        for (uint32_t s = 0; s < schema_space_count; ++s) {
            auto space_name_data = ReadString(data, pos);
            if (!space_name_data.ok()) return Status::InvalidArgument("Corrupt schema space name");
            std::string space_name = space_name_data.value();
            
            if (pos + sizeof(uint32_t) > data.size()) return Status::InvalidArgument("Corrupt label count");
            uint32_t label_count;
            std::memcpy(&label_count, &data[pos], sizeof(label_count));
            pos += sizeof(uint32_t);
            
            for (uint32_t l = 0; l < label_count; ++l) {
                auto schema_data = ReadString(data, pos);
                if (!schema_data.ok()) return Status::InvalidArgument("Corrupt label schema");
                auto schema_result = LabelSchema::Deserialize(schema_data.value());
                if (!schema_result.ok()) return schema_result.status();
                schemas_[space_name][schema_result.value().name] = schema_result.value();
            }
        }
    }
    
    return Status::OK();
}

// MetadataService implementation
MetadataService::MetadataService() = default;
MetadataService::~MetadataService() { Shutdown(); }

Status MetadataService::Initialize(const MetaServiceConfig& config) {
    config_ = config;
    
    // Initialize Raft with braft (production consensus)
    raft_node_ = std::make_unique<BRaftNode>();
    BRaftNode::Options options;
    options.node_id = config.node_id;
    options.listen_address = config.advertise_address.empty() 
        ? config.listen_address : config.advertise_address;
    options.data_path = config.data_dir;
    options.election_timeout_ms = static_cast<int>(config.election_timeout_ms);
    
    // Add self and peers to initial configuration
    options.initial_peers.push_back(options.listen_address);
    for (const auto& peer : config.peers) {
        if (peer.first != config.node_id) {
            options.initial_peers.push_back(peer.second);
        }
    }
    
    auto status = raft_node_->Init(options, this);
    CEDAR_RETURN_IF_ERROR(status);
    
    // Start heartbeat check thread
    running_ = true;
    heartbeat_thread_ = std::thread(&MetadataService::HeartbeatCheckLoop, this);
    
    initialized_ = true;
    return Status::OK();
}

Status MetadataService::Shutdown() {
    if (!initialized_) return Status::OK();
    
    running_ = false;
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    
    if (raft_node_) raft_node_->Shutdown();
    
    initialized_ = false;
    return Status::OK();
}

Status MetadataService::CreateSpace(const SpaceDef& space) {
    RaftCommand cmd;
    cmd.type = RaftCommandType::kCreateSpace;
    cmd.payload = space.Serialize();
    return raft_node_->Propose(cmd);
}

Status MetadataService::DropSpace(const std::string& space_name) {
    RaftCommand cmd;
    cmd.type = RaftCommandType::kDropSpace;
    cmd.payload = space_name;
    return raft_node_->Propose(cmd);
}

StatusOr<SpaceDef> MetadataService::GetSpace(const std::string& space_name) const {
    return state_machine_.GetSpace(space_name);
}

std::vector<std::string> MetadataService::ListSpaces() const {
    return state_machine_.ListSpaces();
}

StatusOr<PartitionAssignment> MetadataService::GetPartitionAssignment(
    const std::string& space_name, PartitionID partition_id) const {
    return state_machine_.GetPartitionAssignment(space_name, partition_id);
}

StatusOr<std::vector<PartitionAssignment>> MetadataService::GetPartitionAssignments(
    const std::string& space_name, const std::vector<PartitionID>& partitions) const {
    std::vector<PartitionAssignment> result;
    for (auto pid : partitions) {
        auto assign = GetPartitionAssignment(space_name, pid);
        if (!assign.ok()) return assign.status();
        result.push_back(assign.value());
    }
    return result;
}

StatusOr<SpacePartitionMap> MetadataService::GetSpacePartitionMap(const std::string& space_name) const {
    return state_machine_.GetSpacePartitionMap(space_name);
}

Status MetadataService::UpdatePartitionLeader(const std::string& space_name, 
                                               PartitionID partition_id, 
                                               NodeID new_leader) {
    RaftCommand cmd;
    cmd.type = RaftCommandType::kUpdateAssignment;
    std::ostringstream oss;
    oss << space_name << "|" << partition_id << "|" << new_leader;
    cmd.payload = oss.str();
    return raft_node_->Propose(cmd);
}

Status MetadataService::RegisterNode(const NodeInfo& info) {
    RaftCommand cmd;
    cmd.type = RaftCommandType::kUpdateNode;
    cmd.payload = info.Serialize();
    return raft_node_->Propose(cmd);
}

Status MetadataService::Heartbeat(const NodeStatus& status) {
    if (!raft_node_) {
        return Status::IOError("Raft node not initialized");
    }
    // Heartbeat must go through Raft consensus to ensure all replicas see
    // the same node status updates. Only the leader can propose.
    if (!raft_node_->IsLeader()) {
        return Status::InvalidArgument("Not leader");
    }
    RaftCommand cmd;
    cmd.type = RaftCommandType::kUpdateNode;
    cmd.payload = status.Serialize();
    return raft_node_->Propose(cmd);
}

StatusOr<NodeInfo> MetadataService::GetNode(NodeID node_id) const {
    return state_machine_.GetNode(node_id);
}

std::vector<NodeInfo> MetadataService::GetAliveNodes() const {
    return state_machine_.GetAliveNodes(config_.heartbeat_timeout_sec);
}

std::vector<NodeInfo> MetadataService::GetAllNodes() const {
    return state_machine_.GetAllNodes();
}

Status MetadataService::CreateLabelSchema(const std::string& space_name, const LabelSchema& schema) {
    return state_machine_.CreateLabelSchema(space_name, schema);
}

std::vector<LabelSchema> MetadataService::GetSchema(const std::string& space_name,
                                                     const std::vector<std::string>& labels) const {
    return state_machine_.GetSchema(space_name, labels);
}

bool MetadataService::IsLeader() const {
    return raft_node_ && raft_node_->IsLeader();
}

NodeID MetadataService::GetLeader() const {
    if (!raft_node_) return kInvalidNodeID;
    auto leader = raft_node_->GetLeaderId();
    return leader.value_or(kInvalidNodeID);
}

void MetadataService::ApplyRaftCommand(const struct RaftCommand& cmd) {
    switch (cmd.type) {
        case RaftCommandType::kCreateSpace: {
            auto space = SpaceDef::Deserialize(cmd.payload);
            if (space.ok()) {
                state_machine_.ApplyCreateSpace(space.value());
            }
            break;
        }
        case RaftCommandType::kDropSpace:
            state_machine_.ApplyDropSpace(cmd.payload);
            break;
        case RaftCommandType::kUpdateNode: {
            auto info = NodeInfo::Deserialize(cmd.payload);
            if (info.ok()) {
                state_machine_.ApplyRegisterNode(info.value());
                break;
            }
            auto status = NodeStatus::Deserialize(cmd.payload);
            if (status.ok()) {
                state_machine_.ApplyUpdateNodeStatus(status.value());
            }
            break;
        }
        case RaftCommandType::kUpdateAssignment: {
            size_t p1 = cmd.payload.find('|');
            size_t p2 = cmd.payload.find('|', p1 + 1);
            if (p1 != std::string::npos && p2 != std::string::npos) {
                try {
                    std::string space_name = cmd.payload.substr(0, p1);
                    unsigned long pid_raw = std::stoul(cmd.payload.substr(p1 + 1, p2 - p1 - 1));
                    unsigned long leader_raw = std::stoul(cmd.payload.substr(p2 + 1));
                    if (pid_raw > std::numeric_limits<PartitionID>::max() ||
                        leader_raw > std::numeric_limits<NodeID>::max()) {
                        std::cerr << "[MetadataService] kUpdateAssignment value out of range"
                                  << std::endl;
                        break;
                    }
                    PartitionID pid = static_cast<PartitionID>(pid_raw);
                    NodeID leader = static_cast<NodeID>(leader_raw);
                    state_machine_.ApplyUpdatePartitionLeader(space_name, pid, leader);
                } catch (const std::exception& e) {
                    std::cerr << "[MetadataService] Invalid kUpdateAssignment payload: "
                              << e.what() << std::endl;
                }
            }
            break;
        }
        default:
            break;
    }
}

void MetadataService::OnBecomeLeader() {
    std::cout << "[MetadataService] Node " << config_.node_id << " became leader" << std::endl;
}

void MetadataService::OnStepDown() {
    std::cout << "[MetadataService] Node " << config_.node_id << " stepped down" << std::endl;
}

void MetadataService::HeartbeatCheckLoop() {
    while (running_) {
        try {
            // Check for node timeouts
            auto failed_nodes = state_machine_.CheckNodeHeartbeats(config_.heartbeat_timeout_sec);
            
            // Mark failed nodes as offline
            for (NodeID node_id : failed_nodes) {
                if (state_machine_.MarkNodeOffline(node_id)) {
                    std::cerr << "[MetaD] Node " << node_id << " marked as OFFLINE "
                              << "(heartbeat timeout after " << config_.heartbeat_timeout_sec << "s)" << std::endl;
                    
                    // Trigger node change callbacks
                    NodeChange change;
                    change.node_id = node_id;
                    change.change_type = NodeChangeType::kNodeLeft;
                    change.timestamp = std::chrono::system_clock::now();
                    
                    std::vector<NodeChangeCallback> callbacks_copy;
                    {
                      std::lock_guard<std::mutex> cb_lock(callbacks_mutex_);
                      callbacks_copy = node_callbacks_;
                    }
                    for (const auto& callback : callbacks_copy) {
                        try {
                            callback(change);
                        } catch (...) {
                            // 回调异常不应导致心跳检测线程崩溃
                        }
                    }
                }
            }
        } catch (...) {
            // 心跳检测循环异常不应导致后台线程崩溃
        }
        
        std::this_thread::sleep_for(std::chrono::seconds(config_.heartbeat_check_interval_sec));
    }
}

void MetadataService::WatchPartitionMap(const std::string& space_name, PartitionChangeCallback callback) {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    partition_callbacks_.push_back({space_name, callback});
}

void MetadataService::WatchNodes(NodeChangeCallback callback) {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    node_callbacks_.push_back(callback);
}

std::string MetadataService::SerializeState() const {
    return state_machine_.Serialize();
}

bool MetadataService::DeserializeState(const std::string& data) {
    return state_machine_.Deserialize(data).ok();
}

} // namespace dtx
} // namespace cedar
