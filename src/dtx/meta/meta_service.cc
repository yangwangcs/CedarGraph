#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_impl.h"
#include <chrono>
#include <thread>
#include <sstream>
#include <iostream>
#include <cstring>
#include "meta_service.pb.h"

namespace cedar {
namespace dtx {

// =============================================================================
// MetaServiceClient base class
// =============================================================================

MetaServiceClient::MetaServiceClient() = default;
MetaServiceClient::~MetaServiceClient() = default;

// =============================================================================
// Protobuf Serialization Helpers
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

namespace {

template <typename Proto>
std::string SerializeProto(const Proto& proto) {
    std::string data = proto.SerializeAsString();
    std::string result;
    uint32_t len = static_cast<uint32_t>(data.size());
    result.append(reinterpret_cast<const char*>(&len), sizeof(len));
    result.append(data);
    return result;
}

template <typename Proto>
bool DeserializeProto(const std::string& data, size_t& pos, Proto* proto) {
    if (pos + sizeof(uint32_t) > data.size()) return false;
    uint32_t len;
    std::memcpy(&len, &data[pos], sizeof(len));
    pos += sizeof(len);
    if (pos + len > data.size()) return false;
    bool ok = proto->ParseFromString(data.substr(pos, len));
    pos += len;
    return ok;
}

cedar::meta::SpaceDef ToProto(const SpaceDef& space) {
    cedar::meta::SpaceDef proto;
    proto.set_name(space.name);
    proto.set_partition_num(space.partition_num);
    proto.set_replica_factor(space.replica_factor);
    proto.set_created_at_unix(std::chrono::system_clock::to_time_t(space.created_at));
    return proto;
}

SpaceDef FromProto(const cedar::meta::SpaceDef& proto) {
    SpaceDef space;
    space.name = proto.name();
    space.partition_num = proto.partition_num();
    space.replica_factor = proto.replica_factor();
    space.created_at = std::chrono::system_clock::from_time_t(proto.created_at_unix());
    return space;
}

cedar::meta::PartitionAssignment ToProto(const PartitionAssignment& assign) {
    cedar::meta::PartitionAssignment proto;
    proto.set_partition_id(assign.partition_id);
    proto.set_space_name(assign.space_name);
    proto.set_leader_node(assign.leader_node);
    for (auto nid : assign.follower_nodes) {
        proto.add_follower_nodes(nid);
    }
    proto.set_version(assign.version);
    proto.set_last_updated_unix(std::chrono::system_clock::to_time_t(assign.last_updated));
    proto.set_state(static_cast<uint32_t>(assign.state));
    return proto;
}

PartitionAssignment FromProto(const cedar::meta::PartitionAssignment& proto) {
    PartitionAssignment assign;
    assign.partition_id = proto.partition_id();
    assign.space_name = proto.space_name();
    assign.leader_node = proto.leader_node();
    for (int i = 0; i < proto.follower_nodes_size(); ++i) {
        assign.follower_nodes.push_back(proto.follower_nodes(i));
    }
    assign.version = proto.version();
    assign.last_updated = std::chrono::system_clock::from_time_t(proto.last_updated_unix());
    assign.state = static_cast<PartitionAssignment::State>(proto.state());
    return assign;
}

cedar::meta::SpacePartitionMap ToProto(const SpacePartitionMap& map) {
    cedar::meta::SpacePartitionMap proto;
    proto.set_space_name(map.space_name);
    proto.set_num_partitions(map.num_partitions);
    proto.set_replication_factor(map.replication_factor);
    proto.set_version(map.version);
    for (const auto& [pid, assign] : map.assignments) {
        (*proto.mutable_assignments())[pid] = ToProto(assign);
    }
    return proto;
}

SpacePartitionMap FromProto(const cedar::meta::SpacePartitionMap& proto) {
    SpacePartitionMap map;
    map.space_name = proto.space_name();
    map.num_partitions = proto.num_partitions();
    map.replication_factor = proto.replication_factor();
    map.version = proto.version();
    for (const auto& [pid, assign_pb] : proto.assignments()) {
        map.assignments[pid] = FromProto(assign_pb);
    }
    return map;
}

cedar::meta::NodeInfo ToProto(const NodeInfo& info) {
    cedar::meta::NodeInfo proto;
    proto.set_node_id(info.node_id);
    proto.set_address(info.address);
    proto.set_data_path(info.data_path);
    proto.set_num_cpu_cores(info.num_cpu_cores);
    proto.set_total_memory_bytes(info.total_memory_bytes);
    proto.set_total_disk_bytes(info.total_disk_bytes);
    proto.set_registered_at_unix(std::chrono::system_clock::to_time_t(info.registered_at));
    proto.set_last_heartbeat_unix(std::chrono::system_clock::to_time_t(info.last_heartbeat));
    switch (info.state) {
        case NodeInfo::State::kOnline: proto.set_state("ONLINE"); break;
        case NodeInfo::State::kOffline: proto.set_state("OFFLINE"); break;
        case NodeInfo::State::kSuspected: proto.set_state("SUSPECTED"); break;
        default:
          std::cerr << "[MetaService] Unknown node state" << std::endl;
          break;
    }
    return proto;
}

NodeInfo FromProto(const cedar::meta::NodeInfo& proto) {
    NodeInfo info;
    info.node_id = proto.node_id();
    info.address = proto.address();
    info.data_path = proto.data_path();
    info.num_cpu_cores = proto.num_cpu_cores();
    info.total_memory_bytes = proto.total_memory_bytes();
    info.total_disk_bytes = proto.total_disk_bytes();
    info.registered_at = std::chrono::system_clock::from_time_t(proto.registered_at_unix());
    info.last_heartbeat = std::chrono::system_clock::from_time_t(proto.last_heartbeat_unix());
    if (proto.state() == "ONLINE") info.state = NodeInfo::State::kOnline;
    else if (proto.state() == "OFFLINE") info.state = NodeInfo::State::kOffline;
    else if (proto.state() == "SUSPECTED") info.state = NodeInfo::State::kSuspected;
    return info;
}

cedar::meta::NodeStatus ToProto(const NodeStatus& status) {
    cedar::meta::NodeStatus proto;
    proto.set_node_id(status.node_id);
    proto.set_cpu_usage_percent(status.cpu_usage_percent);
    proto.set_memory_usage_percent(status.memory_usage_percent);
    proto.set_disk_usage_percent(status.disk_usage_percent);
    proto.set_qps(status.qps);
    proto.set_latency_ms(status.latency_ms);
    for (auto pid : status.leader_partitions) {
        proto.add_leader_partitions(pid);
    }
    for (auto pid : status.follower_partitions) {
        proto.add_follower_partitions(pid);
    }
    proto.set_timestamp_unix(std::chrono::system_clock::to_time_t(status.timestamp));
    return proto;
}

NodeStatus FromProto(const cedar::meta::NodeStatus& proto) {
    NodeStatus status;
    status.node_id = proto.node_id();
    status.cpu_usage_percent = proto.cpu_usage_percent();
    status.memory_usage_percent = proto.memory_usage_percent();
    status.disk_usage_percent = proto.disk_usage_percent();
    status.qps = proto.qps();
    status.latency_ms = proto.latency_ms();
    for (int i = 0; i < proto.leader_partitions_size(); ++i) {
        status.leader_partitions.push_back(proto.leader_partitions(i));
    }
    for (int i = 0; i < proto.follower_partitions_size(); ++i) {
        status.follower_partitions.push_back(proto.follower_partitions(i));
    }
    status.timestamp = std::chrono::system_clock::from_time_t(proto.timestamp_unix());
    return status;
}

cedar::meta::PropertyDef ToProto(const PropertyDef& def) {
    cedar::meta::PropertyDef proto;
    proto.set_name(def.name);
    proto.set_type(def.type);
    proto.set_nullable(def.nullable);
    proto.set_indexed(def.indexed);
    return proto;
}

PropertyDef FromProto(const cedar::meta::PropertyDef& proto) {
    PropertyDef def;
    def.name = proto.name();
    def.type = proto.type();
    def.nullable = proto.nullable();
    def.indexed = proto.indexed();
    return def;
}

cedar::meta::LabelSchema ToProto(const LabelSchema& schema) {
    cedar::meta::LabelSchema proto;
    proto.set_name(schema.name);
    for (const auto& prop : schema.properties) {
        *proto.add_properties() = ToProto(prop);
    }
    for (const auto& idx : schema.indexes) {
        proto.add_indexes(idx);
    }
    return proto;
}

LabelSchema FromProto(const cedar::meta::LabelSchema& proto) {
    LabelSchema schema;
    schema.name = proto.name();
    for (int i = 0; i < proto.properties_size(); ++i) {
        schema.properties.push_back(FromProto(proto.properties(i)));
    }
    for (int i = 0; i < proto.indexes_size(); ++i) {
        schema.indexes.push_back(proto.indexes(i));
    }
    return schema;
}

}  // namespace

std::string SpaceDef::Serialize() const {
    return SerializeProto(ToProto(*this));
}

StatusOr<SpaceDef> SpaceDef::Deserialize(const std::string& data) {
    cedar::meta::SpaceDef proto;
    size_t pos = 0;
    if (!DeserializeProto(data, pos, &proto)) {
        return Status::InvalidArgument("Corrupt SpaceDef");
    }
    return FromProto(proto);
}

std::string PartitionAssignment::Serialize() const {
    return SerializeProto(ToProto(*this));
}

StatusOr<PartitionAssignment> PartitionAssignment::Deserialize(const std::string& data) {
    cedar::meta::PartitionAssignment proto;
    size_t pos = 0;
    if (!DeserializeProto(data, pos, &proto)) {
        return Status::InvalidArgument("Corrupt PartitionAssignment");
    }
    return FromProto(proto);
}

std::string SpacePartitionMap::Serialize() const {
    return SerializeProto(ToProto(*this));
}

StatusOr<SpacePartitionMap> SpacePartitionMap::Deserialize(const std::string& data) {
    cedar::meta::SpacePartitionMap proto;
    size_t pos = 0;
    if (!DeserializeProto(data, pos, &proto)) {
        return Status::InvalidArgument("Corrupt SpacePartitionMap");
    }
    return FromProto(proto);
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
    return SerializeProto(ToProto(*this));
}

StatusOr<NodeInfo> NodeInfo::Deserialize(const std::string& data) {
    cedar::meta::NodeInfo proto;
    size_t pos = 0;
    if (!DeserializeProto(data, pos, &proto)) {
        return Status::InvalidArgument("Corrupt NodeInfo");
    }
    return FromProto(proto);
}

std::string NodeStatus::Serialize() const {
    return SerializeProto(ToProto(*this));
}

StatusOr<NodeStatus> NodeStatus::Deserialize(const std::string& data) {
    cedar::meta::NodeStatus proto;
    size_t pos = 0;
    if (!DeserializeProto(data, pos, &proto)) {
        return Status::InvalidArgument("Corrupt NodeStatus");
    }
    return FromProto(proto);
}

std::string PropertyDef::Serialize() const {
    return SerializeProto(ToProto(*this));
}

StatusOr<PropertyDef> PropertyDef::Deserialize(const std::string& data) {
    cedar::meta::PropertyDef proto;
    size_t pos = 0;
    if (!DeserializeProto(data, pos, &proto)) {
        return Status::InvalidArgument("Corrupt PropertyDef");
    }
    return FromProto(proto);
}

std::string LabelSchema::Serialize() const {
    return SerializeProto(ToProto(*this));
}

StatusOr<LabelSchema> LabelSchema::Deserialize(const std::string& data) {
    cedar::meta::LabelSchema proto;
    size_t pos = 0;
    if (!DeserializeProto(data, pos, &proto)) {
        return Status::InvalidArgument("Corrupt LabelSchema");
    }
    return FromProto(proto);
}

// MetadataStateMachine implementation
void MetadataService::MetadataStateMachine::Apply(const LogEntry& entry) {
    auto cmd_result = MetaCommand::Deserialize(entry.data);
    if (cmd_result.ok()) {
        ApplyCommand(cmd_result.value());
    }
    last_applied_index_.store(entry.index);
}

void MetadataService::MetadataStateMachine::ApplyCommand(const MetaCommand& cmd) {
    switch (cmd.type) {
        case MetaCommandType::kCreateSpace: {
            auto space = SpaceDef::Deserialize(cmd.data);
            if (space.ok()) ApplyCreateSpace(space.value());
            break;
        }
        case MetaCommandType::kDropSpace:
            ApplyDropSpace(cmd.data);
            break;
        case MetaCommandType::kRegisterNode: {
            auto info = NodeInfo::Deserialize(cmd.data);
            if (info.ok()) ApplyRegisterNode(info.value());
            break;
        }
        case MetaCommandType::kUpdateNodeStatus: {
            auto status = NodeStatus::Deserialize(cmd.data);
            if (status.ok()) ApplyUpdateNodeStatus(status.value());
            break;
        }
        case MetaCommandType::kUpdatePartitionLeader: {
            if (cmd.data.size() < sizeof(uint32_t)) break;
            size_t pos = 0;
            auto space_result = ReadString(cmd.data, pos);
            if (!space_result.ok()) break;
            std::string space_name = space_result.value();
            if (pos + sizeof(PartitionID) + sizeof(NodeID) > cmd.data.size()) break;
            PartitionID pid;
            std::memcpy(&pid, cmd.data.data() + pos, sizeof(pid));
            pos += sizeof(pid);
            NodeID leader;
            std::memcpy(&leader, cmd.data.data() + pos, sizeof(leader));
            ApplyUpdatePartitionLeader(space_name, pid, leader);
            break;
        }
        default:
            break;
    }
}

Snapshot MetadataService::MetadataStateMachine::CreateSnapshot() {
    Snapshot snapshot;
    snapshot.last_included_index = last_applied_index_.load();
    snapshot.data = Serialize();
    return snapshot;
}

Status MetadataService::MetadataStateMachine::RestoreSnapshot(const Snapshot& snapshot) {
    return Deserialize(snapshot.data);
}

LogIndex MetadataService::MetadataStateMachine::GetLastAppliedIndex() const {
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
    NodeInfo node = info;
    // Newly registered nodes are considered alive
    node.last_heartbeat = std::chrono::system_clock::now();
    nodes_[node.node_id] = std::move(node);

    // Test-mode fallback: if "default" space exists but has no assignments,
    // auto-assign all partitions to the first registered node.
    auto map_it = partition_maps_.find("default");
    if (map_it != partition_maps_.end() && map_it->second.assignments.empty()) {
        auto& partition_map = map_it->second;
        for (PartitionID pid = 0; pid < partition_map.num_partitions; ++pid) {
            PartitionAssignment assign;
            assign.partition_id = pid;
            assign.space_name = partition_map.space_name;
            assign.leader_node = info.node_id;
            assign.version = 1;
            assign.state = PartitionAssignment::State::kNormal;
            partition_map.assignments[pid] = std::move(assign);
        }
        partition_map.version++;
    }
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

std::pair<uint64_t, NodeID> MetadataService::MetadataStateMachine::ApplyUpdatePartitionLeader(
    const std::string& space_name, PartitionID partition_id, NodeID new_leader) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto node_it = nodes_.find(new_leader);
    if (node_it == nodes_.end()) {
        return {0, kInvalidNodeID};
    }
    auto it = partition_maps_.find(space_name);
    if (it != partition_maps_.end()) {
        auto& partition_map = it->second;
        auto assign_it = partition_map.assignments.find(partition_id);
        if (assign_it != partition_map.assignments.end()) {
            NodeID old_leader = assign_it->second.leader_node;
            assign_it->second.leader_node = new_leader;
            assign_it->second.version++;
            return {assign_it->second.version, old_leader};
        }
    }
    return {0, kInvalidNodeID};
}

std::pair<uint64_t, NodeID> MetadataService::MetadataStateMachine::ApplyUpdatePartitionAssignment(
    const PartitionAssignment& assignment) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = partition_maps_.find(assignment.space_name);
    if (it == partition_maps_.end()) {
        return {0, kInvalidNodeID};
    }
    auto& partition_map = it->second;
    auto assign_it = partition_map.assignments.find(assignment.partition_id);
    if (assign_it == partition_map.assignments.end()) {
        return {0, kInvalidNodeID};
    }
    NodeID old_leader = assign_it->second.leader_node;
    assign_it->second = assignment;
    assign_it->second.version++;
    assign_it->second.last_updated = std::chrono::system_clock::now();
    partition_map.version = std::max(partition_map.version, assignment.version + 1);
    return {assign_it->second.version, old_leader};
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
    uint32_t version = 2;
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
    if (version != 2) {
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

    if (!config_.test_mode) {
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
    } else {
        // Test mode: auto-create default space so partition routing works
        // out of the box for single-node testing.
        SpaceDef default_space;
        default_space.name = "default";
        default_space.partition_num = 32768;
        default_space.replica_factor = 1;
        default_space.created_at = std::chrono::system_clock::now();
        state_machine_.ApplyCreateSpace(default_space);
        std::cout << "[MetaD] Test mode: created default space with "
                  << default_space.partition_num << " partitions" << std::endl;
    }

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
    if (config_.test_mode) {
        state_machine_.ApplyCreateSpace(space);
        return Status::OK();
    }
    RaftCommand cmd;
    cmd.type = RaftCommandType::kCreateSpace;
    cmd.payload = space.Serialize();
    return raft_node_->Propose(cmd);
}

Status MetadataService::DropSpace(const std::string& space_name) {
    if (config_.test_mode) {
        state_machine_.ApplyDropSpace(space_name);
        return Status::OK();
    }
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
    auto node = GetNode(new_leader);
    if (!node.ok()) {
        return Status::InvalidArgument("New leader is not a registered node");
    }
    if (config_.test_mode) {
        auto [version, old_leader] = state_machine_.ApplyUpdatePartitionLeader(space_name, partition_id, new_leader);
        if (version > 0) {
            PartitionMapChange change;
            change.space_name = space_name;
            change.partition_id = partition_id;
            change.change_type = PartitionChangeType::kLeaderChanged;
            change.old_leader = old_leader;
            change.new_leader = new_leader;
            change.version = version;
            change.timestamp = std::chrono::system_clock::now();
            NotifyPartitionChange(change);
        }
        return Status::OK();
    }
    RaftCommand cmd;
    cmd.type = RaftCommandType::kUpdateAssignment;
    std::ostringstream oss;
    oss << space_name << "|" << partition_id << "|" << new_leader;
    cmd.payload = oss.str();
    return raft_node_->Propose(cmd);
}

Status MetadataService::UpdatePartitionAssignment(const PartitionAssignment& assignment) {
    if (config_.test_mode) {
        auto [version, old_leader] = state_machine_.ApplyUpdatePartitionAssignment(assignment);
        if (version > 0) {
            PartitionMapChange change;
            change.space_name = assignment.space_name;
            change.partition_id = assignment.partition_id;
            change.change_type = PartitionChangeType::kLeaderChanged;
            change.old_leader = old_leader;
            change.new_leader = assignment.leader_node;
            change.version = version;
            change.timestamp = std::chrono::system_clock::now();
            NotifyPartitionChange(change);
        }
        return Status::OK();
    }
    RaftCommand cmd;
    cmd.type = RaftCommandType::kUpdateAssignment;
    std::ostringstream oss;
    oss << assignment.space_name << "|" << assignment.partition_id << "|" << assignment.leader_node;
    cmd.payload = oss.str();
    return raft_node_->Propose(cmd);
}

Status MetadataService::RegisterNode(const NodeInfo& info) {
    if (config_.test_mode) {
        state_machine_.ApplyRegisterNode(info);
        return Status::OK();
    }
    RaftCommand cmd;
    cmd.type = RaftCommandType::kUpdateNode;
    cmd.payload = info.Serialize();
    return raft_node_->Propose(cmd);
}

Status MetadataService::Heartbeat(const NodeStatus& status) {
    // Token bucket rate limiting: max 10 proposals/sec per node
    {
        std::lock_guard<std::mutex> lock(heartbeat_tokens_mutex_);
        auto now = std::chrono::steady_clock::now();
        auto& [last_refill, tokens] = heartbeat_tokens_[status.node_id];
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_refill).count();
        tokens = std::min(kMaxHeartbeatsPerSecond,
                          tokens + static_cast<uint32_t>(elapsed_ms * kMaxHeartbeatsPerSecond / 1000));
        last_refill = now;
        if (tokens == 0) {
            return Status::ResourceExhausted("Heartbeat rate limit exceeded");
        }
        --tokens;
    }
    if (config_.test_mode) {
        state_machine_.ApplyUpdateNodeStatus(status);
        return Status::OK();
    }
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
    if (config_.test_mode) return true;
    return raft_node_ && raft_node_->IsLeader();
}

NodeID MetadataService::GetLeader() const {
    if (config_.test_mode) return config_.node_id;
    if (!raft_node_) return kInvalidNodeID;
    auto leader = raft_node_->GetLeaderId();
    return leader.value_or(kInvalidNodeID);
}

bool MetadataService::ApplyRaftCommand(const struct RaftCommand& cmd) {
    switch (cmd.type) {
        case RaftCommandType::kCreateSpace: {
            auto space = SpaceDef::Deserialize(cmd.payload);
            if (!space.ok()) {
                std::cerr << "[MetadataService] Failed to deserialize space: "
                          << space.status().ToString() << std::endl;
                return false;
            }
            state_machine_.ApplyCreateSpace(space.value());
            return true;
        }
        case RaftCommandType::kDropSpace:
            state_machine_.ApplyDropSpace(cmd.payload);
            return true;
        case RaftCommandType::kUpdateNode: {
            auto info = NodeInfo::Deserialize(cmd.payload);
            if (info.ok()) {
                state_machine_.ApplyRegisterNode(info.value());
                return true;
            }
            auto status = NodeStatus::Deserialize(cmd.payload);
            if (status.ok()) {
                state_machine_.ApplyUpdateNodeStatus(status.value());
                return true;
            }
            std::cerr << "[MetadataService] Failed to deserialize node info/status"
                      << std::endl;
            return false;
        }
        case RaftCommandType::kUpdateAssignment: {
            size_t p1 = cmd.payload.find('|');
            size_t p2 = cmd.payload.find('|', p1 + 1);
            if (p1 == std::string::npos || p2 == std::string::npos) {
                std::cerr << "[MetadataService] Invalid kUpdateAssignment format"
                          << std::endl;
                return false;
            }
            try {
                std::string space_name = cmd.payload.substr(0, p1);
                unsigned long pid_raw = std::stoul(cmd.payload.substr(p1 + 1, p2 - p1 - 1));
                unsigned long leader_raw = std::stoul(cmd.payload.substr(p2 + 1));
                if (pid_raw > std::numeric_limits<PartitionID>::max() ||
                    leader_raw > std::numeric_limits<NodeID>::max()) {
                    std::cerr << "[MetadataService] kUpdateAssignment value out of range"
                              << std::endl;
                    return false;
                }
                PartitionID pid = static_cast<PartitionID>(pid_raw);
                NodeID leader = static_cast<NodeID>(leader_raw);
                auto [version, old_leader] = state_machine_.ApplyUpdatePartitionLeader(space_name, pid, leader);
                if (version > 0) {
                    PartitionMapChange change;
                    change.space_name = space_name;
                    change.partition_id = pid;
                    change.change_type = PartitionChangeType::kLeaderChanged;
                    change.old_leader = old_leader;
                    change.new_leader = leader;
                    change.version = version;
                    change.timestamp = std::chrono::system_clock::now();
                    NotifyPartitionChange(change);
                }
                return true;
            } catch (const std::exception& e) {
                std::cerr << "[MetadataService] Invalid kUpdateAssignment payload: "
                          << e.what() << std::endl;
                return false;
            }
        }
        default:
            return true;
    }
}

void MetadataService::OnBecomeLeader() {
    std::cerr << "[MetadataService] Node " << config_.node_id << " became leader" << std::endl;

    // Notify watchers on leader change
    PartitionMapChange change;
    change.space_name = "";
    change.partition_id = kInvalidPartitionID;
    change.change_type = PartitionChangeType::kLeaderChanged;
    change.old_leader = kInvalidNodeID;
    change.new_leader = config_.node_id;
    change.version = 0;
    change.timestamp = std::chrono::system_clock::now();
    NotifyPartitionChange(change);
}

void MetadataService::OnStepDown() {
    std::cerr << "[MetadataService] Node " << config_.node_id << " stepped down" << std::endl;
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
                            std::cerr << "[MetaD] Heartbeat callback exception" << std::endl;
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "[MetaD] HeartbeatCheckLoop exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[MetaD] HeartbeatCheckLoop unknown exception" << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(config_.heartbeat_check_interval_sec));
    }
}

void MetadataService::NotifyPartitionChange(const PartitionMapChange& change) {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    for (const auto& [space_name, callback] : partition_callbacks_) {
        if (space_name.empty() || space_name == change.space_name) {
            try {
                callback(change);
            } catch (...) {
                std::cerr << "[MetaD] Partition change callback exception" << std::endl;
            }
        }
    }
}

void MetadataService::NotifyNodeChange(const NodeChange& change) {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    for (const auto& callback : node_callbacks_) {
        try {
            callback(change);
        } catch (...) {
            std::cerr << "[MetaD] Node change callback exception" << std::endl;
        }
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
