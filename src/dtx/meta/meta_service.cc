#include "cedar/dtx/meta_service.h"
#include <chrono>
#include <thread>
#include <sstream>

namespace cedar {
namespace dtx {

// Serialize implementations
std::string SpaceDef::Serialize() const {
    return name + "|" + std::to_string(partition_num) + "|" + std::to_string(replica_factor);
}

StatusOr<SpaceDef> SpaceDef::Deserialize(const std::string& data) {
    SpaceDef space;
    size_t pos1 = data.find('|');
    if (pos1 == std::string::npos) return space;
    space.name = data.substr(0, pos1);
    size_t pos2 = data.find('|', pos1 + 1);
    if (pos2 == std::string::npos) return space;
    space.partition_num = std::stoul(data.substr(pos1 + 1, pos2 - pos1 - 1));
    space.replica_factor = std::stoul(data.substr(pos2 + 1));
    return space;
}

std::string PartitionAssignment::Serialize() const {
    return std::to_string(partition_id) + "|" + space_name + "|" + std::to_string(leader_node);
}

StatusOr<PartitionAssignment> PartitionAssignment::Deserialize(const std::string& data) {
    PartitionAssignment assign;
    size_t pos1 = data.find('|');
    if (pos1 == std::string::npos) return assign;
    assign.partition_id = static_cast<PartitionID>(std::stoul(data.substr(0, pos1)));
    size_t pos2 = data.find('|', pos1 + 1);
    if (pos2 == std::string::npos) return assign;
    assign.space_name = data.substr(pos1 + 1, pos2 - pos1 - 1);
    assign.leader_node = std::stoul(data.substr(pos2 + 1));
    return assign;
}

std::string SpacePartitionMap::Serialize() const {
    return space_name + "|" + std::to_string(num_partitions);
}

StatusOr<SpacePartitionMap> SpacePartitionMap::Deserialize(const std::string& data) {
    SpacePartitionMap map;
    size_t pos = data.find('|');
    if (pos == std::string::npos) return map;
    map.space_name = data.substr(0, pos);
    map.num_partitions = std::stoul(data.substr(pos + 1));
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
    return std::to_string(node_id) + "|" + address;
}

StatusOr<NodeInfo> NodeInfo::Deserialize(const std::string& data) {
    NodeInfo info;
    size_t pos = data.find('|');
    if (pos == std::string::npos) return info;
    info.node_id = std::stoul(data.substr(0, pos));
    info.address = data.substr(pos + 1);
    return info;
}

std::string NodeStatus::Serialize() const {
    return std::to_string(node_id) + "|" + std::to_string(cpu_usage_percent);
}

StatusOr<NodeStatus> NodeStatus::Deserialize(const std::string& data) {
    NodeStatus status;
    size_t pos = data.find('|');
    if (pos == std::string::npos) return status;
    status.node_id = std::stoul(data.substr(0, pos));
    status.cpu_usage_percent = std::stod(data.substr(pos + 1));
    return status;
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
    partition_maps_[space.name] = partition_map;
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

std::string MetadataService::MetadataStateMachine::Serialize() const {
    // Simple serialization - in production use protobuf
    std::string result;
    result += "spaces:" + std::to_string(spaces_.size()) + ";";
    result += "nodes:" + std::to_string(nodes_.size()) + ";";
    return result;
}

Status MetadataService::MetadataStateMachine::Deserialize(const std::string& data) {
    // Simple deserialization
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
    // TODO: implement
    return {};
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
    // Heartbeat can be processed by any node
    state_machine_.ApplyUpdateNodeStatus(status);
    return Status::OK();
}

StatusOr<NodeInfo> MetadataService::GetNode(NodeID node_id) const {
    return state_machine_.GetNode(node_id);
}

std::vector<NodeInfo> MetadataService::GetAliveNodes() const {
    return state_machine_.GetAliveNodes(config_.heartbeat_timeout_sec);
}

std::vector<NodeInfo> MetadataService::GetAllNodes() const {
    // TODO: implement
    return {};
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
                std::string space_name = cmd.payload.substr(0, p1);
                PartitionID pid = std::stoul(cmd.payload.substr(p1 + 1, p2 - p1 - 1));
                NodeID leader = std::stoul(cmd.payload.substr(p2 + 1));
                state_machine_.ApplyUpdatePartitionLeader(space_name, pid, leader);
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
        // Check for node timeouts
        // TODO: implement node failure detection
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

// MetaServiceClient implementation
MetaServiceClient::MetaServiceClient() = default;
MetaServiceClient::~MetaServiceClient() = default;

Status MetaServiceClient::Connect(const std::vector<std::string>& meta_addresses) {
    meta_addresses_ = meta_addresses;
    // TODO: implement actual connection
    return Status::OK();
}

StatusOr<NodeID> MetaServiceClient::GetPartitionLeader(const std::string& space_name, PartitionID partition_id) {
    // Check cache first
    {
        std::shared_lock<std::shared_mutex> lock(cache_mutex_);
        auto it = partition_map_cache_.find(space_name);
        if (it != partition_map_cache_.end() && !it->second.IsExpired()) {
            return it->second.map.GetLeader(partition_id);
        }
    }
    // TODO: fetch from MetaD and update cache
    return kInvalidNodeID;
}

StatusOr<NodeID> MetaServiceClient::GetRouteForKey(const std::string& space_name, const CedarKey& key) {
    // Get partition for key, then get leader
    // TODO: implement
    return kInvalidNodeID;
}

void MetaServiceClient::RefreshCache(const std::string& space_name) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    partition_map_cache_.erase(space_name);
}

StatusOr<PartitionAssignment> MetaServiceClient::GetPartitionAssignment(
    const std::string& space_name, PartitionID partition_id) {
    // TODO: implement RPC call
    return PartitionAssignment();
}

StatusOr<SpacePartitionMap> MetaServiceClient::GetSpacePartitionMap(const std::string& space_name) {
    // TODO: implement RPC call
    return SpacePartitionMap();
}

StatusOr<NodeInfo> MetaServiceClient::GetNode(NodeID node_id) {
    // TODO: implement RPC call
    return NodeInfo();
}

Status MetaServiceClient::RegisterNode(const NodeInfo& info) {
    // TODO: implement RPC call
    return Status::OK();
}

Status MetaServiceClient::Heartbeat(const NodeStatus& status) {
    // TODO: implement RPC call
    return Status::OK();
}

void MetaServiceClient::WatchPartitionMap(const std::string& space_name,
                                          std::function<void(const PartitionMapChange&)> callback) {
    // TODO: implement watch
}

std::string MetadataService::SerializeState() const {
    return state_machine_.Serialize();
}

bool MetadataService::DeserializeState(const std::string& data) {
    return state_machine_.Deserialize(data).ok();
}

} // namespace dtx
} // namespace cedar
