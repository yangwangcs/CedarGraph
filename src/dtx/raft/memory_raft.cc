#include "cedar/dtx/raft/raft_interface.h"
namespace cedar {
namespace dtx {
namespace raft {
Status MemoryRaftNode::Initialize(const RaftConfig& config, StateMachine* state_machine) {
    config_ = config;
    state_machine_ = state_machine;
    leader_id_ = config.node_id;
    members_ = config.peers;
    members_.push_back({config.node_id, config.advertise_address});
    return Status::OK();
}
Status MemoryRaftNode::Shutdown() { return Status::OK(); }
StatusOr<LogIndex> MemoryRaftNode::Propose(const std::string& data) {
    if (!is_leader_.load()) return Status::NotLeader("not leader", "");
    LogIndex index = next_index_.fetch_add(1);
    LogEntry entry(current_term_.load(), index, data);
    if (state_machine_) state_machine_->Apply(entry);
    return index;
}
bool MemoryRaftNode::IsLeader() const { return is_leader_.load(); }
NodeID MemoryRaftNode::GetLeader() const { return leader_id_.load(); }
RaftState MemoryRaftNode::GetState() const { return is_leader_.load() ? RaftState::kLeader : RaftState::kFollower; }
LogTerm MemoryRaftNode::GetTerm() const { return current_term_.load(); }
Status MemoryRaftNode::AddNode(NodeID node_id, const std::string& address) {
    std::lock_guard<std::mutex> lock(mutex_);
    members_.push_back({node_id, address});
    return Status::OK();
}
Status MemoryRaftNode::RemoveNode(NodeID node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    members_.erase(std::remove_if(members_.begin(), members_.end(), [node_id](const auto& p) { return p.first == node_id; }), members_.end());
    return Status::OK();
}
std::vector<std::pair<NodeID, std::string>> MemoryRaftNode::GetMembers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return members_;
}
Status MemoryRaftNode::TriggerSnapshot() { return Status::OK(); }
void MemoryRaftNode::RegisterStateCallback(std::function<void(RaftState, RaftState)> callback) {}
void MemoryRaftNode::RegisterLeaderCallback(std::function<void(NodeID, NodeID)> callback) {}
} // namespace raft
} // namespace dtx
} // namespace cedar
