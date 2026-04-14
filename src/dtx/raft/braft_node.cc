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

#include "cedar/dtx/raft/braft_node.h"

#ifdef CEDAR_WITH_BRAFT

#include <braft/util.h>
#include <braft/storage.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include <fstream>
#include <sstream>

namespace cedar {
namespace dtx {

// =============================================================================
// MetaRaftStateMachine Implementation
// =============================================================================

MetaRaftStateMachine::MetaRaftStateMachine(MetaService* meta_service)
    : meta_service_(meta_service) {}

void MetaRaftStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard closure_guard(iter.done());
        
        // Deserialize command from log data
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        
        // Read command type (first byte)
        uint8_t cmd_type;
        if (!wrapper.ReadRaw(&cmd_type, sizeof(cmd_type))) {
            LOG(ERROR) << "Failed to read command type from log";
            continue;
        }
        
        // Read payload length
        uint32_t payload_len;
        if (!wrapper.ReadRaw(&payload_len, sizeof(payload_len))) {
            LOG(ERROR) << "Failed to read payload length from log";
            continue;
        }
        
        // Read payload
        std::string payload;
        payload.resize(payload_len);
        if (!wrapper.ReadRaw(&payload[0], payload_len)) {
            LOG(ERROR) << "Failed to read payload from log";
            continue;
        }
        
        RaftCommand cmd;
        cmd.type = static_cast<RaftCommandType>(cmd_type);
        cmd.payload = payload;
        cmd.term = iter.term();
        cmd.index = iter.index();
        
        // Apply to MetaService
        meta_service_->ApplyRaftCommand(cmd);
        
        last_term_ = iter.term();
    }
}

void MetaRaftStateMachine::on_snapshot_save(
        braft::SnapshotWriter* writer, 
        braft::Closure* done) {
    
    // Save MetaService state to snapshot file
    std::string snapshot_path = writer->get_path();
    snapshot_path += "/meta_snapshot.bin";
    
    // Serialize MetaService state
    std::string snapshot_data = meta_service_->SerializeState();
    
    std::ofstream file(snapshot_path, std::ios::binary);
    if (!file) {
        LOG(ERROR) << "Failed to open snapshot file for writing: " << snapshot_path;
        if (done) {
            done->status().set_error(EIO, "Failed to open snapshot file");
            done->Run();
        }
        return;
    }
    
    file.write(snapshot_data.data(), snapshot_data.size());
    file.close();
    
    // Add file to snapshot
    if (writer->add_file("meta_snapshot.bin") != 0) {
        LOG(ERROR) << "Failed to add file to snapshot";
        if (done) {
            done->status().set_error(EIO, "Failed to add file to snapshot");
            done->Run();
        }
        return;
    }
    
    LOG(INFO) << "Snapshot saved successfully, size=" << snapshot_data.size();
    
    if (done) {
        done->Run();
    }
}

int MetaRaftStateMachine::on_snapshot_load(
        braft::SnapshotReader* reader) {
    
    std::string snapshot_path = reader->get_path();
    snapshot_path += "/meta_snapshot.bin";
    
    std::ifstream file(snapshot_path, std::ios::binary | std::ios::ate);
    if (!file) {
        LOG(ERROR) << "Failed to open snapshot file for reading: " << snapshot_path;
        return -1;
    }
    
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::string snapshot_data(size, '\0');
    if (!file.read(&snapshot_data[0], size)) {
        LOG(ERROR) << "Failed to read snapshot file";
        return -1;
    }
    file.close();
    
    // Deserialize and apply to MetaService
    if (!meta_service_->DeserializeState(snapshot_data)) {
        LOG(ERROR) << "Failed to deserialize snapshot";
        return -1;
    }
    
    LOG(INFO) << "Snapshot loaded successfully, size=" << size;
    return 0;
}

void MetaRaftStateMachine::on_leader_start(int64_t term) {
    last_term_ = term;
    LOG(INFO) << "Node becomes leader, term=" << term;
    meta_service_->OnBecomeLeader();
}

void MetaRaftStateMachine::on_leader_stop(const butil::Status& status) {
    LOG(INFO) << "Node steps down from leader: " << status.error_str();
    meta_service_->OnStepDown();
}

void MetaRaftStateMachine::on_shutdown() {
    LOG(INFO) << "State machine shutting down";
}

void MetaRaftStateMachine::on_error(const braft::Error& e) {
    LOG(ERROR) << "Raft error: type=" << e.type() 
               << ", status=" << e.status().error_str();
}

void MetaRaftStateMachine::on_configuration_committed(
        const braft::Configuration& conf) {
    LOG(INFO) << "Configuration committed: " << conf;
}

void MetaRaftStateMachine::on_stop_following(
        const braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Stopped following leader: " << ctx;
}

void MetaRaftStateMachine::on_start_following(
        const braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Started following leader: " << ctx;
}

// =============================================================================
// BRaftNode Implementation
// =============================================================================

class BRaftNode::Impl {
 public:
  std::unique_ptr<MetaRaftStateMachine> fsm_;
  std::unique_ptr<braft::Node> node_;
  braft::NodeOptions node_options_;
  MetaService* meta_service_ = nullptr;
  std::function<void(bool is_leader, int64_t term)> leader_change_callback_;
  
  butil::EndPoint listen_endpoint_;
};

BRaftNode::BRaftNode() : impl_(std::make_unique<Impl>()) {}

BRaftNode::~BRaftNode() {
  Shutdown();
}

Status BRaftNode::Init(const Options& options, MetaService* meta_service) {
  impl_->meta_service_ = meta_service;
  
  // Parse listen address
  std::vector<std::string> parts;
  std::stringstream ss(options.listen_address);
  std::string part;
  while (std::getline(ss, part, ':')) {
    parts.push_back(part);
  }
  
  if (parts.size() != 2) {
    return Status::InvalidArgument("Invalid listen address format, expected host:port");
  }
  
  std::string host = parts[0];
  int port = std::stoi(parts[1]);
  
  if (butil::str2endpoint(host.c_str(), port, &impl_->listen_endpoint_) != 0) {
    return Status::InvalidArgument("Failed to parse endpoint");
  }
  
  // Create state machine
  impl_->fsm_ = std::make_unique<MetaRaftStateMachine>(meta_service);
  
  // Configure Raft node
  impl_->node_options_.election_timeout_ms = options.election_timeout_ms;
  impl_->node_options_.fsm = impl_->fsm_.get();
  impl_->node_options_.node_owns_fsm = false;
  
  // Set up storage paths
  std::string data_path = options.data_path;
  impl_->node_options_.log_uri = "local://" + data_path + "/log";
  impl_->node_options_.raft_meta_uri = "local://" + data_path + "/meta";
  impl_->node_options_.snapshot_uri = "local://" + data_path + "/snapshot";
  
  // Snapshot configuration
  impl_->node_options_.snapshot_interval_s = options.snapshot_interval_s;
  
  // Parse initial configuration
  braft::Configuration conf;
  for (const auto& peer : options.initial_peers) {
    if (braft::PeerId peer_id;
        peer_id.parse(peer) == 0) {
      conf.add_peer(peer_id);
    } else {
      LOG(WARNING) << "Failed to parse peer address: " << peer;
    }
  }
  impl_->node_options_.initial_conf = conf;
  
  // Create and start node
  impl_->node_.reset(new braft::Node("MetaD", impl_->listen_endpoint_));
  
  if (impl_->node_->init(impl_->node_options_) != 0) {
    return Status::IOError("Failed to initialize braft node");
  }
  
  LOG(INFO) << "BRaftNode initialized successfully, listening on " 
            << options.listen_address;
  
  return Status::OK();
}

void BRaftNode::Shutdown() {
  if (impl_->node_) {
    LOG(INFO) << "Shutting down braft node...";
    impl_->node_->shutdown(nullptr);
    impl_->node_->join();
    impl_->node_.reset();
  }
  impl_->fsm_.reset();
}

bool BRaftNode::IsLeader() const {
  return impl_->node_ && impl_->node_->is_leader();
}

std::optional<NodeId> BRaftNode::GetLeaderId() const {
  if (!impl_->node_) {
    return std::nullopt;
  }
  
  braft::PeerId leader_id;
  if (impl_->node_->leader_id(&leader_id) != 0) {
    return std::nullopt;
  }
  
  // Parse NodeId from leader address
  // This is simplified - real implementation should maintain node ID mapping
  return static_cast<NodeId>(leader_id.addr.port);  // Use port as ID for now
}

Status BRaftNode::Propose(const RaftCommand& command) {
  if (!IsLeader()) {
    auto leader_id = GetLeaderId();
    std::string msg = "Not leader";
    if (leader_id.has_value()) {
      msg += ", leader is node " + std::to_string(leader_id.value());
    }
    return Status::NotLeader(msg);
  }
  
  // Serialize command
  butil::IOBuf log;
  
  // Write command type
  uint8_t cmd_type = static_cast<uint8_t>(command.type);
  log.append(&cmd_type, sizeof(cmd_type));
  
  // Write payload length
  uint32_t payload_len = command.payload.size();
  log.append(&payload_len, sizeof(payload_len));
  
  // Write payload
  log.append(command.payload);
  
  // Create task
  braft::Task task;
  task.data = &log;
  task.done = nullptr;
  
  impl_->node_->apply(task);
  
  return Status::OK();
}

Status BRaftNode::AddPeer(const std::string& peer_address) {
  if (!IsLeader()) {
    return Status::NotLeader("Not leader");
  }
  
  braft::PeerId peer_id;
  if (peer_id.parse(peer_address) != 0) {
    return Status::InvalidArgument("Invalid peer address: " + peer_address);
  }
  
  butil::Status status = impl_->node_->add_peer(peer_id);
  if (!status.ok()) {
    return Status::IOError("Failed to add peer: " + status.error_str());
  }
  
  return Status::OK();
}

Status BRaftNode::RemovePeer(const std::string& peer_address) {
  if (!IsLeader()) {
    return Status::NotLeader("Not leader");
  }
  
  braft::PeerId peer_id;
  if (peer_id.parse(peer_address) != 0) {
    return Status::InvalidArgument("Invalid peer address: " + peer_address);
  }
  
  butil::Status status = impl_->node_->remove_peer(peer_id);
  if (!status.ok()) {
    return Status::IOError("Failed to remove peer: " + status.error_str());
  }
  
  return Status::OK();
}

BRaftNode::Status BRaftNode::GetStatus() const {
  Status status;
  
  if (!impl_->node_) {
    status.is_leader = false;
    status.term = 0;
    status.committed_index = 0;
    status.applied_index = 0;
    status.peer_count = 0;
    return status;
  }
  
  status.is_leader = impl_->node_->is_leader();
  
  // Get node status
  std::vector<braft::NodeStatus> node_statuses;
  impl_->node_->get_status(&node_statuses);
  
  if (!node_statuses.empty()) {
    const auto& ns = node_statuses[0];
    status.term = ns.term;
    status.committed_index = ns.committed_index;
    status.applied_index = ns.known_applied_index;
  }
  
  // Get leader address
  braft::PeerId leader_id;
  if (impl_->node_->leader_id(&leader_id) == 0) {
    status.leader_address = leader_id.to_string();
  }
  
  // Get peer count from configuration
  braft::ConfigurationEntry conf;
  impl_->node_->get_configuration(&conf);
  status.peer_count = conf.conf.peer_size();
  
  return status;
}

void BRaftNode::SetLeaderChangeCallback(
    std::function<void(bool is_leader, int64_t term)> callback) {
  impl_->leader_change_callback_ = callback;
  
  // Note: In a full implementation, we'd need to hook into the 
  // state machine's on_leader_start/on_leader_stop to call this callback
}

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_WITH_BRAFT
