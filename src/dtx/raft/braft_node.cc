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
#include "cedar/dtx/meta_service.h"

#include <butil/logging.h>
#include <braft/util.h>
#include <braft/storage.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include <fstream>
#include <future>

namespace cedar {
namespace dtx {

// =============================================================================
// MetaRaftStateMachine Implementation
// =============================================================================

MetaRaftStateMachine::MetaRaftStateMachine(MetadataService* meta_service)
    : meta_service_(meta_service) {}

void MetaRaftStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard closure_guard(iter.done());
        
        std::string data = iter.data().to_string();
        
        if (data.size() < sizeof(uint8_t) + sizeof(uint32_t)) {
            LOG(ERROR) << "Log entry too small";
            continue;
        }
        
        uint8_t cmd_type = static_cast<uint8_t>(data[0]);
        
        uint32_t payload_len;
        memcpy(&payload_len, data.data() + sizeof(uint8_t), sizeof(uint32_t));
        
        std::string payload = data.substr(sizeof(uint8_t) + sizeof(uint32_t), payload_len);
        
        RaftCommand cmd;
        cmd.type = static_cast<RaftCommandType>(cmd_type);
        cmd.payload = payload;
        cmd.term = iter.term();
        cmd.index = iter.index();
        
        if (meta_service_) {
            meta_service_->ApplyRaftCommand(cmd);
        }
        
        last_term_ = iter.term();
    }
}

void MetaRaftStateMachine::on_snapshot_save(
        braft::SnapshotWriter* writer, 
        braft::Closure* done) {
    
    std::string snapshot_path = writer->get_path() + "/meta_snapshot.bin";
    
    std::string snapshot_data;
    if (meta_service_) {
        snapshot_data = meta_service_->SerializeState();
    }
    
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
    
    writer->add_file("meta_snapshot.bin");
    
    LOG(INFO) << "MetadataService snapshot saved to " << snapshot_path;
    
    if (done) {
        done->Run();
    }
}

int MetaRaftStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    std::string snapshot_path = reader->get_path() + "/meta_snapshot.bin";
    
    std::ifstream file(snapshot_path, std::ios::binary);
    if (!file) {
        LOG(ERROR) << "Failed to open snapshot file for reading: " << snapshot_path;
        return -1;
    }
    
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::string snapshot_data(size, '\0');
    file.read(snapshot_data.data(), size);
    file.close();
    
    if (meta_service_) {
        meta_service_->DeserializeState(snapshot_data);
    }
    
    LOG(INFO) << "MetadataService snapshot loaded from " << snapshot_path;
    return 0;
}

void MetaRaftStateMachine::on_leader_start(int64_t term) {
    LOG(INFO) << "MetadataService became leader, term=" << term;
}

void MetaRaftStateMachine::on_leader_stop(const butil::Status& status) {
    LOG(INFO) << "MetadataService stopped being leader: " << status.error_str();
}

void MetaRaftStateMachine::on_shutdown() {
    LOG(INFO) << "MetaRaftStateMachine shutting down";
}

void MetaRaftStateMachine::on_error(const braft::Error& e) {
    LOG(ERROR) << "MetaRaftStateMachine error: " << e.status().error_str();
}

void MetaRaftStateMachine::on_configuration_committed(const braft::Configuration& conf) {
    std::vector<braft::PeerId> peer_list;
    conf.list_peers(&peer_list);
    std::ostringstream oss;
    for (auto& peer : peer_list) {
        oss << peer.to_string() << ",";
    }
    LOG(INFO) << "MetadataService configuration committed: " << oss.str();
}

void MetaRaftStateMachine::on_stop_following(const braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "MetadataService stopped following: " << ctx.leader_id().to_string();
}

void MetaRaftStateMachine::on_start_following(const braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "MetadataService started following: " << ctx.leader_id().to_string();
}

// =============================================================================
// BRaftNode::Impl
// =============================================================================

class BRaftNode::Impl {
public:
    Impl() : node_(nullptr), initialized_(false) {}
    
    ~Impl() {
        Shutdown();
    }
    
    ::cedar::Status Init(const BRaftNode::Options& options, MetadataService* meta_service) {
        if (initialized_) {
            return ::cedar::Status::InvalidArgument("Already initialized");
        }
        
        options_ = options;
        meta_service_ = meta_service;
        
        state_machine_ = std::make_unique<MetaRaftStateMachine>(meta_service);
        
        braft::NodeOptions node_options;
        node_options.election_timeout_ms = options.election_timeout_ms;
        node_options.snapshot_interval_s = options.snapshot_interval_s;
        
        for (const auto& peer : options.initial_peers) {
            node_options.initial_conf.add_peer(braft::PeerId(peer));
        }
        
        node_options.fsm = state_machine_.get();
        node_options.node_owns_fsm = false;
        
        node_options.log_uri = "local://" + options.data_path + "/log";
        node_options.raft_meta_uri = "local://" + options.data_path + "/meta";
        node_options.snapshot_uri = "local://" + options.data_path + "/snapshot";
        
        braft::PeerId self(options.listen_address);
        node_ = new braft::Node("meta_service", self);
        
        int ret = node_->init(node_options);
        if (ret != 0) {
            LOG(ERROR) << "Failed to init braft node";
            delete node_;
            node_ = nullptr;
            return ::cedar::Status::IOError("Failed to init braft node");
        }
        
        initialized_ = true;
        return ::cedar::Status::OK();
    }
    
    void Shutdown() {
        if (node_) {
            node_->shutdown(nullptr);
            node_->join();
            delete node_;
            node_ = nullptr;
        }
        initialized_ = false;
    }
    
    bool IsLeader() const {
        return node_ && node_->is_leader();
    }
    
    std::optional<NodeID> GetLeaderId() const {
        if (!node_ || !node_->is_leader()) {
            return std::nullopt;
        }
        return node_->leader_id().idx;
    }
    
    class ProposeClosure : public braft::Closure {
     public:
      explicit ProposeClosure(std::promise<::cedar::Status>* promise) : promise_(promise) {}
      
      void Run() override {
        if (this->status().ok()) {
          promise_->set_value(::cedar::Status::OK());
        } else {
          promise_->set_value(::cedar::Status::IOError("BRaftNode", this->status().error_cstr()));
        }
        delete this;
      }
      
     private:
      std::promise<::cedar::Status>* promise_;
    };
    
    ::cedar::Status Propose(const RaftCommand& command) {
        if (!node_ || !node_->is_leader()) {
            return ::cedar::Status::InvalidArgument("Not a leader");
        }
        
        butil::IOBuf data;
        uint8_t type = static_cast<uint8_t>(command.type);
        data.append(&type, sizeof(type));
        uint32_t len = static_cast<uint32_t>(command.payload.size());
        data.append(&len, sizeof(len));
        data.append(command.payload);
        
        braft::Task task;
        task.data = &data;
        
        std::promise<::cedar::Status> promise;
        task.done = new ProposeClosure(&promise);
        
        node_->apply(task);
        
        auto future = promise.get_future();
        if (future.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
          return ::cedar::Status::IOError("BRaftNode", "propose timeout");
        }
        
        return future.get();
    }
    
    ::cedar::Status AddPeer(const std::string& peer_address) {
        if (!node_ || !node_->is_leader()) {
            return ::cedar::Status::InvalidArgument("Not a leader");
        }
        
        braft::PeerId new_peer(peer_address);
        node_->add_peer(new_peer, nullptr);
        return ::cedar::Status::OK();
    }
    
    ::cedar::Status RemovePeer(const std::string& peer_address) {
        if (!node_ || !node_->is_leader()) {
            return ::cedar::Status::InvalidArgument("Not a leader");
        }
        
        braft::PeerId old_peer(peer_address);
        node_->remove_peer(old_peer, nullptr);
        return ::cedar::Status::OK();
    }
    
    BRaftNode::NodeStatus GetStatus() const {
        BRaftNode::NodeStatus status;
        if (!node_) {
            return status;
        }
        
        status.is_leader = node_->is_leader();
        status.leader_address = node_->leader_id().to_string();
        
        std::vector<braft::PeerId> peers;
        if (node_->list_peers(&peers).ok()) {
            status.peer_count = peers.size();
        }
        
        return status;
    }
    
    bool initialized() const { return initialized_; }
    
private:
    braft::Node* node_;
    MetadataService* meta_service_;
    std::unique_ptr<MetaRaftStateMachine> state_machine_;
    BRaftNode::Options options_;
    bool initialized_;
};

// =============================================================================
// BRaftNode Implementation
// =============================================================================

BRaftNode::BRaftNode() : impl_(std::make_unique<Impl>()) {}
BRaftNode::~BRaftNode() = default;

::cedar::Status BRaftNode::Init(const Options& options, MetadataService* meta_service) {
    return impl_->Init(options, meta_service);
}

void BRaftNode::Shutdown() {
    impl_->Shutdown();
}

bool BRaftNode::IsLeader() const {
    return impl_->IsLeader();
}

std::optional<NodeID> BRaftNode::GetLeaderId() const {
    return impl_->GetLeaderId();
}

::cedar::Status BRaftNode::Propose(const RaftCommand& command) {
    return impl_->Propose(command);
}

::cedar::Status BRaftNode::AddPeer(const std::string& peer_address) {
    return impl_->AddPeer(peer_address);
}

::cedar::Status BRaftNode::RemovePeer(const std::string& peer_address) {
    return impl_->RemovePeer(peer_address);
}

BRaftNode::NodeStatus BRaftNode::GetStatus() const {
    return impl_->GetStatus();
}

}  // namespace dtx
}  // namespace cedar