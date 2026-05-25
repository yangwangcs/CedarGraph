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

#include <gflags/gflags.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <future>
#include <shared_mutex>

DECLARE_int64(raft_propose_timeout_ms);

namespace cedar {
namespace dtx {

namespace {

class RaftCircuitBreaker {
 public:
  bool IsOpen() const {
    return std::chrono::steady_clock::now() < open_until_;
  }

  void RecordSuccess() { consecutive_failures_ = 0; }

  void RecordFailure() {
    ++consecutive_failures_;
    if (consecutive_failures_ >= 5) {
      open_until_ = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    }
  }

 private:
  size_t consecutive_failures_ = 0;
  std::chrono::steady_clock::time_point open_until_;
};

}  // namespace

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
            LOG(ERROR) << "Corrupt log entry: size too small at index=" << iter.index()
                       << " — stepping down";
            iter.set_error_and_rollback();
            return;
        }
        
        uint8_t cmd_type = static_cast<uint8_t>(data[0]);
        
        uint32_t payload_len;
        memcpy(&payload_len, data.data() + sizeof(uint8_t), sizeof(uint32_t));
        
        if (data.size() < sizeof(uint8_t) + sizeof(uint32_t) + payload_len) {
            LOG(ERROR) << "Corrupt log entry: payload truncated at index=" << iter.index()
                       << " — stepping down";
            iter.set_error_and_rollback();
            return;
        }
        
        std::string payload = data.substr(sizeof(uint8_t) + sizeof(uint32_t), payload_len);
        
        RaftCommand cmd;
        cmd.type = static_cast<RaftCommandType>(cmd_type);
        cmd.payload = payload;
        cmd.term = iter.term();
        cmd.index = iter.index();
        
        if (meta_service_) {
            std::lock_guard<std::mutex> lock(sm_mutex_);
            if (!meta_service_->ApplyRaftCommand(cmd)) {
                LOG(ERROR) << "ApplyRaftCommand failed at index=" << iter.index()
                           << " — stepping down";
                iter.set_error_and_rollback();
                return;
            }
        }
        
        last_term_.store(iter.term(), std::memory_order_release);
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
    
    // Atomic snapshot write: write to temp file, fsync, then rename
    std::string temp_path = snapshot_path + ".tmp";
    int fd = ::open(temp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open snapshot temp file: " << temp_path;
        if (done) {
            done->status().set_error(EIO, "Failed to open snapshot temp file");
            done->Run();
        }
        return;
    }
    
    ssize_t written = ::write(fd, snapshot_data.data(), snapshot_data.size());
    if (written < 0 || static_cast<size_t>(written) != snapshot_data.size()) {
        LOG(ERROR) << "Failed to write snapshot data";
        ::close(fd);
        ::unlink(temp_path.c_str());
        if (done) {
            done->status().set_error(EIO, "Failed to write snapshot data");
            done->Run();
        }
        return;
    }
    
    // Ensure data is on physical storage
    #ifdef __APPLE__
        if (fcntl(fd, F_FULLFSYNC) < 0) {
            LOG(ERROR) << "F_FULLFSYNC failed for snapshot";
            ::close(fd);
            ::unlink(temp_path.c_str());
            if (done) {
                done->status().set_error(EIO, "F_FULLFSYNC failed");
                done->Run();
            }
            return;
        }
    #else
        if (fsync(fd) < 0) {
            LOG(ERROR) << "fsync failed for snapshot";
            ::close(fd);
            ::unlink(temp_path.c_str());
            if (done) {
                done->status().set_error(EIO, "fsync failed");
                done->Run();
            }
            return;
        }
    #endif
    
    ::close(fd);
    
    // Atomic rename
    if (::rename(temp_path.c_str(), snapshot_path.c_str()) < 0) {
        LOG(ERROR) << "rename failed for snapshot";
        ::unlink(temp_path.c_str());
        if (done) {
            done->status().set_error(EIO, "rename failed");
            done->Run();
        }
        return;
    }
    
    writer->add_file("meta_snapshot.bin");
    
    LOG(INFO) << "MetadataService snapshot saved to " << snapshot_path;
    
    if (done) {
        done->Run();
    }
}

int MetaRaftStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    std::string snapshot_path = reader->get_path() + "/meta_snapshot.bin";
    
    int fd = ::open(snapshot_path.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open snapshot file for reading: " << snapshot_path;
        return -1;
    }
    
    struct stat st;
    if (fstat(fd, &st) < 0) {
        LOG(ERROR) << "Failed to stat snapshot file";
        ::close(fd);
        return -1;
    }
    
    std::string snapshot_data(st.st_size, '\0');
    ssize_t n = ::read(fd, &snapshot_data[0], st.st_size);
    ::close(fd);
    
    if (n < 0 || n != st.st_size) {
        LOG(ERROR) << "Failed to read complete snapshot data (expected "
                   << st.st_size << ", got " << n << ")";
        return -1;
    }
    
    if (meta_service_) {
        if (!meta_service_->DeserializeState(snapshot_data)) {
            LOG(ERROR) << "Snapshot deserialization failed";
            return -1;
        }
    }
    
    LOG(INFO) << "MetadataService snapshot loaded from " << snapshot_path;
    return 0;
}

void MetaRaftStateMachine::on_leader_start(int64_t term) {
    LOG(INFO) << "MetadataService became leader, term=" << term;
    if (meta_service_) {
        meta_service_->OnBecomeLeader();
    }
}

void MetaRaftStateMachine::on_leader_stop(const butil::Status& status) {
    LOG(INFO) << "MetadataService stopped being leader: " << status.error_str();
    if (meta_service_) {
        meta_service_->OnStepDown();
    }
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
    Impl() : node_(nullptr), meta_service_(nullptr), initialized_(false) {}
    
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
        {
            std::lock_guard<std::mutex> lock(node_mutex_);
            node_ = new braft::Node("meta_service", self);
            
            int ret = node_->init(node_options);
            if (ret != 0) {
                LOG(ERROR) << "Failed to init braft node";
                delete node_;
                node_ = nullptr;
                return ::cedar::Status::IOError("Failed to init braft node");
            }
        }
        
        initialized_ = true;
        return ::cedar::Status::OK();
    }
    
    void Shutdown() {
        {
            std::lock_guard<std::mutex> lock(node_mutex_);
            if (node_) {
                node_->shutdown(nullptr);
                node_->join();
                delete node_;
                node_ = nullptr;
            }
        }
        initialized_ = false;
    }
    
    bool IsLeader() const {
        std::lock_guard<std::mutex> lock(node_mutex_);
        return node_ && node_->is_leader();
    }
    
    std::optional<NodeID> GetLeaderId() const {
        std::lock_guard<std::mutex> lock(node_mutex_);
        if (!node_) {
            return std::nullopt;
        }
        braft::PeerId leader = node_->leader_id();
        if (leader.is_empty()) {
            return std::nullopt;
        }
        return leader.idx;
    }
    
    class ProposeClosure : public braft::Closure {
     public:
      ProposeClosure(std::shared_ptr<std::promise<::cedar::Status>> promise,
                     std::shared_ptr<butil::IOBuf> data_holder)
          : promise_(promise), data_holder_(data_holder) {}

      void Run() override {
        promise_->set_value(
            status().ok() ? ::cedar::Status::OK() : ::cedar::Status::IOError(status().error_str()));
        delete this;
      }

     private:
      std::shared_ptr<std::promise<::cedar::Status>> promise_;
      std::shared_ptr<butil::IOBuf> data_holder_;  // Keeps IOBuf alive until async done
    };
    
    ::cedar::Status Propose(const RaftCommand& command) {
        if (circuit_breaker_.IsOpen()) {
            return ::cedar::Status::IOError("BRaftNode", "circuit breaker open");
        }

        // Build the task outside the lock (no side effects)
        auto data = std::make_shared<butil::IOBuf>();
        uint8_t type = static_cast<uint8_t>(command.type);
        data->append(&type, sizeof(type));
        uint32_t len = static_cast<uint32_t>(command.payload.size());
        data->append(&len, sizeof(len));
        data->append(command.payload);

        braft::Task task;
        task.data = data.get();

        auto promise = std::make_shared<std::promise<::cedar::Status>>();
        task.done = new ProposeClosure(promise, data);  // Closure holds shared_ptr

        // Acquire lock, verify leadership AND node_ validity, then apply atomically
        {
            std::lock_guard<std::mutex> lock(node_mutex_);
            if (!node_ || !node_->is_leader()) {
                return ::cedar::Status::InvalidArgument("Not a leader");
            }
            node_->apply(task);
        }
        
        auto future = promise->get_future();
        if (future.wait_for(std::chrono::milliseconds(FLAGS_raft_propose_timeout_ms)) == std::future_status::timeout) {
          circuit_breaker_.RecordFailure();
          return ::cedar::Status::IOError("BRaftNode", "propose timeout");
        }
        
        auto status = future.get();
        if (!status.ok()) {
          circuit_breaker_.RecordFailure();
        } else {
          circuit_breaker_.RecordSuccess();
        }
        return status;
    }
    
    class PeerChangeClosure : public braft::Closure {
     public:
      explicit PeerChangeClosure(std::shared_ptr<std::promise<::cedar::Status>> promise)
          : promise_(promise) {}

      void Run() override {
        promise_->set_value(
            status().ok() ? ::cedar::Status::OK() : ::cedar::Status::IOError(status().error_str()));
        delete this;
      }

     private:
      std::shared_ptr<std::promise<::cedar::Status>> promise_;
    };

    ::cedar::Status AddPeer(const std::string& peer_address) {
        std::lock_guard<std::mutex> lock(node_mutex_);
        if (!node_ || !node_->is_leader()) {
            return ::cedar::Status::InvalidArgument("Not a leader");
        }
        
        braft::PeerId new_peer(peer_address);
        auto promise = std::make_shared<std::promise<::cedar::Status>>();
        node_->add_peer(new_peer, new PeerChangeClosure(promise));
        
        auto future = promise->get_future();
        if (future.wait_for(std::chrono::milliseconds(FLAGS_raft_propose_timeout_ms)) == std::future_status::timeout) {
          return ::cedar::Status::IOError("BRaftNode", "add_peer timeout");
        }
        return future.get();
    }
    
    ::cedar::Status RemovePeer(const std::string& peer_address) {
        std::lock_guard<std::mutex> lock(node_mutex_);
        if (!node_ || !node_->is_leader()) {
            return ::cedar::Status::InvalidArgument("Not a leader");
        }
        
        braft::PeerId old_peer(peer_address);
        auto promise = std::make_shared<std::promise<::cedar::Status>>();
        node_->remove_peer(old_peer, new PeerChangeClosure(promise));
        
        auto future = promise->get_future();
        if (future.wait_for(std::chrono::milliseconds(FLAGS_raft_propose_timeout_ms)) == std::future_status::timeout) {
          return ::cedar::Status::IOError("BRaftNode", "remove_peer timeout");
        }
        return future.get();
    }
    
    BRaftNode::NodeStatus GetStatus() const {
        std::lock_guard<std::mutex> lock(node_mutex_);
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
    mutable std::mutex node_mutex_;
    MetadataService* meta_service_;
    std::unique_ptr<MetaRaftStateMachine> state_machine_;
    BRaftNode::Options options_;
    bool initialized_;

    RaftCircuitBreaker circuit_breaker_;
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
