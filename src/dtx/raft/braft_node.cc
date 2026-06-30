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

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <future>
#include <shared_mutex>
#include <thread>

DECLARE_int64(raft_propose_timeout_ms);

namespace cedar {
namespace dtx {

namespace {

bool ResolveRaftAddress(const std::string& address, std::string* resolved, std::string* error) {
    const auto colon = address.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= address.size()) {
        if (error) {
            *error = "Raft address must be host:port: " + address;
        }
        return false;
    }

    const std::string host = address.substr(0, colon);
    const std::string port = address.substr(colon + 1);

    in_addr ipv4_addr{};
    if (inet_pton(AF_INET, host.c_str(), &ipv4_addr) == 1) {
        if (resolved) {
            *resolved = address;
        }
        return true;
    }

    // Docker DNS may not expose every peer at the exact instant all metad
    // containers start. Wait briefly so a parallel Raft bootstrap does not fail
    // on a transient hostname miss.
    for (int attempt = 0; attempt < 120; ++attempt) {
        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* result = nullptr;
        const int gai_rc = getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
        if (gai_rc == 0 && result != nullptr) {
            char ip[NI_MAXHOST] = {0};
            const int name_rc = getnameinfo(result->ai_addr, result->ai_addrlen, ip, sizeof(ip),
                                            nullptr, 0, NI_NUMERICHOST);
            freeaddrinfo(result);
            if (name_rc == 0 && ip[0] != '\0') {
                if (resolved) {
                    *resolved = std::string(ip) + ":" + port;
                }
                return true;
            }
        } else if (result != nullptr) {
            freeaddrinfo(result);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    if (error) {
        *error = "Raft address DNS did not resolve to IPv4 before braft init: " + address;
    }
    return false;
}

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
  std::chrono::steady_clock::time_point open_until_{std::chrono::steady_clock::time_point::min()};
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
    
    // Robust write: loop until all data is persisted or an error occurs
    size_t total_written = 0;
    const char* data_ptr = snapshot_data.data();
    const size_t data_size = snapshot_data.size();
    while (total_written < data_size) {
        ssize_t written = ::write(fd, data_ptr + total_written, data_size - total_written);
        if (written < 0) {
            if (errno == EINTR) continue;
            LOG(ERROR) << "Failed to write snapshot data";
            ::close(fd);
            ::unlink(temp_path.c_str());
            if (done) {
                done->status().set_error(EIO, "Failed to write snapshot data");
                done->Run();
            }
            return;
        }
        total_written += static_cast<size_t>(written);
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
    size_t total_read = 0;
    while (total_read < static_cast<size_t>(st.st_size)) {
        ssize_t n = ::read(fd, &snapshot_data[total_read], st.st_size - total_read);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG(ERROR) << "Failed to read snapshot data";
            ::close(fd);
            return -1;
        }
        if (n == 0) {
            LOG(ERROR) << "Unexpected EOF reading snapshot data (expected "
                       << st.st_size << ", got " << total_read << ")";
            ::close(fd);
            return -1;
        }
        total_read += static_cast<size_t>(n);
    }
    ::close(fd);
    
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

        // Start brpc server first — braft requires the RPC server to be
        // listening before the raft node can be initialized.
        server_ = std::make_unique<brpc::Server>();
        if (braft::add_service(server_.get(), options.listen_address.c_str()) != 0) {
            return ::cedar::Status::IOError(
                "Failed to add braft service to brpc server on " + options.listen_address);
        }
        if (server_->Start(options.listen_address.c_str(), nullptr) != 0) {
            return ::cedar::Status::IOError(
                "Failed to start brpc server on " + options.listen_address);
        }
        
        braft::NodeOptions node_options;
        node_options.election_timeout_ms = options.election_timeout_ms;
        node_options.snapshot_interval_s = options.snapshot_interval_s;
        
        for (const auto& peer : options.initial_peers) {
            std::string resolved_peer;
            std::string resolve_error;
            if (!ResolveRaftAddress(peer, &resolved_peer, &resolve_error)) {
                return ::cedar::Status::IOError(resolve_error);
            }
            braft::PeerId peer_id;
            if (peer_id.parse(resolved_peer) != 0) {
                return ::cedar::Status::InvalidArgument(
                    "Invalid braft peer address: " + peer + " resolved to " + resolved_peer);
            }
            node_options.initial_conf.add_peer(peer_id);
        }
        
        node_options.fsm = state_machine_.get();
        node_options.node_owns_fsm = false;
        
        node_options.log_uri = "local://" + options.data_path + "/log";
        node_options.raft_meta_uri = "local://" + options.data_path + "/meta";
        node_options.snapshot_uri = "local://" + options.data_path + "/snapshot";
        
        const std::string self_address = options.advertise_address.empty()
            ? options.listen_address
            : options.advertise_address;
        std::string resolved_self_address;
        std::string self_resolve_error;
        if (!ResolveRaftAddress(self_address, &resolved_self_address, &self_resolve_error)) {
            return ::cedar::Status::IOError(self_resolve_error);
        }
        braft::PeerId self;
        if (self.parse(resolved_self_address) != 0) {
            return ::cedar::Status::InvalidArgument(
                "Invalid braft self address: " + self_address +
                " resolved to " + resolved_self_address);
        }
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
        if (server_) {
            server_->Stop(0);
            server_->Join();
            server_.reset();
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
    
    std::string GetLeaderAddress() const {
        std::lock_guard<std::mutex> lock(node_mutex_);
        if (!node_) {
            return "";
        }
        braft::PeerId leader = node_->leader_id();
        if (leader.is_empty()) {
            return "";
        }
        std::ostringstream oss;
        oss << leader.addr;
        return oss.str();
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
        auto future = promise->get_future();
        
        // Acquire lock, verify leadership, apply task, then release before waiting
        {
            std::lock_guard<std::mutex> lock(node_mutex_);
            if (!node_ || !node_->is_leader()) {
                return ::cedar::Status::InvalidArgument("Not a leader");
            }
            task.done = new ProposeClosure(promise, data);
            node_->apply(task);
        }
        
        // Wait for Raft apply outside the lock
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
        std::shared_ptr<std::promise<::cedar::Status>> promise;
        {
            std::lock_guard<std::mutex> lock(node_mutex_);
            if (!node_ || !node_->is_leader()) {
                return ::cedar::Status::InvalidArgument("Not a leader");
            }
            
            braft::PeerId new_peer(peer_address);
            promise = std::make_shared<std::promise<::cedar::Status>>();
            node_->add_peer(new_peer, new PeerChangeClosure(promise));
        }
        // Release lock before waiting to avoid blocking other operations
        auto future = promise->get_future();
        if (future.wait_for(std::chrono::milliseconds(FLAGS_raft_propose_timeout_ms)) == std::future_status::timeout) {
          return ::cedar::Status::IOError("BRaftNode", "add_peer timeout");
        }
        return future.get();
    }
    
    ::cedar::Status RemovePeer(const std::string& peer_address) {
        std::shared_ptr<std::promise<::cedar::Status>> promise;
        {
            std::lock_guard<std::mutex> lock(node_mutex_);
            if (!node_ || !node_->is_leader()) {
                return ::cedar::Status::InvalidArgument("Not a leader");
            }
            
            braft::PeerId old_peer(peer_address);
            promise = std::make_shared<std::promise<::cedar::Status>>();
            node_->remove_peer(old_peer, new PeerChangeClosure(promise));
        }
        // Release lock before waiting to avoid blocking other operations
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
    std::unique_ptr<brpc::Server> server_;
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

std::string BRaftNode::GetLeaderAddress() const {
    return impl_->GetLeaderAddress();
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
