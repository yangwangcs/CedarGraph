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

// =============================================================================
// CedarGraph Metadata Server (metad)
// =============================================================================

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <csignal>
#include <atomic>
#include <chrono>

#include "cedar/dtx/meta_service_impl.h"
#include "cedar/dtx/raft/embedded_raft.h"
#include "cedar/dtx/raft/grpc_transport.h"
#include "cedar/core/status.h"
#include "raft.grpc.pb.h"

namespace cedar {
namespace dtx {

namespace grpc_pb = ::cedar::dtx::raft::grpc;

// Global flag for graceful shutdown
std::atomic<bool> g_running{true};
std::unique_ptr<grpc::Server> g_grpc_server;

void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cout << "\nShutting down metad server..." << std::endl;
    g_running = false;
    if (g_grpc_server) {
      g_grpc_server->Shutdown();
    }
  }
}

struct ServerConfig {
  uint32_t node_id = 0;
  std::string bind_address = "0.0.0.0:6000";
  std::string data_dir = "/tmp/cedar/metad";
  std::vector<std::pair<uint32_t, std::string>> peers;
  
  // Raft settings
  uint64_t election_timeout_min_ms = 150;
  uint64_t election_timeout_max_ms = 300;
  uint64_t heartbeat_interval_ms = 50;
  
  // gRPC settings
  uint64_t rpc_timeout_ms = 1000;
};

ServerConfig ParseConfigFile(const std::string& path) {
  ServerConfig config;
  
  std::ifstream file(path);
  if (!file.is_open()) {
    std::cerr << "Failed to open config file: " << path << std::endl;
    return config;
  }
  
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty() || line[0] == '#') continue;
    
    auto pos = line.find('=');
    if (pos == std::string::npos) continue;
    
    std::string key = line.substr(0, pos);
    std::string value = line.substr(pos + 1);
    
    auto trim = [](std::string& s) {
      s.erase(0, s.find_first_not_of(" \t"));
      s.erase(s.find_last_not_of(" \t") + 1);
    };
    trim(key);
    trim(value);
    
    if (key == "node_id") {
      config.node_id = std::stoul(value);
    } else if (key == "bind_address") {
      config.bind_address = value;
    } else if (key == "data_dir") {
      config.data_dir = value;
    } else if (key == "peers") {
      size_t start = 0;
      while (start < value.size()) {
        size_t end = value.find(',', start);
        if (end == std::string::npos) end = value.size();
        
        std::string peer = value.substr(start, end - start);
        size_t colon = peer.find(':');
        
        if (colon != std::string::npos) {
          uint32_t id = std::stoul(peer.substr(0, colon));
          std::string addr = peer.substr(colon + 1);
          config.peers.push_back({id, addr});
        }
        
        start = end + 1;
      }
    } else if (key == "election_timeout_min_ms") {
      config.election_timeout_min_ms = std::stoul(value);
    } else if (key == "election_timeout_max_ms") {
      config.election_timeout_max_ms = std::stoul(value);
    } else if (key == "heartbeat_interval_ms") {
      config.heartbeat_interval_ms = std::stoul(value);
    } else if (key == "rpc_timeout_ms") {
      config.rpc_timeout_ms = std::stoul(value);
    }
  }
  
  return config;
}

// =============================================================================
// gRPC Raft Service Implementation
// =============================================================================

class RaftGrpcServiceImpl final : public grpc_pb::RaftService::Service {
 public:
  explicit RaftGrpcServiceImpl(raft::EmbeddedRaftNode* node) : node_(node) {}

  grpc::Status RequestVote(grpc::ServerContext* context,
                           const grpc_pb::GrpcVoteRequest* request,
                           grpc_pb::GrpcVoteResponse* response) override {
    (void)context;
    
    if (!node_) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Node not initialized");
    }

    raft::VoteRequest req;
    req.term = request->term();
    req.candidate_id = request->candidate_id();
    req.last_log_index = request->last_log_index();
    req.last_log_term = request->last_log_term();

    raft::VoteResponse resp = node_->HandleVoteRequest(req);
    
    response->set_term(resp.term);
    response->set_vote_granted(resp.vote_granted);
    
    return grpc::Status::OK;
  }

  grpc::Status AppendEntries(grpc::ServerContext* context,
                             const grpc_pb::GrpcAppendEntriesRequest* request,
                             grpc_pb::GrpcAppendEntriesResponse* response) override {
    (void)context;
    
    if (!node_) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Node not initialized");
    }

    raft::AppendEntriesRequest req;
    req.term = request->term();
    req.leader_id = request->leader_id();
    req.prev_log_index = request->prev_log_index();
    req.prev_log_term = request->prev_log_term();
    req.leader_commit = request->leader_commit();
    
    for (int i = 0; i < request->entries_size(); ++i) {
      const auto& proto_entry = request->entries(i);
      raft::LogEntry entry;
      entry.term = proto_entry.term();
      entry.index = proto_entry.index();
      entry.data = proto_entry.data();
      req.entries.push_back(entry);
    }

    raft::AppendEntriesResponse resp = node_->HandleAppendEntries(req);
    
    response->set_term(resp.term);
    response->set_success(resp.success);
    response->set_match_index(resp.match_index);
    
    return grpc::Status::OK;
  }

  grpc::Status InstallSnapshot(grpc::ServerContext* context,
                               grpc::ServerReader<grpc_pb::GrpcInstallSnapshotRequest>* reader,
                               grpc_pb::GrpcInstallSnapshotResponse* response) override {
    (void)context;
    
    if (!node_) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Node not initialized");
    }

    raft::InstallSnapshotRequest req;
    grpc_pb::GrpcInstallSnapshotRequest proto_req;
    
    while (reader->Read(&proto_req)) {
      req.term = proto_req.term();
      req.leader_id = proto_req.leader_id();
      req.offset = proto_req.offset();
      req.done = proto_req.done();
      
      if (proto_req.has_metadata()) {
        req.snapshot.last_included_term = proto_req.metadata().last_included_term();
        req.snapshot.last_included_index = proto_req.metadata().last_included_index();
      }
      
      req.snapshot.data += proto_req.data();
    }

    raft::InstallSnapshotResponse resp = node_->HandleInstallSnapshot(req);
    
    response->set_term(resp.term);
    response->set_success(resp.success);
    
    return grpc::Status::OK;
  }

 private:
  raft::EmbeddedRaftNode* node_;
};

// =============================================================================
// Main Server
// =============================================================================

class MetaServer {
 public:
  explicit MetaServer(const ServerConfig& config) : config_(config) {}
  
  Status Initialize() {
    std::cout << "╔════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║     CedarGraph Metadata Server (metad)                 ║" << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════╝" << std::endl;
    std::cout << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Node ID:    " << config_.node_id << std::endl;
    std::cout << "  Bind:       " << config_.bind_address << std::endl;
    std::cout << "  Data Dir:   " << config_.data_dir << std::endl;
    std::cout << "  Peers:      " << config_.peers.size() << std::endl;
    for (const auto& [id, addr] : config_.peers) {
      std::cout << "    - Node " << id << " @ " << addr << std::endl;
    }
    std::cout << std::endl;
    
    // Create transport
    raft::GrpcRaftTransport::Options transport_options;
    transport_options.rpc_timeout_ms = config_.rpc_timeout_ms;
    transport_ = std::make_unique<raft::GrpcRaftTransport>(
        transport_options, config_.peers);
    
    // Create meta service (this is the Raft state machine)
    meta_service_ = std::make_unique<RaftMetaService>();
    
    // Create state machine adapter
    state_machine_adapter_ = std::make_unique<raft::StateMachineAdapter>(
        meta_service_.get());
    
    // Create storage
    storage_ = std::make_unique<raft::FileRaftStorage>(config_.data_dir);
    auto init_status = storage_->Initialize();
    if (!init_status.ok()) {
      return init_status;
    }
    std::cout << "[1/5] Storage initialized" << std::endl;
    
    // Create Raft node
    raft::EmbeddedRaftNode::Options raft_options;
    raft_options.node_id = config_.node_id;
    raft_options.peers = config_.peers;
    raft_options.data_dir = config_.data_dir;
    raft_options.election_timeout_min = config_.election_timeout_min_ms;
    raft_options.election_timeout_max = config_.election_timeout_max_ms;
    raft_options.heartbeat_interval = config_.heartbeat_interval_ms;
    
    raft_node_ = std::make_unique<raft::EmbeddedRaftNode>(
        raft_options,
        transport_.get(),
        state_machine_adapter_.get(),
        storage_.get());
    std::cout << "[2/5] Raft node created" << std::endl;
    
    // Initialize meta service with Raft node
    meta_service_->Initialize(raft_node_.get());
    std::cout << "[3/5] Meta service initialized" << std::endl;
    
    // Create gRPC service
    raft_service_ = std::make_unique<RaftGrpcServiceImpl>(raft_node_.get());
    std::cout << "[4/5] gRPC service created" << std::endl;
    
    return Status::OK();
  }
  
  Status Start() {
    // Start gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(config_.bind_address, grpc::InsecureServerCredentials());
    builder.RegisterService(raft_service_.get());
    
    g_grpc_server = builder.BuildAndStart();
    if (!g_grpc_server) {
      return Status::IOError("Failed to start gRPC server");
    }
    std::cout << "[5/5] gRPC server listening on " << config_.bind_address << std::endl;
    std::cout << std::endl;
    
    // Start Raft node
    auto start_status = raft_node_->Start();
    if (!start_status.ok()) {
      g_grpc_server->Shutdown();
      return start_status;
    }
    std::cout << "✓ Raft node started successfully" << std::endl;
    std::cout << "✓ Waiting for leader election..." << std::endl;
    std::cout << std::endl;
    
    return Status::OK();
  }
  
  void Run() {
    // Wait for election and monitor status
    int ticks = 0;
    raft::NodeState last_state = raft::NodeState::kFollower;
    raft::Term last_term = 0;
    
    while (g_running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      ticks++;
      
      auto status = raft_node_->GetStatus();
      
      // Print status on change or every 10 seconds
      if (status.state != last_state || status.current_term != last_term || ticks % 10 == 0) {
        std::string leader_str = status.leader_id ? 
            std::to_string(*status.leader_id) : "none";
        
        std::cout << "[Status] state=" << raft::NodeStateToString(status.state)
                  << " term=" << status.current_term
                  << " commit=" << status.commit_index
                  << " leader=" << leader_str;
        
        // Print cluster info if leader
        if (status.state == raft::NodeState::kLeader) {
          auto nodes = meta_service_->GetAllNodes();
          auto spaces = meta_service_->GetStore().ListSpaces();
          std::cout << " nodes=" << nodes.size() 
                    << " spaces=" << spaces.size();
        }
        
        std::cout << std::endl;
        
        last_state = status.state;
        last_term = status.current_term;
      }
    }
  }
  
  void Shutdown() {
    std::cout << std::endl << "Shutting down..." << std::endl;
    
    if (raft_node_) {
      raft_node_->Shutdown();
    }
    
    if (g_grpc_server) {
      g_grpc_server->Shutdown();
    }
  }
  
  // API for testing
  RaftMetaService* GetMetaService() { return meta_service_.get(); }
  bool IsLeader() const { return meta_service_->IsLeader(); }

 private:
  ServerConfig config_;
  
  std::unique_ptr<raft::GrpcRaftTransport> transport_;
  std::unique_ptr<RaftMetaService> meta_service_;
  std::unique_ptr<raft::StateMachineAdapter> state_machine_adapter_;
  std::unique_ptr<raft::FileRaftStorage> storage_;
  std::unique_ptr<raft::EmbeddedRaftNode> raft_node_;
  std::unique_ptr<RaftGrpcServiceImpl> raft_service_;
};

}  // namespace dtx
}  // namespace cedar

// =============================================================================
// Main Entry Point
// =============================================================================

int main(int argc, char* argv[]) {
  using namespace cedar::dtx;
  
  // Parse command line
  std::string config_path;
  
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--config" || arg == "-c") {
      if (i + 1 < argc) {
        config_path = argv[++i];
      }
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "CedarGraph Metadata Server (metad)\n"
                << "Usage: " << argv[0] << " --config <config_file>\n"
                << "\n"
                << "Options:\n"
                << "  -c, --config <path>  Configuration file path\n"
                << "  -h, --help           Show this help\n";
      return 0;
    }
  }
  
  if (config_path.empty()) {
    std::cerr << "Error: No config file specified. Use --config <path>" << std::endl;
    return 1;
  }
  
  // Setup signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
  
  // Parse config
  ServerConfig config = ParseConfigFile(config_path);
  
  // Create and run server
  MetaServer server(config);
  
  auto init_status = server.Initialize();
  if (!init_status.ok()) {
    std::cerr << "Failed to initialize server: " << init_status.ToString() << std::endl;
    return 1;
  }
  
  auto start_status = server.Start();
  if (!start_status.ok()) {
    std::cerr << "Failed to start server: " << start_status.ToString() << std::endl;
    return 1;
  }
  
  server.Run();
  server.Shutdown();
  
  std::cout << "Server shutdown complete." << std::endl;
  return 0;
}
