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

#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/storage/partition_raft_manager.h"
#include "cedar/dtx/failover_manager.h"
#include "cedar/dtx/monitoring.h"

#include <csignal>
#include <iostream>
#include <grpcpp/grpcpp.h>

namespace cedar {
namespace dtx {

// =============================================================================
// StorageServer Implementation
// =============================================================================

StorageServer::StorageServer()
    : node_id_(0),
      running_(false) {}

StorageServer::~StorageServer() {
  Shutdown();
}

Status StorageServer::Initialize(const StorageServerConfig& config) {
  if (running_.load()) {
    return Status::InvalidArgument("Server already running");
  }
  
  config_ = config;
  node_id_ = config.node_id;
  
  std::cout << "Initializing StorageServer (node_id=" << node_id_ 
            << ", address=" << config.listen_address << ")" << std::endl;
  
  // Initialize braft partition replication manager first
  // (needed by PartitionManager for failover handler registration)
  raft_manager_ = std::make_unique<PartitionRaftManager>();
  Status s = raft_manager_->Initialize(node_id_, config.data_root, config.listen_address);
  if (!s.ok()) {
    std::cerr << "Failed to initialize raft manager: " << s.ToString() << std::endl;
    return s;
  }
  
  // Initialize partition manager (registers failover handlers)
  StoragePartitionManager::PartitionConfig pm_config;
  pm_config.data_root = config.data_root;
  pm_config.max_partitions = config.max_partitions;
  
  partition_manager_.SetRaftManager(raft_manager_.get());
  partition_manager_.SetFailoverManager(GetGlobalFailoverManager());
  s = partition_manager_.Initialize(pm_config);
  if (!s.ok()) {
    std::cerr << "Failed to initialize partition manager: " << s.ToString() << std::endl;
    return s;
  }
  
  // Create the gRPC service implementation (with raft manager for replication)
  service_impl_ = std::make_unique<StorageServiceImpl>(&partition_manager_, raft_manager_.get());
  
  // Initialize MetaD client
  MetaServiceNodeClient::ClientConfig meta_config;
  meta_config.SetMetaAddress(config.metad_address);
  meta_config.node_id = node_id_;
  meta_config.listen_address = config.listen_address;
  meta_config.data_root = config.data_root;
  meta_config.max_partitions = config.max_partitions;
  meta_config.heartbeat_interval = config.heartbeat_interval;
  
  meta_client_ = std::make_unique<MetaServiceNodeClient>();
  s = meta_client_->Initialize(meta_config);
  if (!s.ok()) {
    std::cerr << "Warning: Failed to initialize MetaD client: " << s.ToString() << std::endl;
    // Continue anyway - we can still serve storage requests
  } else {
    // Register with MetaD
    s = RegisterToMetaD();
    if (!s.ok()) {
      std::cerr << "Warning: Failed to register with MetaD: " << s.ToString() << std::endl;
      // Continue anyway - we'll retry in heartbeat loop
    }
  }
  
  return Status::OK();
}

void StorageServer::Serve() {
  if (running_.exchange(true)) {
    return;
  }
  
  std::cout << "Starting StorageServer on " << config_.listen_address << std::endl;
  
  // Build and start gRPC server
  grpc::ServerBuilder builder;
  builder.AddListeningPort(config_.listen_address,
                           cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentialsFromEnv());
  builder.RegisterService(service_impl_.get());
  
  // Set server options
  builder.SetMaxMessageSize(64 * 1024 * 1024);  // 64MB max message size
  builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
  
  grpc_server_ = builder.BuildAndStart();
  if (!grpc_server_) {
    std::cerr << "Failed to start gRPC server" << std::endl;
    running_.store(false);
    return;
  }
  
  std::cout << "gRPC server started successfully on " << config_.listen_address << std::endl;
  
  // Start heartbeat loop if MetaD client is connected
  if (meta_client_ && meta_client_->IsConnected()) {
    meta_client_->StartHeartbeatLoop([this]() {
      return partition_manager_.GetAllPartitions();
    });
  }
  
  // Wait for shutdown signal
  std::unique_lock<std::mutex> lock(shutdown_mutex_);
  shutdown_cv_.wait(lock, [this]() { return !running_.load(); });
  
  std::cout << "StorageServer stopped" << std::endl;
}

Status StorageServer::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }
  
  std::cout << "Shutting down StorageServer..." << std::endl;
  
  // Signal shutdown
  shutdown_cv_.notify_all();
  
  // Stop heartbeat loop
  if (meta_client_) {
    meta_client_->StopHeartbeatLoop();
  }
  
  // Shutdown gRPC server
  if (grpc_server_) {
    grpc_server_->Shutdown();
    grpc_server_->Wait();
    std::cout << "gRPC server stopped" << std::endl;
  }
  
  // Shutdown MetaD client
  if (meta_client_) {
    meta_client_->Shutdown();
  }
  
  // Shutdown partition manager
  partition_manager_.Shutdown();
  
  // Shutdown raft manager
  if (raft_manager_) {
    raft_manager_->Shutdown();
  }
  
  std::cout << "StorageServer shutdown complete" << std::endl;
  return Status::OK();
}

Status StorageServer::RegisterToMetaD() {
  if (!meta_client_) {
    return Status::IOError("MetaD client not initialized");
  }
  
  std::cout << "Registering with MetaD at " << config_.metad_address << std::endl;
  
  auto status = meta_client_->RegisterNode();
  if (!status.ok()) {
    return status;
  }
  
  std::cout << "Registered with MetaD successfully" << std::endl;
  
  // Fetch partition assignment and initialize raft groups
  auto nodes_result = meta_client_->GetAliveNodes();
  if (!nodes_result.ok()) {
    std::cerr << "Warning: Failed to get alive nodes: " << nodes_result.status().ToString() << std::endl;
    return Status::OK();  // Continue anyway
  }
  
  std::unordered_map<uint32_t, std::string> node_address_map;
  for (const auto& node : nodes_result.value()) {
    node_address_map[node.node_id()] = node.address();
  }
  
  auto map_result = meta_client_->GetSpacePartitionMap("default");
  if (!map_result.ok()) {
    std::cerr << "Warning: Failed to get partition map: " << map_result.status().ToString() << std::endl;
    return Status::OK();  // Continue anyway, partitions can be added later
  }
  
  const auto& partition_map = map_result.value();
  for (const auto& entry : partition_map.assignments()) {
    PartitionID pid = entry.first;
    const auto& assignment = entry.second;
    
    // Check if this node is part of the partition's raft group
    bool is_member = (assignment.leader_node() == node_id_);
    if (!is_member) {
      for (uint32_t follower : assignment.follower_nodes()) {
        if (follower == node_id_) {
          is_member = true;
          break;
        }
      }
    }
    
    if (!is_member) continue;
    
    // Add partition to manager
    auto s = partition_manager_.AddPartition(pid);
    if (!s.ok() && !s.IsInvalidArgument()) {
      std::cerr << "Warning: Failed to add partition " << pid << ": " << s.ToString() << std::endl;
      continue;
    }
    
    // Build peer list
    std::vector<std::string> peers;
    auto it = node_address_map.find(assignment.leader_node());
    if (it != node_address_map.end()) {
      peers.push_back(it->second);
    }
    for (uint32_t follower : assignment.follower_nodes()) {
      auto fit = node_address_map.find(follower);
      if (fit != node_address_map.end()) {
        peers.push_back(fit->second);
      }
    }
    
    if (peers.empty()) {
      std::cerr << "Warning: No peers found for partition " << pid << std::endl;
      continue;
    }
    
    auto* storage = partition_manager_.GetPartition(pid);
    if (!storage) {
      std::cerr << "Warning: Partition " << pid << " not found after add" << std::endl;
      continue;
    }
    
    // Build peer_node_ids for leader address resolution
    std::unordered_map<std::string, NodeID> peer_node_ids;
    for (const auto& [nid, addr] : node_address_map) {
      peer_node_ids[addr] = nid;
    }
    
    s = raft_manager_->CreateRaftGroup(pid, peers, storage, 1000, peer_node_ids);
    if (!s.ok()) {
      std::cerr << "Warning: Failed to create raft group for partition " << pid << ": " << s.ToString() << std::endl;
      continue;
    }
    
    std::cout << "Created raft group for partition " << pid << " with " << peers.size() << " peers" << std::endl;
  }
  
  return Status::OK();
}

void StorageServer::HeartbeatLoop() {
  // This is now handled by MetaServiceNodeClient
  // Keeping this method for backward compatibility
}

}  // namespace dtx
}  // namespace cedar
