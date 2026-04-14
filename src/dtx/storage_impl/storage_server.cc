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
  
  // Initialize partition manager
  StoragePartitionManager::PartitionConfig pm_config;
  pm_config.data_root = config.data_root;
  pm_config.max_partitions = config.max_partitions;
  
  Status s = partition_manager_.Initialize(pm_config);
  if (!s.ok()) {
    std::cerr << "Failed to initialize partition manager: " << s.ToString() << std::endl;
    return s;
  }
  
  // Create the gRPC service implementation
  service_impl_ = std::make_unique<StorageServiceImpl>(&partition_manager_);
  
  // Initialize MetaD client
  MetaServiceClient::ClientConfig meta_config;
  meta_config.metad_address = config.metad_address;
  meta_config.node_id = node_id_;
  meta_config.listen_address = config.listen_address;
  meta_config.data_root = config.data_root;
  meta_config.max_partitions = config.max_partitions;
  meta_config.heartbeat_interval = config.heartbeat_interval;
  
  meta_client_ = std::make_unique<MetaServiceClient>();
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
  builder.AddListeningPort(config_.listen_address, grpc::InsecureServerCredentials());
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
  return Status::OK();
}

void StorageServer::HeartbeatLoop() {
  // This is now handled by MetaServiceClient
  // Keeping this method for backward compatibility
}

}  // namespace dtx
}  // namespace cedar
