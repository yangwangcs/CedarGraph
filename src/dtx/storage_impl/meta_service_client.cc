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

#include <chrono>
#include <iostream>

namespace cedar {
namespace dtx {

// =============================================================================
// MetaServiceClient Implementation
// =============================================================================

MetaServiceClient::MetaServiceClient()
    : connected_(false),
      shutdown_(false) {}

MetaServiceClient::~MetaServiceClient() {
  Shutdown();
}

Status MetaServiceClient::Initialize(const ClientConfig& config) {
  if (connected_.load()) {
    return Status::InvalidArgument("MetaServiceClient already initialized");
  }
  
  config_ = config;
  
  // Create gRPC channel to MetaD
  channel_ = grpc::CreateChannel(config_.metad_address, 
                                  grpc::InsecureChannelCredentials());
  stub_ = cedar::meta::MetaService::NewStub(channel_);
  
  // Wait for channel to be ready
  auto deadline = std::chrono::system_clock::now() + config_.registration_timeout;
  if (!channel_->WaitForConnected(deadline)) {
    return Status::IOError("Failed to connect to MetaD: " + config_.metad_address);
  }
  
  connected_ = true;
  return Status::OK();
}

void MetaServiceClient::Shutdown() {
  if (shutdown_.exchange(true)) {
    return;
  }
  
  connected_ = false;
  
  // Stop heartbeat loop if running
  StopHeartbeatLoop();
  
  // Clean up gRPC resources
  stub_.reset();
  channel_.reset();
}

Status MetaServiceClient::RegisterNode() {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceClient not connected");
  }
  
  cedar::meta::RegisterNodeRequest request;
  cedar::meta::RegisterNodeResponse response;
  grpc::ClientContext context;
  
  // Set deadline
  context.set_deadline(std::chrono::system_clock::now() + config_.registration_timeout);
  
  // Populate node info
  auto* node_info = request.mutable_node_info();
  node_info->set_node_id(config_.node_id);
  node_info->set_address(config_.listen_address);
  node_info->set_data_path(config_.data_root);
  node_info->set_num_cpu_cores(std::thread::hardware_concurrency());
  node_info->set_total_memory_bytes(0);  // TODO: Get actual memory
  node_info->set_total_disk_bytes(0);    // TODO: Get actual disk
  node_info->set_state("ONLINE");
  
  grpc::Status status = stub_->RegisterNode(&context, request, &response);
  
  if (!status.ok()) {
    return Status::IOError("MetaD registration failed: " + status.error_message());
  }
  
  if (!response.success()) {
    return Status::IOError("MetaD registration rejected: " + response.error_msg());
  }
  
  return Status::OK();
}

Status MetaServiceClient::SendHeartbeat(const std::vector<PartitionID>& partitions,
                                         double cpu_usage,
                                         double memory_usage) {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceClient not connected");
  }
  
  cedar::meta::HeartbeatRequest request;
  cedar::meta::HeartbeatResponse response;
  grpc::ClientContext context;
  
  // Set deadline
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::seconds(5));
  
  // Populate node status
  auto* status = request.mutable_status();
  status->set_node_id(config_.node_id);
  status->set_cpu_usage_percent(cpu_usage);
  status->set_memory_usage_percent(memory_usage);
  status->set_disk_usage_percent(0.0);  // TODO: Get actual disk usage
  status->set_qps(0);  // TODO: Track QPS
  status->set_latency_ms(0);  // TODO: Track latency
  status->set_timestamp_unix(
      std::chrono::system_clock::now().time_since_epoch().count());
  
  // Add partition info
  for (PartitionID pid : partitions) {
    status->add_leader_partitions(pid);
  }
  
  grpc::Status grpc_status = stub_->Heartbeat(&context, request, &response);
  
  if (!grpc_status.ok()) {
    return Status::IOError("Heartbeat failed: " + grpc_status.error_message());
  }
  
  if (!response.success()) {
    return Status::IOError("Heartbeat rejected: " + response.error_msg());
  }
  
  return Status::OK();
}

StatusOr<cedar::meta::PartitionAssignment> MetaServiceClient::GetPartitionAssignment(
    const std::string& space_name, PartitionID partition_id) {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceClient not connected");
  }
  
  cedar::meta::GetPartitionAssignmentRequest request;
  cedar::meta::GetPartitionAssignmentResponse response;
  grpc::ClientContext context;
  
  request.set_space_name(space_name);
  request.set_partition_id(partition_id);
  
  grpc::Status status = stub_->GetPartitionAssignment(&context, request, &response);
  
  if (!status.ok()) {
    return Status::IOError("GetPartitionAssignment failed: " + status.error_message());
  }
  
  if (!response.success()) {
    return Status::IOError("GetPartitionAssignment rejected: " + response.error_msg());
  }
  
  return response.assignment();
}

StatusOr<std::vector<cedar::meta::NodeInfo>> MetaServiceClient::GetAliveNodes() {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceClient not connected");
  }
  
  cedar::meta::GetAliveNodesRequest request;
  cedar::meta::GetAliveNodesResponse response;
  grpc::ClientContext context;
  
  grpc::Status status = stub_->GetAliveNodes(&context, request, &response);
  
  if (!status.ok()) {
    return Status::IOError("GetAliveNodes failed: " + status.error_message());
  }
  
  if (!response.success()) {
    return Status::IOError("GetAliveNodes rejected: " + response.error_msg());
  }
  
  std::vector<cedar::meta::NodeInfo> nodes;
  for (const auto& node : response.nodes()) {
    nodes.push_back(node);
  }
  
  return nodes;
}

void MetaServiceClient::StartHeartbeatLoop(
    std::function<std::vector<PartitionID>()> partition_provider) {
  if (heartbeat_thread_.joinable()) {
    return;  // Already running
  }
  
  shutdown_ = false;
  heartbeat_thread_ = std::thread([this, provider = std::move(partition_provider)]() {
    HeartbeatLoop(provider);
  });
}

void MetaServiceClient::StopHeartbeatLoop() {
  shutdown_ = true;
  
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
}

void MetaServiceClient::HeartbeatLoop(
    std::function<std::vector<PartitionID>()> partition_provider) {
  int consecutive_failures = 0;
  
  while (!shutdown_.load()) {
    // Sleep for heartbeat interval
    std::this_thread::sleep_for(config_.heartbeat_interval);
    
    if (shutdown_.load()) {
      break;
    }
    
    // Get current partitions
    auto partitions = partition_provider();
    
    // Send heartbeat
    auto status = SendHeartbeat(partitions);
    
    if (!status.ok()) {
      consecutive_failures++;
      std::cerr << "Heartbeat failed (" << consecutive_failures << "): " 
                << status.ToString() << std::endl;
      
      // Re-register if too many consecutive failures
      if (consecutive_failures >= 3) {
        std::cerr << "Too many heartbeat failures, re-registering..." << std::endl;
        auto reg_status = RegisterNode();
        if (reg_status.ok()) {
          consecutive_failures = 0;
        }
      }
    } else {
      if (consecutive_failures > 0) {
        std::cerr << "Heartbeat recovered" << std::endl;
      }
      consecutive_failures = 0;
    }
  }
}

bool MetaServiceClient::IsConnected() const {
  return connected_.load() && !shutdown_.load();
}

}  // namespace dtx
}  // namespace cedar
