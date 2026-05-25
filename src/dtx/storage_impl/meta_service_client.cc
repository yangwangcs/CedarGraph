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
#include <sys/statvfs.h>
#include <unistd.h>

#ifdef __linux__
#include <fstream>
#include <string>
#endif

#ifdef __APPLE__
#include <sys/types.h>
#include <sys/sysctl.h>
#endif

namespace cedar {
namespace dtx {

namespace {
// =============================================================================
// System Metrics Helpers
// =============================================================================

uint64_t GetTotalMemoryBytes() {
#ifdef __linux__
  std::ifstream meminfo("/proc/meminfo");
  std::string line;
  while (std::getline(meminfo, line)) {
    if (line.find("MemTotal:") == 0) {
      size_t pos = line.find_first_of("0123456789");
      if (pos != std::string::npos) {
        return std::stoull(line.substr(pos)) * 1024;  // kB → bytes
      }
    }
  }
  return 0;
#elif defined(__APPLE__)
  int64_t memsize = 0;
  size_t len = sizeof(memsize);
  if (sysctlbyname("hw.memsize", &memsize, &len, nullptr, 0) == 0) {
    return static_cast<uint64_t>(memsize);
  }
  return 0;
#else
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  if (pages > 0 && page_size > 0) {
    return static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);
  }
  return 0;
#endif
}

uint64_t GetTotalDiskBytes(const std::string& path) {
  struct statvfs buf;
  if (statvfs(path.c_str(), &buf) == 0) {
    return static_cast<uint64_t>(buf.f_blocks) * static_cast<uint64_t>(buf.f_frsize);
  }
  return 0;
}

double GetDiskUsagePercent(const std::string& path) {
  struct statvfs buf;
  if (statvfs(path.c_str(), &buf) == 0) {
    uint64_t total = static_cast<uint64_t>(buf.f_blocks) * buf.f_frsize;
    uint64_t free = static_cast<uint64_t>(buf.f_bfree) * buf.f_frsize;
    if (total > 0) {
      return 100.0 * static_cast<double>(total - free) / static_cast<double>(total);
    }
  }
  return 0.0;
}

}  // namespace

// =============================================================================
// MetaServiceClient Implementation
// =============================================================================

MetaServiceNodeClient::MetaServiceNodeClient()
    : connected_(false),
      shutdown_(false) {}

MetaServiceNodeClient::~MetaServiceNodeClient() {
  Shutdown();
}

Status MetaServiceNodeClient::Initialize(const ClientConfig& config) {
  if (connected_.load()) {
    return Status::InvalidArgument("MetaServiceNodeClient already initialized");
  }

  config_ = config;

  if (config_.metad_addresses.empty()) {
    return Status::InvalidArgument("No MetaD addresses provided");
  }

  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(config_.tls);
  if (!creds) creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();

  // Try each address until one connects
  for (size_t i = 0; i < config_.metad_addresses.size(); ++i) {
    channel_ = grpc::CreateChannel(config_.metad_addresses[i], creds);
    stub_ = cedar::meta::MetaService::NewStub(channel_);

    auto deadline = std::chrono::system_clock::now() + config_.registration_timeout;
    if (channel_->WaitForConnected(deadline)) {
      connected_ = true;
      current_metad_index_ = i;
      return Status::OK();
    }
  }

  return Status::IOError("Failed to connect to any MetaD node");
}

void MetaServiceNodeClient::Shutdown() {
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

Status MetaServiceNodeClient::RegisterNode() {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceNodeClient not connected");
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
  node_info->set_total_memory_bytes(GetTotalMemoryBytes());
  node_info->set_total_disk_bytes(GetTotalDiskBytes(config_.data_root));
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

Status MetaServiceNodeClient::SendHeartbeat(const std::vector<PartitionID>& partitions,
                                         double cpu_usage,
                                         double memory_usage) {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceNodeClient not connected");
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
  status->set_disk_usage_percent(GetDiskUsagePercent(config_.data_root));
  status->set_qps(0);  // QPS tracking requires query counter instrumentation
  status->set_latency_ms(0);  // Latency tracking requires histogram instrumentation
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

StatusOr<cedar::meta::PartitionAssignment> MetaServiceNodeClient::GetPartitionAssignment(
    const std::string& space_name, PartitionID partition_id) {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceNodeClient not connected");
  }
  
  cedar::meta::GetPartitionAssignmentRequest request;
  cedar::meta::GetPartitionAssignmentResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

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

StatusOr<cedar::meta::PartitionAssignment> MetaServiceNodeClient::GetPartitionAssignment(
    PartitionID partition_id) {
  return GetPartitionAssignment("default", partition_id);
}

Status MetaServiceNodeClient::UpdatePartitionAssignment(
    const cedar::meta::PartitionAssignment& assignment) {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceNodeClient not connected");
  }

  // TODO(#dtx-001): Implement gRPC call to MetaD for UpdatePartitionAssignment.
  // Returning NotSupported so callers do not assume the update succeeded.
  std::cerr << "[MetaServiceNodeClient] UpdatePartitionAssignment stub: "
            << "partition=" << assignment.partition_id()
            << " leader_node=" << assignment.leader_node()
            << " space_name=" << assignment.space_name() << std::endl;
  return Status::NotSupported("UpdatePartitionAssignment RPC not yet implemented");
}

StatusOr<cedar::meta::SpacePartitionMap> MetaServiceNodeClient::GetSpacePartitionMap(
    const std::string& space_name) {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceNodeClient not connected");
  }
  
  cedar::meta::GetSpacePartitionMapRequest request;
  cedar::meta::GetSpacePartitionMapResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

  request.set_space_name(space_name);

  grpc::Status status = stub_->GetSpacePartitionMap(&context, request, &response);
  
  if (!status.ok()) {
    return Status::IOError("GetSpacePartitionMap failed: " + status.error_message());
  }
  
  if (!response.success()) {
    return Status::IOError("GetSpacePartitionMap rejected: " + response.error_msg());
  }
  
  return response.partition_map();
}

StatusOr<std::vector<cedar::meta::NodeInfo>> MetaServiceNodeClient::GetAliveNodes() {
  if (!connected_.load()) {
    return Status::IOError("MetaServiceNodeClient not connected");
  }
  
  cedar::meta::GetAliveNodesRequest request;
  cedar::meta::GetAliveNodesResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

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

void MetaServiceNodeClient::StartHeartbeatLoop(
    std::function<std::vector<PartitionID>()> partition_provider) {
  if (heartbeat_thread_.joinable()) {
    return;  // Already running
  }
  
  shutdown_ = false;
  heartbeat_thread_ = std::thread([this, provider = std::move(partition_provider)]() {
    HeartbeatLoop(provider);
  });
}

void MetaServiceNodeClient::StopHeartbeatLoop() {
  shutdown_ = true;
  
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
}

void MetaServiceNodeClient::HeartbeatLoop(
    std::function<std::vector<PartitionID>()> partition_provider) {
  int consecutive_failures = 0;
  
  while (!shutdown_.load()) {
    try {
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

        // Try failover to another MetaD node if too many consecutive failures
        if (consecutive_failures >= 3) {
          std::cerr << "Too many heartbeat failures, trying next MetaD node..." << std::endl;
          auto failover = TryNextMetaAddress();
          if (failover.ok()) {
            consecutive_failures = 0;
            std::cerr << "Failover to MetaD node succeeded" << std::endl;
          } else {
            std::cerr << "Failover failed: " << failover.ToString() << std::endl;
            // As last resort, try re-registering with current node
            auto reg_status = RegisterNode();
            if (reg_status.ok()) {
              consecutive_failures = 0;
            }
          }
        }
      } else {
        if (consecutive_failures > 0) {
          std::cerr << "Heartbeat recovered" << std::endl;
        }
        consecutive_failures = 0;
      }
    } catch (const std::exception& e) {
      std::cerr << "Heartbeat loop exception: " << e.what() << std::endl;
      consecutive_failures++;
    }
  }
}

bool MetaServiceNodeClient::IsConnected() const {
  return connected_.load() && !shutdown_.load();
}

Status MetaServiceNodeClient::TryNextMetaAddress() {
  if (config_.metad_addresses.size() <= 1) {
    return Status::IOError("No fallback MetaD addresses available");
  }
  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(config_.tls);
  if (!creds) creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();

  size_t start = (current_metad_index_ + 1) % config_.metad_addresses.size();
  for (size_t i = 0; i < config_.metad_addresses.size(); ++i) {
    size_t idx = (start + i) % config_.metad_addresses.size();
    auto channel = grpc::CreateChannel(config_.metad_addresses[idx], creds);
    auto stub = cedar::meta::MetaService::NewStub(channel);

    cedar::meta::GetAliveNodesRequest req;
    cedar::meta::GetAliveNodesResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    auto status = stub->GetAliveNodes(&ctx, req, &resp);
    if (status.ok() && resp.success()) {
      channel_ = std::move(channel);
      stub_ = std::move(stub);
      current_metad_index_ = idx;
      return Status::OK();
    }
  }
  return Status::IOError("All MetaD nodes unreachable");
}

}  // namespace dtx
}  // namespace cedar
