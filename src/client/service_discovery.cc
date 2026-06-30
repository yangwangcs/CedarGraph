// Copyright 2025 The Cedar Authors
//
// Service Discovery implementation (Simplified)

#include "cedar/client/service_discovery.h"

#include <chrono>
#include <iostream>
#include <thread>

#include "cedar/dtx/raft/grpc_tls.h"

namespace cedar {
namespace client {
namespace {

int64_t CurrentTimeMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::shared_ptr<grpc::ChannelCredentials> CreateMetaChannelCredentials(
    const ServiceDiscoveryConfig& config) {
  if (!config.enable_tls && !cedar::dtx::raft::TlsCredentialFactory::EnvTlsEnabled()) {
    return grpc::InsecureChannelCredentials();
  }

  cedar::dtx::raft::TlsConfig tls;
  tls.enabled = true;
  tls.ca_cert_file = config.ca_cert_path;
  tls.mtls_enabled = config.mtls_enabled;
  tls.client_cert_file = config.client_cert_path;
  tls.client_key_file = config.client_key_path;

  auto creds = config.enable_tls
      ? cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(tls)
      : cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
  if (!creds.ok() && cedar::dtx::raft::TlsCredentialFactory::EnvTlsEnabled()) {
    creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
  }
  if (!creds.ok()) {
    std::cerr << "Failed to create MetaD TLS credentials for service discovery: "
              << creds.status().ToString() << std::endl;
    return nullptr;
  }
  return creds.ValueOrDie();
}

}  // namespace

// ============================================================================
// ServiceDiscovery
// ============================================================================

ServiceDiscovery::ServiceDiscovery(const ServiceDiscoveryConfig& config)
    : config_(config) {}

ServiceDiscovery::~ServiceDiscovery() {
  Stop();
}

bool ServiceDiscovery::Initialize() {
  if (config_.metad_host.empty() || config_.metad_port <= 0) {
    std::cerr << "Invalid MetaD address for service discovery" << std::endl;
    return false;
  }
  if (config_.refresh_interval_ms <= 0 || config_.heartbeat_timeout_ms <= 0) {
    std::cerr << "Service discovery intervals must be positive" << std::endl;
    return false;
  }
  if (running_.load()) {
    return false;
  }

  // Create connection to MetaD
  auto credentials = CreateMetaChannelCredentials(config_);
  if (!credentials) {
    return false;
  }
  metad_channel_ = grpc::CreateChannel(
      config_.metad_host + ":" + std::to_string(config_.metad_port),
      credentials);
  
  metad_stub_ = cedar::meta::MetaService::NewStub(metad_channel_);
  
  // Initial refresh
  if (!RefreshNodes()) {
    std::cerr << "Failed to refresh nodes from MetaD" << std::endl;
    return false;
  }
  
  // Start background refresh thread
  running_ = true;
  refresh_thread_ = std::thread([this]() {
    while (running_) {
      std::unique_lock<std::mutex> lock(refresh_cv_mutex_);
      refresh_cv_.wait_for(lock,
                           std::chrono::milliseconds(config_.refresh_interval_ms),
                           [this]() { return !running_.load(); });
      if (!running_) {
        break;
      }
      RefreshNodes();
    }
  });
  
  return true;
}

void ServiceDiscovery::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  refresh_cv_.notify_all();
  if (refresh_thread_.joinable()) {
    refresh_thread_.join();
  }
}

std::vector<ServiceNode> ServiceDiscovery::GetGraphDNodes() {
  std::lock_guard<std::mutex> lock(mutex_);
  return graphd_nodes_;
}

std::vector<ServiceNode> ServiceDiscovery::GetStorageDNodes() {
  std::lock_guard<std::mutex> lock(mutex_);
  return storaged_nodes_;
}

ServiceNode ServiceDiscovery::GetPartitionLeader(const std::string& space_name, int partition_id) {
  // TODO: Implement using MetaD proto stubs
  ServiceNode node;
  node.host = config_.metad_host;
  node.port = 9779;
  node.is_leader = true;
  return node;
}

std::vector<ServiceNode> ServiceDiscovery::GetPartitionNodes(const std::string& space_name, int partition_id) {
  std::vector<ServiceNode> nodes;
  
  // TODO: Implement using MetaD proto stubs
  ServiceNode node;
  node.host = config_.metad_host;
  node.port = 9779;
  node.is_leader = true;
  nodes.push_back(node);
  
  return nodes;
}

bool ServiceDiscovery::RefreshNodes() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // TODO: Implement using MetaD proto stubs
  // For now, use default nodes
  graphd_nodes_.clear();
  ServiceNode graphd_node;
  graphd_node.host = config_.metad_host;
  graphd_node.port = 9669;
  graphd_node.node_id = "graphd-1";
  graphd_node.is_leader = true;
  graphd_node.last_heartbeat = CurrentTimeMillis();
  graphd_nodes_.push_back(graphd_node);
  
  storaged_nodes_.clear();
  ServiceNode storaged_node;
  storaged_node.host = config_.metad_host;
  storaged_node.port = 9779;
  storaged_node.node_id = "storaged-1";
  storaged_node.is_leader = true;
  storaged_node.last_heartbeat = CurrentTimeMillis();
  storaged_nodes_.push_back(storaged_node);

  last_heartbeats_[graphd_node.node_id] = graphd_node.last_heartbeat;
  last_heartbeats_[storaged_node.node_id] = storaged_node.last_heartbeat;
  
  return true;
}

bool ServiceDiscovery::IsNodeHealthy(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = last_heartbeats_.find(node_id);
  if (it == last_heartbeats_.end()) {
    return false;
  }
  
  auto now = CurrentTimeMillis();
  return (now - it->second) < config_.heartbeat_timeout_ms;
}

}  // namespace client
}  // namespace cedar
