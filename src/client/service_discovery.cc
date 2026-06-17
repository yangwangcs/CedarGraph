// Copyright 2025 The Cedar Authors
//
// Service Discovery implementation (Simplified)

#include "cedar/client/service_discovery.h"

#include <chrono>
#include <iostream>
#include <thread>

namespace cedar {
namespace client {

// ============================================================================
// ServiceDiscovery
// ============================================================================

ServiceDiscovery::ServiceDiscovery(const ServiceDiscoveryConfig& config)
    : config_(config) {}

ServiceDiscovery::~ServiceDiscovery() {
  running_ = false;
  if (refresh_thread_.joinable()) {
    refresh_thread_.join();
  }
}

bool ServiceDiscovery::Initialize() {
  // Create connection to MetaD
  metad_channel_ = grpc::CreateChannel(
      config_.metad_host + ":" + std::to_string(config_.metad_port),
      grpc::InsecureChannelCredentials());
  
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
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.refresh_interval_ms));
      RefreshNodes();
    }
  });
  
  return true;
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
  graphd_node.last_heartbeat = std::chrono::system_clock::now().time_since_epoch().count();
  graphd_nodes_.push_back(graphd_node);
  
  storaged_nodes_.clear();
  ServiceNode storaged_node;
  storaged_node.host = config_.metad_host;
  storaged_node.port = 9779;
  storaged_node.node_id = "storaged-1";
  storaged_node.is_leader = true;
  storaged_node.last_heartbeat = std::chrono::system_clock::now().time_since_epoch().count();
  storaged_nodes_.push_back(storaged_node);
  
  return true;
}

bool ServiceDiscovery::IsNodeHealthy(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = last_heartbeats_.find(node_id);
  if (it == last_heartbeats_.end()) {
    return false;
  }
  
  auto now = std::chrono::system_clock::now().time_since_epoch().count();
  return (now - it->second) < config_.heartbeat_timeout_ms * 1000;
}

}  // namespace client
}  // namespace cedar
