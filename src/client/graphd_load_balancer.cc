// Copyright 2025 The Cedar Authors. All rights reserved.
// GraphD Load Balancer Implementation

#include "cedar/client/graphd_load_balancer.h"
#include <iostream>
#include <random>
#include <algorithm>
#include <thread>

namespace cedar {
namespace client {

GraphDLoadBalancer::GraphDLoadBalancer(const Config& config) : config_(config) {}

bool GraphDLoadBalancer::Initialize() {
  // Create gRPC channel to MetaD
  auto channel = grpc::CreateChannel(config_.meta_address, grpc::InsecureChannelCredentials());
  stub_ = cedar::meta::MetaService::NewStub(channel);

  // Initial refresh
  if (!RefreshNodes()) {
    std::cerr << "[LoadBalancer] Failed to refresh nodes from MetaD" << std::endl;
    return false;
  }

  // Start refresh thread
  running_ = true;
  refresh_thread_ = std::make_unique<std::thread>([this]() {
    while (running_) {
      std::this_thread::sleep_for(std::chrono::seconds(config_.refresh_interval_seconds));
      if (running_) {
        RefreshNodes();
      }
    }
  });

  std::cout << "[LoadBalancer] Initialized with " << nodes_.size() << " GraphD nodes" << std::endl;
  return true;
}

GraphDNode GraphDLoadBalancer::SelectNode() {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  if (nodes_.empty()) {
    throw std::runtime_error("No GraphD nodes available");
  }

  // Filter out BUSY nodes
  std::vector<GraphDNode*> available;
  for (auto& node : nodes_) {
    if (node.state == "ONLINE") {
      available.push_back(&node);
    }
  }

  // If no ONLINE nodes, use all nodes
  if (available.empty()) {
    for (auto& node : nodes_) {
      available.push_back(&node);
    }
  }

  if (available.empty()) {
    throw std::runtime_error("No GraphD nodes available");
  }

  // Select based on strategy
  switch (config_.strategy) {
    case Strategy::ROUND_ROBIN:
      return SelectRoundRobin();
    case Strategy::LEAST_CONNECTIONS:
      return SelectLeastConnections();
    case Strategy::RANDOM:
      return SelectRandom();
    default:
      return SelectRoundRobin();
  }
}

std::vector<GraphDNode> GraphDLoadBalancer::GetAvailableNodes() {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  return nodes_;
}

bool GraphDLoadBalancer::RefreshNodes() {
  cedar::meta::GetGraphDNodesRequest request;
  request.set_state_filter("");  // Get all nodes

  cedar::meta::GetGraphDNodesResponse response;
  grpc::ClientContext context;

  grpc::Status status = stub_->GetGraphDNodes(&context, request, &response);
  if (!status.ok()) {
    std::cerr << "[LoadBalancer] GetGraphDNodes failed: " << status.error_message() << std::endl;
    return false;
  }

  if (!response.success()) {
    std::cerr << "[LoadBalancer] GetGraphDNodes error: " << response.error_msg() << std::endl;
    return false;
  }

  std::lock_guard<std::mutex> lock(nodes_mutex_);
  nodes_.clear();

  for (const auto& node_info : response.nodes()) {
    GraphDNode node;
    node.address = node_info.address();
    node.port = node_info.port();
    node.state = node_info.state();
    node.current_qps = node_info.current_qps();
    node.active_queries = node_info.active_queries();
    node.cpu_usage = node_info.cpu_usage();
    node.last_seen = std::chrono::steady_clock::now();
    nodes_.push_back(node);
  }

  return true;
}

size_t GraphDLoadBalancer::GetNodeCount() const {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  return nodes_.size();
}

GraphDNode GraphDLoadBalancer::SelectRoundRobin() {
  size_t index = round_robin_index_.fetch_add(1) % nodes_.size();
  return nodes_[index];
}

GraphDNode GraphDLoadBalancer::SelectLeastConnections() {
  auto it = std::min_element(nodes_.begin(), nodes_.end(),
    [](const GraphDNode& a, const GraphDNode& b) {
      return a.active_queries < b.active_queries;
    });
  return *it;
}

GraphDNode GraphDLoadBalancer::SelectRandom() {
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<size_t> dist(0, nodes_.size() - 1);
  return nodes_[dist(gen)];
}

GraphDNode GraphDLoadBalancer::SelectNodeWithFailover() {
  // Try to select a node, excluding failed nodes
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    try {
      GraphDNode node = SelectNode();
      
      // Check if this node is in the failed list
      bool is_failed = false;
      {
        std::lock_guard<std::mutex> lock(nodes_mutex_);
        for (const auto& failed : failed_nodes_) {
          if (failed.address == node.address && failed.port == node.port) {
            // Check if enough time has passed for retry
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - failed.failed_at).count();
            if (elapsed < kRetryDelaySeconds * failed.retry_count) {
              is_failed = true;
              break;
            }
          }
        }
      }
      
      if (!is_failed) {
        return node;
      }
    } catch (const std::exception& e) {
      // No nodes available, wait and retry
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  
  // If all retries failed, try to refresh nodes and select again
  RefreshNodes();
  return SelectNode();
}

void GraphDLoadBalancer::MarkNodeFailed(const std::string& address, int port) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  // Check if already in failed list
  for (auto& failed : failed_nodes_) {
    if (failed.address == address && failed.port == port) {
      failed.retry_count++;
      failed.failed_at = std::chrono::steady_clock::now();
      return;
    }
  }
  
  // Add to failed list
  FailedNode failed;
  failed.address = address;
  failed.port = port;
  failed.failed_at = std::chrono::steady_clock::now();
  failed.retry_count = 1;
  failed_nodes_.push_back(failed);
}

}  // namespace client
}  // namespace cedar
