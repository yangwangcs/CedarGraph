// Copyright 2025 The Cedar Authors. All rights reserved.
// GraphD Load Balancer Implementation

#include "cedar/client/graphd_load_balancer.h"
#include <iostream>
#include <random>
#include <algorithm>
#include <thread>

#include "cedar/dtx/raft/grpc_tls.h"

namespace cedar {
namespace client {
namespace {

std::shared_ptr<grpc::ChannelCredentials> CreateMetaChannelCredentials(
    const GraphDLoadBalancer::Config& config) {
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
    std::cerr << "[LoadBalancer] Failed to create MetaD TLS credentials: "
              << creds.status().ToString() << std::endl;
    return nullptr;
  }
  return creds.ValueOrDie();
}

}  // namespace

GraphDLoadBalancer::GraphDLoadBalancer(const Config& config) : config_(config) {}

GraphDLoadBalancer::~GraphDLoadBalancer() {
  Stop();
}

bool GraphDLoadBalancer::Initialize() {
  if (config_.refresh_interval_seconds <= 0) {
    std::cerr << "[LoadBalancer] refresh_interval_seconds must be positive" << std::endl;
    return false;
  }
  if (running_.load()) {
    return false;
  }

  // Create gRPC channel to MetaD
  auto credentials = CreateMetaChannelCredentials(config_);
  if (!credentials) {
    return false;
  }
  auto channel = grpc::CreateChannel(config_.meta_address, credentials);
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
      std::unique_lock<std::mutex> lock(refresh_cv_mutex_);
      refresh_cv_.wait_for(lock,
                           std::chrono::seconds(config_.refresh_interval_seconds),
                           [this]() { return !running_.load(); });
      if (!running_) {
        break;
      }
      RefreshNodes();
    }
  });

  std::cout << "[LoadBalancer] Initialized with " << nodes_.size() << " GraphD nodes" << std::endl;
  return true;
}

void GraphDLoadBalancer::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  refresh_cv_.notify_all();
  if (refresh_thread_ && refresh_thread_->joinable()) {
    refresh_thread_->join();
  }
}

GraphDNode GraphDLoadBalancer::SelectNode() {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  if (nodes_.empty()) {
    throw std::runtime_error("No GraphD nodes available");
  }

  // Only route to ONLINE nodes. Routing to OFFLINE/BUSY nodes can amplify
  // failures during failover and maintenance windows.
  std::vector<size_t> available;
  for (size_t i = 0; i < nodes_.size(); ++i) {
    if (nodes_[i].state == "ONLINE") {
      available.push_back(i);
    }
  }

  if (available.empty()) {
    throw std::runtime_error("No ONLINE GraphD nodes available");
  }

  // Select based on strategy
  switch (config_.strategy) {
    case Strategy::ROUND_ROBIN:
      return SelectRoundRobin(available);
    case Strategy::LEAST_CONNECTIONS:
      return SelectLeastConnections(available);
    case Strategy::RANDOM:
      return SelectRandom(available);
    default:
      return SelectRoundRobin(available);
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

GraphDNode GraphDLoadBalancer::SelectRoundRobin(
    const std::vector<size_t>& candidate_indexes) {
  size_t index = round_robin_index_.fetch_add(1) % candidate_indexes.size();
  return nodes_[candidate_indexes[index]];
}

GraphDNode GraphDLoadBalancer::SelectLeastConnections(
    const std::vector<size_t>& candidate_indexes) {
  size_t best_index = candidate_indexes.front();
  for (size_t index : candidate_indexes) {
    if (nodes_[index].active_queries < nodes_[best_index].active_queries) {
      best_index = index;
    }
  }
  return nodes_[best_index];
}

GraphDNode GraphDLoadBalancer::SelectRandom(
    const std::vector<size_t>& candidate_indexes) {
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<size_t> dist(0, candidate_indexes.size() - 1);
  return nodes_[candidate_indexes[dist(gen)]];
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
      std::unique_lock<std::mutex> lock(refresh_cv_mutex_);
      refresh_cv_.wait_for(lock, std::chrono::seconds(1), [this]() {
        return !running_.load();
      });
      if (!running_.load()) {
        break;
      }
    }
  }
  
  // If all retries failed, try to refresh nodes and select again
  if (!running_.load()) {
    throw std::runtime_error("GraphD load balancer is stopped");
  }
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
