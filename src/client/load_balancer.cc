// Copyright 2025 The Cedar Authors
//
// Load Balancer implementation

#include "cedar/client/load_balancer.h"

#include <algorithm>
#include <random>

namespace cedar {
namespace client {

LoadBalancer::LoadBalancer(LoadBalancingStrategy strategy)
    : strategy_(strategy) {}

LoadBalancer::~LoadBalancer() = default;

void LoadBalancer::UpdateNodes(const std::vector<LoadBalancerNode>& nodes) {
  std::lock_guard<std::mutex> lock(mutex_);
  nodes_ = nodes;
}

LoadBalancerNode LoadBalancer::SelectNode() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (nodes_.empty()) {
    return {};
  }

  // Filter healthy nodes
  std::vector<LoadBalancerNode*> healthy_nodes;
  for (auto& node : nodes_) {
    if (node.healthy) {
      healthy_nodes.push_back(&node);
    }
  }

  if (healthy_nodes.empty()) {
    // Return first node even if unhealthy (fallback)
    return nodes_[0];
  }

  switch (strategy_) {
    case LoadBalancingStrategy::ROUND_ROBIN:
      return SelectRoundRobin();
    case LoadBalancingStrategy::WEIGHTED:
      return SelectWeighted();
    case LoadBalancingStrategy::LEAST_CONNECTIONS:
      return SelectLeastConnections();
    case LoadBalancingStrategy::RANDOM:
      return SelectRandom();
    default:
      return SelectRoundRobin();
  }
}

void LoadBalancer::OnConnectionStart(const std::string& host, int port) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& node : nodes_) {
    if (node.host == host && node.port == port) {
      node.active_connections++;
      break;
    }
  }
}

void LoadBalancer::OnConnectionEnd(const std::string& host, int port) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& node : nodes_) {
    if (node.host == host && node.port == port) {
      if (node.active_connections > 0) {
        node.active_connections--;
      }
      break;
    }
  }
}

void LoadBalancer::SetNodeHealth(const std::string& host, int port, bool healthy) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& node : nodes_) {
    if (node.host == host && node.port == port) {
      node.healthy = healthy;
      break;
    }
  }
}

LoadBalancerNode LoadBalancer::SelectRoundRobin() {
  // Filter healthy nodes
  std::vector<LoadBalancerNode*> healthy_nodes;
  for (auto& node : nodes_) {
    if (node.healthy) {
      healthy_nodes.push_back(&node);
    }
  }

  if (healthy_nodes.empty()) {
    return nodes_[0];
  }

  int index = round_robin_index_++ % healthy_nodes.size();
  return *healthy_nodes[index];
}

LoadBalancerNode LoadBalancer::SelectWeighted() {
  // Calculate total weight
  int total_weight = 0;
  for (const auto& node : nodes_) {
    if (node.healthy) {
      total_weight += node.weight;
    }
  }

  if (total_weight == 0) {
    return nodes_[0];
  }

  // Select based on weight
  int random_weight = rand() % total_weight;
  int current_weight = 0;

  for (const auto& node : nodes_) {
    if (node.healthy) {
      current_weight += node.weight;
      if (random_weight < current_weight) {
        return node;
      }
    }
  }

  return nodes_[0];
}

LoadBalancerNode LoadBalancer::SelectLeastConnections() {
  LoadBalancerNode* best = nullptr;
  int min_connections = INT_MAX;

  for (auto& node : nodes_) {
    if (node.healthy && node.active_connections < min_connections) {
      min_connections = node.active_connections;
      best = &node;
    }
  }

  if (best) {
    return *best;
  }

  return nodes_[0];
}

LoadBalancerNode LoadBalancer::SelectRandom() {
  // Filter healthy nodes
  std::vector<LoadBalancerNode*> healthy_nodes;
  for (auto& node : nodes_) {
    if (node.healthy) {
      healthy_nodes.push_back(&node);
    }
  }

  if (healthy_nodes.empty()) {
    return nodes_[0];
  }

  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, healthy_nodes.size() - 1);

  return *healthy_nodes[dis(gen)];
}

}  // namespace client
}  // namespace cedar
