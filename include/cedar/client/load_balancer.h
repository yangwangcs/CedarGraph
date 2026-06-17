// Copyright 2025 The Cedar Authors
//
// Load Balancer for distributing requests across multiple nodes

#ifndef CEDAR_CLIENT_LOAD_BALANCER_H_
#define CEDAR_CLIENT_LOAD_BALANCER_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace cedar {
namespace client {

// Load balancing strategy
enum class LoadBalancingStrategy {
  ROUND_ROBIN,      // Simple round-robin
  WEIGHTED,         // Weighted round-robin
  LEAST_CONNECTIONS, // Route to node with fewest connections
  RANDOM            // Random selection
};

// Node with load information
struct LoadBalancerNode {
  std::string host;
  int port;
  int weight = 1;
  int active_connections = 0;
  bool healthy = true;
};

// Load Balancer
class LoadBalancer {
 public:
  LoadBalancer(LoadBalancingStrategy strategy = LoadBalancingStrategy::ROUND_ROBIN);
  ~LoadBalancer();

  // Update available nodes
  void UpdateNodes(const std::vector<LoadBalancerNode>& nodes);

  // Select a node for the next request
  LoadBalancerNode SelectNode();

  // Report connection start/end for load tracking
  void OnConnectionStart(const std::string& host, int port);
  void OnConnectionEnd(const std::string& host, int port);

  // Mark node as healthy/unhealthy
  void SetNodeHealth(const std::string& host, int port, bool healthy);

  // Get current strategy
  LoadBalancingStrategy GetStrategy() const { return strategy_; }

  // Set strategy
  void SetStrategy(LoadBalancingStrategy strategy) { strategy_ = strategy; }

 private:
  LoadBalancingStrategy strategy_;
  std::vector<LoadBalancerNode> nodes_;
  mutable std::mutex mutex_;
  std::atomic<int> round_robin_index_{0};

  // Select using round-robin
  LoadBalancerNode SelectRoundRobin();

  // Select using weighted round-robin
  LoadBalancerNode SelectWeighted();

  // Select using least connections
  LoadBalancerNode SelectLeastConnections();

  // Select using random
  LoadBalancerNode SelectRandom();
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_LOAD_BALANCER_H_
