// Copyright 2025 The Cedar Authors. All rights reserved.
// GraphD Load Balancer - Client-side load balancing for multiple GraphD instances

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include <chrono>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "meta_service.grpc.pb.h"

namespace cedar {
namespace client {

struct GraphDNode {
  std::string address;
  int port;
  std::string node_id;
  std::string state;  // ONLINE, BUSY, OFFLINE
  uint32_t current_qps;
  uint32_t active_queries;
  double cpu_usage;
  std::chrono::steady_clock::time_point last_seen;
};

class GraphDLoadBalancer {
 public:
  enum class Strategy {
    ROUND_ROBIN,
    LEAST_CONNECTIONS,
    RANDOM
  };

  struct Config {
    std::string meta_address = "127.0.0.1:10559";
    Strategy strategy = Strategy::ROUND_ROBIN;
    int refresh_interval_seconds = 10;
    int connection_timeout_ms = 5000;
  };

  explicit GraphDLoadBalancer(const Config& config);
  ~GraphDLoadBalancer() = default;

  // Initialize and start refreshing node list
  bool Initialize();

  // Select a GraphD node based on load balancing strategy
  GraphDNode SelectNode();

  // Get all available nodes
  std::vector<GraphDNode> GetAvailableNodes();

  // Refresh node list from MetaD
  bool RefreshNodes();

  // Get node count
  size_t GetNodeCount() const;

 private:
  GraphDNode SelectRoundRobin();
  GraphDNode SelectLeastConnections();
  GraphDNode SelectRandom();

  Config config_;
  std::vector<GraphDNode> nodes_;
  mutable std::mutex nodes_mutex_;
  std::atomic<size_t> round_robin_index_{0};
  std::unique_ptr<cedar::meta::MetaService::Stub> stub_;
  std::unique_ptr<std::thread> refresh_thread_;
  std::atomic<bool> running_{false};
};

}  // namespace client
}  // namespace cedar
