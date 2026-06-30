// Copyright 2025 The Cedar Authors. All rights reserved.
// GraphD Load Balancer - Client-side load balancing for multiple GraphD instances

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include <chrono>
#include <condition_variable>
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
    bool enable_tls = false;
    bool mtls_enabled = false;
    std::string ca_cert_path;
    std::string client_cert_path;
    std::string client_key_path;
  };

  explicit GraphDLoadBalancer(const Config& config);
  ~GraphDLoadBalancer();

  // Initialize and start refreshing node list
  bool Initialize();

  // Stop background refresh, if running.
  void Stop();

  // Select a GraphD node based on load balancing strategy
  GraphDNode SelectNode();

  // Select a node with failover support (retries on failure)
  GraphDNode SelectNodeWithFailover();

  // Mark a node as failed (for failover)
  void MarkNodeFailed(const std::string& address, int port);

  // Get all available nodes
  std::vector<GraphDNode> GetAvailableNodes();

  // Refresh node list from MetaD
  bool RefreshNodes();

  // Get node count
  size_t GetNodeCount() const;

 private:
  GraphDNode SelectRoundRobin(const std::vector<size_t>& candidate_indexes);
  GraphDNode SelectLeastConnections(const std::vector<size_t>& candidate_indexes);
  GraphDNode SelectRandom(const std::vector<size_t>& candidate_indexes);

  Config config_;
  std::vector<GraphDNode> nodes_;
  mutable std::mutex nodes_mutex_;
  std::atomic<size_t> round_robin_index_{0};
  std::unique_ptr<cedar::meta::MetaService::Stub> stub_;
  std::unique_ptr<std::thread> refresh_thread_;
  std::atomic<bool> running_{false};
  std::condition_variable refresh_cv_;
  std::mutex refresh_cv_mutex_;
  
  // Failover tracking
  struct FailedNode {
    std::string address;
    int port;
    std::chrono::steady_clock::time_point failed_at;
    int retry_count;
  };
  std::vector<FailedNode> failed_nodes_;
  static constexpr int kMaxRetries = 3;
  static constexpr int kRetryDelaySeconds = 5;
};

}  // namespace client
}  // namespace cedar
