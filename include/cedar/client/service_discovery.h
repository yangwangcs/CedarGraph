// Copyright 2025 The Cedar Authors
//
// Service Discovery for finding available nodes
// Uses MetaD to discover GraphD and StorageD nodes

#ifndef CEDAR_CLIENT_SERVICE_DISCOVERY_H_
#define CEDAR_CLIENT_SERVICE_DISCOVERY_H_

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "meta_service.grpc.pb.h"

namespace cedar {
namespace client {

// Service node information
struct ServiceNode {
  std::string host;
  int port;
  std::string node_id;
  bool is_leader;
  int64_t last_heartbeat;
};

// Service discovery configuration
struct ServiceDiscoveryConfig {
  std::string metad_host;
  int metad_port;
  int refresh_interval_ms = 10000;
  int heartbeat_timeout_ms = 30000;
};

// Service discovery class
class ServiceDiscovery {
 public:
  ServiceDiscovery(const ServiceDiscoveryConfig& config);
  ~ServiceDiscovery();

  // Initialize service discovery
  bool Initialize();

  // Get available GraphD nodes
  std::vector<ServiceNode> GetGraphDNodes();

  // Get available StorageD nodes
  std::vector<ServiceNode> GetStorageDNodes();

  // Get leader node for a partition
  ServiceNode GetPartitionLeader(const std::string& space_name, int partition_id);

  // Get all nodes for a partition
  std::vector<ServiceNode> GetPartitionNodes(const std::string& space_name, int partition_id);

  // Refresh node information
  bool RefreshNodes();

  // Get node health status
  bool IsNodeHealthy(const std::string& node_id);

 private:
  ServiceDiscoveryConfig config_;
  std::shared_ptr<grpc::Channel> metad_channel_;
  std::unique_ptr<cedar::meta::MetaService::Stub> metad_stub_;
  
  std::vector<ServiceNode> graphd_nodes_;
  std::vector<ServiceNode> storaged_nodes_;
  std::mutex mutex_;
  
  // Background refresh thread
  std::thread refresh_thread_;
  std::atomic<bool> running_{false};
  
  // Node health tracking
  std::unordered_map<std::string, int64_t> last_heartbeats_;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_SERVICE_DISCOVERY_H_
