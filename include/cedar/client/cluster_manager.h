// Copyright 2025 The Cedar Authors
//
// Cluster Manager - manages CedarGraph cluster lifecycle

#ifndef CEDAR_CLIENT_CLUSTER_MANAGER_H_
#define CEDAR_CLIENT_CLUSTER_MANAGER_H_

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/client/cluster_types.h"
#include "cedar/client/docker_manager.h"
#include "cedar/client/k8s_manager.h"

namespace cedar {
namespace client {

// Cluster event callback
using ClusterEventCallback = std::function<void(const ClusterEvent&)>;

// Cluster Manager
class ClusterManager {
 public:
  ClusterManager();
  ~ClusterManager();

  // Initialize cluster manager
  bool Initialize(const ClusterConfig& config);

  // Cluster lifecycle
  bool StartCluster();
  bool StopCluster();
  bool RestartCluster();

  // Component lifecycle
  bool StartComponent(const std::string& component);
  bool StopComponent(const std::string& component);
  bool RestartComponent(const std::string& component);

  // Cluster status
  ClusterStatusInfo GetClusterStatus();
  NodeStatus GetNodeStatus(const std::string& node_id);
  std::vector<NodeInfo> GetNodes();
  std::vector<NodeInfo> GetNodesByRole(NodeRole role);

  // Scaling
  bool ScaleComponent(const std::string& component, int replicas);
  bool ScaleUp(const std::string& component);
  bool ScaleDown(const std::string& component);

  // Logs
  std::string GetLogs(const std::string& component, int lines = 100);
  std::string GetNodeLogs(const std::string& node_id, int lines = 100);

  // Events
  std::vector<ClusterEvent> GetEvents(int limit = 100);
  void SetEventCallback(ClusterEventCallback callback);

  // Health check
  bool IsHealthy();
  bool IsComponentHealthy(const std::string& component);

  // Configuration
  const ClusterConfig& GetConfig() const;
  void UpdateConfig(const ClusterConfig& config);

 private:
  ClusterConfig config_;
  std::unique_ptr<DockerComposeManager> docker_manager_;
  std::unique_ptr<K8sManager> k8s_manager_;
  mutable std::mutex mutex_;
  ClusterEventCallback event_callback_;
  std::vector<ClusterEvent> events_;
  bool initialized_ = false;

  // Internal methods
  bool StartDockerCompose(const ClusterConfig& config);
  bool StopDockerCompose(const ClusterConfig& config);
  bool StartKubernetes(const ClusterConfig& config);
  bool StopKubernetes(const ClusterConfig& config);

  // Parse docker-compose ps output
  std::vector<ContainerStatus> ParseDockerPs(const std::string& output);

  // Parse kubectl get pods output
  std::vector<ResourceStatus> ParseKubectlGetPods(const std::string& output);

  // Add event
  void AddEvent(const std::string& component, const std::string& type,
                const std::string& reason, const std::string& message);

  bool IsValidComponentName(const std::string& component) const;
  bool IsValidResourceName(const std::string& name) const;
  std::string ShellQuote(const std::string& arg) const;
  std::string ComposeBaseCommand(const ClusterConfig& config) const;

  // Execute command
  std::string ExecuteCommand(const std::string& command);

  // Check if Docker is available
  bool IsDockerAvailable();

  // Check if K8s is available
  bool IsKubernetesAvailable();
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CLUSTER_MANAGER_H_
