// Copyright 2025 The Cedar Authors
//
// Kubernetes Manager

#ifndef CEDAR_CLIENT_K8S_MANAGER_H_
#define CEDAR_CLIENT_K8S_MANAGER_H_

#include <string>
#include <vector>

#include "cedar/client/cluster_types.h"

namespace cedar {
namespace client {

// Kubernetes Manager
class K8sManager {
 public:
  K8sManager();
  ~K8sManager();

  // Initialize with namespace
  bool Initialize(const std::string& namespace_name = "cedargraph");

  // Resource operations
  bool Apply(const std::string& yaml_file);
  bool Delete(const std::string& yaml_file);
  bool ApplyDirectory(const std::string& directory);

  // Pod operations
  std::vector<ResourceStatus> GetPods();
  ResourceStatus GetPod(const std::string& pod_name);
  bool DeletePod(const std::string& pod_name);
  bool RestartPod(const std::string& pod_name);

  // Deployment operations
  bool ScaleDeployment(const std::string& deployment, int replicas);
  bool RolloutRestart(const std::string& deployment);

  // StatefulSet operations
  bool ScaleStatefulSet(const std::string& statefulset, int replicas);

  // Service operations
  std::vector<std::string> GetServices();

  // Logs
  std::string GetPodLogs(const std::string& pod_name, int lines = 100);
  std::string GetDeploymentLogs(const std::string& deployment, int lines = 100);

  // Events
  std::vector<ClusterEvent> GetEvents(int limit = 100);

  // Namespace operations
  bool CreateNamespace();
  bool DeleteNamespace();
  bool NamespaceExists();

  // Status
  bool IsReady();
  int GetReadyPods();

 private:
  std::string namespace_name_;
  bool initialized_ = false;

  // Execute kubectl command
  std::string ExecuteKubectl(const std::string& command);

  // Parse get pods output
  std::vector<ResourceStatus> ParseGetPods(const std::string& output);

  // Parse get events output
  std::vector<ClusterEvent> ParseGetEvents(const std::string& output);
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_K8S_MANAGER_H_
