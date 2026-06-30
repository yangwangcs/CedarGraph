// Copyright 2025 The Cedar Authors
//
// Cluster type definitions

#ifndef CEDAR_CLIENT_CLUSTER_TYPES_H_
#define CEDAR_CLIENT_CLUSTER_TYPES_H_

#include <string>
#include <vector>
#include <unordered_map>

namespace cedar {
namespace client {

// Node role
enum class NodeRole {
  METAD,
  STORAGED,
  GRAPHD,
  QUERYD,
  GCN
};

// Node status
enum class NodeStatus {
  UNKNOWN,
  STARTING,
  RUNNING,
  STOPPING,
  STOPPED,
  ERROR
};

// Cluster status
enum class ClusterStatus {
  UNKNOWN,
  STARTING,
  RUNNING,
  STOPPING,
  STOPPED,
  ERROR,
  DEGRADED
};

// Deployment mode
enum class DeploymentMode {
  DOCKER_COMPOSE,
  KUBERNETES,
  LOCAL
};

// Node information
struct NodeInfo {
  std::string node_id;
  NodeRole role = NodeRole::METAD;
  NodeStatus status = NodeStatus::UNKNOWN;
  std::string host;
  int port = 0;
  std::string container_id;  // Docker container ID
  std::string pod_name;      // K8s pod name
  int64_t start_time = 0;
  std::unordered_map<std::string, std::string> labels;
};

// Cluster configuration
struct ClusterConfig {
  DeploymentMode mode = DeploymentMode::DOCKER_COMPOSE;
  std::string config_file_path;  // docker-compose.yml or k8s yaml
  std::string namespace_name = "cedargraph";
  int metad_replicas = 3;
  int storaged_replicas = 3;
  int graphd_replicas = 2;
  int queryd_replicas = 1;
  std::string image_name = "cedargraph/cedar:k8s-fix-20260630";
  std::string data_dir = "./data";
  std::string log_dir = "./logs";
};

// Cluster status information
struct ClusterStatusInfo {
  ClusterStatus status = ClusterStatus::UNKNOWN;
  int metad_nodes = 0;
  int storaged_nodes = 0;
  int graphd_nodes = 0;
  int queryd_nodes = 0;
  int metad_healthy = 0;
  int storaged_healthy = 0;
  int graphd_healthy = 0;
  int queryd_healthy = 0;
  std::vector<NodeInfo> nodes;
  std::string error_message;
};

// Container status (Docker)
struct ContainerStatus {
  std::string container_id;
  std::string name;
  std::string image;
  std::string status;
  std::string ports;
  int64_t created = 0;
};

// Resource status (K8s)
struct ResourceStatus {
  std::string name;
  std::string kind;
  std::string namespace_name;
  std::string status;
  int ready_replicas = 0;
  int desired_replicas = 0;
  int64_t age = 0;
};

// Scale request
struct ScaleRequest {
  std::string component;  // metad, storaged, graphd, queryd
  int replicas;
};

// Cluster event
struct ClusterEvent {
  std::string timestamp;
  std::string component;
  std::string type;  // Normal, Warning
  std::string reason;
  std::string message;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CLUSTER_TYPES_H_
