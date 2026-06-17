// Copyright 2025 The Cedar Authors
//
// Cluster Manager implementation

#include "cedar/client/cluster_manager.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>

namespace cedar {
namespace client {

ClusterManager::ClusterManager() = default;

ClusterManager::~ClusterManager() {
  // Cleanup
}

bool ClusterManager::Initialize(const ClusterConfig& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  config_ = config;
  initialized_ = true;
  return true;
}

bool ClusterManager::StartCluster() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!initialized_) {
    return false;
  }

  AddEvent("cluster", "Normal", "Starting", "Starting CedarGraph cluster");

  switch (config_.mode) {
    case DeploymentMode::DOCKER_COMPOSE:
      return StartDockerCompose();
    case DeploymentMode::KUBERNETES:
      return StartKubernetes();
    case DeploymentMode::LOCAL:
      // Local mode - not implemented yet
      return false;
    default:
      return false;
  }
}

bool ClusterManager::StopCluster() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!initialized_) {
    return false;
  }

  AddEvent("cluster", "Normal", "Stopping", "Stopping CedarGraph cluster");

  switch (config_.mode) {
    case DeploymentMode::DOCKER_COMPOSE:
      return StopDockerCompose();
    case DeploymentMode::KUBERNETES:
      return StopKubernetes();
    case DeploymentMode::LOCAL:
      return false;
    default:
      return false;
  }
}

bool ClusterManager::RestartCluster() {
  if (!StopCluster()) {
    return false;
  }
  return StartCluster();
}

bool ClusterManager::StartComponent(const std::string& component) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!initialized_) {
    return false;
  }

  AddEvent(component, "Normal", "Starting", "Starting " + component);

  if (config_.mode == DeploymentMode::DOCKER_COMPOSE) {
    std::string command = "docker-compose -f " + config_.config_file_path + 
                          " start " + component;
    std::string output = ExecuteCommand(command);
    return output.find("Error") == std::string::npos;
  }

  return false;
}

bool ClusterManager::StopComponent(const std::string& component) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!initialized_) {
    return false;
  }

  AddEvent(component, "Normal", "Stopping", "Stopping " + component);

  if (config_.mode == DeploymentMode::DOCKER_COMPOSE) {
    std::string command = "docker-compose -f " + config_.config_file_path + 
                          " stop " + component;
    std::string output = ExecuteCommand(command);
    return output.find("Error") == std::string::npos;
  }

  return false;
}

bool ClusterManager::RestartComponent(const std::string& component) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!initialized_) {
    return false;
  }

  AddEvent(component, "Normal", "Restarting", "Restarting " + component);

  if (config_.mode == DeploymentMode::DOCKER_COMPOSE) {
    std::string command = "docker-compose -f " + config_.config_file_path + 
                          " restart " + component;
    std::string output = ExecuteCommand(command);
    return output.find("Error") == std::string::npos;
  }

  return false;
}

ClusterStatusInfo ClusterManager::GetClusterStatus() {
  std::lock_guard<std::mutex> lock(mutex_);

  ClusterStatusInfo info;
  info.status = ClusterStatus::UNKNOWN;

  if (!initialized_) {
    return info;
  }

  if (config_.mode == DeploymentMode::DOCKER_COMPOSE) {
    // Get docker-compose ps output
    std::string command = "docker-compose -f " + config_.config_file_path + " ps";
    std::string output = ExecuteCommand(command);

    // Parse output
    auto containers = ParseDockerPs(output);

    // Count nodes by role
    for (const auto& container : containers) {
      if (container.name.find("metad") != std::string::npos) {
        info.metad_nodes++;
        if (container.status.find("Up") != std::string::npos) {
          info.metad_healthy++;
        }
      } else if (container.name.find("storaged") != std::string::npos) {
        info.storaged_nodes++;
        if (container.status.find("Up") != std::string::npos) {
          info.storaged_healthy++;
        }
      } else if (container.name.find("graphd") != std::string::npos) {
        info.graphd_nodes++;
        if (container.status.find("Up") != std::string::npos) {
          info.graphd_healthy++;
        }
      } else if (container.name.find("queryd") != std::string::npos) {
        info.queryd_nodes++;
        if (container.status.find("Up") != std::string::npos) {
          info.queryd_healthy++;
        }
      }
    }

    // Determine cluster status
    if (info.metad_healthy == 0 || info.storaged_healthy == 0) {
      info.status = ClusterStatus::ERROR;
    } else if (info.metad_healthy < info.metad_nodes || 
               info.storaged_healthy < info.storaged_nodes) {
      info.status = ClusterStatus::DEGRADED;
    } else {
      info.status = ClusterStatus::RUNNING;
    }
  }

  return info;
}

std::vector<NodeInfo> ClusterManager::GetNodes() {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<NodeInfo> nodes;

  if (!initialized_) {
    return nodes;
  }

  if (config_.mode == DeploymentMode::DOCKER_COMPOSE) {
    std::string command = "docker-compose -f " + config_.config_file_path + " ps";
    std::string output = ExecuteCommand(command);

    auto containers = ParseDockerPs(output);

    for (const auto& container : containers) {
      NodeInfo node;
      node.container_id = container.container_id;
      node.start_time = container.created;

      // Determine role
      if (container.name.find("metad") != std::string::npos) {
        node.role = NodeRole::METAD;
      } else if (container.name.find("storaged") != std::string::npos) {
        node.role = NodeRole::STORAGED;
      } else if (container.name.find("graphd") != std::string::npos) {
        node.role = NodeRole::GRAPHD;
      } else if (container.name.find("queryd") != std::string::npos) {
        node.role = NodeRole::QUERYD;
      }

      // Determine status
      if (container.status.find("Up") != std::string::npos) {
        node.status = NodeStatus::RUNNING;
      } else {
        node.status = NodeStatus::STOPPED;
      }

      node.node_id = container.name;
      nodes.push_back(node);
    }
  }

  return nodes;
}

std::vector<NodeInfo> ClusterManager::GetNodesByRole(NodeRole role) {
  auto all_nodes = GetNodes();
  std::vector<NodeInfo> filtered_nodes;

  for (const auto& node : all_nodes) {
    if (node.role == role) {
      filtered_nodes.push_back(node);
    }
  }

  return filtered_nodes;
}

bool ClusterManager::ScaleComponent(const std::string& component, int replicas) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!initialized_) {
    return false;
  }

  AddEvent(component, "Normal", "Scaling", 
           "Scaling " + component + " to " + std::to_string(replicas) + " replicas");

  if (config_.mode == DeploymentMode::DOCKER_COMPOSE) {
    std::string command = "docker-compose -f " + config_.config_file_path + 
                          " up -d --scale " + component + "=" + std::to_string(replicas);
    std::string output = ExecuteCommand(command);
    return output.find("Error") == std::string::npos;
  }

  return false;
}

bool ClusterManager::ScaleUp(const std::string& component) {
  // Get current replicas and increase by 1
  auto status = GetClusterStatus();
  int current_replicas = 0;

  if (component == "metad") {
    current_replicas = status.metad_nodes;
  } else if (component == "storaged") {
    current_replicas = status.storaged_nodes;
  } else if (component == "graphd") {
    current_replicas = status.graphd_nodes;
  } else if (component == "queryd") {
    current_replicas = status.queryd_nodes;
  }

  return ScaleComponent(component, current_replicas + 1);
}

bool ClusterManager::ScaleDown(const std::string& component) {
  // Get current replicas and decrease by 1
  auto status = GetClusterStatus();
  int current_replicas = 0;

  if (component == "metad") {
    current_replicas = status.metad_nodes;
  } else if (component == "storaged") {
    current_replicas = status.storaged_nodes;
  } else if (component == "graphd") {
    current_replicas = status.graphd_nodes;
  } else if (component == "queryd") {
    current_replicas = status.queryd_nodes;
  }

  if (current_replicas <= 1) {
    return false;  // Cannot scale below 1
  }

  return ScaleComponent(component, current_replicas - 1);
}

std::string ClusterManager::GetLogs(const std::string& component, int lines) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!initialized_) {
    return "";
  }

  if (config_.mode == DeploymentMode::DOCKER_COMPOSE) {
    std::string command = "docker-compose -f " + config_.config_file_path + 
                          " logs --tail=" + std::to_string(lines) + " " + component;
    return ExecuteCommand(command);
  }

  return "";
}

std::string ClusterManager::GetNodeLogs(const std::string& node_id, int lines) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!initialized_) {
    return "";
  }

  if (config_.mode == DeploymentMode::DOCKER_COMPOSE) {
    std::string command = "docker logs --tail=" + std::to_string(lines) + " " + node_id;
    return ExecuteCommand(command);
  }

  return "";
}

std::vector<ClusterEvent> ClusterManager::GetEvents(int limit) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (events_.size() > limit) {
    return std::vector<ClusterEvent>(events_.end() - limit, events_.end());
  }

  return events_;
}

void ClusterManager::SetEventCallback(ClusterEventCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  event_callback_ = callback;
}

bool ClusterManager::IsHealthy() {
  auto status = GetClusterStatus();
  return status.status == ClusterStatus::RUNNING;
}

bool ClusterManager::IsComponentHealthy(const std::string& component) {
  auto status = GetClusterStatus();

  if (component == "metad") {
    return status.metad_healthy > 0;
  } else if (component == "storaged") {
    return status.storaged_healthy > 0;
  } else if (component == "graphd") {
    return status.graphd_healthy > 0;
  } else if (component == "queryd") {
    return status.queryd_healthy > 0;
  }

  return false;
}

const ClusterConfig& ClusterManager::GetConfig() const {
  return config_;
}

void ClusterManager::UpdateConfig(const ClusterConfig& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  config_ = config;
}

// ============================================================================
// Internal methods
// ============================================================================

bool ClusterManager::StartDockerCompose() {
  std::string command = "docker-compose -f " + config_.config_file_path + " up -d";
  std::string output = ExecuteCommand(command);

  if (output.find("Error") != std::string::npos) {
    AddEvent("cluster", "Warning", "StartFailed", "Failed to start cluster: " + output);
    return false;
  }

  AddEvent("cluster", "Normal", "Started", "Cluster started successfully");
  return true;
}

bool ClusterManager::StopDockerCompose() {
  std::string command = "docker-compose -f " + config_.config_file_path + " down";
  std::string output = ExecuteCommand(command);

  if (output.find("Error") != std::string::npos) {
    AddEvent("cluster", "Warning", "StopFailed", "Failed to stop cluster: " + output);
    return false;
  }

  AddEvent("cluster", "Normal", "Stopped", "Cluster stopped successfully");
  return true;
}

bool ClusterManager::StartKubernetes() {
  std::string command = "kubectl apply -f " + config_.config_file_path;
  std::string output = ExecuteCommand(command);

  if (output.find("Error") != std::string::npos) {
    AddEvent("cluster", "Warning", "StartFailed", "Failed to start cluster: " + output);
    return false;
  }

  AddEvent("cluster", "Normal", "Started", "Cluster started successfully");
  return true;
}

bool ClusterManager::StopKubernetes() {
  std::string command = "kubectl delete -f " + config_.config_file_path;
  std::string output = ExecuteCommand(command);

  if (output.find("Error") != std::string::npos) {
    AddEvent("cluster", "Warning", "StopFailed", "Failed to stop cluster: " + output);
    return false;
  }

  AddEvent("cluster", "Normal", "Stopped", "Cluster stopped successfully");
  return true;
}

std::vector<ContainerStatus> ClusterManager::ParseDockerPs(const std::string& output) {
  std::vector<ContainerStatus> containers;

  std::istringstream stream(output);
  std::string line;

  // Skip header lines
  std::getline(stream, line);  // Header line 1
  std::getline(stream, line);  // Header line 2

  while (std::getline(stream, line)) {
    if (line.empty()) {
      continue;
    }

    ContainerStatus container;

    // Parse docker-compose ps output
    // Format: Name  Command  State  Ports
    std::istringstream line_stream(line);
    std::string name, command, state, ports;

    std::getline(line_stream, name, ' ');
    std::getline(line_stream, command, ' ');
    std::getline(line_stream, state, ' ');
    std::getline(line_stream, ports);

    container.name = name;
    container.status = state;
    container.ports = ports;

    containers.push_back(container);
  }

  return containers;
}

std::vector<ResourceStatus> ClusterManager::ParseKubectlGetPods(const std::string& output) {
  std::vector<ResourceStatus> resources;

  std::istringstream stream(output);
  std::string line;

  // Skip header
  std::getline(stream, line);

  while (std::getline(stream, line)) {
    if (line.empty()) {
      continue;
    }

    ResourceStatus resource;

    // Parse kubectl get pods output
    // Format: NAME  READY  STATUS  RESTARTS  AGE
    std::istringstream line_stream(line);
    std::string name, ready, status, restarts, age;

    line_stream >> name >> ready >> status >> restarts >> age;

    resource.name = name;
    resource.status = status;

    // Parse ready (e.g., "1/1")
    size_t slash = ready.find('/');
    if (slash != std::string::npos) {
      resource.ready_replicas = std::stoi(ready.substr(0, slash));
      resource.desired_replicas = std::stoi(ready.substr(slash + 1));
    }

    resources.push_back(resource);
  }

  return resources;
}

void ClusterManager::AddEvent(const std::string& component, const std::string& type,
                               const std::string& reason, const std::string& message) {
  ClusterEvent event;

  // Get current timestamp
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  char buffer[100];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&time));

  event.timestamp = buffer;
  event.component = component;
  event.type = type;
  event.reason = reason;
  event.message = message;

  events_.push_back(event);

  // Call callback if set
  if (event_callback_) {
    event_callback_(event);
  }
}

std::string ClusterManager::ExecuteCommand(const std::string& command) {
  std::array<char, 128> buffer;
  std::string result;

  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(command.c_str(), "r"), pclose);

  if (!pipe) {
    return "Error: Failed to execute command";
  }

  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }

  return result;
}

bool ClusterManager::IsDockerAvailable() {
  std::string output = ExecuteCommand("docker --version");
  return output.find("Docker version") != std::string::npos;
}

bool ClusterManager::IsKubernetesAvailable() {
  std::string output = ExecuteCommand("kubectl version --client");
  return output.find("Client Version") != std::string::npos;
}

}  // namespace client
}  // namespace cedar
