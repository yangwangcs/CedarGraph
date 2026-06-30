// Copyright 2025 The Cedar Authors
//
// Kubernetes Manager implementation

#include "cedar/client/k8s_manager.h"

#include <array>
#include <cctype>
#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>
#include <sys/wait.h>

namespace cedar {
namespace client {

K8sManager::K8sManager() = default;

K8sManager::~K8sManager() = default;

bool K8sManager::Initialize(const std::string& namespace_name) {
  if (!IsValidResourceName(namespace_name)) {
    return false;
  }
  namespace_name_ = namespace_name;
  initialized_ = true;
  return true;
}

bool K8sManager::Apply(const std::string& yaml_file) {
  if (!initialized_) {
    return false;
  }

  std::string command = "kubectl apply -f " + ShellQuote(yaml_file) + NamespaceArg();
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

bool K8sManager::Delete(const std::string& yaml_file) {
  if (!initialized_) {
    return false;
  }

  std::string command = "kubectl delete -f " + ShellQuote(yaml_file) + NamespaceArg();
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

bool K8sManager::ApplyDirectory(const std::string& directory) {
  if (!initialized_) {
    return false;
  }

  std::string command = "kubectl apply -f " + ShellQuote(directory) + NamespaceArg();
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

std::vector<ResourceStatus> K8sManager::GetPods() {
  if (!initialized_) {
    return {};
  }

  std::string command = "kubectl get pods" + NamespaceArg() + " -o wide";
  std::string output = ExecuteKubectl(command);
  return ParseGetPods(output);
}

ResourceStatus K8sManager::GetPod(const std::string& pod_name) {
  if (!initialized_) {
    return {};
  }
  if (!IsValidResourceName(pod_name)) {
    return {};
  }

  std::string command = "kubectl get pod " + pod_name + NamespaceArg();
  std::string output = ExecuteKubectl(command);

  auto pods = ParseGetPods(output);
  if (!pods.empty()) {
    return pods[0];
  }

  return {};
}

bool K8sManager::DeletePod(const std::string& pod_name) {
  if (!initialized_) {
    return false;
  }
  if (!IsValidResourceName(pod_name)) {
    return false;
  }

  std::string command = "kubectl delete pod " + pod_name + NamespaceArg();
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

bool K8sManager::RestartPod(const std::string& pod_name) {
  if (!initialized_) {
    return false;
  }

  // Delete pod to trigger restart
  return DeletePod(pod_name);
}

bool K8sManager::ScaleDeployment(const std::string& deployment, int replicas) {
  if (!initialized_) {
    return false;
  }
  if (!IsValidResourceName(deployment) || replicas < 0) {
    return false;
  }

  std::string command = "kubectl scale deployment " + deployment + 
                        " --replicas=" + std::to_string(replicas) + NamespaceArg();
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

bool K8sManager::RolloutRestart(const std::string& deployment) {
  if (!initialized_) {
    return false;
  }
  if (!IsValidResourceName(deployment)) {
    return false;
  }

  std::string command = "kubectl rollout restart deployment " + deployment + 
                        NamespaceArg();
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

bool K8sManager::ScaleStatefulSet(const std::string& statefulset, int replicas) {
  if (!initialized_) {
    return false;
  }
  if (!IsValidResourceName(statefulset) || replicas < 0) {
    return false;
  }

  std::string command = "kubectl scale statefulset " + statefulset + 
                        " --replicas=" + std::to_string(replicas) + NamespaceArg();
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

std::vector<std::string> K8sManager::GetServices() {
  if (!initialized_) {
    return {};
  }

  std::string command = "kubectl get services" + NamespaceArg();
  std::string output = ExecuteKubectl(command);

  std::vector<std::string> services;
  std::istringstream stream(output);
  std::string line;

  // Skip header
  std::getline(stream, line);

  while (std::getline(stream, line)) {
    if (line.empty()) {
      continue;
    }

    std::istringstream line_stream(line);
    std::string name;
    line_stream >> name;
    services.push_back(name);
  }

  return services;
}

std::string K8sManager::GetPodLogs(const std::string& pod_name, int lines) {
  if (!initialized_) {
    return "";
  }
  if (!IsValidResourceName(pod_name) || lines < 0) {
    return "";
  }

  std::string command = "kubectl logs " + pod_name + " --tail=" + 
                        std::to_string(lines) + NamespaceArg();
  return ExecuteKubectl(command);
}

std::string K8sManager::GetDeploymentLogs(const std::string& deployment, int lines) {
  if (!initialized_) {
    return "";
  }
  if (!IsValidResourceName(deployment) || lines < 0) {
    return "";
  }

  std::string command = "kubectl logs deployment/" + deployment + " --tail=" + 
                        std::to_string(lines) + NamespaceArg();
  return ExecuteKubectl(command);
}

std::vector<ClusterEvent> K8sManager::GetEvents(int limit) {
  if (!initialized_) {
    return {};
  }
  if (limit <= 0) {
    return {};
  }

  std::string command = "kubectl get events" + NamespaceArg() +
                        " --sort-by=.metadata.creationTimestamp";
  std::string output = ExecuteKubectl(command);
  return ParseGetEvents(output);
}

bool K8sManager::CreateNamespace() {
  if (!initialized_) {
    return false;
  }
  std::string command = "kubectl create namespace " + namespace_name_;
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

bool K8sManager::DeleteNamespace() {
  if (!initialized_) {
    return false;
  }
  std::string command = "kubectl delete namespace " + namespace_name_;
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

bool K8sManager::NamespaceExists() {
  if (!initialized_) {
    return false;
  }
  std::string command = "kubectl get namespace " + namespace_name_;
  std::string output = ExecuteKubectl(command);
  return output.find("Error") == std::string::npos;
}

bool K8sManager::IsReady() {
  auto pods = GetPods();
  if (pods.empty()) {
    return false;
  }

  for (const auto& pod : pods) {
    if (pod.ready_replicas < pod.desired_replicas) {
      return false;
    }
    if (pod.desired_replicas <= 0 || pod.status != "Running") {
      return false;
    }
  }

  return true;
}

int K8sManager::GetReadyPods() {
  auto pods = GetPods();

  int ready = 0;
  for (const auto& pod : pods) {
    if (pod.ready_replicas == pod.desired_replicas) {
      ready++;
    }
  }

  return ready;
}

// ============================================================================
// Private methods
// ============================================================================

bool K8sManager::IsValidResourceName(const std::string& name) const {
  if (name.empty()) {
    return false;
  }
  for (unsigned char c : name) {
    if (!std::isalnum(c) && c != '-' && c != '.') {
      return false;
    }
  }
  return true;
}

std::string K8sManager::ShellQuote(const std::string& arg) const {
  std::string quoted = "'";
  for (char c : arg) {
    if (c == '\'') {
      quoted += "'\\''";
    } else {
      quoted += c;
    }
  }
  quoted += "'";
  return quoted;
}

std::string K8sManager::NamespaceArg() const {
  return " -n " + namespace_name_;
}

std::string K8sManager::ExecuteKubectl(const std::string& command) {
  std::array<char, 128> buffer;
  std::string result;

  std::string redirected_command = command + " 2>&1";
  FILE* pipe = popen(redirected_command.c_str(), "r");

  if (!pipe) {
    return "Error: Failed to execute command";
  }

  while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
    result += buffer.data();
  }

  int exit_status = pclose(pipe);
  if (exit_status == -1) {
    if (!result.empty() && result.back() != '\n') {
      result += '\n';
    }
    result += "Error: Failed to close command pipe";
  } else if (WIFEXITED(exit_status) && WEXITSTATUS(exit_status) != 0) {
    if (!result.empty() && result.back() != '\n') {
      result += '\n';
    }
    result += "Error: command exited with status " +
              std::to_string(WEXITSTATUS(exit_status));
  } else if (WIFSIGNALED(exit_status)) {
    if (!result.empty() && result.back() != '\n') {
      result += '\n';
    }
    result += "Error: command terminated by signal " +
              std::to_string(WTERMSIG(exit_status));
  }

  return result;
}

std::vector<ResourceStatus> K8sManager::ParseGetPods(const std::string& output) {
  std::vector<ResourceStatus> pods;

  std::istringstream stream(output);
  std::string line;

  // Skip header
  std::getline(stream, line);

  while (std::getline(stream, line)) {
    if (line.empty()) {
      continue;
    }

    ResourceStatus pod;

    // Parse kubectl get pods output
    // Format: NAME  READY  STATUS  RESTARTS  AGE
    std::istringstream line_stream(line);
    std::string name, ready, status, restarts, age;

    line_stream >> name >> ready >> status >> restarts >> age;

    pod.name = name;
    pod.status = status;
    pod.namespace_name = namespace_name_;

    // Parse ready (e.g., "1/1")
    size_t slash = ready.find('/');
    if (slash != std::string::npos) {
      try {
        pod.ready_replicas = std::stoi(ready.substr(0, slash));
        pod.desired_replicas = std::stoi(ready.substr(slash + 1));
      } catch (...) {
        pod.ready_replicas = 0;
        pod.desired_replicas = 0;
      }
    }

    pods.push_back(pod);
  }

  return pods;
}

std::vector<ClusterEvent> K8sManager::ParseGetEvents(const std::string& output) {
  std::vector<ClusterEvent> events;

  std::istringstream stream(output);
  std::string line;

  // Skip header
  std::getline(stream, line);

  while (std::getline(stream, line)) {
    if (line.empty()) {
      continue;
    }

    ClusterEvent event;

    // Parse kubectl get events output
    // Format: LAST SEEN  TYPE  REASON  OBJECT  MESSAGE
    std::istringstream line_stream(line);
    std::string last_seen, type, reason, object, message;

    line_stream >> last_seen >> type >> reason >> object;
    std::getline(line_stream, message);

    event.timestamp = last_seen;
    event.type = type;
    event.reason = reason;
    event.message = message;

    events.push_back(event);
  }

  return events;
}

}  // namespace client
}  // namespace cedar
