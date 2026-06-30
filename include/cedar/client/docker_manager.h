// Copyright 2025 The Cedar Authors
//
// Docker Compose Manager

#ifndef CEDAR_CLIENT_DOCKER_MANAGER_H_
#define CEDAR_CLIENT_DOCKER_MANAGER_H_

#include <string>
#include <vector>

#include "cedar/client/cluster_types.h"

namespace cedar {
namespace client {

// Docker Compose Manager
class DockerComposeManager {
 public:
  DockerComposeManager();
  ~DockerComposeManager();

  // Initialize with compose file
  bool Initialize(const std::string& compose_file);

  // Cluster operations
  bool Up(bool detach = true);
  bool Down();
  bool Restart();

  // Service operations
  bool StartService(const std::string& service);
  bool StopService(const std::string& service);
  bool RestartService(const std::string& service);

  // Scaling
  bool Scale(const std::string& service, int replicas);

  // Status
  std::vector<ContainerStatus> Ps();
  bool IsServiceRunning(const std::string& service);
  int GetServiceReplicas(const std::string& service);

  // Logs
  std::string Logs(const std::string& service, int lines = 100);
  std::string LogsAll(int lines = 100);

  // Pull images
  bool Pull();

  // Build images
  bool Build();

  // Execute command in container
  std::string Exec(const std::string& service, const std::string& command);

 private:
  std::string compose_file_;
  bool initialized_ = false;

  bool IsValidServiceName(const std::string& service) const;
  std::string ShellQuote(const std::string& arg) const;
  std::string ComposeBaseCommand() const;

  // Execute docker-compose command
  std::string ExecuteComposeCommand(const std::string& command);

  // Parse ps output
  std::vector<ContainerStatus> ParsePsOutput(const std::string& output);
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_DOCKER_MANAGER_H_
