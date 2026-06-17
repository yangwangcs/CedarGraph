// Copyright 2025 The Cedar Authors
//
// Docker Compose Manager implementation

#include "cedar/client/docker_manager.h"

#include <array>
#include <iostream>
#include <memory>
#include <sstream>

namespace cedar {
namespace client {

DockerComposeManager::DockerComposeManager() = default;

DockerComposeManager::~DockerComposeManager() = default;

bool DockerComposeManager::Initialize(const std::string& compose_file) {
  compose_file_ = compose_file;
  initialized_ = true;
  return true;
}

bool DockerComposeManager::Up(bool detach) {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + " up";
  if (detach) {
    command += " -d";
  }

  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::Down() {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + " down";
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::Restart() {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + " restart";
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::StartService(const std::string& service) {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + " start " + service;
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::StopService(const std::string& service) {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + " stop " + service;
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::RestartService(const std::string& service) {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + " restart " + service;
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::Scale(const std::string& service, int replicas) {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + 
                        " up -d --scale " + service + "=" + std::to_string(replicas);
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

std::vector<ContainerStatus> DockerComposeManager::Ps() {
  if (!initialized_) {
    return {};
  }

  std::string command = "docker-compose -f " + compose_file_ + " ps";
  std::string output = ExecuteComposeCommand(command);
  return ParsePsOutput(output);
}

bool DockerComposeManager::IsServiceRunning(const std::string& service) {
  auto containers = Ps();

  for (const auto& container : containers) {
    if (container.name.find(service) != std::string::npos) {
      if (container.status.find("Up") != std::string::npos) {
        return true;
      }
    }
  }

  return false;
}

int DockerComposeManager::GetServiceReplicas(const std::string& service) {
  auto containers = Ps();

  int count = 0;
  for (const auto& container : containers) {
    if (container.name.find(service) != std::string::npos) {
      count++;
    }
  }

  return count;
}

std::string DockerComposeManager::Logs(const std::string& service, int lines) {
  if (!initialized_) {
    return "";
  }

  std::string command = "docker-compose -f " + compose_file_ + 
                        " logs --tail=" + std::to_string(lines) + " " + service;
  return ExecuteComposeCommand(command);
}

std::string DockerComposeManager::LogsAll(int lines) {
  if (!initialized_) {
    return "";
  }

  std::string command = "docker-compose -f " + compose_file_ + 
                        " logs --tail=" + std::to_string(lines);
  return ExecuteComposeCommand(command);
}

bool DockerComposeManager::Pull() {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + " pull";
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::Build() {
  if (!initialized_) {
    return false;
  }

  std::string command = "docker-compose -f " + compose_file_ + " build";
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

std::string DockerComposeManager::Exec(const std::string& service, const std::string& command) {
  if (!initialized_) {
    return "";
  }

  std::string full_command = "docker-compose -f " + compose_file_ + 
                             " exec " + service + " " + command;
  return ExecuteComposeCommand(full_command);
}

// ============================================================================
// Private methods
// ============================================================================

std::string DockerComposeManager::ExecuteComposeCommand(const std::string& command) {
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

std::vector<ContainerStatus> DockerComposeManager::ParsePsOutput(const std::string& output) {
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

}  // namespace client
}  // namespace cedar
