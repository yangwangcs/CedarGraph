// Copyright 2025 The Cedar Authors
//
// Docker Compose Manager implementation

#include "cedar/client/docker_manager.h"

#include <array>
#include <cctype>
#include <cstdio>
#include <iostream>
#include <memory>
#include <sstream>
#include <sys/wait.h>

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

  std::string command = ComposeBaseCommand() + " up";
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

  std::string command = ComposeBaseCommand() + " down";
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::Restart() {
  if (!initialized_) {
    return false;
  }

  std::string command = ComposeBaseCommand() + " restart";
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::StartService(const std::string& service) {
  if (!initialized_) {
    return false;
  }
  if (!IsValidServiceName(service)) {
    return false;
  }

  std::string command = ComposeBaseCommand() + " start " + service;
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::StopService(const std::string& service) {
  if (!initialized_) {
    return false;
  }
  if (!IsValidServiceName(service)) {
    return false;
  }

  std::string command = ComposeBaseCommand() + " stop " + service;
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::RestartService(const std::string& service) {
  if (!initialized_) {
    return false;
  }
  if (!IsValidServiceName(service)) {
    return false;
  }

  std::string command = ComposeBaseCommand() + " restart " + service;
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::Scale(const std::string& service, int replicas) {
  if (!initialized_) {
    return false;
  }
  if (!IsValidServiceName(service) || replicas < 0) {
    return false;
  }

  std::string command = ComposeBaseCommand() +
                        " up -d --scale " + service + "=" + std::to_string(replicas);
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

std::vector<ContainerStatus> DockerComposeManager::Ps() {
  if (!initialized_) {
    return {};
  }

  std::string command = ComposeBaseCommand() + " ps";
  std::string output = ExecuteComposeCommand(command);
  return ParsePsOutput(output);
}

bool DockerComposeManager::IsServiceRunning(const std::string& service) {
  if (!IsValidServiceName(service)) {
    return false;
  }

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
  if (!IsValidServiceName(service)) {
    return 0;
  }

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
  if (!IsValidServiceName(service) || lines < 0) {
    return "";
  }

  std::string command = ComposeBaseCommand() +
                        " logs --tail=" + std::to_string(lines) + " " + service;
  return ExecuteComposeCommand(command);
}

std::string DockerComposeManager::LogsAll(int lines) {
  if (!initialized_) {
    return "";
  }
  if (lines < 0) {
    return "";
  }

  std::string command = ComposeBaseCommand() +
                        " logs --tail=" + std::to_string(lines);
  return ExecuteComposeCommand(command);
}

bool DockerComposeManager::Pull() {
  if (!initialized_) {
    return false;
  }

  std::string command = ComposeBaseCommand() + " pull";
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

bool DockerComposeManager::Build() {
  if (!initialized_) {
    return false;
  }

  std::string command = ComposeBaseCommand() + " build";
  std::string output = ExecuteComposeCommand(command);
  return output.find("Error") == std::string::npos;
}

std::string DockerComposeManager::Exec(const std::string& service, const std::string& command) {
  if (!initialized_) {
    return "";
  }
  if (!IsValidServiceName(service) || command.empty()) {
    return "";
  }

  std::string full_command = ComposeBaseCommand() +
                             " exec " + service + " " + command;
  return ExecuteComposeCommand(full_command);
}

// ============================================================================
// Private methods
// ============================================================================

bool DockerComposeManager::IsValidServiceName(const std::string& service) const {
  if (service.empty()) {
    return false;
  }
  for (unsigned char c : service) {
    if (!std::isalnum(c) && c != '_' && c != '-' && c != '.') {
      return false;
    }
  }
  return true;
}

std::string DockerComposeManager::ShellQuote(const std::string& arg) const {
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

std::string DockerComposeManager::ComposeBaseCommand() const {
  return "docker-compose -f " + ShellQuote(compose_file_);
}

std::string DockerComposeManager::ExecuteComposeCommand(const std::string& command) {
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
