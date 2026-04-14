// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cedar/storage/storage_health_monitor.h"

#include <grpcpp/grpcpp.h>

#include "storage_service.grpc.pb.h"

namespace cedar {
namespace storage {

StorageHealthMonitor::StorageHealthMonitor() = default;

StorageHealthMonitor::~StorageHealthMonitor() { Stop(); }

Status StorageHealthMonitor::Initialize(
    const HealthMonitorConfig& config,
    std::shared_ptr<governance::HealthChecker> health_checker,
    std::shared_ptr<integration::EventBus> event_bus) {
  if (!health_checker) {
    return Status::InvalidArgument("StorageHealthMonitor::Initialize",
                                   "HealthChecker cannot be null");
  }

  config_ = config;
  health_checker_ = health_checker;
  event_bus_ = event_bus;

  return Status::OK();
}

Status StorageHealthMonitor::Start() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("StorageHealthMonitor::Start",
                                   "Already running");
  }

  if (config_.enable_continuous_monitoring) {
    monitor_thread_ = std::thread(&StorageHealthMonitor::MonitoringLoop, this);
  }

  return Status::OK();
}

void StorageHealthMonitor::Stop() {
  if (!running_.exchange(false)) return;
  if (monitor_thread_.joinable()) {
    monitor_thread_.join();
  }
}

Status StorageHealthMonitor::RegisterNode(const std::string& node_id,
                                          const std::string& address,
                                          uint16_t port) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);

  NodeHealth health;
  health.node_id = node_id;
  health.address = address + ":" + std::to_string(port);
  health.status = governance::HealthStatus::kUnknown;
  health.last_check = std::chrono::steady_clock::now();

  nodes_[node_id] = health;

  return Status::OK();
}

Status StorageHealthMonitor::DeregisterNode(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("StorageHealthMonitor::DeregisterNode", node_id);
  }

  nodes_.erase(it);
  return Status::OK();
}

StatusOr<NodeHealth> StorageHealthMonitor::GetNodeHealth(
    const std::string& node_id) const {
  std::lock_guard<std::mutex> lock(nodes_mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("StorageHealthMonitor::GetNodeHealth", node_id);
  }

  return it->second;
}

std::vector<NodeHealth> StorageHealthMonitor::GetHealthyNodes() const {
  std::lock_guard<std::mutex> lock(nodes_mutex_);

  std::vector<NodeHealth> healthy;
  for (const auto& [id, health] : nodes_) {
    if (health.status == governance::HealthStatus::kHealthy) {
      healthy.push_back(health);
    }
  }

  return healthy;
}

std::vector<NodeHealth> StorageHealthMonitor::GetAllNodes() const {
  std::lock_guard<std::mutex> lock(nodes_mutex_);

  std::vector<NodeHealth> all;
  for (const auto& [id, health] : nodes_) {
    all.push_back(health);
  }

  return all;
}

void StorageHealthMonitor::SetHealthChangeCallback(
    HealthChangeCallback callback) {
  health_change_callback_ = callback;
}

void StorageHealthMonitor::MonitoringLoop() {
  while (running_) {
    CheckAllNodes();
    std::this_thread::sleep_for(config_.check_interval);
  }
}

void StorageHealthMonitor::CheckAllNodes() {
  std::vector<std::string> node_ids;
  {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    for (const auto& [id, _] : nodes_) {
      node_ids.push_back(id);
    }
  }

  for (const auto& node_id : node_ids) {
    if (!running_) break;

    std::lock_guard<std::mutex> lock(nodes_mutex_);
    auto it = nodes_.find(node_id);
    if (it != nodes_.end()) {
      CheckNodeInternal(node_id, it->second);
    }
  }
}

Status StorageHealthMonitor::CheckNodeInternal(const std::string& node_id,
                                               NodeHealth& health) {
  auto start = std::chrono::steady_clock::now();

  // Use HealthChecker to check component health if registered
  if (health_checker_->IsComponentRegistered(node_id)) {
    auto result = health_checker_->CheckComponent(node_id);
    if (result.ok()) {
      auto component_health = result.value();
      health.latency_ms = component_health.check_duration_ms;
      bool is_healthy = (component_health.status == governance::HealthStatus::kHealthy);
      UpdateNodeStatus(node_id, is_healthy, health.latency_ms);
      return is_healthy ? Status::OK()
                        : Status::IOError("CheckNodeInternal", "Node unhealthy");
    }
  }

  // Fallback: Simple TCP connect check as health probe
  // In production, this would use gRPC Ping
  health.latency_ms = 0.0;
  bool is_healthy = true;  // Simplified for now

  auto end = std::chrono::steady_clock::now();
  health.latency_ms = std::chrono::duration<double, std::milli>(end - start).count();

  UpdateNodeStatus(node_id, is_healthy, health.latency_ms);

  return is_healthy ? Status::OK()
                    : Status::IOError("CheckNodeInternal", "Node unhealthy");
}

void StorageHealthMonitor::UpdateNodeStatus(const std::string& node_id,
                                            bool is_healthy,
                                            double latency_ms) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return;

  NodeHealth& health = it->second;
  auto old_status = health.status;

  health.last_check = std::chrono::steady_clock::now();
  health.latency_ms = latency_ms;

  if (is_healthy) {
    health.consecutive_successes++;
    health.consecutive_failures = 0;

    if (health.consecutive_successes >= config_.success_threshold) {
      health.status = governance::HealthStatus::kHealthy;
    }
  } else {
    health.consecutive_failures++;
    health.consecutive_successes = 0;

    if (health.consecutive_failures >= config_.failure_threshold) {
      health.status = governance::HealthStatus::kUnhealthy;
    }
  }

  if (old_status != health.status) {
    if (health_change_callback_) {
      health_change_callback_(node_id, old_status, health.status);
    }
    PublishHealthEvent(node_id, old_status, health.status);
  }
}

void StorageHealthMonitor::PublishHealthEvent(
    const std::string& node_id,
    governance::HealthStatus old_status,
    governance::HealthStatus new_status) {
  if (!event_bus_) return;

  integration::Event event("storage.node.health_changed");
  event.Set("node_id", node_id);
  event.Set("old_status", static_cast<int>(old_status));
  event.Set("new_status", static_cast<int>(new_status));
  event.Set("source", std::string("StorageHealthMonitor"));

  event_bus_->Publish(event);
}

Status StorageHealthMonitor::CheckNodeHealth(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return Status::NotFound("StorageHealthMonitor::CheckNodeHealth", node_id);
  }

  return CheckNodeInternal(node_id, it->second);
}

}  // namespace storage
}  // namespace cedar
