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

#ifndef CEDAR_STORAGE_HEALTH_MONITOR_H_
#define CEDAR_STORAGE_HEALTH_MONITOR_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/governance/health_checker.h"
#include "cedar/integration/event_bus.h"

namespace cedar {
namespace storage {

// =============================================================================
// Node Health Structure
// =============================================================================

struct NodeHealth {
  std::string node_id;
  std::string address;
  governance::HealthStatus status;
  std::chrono::steady_clock::time_point last_check;
  uint32_t consecutive_failures = 0;
  uint32_t consecutive_successes = 0;
  double latency_ms = 0.0;

  NodeHealth()
      : status(governance::HealthStatus::kUnknown),
        last_check(std::chrono::steady_clock::now()) {}
};

// =============================================================================
// Health Monitor Configuration
// =============================================================================

struct HealthMonitorConfig {
  std::chrono::seconds check_interval{5};
  std::chrono::seconds timeout{2};
  uint32_t failure_threshold = 3;
  uint32_t success_threshold = 2;
  bool enable_continuous_monitoring = true;
};

// =============================================================================
// Storage Health Monitor
// =============================================================================

class StorageHealthMonitor {
 public:
  using HealthChangeCallback = std::function<void(
      const std::string& node_id,
      governance::HealthStatus old_status,
      governance::HealthStatus new_status)>;

  StorageHealthMonitor();
  ~StorageHealthMonitor();

  StorageHealthMonitor(const StorageHealthMonitor&) = delete;
  StorageHealthMonitor& operator=(const StorageHealthMonitor&) = delete;
  StorageHealthMonitor(StorageHealthMonitor&&) = delete;
  StorageHealthMonitor& operator=(StorageHealthMonitor&&) = delete;

  // ---------------------------------------------------------------------------
  // Initialization
  // ---------------------------------------------------------------------------

  Status Initialize(const HealthMonitorConfig& config,
                    std::shared_ptr<governance::HealthChecker> health_checker,
                    std::shared_ptr<integration::EventBus> event_bus = nullptr);

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  Status Start();
  void Stop();

  // ---------------------------------------------------------------------------
  // Node Management
  // ---------------------------------------------------------------------------

  Status RegisterNode(const std::string& node_id,
                      const std::string& address,
                      uint16_t port);
  Status DeregisterNode(const std::string& node_id);

  // ---------------------------------------------------------------------------
  // Health Queries
  // ---------------------------------------------------------------------------

  StatusOr<NodeHealth> GetNodeHealth(const std::string& node_id) const;
  std::vector<NodeHealth> GetHealthyNodes() const;
  std::vector<NodeHealth> GetAllNodes() const;

  // ---------------------------------------------------------------------------
  // Callbacks
  // ---------------------------------------------------------------------------

  void SetHealthChangeCallback(HealthChangeCallback callback);

  // ---------------------------------------------------------------------------
  // Manual Health Check
  // ---------------------------------------------------------------------------

  Status CheckNodeHealth(const std::string& node_id);

 private:
  void MonitoringLoop();
  void CheckAllNodes();
  Status CheckNodeInternal(const std::string& node_id, NodeHealth& health);
  void UpdateNodeStatus(const std::string& node_id,
                        bool is_healthy,
                        double latency_ms);
  void PublishHealthEvent(const std::string& node_id,
                          governance::HealthStatus old_status,
                          governance::HealthStatus new_status);

  HealthMonitorConfig config_;
  std::shared_ptr<governance::HealthChecker> health_checker_;
  std::shared_ptr<integration::EventBus> event_bus_;

  mutable std::mutex nodes_mutex_;
  std::unordered_map<std::string, NodeHealth> nodes_;

  std::atomic<bool> running_{false};
  std::thread monitor_thread_;
  HealthChangeCallback health_change_callback_;
};

}  // namespace storage
}  // namespace cedar

#endif  // CEDAR_STORAGE_HEALTH_MONITOR_H_
