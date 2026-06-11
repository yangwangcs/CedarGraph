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

#ifndef CEDAR_GOVERNANCE_HEALTH_CHECKER_H_
#define CEDAR_GOVERNANCE_HEALTH_CHECKER_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/core/threading.h"

namespace cedar {
namespace governance {

// =============================================================================
// Health Status Enumeration
// =============================================================================

enum class HealthStatus {
  kUnknown = 0,     // Status is unknown (initial state)
  kStarting = 1,    // Component is starting up
  kHealthy = 2,     // Component is healthy
  kDegraded = 3,    // Component is functioning but degraded
  kUnhealthy = 4    // Component is unhealthy
};

// Convert HealthStatus to string
std::string HealthStatusToString(HealthStatus status);

// Convert HealthStatus to HTTP status code
int HealthStatusToHttpCode(HealthStatus status);

// Compare two health statuses - returns the worse one
HealthStatus WorseHealthStatus(HealthStatus a, HealthStatus b);

// =============================================================================
// Component Health Structure
// =============================================================================

struct ComponentHealth {
  std::string name;             // Component name
  HealthStatus status;          // Current health status
  std::string message;          // Optional status message
  int64_t last_check_time;      // Last check timestamp (ms since epoch)
  int64_t check_duration_ms;    // Duration of last check (ms)

  ComponentHealth()
      : status(HealthStatus::kUnknown),
        last_check_time(0),
        check_duration_ms(0) {}
};

// =============================================================================
// Health Change Event Types
// =============================================================================

enum class HealthChangeType {
  kStatusChanged = 0,   // Component status changed
  kCheckPerformed = 1,  // Health check was performed (status unchanged)
  kComponentAdded = 2,  // New component registered
  kComponentRemoved = 3 // Component unregistered
};

// Convert HealthChangeType to string
std::string HealthChangeTypeToString(HealthChangeType type);

// =============================================================================
// Health Change Event Structure
// =============================================================================

struct HealthChangeEvent {
  HealthChangeType type;        // Type of change
  std::string component_name;   // Component that changed
  HealthStatus old_status;      // Previous status (for kStatusChanged)
  HealthStatus new_status;      // New status
  int64_t timestamp_ms;         // When the change occurred
  std::string message;          // Optional message
};

// =============================================================================
// Health Change Callback Type
// =============================================================================

using HealthChangeCallback = std::function<void(const HealthChangeEvent& event)>;

// =============================================================================
// HealthChecker Implementation
// =============================================================================

// Forward declaration of implementation class (Pimpl idiom)
class HealthCheckerImpl;

class HealthChecker {
 public:
  // Health check function type - returns the current health status
  using HealthCheckFunc = std::function<HealthStatus()>;

  // Constructor and Destructor
  HealthChecker();
  ~HealthChecker();

  // Disable copy and move
  HealthChecker(const HealthChecker&) = delete;
  HealthChecker& operator=(const HealthChecker&) = delete;
  HealthChecker(HealthChecker&&) = delete;
  HealthChecker& operator=(HealthChecker&&) = delete;

  // ---------------------------------------------------------------------------
  // Component Registration
  // ---------------------------------------------------------------------------

  // Register a component with its health check function
  // Returns OK on success, InvalidArgument if name is empty or already exists
  Status RegisterComponent(const std::string& name, HealthCheckFunc check_func);

  // Register a component with health check function and optional message function
  // The message function should return a descriptive status message
  Status RegisterComponent(const std::string& name,
                          HealthCheckFunc check_func,
                          std::function<std::string()> message_func);

  // Unregister a component
  // Returns OK on success, NotFound if component doesn't exist
  Status UnregisterComponent(const std::string& name);

  // Check if a component is registered
  bool IsComponentRegistered(const std::string& name) const;

  // Get the number of registered components
  size_t GetComponentCount() const;

  // ---------------------------------------------------------------------------
  // Health Checks
  // ---------------------------------------------------------------------------

  // Check a single component's health immediately
  // Returns the component health if found, NotFound if component doesn't exist
  StatusOr<ComponentHealth> CheckComponent(const std::string& name);

  // Check all components' health immediately
  // Returns a vector of all component health statuses
  StatusOr<std::vector<ComponentHealth>> CheckAllComponents();

  // Get the last known health for a component (without running check)
  // Returns the component health if found, NotFound if component doesn't exist
  StatusOr<ComponentHealth> GetComponentHealth(const std::string& name) const;

  // Get all known component healths (without running checks)
  StatusOr<std::vector<ComponentHealth>> GetAllHealth() const;

  // ---------------------------------------------------------------------------
  // Overall Health Aggregation
  // ---------------------------------------------------------------------------

  // Get the overall health status (worst status of all components)
  // Returns kUnknown if no components are registered
  HealthStatus GetOverallHealth() const;

  // Get the overall health with a summary message
  // Returns a pair of (status, message)
  std::pair<HealthStatus, std::string> GetOverallHealthWithMessage() const;

  // Check if the system is healthy (overall status is kHealthy)
  bool IsHealthy() const;

  // Check if the system is ready (overall status is kHealthy or kDegraded)
  bool IsReady() const;

  // ---------------------------------------------------------------------------
  // Background Health Checks
  // ---------------------------------------------------------------------------

  // Start background health checks
  // interval_ms: how often to run health checks (in milliseconds)
  // Returns OK on success, InvalidArgument if interval_ms <= 0,
  //         Busy if already running
  Status Start(int interval_ms);

  // Stop background health checks
  void Stop();

  // Check if background health checks are running
  bool IsRunning() const;

  // Get the current check interval (0 if not running)
  int GetCheckIntervalMs() const;

  // Force an immediate health check (runs synchronously)
  void ForceCheck();

  // ---------------------------------------------------------------------------
  // Watch/Callback Mechanism
  // ---------------------------------------------------------------------------

  // Register a callback for health change events
  // Returns a watch ID that can be used to unregister
  StatusOr<int64_t> Watch(HealthChangeCallback callback);

  // Register a callback for a specific component's health changes
  // Returns a watch ID that can be used to unregister
  StatusOr<int64_t> WatchComponent(const std::string& name,
                                   HealthChangeCallback callback);

  // Unregister a watch callback by ID
  // Returns OK on success, NotFound if watch ID doesn't exist
  Status Unwatch(int64_t watch_id);

  // ---------------------------------------------------------------------------
  // HTTP Health Endpoint
  // ---------------------------------------------------------------------------

  // Start an HTTP endpoint for external health checks
  // This is useful for Kubernetes/Docker health probes
  // host: the host address to bind to (e.g., "0.0.0.0" or "localhost")
  // port: the port to listen on
  // Returns OK on success, IOError if cannot bind to port,
  //         Busy if already running, InvalidArgument if port is invalid
  Status StartHttpEndpoint(const std::string& host, int port);

  // Stop the HTTP endpoint
  void StopHttpEndpoint();

  // Check if the HTTP endpoint is running
  bool IsHttpEndpointRunning() const;

  // Get the HTTP endpoint address (empty string if not running)
  std::string GetHttpEndpointAddress() const;

  // ---------------------------------------------------------------------------
  // Utility Methods
  // ---------------------------------------------------------------------------

  // Get a JSON representation of the current health status
  // Useful for HTTP responses or logging
  std::string ToJson() const;

  // Get a human-readable summary of health status
  std::string ToString() const;

  // Clear all registered components and stop all background tasks
  void Clear();

 private:
  // Pimpl idiom - implementation details are hidden
  std::unique_ptr<HealthCheckerImpl> impl_;
};

}  // namespace governance
}  // namespace cedar

#endif  // CEDAR_GOVERNANCE_HEALTH_CHECKER_H_
