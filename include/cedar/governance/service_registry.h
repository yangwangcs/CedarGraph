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

#ifndef CEDAR_GOVERNANCE_SERVICE_REGISTRY_H_
#define CEDAR_GOVERNANCE_SERVICE_REGISTRY_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/core/status.h"

namespace cedar {
namespace governance {

// =============================================================================
// Service Status Enumeration
// =============================================================================

enum class ServiceStatus {
  kUnknown = 0,    // Unknown status
  kStarting = 1,   // Service is starting up
  kHealthy = 2,    // Service is healthy
  kUnhealthy = 3,  // Service is unhealthy
  kStopping = 4    // Service is shutting down
};

// Convert ServiceStatus to string
std::string ServiceStatusToString(ServiceStatus status);

// =============================================================================
// Service Information Structure
// =============================================================================

struct ServiceInfo {
  std::string id;                                            // Unique instance ID
  std::string name;                                          // Service name (storaged, graphd, metad)
  std::string host;                                          // Host address
  int port;                                                  // Port number
  ServiceStatus status;                                      // Current status
  int64_t register_time_ms;                                  // Registration timestamp (ms)
  int64_t last_heartbeat_ms;                                 // Last heartbeat timestamp (ms)
  std::unordered_map<std::string, std::string> metadata;     // Additional metadata

  ServiceInfo()
      : port(0),
        status(ServiceStatus::kUnknown),
        register_time_ms(0),
        last_heartbeat_ms(0) {}
};

// =============================================================================
// Service Event Types
// =============================================================================

enum class ServiceEventType {
  kRegistered = 0,     // Service was registered
  kDeregistered = 1,   // Service was deregistered
  kStatusChanged = 2   // Service status changed
};

// Convert ServiceEventType to string
std::string ServiceEventTypeToString(ServiceEventType type);

// =============================================================================
// Service Event Structure
// =============================================================================

struct ServiceEvent {
  ServiceEventType type;   // Event type
  ServiceInfo service;     // Service information snapshot at event time
};

// =============================================================================
// Watch Callback Type
// =============================================================================

using ServiceWatchCallback = std::function<void(const ServiceEvent& event)>;

// =============================================================================
// ServiceRegistry Implementation
// =============================================================================

// Forward declaration of implementation class (Pimpl idiom)
class ServiceRegistryImpl;

class ServiceRegistry {
 public:
  // Constructor and Destructor
  ServiceRegistry();
  ~ServiceRegistry();

  // Disable copy and move
  ServiceRegistry(const ServiceRegistry&) = delete;
  ServiceRegistry& operator=(const ServiceRegistry&) = delete;
  ServiceRegistry(ServiceRegistry&&) = delete;
  ServiceRegistry& operator=(ServiceRegistry&&) = delete;

  // ---------------------------------------------------------------------------
  // Service Registration
  // ---------------------------------------------------------------------------

  // Register a new service instance
  // Returns OK on success, error status on failure
  Status Register(const ServiceInfo& info);

  // Deregister a service instance by ID
  // Returns OK on success, NotFound if service doesn't exist
  Status Deregister(const std::string& service_id);

  // ---------------------------------------------------------------------------
  // Service Status Management
  // ---------------------------------------------------------------------------

  // Update the status of a registered service
  // Returns OK on success, NotFound if service doesn't exist
  Status UpdateStatus(const std::string& service_id, ServiceStatus status);

  // Record a heartbeat from a service instance
  // Returns OK on success, NotFound if service doesn't exist
  Status Heartbeat(const std::string& service_id);

  // ---------------------------------------------------------------------------
  // Service Discovery
  // ---------------------------------------------------------------------------

  // Discover all services with the given name
  // Returns a vector of matching services
  StatusOr<std::vector<ServiceInfo>> Discover(const std::string& service_name) const;

  // Get a single service by its unique ID
  // Returns the service info if found, NotFound if it doesn't exist
  StatusOr<ServiceInfo> GetService(const std::string& service_id) const;

  // Get all registered services
  StatusOr<std::vector<ServiceInfo>> GetAllServices() const;

  // Get all services with a specific status
  StatusOr<std::vector<ServiceInfo>> GetServicesByStatus(ServiceStatus status) const;

  // ---------------------------------------------------------------------------
  // Watch Mechanism
  // ---------------------------------------------------------------------------

  // Watch for changes to services with the given name
  // The callback will be invoked synchronously under lock when events occur
  // Returns a watch ID that can be used to unregister
  StatusOr<int64_t> Watch(const std::string& service_name, ServiceWatchCallback callback);

  // Unregister a watch callback by ID
  Status Unwatch(int64_t watch_id);

  // ---------------------------------------------------------------------------
  // Health Check Management
  // ---------------------------------------------------------------------------

  // Start the background health check thread
  // interval_ms: how often to check for stale services
  // stale_threshold_ms: how long without heartbeat before marking unhealthy
  Status StartHealthCheck(int interval_ms, int stale_threshold_ms = 30000);

  // Stop the background health check thread
  void StopHealthCheck();

  // Check if health check thread is running
  bool IsHealthCheckRunning() const;

  // ---------------------------------------------------------------------------
  // Statistics
  // ---------------------------------------------------------------------------

  // Get count of registered services
  size_t GetServiceCount() const;

  // Get count of services by name
  size_t GetServiceCountByName(const std::string& service_name) const;

  // Get count of healthy services
  size_t GetHealthyServiceCount() const;

 private:
  // Pimpl idiom - implementation details are hidden
  std::unique_ptr<ServiceRegistryImpl> impl_;
};

}  // namespace governance
}  // namespace cedar

#endif  // CEDAR_GOVERNANCE_SERVICE_REGISTRY_H_
