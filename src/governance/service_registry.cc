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

#include "cedar/governance/service_registry.h"

#include <algorithm>
#include <cassert>
#include <chrono>

namespace cedar {
namespace governance {

// =============================================================================
// Helper Functions
// =============================================================================

std::string ServiceStatusToString(ServiceStatus status) {
  switch (status) {
    case ServiceStatus::kUnknown:
      return "Unknown";
    case ServiceStatus::kStarting:
      return "Starting";
    case ServiceStatus::kHealthy:
      return "Healthy";
    case ServiceStatus::kUnhealthy:
      return "Unhealthy";
    case ServiceStatus::kStopping:
      return "Stopping";
    default:
      return "Invalid";
  }
}

std::string ServiceEventTypeToString(ServiceEventType type) {
  switch (type) {
    case ServiceEventType::kRegistered:
      return "Registered";
    case ServiceEventType::kDeregistered:
      return "Deregistered";
    case ServiceEventType::kStatusChanged:
      return "StatusChanged";
    default:
      return "Invalid";
  }
}

// Get current timestamp in milliseconds
static inline int64_t CurrentTimeMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// Get steady timestamp in milliseconds (monotonic, unaffected by NTP)
static int64_t SteadyTimeMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

// =============================================================================
// ServiceRegistryImpl - Private Implementation
// =============================================================================

class ServiceRegistryImpl {
 public:
  ServiceRegistryImpl()
      : next_watch_id_(1),
        health_check_running_(false),
        health_check_interval_ms_(5000),
        stale_threshold_ms_(30000) {}

  ~ServiceRegistryImpl() { StopHealthCheck(); }

  // Register a new service
  Status Register(const ServiceInfo& info) {
    if (info.id.empty()) {
      return Status::InvalidArgument("Service ID cannot be empty");
    }
    if (info.name.empty()) {
      return Status::InvalidArgument("Service name cannot be empty");
    }
    if (info.host.empty()) {
      return Status::InvalidArgument("Service host cannot be empty");
    }
    if (info.port <= 0 || info.port > 65535) {
      return Status::InvalidArgument("Invalid port number");
    }

    ServiceEvent event;
    {
      std::lock_guard<std::mutex> lock(mutex_);

      // Check if service already exists
      auto it = services_.find(info.id);
      if (it != services_.end()) {
        return Status::Conflict("Service with ID already exists: " + info.id);
      }

      // Create service info with timestamps
      ServiceInfo service_info = info;
      service_info.register_time_ms = CurrentTimeMillis();
      service_info.last_heartbeat_ms = service_info.register_time_ms;

      // Set initial status if unknown
      if (service_info.status == ServiceStatus::kUnknown) {
        service_info.status = ServiceStatus::kStarting;
      }

      // Store service
      services_[service_info.id] = service_info;
      services_by_name_[service_info.name].insert(service_info.id);

      event = ServiceEvent{ServiceEventType::kRegistered, service_info};
    }

    NotifyWatchers(event.service.name, event);
    return Status::OK();
  }

  // Deregister a service
  Status Deregister(const std::string& service_id) {
    std::string service_name;
    ServiceEvent event;
    {
      std::lock_guard<std::mutex> lock(mutex_);

      auto it = services_.find(service_id);
      if (it == services_.end()) {
        return Status::NotFound("Service not found: " + service_id);
      }

      ServiceInfo service_info = it->second;
      service_name = service_info.name;

      // Remove from name index
      auto name_it = services_by_name_.find(service_name);
      if (name_it != services_by_name_.end()) {
        name_it->second.erase(service_id);
        if (name_it->second.empty()) {
          services_by_name_.erase(name_it);
        }
      }

      // Remove from main map
      services_.erase(it);

      event = ServiceEvent{ServiceEventType::kDeregistered, service_info};
    }

    NotifyWatchers(service_name, event);
    return Status::OK();
  }

  // Update service status
  Status UpdateStatus(const std::string& service_id, ServiceStatus new_status) {
    ServiceEvent event;
    bool should_notify = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);

      auto it = services_.find(service_id);
      if (it == services_.end()) {
        return Status::NotFound("Service not found: " + service_id);
      }

      ServiceStatus old_status = it->second.status;
      if (old_status != new_status) {
        it->second.status = new_status;
        event = ServiceEvent{ServiceEventType::kStatusChanged, it->second};
        should_notify = true;
      }
    }

    if (should_notify) {
      NotifyWatchers(event.service.name, event);
    }
    return Status::OK();
  }

  // Record heartbeat
  Status Heartbeat(const std::string& service_id) {
    ServiceEvent event;
    bool should_notify = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);

      auto it = services_.find(service_id);
      if (it == services_.end()) {
        return Status::NotFound("Service not found: " + service_id);
      }

      it->second.last_heartbeat_ms = SteadyTimeMillis();

      // Auto-transition from Starting to Healthy on first heartbeat
      if (it->second.status == ServiceStatus::kStarting) {
        it->second.status = ServiceStatus::kHealthy;
        event = ServiceEvent{ServiceEventType::kStatusChanged, it->second};
        should_notify = true;
      }
    }

    if (should_notify) {
      NotifyWatchers(event.service.name, event);
    }
    return Status::OK();
  }

  // Discover services by name
  StatusOr<std::vector<ServiceInfo>> Discover(const std::string& service_name) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<ServiceInfo> result;
    auto it = services_by_name_.find(service_name);
    if (it != services_by_name_.end()) {
      result.reserve(it->second.size());
      for (const auto& service_id : it->second) {
        auto service_it = services_.find(service_id);
        if (service_it != services_.end()) {
          result.push_back(service_it->second);
        }
      }
    }

    return result;
  }

  // Get single service by ID
  StatusOr<ServiceInfo> GetService(const std::string& service_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = services_.find(service_id);
    if (it == services_.end()) {
      return Status::NotFound("Service not found: " + service_id);
    }

    return it->second;
  }

  // Get all services
  StatusOr<std::vector<ServiceInfo>> GetAllServices() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<ServiceInfo> result;
    result.reserve(services_.size());
    for (const auto& pair : services_) {
      result.push_back(pair.second);
    }

    return result;
  }

  // Get services by status
  StatusOr<std::vector<ServiceInfo>> GetServicesByStatus(ServiceStatus status) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<ServiceInfo> result;
    for (const auto& pair : services_) {
      if (pair.second.status == status) {
        result.push_back(pair.second);
      }
    }

    return result;
  }

  // Watch for service changes
  StatusOr<int64_t> Watch(const std::string& service_name, ServiceWatchCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);

    int64_t watch_id = next_watch_id_++;
    WatchEntry entry{watch_id, service_name, std::move(callback)};
    
    watchers_[watch_id] = std::move(entry);
    watches_by_name_[service_name].insert(watch_id);

    return watch_id;
  }

  // Unregister a watch
  Status Unwatch(int64_t watch_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = watchers_.find(watch_id);
    if (it == watchers_.end()) {
      return Status::NotFound("Watch not found: " + std::to_string(watch_id));
    }

    // Remove from name index
    const std::string& service_name = it->second.service_name;
    auto name_it = watches_by_name_.find(service_name);
    if (name_it != watches_by_name_.end()) {
      name_it->second.erase(watch_id);
      if (name_it->second.empty()) {
        watches_by_name_.erase(name_it);
      }
    }

    watchers_.erase(it);
    return Status::OK();
  }

  // Start health check thread
  Status StartHealthCheck(int interval_ms, int stale_threshold_ms) {
    if (interval_ms <= 0) {
      return Status::InvalidArgument("Interval must be positive");
    }
    if (stale_threshold_ms <= 0) {
      return Status::InvalidArgument("Stale threshold must be positive");
    }

    std::lock_guard<std::mutex> lock(health_check_mutex_);

    if (health_check_running_) {
      return Status::Busy("Health check already running");
    }

    health_check_running_ = true;
    health_check_interval_ms_ = interval_ms;
    stale_threshold_ms_ = stale_threshold_ms;

    health_check_thread_ = std::thread(&ServiceRegistryImpl::HealthCheckLoop, this);

    return Status::OK();
  }

  // Stop health check thread
  void StopHealthCheck() {
    {
      std::lock_guard<std::mutex> lock(health_check_mutex_);
      health_check_running_ = false;
    }

    if (health_check_thread_.joinable()) {
      health_check_thread_.join();
    }
  }

  // Check if health check is running
  bool IsHealthCheckRunning() const {
    std::lock_guard<std::mutex> lock(health_check_mutex_);
    return health_check_running_;
  }

  // Get total service count
  size_t GetServiceCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return services_.size();
  }

  // Get service count by name
  size_t GetServiceCountByName(const std::string& service_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = services_by_name_.find(service_name);
    return (it != services_by_name_.end()) ? it->second.size() : 0;
  }

  // Get healthy service count
  size_t GetHealthyServiceCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (const auto& pair : services_) {
      if (pair.second.status == ServiceStatus::kHealthy) {
        ++count;
      }
    }
    return count;
  }

 private:
  // Watch entry structure
  struct WatchEntry {
    int64_t id;
    std::string service_name;
    ServiceWatchCallback callback;
  };

  // Notify watchers for a service name
  void NotifyWatchers(const std::string& service_name, const ServiceEvent& event) {
    std::vector<ServiceWatchCallback> callbacks_to_invoke;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = watches_by_name_.find(service_name);
      if (it != watches_by_name_.end()) {
        for (int64_t watch_id : it->second) {
          auto watcher_it = watchers_.find(watch_id);
          if (watcher_it != watchers_.end()) {
            callbacks_to_invoke.push_back(watcher_it->second.callback);
          }
        }
      }
    }  // Lock released

    // Invoke callbacks outside the lock to avoid deadlock
    for (auto& callback : callbacks_to_invoke) {
      callback(event);
    }
  }

  // Health check loop
  void HealthCheckLoop() {
    while (true) {
      {
        std::lock_guard<std::mutex> lock(health_check_mutex_);
        if (!health_check_running_) {
          break;
        }
      }

      CheckForStaleServices();

      // Sleep for interval
      std::this_thread::sleep_for(std::chrono::milliseconds(health_check_interval_ms_));
    }
  }

  // Check for and mark stale services as unhealthy
  void CheckForStaleServices() {
    int64_t stale_threshold;
    {
      std::lock_guard<std::mutex> lock(health_check_mutex_);
      stale_threshold = stale_threshold_ms_;
    }

    std::vector<ServiceEvent> events_to_notify;
    {
      std::lock_guard<std::mutex> lock(mutex_);

      int64_t now = SteadyTimeMillis();

      for (auto& pair : services_) {
        ServiceInfo& info = pair.second;
        
        // Only check healthy or starting services
        if (info.status == ServiceStatus::kHealthy || info.status == ServiceStatus::kStarting) {
          int64_t time_since_heartbeat = now - info.last_heartbeat_ms;
          if (time_since_heartbeat > stale_threshold) {
            info.status = ServiceStatus::kUnhealthy;
            events_to_notify.emplace_back(ServiceEventType::kStatusChanged, info);
          }
        }
      }
    }

    for (auto& event : events_to_notify) {
      NotifyWatchers(event.service.name, event);
    }
  }

  // Main mutex protecting all data structures
  mutable std::mutex mutex_;

  // Service storage
  std::unordered_map<std::string, ServiceInfo> services_;
  std::unordered_map<std::string, std::unordered_set<std::string>> services_by_name_;

  // Watch storage
  std::unordered_map<int64_t, WatchEntry> watchers_;
  std::unordered_map<std::string, std::unordered_set<int64_t>> watches_by_name_;
  int64_t next_watch_id_;

  // Health check state
  mutable std::mutex health_check_mutex_;
  std::atomic<bool> health_check_running_;
  int health_check_interval_ms_;
  int stale_threshold_ms_;
  std::thread health_check_thread_;
};

// =============================================================================
// ServiceRegistry Public API
// =============================================================================

ServiceRegistry::ServiceRegistry() : impl_(std::make_unique<ServiceRegistryImpl>()) {}

ServiceRegistry::~ServiceRegistry() = default;

Status ServiceRegistry::Register(const ServiceInfo& info) {
  return impl_->Register(info);
}

Status ServiceRegistry::Deregister(const std::string& service_id) {
  return impl_->Deregister(service_id);
}

Status ServiceRegistry::UpdateStatus(const std::string& service_id, ServiceStatus status) {
  return impl_->UpdateStatus(service_id, status);
}

Status ServiceRegistry::Heartbeat(const std::string& service_id) {
  return impl_->Heartbeat(service_id);
}

StatusOr<std::vector<ServiceInfo>> ServiceRegistry::Discover(const std::string& service_name) const {
  return impl_->Discover(service_name);
}

StatusOr<ServiceInfo> ServiceRegistry::GetService(const std::string& service_id) const {
  return impl_->GetService(service_id);
}

StatusOr<std::vector<ServiceInfo>> ServiceRegistry::GetAllServices() const {
  return impl_->GetAllServices();
}

StatusOr<std::vector<ServiceInfo>> ServiceRegistry::GetServicesByStatus(ServiceStatus status) const {
  return impl_->GetServicesByStatus(status);
}

StatusOr<int64_t> ServiceRegistry::Watch(const std::string& service_name, ServiceWatchCallback callback) {
  return impl_->Watch(service_name, std::move(callback));
}

Status ServiceRegistry::Unwatch(int64_t watch_id) {
  return impl_->Unwatch(watch_id);
}

Status ServiceRegistry::StartHealthCheck(int interval_ms, int stale_threshold_ms) {
  return impl_->StartHealthCheck(interval_ms, stale_threshold_ms);
}

void ServiceRegistry::StopHealthCheck() {
  impl_->StopHealthCheck();
}

bool ServiceRegistry::IsHealthCheckRunning() const {
  return impl_->IsHealthCheckRunning();
}

size_t ServiceRegistry::GetServiceCount() const {
  return impl_->GetServiceCount();
}

size_t ServiceRegistry::GetServiceCountByName(const std::string& service_name) const {
  return impl_->GetServiceCountByName(service_name);
}

size_t ServiceRegistry::GetHealthyServiceCount() const {
  return impl_->GetHealthyServiceCount();
}

}  // namespace governance
}  // namespace cedar
