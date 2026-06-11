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

#include "cedar/governance/health_checker.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cassert>
#include <chrono>
#include <cstring>
#include <future>
#include <iomanip>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>

#include "cedar/core/threading.h"

namespace cedar {
namespace governance {

// =============================================================================
// Helper Functions
// =============================================================================

std::string HealthStatusToString(HealthStatus status) {
  switch (status) {
    case HealthStatus::kUnknown:
      return "Unknown";
    case HealthStatus::kStarting:
      return "Starting";
    case HealthStatus::kHealthy:
      return "Healthy";
    case HealthStatus::kDegraded:
      return "Degraded";
    case HealthStatus::kUnhealthy:
      return "Unhealthy";
    default:
      return "Invalid";
  }
}

int HealthStatusToHttpCode(HealthStatus status) {
  switch (status) {
    case HealthStatus::kUnknown:
      return 503;  // Service Unavailable
    case HealthStatus::kStarting:
      return 503;  // Service Unavailable
    case HealthStatus::kHealthy:
      return 200;  // OK
    case HealthStatus::kDegraded:
      return 200;  // OK (but with warning)
    case HealthStatus::kUnhealthy:
      return 503;  // Service Unavailable
    default:
      return 503;  // Service Unavailable
  }
}

HealthStatus WorseHealthStatus(HealthStatus a, HealthStatus b) {
  // Priority order (higher = worse):
  // kUnknown (0), kStarting (1), kHealthy (2), kDegraded (3), kUnhealthy (4)
  return (static_cast<int>(a) > static_cast<int>(b)) ? a : b;
}

std::string HealthChangeTypeToString(HealthChangeType type) {
  switch (type) {
    case HealthChangeType::kStatusChanged:
      return "StatusChanged";
    case HealthChangeType::kCheckPerformed:
      return "CheckPerformed";
    case HealthChangeType::kComponentAdded:
      return "ComponentAdded";
    case HealthChangeType::kComponentRemoved:
      return "ComponentRemoved";
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

// Basic HTTP request validation to prevent request smuggling
static bool IsValidHttpRequest(const std::string& request) {
  // Must start with a known method
  if (request.find("GET ") != 0 && request.find("HEAD ") != 0) {
    return false;
  }
  // Must have HTTP version indicator
  if (request.find("HTTP/") == std::string::npos) {
    return false;
  }
  // Must end with double CRLF (end of headers)
  if (request.find("\r\n\r\n") == std::string::npos) {
    return false;
  }
  // Reject requests with Transfer-Encoding (potential smuggling)
  if (request.find("Transfer-Encoding") != std::string::npos) {
    return false;
  }
  // Reject requests with multiple Content-Length headers
  size_t first_cl = request.find("Content-Length:");
  if (first_cl != std::string::npos) {
    size_t second_cl = request.find("Content-Length:", first_cl + 1);
    if (second_cl != std::string::npos) {
      return false;
    }
  }
  return true;
}

// =============================================================================
// HealthCheckerImpl - Private Implementation
// =============================================================================

class HealthCheckerImpl {
 public:
  HealthCheckerImpl()
      : next_watch_id_(1),
        background_running_(false),
        check_interval_ms_(0),
        http_running_(false),
        http_port_(0),
        http_socket_(-1),
        check_thread_pool_(std::make_unique<cedar::ThreadPool>(kMaxConcurrentHealthChecks)) {}

  ~HealthCheckerImpl() {
    Stop();
    StopHttpEndpoint();
  }

  // ---------------------------------------------------------------------------
  // Component Registration
  // ---------------------------------------------------------------------------

  Status RegisterComponent(const std::string& name, 
                          HealthChecker::HealthCheckFunc check_func,
                          std::function<std::string()> message_func) {
    if (name.empty()) {
      return Status::InvalidArgument("Component name cannot be empty");
    }
    if (!check_func) {
      return Status::InvalidArgument("Health check function cannot be null");
    }

    std::lock_guard<std::mutex> lock(components_mutex_);

    // Check if component already exists
    auto it = components_.find(name);
    if (it != components_.end()) {
      return Status::Conflict("Component already exists: " + name);
    }

    // Create component entry
    ComponentEntry entry;
    entry.name = name;
    entry.check_func = std::move(check_func);
    entry.message_func = std::move(message_func);
    entry.health.status = HealthStatus::kUnknown;
    entry.health.name = name;

    components_[name] = std::move(entry);

    // Notify watchers
    HealthChangeEvent event;
    event.type = HealthChangeType::kComponentAdded;
    event.component_name = name;
    event.old_status = HealthStatus::kUnknown;
    event.new_status = HealthStatus::kUnknown;
    event.timestamp_ms = CurrentTimeMillis();
    NotifyWatchers(name, event);

    return Status::OK();
  }

  Status UnregisterComponent(const std::string& name) {
    std::lock_guard<std::mutex> lock(components_mutex_);

    auto it = components_.find(name);
    if (it == components_.end()) {
      return Status::NotFound("Component not found: " + name);
    }

    HealthStatus old_status = it->second.health.status;
    components_.erase(it);

    // Notify watchers
    HealthChangeEvent event;
    event.type = HealthChangeType::kComponentRemoved;
    event.component_name = name;
    event.old_status = old_status;
    event.new_status = HealthStatus::kUnknown;
    event.timestamp_ms = CurrentTimeMillis();
    NotifyWatchers(name, event);

    return Status::OK();
  }

  bool IsComponentRegistered(const std::string& name) const {
    std::lock_guard<std::mutex> lock(components_mutex_);
    return components_.find(name) != components_.end();
  }

  size_t GetComponentCount() const {
    std::lock_guard<std::mutex> lock(components_mutex_);
    return components_.size();
  }

  // ---------------------------------------------------------------------------
  // Health Checks
  // ---------------------------------------------------------------------------

  StatusOr<ComponentHealth> CheckComponent(const std::string& name) {
    std::function<std::string()> message_func;
    HealthChecker::HealthCheckFunc check_func;

    // Get the check function while holding the lock
    {
      std::lock_guard<std::mutex> lock(components_mutex_);
      auto it = components_.find(name);
      if (it == components_.end()) {
        return Status::NotFound("Component not found: " + name);
      }
      check_func = it->second.check_func;
      message_func = it->second.message_func;
    }

    // Run the check outside the lock
    return RunCheck(name, check_func, message_func);
  }

  StatusOr<std::vector<ComponentHealth>> CheckAllComponents() {
    std::vector<std::pair<std::string, std::pair<HealthChecker::HealthCheckFunc, std::function<std::string()>>>> checks;

    // Collect all check functions while holding the lock
    {
      std::lock_guard<std::mutex> lock(components_mutex_);
      checks.reserve(components_.size());
      for (const auto& pair : components_) {
        checks.emplace_back(pair.first, 
                          std::make_pair(pair.second.check_func, pair.second.message_func));
      }
    }

    // Run all checks outside the lock
    std::vector<ComponentHealth> results;
    results.reserve(checks.size());
    for (const auto& check : checks) {
      ComponentHealth result = RunCheck(check.first, check.second.first, check.second.second);
      results.push_back(result);
    }

    return results;
  }

  StatusOr<ComponentHealth> GetComponentHealth(const std::string& name) const {
    std::lock_guard<std::mutex> lock(components_mutex_);

    auto it = components_.find(name);
    if (it == components_.end()) {
      return Status::NotFound("Component not found: " + name);
    }

    return it->second.health;
  }

  StatusOr<std::vector<ComponentHealth>> GetAllHealth() const {
    std::lock_guard<std::mutex> lock(components_mutex_);

    std::vector<ComponentHealth> results;
    results.reserve(components_.size());
    for (const auto& pair : components_) {
      results.push_back(pair.second.health);
    }

    return results;
  }

  // ---------------------------------------------------------------------------
  // Overall Health Aggregation
  // ---------------------------------------------------------------------------

  HealthStatus GetOverallHealth() const {
    std::lock_guard<std::mutex> lock(components_mutex_);

    if (components_.empty()) {
      return HealthStatus::kUnknown;
    }

    HealthStatus overall = HealthStatus::kUnknown;
    for (const auto& pair : components_) {
      overall = WorseHealthStatus(overall, pair.second.health.status);
    }

    return overall;
  }

  std::pair<HealthStatus, std::string> GetOverallHealthWithMessage() const {
    std::lock_guard<std::mutex> lock(components_mutex_);

    if (components_.empty()) {
      return {HealthStatus::kUnknown, "No components registered"};
    }

    HealthStatus overall = HealthStatus::kUnknown;
    size_t healthy_count = 0;
    size_t degraded_count = 0;
    size_t unhealthy_count = 0;
    size_t unknown_count = 0;

    for (const auto& pair : components_) {
      HealthStatus status = pair.second.health.status;
      overall = WorseHealthStatus(overall, status);

      switch (status) {
        case HealthStatus::kHealthy:
          ++healthy_count;
          break;
        case HealthStatus::kDegraded:
          ++degraded_count;
          break;
        case HealthStatus::kUnhealthy:
          ++unhealthy_count;
          break;
        default:
          ++unknown_count;
          break;
      }
    }

    std::ostringstream oss;
    oss << healthy_count << " healthy, "
        << degraded_count << " degraded, "
        << unhealthy_count << " unhealthy, "
        << unknown_count << " unknown";

    return {overall, oss.str()};
  }

  bool IsHealthy() const {
    return GetOverallHealth() == HealthStatus::kHealthy;
  }

  bool IsReady() const {
    HealthStatus status = GetOverallHealth();
    return status == HealthStatus::kHealthy || status == HealthStatus::kDegraded;
  }

  // ---------------------------------------------------------------------------
  // Background Health Checks
  // ---------------------------------------------------------------------------

  Status Start(int interval_ms) {
    if (interval_ms <= 0) {
      return Status::InvalidArgument("Interval must be positive");
    }

    std::lock_guard<std::mutex> lock(background_mutex_);

    if (background_running_) {
      return Status::Busy("Background health check already running");
    }

    background_running_ = true;
    check_interval_ms_ = interval_ms;

    background_thread_ = std::thread(&HealthCheckerImpl::BackgroundCheckLoop, this);

    return Status::OK();
  }

  void Stop() {
    {
      std::lock_guard<std::mutex> lock(background_mutex_);
      background_running_ = false;
    }

    if (background_thread_.joinable()) {
      background_thread_.join();
    }
  }

  bool IsRunning() const {
    std::lock_guard<std::mutex> lock(background_mutex_);
    return background_running_;
  }

  int GetCheckIntervalMs() const {
    std::lock_guard<std::mutex> lock(background_mutex_);
    return background_running_ ? check_interval_ms_ : 0;
  }

  void ForceCheck() {
    RunAllChecks();
  }

  // ---------------------------------------------------------------------------
  // Watch/Callback Mechanism
  // ---------------------------------------------------------------------------

  StatusOr<int64_t> Watch(HealthChangeCallback callback) {
    if (!callback) {
      return Status::InvalidArgument("Callback cannot be null");
    }

    std::lock_guard<std::mutex> lock(watchers_mutex_);

    int64_t watch_id = next_watch_id_++;
    WatchEntry entry;
    entry.id = watch_id;
    // component_name is empty, which means watch all components
    entry.callback = std::move(callback);

    watchers_[watch_id] = std::move(entry);

    return watch_id;
  }

  StatusOr<int64_t> WatchComponent(const std::string& name,
                                   HealthChangeCallback callback) {
    if (name.empty()) {
      return Status::InvalidArgument("Component name cannot be empty");
    }
    if (!callback) {
      return Status::InvalidArgument("Callback cannot be null");
    }

    std::lock_guard<std::mutex> lock(watchers_mutex_);

    int64_t watch_id = next_watch_id_++;
    WatchEntry entry;
    entry.id = watch_id;
    entry.component_name = name;
    entry.callback = std::move(callback);

    watchers_[watch_id] = std::move(entry);

    return watch_id;
  }

  Status Unwatch(int64_t watch_id) {
    std::lock_guard<std::mutex> lock(watchers_mutex_);

    auto it = watchers_.find(watch_id);
    if (it == watchers_.end()) {
      return Status::NotFound("Watch not found: " + std::to_string(watch_id));
    }

    watchers_.erase(it);
    return Status::OK();
  }

  // ---------------------------------------------------------------------------
  // HTTP Health Endpoint
  // ---------------------------------------------------------------------------

  Status StartHttpEndpoint(const std::string& host, int port) {
    if (host.empty()) {
      return Status::InvalidArgument("Host cannot be empty");
    }
    if (port <= 0 || port > 65535) {
      return Status::InvalidArgument("Invalid port number");
    }

    std::lock_guard<std::mutex> lock(http_mutex_);

    if (http_running_) {
      return Status::Busy("HTTP endpoint already running");
    }

    // Create socket
    http_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (http_socket_ < 0) {
      return Status::IOError("Failed to create socket: " + std::string(strerror(errno)));
    }

    // Allow socket reuse
    int opt = 1;
    if (setsockopt(http_socket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
      close(http_socket_);
      http_socket_ = -1;
      return Status::IOError("Failed to set socket options: " + std::string(strerror(errno)));
    }

    // Bind to address
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (host == "0.0.0.0") {
      addr.sin_addr.s_addr = INADDR_ANY;
    } else {
      if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(http_socket_);
        http_socket_ = -1;
        return Status::InvalidArgument("Invalid host address: " + host);
      }
    }

    if (bind(http_socket_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
      close(http_socket_);
      http_socket_ = -1;
      return Status::IOError("Failed to bind to " + host + ":" + std::to_string(port) +
                            ": " + std::string(strerror(errno)));
    }

    // Start listening
    if (listen(http_socket_, 5) < 0) {
      close(http_socket_);
      http_socket_ = -1;
      return Status::IOError("Failed to listen on socket: " + std::string(strerror(errno)));
    }

    http_running_ = true;
    http_host_ = host;
    http_port_ = port;

    // Initialize bounded thread pool for HTTP requests
    http_thread_pool_ = std::make_unique<cedar::ThreadPool>(kHttpThreadPoolSize);

    // Start HTTP server thread
    http_thread_ = std::thread(&HealthCheckerImpl::HttpServerLoop, this);

    return Status::OK();
  }

  void StopHttpEndpoint() {
    {
      std::lock_guard<std::mutex> lock(http_mutex_);
      http_running_ = false;
      
      // Close the socket to unblock accept()
      if (http_socket_ >= 0) {
        close(http_socket_);
        http_socket_ = -1;
      }
    }

    if (http_thread_.joinable()) {
      http_thread_.join();
    }

    // Shut down the thread pool and wait for in-flight requests
    if (http_thread_pool_) {
      http_thread_pool_.reset();
    }
  }

  bool IsHttpEndpointRunning() const {
    std::lock_guard<std::mutex> lock(http_mutex_);
    return http_running_;
  }

  std::string GetHttpEndpointAddress() const {
    std::lock_guard<std::mutex> lock(http_mutex_);
    if (!http_running_) {
      return "";
    }
    return http_host_ + ":" + std::to_string(http_port_);
  }

  // ---------------------------------------------------------------------------
  // Utility Methods
  // ---------------------------------------------------------------------------

  std::string ToJson() const {
    std::lock_guard<std::mutex> lock(components_mutex_);

    std::ostringstream oss;
    oss << "{";
    
    // Overall health
    HealthStatus overall = HealthStatus::kUnknown;
    for (const auto& pair : components_) {
      overall = WorseHealthStatus(overall, pair.second.health.status);
    }
    oss << "\"overall\":\"" << HealthStatusToString(overall) << "\",";
    oss << "\"components\":[";

    bool first = true;
    for (const auto& pair : components_) {
      if (!first) oss << ",";
      first = false;

      const ComponentHealth& health = pair.second.health;
      oss << "{";
      oss << "\"name\":\"" << health.name << "\",";
      oss << "\"status\":\"" << HealthStatusToString(health.status) << "\",";
      oss << "\"message\":\"" << EscapeJsonString(health.message) << "\",";
      oss << "\"last_check_time\":" << health.last_check_time << ",";
      oss << "\"check_duration_ms\":" << health.check_duration_ms;
      oss << "}";
    }

    oss << "]}";
    return oss.str();
  }

  std::string ToString() const {
    auto [overall, message] = GetOverallHealthWithMessage();
    
    std::ostringstream oss;
    oss << "Health Status: " << HealthStatusToString(overall) << "\n";
    oss << "Summary: " << message << "\n";
    oss << "Components:\n";

    auto health_result = GetAllHealth();
    if (health_result.ok()) {
      for (const auto& health : health_result.value()) {
        oss << "  - " << health.name << ": " 
            << HealthStatusToString(health.status);
        if (!health.message.empty()) {
          oss << " (" << health.message << ")";
        }
        oss << "\n";
      }
    }

    return oss.str();
  }

  void Clear() {
    Stop();
    StopHttpEndpoint();

    std::lock_guard<std::mutex> lock(components_mutex_);
    components_.clear();
  }

 private:
  // Component entry structure
  struct ComponentEntry {
    std::string name;
    HealthChecker::HealthCheckFunc check_func;
    std::function<std::string()> message_func;
    ComponentHealth health;
  };

  // Watch entry structure
  struct WatchEntry {
    int64_t id;
    std::string component_name;  // Empty means watch all
    HealthChangeCallback callback;
  };

  // Run a health check for a component
  ComponentHealth RunCheck(const std::string& name,
                          const HealthChecker::HealthCheckFunc& check_func,
                          const std::function<std::string()>& message_func) {
    ComponentHealth result;
    result.name = name;
    result.last_check_time = CurrentTimeMillis();

    auto start = std::chrono::steady_clock::now();
    
    try {
      result.status = check_func();
      if (message_func) {
        result.message = message_func();
      }
    } catch (...) {
      std::cerr << "[HealthChecker] Unknown exception during health check" << std::endl;
      result.status = HealthStatus::kUnhealthy;
      result.message = "Health check threw exception";
    }

    auto end = std::chrono::steady_clock::now();
    result.check_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   end - start).count();

    // Update stored health and notify watchers
    {
      std::unique_lock<std::mutex> lock(components_mutex_);
      auto it = components_.find(name);
      if (it != components_.end()) {
        HealthStatus old_status = it->second.health.status;
        it->second.health = result;

        // Notify if status changed
        if (old_status != result.status) {
          HealthChangeEvent event;
          event.type = HealthChangeType::kStatusChanged;
          event.component_name = name;
          event.old_status = old_status;
          event.new_status = result.status;
          event.timestamp_ms = result.last_check_time;
          event.message = result.message;

          // Release lock before notifying to avoid deadlock
          lock.unlock();
          NotifyWatchers(name, event);
        }
      }
    }

    return result;
  }

  // Run health checks for all components
  void RunAllChecks() {
    std::vector<std::pair<std::string, std::pair<HealthChecker::HealthCheckFunc, std::function<std::string()>>>> checks;

    {
      std::lock_guard<std::mutex> lock(components_mutex_);
      checks.reserve(components_.size());
      for (const auto& pair : components_) {
        checks.emplace_back(pair.first,
                          std::make_pair(pair.second.check_func, pair.second.message_func));
      }
    }

    // P1-3: Run checks on a bounded thread pool instead of std::async
    std::vector<std::future<void>> futures;
    futures.reserve(checks.size());
    std::mutex results_mutex;
    std::vector<ComponentHealth> results;
    results.reserve(checks.size());

    for (const auto& check : checks) {
      auto promise = std::make_shared<std::promise<void>>();
      futures.push_back(promise->get_future());

      check_thread_pool_->Schedule([
          this, &check, &results, &results_mutex, promise]() {
        try {
          ComponentHealth health = RunCheck(
              check.first, check.second.first, check.second.second);
          std::lock_guard<std::mutex> lock(results_mutex);
          results.push_back(std::move(health));
        } catch (const std::exception& e) {
          std::cerr << "[HealthChecker] Check task exception for " << check.first
                    << ": " << e.what() << std::endl;
        } catch (...) {
          std::cerr << "[HealthChecker] Unknown check task exception for "
                    << check.first << std::endl;
        }
        promise->set_value();
      });
    }

    for (auto& future : futures) {
      future.wait();
    }
  }

  // Background check loop
  void BackgroundCheckLoop() {
    while (true) {
      {
        std::lock_guard<std::mutex> lock(background_mutex_);
        if (!background_running_) {
          break;
        }
      }

      RunAllChecks();

      // Sleep for interval
      std::this_thread::sleep_for(std::chrono::milliseconds(check_interval_ms_));
    }
  }

  // HTTP server loop
  void HttpServerLoop() {
    while (true) {
      {
        std::lock_guard<std::mutex> lock(http_mutex_);
        if (!http_running_) {
          break;
        }
      }

      struct sockaddr_in client_addr;
      socklen_t client_len = sizeof(client_addr);
      int client_socket = accept(http_socket_,
                                 reinterpret_cast<struct sockaddr*>(&client_addr),
                                 &client_len);
      if (client_socket < 0) {
        std::lock_guard<std::mutex> lock(http_mutex_);
        if (!http_running_) break;
        continue;
      }

      int current = active_http_connections_.fetch_add(1);
      if (current >= kMaxHttpConnections) {
        active_http_connections_.fetch_sub(1);
        close(client_socket);
        continue;
      }

      struct timeval tv;
      tv.tv_sec = 5;
      tv.tv_usec = 0;
      if (setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0 ||
          setsockopt(client_socket, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0) {
        std::cerr << "[HealthChecker] Failed to set socket timeout" << std::endl;
      }

      try {
        http_thread_pool_->Schedule([this, client_socket]() {
          try {
            HandleHttpRequest(client_socket);
          } catch (const std::exception& e) {
            std::cerr << "[HealthChecker] HandleHttpRequest exception: " << e.what() << std::endl;
          } catch (...) {
            std::cerr << "[HealthChecker] HandleHttpRequest unknown exception" << std::endl;
          }
          close(client_socket);
          active_http_connections_.fetch_sub(1);
        });
      } catch (...) {
        active_http_connections_.fetch_sub(1);
        close(client_socket);
        std::cerr << "[HealthChecker] Failed to schedule HTTP handler" << std::endl;
      }
    }
  }

  // Handle HTTP requests on a connection
  void HandleHttpRequest(int client_socket) {
    std::string request_buffer;
    int request_count = 0;

    while (request_count < kMaxRequestsPerConnection) {
      // Read data into buffer
      char buffer[4096];
      ssize_t received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
      if (received <= 0) {
        break;
      }
      buffer[received] = '\0';
      request_buffer.append(buffer, received);

      // Check max request size
      if (request_buffer.size() > kMaxHttpRequestSize) {
        SendHttpResponse(client_socket, 413, "Payload Too Large",
                         "{\"error\":\"Request too large\"}", "application/json", false);
        break;
      }

      // Check if we have complete headers
      size_t headers_end = request_buffer.find("\r\n\r\n");
      if (headers_end == std::string::npos) {
        continue;
      }

      // Validate the request
      if (!IsValidHttpRequest(request_buffer)) {
        SendHttpResponse(client_socket, 400, "Bad Request",
                         "{\"error\":\"Invalid HTTP request\"}", "application/json", false);
        break;
      }

      // Extract request line
      size_t line_end = request_buffer.find("\r\n");
      std::string request_line = request_buffer.substr(0, line_end);

      // Check for keep-alive
      bool client_keep_alive = false;
      if (request_buffer.find("Connection: keep-alive") != std::string::npos ||
          request_buffer.find("Connection: Keep-Alive") != std::string::npos) {
        client_keep_alive = true;
      }

      // Route and generate response
      HealthStatus overall = GetOverallHealth();
      int http_code = HealthStatusToHttpCode(overall);
      std::string status_text = (http_code == 200) ? "OK" : "Service Unavailable";
      std::string response_body;
      std::string content_type = "application/json";

      if (request_line.find("GET /ready") == 0 ||
          request_line.find("GET /readyz") == 0 ||
          request_line.find("HEAD /ready") == 0 ||
          request_line.find("HEAD /readyz") == 0) {
        bool ready = IsReady();
        response_body = ready ? "{\"status\":\"ready\"}" : "{\"status\":\"not ready\"}";
        http_code = ready ? 200 : 503;
        status_text = ready ? "OK" : "Service Unavailable";
      } else if (request_line.find("GET /health") == 0 ||
                 request_line.find("GET /healthz") == 0 ||
                 request_line.find("HEAD /health") == 0 ||
                 request_line.find("HEAD /healthz") == 0 ||
                 request_line.find("GET / ") == 0 ||
                 request_line.find("HEAD / ") == 0) {
        response_body = ToJson();
      } else {
        http_code = 404;
        status_text = "Not Found";
        response_body = "{\"error\":\"Not Found\"}";
      }

      // For HEAD requests, body is not sent
      std::string actual_body = response_body;
      if (request_line.find("HEAD ") == 0) {
        actual_body.clear();
      }

      // Determine if we should keep the connection alive
      bool keep_alive = client_keep_alive &&
                        (request_count + 1 < kMaxRequestsPerConnection);

      SendHttpResponse(client_socket, http_code, status_text, actual_body,
                       content_type, keep_alive);

      request_count++;

      if (!keep_alive) {
        break;
      }

      // Remove processed request from buffer for potential pipelining
      request_buffer = request_buffer.substr(headers_end + 4);
    }
  }

  // Notify watchers of a health change event
  void NotifyWatchers(const std::string& component_name, const HealthChangeEvent& event) {
    std::vector<HealthChangeCallback> callbacks_to_invoke;
    
    {
      std::lock_guard<std::mutex> lock(watchers_mutex_);
      for (const auto& pair : watchers_) {
        const WatchEntry& entry = pair.second;
        // Invoke if watching all (empty name) or watching this specific component
        if (entry.component_name.empty() || entry.component_name == component_name) {
          callbacks_to_invoke.push_back(entry.callback);
        }
      }
    }

    // Invoke callbacks outside the lock to avoid deadlock
    for (auto& callback : callbacks_to_invoke) {
      callback(event);
    }
  }

  // Escape a string for JSON
  std::string EscapeJsonString(const std::string& input) const {
    std::ostringstream oss;
    for (char c : input) {
      switch (c) {
        case '"': oss << "\\\""; break;
        case '\\': oss << "\\\\"; break;
        case '\b': oss << "\\b"; break;
        case '\f': oss << "\\f"; break;
        case '\n': oss << "\\n"; break;
        case '\r': oss << "\\r"; break;
        case '\t': oss << "\\t"; break;
        default:
          if (c >= 0x20 && c <= 0x7E) {
            oss << c;
          } else {
            // Save and restore stream flags to prevent hex state pollution
            auto old_flags = oss.flags();
            oss << "\\u" << std::hex << std::setw(4) << std::setfill('0') 
                << (static_cast<unsigned int>(c) & 0xFF);
            oss.flags(old_flags);
          }
      }
    }
    return oss.str();
  }

  // Component storage
  mutable std::mutex components_mutex_;
  std::unordered_map<std::string, ComponentEntry> components_;

  // Watch storage
  mutable std::mutex watchers_mutex_;
  std::unordered_map<int64_t, WatchEntry> watchers_;
  int64_t next_watch_id_;

  // Background check state
  mutable std::mutex background_mutex_;
  std::atomic<bool> background_running_;
  int check_interval_ms_;
  std::thread background_thread_;

  // HTTP endpoint state
  mutable std::mutex http_mutex_;
  std::atomic<bool> http_running_;
  std::string http_host_;
  int http_port_;
  int http_socket_;
  std::thread http_thread_;

  // Health check thread pool and concurrency limiting
  std::unique_ptr<cedar::ThreadPool> check_thread_pool_;
  static constexpr int kMaxConcurrentHealthChecks = 4;

  // HTTP thread pool and connection limiting
  std::unique_ptr<cedar::ThreadPool> http_thread_pool_;
  std::atomic<int> active_http_connections_{0};
  static constexpr int kMaxHttpConnections = 100;
  static constexpr int kHttpThreadPoolSize = 4;
  static constexpr size_t kMaxHttpRequestSize = 65536;
  static constexpr int kMaxRequestsPerConnection = 10;

  // Send an HTTP response
  void SendHttpResponse(int client_socket, int code, const std::string& status_text,
                        const std::string& body, const std::string& content_type,
                        bool keep_alive) {
    std::ostringstream response;
    response << "HTTP/1.1 " << code << " " << status_text << "\r\n";
    response << "Content-Type: " << content_type << "\r\n";
    response << "Content-Length: " << body.length() << "\r\n";
    if (keep_alive) {
      response << "Connection: keep-alive\r\n";
    } else {
      response << "Connection: close\r\n";
    }
    response << "\r\n";
    response << body;

    std::string response_str = response.str();
    send(client_socket, response_str.c_str(), response_str.length(), 0);
  }
};

// =============================================================================
// HealthChecker Public API
// =============================================================================

HealthChecker::HealthChecker() : impl_(std::make_unique<HealthCheckerImpl>()) {}

HealthChecker::~HealthChecker() = default;

Status HealthChecker::RegisterComponent(const std::string& name, HealthCheckFunc check_func) {
  return impl_->RegisterComponent(name, std::move(check_func), nullptr);
}

Status HealthChecker::RegisterComponent(const std::string& name,
                                       HealthCheckFunc check_func,
                                       std::function<std::string()> message_func) {
  return impl_->RegisterComponent(name, std::move(check_func), std::move(message_func));
}

Status HealthChecker::UnregisterComponent(const std::string& name) {
  return impl_->UnregisterComponent(name);
}

bool HealthChecker::IsComponentRegistered(const std::string& name) const {
  return impl_->IsComponentRegistered(name);
}

size_t HealthChecker::GetComponentCount() const {
  return impl_->GetComponentCount();
}

StatusOr<ComponentHealth> HealthChecker::CheckComponent(const std::string& name) {
  return impl_->CheckComponent(name);
}

StatusOr<std::vector<ComponentHealth>> HealthChecker::CheckAllComponents() {
  return impl_->CheckAllComponents();
}

StatusOr<ComponentHealth> HealthChecker::GetComponentHealth(const std::string& name) const {
  return impl_->GetComponentHealth(name);
}

StatusOr<std::vector<ComponentHealth>> HealthChecker::GetAllHealth() const {
  return impl_->GetAllHealth();
}

HealthStatus HealthChecker::GetOverallHealth() const {
  return impl_->GetOverallHealth();
}

std::pair<HealthStatus, std::string> HealthChecker::GetOverallHealthWithMessage() const {
  return impl_->GetOverallHealthWithMessage();
}

bool HealthChecker::IsHealthy() const {
  return impl_->IsHealthy();
}

bool HealthChecker::IsReady() const {
  return impl_->IsReady();
}

Status HealthChecker::Start(int interval_ms) {
  return impl_->Start(interval_ms);
}

void HealthChecker::Stop() {
  impl_->Stop();
}

bool HealthChecker::IsRunning() const {
  return impl_->IsRunning();
}

int HealthChecker::GetCheckIntervalMs() const {
  return impl_->GetCheckIntervalMs();
}

void HealthChecker::ForceCheck() {
  impl_->ForceCheck();
}

StatusOr<int64_t> HealthChecker::Watch(HealthChangeCallback callback) {
  return impl_->Watch(std::move(callback));
}

StatusOr<int64_t> HealthChecker::WatchComponent(const std::string& name,
                                               HealthChangeCallback callback) {
  return impl_->WatchComponent(name, std::move(callback));
}

Status HealthChecker::Unwatch(int64_t watch_id) {
  return impl_->Unwatch(watch_id);
}

Status HealthChecker::StartHttpEndpoint(const std::string& host, int port) {
  return impl_->StartHttpEndpoint(host, port);
}

void HealthChecker::StopHttpEndpoint() {
  impl_->StopHttpEndpoint();
}

bool HealthChecker::IsHttpEndpointRunning() const {
  return impl_->IsHttpEndpointRunning();
}

std::string HealthChecker::GetHttpEndpointAddress() const {
  return impl_->GetHttpEndpointAddress();
}

std::string HealthChecker::ToJson() const {
  return impl_->ToJson();
}

std::string HealthChecker::ToString() const {
  return impl_->ToString();
}

void HealthChecker::Clear() {
  impl_->Clear();
}

}  // namespace governance
}  // namespace cedar
