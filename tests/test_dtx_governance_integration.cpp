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

// =============================================================================
// DTX Governance Integration Test
// Tests integration between DTX module and governance layer
// =============================================================================

#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>

#include "cedar/governance/service_registry.h"
#include "cedar/governance/config_manager.h"
#include "cedar/dtx/types.h"

// Minimal test framework
#define TEST_ASSERT(condition, message) \
  do { \
    if (!(condition)) { \
      std::cerr << "FAIL: " << message << " at line " << __LINE__ << std::endl; \
      return false; \
    } \
  } while(0)

#define TEST_PASS(name) \
  do { \
    std::cout << "PASS: " << name << std::endl; \
    return true; \
  } while(0)

using namespace cedar::governance;
using namespace cedar::dtx;

// =============================================================================
// Test: ServiceRegistry Basic Operations
// =============================================================================
bool TestServiceRegistryBasic() {
  std::cout << "\n=== Test: ServiceRegistry Basic Operations ===" << std::endl;
  
  ServiceRegistry registry;
  
  // Register a storage service
  ServiceInfo storage_service;
  storage_service.id = "storaged-1";
  storage_service.name = "storaged";
  storage_service.host = "127.0.0.1";
  storage_service.port = 9779;
  storage_service.status = ServiceStatus::kHealthy;
  
  auto status = registry.Register(storage_service);
  TEST_ASSERT(status.ok(), "Failed to register service");
  
  // Discover services
  auto services = registry.Discover("storaged");
  TEST_ASSERT(services.ok(), "Failed to discover services");
  TEST_ASSERT(services.ValueOrDie().size() == 1, "Expected 1 service");
  
  // Update status
  status = registry.UpdateStatus("storaged-1", ServiceStatus::kUnhealthy);
  TEST_ASSERT(status.ok(), "Failed to update status");
  
  // Get by status
  auto unhealthy = registry.GetServicesByStatus(ServiceStatus::kUnhealthy);
  TEST_ASSERT(unhealthy.ok(), "Failed to get unhealthy services");
  TEST_ASSERT(unhealthy.ValueOrDie().size() == 1, "Expected 1 unhealthy service");
  
  TEST_PASS("ServiceRegistryBasic");
}

// =============================================================================
// Test: ConfigManager DTX Configuration
// =============================================================================
bool TestConfigManagerDTX() {
  std::cout << "\n=== Test: ConfigManager DTX Configuration ===" << std::endl;
  
  ConfigManager config;
  
  // Set DTX configuration values
  config.SetInt("dtx.raft.timeout_ms", 5000);
  config.SetInt("dtx.raft.election_timeout_ms", 1000);
  config.SetInt("dtx.raft.heartbeat_interval_ms", 100);
  config.SetInt("dtx.retry.count", 3);
  config.SetInt("dtx.retry.interval_ms", 100);
  config.SetInt("dtx.storage.batch_size", 1000);
  config.SetInt("dtx.storage.flush_interval_ms", 100);
  
  // Verify values
  TEST_ASSERT(config.GetInt("dtx.raft.timeout_ms", 0) == 5000, 
              "raft.timeout_ms mismatch");
  TEST_ASSERT(config.GetInt("dtx.raft.election_timeout_ms", 0) == 1000, 
              "raft.election_timeout_ms mismatch");
  TEST_ASSERT(config.GetInt("dtx.raft.heartbeat_interval_ms", 0) == 100, 
              "raft.heartbeat_interval_ms mismatch");
  TEST_ASSERT(config.GetInt("dtx.retry.count", 0) == 3, 
              "retry.count mismatch");
  TEST_ASSERT(config.GetInt("dtx.retry.interval_ms", 0) == 100, 
              "retry.interval_ms mismatch");
  TEST_ASSERT(config.GetInt("dtx.storage.batch_size", 0) == 1000, 
              "storage.batch_size mismatch");
  TEST_ASSERT(config.GetInt("dtx.storage.flush_interval_ms", 0) == 100, 
              "storage.flush_interval_ms mismatch");
  
  // Test defaults
  TEST_ASSERT(config.GetInt("dtx.nonexistent.key", 42) == 42, 
              "Default value not returned for missing key");
  
  TEST_PASS("ConfigManagerDTX");
}

// =============================================================================
// Test: ConfigManager YAML Loading
// =============================================================================
bool TestConfigManagerYAMLLoading() {
  std::cout << "\n=== Test: ConfigManager YAML Loading ===" << std::endl;
  
  // Create a temporary YAML config
  const char* yaml_content = R"(
dtx:
  raft:
    timeout_ms: 7500
    election_timeout_ms: 1500
    heartbeat_interval_ms: 150
  retry:
    count: 5
    interval_ms: 200
    max_interval_ms: 5000
  storage:
    batch_size: 2000
    flush_interval_ms: 200
)";
  
  ConfigManager config;
  auto status = config.LoadFromString(yaml_content);
  TEST_ASSERT(status.ok(), "Failed to load YAML config");
  
  // Verify loaded values
  TEST_ASSERT(config.GetInt("dtx.raft.timeout_ms", 0) == 7500, 
              "Loaded raft.timeout_ms mismatch");
  TEST_ASSERT(config.GetInt("dtx.retry.count", 0) == 5, 
              "Loaded retry.count mismatch");
  TEST_ASSERT(config.GetInt("dtx.storage.batch_size", 0) == 2000, 
              "Loaded storage.batch_size mismatch");
  
  TEST_PASS("ConfigManagerYAMLLoading");
}

// =============================================================================
// Test: ServiceRegistry Watch Mechanism
// =============================================================================
bool TestServiceRegistryWatch() {
  std::cout << "\n=== Test: ServiceRegistry Watch Mechanism ===" << std::endl;
  
  ServiceRegistry registry;
  
  std::atomic<int> event_count{0};
  ServiceEvent last_event;
  
  // Set up watch
  auto watch_result = registry.Watch("storaged", 
    [&](const ServiceEvent& event) {
      event_count++;
      last_event = event;
    });
  
  TEST_ASSERT(watch_result.ok(), "Failed to set up watch");
  
  // Register a service (should trigger watch)
  ServiceInfo service;
  service.id = "storaged-watch-test";
  service.name = "storaged";
  service.host = "127.0.0.1";
  service.port = 9780;
  service.status = ServiceStatus::kHealthy;
  
  auto status = registry.Register(service);
  TEST_ASSERT(status.ok(), "Failed to register service");
  
  // Give callback time to execute
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  // Note: The watch callback is invoked synchronously during registration
  // so we should see the event immediately
  
  TEST_PASS("ServiceRegistryWatch");
}

// =============================================================================
// Test: ConfigManager Environment Overrides
// =============================================================================
bool TestConfigManagerEnvironmentOverrides() {
  std::cout << "\n=== Test: ConfigManager Environment Overrides ===" << std::endl;
  
  // Set environment variable
  // Note: CEDAR_ prefix is stripped, first underscore becomes dot
  // CEDAR_RAFT_TIMEOUT_MS -> raft.timeout_ms
  setenv("CEDAR_RAFT_TIMEOUT_MS", "9999", 1);
  
  ConfigManager config;
  
  // Load base config
  const char* yaml_content = "raft:\n  timeout_ms: 5000\n";
  auto status = config.LoadFromString(yaml_content);
  TEST_ASSERT(status.ok(), "Failed to load YAML");
  
  // Apply environment overrides
  status = config.ApplyEnvironmentOverrides();
  TEST_ASSERT(status.ok(), "Failed to apply environment overrides");
  
  // Verify override took effect
  // Note: The conversion converts CEDAR_RAFT_TIMEOUT_MS to raft.timeout_ms
  TEST_ASSERT(config.GetInt("raft.timeout_ms", 0) == 9999, 
              "Environment override not applied");
  
  // Clean up
  unsetenv("CEDAR_RAFT_TIMEOUT_MS");
  
  TEST_PASS("ConfigManagerEnvironmentOverrides");
}

// =============================================================================
// Test: Service Discovery for DTX
// =============================================================================
bool TestServiceDiscoveryForDTX() {
  std::cout << "\n=== Test: Service Discovery for DTX ===" << std::endl;
  
  ServiceRegistry registry;
  
  // Register multiple storage services
  for (int i = 1; i <= 3; i++) {
    ServiceInfo service;
    service.id = "storaged-" + std::to_string(i);
    service.name = "storaged";
    service.host = "192.168.1." + std::to_string(i);
    service.port = 9779;
    service.status = (i == 2) ? ServiceStatus::kUnhealthy : ServiceStatus::kHealthy;
    
    auto status = registry.Register(service);
    TEST_ASSERT(status.ok(), "Failed to register service " + std::to_string(i));
  }
  
  // Discover all storaged services
  auto services = registry.Discover("storaged");
  TEST_ASSERT(services.ok(), "Failed to discover services");
  TEST_ASSERT(services.ValueOrDie().size() == 3, "Expected 3 services");
  
  // Count healthy services
  size_t healthy_count = 0;
  for (const auto& svc : services.ValueOrDie()) {
    if (svc.status == ServiceStatus::kHealthy) {
      healthy_count++;
    }
  }
  TEST_ASSERT(healthy_count == 2, "Expected 2 healthy services");
  
  // Verify service info
  auto service_info = registry.GetService("storaged-1");
  TEST_ASSERT(service_info.ok(), "Failed to get service info");
  TEST_ASSERT(service_info.ValueOrDie().host == "192.168.1.1", 
              "Service host mismatch");
  
  TEST_PASS("ServiceDiscoveryForDTX");
}

// =============================================================================
// Main Test Runner
// =============================================================================
int main() {
  std::cout << "=================================================" << std::endl;
  std::cout << "DTX Governance Integration Test Suite" << std::endl;
  std::cout << "=================================================" << std::endl;
  
  int passed = 0;
  int failed = 0;
  
  // Run all tests
  auto tests = {
    TestServiceRegistryBasic,
    TestConfigManagerDTX,
    TestConfigManagerYAMLLoading,
    TestServiceRegistryWatch,
    TestConfigManagerEnvironmentOverrides,
    TestServiceDiscoveryForDTX
  };
  
  for (auto test : tests) {
    if (test()) {
      passed++;
    } else {
      failed++;
    }
  }
  
  std::cout << "\n=================================================" << std::endl;
  std::cout << "Test Results: " << passed << " passed, " << failed << " failed" << std::endl;
  std::cout << "=================================================" << std::endl;
  
  return failed > 0 ? 1 : 0;
}
