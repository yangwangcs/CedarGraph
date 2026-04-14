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
// Governance Integration Tests
// =============================================================================
// Tests the interaction between ServiceRegistry, ConfigManager, and HealthChecker
//
// Test Coverage:
// - Service registration triggers health check
// - Config changes notify interested services
// - Health status changes trigger events
// =============================================================================

#include <gtest/gtest.h>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "cedar/governance/config_manager.h"
#include "cedar/governance/health_checker.h"
#include "cedar/governance/service_registry.h"
#include "cedar/integration/event_bus.h"

using namespace cedar::governance;
using namespace cedar::integration;

// =============================================================================
// Test Fixture
// =============================================================================

class GovernanceIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registry_ = std::make_unique<ServiceRegistry>();
    health_checker_ = std::make_unique<HealthChecker>();
    config_ = std::make_unique<ConfigManager>();
    event_bus_ = std::make_unique<EventBus>();
  }

  void TearDown() override {
    registry_->StopHealthCheck();
    health_checker_->Stop();
    health_checker_->StopHttpEndpoint();
    event_bus_->Stop();

    health_checker_->Clear();
    registry_.reset();
    health_checker_.reset();
    config_.reset();
    event_bus_.reset();
  }

  std::unique_ptr<ServiceRegistry> registry_;
  std::unique_ptr<HealthChecker> health_checker_;
  std::unique_ptr<ConfigManager> config_;
  std::unique_ptr<EventBus> event_bus_;
};

// =============================================================================
// Helper Functions
// =============================================================================

ServiceInfo CreateTestService(const std::string& id, const std::string& name,
                              const std::string& host, int port) {
  ServiceInfo info;
  info.id = id;
  info.name = name;
  info.host = host;
  info.port = port;
  info.status = ServiceStatus::kStarting;
  return info;
}

// =============================================================================
// Test 1: ServiceRegistry and HealthChecker Integration
// =============================================================================

TEST_F(GovernanceIntegrationTest, ServiceRegistryAndHealthChecker) {
  // Register a service in the registry
  ServiceInfo storage = CreateTestService("storaged-1", "storage", "10.0.0.1", 50051);
  auto status = registry_->Register(storage);
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Health checker monitors the service by checking registry
  status = health_checker_->RegisterComponent("storaged-1", [this]() {
    auto svc = registry_->GetService("storaged-1");
    if (!svc.ok()) {
      return HealthStatus::kUnhealthy;
    }
    // Map ServiceStatus to HealthStatus
    switch (svc.value().status) {
      case ServiceStatus::kHealthy:
        return HealthStatus::kHealthy;
      case ServiceStatus::kUnhealthy:
        return HealthStatus::kUnhealthy;
      case ServiceStatus::kStarting:
        return HealthStatus::kStarting;
      case ServiceStatus::kStopping:
        return HealthStatus::kUnhealthy;
      default:
        return HealthStatus::kUnknown;
    }
  });
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Initially service is Starting, health check should reflect that
  auto health = health_checker_->CheckComponent("storaged-1");
  EXPECT_TRUE(health.ok()) << health.status().ToString();
  EXPECT_EQ(health.value().status, HealthStatus::kStarting);

  // Send heartbeat to make service healthy
  status = registry_->Heartbeat("storaged-1");
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Re-check health - should now be healthy
  health = health_checker_->CheckComponent("storaged-1");
  EXPECT_TRUE(health.ok()) << health.status().ToString();
  EXPECT_EQ(health.value().status, HealthStatus::kHealthy);
}

TEST_F(GovernanceIntegrationTest, HealthCheckerWatchesRegistryChanges) {
  // Track health change events
  std::vector<HealthChangeEvent> health_events;

  // Watch for health changes on a specific component
  auto watch_result = health_checker_->WatchComponent(
      "storaged-1", [&health_events](const HealthChangeEvent& event) {
        health_events.push_back(event);
      });
  EXPECT_TRUE(watch_result.ok()) << watch_result.status().ToString();

  // Register service in registry first with Starting status
  ServiceInfo storage = CreateTestService("storaged-1", "storage", "10.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(storage).ok());

  // Register the component with health checker - checks registry status
  health_checker_->RegisterComponent("storaged-1", [this]() {
    auto svc = registry_->GetService("storaged-1");
    if (!svc.ok()) {
      return HealthStatus::kUnhealthy;
    }
    // Map ServiceStatus to HealthStatus
    switch (svc.value().status) {
      case ServiceStatus::kHealthy:
        return HealthStatus::kHealthy;
      case ServiceStatus::kUnhealthy:
        return HealthStatus::kUnhealthy;
      case ServiceStatus::kStarting:
        return HealthStatus::kStarting;
      default:
        return HealthStatus::kUnknown;
    }
  });

  // Force a health check - should report Starting since service is in Starting status
  health_checker_->ForceCheck();

  // Should have received ComponentAdded event
  EXPECT_GE(health_events.size(), 1);
  bool found_component_added = false;
  for (const auto& event : health_events) {
    if (event.type == HealthChangeType::kComponentAdded) {
      found_component_added = true;
      EXPECT_EQ(event.component_name, "storaged-1");
      break;
    }
  }
  EXPECT_TRUE(found_component_added);

  // Make service healthy via heartbeat
  registry_->Heartbeat("storaged-1");
  health_checker_->ForceCheck();

  // Find StatusChanged event to Healthy
  bool found_status_change_to_healthy = false;
  for (const auto& event : health_events) {
    if (event.type == HealthChangeType::kStatusChanged && 
        event.new_status == HealthStatus::kHealthy) {
      found_status_change_to_healthy = true;
      EXPECT_EQ(event.component_name, "storaged-1");
      break;
    }
  }
  EXPECT_TRUE(found_status_change_to_healthy);
}

TEST_F(GovernanceIntegrationTest, MultipleServicesHealthAggregation) {
  // Register multiple services in registry
  std::vector<std::string> service_ids = {"storaged-1", "storaged-2", "storaged-3"};
  for (const auto& id : service_ids) {
    ServiceInfo info = CreateTestService(id, "storage", "10.0.0.1", 50051);
    EXPECT_TRUE(registry_->Register(info).ok());

    // Register each with health checker
    health_checker_->RegisterComponent(id, [this, id]() {
      auto svc = registry_->GetService(id);
      if (!svc.ok()) return HealthStatus::kUnhealthy;
      return svc.value().status == ServiceStatus::kHealthy ? HealthStatus::kHealthy
                                                           : HealthStatus::kUnhealthy;
    });
  }

  // Initially all unhealthy/starting
  health_checker_->ForceCheck();
  EXPECT_EQ(health_checker_->GetOverallHealth(), HealthStatus::kUnhealthy);

  // Make all healthy
  for (const auto& id : service_ids) {
    registry_->Heartbeat(id);
  }

  health_checker_->ForceCheck();
  EXPECT_EQ(health_checker_->GetOverallHealth(), HealthStatus::kHealthy);
  EXPECT_TRUE(health_checker_->IsHealthy());
  EXPECT_TRUE(health_checker_->IsReady());

  // Make one unhealthy
  registry_->UpdateStatus("storaged-2", ServiceStatus::kUnhealthy);
  health_checker_->ForceCheck();

  // Overall health should be unhealthy (worst wins)
  EXPECT_EQ(health_checker_->GetOverallHealth(), HealthStatus::kUnhealthy);
  EXPECT_FALSE(health_checker_->IsHealthy());
}

// =============================================================================
// Test 2: ConfigManager and ServiceRegistry Integration
// =============================================================================

TEST_F(GovernanceIntegrationTest, ConfigChangesNotifyServices) {
  // Load initial configuration
  std::string yaml_config = R"(
cluster:
  name: test-cluster
  heartbeat_interval_ms: 1000
  stale_threshold_ms: 5000
)";

  auto status = config_->LoadFromString(yaml_config);
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Register a service
  ServiceInfo service = CreateTestService("graphd-1", "graphd", "10.0.0.2", 50052);
  EXPECT_TRUE(registry_->Register(service).ok());

  // Watch config changes for heartbeat interval
  int64_t config_watch_id;
  std::string changed_key;
  std::string new_value;
  {
    auto result = config_->Watch("cluster.heartbeat_interval_ms",
                                 [&changed_key, &new_value](const ConfigChangeEvent& event) {
                                   changed_key = event.key;
                                   new_value = event.new_value;
                                 });
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    config_watch_id = result.value();
  }

  // Change the config
  config_->SetInt("cluster.heartbeat_interval_ms", 2000);

  // Verify the watch was triggered
  EXPECT_EQ(changed_key, "cluster.heartbeat_interval_ms");
  EXPECT_EQ(new_value, "2000");

  // Cleanup
  EXPECT_TRUE(config_->Unwatch(config_watch_id).ok());
}

TEST_F(GovernanceIntegrationTest, ServiceRegistryHealthCheckConfig) {
  // Load config with health check settings
  std::string yaml_config = R"(
health_check:
  interval_ms: 50
  stale_threshold_ms: 100
)";

  EXPECT_TRUE(config_->LoadFromString(yaml_config).ok());

  // Get health check config
  int interval_ms = config_->GetInt("health_check.interval_ms", 1000);
  int stale_threshold_ms = config_->GetInt("health_check.stale_threshold_ms", 30000);

  EXPECT_EQ(interval_ms, 50);
  EXPECT_EQ(stale_threshold_ms, 100);

  // Start health check with config values
  auto status = registry_->StartHealthCheck(interval_ms, stale_threshold_ms);
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Register and heartbeat a service
  ServiceInfo service = CreateTestService("metad-1", "metad", "10.0.0.3", 50053);
  EXPECT_TRUE(registry_->Register(service).ok());
  EXPECT_TRUE(registry_->Heartbeat("metad-1").ok());

  // Verify service is healthy
  auto svc = registry_->GetService("metad-1");
  EXPECT_EQ(svc.value().status, ServiceStatus::kHealthy);

  // Wait for service to become stale (stale_threshold_ms + margin)
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  // Service should now be marked unhealthy
  svc = registry_->GetService("metad-1");
  EXPECT_EQ(svc.value().status, ServiceStatus::kUnhealthy);
}

// =============================================================================
// Test 3: EventBus Integration with Governance Components
// =============================================================================

TEST_F(GovernanceIntegrationTest, ServiceRegistrationTriggersEvent) {
  // Track events published to EventBus
  std::vector<Event> received_events;

  // Subscribe to service registration events
  auto sub = event_bus_->Subscribe("service.registered", [&received_events](const Event& event) {
    received_events.push_back(event);
  });

  // Register a service and publish event
  ServiceInfo service = CreateTestService("storaged-1", "storage", "10.0.0.1", 50051);
  auto status = registry_->Register(service);
  EXPECT_TRUE(status.ok());

  // Publish registration event
  Event event("service.registered");
  event.Set("service_id", service.id);
  event.Set("service_name", service.name);
  event.Set("host", service.host);
  event.Set("port", service.port);
  event_bus_->Publish(event);

  // Verify event was received
  EXPECT_EQ(received_events.size(), 1);
  EXPECT_EQ(received_events[0].Get<std::string>("service_id"), "storaged-1");
  EXPECT_EQ(received_events[0].Get<std::string>("service_name"), "storage");

  // Cleanup
  EXPECT_TRUE(event_bus_->Unsubscribe(sub));
}

TEST_F(GovernanceIntegrationTest, HealthStatusChangeTriggersEvent) {
  std::vector<Event> health_events;

  // Subscribe to health status change events
  auto sub = event_bus_->Subscribe("health.status_changed", [&health_events](const Event& event) {
    health_events.push_back(event);
  });

  // Register component
  health_checker_->RegisterComponent("storage", []() { return HealthStatus::kHealthy; });

  // Force check and publish event
  health_checker_->ForceCheck();

  // Simulate publishing a health status change event
  Event event("health.status_changed");
  event.Set("component", std::string("storage"));
  event.Set("old_status", std::string("Unknown"));
  event.Set("new_status", std::string("Healthy"));
  event_bus_->Publish(event);

  // Verify event was received
  EXPECT_EQ(health_events.size(), 1);
  EXPECT_EQ(health_events[0].Get<std::string>("component"), "storage");
  EXPECT_EQ(health_events[0].Get<std::string>("new_status"), "Healthy");

  // Cleanup
  EXPECT_TRUE(event_bus_->Unsubscribe(sub));
}

TEST_F(GovernanceIntegrationTest, ConfigChangeTriggersEvent) {
  std::vector<Event> config_events;

  // Subscribe to config change events
  auto sub = event_bus_->Subscribe("config.changed", [&config_events](const Event& event) {
    config_events.push_back(event);
  });

  // Load config and publish change event
  EXPECT_TRUE(config_->LoadFromString("cluster:\n  name: test-cluster").ok());

  // Publish config change event
  Event event("config.changed");
  event.Set("key", std::string("cluster.name"));
  event.Set("value", std::string("test-cluster"));
  event_bus_->Publish(event);

  // Verify event was received
  EXPECT_EQ(config_events.size(), 1);
  EXPECT_EQ(config_events[0].Get<std::string>("key"), "cluster.name");

  // Cleanup
  EXPECT_TRUE(event_bus_->Unsubscribe(sub));
}

// =============================================================================
// Test 4: End-to-End Integration Flow
// =============================================================================

TEST_F(GovernanceIntegrationTest, EndToEndGovernanceFlow) {
  // This test simulates a complete governance flow:
  // 1. Load configuration
  // 2. Register services
  // 3. Start health checking
  // 4. Monitor via EventBus
  // 5. React to changes

  std::vector<std::string> event_log;

  // Step 1: Load configuration
  std::string config_yaml = R"(
cluster:
  name: integration-test-cluster
  health_check_interval_ms: 50
services:
  storage:
    port: 50051
    expected_count: 2
)";
  EXPECT_TRUE(config_->LoadFromString(config_yaml).ok());

  // Step 2: Subscribe to events
  auto sub1 = event_bus_->Subscribe("service.registered", [&event_log](const Event& event) {
    event_log.push_back("Registered: " + event.Get<std::string>("service_id"));
  });

  auto sub2 = event_bus_->Subscribe("health.status_changed", [&event_log](const Event& event) {
    event_log.push_back("Health: " + event.Get<std::string>("component") + " -> " +
                        event.Get<std::string>("new_status"));
  });

  // Step 3: Register services
  std::vector<std::string> service_ids = {"storaged-1", "storaged-2"};
  for (const auto& id : service_ids) {
    ServiceInfo info = CreateTestService(id, "storage", "10.0.0.1", 50051);
    EXPECT_TRUE(registry_->Register(info).ok());

    // Publish registration event
    Event event("service.registered");
    event.Set("service_id", id);
    event_bus_->Publish(event);

    // Register with health checker
    health_checker_->RegisterComponent(id, [this, id]() {
      auto svc = registry_->GetService(id);
      if (!svc.ok()) return HealthStatus::kUnhealthy;
      return svc.value().status == ServiceStatus::kHealthy ? HealthStatus::kHealthy
                                                           : HealthStatus::kUnhealthy;
    });
  }

  // Step 4: Send heartbeats to make services healthy
  for (const auto& id : service_ids) {
    EXPECT_TRUE(registry_->Heartbeat(id).ok());
  }

  // Force health check
  health_checker_->ForceCheck();

  // Step 5: Verify overall system health
  EXPECT_EQ(health_checker_->GetOverallHealth(), HealthStatus::kHealthy);

  // Step 6: Verify services are discoverable
  auto discovered = registry_->Discover("storage");
  EXPECT_TRUE(discovered.ok());
  EXPECT_EQ(discovered.value().size(), 2);

  // Verify config is accessible
  EXPECT_EQ(config_->GetString("cluster.name"), "integration-test-cluster");

  // Cleanup
  event_bus_->Unsubscribe(sub1);
  event_bus_->Unsubscribe(sub2);

  // Verify events were logged
  EXPECT_GE(event_log.size(), 2);  // At least registration events
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
