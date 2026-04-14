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
// Full Stack Integration Tests
// =============================================================================
// End-to-end tests simulating a real CedarGraph cluster
//
// Test Coverage:
// - Multi-service cluster simulation
// - Service registration/discovery with health checking
// - Event broadcasting across components
// - Configuration management
// =============================================================================

#include <gtest/gtest.h>
#include <atomic>
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

class FullStackIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_ = std::make_unique<ConfigManager>();
    registry_ = std::make_unique<ServiceRegistry>();
    event_bus_ = std::make_unique<EventBus>();
    health_checker_ = std::make_unique<HealthChecker>();
  }

  void TearDown() override {
    registry_->StopHealthCheck();
    health_checker_->Stop();
    health_checker_->StopHttpEndpoint();
    event_bus_->Stop();

    health_checker_->Clear();
    config_.reset();
    registry_.reset();
    event_bus_.reset();
    health_checker_.reset();
  }

  std::unique_ptr<ConfigManager> config_;
  std::unique_ptr<ServiceRegistry> registry_;
  std::unique_ptr<EventBus> event_bus_;
  std::unique_ptr<HealthChecker> health_checker_;
};

// =============================================================================
// Helper Functions
// =============================================================================

ServiceInfo CreateClusterService(const std::string& id, const std::string& name,
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
// Test 1: Mini Cluster Simulation
// =============================================================================

TEST_F(FullStackIntegrationTest, ClusterSimulation) {
  // Simulate a mini CedarGraph cluster with 3 nodes
  // Load config
  std::string config_yaml = R"(
cluster:
  name: test-cluster
  nodes: 3
  data_dir: /data/cedar
health_check:
  interval_ms: 100
  stale_threshold_ms: 200
)";

  EXPECT_TRUE(config_->LoadFromString(config_yaml).ok());

  // Verify config loaded correctly
  EXPECT_EQ(config_->GetString("cluster.name"), "test-cluster");
  EXPECT_EQ(config_->GetInt("cluster.nodes"), 3);

  // Register 3 storage services
  for (int i = 0; i < 3; i++) {
    ServiceInfo node = CreateClusterService("node-" + std::to_string(i), "storage",
                                            "127.0.0.1", 50051 + i);
    EXPECT_TRUE(registry_->Register(node).ok());
  }

  // Verify all services registered
  EXPECT_EQ(registry_->GetServiceCount(), 3);

  // Discover storage services
  auto services = registry_->Discover("storage");
  EXPECT_TRUE(services.ok());
  EXPECT_EQ(services.value().size(), 3);

  // Subscribe to service registration events
  std::atomic<int> registration_count{0};
  auto sub = event_bus_->Subscribe("service.registered", [&registration_count](const Event& event) {
    registration_count++;
  });

  // Publish registration events for each service
  for (int i = 0; i < 3; i++) {
    Event event("service.registered");
    event.Set("service_id", std::string("node-") + std::to_string(i));
    event.Set("service_name", std::string("storage"));
    event_bus_->Publish(event);
  }

  EXPECT_EQ(registration_count.load(), 3);

  // Cleanup
  event_bus_->Unsubscribe(sub);
}

// =============================================================================
// Test 2: Multi-Service Type Cluster
// =============================================================================

TEST_F(FullStackIntegrationTest, MultiServiceTypeCluster) {
  // Simulate a full cluster with metad, graphd, and storaged services

  // Load cluster configuration
  std::string config_yaml = R"(
cluster:
  name: production-cluster
  zone: us-west-1
services:
  metad:
    count: 1
    port: 50050
  graphd:
    count: 2
    port: 50051
  storaged:
    count: 3
    port: 50052
)";

  EXPECT_TRUE(config_->LoadFromString(config_yaml).ok());

  // Register metad services
  ServiceInfo metad = CreateClusterService("metad-0", "metad", "10.0.0.10", 50050);
  metad.metadata["zone"] = "us-west-1";
  metad.metadata["role"] = "leader";
  EXPECT_TRUE(registry_->Register(metad).ok());

  // Register graphd services
  for (int i = 0; i < 2; i++) {
    ServiceInfo graphd = CreateClusterService("graphd-" + std::to_string(i), "graphd",
                                              "10.0.0." + std::to_string(20 + i), 50051);
    graphd.metadata["zone"] = "us-west-1";
    EXPECT_TRUE(registry_->Register(graphd).ok());
  }

  // Register storaged services
  for (int i = 0; i < 3; i++) {
    ServiceInfo storaged = CreateClusterService("storaged-" + std::to_string(i), "storaged",
                                                "10.0.0." + std::to_string(30 + i), 50052);
    storaged.metadata["zone"] = "us-west-1";
    storaged.metadata["disk"] = "ssd";
    EXPECT_TRUE(registry_->Register(storaged).ok());
  }

  // Verify total services
  EXPECT_EQ(registry_->GetServiceCount(), 6);

  // Verify counts by type
  EXPECT_EQ(registry_->GetServiceCountByName("metad"), 1);
  EXPECT_EQ(registry_->GetServiceCountByName("graphd"), 2);
  EXPECT_EQ(registry_->GetServiceCountByName("storaged"), 3);

  // Test discovery for each service type
  auto metad_services = registry_->Discover("metad");
  EXPECT_TRUE(metad_services.ok());
  EXPECT_EQ(metad_services.value().size(), 1);

  auto graphd_services = registry_->Discover("graphd");
  EXPECT_TRUE(graphd_services.ok());
  EXPECT_EQ(graphd_services.value().size(), 2);

  auto storaged_services = registry_->Discover("storaged");
  EXPECT_TRUE(storaged_services.ok());
  EXPECT_EQ(storaged_services.value().size(), 3);

  // Verify metadata
  auto svc = registry_->GetService("metad-0");
  EXPECT_TRUE(svc.ok());
  EXPECT_EQ(svc.value().metadata["role"], "leader");
}

// =============================================================================
// Test 3: Health Monitoring with Event Broadcasting
// =============================================================================

TEST_F(FullStackIntegrationTest, HealthMonitoringWithEvents) {
  // Track events
  std::vector<std::string> event_log;

  // Subscribe to various events
  auto sub1 = event_bus_->Subscribe("service.registered", [&event_log](const Event& event) {
    event_log.push_back("REG:" + event.Get<std::string>("service_id"));
  });

  auto sub2 = event_bus_->Subscribe("health.check", [&event_log](const Event& event) {
    event_log.push_back("HEALTH:" + event.Get<std::string>("component"));
  });

  auto sub3 = event_bus_->Subscribe("service.status_changed", [&event_log](const Event& event) {
    event_log.push_back("STATUS:" + event.Get<std::string>("service_id") + "=" +
                        event.Get<std::string>("new_status"));
  });

  // Register services and their health check functions
  std::vector<std::string> service_ids = {"storaged-1", "storaged-2", "storaged-3"};
  for (const auto& id : service_ids) {
    ServiceInfo info = CreateClusterService(id, "storage", "127.0.0.1", 50051);
    EXPECT_TRUE(registry_->Register(info).ok());

    // Publish registration event
    Event reg_event("service.registered");
    reg_event.Set("service_id", id);
    event_bus_->Publish(reg_event);

    // Register with health checker - checks registry status
    health_checker_->RegisterComponent(id, [this, id]() {
      auto svc = registry_->GetService(id);
      if (!svc.ok()) return HealthStatus::kUnhealthy;
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
  }

  // Send heartbeats to make services healthy
  for (const auto& id : service_ids) {
    EXPECT_TRUE(registry_->Heartbeat(id).ok());

    // Publish status change event
    Event status_event("service.status_changed");
    status_event.Set("service_id", id);
    status_event.Set("new_status", std::string("Healthy"));
    event_bus_->Publish(status_event);
  }

  // Force health check
  health_checker_->ForceCheck();

  // Publish health check events
  for (const auto& id : service_ids) {
    Event health_event("health.check");
    health_event.Set("component", id);
    event_bus_->Publish(health_event);
  }

  // Verify events were received
  EXPECT_GE(event_log.size(), 9);  // 3 REG + 3 STATUS + 3 HEALTH

  // Verify overall health
  EXPECT_EQ(health_checker_->GetOverallHealth(), HealthStatus::kHealthy);

  // Cleanup
  event_bus_->Unsubscribe(sub1);
  event_bus_->Unsubscribe(sub2);
  event_bus_->Unsubscribe(sub3);
}

// =============================================================================
// Test 4: Service Watch and Auto-Health Registration
// =============================================================================

TEST_F(FullStackIntegrationTest, ServiceWatchAutoHealthRegistration) {
  // This test demonstrates how EventBus can be used to auto-register
  // services with the health checker when they're registered in the registry

  std::atomic<int> auto_registered_count{0};

  // Subscribe to service registration events and auto-register with health checker
  auto sub = event_bus_->Subscribe("service.registered", [this, &auto_registered_count](const Event& event) {
    std::string service_id = event.Get<std::string>("service_id");

    // Auto-register with health checker
    auto status = health_checker_->RegisterComponent(service_id, [this, service_id]() {
      auto svc = registry_->GetService(service_id);
      if (!svc.ok()) return HealthStatus::kUnhealthy;
      return svc.value().status == ServiceStatus::kHealthy ? HealthStatus::kHealthy
                                                           : HealthStatus::kStarting;
    });

    if (status.ok()) {
      auto_registered_count++;
    }
  });

  // Register services and publish events
  std::vector<std::string> service_ids = {"node-0", "node-1", "node-2"};
  for (const auto& id : service_ids) {
    ServiceInfo info = CreateClusterService(id, "storage", "127.0.0.1", 50051);
    EXPECT_TRUE(registry_->Register(info).ok());

    // Publish registration event
    Event event("service.registered");
    event.Set("service_id", id);
    event_bus_->Publish(event);
  }

  // Verify all services were auto-registered with health checker
  EXPECT_EQ(auto_registered_count.load(), 3);
  EXPECT_EQ(health_checker_->GetComponentCount(), 3);

  // Verify all components are registered
  for (const auto& id : service_ids) {
    EXPECT_TRUE(health_checker_->IsComponentRegistered(id));
  }

  // Cleanup
  event_bus_->Unsubscribe(sub);
}

// =============================================================================
// Test 5: Configuration-Driven Service Management
// =============================================================================

TEST_F(FullStackIntegrationTest, ConfigDrivenServiceManagement) {
  // Load configuration that defines expected services
  std::string config_yaml = R"(
cluster:
  name: config-driven-cluster
  expected_services:
    - id: metad-0
      name: metad
      host: 10.0.0.1
      port: 50050
    - id: graphd-0
      name: graphd
      host: 10.0.0.2
      port: 50051
    - id: storaged-0
      name: storaged
      host: 10.0.0.3
      port: 50052
)";

  EXPECT_TRUE(config_->LoadFromString(config_yaml).ok());

  // Get expected services from config
  // Note: ConfigManager doesn't have direct array access, so we simulate by
  // using individual keys or loading from the expected structure

  // Simulate registering services based on config
  struct ExpectedService {
    std::string id;
    std::string name;
    std::string host;
    int port;
  };

  std::vector<ExpectedService> expected_services = {
      {"metad-0", "metad", "10.0.0.1", 50050},
      {"graphd-0", "graphd", "10.0.0.2", 50051},
      {"storaged-0", "storaged", "10.0.0.3", 50052}};

  // Track which services are registered
  std::vector<std::string> registered_services;

  // Subscribe to registration events
  auto sub = event_bus_->Subscribe("service.registered", [&registered_services](const Event& event) {
    registered_services.push_back(event.Get<std::string>("service_id"));
  });

  // Register services based on config
  for (const auto& expected : expected_services) {
    ServiceInfo info = CreateClusterService(expected.id, expected.name, expected.host, expected.port);
    EXPECT_TRUE(registry_->Register(info).ok());

    // Publish event
    Event event("service.registered");
    event.Set("service_id", expected.id);
    event.Set("service_name", expected.name);
    event_bus_->Publish(event);
  }

  // Verify all expected services are registered
  EXPECT_EQ(registered_services.size(), 3);
  EXPECT_EQ(registry_->GetServiceCount(), 3);

  // Verify each service can be discovered
  for (const auto& expected : expected_services) {
    auto svc = registry_->GetService(expected.id);
    EXPECT_TRUE(svc.ok()) << "Service " << expected.id << " not found";
    if (svc.ok()) {
      EXPECT_EQ(svc.value().name, expected.name);
      EXPECT_EQ(svc.value().host, expected.host);
      EXPECT_EQ(svc.value().port, expected.port);
    }
  }

  // Cleanup
  event_bus_->Unsubscribe(sub);
}

// =============================================================================
// Test 6: Health Status Propagation Through EventBus
// =============================================================================

TEST_F(FullStackIntegrationTest, HealthStatusPropagation) {
  // Set up health status tracking through EventBus
  std::vector<std::pair<std::string, std::string>> status_changes;

  // Subscribe to health status changes
  auto sub = event_bus_->Subscribe("health.status_changed", [&status_changes](const Event& event) {
    std::string component = event.Get<std::string>("component");
    std::string new_status = event.Get<std::string>("new_status");
    status_changes.emplace_back(component, new_status);
  });

  // Register services and health checkers
  std::atomic<bool> service_healthy{true};

  ServiceInfo info = CreateClusterService("storaged-1", "storage", "127.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(info).ok());

  // Register health checker that can change status
  health_checker_->RegisterComponent("storaged-1", [&service_healthy]() {
    return service_healthy.load() ? HealthStatus::kHealthy : HealthStatus::kUnhealthy;
  });

  // Initial check - should be healthy
  health_checker_->ForceCheck();

  // Simulate status change event
  Event event("health.status_changed");
  event.Set("component", std::string("storaged-1"));
  event.Set("old_status", std::string("Unknown"));
  event.Set("new_status", std::string("Healthy"));
  event_bus_->Publish(event);

  // Change status to unhealthy
  service_healthy = false;
  health_checker_->ForceCheck();

  // Simulate status change event
  Event event2("health.status_changed");
  event2.Set("component", std::string("storaged-1"));
  event2.Set("old_status", std::string("Healthy"));
  event2.Set("new_status", std::string("Unhealthy"));
  event_bus_->Publish(event2);

  // Verify status changes were tracked
  EXPECT_EQ(status_changes.size(), 2);
  EXPECT_EQ(status_changes[0].second, "Healthy");
  EXPECT_EQ(status_changes[1].second, "Unhealthy");

  // Verify current health status
  auto health = health_checker_->CheckComponent("storaged-1");
  EXPECT_TRUE(health.ok());
  EXPECT_EQ(health.value().status, HealthStatus::kUnhealthy);

  // Cleanup
  event_bus_->Unsubscribe(sub);
}

// =============================================================================
// Test 7: Concurrent Service Registration
// =============================================================================

TEST_F(FullStackIntegrationTest, ConcurrentServiceRegistration) {
  // Test concurrent registration of services from multiple "nodes"

  const int num_services = 10;
  std::atomic<int> registered_count{0};
  std::atomic<int> event_count{0};

  // Subscribe to registration events
  auto sub = event_bus_->Subscribe("service.registered", [&event_count](const Event& event) {
    event_count++;
  });

  // Start EventBus async processing
  event_bus_->Start();

  // Register services concurrently
  std::vector<std::thread> threads;
  for (int i = 0; i < num_services; i++) {
    threads.emplace_back([this, i, &registered_count]() {
      std::string id = "storaged-" + std::to_string(i);
      ServiceInfo info = CreateClusterService(id, "storage", "127.0.0.1", 50051 + i);

      auto status = registry_->Register(info);
      if (status.ok()) {
        registered_count++;
      }
    });
  }

  // Wait for all registrations
  for (auto& t : threads) {
    t.join();
  }

  // Verify all services registered
  EXPECT_EQ(registered_count.load(), num_services);
  EXPECT_EQ(registry_->GetServiceCount(), num_services);

  // Publish events for each registered service
  for (int i = 0; i < num_services; i++) {
    Event event("service.registered");
    event.Set("service_id", std::string("storaged-") + std::to_string(i));
    event_bus_->PublishAsync(event);
  }

  // Wait for async events to be processed
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify events were processed
  EXPECT_EQ(event_count.load(), num_services);

  // Cleanup
  event_bus_->Stop();
  event_bus_->Unsubscribe(sub);
}

// =============================================================================
// Test 8: Full Cluster Lifecycle
// =============================================================================

TEST_F(FullStackIntegrationTest, FullClusterLifecycle) {
  // Simulate complete cluster lifecycle:
  // 1. Initial setup
  // 2. Service registration
  // 3. Health monitoring
  // 4. Service deregistration
  // 5. Cleanup

  std::vector<std::string> lifecycle_events;

  // Step 1: Initial setup
  lifecycle_events.push_back("SETUP");

  std::string config_yaml = R"(
cluster:
  name: lifecycle-test-cluster
  health_check_interval_ms: 50
)";
  EXPECT_TRUE(config_->LoadFromString(config_yaml).ok());

  // Subscribe to lifecycle events
  auto sub1 = event_bus_->Subscribe("service.registered", [&lifecycle_events](const Event& event) {
    lifecycle_events.push_back("REGISTER:" + event.Get<std::string>("service_id"));
  });

  auto sub2 = event_bus_->Subscribe("service.deregistered", [&lifecycle_events](const Event& event) {
    lifecycle_events.push_back("DEREGISTER:" + event.Get<std::string>("service_id"));
  });

  // Step 2: Service registration
  lifecycle_events.push_back("REGISTRATION_START");

  std::vector<std::string> service_ids = {"metad-0", "graphd-0", "storaged-0", "storaged-1"};
  for (const auto& id : service_ids) {
    std::string name = id.substr(0, id.find('-'));
    ServiceInfo info = CreateClusterService(id, name, "127.0.0.1", 50051);
    EXPECT_TRUE(registry_->Register(info).ok());

    // Register with health checker
    health_checker_->RegisterComponent(id, [this, id]() {
      auto svc = registry_->GetService(id);
      if (!svc.ok()) return HealthStatus::kUnhealthy;
      return svc.value().status == ServiceStatus::kHealthy ? HealthStatus::kHealthy
                                                           : HealthStatus::kStarting;
    });

    // Publish registration event
    Event event("service.registered");
    event.Set("service_id", id);
    event_bus_->Publish(event);
  }

  lifecycle_events.push_back("REGISTRATION_COMPLETE");
  EXPECT_EQ(registry_->GetServiceCount(), 4);

  // Step 3: Health monitoring
  lifecycle_events.push_back("HEALTH_CHECK_START");

  // Make all services healthy
  for (const auto& id : service_ids) {
    EXPECT_TRUE(registry_->Heartbeat(id).ok());
  }

  health_checker_->ForceCheck();
  EXPECT_EQ(health_checker_->GetOverallHealth(), HealthStatus::kHealthy);

  lifecycle_events.push_back("HEALTH_CHECK_COMPLETE");

  // Step 4: Service deregistration
  lifecycle_events.push_back("DEREGISTRATION_START");

  // Deregister two services
  std::vector<std::string> services_to_remove = {"storaged-0", "storaged-1"};
  for (const auto& id : services_to_remove) {
    EXPECT_TRUE(registry_->Deregister(id).ok());

    // Unregister from health checker
    health_checker_->UnregisterComponent(id);

    // Publish deregistration event
    Event event("service.deregistered");
    event.Set("service_id", id);
    event_bus_->Publish(event);
  }

  lifecycle_events.push_back("DEREGISTRATION_COMPLETE");
  EXPECT_EQ(registry_->GetServiceCount(), 2);

  // Step 5: Cleanup
  lifecycle_events.push_back("CLEANUP");

  // Verify lifecycle events
  EXPECT_GE(lifecycle_events.size(), 8);
  EXPECT_EQ(lifecycle_events[0], "SETUP");
  EXPECT_EQ(lifecycle_events[lifecycle_events.size() - 1], "CLEANUP");

  // Cleanup
  event_bus_->Unsubscribe(sub1);
  event_bus_->Unsubscribe(sub2);
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
