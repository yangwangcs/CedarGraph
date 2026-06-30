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

#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "cedar/governance/service_registry.h"

using namespace cedar::governance;

// =============================================================================
// Test Fixture
// =============================================================================

class ServiceRegistryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registry_ = std::make_unique<ServiceRegistry>();
  }

  void TearDown() override {
    registry_->StopHealthCheck();
    registry_.reset();
  }

  std::unique_ptr<ServiceRegistry> registry_;
};

// =============================================================================
// Helper Functions
// =============================================================================

ServiceInfo CreateServiceInfo(const std::string& id, const std::string& name,
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
// Registration and Discovery Tests
// =============================================================================

TEST_F(ServiceRegistryTest, RegisterAndDiscover) {
  // Register a service
  ServiceInfo info = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  
  auto status = registry_->Register(info);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(registry_->GetServiceCount(), 1);

  // Discover the service by name
  auto result = registry_->Discover("storaged");
  EXPECT_TRUE(result.ok()) << result.status().ToString();
  
  auto services = result.value();
  EXPECT_EQ(services.size(), 1);
  EXPECT_EQ(services[0].id, "storaged-1");
  EXPECT_EQ(services[0].name, "storaged");
  EXPECT_EQ(services[0].host, "10.0.0.1");
  EXPECT_EQ(services[0].port, 50051);
}

TEST_F(ServiceRegistryTest, RegisterMultipleServicesSameName) {
  // Register multiple services with the same name
  ServiceInfo info1 = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  ServiceInfo info2 = CreateServiceInfo("storaged-2", "storaged", "10.0.0.2", 50051);
  ServiceInfo info3 = CreateServiceInfo("storaged-3", "storaged", "10.0.0.3", 50051);

  EXPECT_TRUE(registry_->Register(info1).ok());
  EXPECT_TRUE(registry_->Register(info2).ok());
  EXPECT_TRUE(registry_->Register(info3).ok());

  EXPECT_EQ(registry_->GetServiceCount(), 3);
  EXPECT_EQ(registry_->GetServiceCountByName("storaged"), 3);

  // Discover all storaged services
  auto result = registry_->Discover("storaged");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().size(), 3);
}

TEST_F(ServiceRegistryTest, RegisterDifferentServiceNames) {
  // Register services with different names
  ServiceInfo storaged = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  ServiceInfo graphd = CreateServiceInfo("graphd-1", "graphd", "10.0.0.2", 50052);
  ServiceInfo metad = CreateServiceInfo("metad-1", "metad", "10.0.0.3", 50053);

  EXPECT_TRUE(registry_->Register(storaged).ok());
  EXPECT_TRUE(registry_->Register(graphd).ok());
  EXPECT_TRUE(registry_->Register(metad).ok());

  EXPECT_EQ(registry_->GetServiceCount(), 3);

  // Discover each service type
  auto storaged_result = registry_->Discover("storaged");
  EXPECT_EQ(storaged_result.value().size(), 1);

  auto graphd_result = registry_->Discover("graphd");
  EXPECT_EQ(graphd_result.value().size(), 1);

  auto metad_result = registry_->Discover("metad");
  EXPECT_EQ(metad_result.value().size(), 1);

  // Discover non-existent service
  auto unknown_result = registry_->Discover("unknown");
  EXPECT_TRUE(unknown_result.ok());
  EXPECT_EQ(unknown_result.value().size(), 0);
}

TEST_F(ServiceRegistryTest, RegisterDuplicateIdFails) {
  ServiceInfo info1 = CreateServiceInfo("service-1", "storaged", "10.0.0.1", 50051);
  ServiceInfo info2 = CreateServiceInfo("service-1", "graphd", "10.0.0.2", 50052);

  EXPECT_TRUE(registry_->Register(info1).ok());
  
  auto status = registry_->Register(info2);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsConflict());
}

TEST_F(ServiceRegistryTest, RegisterInvalidArguments) {
  // Empty ID
  ServiceInfo no_id = CreateServiceInfo("", "storaged", "10.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(no_id).IsInvalidArgument());

  // Empty name
  ServiceInfo no_name = CreateServiceInfo("service-1", "", "10.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(no_name).IsInvalidArgument());

  // Empty host
  ServiceInfo no_host = CreateServiceInfo("service-1", "storaged", "", 50051);
  EXPECT_TRUE(registry_->Register(no_host).IsInvalidArgument());

  // Invalid port
  ServiceInfo invalid_port = CreateServiceInfo("service-1", "storaged", "10.0.0.1", -1);
  EXPECT_TRUE(registry_->Register(invalid_port).IsInvalidArgument());

  ServiceInfo zero_port = CreateServiceInfo("service-1", "storaged", "10.0.0.1", 0);
  EXPECT_TRUE(registry_->Register(zero_port).IsInvalidArgument());

  ServiceInfo large_port = CreateServiceInfo("service-1", "storaged", "10.0.0.1", 70000);
  EXPECT_TRUE(registry_->Register(large_port).IsInvalidArgument());
}

// =============================================================================
// Deregistration Tests
// =============================================================================

TEST_F(ServiceRegistryTest, DeregisterService) {
  // Register a service
  ServiceInfo info = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(info).ok());
  EXPECT_EQ(registry_->GetServiceCount(), 1);

  // Deregister the service
  auto status = registry_->Deregister("storaged-1");
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(registry_->GetServiceCount(), 0);

  // Try to discover the deregistered service
  auto result = registry_->Discover("storaged");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().size(), 0);
}

TEST_F(ServiceRegistryTest, DeregisterNonExistentService) {
  auto status = registry_->Deregister("non-existent");
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsNotFound());
}

TEST_F(ServiceRegistryTest, DeregisterOnlyOneService) {
  // Register multiple services
  ServiceInfo info1 = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  ServiceInfo info2 = CreateServiceInfo("storaged-2", "storaged", "10.0.0.2", 50051);
  
  EXPECT_TRUE(registry_->Register(info1).ok());
  EXPECT_TRUE(registry_->Register(info2).ok());
  EXPECT_EQ(registry_->GetServiceCount(), 2);

  // Deregister only one
  EXPECT_TRUE(registry_->Deregister("storaged-1").ok());
  EXPECT_EQ(registry_->GetServiceCount(), 1);

  // Verify the other still exists
  auto result = registry_->Discover("storaged");
  EXPECT_EQ(result.value().size(), 1);
  EXPECT_EQ(result.value()[0].id, "storaged-2");
}

// =============================================================================
// GetService Tests
// =============================================================================

TEST_F(ServiceRegistryTest, GetService) {
  ServiceInfo info = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  info.metadata["zone"] = "us-west-1";
  info.metadata["rack"] = "rack-01";
  
  EXPECT_TRUE(registry_->Register(info).ok());

  auto result = registry_->GetService("storaged-1");
  EXPECT_TRUE(result.ok());
  
  auto service = result.value();
  EXPECT_EQ(service.id, "storaged-1");
  EXPECT_EQ(service.name, "storaged");
  EXPECT_EQ(service.metadata["zone"], "us-west-1");
  EXPECT_EQ(service.metadata["rack"], "rack-01");
}

TEST_F(ServiceRegistryTest, GetServiceNotFound) {
  auto result = registry_->GetService("non-existent");
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsNotFound());
}

// =============================================================================
// Status Update Tests
// =============================================================================

TEST_F(ServiceRegistryTest, UpdateStatus) {
  ServiceInfo info = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  info.status = ServiceStatus::kStarting;
  EXPECT_TRUE(registry_->Register(info).ok());

  // Update status to healthy
  auto status = registry_->UpdateStatus("storaged-1", ServiceStatus::kHealthy);
  EXPECT_TRUE(status.ok());

  // Verify status changed
  auto result = registry_->GetService("storaged-1");
  EXPECT_EQ(result.value().status, ServiceStatus::kHealthy);

  // Update status to unhealthy
  EXPECT_TRUE(registry_->UpdateStatus("storaged-1", ServiceStatus::kUnhealthy).ok());
  
  result = registry_->GetService("storaged-1");
  EXPECT_EQ(result.value().status, ServiceStatus::kUnhealthy);
}

TEST_F(ServiceRegistryTest, UpdateStatusNotFound) {
  auto status = registry_->UpdateStatus("non-existent", ServiceStatus::kHealthy);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsNotFound());
}

// =============================================================================
// Heartbeat Tests
// =============================================================================

TEST_F(ServiceRegistryTest, Heartbeat) {
  ServiceInfo info = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(info).ok());

  // Get initial heartbeat time
  auto result = registry_->GetService("storaged-1");
  int64_t initial_heartbeat = result.value().last_heartbeat_ms;

  // Small delay to ensure timestamp changes
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Send heartbeat
  auto status = registry_->Heartbeat("storaged-1");
  EXPECT_TRUE(status.ok());

  // Verify heartbeat was updated
  result = registry_->GetService("storaged-1");
  EXPECT_GT(result.value().last_heartbeat_ms, initial_heartbeat);
}

TEST_F(ServiceRegistryTest, HeartbeatNotFound) {
  auto status = registry_->Heartbeat("non-existent");
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsNotFound());
}

TEST_F(ServiceRegistryTest, HeartbeatTransitionsStartingToHealthy) {
  // Register a service with Starting status
  ServiceInfo info = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  info.status = ServiceStatus::kStarting;
  EXPECT_TRUE(registry_->Register(info).ok());

  auto result = registry_->GetService("storaged-1");
  EXPECT_EQ(result.value().status, ServiceStatus::kStarting);

  // Send heartbeat - should transition to Healthy
  EXPECT_TRUE(registry_->Heartbeat("storaged-1").ok());

  result = registry_->GetService("storaged-1");
  EXPECT_EQ(result.value().status, ServiceStatus::kHealthy);
}

// =============================================================================
// Watch Tests
// =============================================================================

TEST_F(ServiceRegistryTest, WatchServiceChanges) {
  std::vector<ServiceEvent> events;

  // Register a watch
  auto watch_result = registry_->Watch("storaged", 
      [&events](const ServiceEvent& event) {
        events.push_back(event);
      });
  EXPECT_TRUE(watch_result.ok());
  int64_t watch_id = watch_result.value();

  // Register a service - should trigger callback
  ServiceInfo info = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(info).ok());

  EXPECT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].type, ServiceEventType::kRegistered);
  EXPECT_EQ(events[0].service.id, "storaged-1");

  // Update status - should trigger callback
  EXPECT_TRUE(registry_->UpdateStatus("storaged-1", ServiceStatus::kHealthy).ok());
  EXPECT_EQ(events.size(), 2);
  EXPECT_EQ(events[1].type, ServiceEventType::kStatusChanged);

  // Deregister - should trigger callback
  EXPECT_TRUE(registry_->Deregister("storaged-1").ok());
  EXPECT_EQ(events.size(), 3);
  EXPECT_EQ(events[2].type, ServiceEventType::kDeregistered);

  // Unwatch
  EXPECT_TRUE(registry_->Unwatch(watch_id).ok());
}

TEST_F(ServiceRegistryTest, WatchMultipleServices) {
  std::vector<ServiceEvent> events;

  auto watch_result = registry_->Watch("storaged",
      [&events](const ServiceEvent& event) {
        events.push_back(event);
      });
  int64_t watch_id = watch_result.value();

  // Register multiple services
  ServiceInfo info1 = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  ServiceInfo info2 = CreateServiceInfo("storaged-2", "storaged", "10.0.0.2", 50051);
  
  EXPECT_TRUE(registry_->Register(info1).ok());
  EXPECT_TRUE(registry_->Register(info2).ok());

  EXPECT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].service.id, "storaged-1");
  EXPECT_EQ(events[1].service.id, "storaged-2");

  EXPECT_TRUE(registry_->Unwatch(watch_id).ok());
}

TEST_F(ServiceRegistryTest, WatchOnlyNotifiesForMatchingName) {
  std::vector<ServiceEvent> storaged_events;
  std::vector<ServiceEvent> graphd_events;

  auto watch1 = registry_->Watch("storaged",
      [&storaged_events](const ServiceEvent& event) {
        storaged_events.push_back(event);
      });
  
  auto watch2 = registry_->Watch("graphd",
      [&graphd_events](const ServiceEvent& event) {
        graphd_events.push_back(event);
      });

  // Register storaged - only storaged watch should fire
  ServiceInfo storaged = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(storaged).ok());

  EXPECT_EQ(storaged_events.size(), 1);
  EXPECT_EQ(graphd_events.size(), 0);

  // Register graphd - only graphd watch should fire
  ServiceInfo graphd = CreateServiceInfo("graphd-1", "graphd", "10.0.0.2", 50052);
  EXPECT_TRUE(registry_->Register(graphd).ok());

  EXPECT_EQ(storaged_events.size(), 1);
  EXPECT_EQ(graphd_events.size(), 1);

  EXPECT_TRUE(registry_->Unwatch(watch1.value()).ok());
  EXPECT_TRUE(registry_->Unwatch(watch2.value()).ok());
}

TEST_F(ServiceRegistryTest, UnwatchNotFound) {
  auto status = registry_->Unwatch(9999);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsNotFound());
}

// =============================================================================
// Health Check Tests
// =============================================================================

TEST_F(ServiceRegistryTest, StartAndStopHealthCheck) {
  EXPECT_FALSE(registry_->IsHealthCheckRunning());

  auto status = registry_->StartHealthCheck(100, 500);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(registry_->IsHealthCheckRunning());

  registry_->StopHealthCheck();
  EXPECT_FALSE(registry_->IsHealthCheckRunning());
}

TEST_F(ServiceRegistryTest, StopWakesHealthCheckThreadPromptly) {
  EXPECT_TRUE(registry_->StartHealthCheck(60000, 120000).ok());

  auto start = std::chrono::steady_clock::now();
  registry_->StopHealthCheck();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
  EXPECT_FALSE(registry_->IsHealthCheckRunning());
}

TEST_F(ServiceRegistryTest, HealthCheckInvalidArguments) {
  EXPECT_TRUE(registry_->StartHealthCheck(0, 500).IsInvalidArgument());
  EXPECT_TRUE(registry_->StartHealthCheck(-1, 500).IsInvalidArgument());
  EXPECT_TRUE(registry_->StartHealthCheck(100, 0).IsInvalidArgument());
  EXPECT_TRUE(registry_->StartHealthCheck(100, -1).IsInvalidArgument());
}

TEST_F(ServiceRegistryTest, HealthCheckAlreadyRunning) {
  EXPECT_TRUE(registry_->StartHealthCheck(100, 500).ok());
  EXPECT_TRUE(registry_->StartHealthCheck(100, 500).IsBusy());
}

TEST_F(ServiceRegistryTest, HealthCheckMarksStaleServicesUnhealthy) {
  // Start health check with short interval and threshold
  EXPECT_TRUE(registry_->StartHealthCheck(50, 100).ok());

  // Register a service
  ServiceInfo info = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  EXPECT_TRUE(registry_->Register(info).ok());
  EXPECT_TRUE(registry_->Heartbeat("storaged-1").ok());

  // Verify service is healthy
  auto result = registry_->GetService("storaged-1");
  EXPECT_EQ(result.value().status, ServiceStatus::kHealthy);

  // Wait for health check to mark it stale
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Verify service is now unhealthy
  result = registry_->GetService("storaged-1");
  EXPECT_EQ(result.value().status, ServiceStatus::kUnhealthy);
}

// =============================================================================
// Statistics Tests
// =============================================================================

TEST_F(ServiceRegistryTest, GetServiceCounts) {
  EXPECT_EQ(registry_->GetServiceCount(), 0);
  EXPECT_EQ(registry_->GetHealthyServiceCount(), 0);

  // Register services
  ServiceInfo info1 = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  ServiceInfo info2 = CreateServiceInfo("storaged-2", "storaged", "10.0.0.2", 50051);
  ServiceInfo info3 = CreateServiceInfo("graphd-1", "graphd", "10.0.0.3", 50053);

  EXPECT_TRUE(registry_->Register(info1).ok());
  EXPECT_TRUE(registry_->Register(info2).ok());
  EXPECT_TRUE(registry_->Register(info3).ok());

  EXPECT_EQ(registry_->GetServiceCount(), 3);
  EXPECT_EQ(registry_->GetServiceCountByName("storaged"), 2);
  EXPECT_EQ(registry_->GetServiceCountByName("graphd"), 1);
  EXPECT_EQ(registry_->GetServiceCountByName("metad"), 0);

  // Initially no healthy services (status is Starting)
  EXPECT_EQ(registry_->GetHealthyServiceCount(), 0);

  // Send heartbeats to make them healthy
  EXPECT_TRUE(registry_->Heartbeat("storaged-1").ok());
  EXPECT_TRUE(registry_->Heartbeat("storaged-2").ok());
  EXPECT_EQ(registry_->GetHealthyServiceCount(), 2);
}

// =============================================================================
// GetAllServices and GetServicesByStatus Tests
// =============================================================================

TEST_F(ServiceRegistryTest, GetAllServices) {
  ServiceInfo info1 = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  ServiceInfo info2 = CreateServiceInfo("graphd-1", "graphd", "10.0.0.2", 50052);

  EXPECT_TRUE(registry_->Register(info1).ok());
  EXPECT_TRUE(registry_->Register(info2).ok());

  auto result = registry_->GetAllServices();
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().size(), 2);
}

TEST_F(ServiceRegistryTest, GetServicesByStatus) {
  ServiceInfo info1 = CreateServiceInfo("storaged-1", "storaged", "10.0.0.1", 50051);
  ServiceInfo info2 = CreateServiceInfo("storaged-2", "storaged", "10.0.0.2", 50051);
  ServiceInfo info3 = CreateServiceInfo("storaged-3", "storaged", "10.0.0.3", 50051);

  EXPECT_TRUE(registry_->Register(info1).ok());
  EXPECT_TRUE(registry_->Register(info2).ok());
  EXPECT_TRUE(registry_->Register(info3).ok());

  // Initially all starting
  auto result = registry_->GetServicesByStatus(ServiceStatus::kStarting);
  EXPECT_EQ(result.value().size(), 3);

  // Make one healthy
  EXPECT_TRUE(registry_->Heartbeat("storaged-1").ok());

  result = registry_->GetServicesByStatus(ServiceStatus::kHealthy);
  EXPECT_EQ(result.value().size(), 1);
  EXPECT_EQ(result.value()[0].id, "storaged-1");

  // Make one unhealthy
  EXPECT_TRUE(registry_->UpdateStatus("storaged-2", ServiceStatus::kUnhealthy).ok());

  result = registry_->GetServicesByStatus(ServiceStatus::kUnhealthy);
  EXPECT_EQ(result.value().size(), 1);
  EXPECT_EQ(result.value()[0].id, "storaged-2");
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
