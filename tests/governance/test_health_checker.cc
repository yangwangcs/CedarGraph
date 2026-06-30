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
#include <arpa/inet.h>
#include <chrono>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "cedar/governance/health_checker.h"

using namespace cedar::governance;

// =============================================================================
// Test Fixture
// =============================================================================

class HealthCheckerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    checker_ = std::make_unique<HealthChecker>();
  }

  void TearDown() override {
    checker_->Stop();
    checker_->StopHttpEndpoint();
    checker_->Clear();
    checker_.reset();
  }

  std::unique_ptr<HealthChecker> checker_;
};

// =============================================================================
// Helper Functions
// =============================================================================

// Simple HTTP client for testing
class SimpleHttpClient {
 public:
  struct Response {
    int status_code;
    std::string body;
    bool success;
  };

  static Response Get(const std::string& host, int port, const std::string& path) {
    Response response = {0, "", false};

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      return response;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
      close(sock);
      return response;
    }

    std::string request = "GET " + path + " HTTP/1.1\r\n";
    request += "Host: " + host + ":" + std::to_string(port) + "\r\n";
    request += "Connection: close\r\n";
    request += "\r\n";

    if (send(sock, request.c_str(), request.length(), 0) < 0) {
      close(sock);
      return response;
    }

    char buffer[4096];
    std::string raw_response;
    ssize_t received;
    while ((received = recv(sock, buffer, sizeof(buffer) - 1, 0)) > 0) {
      buffer[received] = '\0';
      raw_response += buffer;
    }

    close(sock);

    // Parse status code
    size_t status_pos = raw_response.find("HTTP/1.1 ");
    if (status_pos != std::string::npos) {
      status_pos += 9;
      size_t status_end = raw_response.find(" ", status_pos);
      if (status_end != std::string::npos) {
        response.status_code = std::stoi(raw_response.substr(status_pos, status_end - status_pos));
      }
    }

    // Parse body (after \r\n\r\n)
    size_t body_pos = raw_response.find("\r\n\r\n");
    if (body_pos != std::string::npos) {
      response.body = raw_response.substr(body_pos + 4);
    }

    response.success = true;
    return response;
  }
};

// =============================================================================
// Component Registration Tests
// =============================================================================

TEST_F(HealthCheckerTest, RegisterComponent) {
  auto status = checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  });
  
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(checker_->GetComponentCount(), 1);
  EXPECT_TRUE(checker_->IsComponentRegistered("storage"));
}

TEST_F(HealthCheckerTest, RegisterComponentWithMessage) {
  auto status = checker_->RegisterComponent(
    "storage",
    []() { return HealthStatus::kHealthy; },
    []() { return "Storage is operational"; }
  );
  
  EXPECT_TRUE(status.ok()) << status.ToString();
  
  auto result = checker_->CheckComponent("storage");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().status, HealthStatus::kHealthy);
  EXPECT_EQ(result.value().message, "Storage is operational");
}

TEST_F(HealthCheckerTest, RegisterDuplicateComponentFails) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());

  auto status = checker_->RegisterComponent("storage", []() {
    return HealthStatus::kDegraded;
  });
  
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsConflict());
}

TEST_F(HealthCheckerTest, RegisterEmptyNameFails) {
  auto status = checker_->RegisterComponent("", []() {
    return HealthStatus::kHealthy;
  });
  
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument());
}

TEST_F(HealthCheckerTest, RegisterNullFunctionFails) {
  HealthChecker::HealthCheckFunc null_func;
  auto status = checker_->RegisterComponent("storage", null_func);
  
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument());
}

TEST_F(HealthCheckerTest, UnregisterComponent) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_EQ(checker_->GetComponentCount(), 1);
  
  auto status = checker_->UnregisterComponent("storage");
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(checker_->GetComponentCount(), 0);
  EXPECT_FALSE(checker_->IsComponentRegistered("storage"));
}

TEST_F(HealthCheckerTest, UnregisterNonExistentComponent) {
  auto status = checker_->UnregisterComponent("nonexistent");
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsNotFound());
}

// =============================================================================
// Component Health Check Tests
// =============================================================================

TEST_F(HealthCheckerTest, ComponentHealthCheck) {
  // Register a component that returns healthy
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());

  // Check the component
  auto result = checker_->CheckComponent("storage");
  EXPECT_TRUE(result.ok()) << result.status().ToString();
  
  auto health = result.value();
  EXPECT_EQ(health.name, "storage");
  EXPECT_EQ(health.status, HealthStatus::kHealthy);
  EXPECT_GT(health.last_check_time, 0);
}

TEST_F(HealthCheckerTest, ComponentHealthCheckReturnsCorrectStatus) {
  // Register components with different statuses
  EXPECT_TRUE(checker_->RegisterComponent("healthy_comp", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("degraded_comp", []() {
    return HealthStatus::kDegraded;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("unhealthy_comp", []() {
    return HealthStatus::kUnhealthy;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("starting_comp", []() {
    return HealthStatus::kStarting;
  }).ok());

  // Verify each component returns correct status
  auto healthy = checker_->CheckComponent("healthy_comp");
  EXPECT_EQ(healthy.value().status, HealthStatus::kHealthy);
  
  auto degraded = checker_->CheckComponent("degraded_comp");
  EXPECT_EQ(degraded.value().status, HealthStatus::kDegraded);
  
  auto unhealthy = checker_->CheckComponent("unhealthy_comp");
  EXPECT_EQ(unhealthy.value().status, HealthStatus::kUnhealthy);
  
  auto starting = checker_->CheckComponent("starting_comp");
  EXPECT_EQ(starting.value().status, HealthStatus::kStarting);
}

TEST_F(HealthCheckerTest, CheckComponentNotFound) {
  auto result = checker_->CheckComponent("nonexistent");
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsNotFound());
}

TEST_F(HealthCheckerTest, CheckComponentMeasuresDuration) {
  EXPECT_TRUE(checker_->RegisterComponent("slow_comp", []() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return HealthStatus::kHealthy;
  }).ok());

  auto result = checker_->CheckComponent("slow_comp");
  EXPECT_TRUE(result.ok());
  EXPECT_GE(result.value().check_duration_ms, 10);
}

// =============================================================================
// Overall Health Tests
// =============================================================================

TEST_F(HealthCheckerTest, OverallHealthUnknownWhenEmpty) {
  EXPECT_EQ(checker_->GetOverallHealth(), HealthStatus::kUnknown);
  EXPECT_FALSE(checker_->IsHealthy());
  EXPECT_FALSE(checker_->IsReady());
}

TEST_F(HealthCheckerTest, OverallHealthSingleComponent) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());

  // Force a check to update health
  checker_->ForceCheck();
  
  EXPECT_EQ(checker_->GetOverallHealth(), HealthStatus::kHealthy);
  EXPECT_TRUE(checker_->IsHealthy());
  EXPECT_TRUE(checker_->IsReady());
}

TEST_F(HealthCheckerTest, OverallHealthWorstStatusWins) {
  // Register multiple components with different statuses
  EXPECT_TRUE(checker_->RegisterComponent("healthy", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("degraded", []() {
    return HealthStatus::kDegraded;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("unhealthy", []() {
    return HealthStatus::kUnhealthy;
  }).ok());

  checker_->ForceCheck();
  
  // Overall should be Unhealthy (worst of all)
  EXPECT_EQ(checker_->GetOverallHealth(), HealthStatus::kUnhealthy);
  EXPECT_FALSE(checker_->IsHealthy());
  EXPECT_FALSE(checker_->IsReady());
}

TEST_F(HealthCheckerTest, OverallHealthDegraded) {
  EXPECT_TRUE(checker_->RegisterComponent("healthy", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("degraded", []() {
    return HealthStatus::kDegraded;
  }).ok());

  checker_->ForceCheck();
  
  // Overall should be Degraded (worst of Healthy, Degraded)
  EXPECT_EQ(checker_->GetOverallHealth(), HealthStatus::kDegraded);
  EXPECT_FALSE(checker_->IsHealthy());
  EXPECT_TRUE(checker_->IsReady());  // Ready allows Degraded
}

TEST_F(HealthCheckerTest, OverallHealthWithMessage) {
  EXPECT_TRUE(checker_->RegisterComponent("comp1", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("comp2", []() {
    return HealthStatus::kHealthy;
  }).ok());

  checker_->ForceCheck();
  
  auto [status, message] = checker_->GetOverallHealthWithMessage();
  EXPECT_EQ(status, HealthStatus::kHealthy);
  EXPECT_NE(message.find("2 healthy"), std::string::npos);
}

TEST_F(HealthCheckerTest, OverallHealthAllHealthy) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("network", []() {
    return HealthStatus::kHealthy;
  }).ok());

  checker_->ForceCheck();
  
  EXPECT_EQ(checker_->GetOverallHealth(), HealthStatus::kHealthy);
  EXPECT_TRUE(checker_->IsHealthy());
  EXPECT_TRUE(checker_->IsReady());
}

// =============================================================================
// Multiple Components Tests
// =============================================================================

TEST_F(HealthCheckerTest, CheckAllComponents) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_TRUE(checker_->RegisterComponent("network", []() {
    return HealthStatus::kDegraded;
  }).ok());

  auto result = checker_->CheckAllComponents();
  EXPECT_TRUE(result.ok());
  
  auto healths = result.value();
  EXPECT_EQ(healths.size(), 2);
}

TEST_F(HealthCheckerTest, GetAllHealthWithoutRunningCheck) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());

  // GetAllHealth returns cached health without running check
  auto result = checker_->GetAllHealth();
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().size(), 1);
  // Status should be Unknown since no check was run
  EXPECT_EQ(result.value()[0].status, HealthStatus::kUnknown);
}

// =============================================================================
// Background Check Tests
// =============================================================================

TEST_F(HealthCheckerTest, StartAndStopBackgroundChecks) {
  EXPECT_FALSE(checker_->IsRunning());
  
  auto status = checker_->Start(100);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_TRUE(checker_->IsRunning());
  EXPECT_EQ(checker_->GetCheckIntervalMs(), 100);
  
  checker_->Stop();
  EXPECT_FALSE(checker_->IsRunning());
}

TEST_F(HealthCheckerTest, StopWakesBackgroundThreadPromptly) {
  EXPECT_TRUE(checker_->Start(60000).ok());

  auto start = std::chrono::steady_clock::now();
  checker_->Stop();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
  EXPECT_FALSE(checker_->IsRunning());
}

TEST_F(HealthCheckerTest, StartBackgroundInvalidInterval) {
  EXPECT_TRUE(checker_->Start(0).IsInvalidArgument());
  EXPECT_TRUE(checker_->Start(-1).IsInvalidArgument());
}

TEST_F(HealthCheckerTest, StartBackgroundAlreadyRunning) {
  EXPECT_TRUE(checker_->Start(100).ok());
  EXPECT_TRUE(checker_->Start(100).IsBusy());
}

TEST_F(HealthCheckerTest, BackgroundCheckUpdatesHealth) {
  std::atomic<HealthStatus> current_status{HealthStatus::kUnknown};
  
  EXPECT_TRUE(checker_->RegisterComponent("dynamic", [&current_status]() {
    return current_status.load();
  }).ok());

  // Start with Starting status
  current_status = HealthStatus::kStarting;
  
  // Start background checks
  EXPECT_TRUE(checker_->Start(50).ok());
  
  // Wait for first check
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  // Health should be Starting
  auto health = checker_->GetComponentHealth("dynamic");
  EXPECT_TRUE(health.ok());
  EXPECT_EQ(health.value().status, HealthStatus::kStarting);
  
  // Change to Healthy
  current_status = HealthStatus::kHealthy;
  
  // Wait for next check
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  // Health should be Healthy
  health = checker_->GetComponentHealth("dynamic");
  EXPECT_TRUE(health.ok());
  EXPECT_EQ(health.value().status, HealthStatus::kHealthy);
}

TEST_F(HealthCheckerTest, ForceCheck) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());

  // Before force check, status should be Unknown
  auto health = checker_->GetComponentHealth("storage");
  EXPECT_EQ(health.value().status, HealthStatus::kUnknown);
  
  // Force check
  checker_->ForceCheck();
  
  // After force check, status should be Healthy
  health = checker_->GetComponentHealth("storage");
  EXPECT_EQ(health.value().status, HealthStatus::kHealthy);
}

// =============================================================================
// Watch/Callback Tests
// =============================================================================

TEST_F(HealthCheckerTest, WatchAllChanges) {
  std::vector<HealthChangeEvent> events;
  
  auto watch_result = checker_->Watch([&events](const HealthChangeEvent& event) {
    events.push_back(event);
  });
  EXPECT_TRUE(watch_result.ok());
  int64_t watch_id = watch_result.value();

  // Register a component - should trigger ComponentAdded
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());

  EXPECT_GE(events.size(), 1);
  EXPECT_EQ(events[0].type, HealthChangeType::kComponentAdded);
  EXPECT_EQ(events[0].component_name, "storage");

  // Run a check - should trigger StatusChanged
  checker_->ForceCheck();
  
  // Find the StatusChanged event
  bool found_status_change = false;
  for (const auto& event : events) {
    if (event.type == HealthChangeType::kStatusChanged) {
      found_status_change = true;
      EXPECT_EQ(event.component_name, "storage");
      EXPECT_EQ(event.old_status, HealthStatus::kUnknown);
      EXPECT_EQ(event.new_status, HealthStatus::kHealthy);
      break;
    }
  }
  EXPECT_TRUE(found_status_change);

  // Unwatch
  EXPECT_TRUE(checker_->Unwatch(watch_id).ok());
}

TEST_F(HealthCheckerTest, WatchSpecificComponent) {
  std::vector<HealthChangeEvent> storage_events;
  std::vector<HealthChangeEvent> network_events;
  
  auto storage_watch = checker_->WatchComponent("storage", 
    [&storage_events](const HealthChangeEvent& event) {
      storage_events.push_back(event);
    });
  EXPECT_TRUE(storage_watch.ok());
  
  auto network_watch = checker_->WatchComponent("network",
    [&network_events](const HealthChangeEvent& event) {
      network_events.push_back(event);
    });
  EXPECT_TRUE(network_watch.ok());

  // Register storage - only storage watch should fire
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_GE(storage_events.size(), 1);
  EXPECT_EQ(network_events.size(), 0);

  // Register network - only network watch should fire
  EXPECT_TRUE(checker_->RegisterComponent("network", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_GE(storage_events.size(), 1);
  EXPECT_GE(network_events.size(), 1);

  // Cleanup
  checker_->Unwatch(storage_watch.value());
  checker_->Unwatch(network_watch.value());
}

TEST_F(HealthCheckerTest, UnwatchNotFound) {
  auto status = checker_->Unwatch(9999);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsNotFound());
}

TEST_F(HealthCheckerTest, WatchInvalidArguments) {
  HealthChangeCallback null_callback;
  
  auto result = checker_->Watch(null_callback);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalidArgument());
  
  auto result2 = checker_->WatchComponent("storage", null_callback);
  EXPECT_FALSE(result2.ok());
  EXPECT_TRUE(result2.status().IsInvalidArgument());
  
  auto result3 = checker_->WatchComponent("", [](const HealthChangeEvent&) {});
  EXPECT_FALSE(result3.ok());
  EXPECT_TRUE(result3.status().IsInvalidArgument());
}

// =============================================================================
// HTTP Endpoint Tests
// =============================================================================

TEST_F(HealthCheckerTest, HTTPHealthEndpoint) {
  // Register a healthy component
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  checker_->ForceCheck();
  
  // Start HTTP endpoint
  auto status = checker_->StartHttpEndpoint("127.0.0.1", 18080);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_TRUE(checker_->IsHttpEndpointRunning());
  EXPECT_EQ(checker_->GetHttpEndpointAddress(), "127.0.0.1:18080");
  
  // Give server time to start
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  // Make HTTP request to /health
  auto response = SimpleHttpClient::Get("127.0.0.1", 18080, "/health");
  EXPECT_TRUE(response.success);
  EXPECT_EQ(response.status_code, 200);
  EXPECT_NE(response.body.find("Healthy"), std::string::npos);
  
  // Stop HTTP endpoint
  checker_->StopHttpEndpoint();
  EXPECT_FALSE(checker_->IsHttpEndpointRunning());
}

TEST_F(HealthCheckerTest, HTTPHealthEndpointReturns503WhenUnhealthy) {
  // Register an unhealthy component
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kUnhealthy;
  }).ok());
  
  checker_->ForceCheck();
  
  // Start HTTP endpoint
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 18081).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  // Make HTTP request - should return 503
  auto response = SimpleHttpClient::Get("127.0.0.1", 18081, "/health");
  EXPECT_TRUE(response.success);
  EXPECT_EQ(response.status_code, 503);
  
  checker_->StopHttpEndpoint();
}

TEST_F(HealthCheckerTest, HTTPHealthEndpointReturns200WhenDegraded) {
  // Degraded status should still return 200 (service is functioning)
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kDegraded;
  }).ok());
  
  checker_->ForceCheck();
  
  // Start HTTP endpoint
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 18082).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  // Make HTTP request - should return 200
  auto response = SimpleHttpClient::Get("127.0.0.1", 18082, "/health");
  EXPECT_TRUE(response.success);
  EXPECT_EQ(response.status_code, 200);
  
  checker_->StopHttpEndpoint();
}

TEST_F(HealthCheckerTest, HTTPReadyEndpoint) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  checker_->ForceCheck();
  
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 18083).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  // Test /ready endpoint
  auto response = SimpleHttpClient::Get("127.0.0.1", 18083, "/ready");
  EXPECT_TRUE(response.success);
  EXPECT_EQ(response.status_code, 200);
  // Response is {"status":"ready"} when ready
  EXPECT_NE(response.body.find("\"status\""), std::string::npos);
  EXPECT_NE(response.body.find("ready"), std::string::npos);
  
  checker_->StopHttpEndpoint();
}

TEST_F(HealthCheckerTest, HTTPReadyEndpointReturns503WhenNotReady) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kUnhealthy;
  }).ok());
  
  checker_->ForceCheck();
  
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 18084).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  // Test /ready endpoint - should return 503
  auto response = SimpleHttpClient::Get("127.0.0.1", 18084, "/ready");
  EXPECT_TRUE(response.success);
  EXPECT_EQ(response.status_code, 503);
  
  checker_->StopHttpEndpoint();
}

TEST_F(HealthCheckerTest, HTTPInvalidArguments) {
  // Empty host
  EXPECT_TRUE(checker_->StartHttpEndpoint("", 8080).IsInvalidArgument());
  
  // Invalid port
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", -1).IsInvalidArgument());
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 0).IsInvalidArgument());
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 99999).IsInvalidArgument());
}

TEST_F(HealthCheckerTest, HTTPAlreadyRunning) {
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 18085).ok());
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 18086).IsBusy());
  checker_->StopHttpEndpoint();
}

// =============================================================================
// JSON Serialization Tests
// =============================================================================

TEST_F(HealthCheckerTest, ToJson) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  checker_->ForceCheck();
  
  std::string json = checker_->ToJson();
  
  // Verify JSON contains expected fields
  EXPECT_NE(json.find("\"overall\""), std::string::npos);
  EXPECT_NE(json.find("\"components\""), std::string::npos);
  EXPECT_NE(json.find("\"storage\""), std::string::npos);
  EXPECT_NE(json.find("\"Healthy\""), std::string::npos);
}

TEST_F(HealthCheckerTest, ToString) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  checker_->ForceCheck();
  
  std::string str = checker_->ToString();
  
  // Verify string contains expected content
  EXPECT_NE(str.find("Health Status:"), std::string::npos);
  EXPECT_NE(str.find("storage:"), std::string::npos);
  EXPECT_NE(str.find("Healthy"), std::string::npos);
}

// =============================================================================
// Helper Function Tests
// =============================================================================

TEST(HealthCheckerHelpers, HealthStatusToString) {
  EXPECT_EQ(HealthStatusToString(HealthStatus::kUnknown), "Unknown");
  EXPECT_EQ(HealthStatusToString(HealthStatus::kStarting), "Starting");
  EXPECT_EQ(HealthStatusToString(HealthStatus::kHealthy), "Healthy");
  EXPECT_EQ(HealthStatusToString(HealthStatus::kDegraded), "Degraded");
  EXPECT_EQ(HealthStatusToString(HealthStatus::kUnhealthy), "Unhealthy");
}

TEST(HealthCheckerHelpers, HealthStatusToHttpCode) {
  EXPECT_EQ(HealthStatusToHttpCode(HealthStatus::kUnknown), 503);
  EXPECT_EQ(HealthStatusToHttpCode(HealthStatus::kStarting), 503);
  EXPECT_EQ(HealthStatusToHttpCode(HealthStatus::kHealthy), 200);
  EXPECT_EQ(HealthStatusToHttpCode(HealthStatus::kDegraded), 200);
  EXPECT_EQ(HealthStatusToHttpCode(HealthStatus::kUnhealthy), 503);
}

TEST(HealthCheckerHelpers, WorseHealthStatus) {
  // Same status
  EXPECT_EQ(WorseHealthStatus(HealthStatus::kHealthy, HealthStatus::kHealthy), 
            HealthStatus::kHealthy);
  
  // Worse wins
  EXPECT_EQ(WorseHealthStatus(HealthStatus::kHealthy, HealthStatus::kUnhealthy), 
            HealthStatus::kUnhealthy);
  EXPECT_EQ(WorseHealthStatus(HealthStatus::kUnhealthy, HealthStatus::kHealthy), 
            HealthStatus::kUnhealthy);
  
  // Order matters
  EXPECT_EQ(WorseHealthStatus(HealthStatus::kDegraded, HealthStatus::kHealthy), 
            HealthStatus::kDegraded);
  EXPECT_EQ(WorseHealthStatus(HealthStatus::kHealthy, HealthStatus::kDegraded), 
            HealthStatus::kDegraded);
  
  // Unknown is worst than nothing
  EXPECT_EQ(WorseHealthStatus(HealthStatus::kUnknown, HealthStatus::kHealthy), 
            HealthStatus::kHealthy);
  EXPECT_EQ(WorseHealthStatus(HealthStatus::kHealthy, HealthStatus::kUnknown), 
            HealthStatus::kHealthy);
}

TEST(HealthCheckerHelpers, HealthChangeTypeToString) {
  EXPECT_EQ(HealthChangeTypeToString(HealthChangeType::kStatusChanged), "StatusChanged");
  EXPECT_EQ(HealthChangeTypeToString(HealthChangeType::kCheckPerformed), "CheckPerformed");
  EXPECT_EQ(HealthChangeTypeToString(HealthChangeType::kComponentAdded), "ComponentAdded");
  EXPECT_EQ(HealthChangeTypeToString(HealthChangeType::kComponentRemoved), "ComponentRemoved");
}

// =============================================================================
// Clear Tests
// =============================================================================

TEST_F(HealthCheckerTest, Clear) {
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());
  
  EXPECT_TRUE(checker_->Start(1000).ok());
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 18087).ok());
  
  EXPECT_EQ(checker_->GetComponentCount(), 1);
  EXPECT_TRUE(checker_->IsRunning());
  EXPECT_TRUE(checker_->IsHttpEndpointRunning());
  
  checker_->Clear();
  
  EXPECT_EQ(checker_->GetComponentCount(), 0);
  EXPECT_FALSE(checker_->IsRunning());
  EXPECT_FALSE(checker_->IsHttpEndpointRunning());
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
