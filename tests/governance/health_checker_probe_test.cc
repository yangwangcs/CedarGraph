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

class HealthCheckerProbeTest : public ::testing::Test {
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

TEST_F(HealthCheckerProbeTest, K8sProbeAliasesReturn200) {
  // Register a healthy component
  EXPECT_TRUE(checker_->RegisterComponent("storage", []() {
    return HealthStatus::kHealthy;
  }).ok());

  checker_->ForceCheck();

  // Start HTTP endpoint on a fixed test port
  EXPECT_TRUE(checker_->StartHttpEndpoint("127.0.0.1", 18095).ok());
  EXPECT_TRUE(checker_->IsHttpEndpointRunning());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Test /healthz alias
  auto healthz = SimpleHttpClient::Get("127.0.0.1", 18095, "/healthz");
  EXPECT_TRUE(healthz.success);
  EXPECT_EQ(healthz.status_code, 200);

  // Test /readyz alias
  auto readyz = SimpleHttpClient::Get("127.0.0.1", 18095, "/readyz");
  EXPECT_TRUE(readyz.success);
  EXPECT_EQ(readyz.status_code, 200);

  // Test original /health path still works
  auto health = SimpleHttpClient::Get("127.0.0.1", 18095, "/health");
  EXPECT_TRUE(health.success);
  EXPECT_EQ(health.status_code, 200);

  checker_->StopHttpEndpoint();
  EXPECT_FALSE(checker_->IsHttpEndpointRunning());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
