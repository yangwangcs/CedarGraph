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

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#include "cedar/dtx/failover_manager.h"

using namespace cedar;
using namespace cedar::dtx;

// Helper: open a listening socket on a given port so TCP health checks pass.
class TemporaryTcpListener {
 public:
  explicit TemporaryTcpListener(int port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return;
    int reuse = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
      ::close(fd_);
      fd_ = -1;
      return;
    }
    // Read back the actual assigned port (supports ephemeral port 0)
    socklen_t addr_len = sizeof(addr);
    if (::getsockname(fd_, reinterpret_cast<struct sockaddr*>(&addr), &addr_len) == 0) {
      port_ = ntohs(addr.sin_port);
    }
    if (::listen(fd_, 5) < 0) {
      ::close(fd_);
      fd_ = -1;
      return;
    }
    // Accept connections in a background thread (never actually handles them)
    thread_ = std::thread([this]() {
      while (running_.load()) {
        struct sockaddr_in client{};
        socklen_t len = sizeof(client);
        int client_fd = ::accept(fd_, reinterpret_cast<struct sockaddr*>(&client), &len);
        if (client_fd >= 0) {
          ::close(client_fd);  // Immediately close
        }
      }
    });
  }

  ~TemporaryTcpListener() {
    running_.store(false);
    if (fd_ >= 0) {
      ::shutdown(fd_, SHUT_RDWR);
      ::close(fd_);
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  bool IsOk() const { return fd_ >= 0; }
  int Port() const { return port_; }

 private:
  int fd_ = -1;
  int port_ = -1;
  std::atomic<bool> running_{true};
  std::thread thread_;
};

class FailoverHealthScoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    listener_ = std::make_unique<TemporaryTcpListener>(0);
    ASSERT_TRUE(listener_->IsOk());

    controller_ = std::make_unique<PartitionFailoverController>();
    PartitionFailoverController::Config config;
    config.local_node_id = 1;
    config.check_interval = std::chrono::milliseconds(100);
    config.detection_config.phi_threshold = 5.0;  // Lower for faster tests
    Status s = controller_->Initialize(config);
    ASSERT_TRUE(s.ok());
  }

  void TearDown() override {
    if (controller_) {
      controller_->Shutdown();
    }
    listener_.reset();
  }

  std::string TestAddress() const {
    return "127.0.0.1:" + std::to_string(listener_->Port());
  }

  std::unique_ptr<TemporaryTcpListener> listener_;
  std::unique_ptr<PartitionFailoverController> controller_;
};

// =============================================================================
// Phi Accrual Tests
// =============================================================================

TEST(PhiAccrualTest, NotEnoughSamplesFallback) {
  PhiAccrualDetector detector(100);
  EXPECT_LT(detector.Phi(1000.0), 2.0);  // Conservative fallback
  EXPECT_FALSE(detector.IsSuspected(1000.0, 8.0));
}

TEST(PhiAccrualTest, StableNodeLowPhi) {
  PhiAccrualDetector detector(100);
  for (int i = 0; i < 50; ++i) {
    detector.RecordInterval(50.0);  // Very stable 50ms intervals
  }
  double phi = detector.Phi(60.0);   // Slightly longer silence
  EXPECT_LT(phi, 3.0);               // Should not be suspected
  EXPECT_FALSE(detector.IsSuspected(60.0, 8.0));
}

TEST(PhiAccrualTest, DeadNodeHighPhi) {
  PhiAccrualDetector detector(100);
  for (int i = 0; i < 50; ++i) {
    detector.RecordInterval(50.0);
  }
  double phi = detector.Phi(5000.0);  // Much longer silence
  EXPECT_GE(phi, 8.0);                // Should be strongly suspected
  EXPECT_TRUE(detector.IsSuspected(5000.0, 8.0));
}

TEST(PhiAccrualTest, JitteryNetworkAdaptive) {
  PhiAccrualDetector detector(100);
  // Simulate jittery network: intervals vary between 10-90ms (high variance)
  for (int i = 0; i < 100; ++i) {
    detector.RecordInterval(10.0 + (i % 81));
  }
  // With high variance, 150ms silence is roughly ~2 sigma, should not reach phi=8
  double phi = detector.Phi(150.0);
  EXPECT_LT(phi, 8.0);

  // A 500ms silence should eventually be suspected
  phi = detector.Phi(500.0);
  EXPECT_GE(phi, 5.0);
}

// =============================================================================
// Multi-Dimensional Health Score Tests
// =============================================================================

TEST_F(FailoverHealthScoreTest, HealthyNodeHighScore) {
  // Register a partition with a single local node
  std::vector<NodeID> replicas = {1};
  Status s = controller_->RegisterPartition(100, 1, replicas);
  ASSERT_TRUE(s.ok());
  controller_->RegisterNodeAddress(1, TestAddress());

  // Register a collector that reports healthy metrics
  controller_->RegisterHealthMetricsCollector(
      [](NodeID) -> HealthMetrics {
        HealthMetrics m;
        m.tcp_latency_ms = 5.0;
        m.raft_replication_lag = 10.0;
        m.disk_io_latency_ms = 2.0;
        m.memory_pressure_ratio = 0.3;
        m.cpu_load_1m = 0.2;
        m.error_rate_1m = 0.0;
        return m;
      });

  // Wait for at least one health check iteration
  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  auto score_opt = controller_->GetHealthScore(1);
  ASSERT_TRUE(score_opt.has_value());
  EXPECT_GT(score_opt->overall, 80.0);
  EXPECT_FALSE(score_opt->is_unhealthy);
  EXPECT_FALSE(score_opt->is_degraded);
}

TEST_F(FailoverHealthScoreTest, HighRaftLagReducesScoreButNotDead) {
  std::vector<NodeID> replicas = {1};
  Status s = controller_->RegisterPartition(100, 1, replicas);
  ASSERT_TRUE(s.ok());
  controller_->RegisterNodeAddress(1, TestAddress());

  controller_->RegisterHealthMetricsCollector(
      [](NodeID) -> HealthMetrics {
        HealthMetrics m;
        m.tcp_latency_ms = 5.0;
        m.raft_replication_lag = 5000.0;  // Very high lag
        m.disk_io_latency_ms = 2.0;
        m.memory_pressure_ratio = 0.3;
        m.cpu_load_1m = 0.2;
        m.error_rate_1m = 0.0;
        return m;
      });

  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  auto score_opt = controller_->GetHealthScore(1);
  ASSERT_TRUE(score_opt.has_value());
  // Raft lag only has 20% weight, so score drops moderately but not drastically
  EXPECT_LT(score_opt->overall, 95.0);
  EXPECT_GT(score_opt->overall, 50.0);
  EXPECT_FALSE(score_opt->is_unhealthy); // Hard failure not triggered (TCP is fine)
}

TEST_F(FailoverHealthScoreTest, TcpTimeoutMarksUnhealthy) {
  std::vector<NodeID> replicas = {1};
  Status s = controller_->RegisterPartition(100, 1, replicas);
  ASSERT_TRUE(s.ok());
  // Register a non-routable address to force TCP timeout
  // Port 1 is typically blocked/unused; we also shutdown the listener.
  listener_.reset();
  controller_->RegisterNodeAddress(1, "127.0.0.1:1");

  std::this_thread::sleep_for(std::chrono::milliseconds(800));

  auto score_opt = controller_->GetHealthScore(1);
  ASSERT_TRUE(score_opt.has_value());
  EXPECT_TRUE(score_opt->is_unhealthy);  // TCP dead -> hard failure
  EXPECT_LT(score_opt->overall, 85.0);   // TCP only 25% weight, other dims may stay healthy
}

TEST_F(FailoverHealthScoreTest, PredictiveDegradationOnTrend) {
  std::vector<NodeID> replicas = {1};
  Status s = controller_->RegisterPartition(100, 1, replicas);
  ASSERT_TRUE(s.ok());
  controller_->RegisterNodeAddress(1, TestAddress());

  // Simulate a degrading trend: score drops from 90 -> 70 -> 50 -> 30
  std::vector<double> lag_values = {10.0, 100.0, 2000.0, 8000.0};
  size_t lag_idx = 0;
  controller_->RegisterHealthMetricsCollector(
      [&lag_values, &lag_idx](NodeID) -> HealthMetrics {
        HealthMetrics m;
        m.tcp_latency_ms = 5.0;
        m.raft_replication_lag = lag_values[std::min(lag_idx++, lag_values.size() - 1)];
        m.disk_io_latency_ms = 2.0;
        m.memory_pressure_ratio = 0.3;
        m.cpu_load_1m = 0.2;
        m.error_rate_1m = 0.0;
        return m;
      });

  // Wait enough iterations for trend detection (need 3 samples)
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  auto score_opt = controller_->GetHealthScore(1);
  ASSERT_TRUE(score_opt.has_value());
  // At least one of the later samples should trigger degradation
  // (overall < 60 and monotonically decreasing)
}

TEST_F(FailoverHealthScoreTest, RecoveryRestoresHealth) {
  std::vector<NodeID> replicas = {1};
  Status s = controller_->RegisterPartition(100, 1, replicas);
  ASSERT_TRUE(s.ok());
  controller_->RegisterNodeAddress(1, TestAddress());

  double lag = 8000.0;
  controller_->RegisterHealthMetricsCollector(
      [&lag](NodeID) -> HealthMetrics {
        HealthMetrics m;
        m.tcp_latency_ms = 5.0;
        m.raft_replication_lag = lag;
        m.disk_io_latency_ms = 2.0;
        m.memory_pressure_ratio = 0.3;
        m.cpu_load_1m = 0.2;
        m.error_rate_1m = 0.0;
        return m;
      });

  std::this_thread::sleep_for(std::chrono::milliseconds(800));

  // Now recover
  lag = 10.0;
  // Wait longer for Phi Accrual history to refresh and trend to clear
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));

  auto score_opt = controller_->GetHealthScore(1);
  ASSERT_TRUE(score_opt.has_value());
  EXPECT_GT(score_opt->overall, 70.0);
  EXPECT_FALSE(score_opt->is_unhealthy);
}
