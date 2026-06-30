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
#include <future>
#include <thread>

#include "cedar/dtx/service_discovery.h"

using namespace cedar::dtx;

TEST(ServiceDiscoveryDnsSafetyTest, SlowDnsDoesNotCrashAfterReturn) {
  ServiceDiscoveryConfig config;
  config.enable_docker_discovery = false;
  config.enable_dns_discovery = true;
  config.health_check_timeout_ms = 50;  // Very short timeout
  // Use a hostname that will force DNS resolution and likely time out
  config.dns_names = {"this-hostname-almost-certainly-does-not-exist-12345.local"};

  ServiceDiscovery discovery(config);

  StorageNodeInfo node;
  node.host = "another-nonexistent-host-67890.local";
  node.port = 9779;

  // This function returns quickly because the DNS thread is detached on timeout.
  // The bug was that the detached thread later accessed node.host.c_str()
  // after 'node' had gone out of scope.
  bool healthy = discovery.CheckNodeHealth(node);

  // We don't care about the result; we care that the process survives.
  (void)healthy;

  // Sleep long enough for the detached DNS thread to finish (or time out)
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // If the bug existed, ASan would flag a heap-use-after-free here
  // or the process would segfault.
  EXPECT_TRUE(true) << "Process survived the DNS timeout without UAF";
}

TEST(ServiceDiscoveryDnsSafetyTest, Ipv4LiteralSkipsDns) {
  ServiceDiscoveryConfig config;
  config.health_check_timeout_ms = 500;

  ServiceDiscovery discovery(config);

  StorageNodeInfo node;
  node.host = "127.0.0.1";
  node.port = 9;  // Discard port -- connection will fail but DNS is skipped

  bool healthy = discovery.CheckNodeHealth(node);

  // Connection to port 9 should fail, but the IPv4 fast-path should be taken.
  EXPECT_FALSE(healthy);
}

TEST(ServiceDiscoveryDnsSafetyTest, RejectsInvalidRuntimeConfig) {
  ServiceDiscoveryConfig config;
  config.storaged_port = 0;
  ServiceDiscovery invalid_port(config);
  EXPECT_TRUE(invalid_port.Initialize().IsInvalidArgument());

  config.storaged_port = 9779;
  config.health_check_interval_seconds = 0;
  ServiceDiscovery invalid_interval(config);
  EXPECT_TRUE(invalid_interval.Initialize().IsInvalidArgument());

  config.health_check_interval_seconds = 1;
  config.health_check_timeout_ms = 0;
  ServiceDiscovery invalid_timeout(config);
  EXPECT_TRUE(invalid_timeout.Initialize().IsInvalidArgument());
}

TEST(ServiceDiscoveryDnsSafetyTest, DiscoverViaDnsRespectsConfiguredTimeout) {
  ServiceDiscoveryConfig config;
  config.enable_docker_discovery = false;
  config.enable_dns_discovery = true;
  config.enable_consul_discovery = false;
  config.health_check_timeout_ms = 50;
  config.dns_names = {
      "this-hostname-almost-certainly-does-not-exist-cedar-timeout.local"};

  ServiceDiscovery discovery(config);
  ASSERT_TRUE(discovery.Initialize().ok());

  auto start = std::chrono::steady_clock::now();
  auto nodes = discovery.DiscoverNow();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(nodes.empty());
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

TEST(ServiceDiscoveryDnsSafetyTest, DiscoveryCallbackRunsOutsideCallbackMutex) {
  ServiceDiscoveryConfig config;
  config.enable_docker_discovery = false;
  config.enable_dns_discovery = true;
  config.dns_names = {"127.0.0.1"};
  config.storaged_port = 9;

  ServiceDiscovery discovery(config);

  std::promise<void> callback_entered;
  auto entered_future = callback_entered.get_future();
  discovery.SetNodeDiscoveredCallback(
      [&discovery, &callback_entered](const StorageNodeInfo&) {
        discovery.SetNodeDiscoveredCallback(nullptr);
        callback_entered.set_value();
      });

  auto future = std::async(std::launch::async, [&] {
    return discovery.DiscoverNow();
  });

  EXPECT_EQ(entered_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_EQ(future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_EQ(future.get().size(), 1u);
}

TEST(ServiceDiscoveryDnsSafetyTest, HealthCallbackRunsOutsideCallbackMutex) {
  ServiceDiscoveryConfig config;
  config.enable_docker_discovery = false;
  config.enable_dns_discovery = false;
  config.health_check_interval_seconds = 1;
  config.health_check_timeout_ms = 100;

  ServiceDiscovery discovery(config);
  ASSERT_TRUE(discovery.Initialize().ok());

  StorageNodeInfo node;
  node.host = "127.0.0.1";
  node.port = 9;
  node.is_healthy = true;

  std::promise<void> callback_entered;
  auto entered_future = callback_entered.get_future();
  discovery.SetNodeHealthChangedCallback(
      [&discovery, &callback_entered](const StorageNodeInfo&, bool) {
        discovery.SetNodeHealthChangedCallback(nullptr);
        callback_entered.set_value();
      });

  discovery.MergeNodesForTest({node});
  ASSERT_TRUE(discovery.Start().ok());

  EXPECT_EQ(entered_future.wait_for(std::chrono::seconds(3)),
            std::future_status::ready);
  discovery.Stop();
}
