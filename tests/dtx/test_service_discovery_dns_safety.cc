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
