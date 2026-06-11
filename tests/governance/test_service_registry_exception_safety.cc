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
#include <atomic>
#include <stdexcept>

#include "cedar/governance/service_registry.h"

using namespace cedar::governance;

TEST(ServiceRegistryExceptionSafetyTest, ThrowingWatcherDoesNotKillBackgroundLoop) {
  ServiceRegistry registry;

  std::atomic<int> good_callback_count{0};
  std::atomic<int> bad_callback_count{0};

  // Register a service
  ServiceInfo info;
  info.id = "svc-001";
  info.name = "test-service";
  info.host = "127.0.0.1";
  info.port = 8080;
  info.status = ServiceStatus::kHealthy;
  auto st = registry.Register(info);
  EXPECT_TRUE(st.ok());

  // Watch with a throwing callback
  auto watch_result = registry.Watch("test-service", [&bad_callback_count](const ServiceEvent&) {
    ++bad_callback_count;
    throw std::runtime_error("intentional watcher failure");
  });
  EXPECT_TRUE(watch_result.ok());

  // Watch with a good callback
  auto watch_result2 = registry.Watch("test-service", [&good_callback_count](const ServiceEvent&) {
    ++good_callback_count;
  });
  EXPECT_TRUE(watch_result2.ok());

  // Trigger an event by updating status
  auto update_status = registry.UpdateStatus("svc-001", ServiceStatus::kUnhealthy);
  EXPECT_TRUE(update_status.ok());

  // Give the registry a moment to process (callbacks are synchronous)
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  EXPECT_EQ(bad_callback_count.load(), 1)
      << "Throwing callback should have been invoked once";
  EXPECT_EQ(good_callback_count.load(), 1)
      << "Good callback should still be invoked even when prior callback throws";

  // Trigger another event to prove the background / notification path is still alive
  auto update_status2 = registry.UpdateStatus("svc-001", ServiceStatus::kHealthy);
  EXPECT_TRUE(update_status2.ok());

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  EXPECT_EQ(bad_callback_count.load(), 2);
  EXPECT_EQ(good_callback_count.load(), 2);
}
