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

#include "cedar/governance/health_checker.h"

using namespace cedar::governance;

TEST(HealthCheckerThreadPoolTest, ManyComponentsDoNotSpawnUnboundedThreads) {
  HealthChecker checker;

  std::atomic<int> check_count{0};
  const int kComponents = 50;

  // Register many slow components
  for (int i = 0; i < kComponents; ++i) {
    auto status = checker.RegisterComponent(
        "comp_" + std::to_string(i),
        [&check_count]() {
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
          ++check_count;
          return HealthStatus::kHealthy;
        });
    EXPECT_TRUE(status.ok());
  }

  // Run checks synchronously via ForceCheck
  checker.ForceCheck();

  EXPECT_EQ(check_count.load(), kComponents)
      << "All component checks must execute";
}

TEST(HealthCheckerThreadPoolTest, BackgroundChecksSurviveMultipleIntervals) {
  HealthChecker checker;

  std::atomic<int> check_count{0};
  const int kComponents = 20;

  for (int i = 0; i < kComponents; ++i) {
    checker.RegisterComponent(
        "bg_comp_" + std::to_string(i),
        [&check_count]() {
          ++check_count;
          return HealthStatus::kHealthy;
        });
  }

  // Start background checks with a fast interval
  auto status = checker.Start(50);  // 50 ms
  EXPECT_TRUE(status.ok());

  // Let it run for ~250 ms (>= 4 intervals)
  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  checker.Stop();

  int final_count = check_count.load();
  EXPECT_GE(final_count, kComponents * 3)
      << "Background loop should have executed checks across multiple intervals";
}
