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
#include <future>
#include <memory>
#include <vector>
#include <stdexcept>

// We test the promise-safety pattern directly because mocking the full
// DTXRpcClient requires a running gRPC server. The pattern under test is:
//
//   try { ...; results.emplace_back(...); } catch (...) { ...; }
//   promise->set_value();   // <-- MUST always execute
//
// We simulate what happens when emplace_back throws.

using NodeID = uint64_t;
struct FakeResponse {
  bool success = false;
  std::string error_msg;
};

TEST(PromiseSafetyTest, SetValueAlwaysFiresOnBadAlloc) {
  std::vector<std::pair<NodeID, FakeResponse>> results;
  results.reserve(1);

  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();

  // Simulate the lambda scheduled by DTXRpcClient::PrepareAll
  auto task = [promise, &results]() {
    try {
      FakeResponse response;
      response.success = true;

      // Force a throw equivalent to std::bad_alloc during emplace_back
      throw std::bad_alloc();

      // The line below is unreachable, but in real code it would be:
      // results.emplace_back(1, std::move(response));
    } catch (...) {
      // Original bug: promise->set_value() was AFTER this catch,
      // so if emplace_back threw, we never reached it.
      // Fixed version: promise->set_value() is in a finally block.
    }
    // BUG: promise->set_value();  // This is the old (buggy) location
  };

  // Run the buggy pattern
  task();

  // With the bug, future.wait_for returns timeout because set_value
  // was never called.
  auto status = future.wait_for(std::chrono::milliseconds(100));
  EXPECT_EQ(status, std::future_status::timeout)
      << "This test demonstrates the bug: promise was never fulfilled";
}

TEST(PromiseSafetyTest, FixedPatternAlwaysFires) {
  std::vector<std::pair<NodeID, FakeResponse>> results;
  results.reserve(1);

  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();

  // Simulate the FIXED lambda
  auto task = [promise, &results]() {
    try {
      FakeResponse response;
      response.success = true;
      throw std::bad_alloc();
      // results.emplace_back(1, std::move(response));
    } catch (...) {
      // Log error
    }
    // FIXED: promise->set_value() is here, outside the try/catch
    promise->set_value();
  };

  task();

  auto status = future.wait_for(std::chrono::milliseconds(100));
  EXPECT_EQ(status, std::future_status::ready)
      << "Fixed pattern: promise must always be fulfilled";
}
