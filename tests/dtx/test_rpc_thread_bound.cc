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
// DTX RPC Client Thread Bound Test
// =============================================================================
// Verifies that PrepareAll / CommitAll / AbortAll use a bounded thread pool
// instead of spawning one OS thread per participant.
// =============================================================================

#include <gtest/gtest.h>
#include <mach/mach.h>
#include <vector>

#include "cedar/dtx/dtx_rpc_client.h"

using namespace cedar::dtx;

// Count current threads in this process (macOS)
static size_t CountCurrentThreads() {
  thread_act_array_t threads;
  mach_msg_type_number_t count = 0;
  kern_return_t kr = task_threads(mach_task_self(), &threads, &count);
  if (kr != KERN_SUCCESS) {
    return 0;
  }
  for (mach_msg_type_number_t i = 0; i < count; ++i) {
    mach_port_deallocate(mach_task_self(), threads[i]);
  }
  vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(threads),
                sizeof(thread_t) * count);
  return count;
}

TEST(DTXRpcClientThreadBound, PrepareAllDoesNotExceedBoundedThreads) {
  constexpr size_t kPoolSize = 8;
  constexpr int kParticipants = 1000;

  DTXRpcConfig config;
  config.max_rpc_threads = kPoolSize;
  config.rpc_timeout_ms = 100;  // Short timeout for faster failures
  DTXRpcClient client(config);

  std::vector<NodeID> participant_ids;
  participant_ids.reserve(kParticipants);
  for (int i = 0; i < kParticipants; ++i) {
    participant_ids.push_back(i);
    client.AddParticipant(i, "127.0.0.1:1");  // Invalid port, fails fast
  }

  size_t baseline = CountCurrentThreads();

  auto results = client.PrepareAll(
      participant_ids,
      "txn-test",
      "coordinator-test",
      1,
      {},
      {},
      1,
      50);

  size_t after = CountCurrentThreads();

  EXPECT_EQ(results.size(), kParticipants)
      << "All participants should have a result";
  EXPECT_LE(after, baseline + kPoolSize + 20)
      << "Thread count should stay bounded by pool size, not by participant count";
}

TEST(DTXRpcClientThreadBound, CommitAllDoesNotExceedBoundedThreads) {
  constexpr size_t kPoolSize = 8;
  constexpr int kParticipants = 1000;

  DTXRpcConfig config;
  config.max_rpc_threads = kPoolSize;
  config.rpc_timeout_ms = 100;
  DTXRpcClient client(config);

  std::vector<NodeID> participant_ids;
  participant_ids.reserve(kParticipants);
  for (int i = 0; i < kParticipants; ++i) {
    participant_ids.push_back(i);
    client.AddParticipant(i, "127.0.0.1:1");
  }

  size_t baseline = CountCurrentThreads();

  auto results = client.CommitAll(
      participant_ids,
      "txn-test",
      "coordinator-test",
      1);

  size_t after = CountCurrentThreads();

  EXPECT_EQ(results.size(), kParticipants);
  EXPECT_LE(after, baseline + kPoolSize + 20)
      << "Thread count should stay bounded by pool size";
}

TEST(DTXRpcClientThreadBound, AbortAllDoesNotExceedBoundedThreads) {
  constexpr size_t kPoolSize = 8;
  constexpr int kParticipants = 1000;

  DTXRpcConfig config;
  config.max_rpc_threads = kPoolSize;
  config.rpc_timeout_ms = 100;
  DTXRpcClient client(config);

  std::vector<NodeID> participant_ids;
  participant_ids.reserve(kParticipants);
  for (int i = 0; i < kParticipants; ++i) {
    participant_ids.push_back(i);
    client.AddParticipant(i, "127.0.0.1:1");
  }

  size_t baseline = CountCurrentThreads();

  auto results = client.AbortAll(
      participant_ids,
      "txn-test",
      "coordinator-test",
      "test-abort");

  size_t after = CountCurrentThreads();

  EXPECT_EQ(results.size(), kParticipants);
  EXPECT_LE(after, baseline + kPoolSize + 20)
      << "Thread count should stay bounded by pool size";
}
