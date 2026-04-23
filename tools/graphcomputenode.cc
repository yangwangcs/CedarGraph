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

#include <csignal>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include <gflags/gflags.h>

#include "cedar/gcn/gcn_node.h"

std::atomic<bool> g_shutdown{false};

void SignalHandler(int sig) {
  std::cout << "\n[GCN] Received signal " << sig << ", shutting down..." << std::endl;
  g_shutdown = true;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  cedar::GcnNode node;
  if (!node.Initialize().ok()) {
    std::cerr << "[GCN] Failed to initialize" << std::endl;
    return 1;
  }

  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  node.Start();

  std::cout << "[GCN] Running. Press Ctrl+C to stop." << std::endl;

  // Block until a shutdown signal is received
  while (!g_shutdown.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  node.Stop();
  std::cout << "[GCN] Stopped." << std::endl;
  return 0;
}
