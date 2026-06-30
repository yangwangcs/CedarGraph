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

// Cluster Test Tool for CedarGraph Distributed Storage
// Usage: ./cluster_test_tool --meta 127.0.0.1:10559 --timeout 30

#include <iostream>
#include <vector>
#include <string>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/governance/service_registry.h"

using namespace cedar;

struct TestConfig {
  std::vector<std::string> meta_endpoints;
  std::string data_root = "/tmp/cedar_cluster_test";
  int connection_timeout_sec = 30;
};

class ClusterTestTool {
 public:
  explicit ClusterTestTool(const TestConfig& config) : config_(config) {}
  
  bool TestConnection();
  void RunAllTests();
  
 private:
  TestConfig config_;
  CedarGraphStorage* storage_ = nullptr;
};

bool ClusterTestTool::TestConnection() {
  std::cout << "[TEST] Testing connection to cluster..." << std::endl;
  
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;
  options.meta_endpoints = config_.meta_endpoints;
  options.dtx_config.rpc_timeout_ms = config_.connection_timeout_sec * 1000;
  
  Status s = CedarGraphStorage::Open(options, config_.data_root, &storage_);
  if (!s.ok()) {
    std::cerr << "[FAIL] Connection failed: " << s.ToString() << std::endl;
    return false;
  }
  
  if (!storage_->IsDistributedMode()) {
    std::cerr << "[FAIL] Not in distributed mode" << std::endl;
    delete storage_;
    storage_ = nullptr;
    return false;
  }
  
  std::cout << "[PASS] Connected successfully" << std::endl;
  delete storage_;
  storage_ = nullptr;
  return true;
}

void ClusterTestTool::RunAllTests() {
  std::cout << "=== CedarGraph Cluster Test Tool ===" << std::endl;
  int passed = 0, failed = 0;
  if (TestConnection()) passed++; else failed++;
  std::cout << "=== Results: " << passed << " passed, " << failed << " failed ===" << std::endl;
}

int main(int argc, char** argv) {
  TestConfig config;
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--meta" && i + 1 < argc) {
      config.meta_endpoints.push_back(argv[++i]);
    } else if (arg == "--timeout" && i + 1 < argc) {
      config.connection_timeout_sec = std::stoi(argv[++i]);
    }
  }
  if (config.meta_endpoints.empty()) {
    config.meta_endpoints = {"127.0.0.1:10559"};
  }
  ClusterTestTool tool(config);
  tool.RunAllTests();
  return 0;
}
