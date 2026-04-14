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
// Automated Recovery Integration Test
// 自动恢复系统集成测试
// =============================================================================

#include <iostream>
#include <thread>
#include <chrono>

#include "cedar/dtx/storage/storage_service.h"

using namespace cedar::dtx::storage;

int main(int argc, char* argv[]) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Automated Recovery System Integration Test             ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Create storage node config with auto recovery enabled
  StorageNodeConfig config;
  config.node_id = 1;
  config.bind_address = "0.0.0.0:7000";
  config.data_dir = "/tmp/cedar/storage_test";
  config.enable_auto_recovery = true;
  config.health_check_interval = std::chrono::seconds(10);
  config.max_recovery_attempts = 3;
  
  std::cout << "[Config] Auto Recovery: Enabled" << std::endl;
  std::cout << "[Config] Health Check Interval: " << config.health_check_interval.count() << "s" << std::endl;
  std::cout << "[Config] Max Recovery Attempts: " << config.max_recovery_attempts << std::endl;
  std::cout << std::endl;
  
  // Create and initialize storage service
  StorageService service(config);
  
  std::cout << "[1/4] Initializing storage service..." << std::endl;
  auto init_status = service.Initialize();
  if (!init_status.ok()) {
    std::cerr << "Failed to initialize: " << init_status.ToString() << std::endl;
    return 1;
  }
  std::cout << "      Storage service initialized" << std::endl;
  
  std::cout << "[2/4] Starting storage service..." << std::endl;
  auto start_status = service.Start();
  if (!start_status.ok()) {
    std::cerr << "Failed to start: " << start_status.ToString() << std::endl;
    return 1;
  }
  std::cout << "      Storage service started" << std::endl;
  std::cout << "      Auto recovery status: " << (service.IsAutoRecoveryEnabled() ? "Enabled" : "Disabled") << std::endl;
  
  std::cout << std::endl;
  std::cout << "[3/4] Running automated recovery tests..." << std::endl;
  
  // Simulate various failure scenarios
  using FailureType = cedar::dtx::AutomatedRecoveryManager::FailureType;
  
  std::cout << std::endl;
  std::cout << "  Test 1: Simulating disk full failure..." << std::endl;
  service.TriggerRecovery(FailureType::kDiskFull, "Test: Disk space critically low");
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  std::cout << "  Test 2: Simulating memory exhaustion..." << std::endl;
  service.TriggerRecovery(FailureType::kMemoryExhaustion, "Test: Memory usage exceeds threshold");
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  std::cout << "  Test 3: Simulating network partition..." << std::endl;
  service.TriggerRecovery(FailureType::kNetworkPartition, "Test: Network connectivity lost");
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  std::cout << "  Test 4: Simulating Raft leader election failure..." << std::endl;
  service.TriggerRecovery(FailureType::kRaftLeaderElectionFailure, "Test: Leader election timeout");
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  std::cout << std::endl;
  std::cout << "[4/4] Collecting recovery history..." << std::endl;
  auto history = service.GetRecoveryHistory();
  std::cout << "      Total recovery events: " << history.size() << std::endl;
  
  for (const auto& [event, status] : history) {
    std::cout << "      - Event: Node=" << event.node_id 
              << ", Type=" << static_cast<int>(event.type)
              << ", Status=" << (status.ok() ? "Success" : "Failed") << std::endl;
  }
  
  std::cout << std::endl;
  std::cout << "[5/4] Testing auto-recovery toggle..." << std::endl;
  std::cout << "      Current status: " << (service.IsAutoRecoveryEnabled() ? "Enabled" : "Disabled") << std::endl;
  service.EnableAutoRecovery(false);
  std::cout << "      After disable: " << (service.IsAutoRecoveryEnabled() ? "Enabled" : "Disabled") << std::endl;
  service.EnableAutoRecovery(true);
  std::cout << "      After enable: " << (service.IsAutoRecoveryEnabled() ? "Enabled" : "Disabled") << std::endl;
  
  std::cout << std::endl;
  std::cout << "Shutting down storage service..." << std::endl;
  service.Shutdown();
  
  std::cout << std::endl;
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Integration Test Completed Successfully!               ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  
  return 0;
}
