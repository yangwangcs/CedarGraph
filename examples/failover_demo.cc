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

// examples/failover_demo.cc
// Demonstrates: EventBus + HealthMonitor + FailoverManager

#include <iostream>
#include <chrono>
#include <thread>
#include "cedar/storage/storage_health_monitor.h"
#include "cedar/storage/failover_manager.h"
#include "cedar/integration/event_bus.h"
#include "cedar/governance/health_checker.h"

using namespace cedar;
using namespace cedar::storage;

int main(int argc, char** argv) {
  std::cout << "=== CedarGraph Failover Demo ===" << std::endl;
  std::cout << "Demonstrates: EventBus + HealthMonitor + FailoverManager" << std::endl;
  std::cout << std::endl;
  
  // 1. Create EventBus
  auto event_bus = std::make_shared<integration::EventBus>();
  
  // Subscribe to storage health events
  event_bus->Subscribe("storage.node.health_changed", 
    [](const integration::Event& event) {
      std::string node_id = event.Get<std::string>("node_id");
      int old_status = event.Get<int>("old_status");
      int new_status = event.Get<int>("new_status");
      std::cout << "[EVENT] Node " << node_id 
                << " health changed: " << old_status
                << " -> " << new_status << std::endl;
    });
  
  // Subscribe to failover events
  event_bus->Subscribe("storage.failover.completed",
    [](const integration::Event& event) {
      std::string from_node = event.Get<std::string>("from_node");
      std::string to_node = event.Get<std::string>("to_node");
      std::cout << "[EVENT] Failover: " << from_node
                << " -> " << to_node << std::endl;
    });
  
  // 2. Create HealthChecker
  auto health_checker = std::make_shared<governance::HealthChecker>();
  
  // 3. Create StorageHealthMonitor
  auto health_monitor = std::make_shared<StorageHealthMonitor>();
  
  HealthMonitorConfig health_config;
  health_config.check_interval = std::chrono::seconds(3);
  health_config.failure_threshold = 2;
  health_config.success_threshold = 1;
  
  Status s = health_monitor->Initialize(health_config, health_checker, event_bus);
  if (!s.ok()) {
    std::cerr << "Failed to initialize health monitor: " << s.ToString() << std::endl;
    return 1;
  }
  
  // 4. Create FailoverManager
  auto failover_manager = std::make_shared<FailoverManager>();
  
  FailoverConfig failover_config;
  failover_config.enable_auto_failover = true;
  failover_config.enable_read_from_follower = true;
  
  s = failover_manager->Initialize(failover_config, health_monitor);
  if (!s.ok()) {
    std::cerr << "Failed to initialize failover manager: " << s.ToString() << std::endl;
    return 1;
  }
  
  // Set failover callback
  failover_manager->SetFailoverCallback(
    [&event_bus](const std::string& old_node, const std::string& new_node) {
      std::cout << "[FAILOVER] " << old_node << " -> " << new_node << std::endl;
      
      integration::Event event("storage.failover.completed");
      event.Set("from_node", old_node);
      event.Set("to_node", new_node);
      event.Set("source", std::string("FailoverManager"));
      event_bus->Publish(event);
    });
  
  failover_manager->SetNodeChangeCallback(
    [](const std::string& node_id, NodeRole old_role, NodeRole new_role) {
      std::cout << "[ROLE CHANGE] " << node_id << ": " 
                << NodeRoleToString(old_role) << " -> " 
                << NodeRoleToString(new_role) << std::endl;
    });
  
  // 5. Register nodes (simulate 3-node cluster)
  std::cout << "Registering nodes..." << std::endl;
  
  health_monitor->RegisterNode("storage-1", "127.0.0.1", 9779);
  health_monitor->RegisterNode("storage-2", "127.0.0.1", 9780);
  health_monitor->RegisterNode("storage-3", "127.0.0.1", 9781);
  
  failover_manager->RegisterNode("storage-1", "127.0.0.1:9779", NodeRole::kLeader);
  failover_manager->RegisterNode("storage-2", "127.0.0.1:9780", NodeRole::kFollower);
  failover_manager->RegisterNode("storage-3", "127.0.0.1:9781", NodeRole::kFollower);
  
  // 6. Start
  std::cout << "Starting monitoring..." << std::endl;
  health_monitor->Start();
  failover_manager->Start();
  
  auto leader_result = failover_manager->GetLeader();
  if (leader_result.ok()) {
    auto leader = leader_result.value();
    std::cout << "Initial Leader: " << leader.node_id << std::endl;
  }
  
  // 7. Main loop - display status
  std::cout << "\nPress Ctrl+C to exit. Displaying status every 5 seconds..." << std::endl;
  std::cout << "(Note: These are dummy nodes, health checks will fail)" << std::endl;
  std::cout << std::endl;
  
  int iteration = 0;
  while (iteration < 20) {  // Run for ~100 seconds max
    std::cout << "--- Status (" << iteration * 5 << "s) ---" << std::endl;
    
    // Show leader
    auto leader_res = failover_manager->GetLeader();
    if (leader_res.ok()) {
      auto leader = leader_res.value();
      std::cout << "Leader: " << leader.node_id 
                << " (health: " << governance::HealthStatusToString(leader.health) << ")" << std::endl;
    } else {
      std::cout << "Leader: None" << std::endl;
    }
    
    // Show followers
    auto followers = failover_manager->GetHealthyFollowers();
    std::cout << "Healthy followers: " << followers.size() << std::endl;
    for (const auto& f : followers) {
      std::cout << "  - " << f.node_id << std::endl;
    }
    
    // Show all nodes from health monitor
    auto all_nodes = health_monitor->GetAllNodes();
    std::cout << "All nodes: " << all_nodes.size() << std::endl;
    for (const auto& node : all_nodes) {
      std::cout << "  - " << node.node_id 
                << ": health=" << governance::HealthStatusToString(node.status)
                << ", latency=" << node.latency_ms << "ms" << std::endl;
    }
    
    std::cout << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    iteration++;
  }
  
  std::cout << "Shutting down..." << std::endl;
  failover_manager->Stop();
  health_monitor->Stop();
  
  return 0;
}
