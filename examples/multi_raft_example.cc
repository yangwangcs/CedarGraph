// Copyright 2025 The Cedar Authors
//
// Multi-Raft Storage Example
// Demonstrates how to use CedarGraph with Multi-Raft replication

#include <iostream>
#include <thread>
#include <chrono>

#include "cedar/dtx/storage/raft_storage_integration.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar::dtx::storage;

// =============================================================================
// Example 1: Single Node Multi-Raft Setup
// =============================================================================

void Example1_SingleNodeSetup() {
  std::cout << "=== Example 1: Single Node Multi-Raft Setup ===" << std::endl;
  
  MultiRaftStorageService::Config config;
  config.node_id = 1;
  config.data_root = "/tmp/cedar/raft_node1";
  config.default_partition_count = 16;
  config.replication_factor = 3;
  
  // Thread pool config
  config.thread_pool_config.min_threads = 4;
  config.thread_pool_config.max_threads = 16;
  
  // Batch heartbeat config
  config.heartbeat_config.batch_interval = std::chrono::milliseconds(10);
  config.heartbeat_config.max_batch_size = 100;
  
  // Leader rebalancer config
  config.rebalancer_config.auto_rebalance = true;
  config.rebalancer_config.imbalance_threshold = 0.2;
  
  MultiRaftStorageService service;
  auto status = service.Initialize(config);
  if (!status.ok()) {
    std::cerr << "Failed to initialize: " << status.ToString() << std::endl;
    return;
  }
  
  std::cout << "Service initialized successfully" << std::endl;
  
  // Create a space with 16 partitions
  std::vector<NodeID> nodes = {1, 2, 3};
  status = service.CreateSpace("social_graph", 16, nodes);
  if (!status.ok()) {
    std::cerr << "Failed to create space: " << status.ToString() << std::endl;
    return;
  }
  
  std::cout << "Space 'social_graph' created with 16 partitions" << std::endl;
  
  // Get stats
  auto stats = service.GetStats();
  std::cout << "Total partitions: " << stats.total_partitions << std::endl;
  std::cout << "Leader partitions: " << stats.leader_partitions << std::endl;
  std::cout << "Follower partitions: " << stats.follower_partitions << std::endl;
  
  service.Shutdown();
  std::cout << "Service shutdown" << std::endl;
}

// =============================================================================
// Example 2: Write and Read Data
// =============================================================================

void Example2_WriteReadData() {
  std::cout << "\n=== Example 2: Write and Read Data ===" << std::endl;
  
  MultiRaftStorageService::Config config;
  config.node_id = 1;
  config.data_root = "/tmp/cedar/raft_node1_data";
  
  MultiRaftStorageService service;
  auto status = service.Initialize(config);
  if (!status.ok()) {
    std::cerr << "Failed to initialize: " << status.ToString() << std::endl;
    return;
  }
  
  // Create space
  std::vector<NodeID> nodes = {1};
  service.CreateSpace("test_space", 4, nodes);
  
  // Get partition for a key
  cedar::CedarKey key;
  key.SetVertexId(12345, 0);
  key.SetTimestamp(1000000);
  key.SetPartId(0);
  
  auto* partition = service.GetPartition(0);
  if (!partition) {
    std::cerr << "Partition not found" << std::endl;
    return;
  }
  
  // Write data (if we are leader)
  if (partition->IsLeader()) {
    cedar::Descriptor desc;
    desc.SetColumnId(0);
    
    status = partition->Put(key, desc, cedar::Timestamp(1));
    if (status.ok()) {
      std::cout << "Write successful" << std::endl;
    } else {
      std::cerr << "Write failed: " << status.ToString() << std::endl;
    }
  } else {
    std::cout << "Not leader, skipping write" << std::endl;
  }
  
  // Read data (local read, may be stale)
  auto result = partition->Get(key);
  if (result.ok()) {
    std::cout << "Read successful" << std::endl;
  } else {
    std::cerr << "Read failed: " << result.status().ToString() << std::endl;
  }
  
  // Linearizable read (goes through Raft)
  auto linear_result = partition->GetLinearizable(
      key, std::chrono::milliseconds(5000));
  if (linear_result.ok()) {
    std::cout << "Linearizable read successful" << std::endl;
  }
  
  service.Shutdown();
}

// =============================================================================
// Example 3: Cluster Setup
// =============================================================================

void Example3_ClusterSetup() {
  std::cout << "\n=== Example 3: 3-Node Cluster Setup ===" << std::endl;
  
  // Define cluster nodes
  std::vector<std::pair<NodeID, std::string>> nodes = {
    {1, "127.0.0.1:7001"},
    {2, "127.0.0.1:7002"},
    {3, "127.0.0.1:7003"}
  };
  
  // Setup cluster
  auto status = SetupMultiRaftCluster(
      nodes,
      "/data/cedar",
      128,    // 128 partitions
      3       // replication factor
  );
  
  if (status.ok()) {
    std::cout << "Cluster setup successful" << std::endl;
  } else {
    std::cerr << "Cluster setup failed: " << status.ToString() << std::endl;
  }
  
  // Example: Start each node
  for (const auto& [node_id, address] : nodes) {
    std::cout << "Node " << node_id << " at " << address << std::endl;
    
    MultiRaftStorageService::Config config;
    config.node_id = node_id;
    config.data_root = "/data/cedar/node" + std::to_string(node_id);
    
    // Each node would run this:
    // MultiRaftStorageService service;
    // service.Initialize(config);
  }
}

// =============================================================================
// Example 4: Monitoring and Stats
// =============================================================================

void Example4_Monitoring() {
  std::cout << "\n=== Example 4: Monitoring and Stats ===" << std::endl;
  
  MultiRaftStorageService::Config config;
  config.node_id = 1;
  config.data_root = "/tmp/cedar/monitoring";
  
  MultiRaftStorageService service;
  service.Initialize(config);
  
  // Create some partitions
  service.CreateSpace("monitored_space", 8, {1});
  
  // Get stats periodically
  for (int i = 0; i < 3; ++i) {
    auto stats = service.GetStats();
    
    std::cout << "\n--- Stats Round " << (i + 1) << " ---" << std::endl;
    std::cout << "Total partitions: " << stats.total_partitions << std::endl;
    std::cout << "Leader partitions: " << stats.leader_partitions << std::endl;
    std::cout << "Follower partitions: " << stats.follower_partitions << std::endl;
    std::cout << "Leader imbalance: " << stats.leader_imbalance_score << std::endl;
    
    std::cout << "Thread pool stats:" << std::endl;
    std::cout << "  Current threads: " << stats.thread_pool_stats.current_threads << std::endl;
    std::cout << "  Active threads: " << stats.thread_pool_stats.active_threads << std::endl;
    std::cout << "  Tasks submitted: " << stats.thread_pool_stats.total_tasks_submitted << std::endl;
    std::cout << "  Tasks executed: " << stats.thread_pool_stats.total_tasks_executed << std::endl;
    
    std::cout << "Heartbeat stats:" << std::endl;
    std::cout << "  Batches sent: " << stats.heartbeat_stats.total_batches_sent << std::endl;
    std::cout << "  Avg batch size: " << stats.heartbeat_stats.avg_batch_size << std::endl;
    std::cout << "  Bytes saved: " << stats.heartbeat_stats.network_bytes_saved << std::endl;
    
    // Check health
    if (!service.IsHealthy()) {
      auto issues = service.GetHealthIssues();
      std::cout << "Health issues:" << std::endl;
      for (const auto& issue : issues) {
        std::cout << "  - " << issue << std::endl;
      }
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  
  service.Shutdown();
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char* argv[]) {
  std::cout << "========================================" << std::endl;
  std::cout << "  CedarGraph Multi-Raft Examples" << std::endl;
  std::cout << "========================================" << std::endl;
  
  Example1_SingleNodeSetup();
  Example2_WriteReadData();
  Example3_ClusterSetup();
  Example4_Monitoring();
  
  std::cout << "\n========================================" << std::endl;
  std::cout << "  All examples completed" << std::endl;
  std::cout << "========================================" << std::endl;
  
  return 0;
}
