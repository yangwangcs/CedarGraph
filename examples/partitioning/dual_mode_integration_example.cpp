// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
//
// Example: Dual-Mode Partition Strategy Integration
//
// This example demonstrates:
// 1. Loading partition config from YAML file
// 2. Initializing DualModePartitionStrategy
// 3. Using with PartitionManager
// 4. Runtime mode switching via GraphServiceRouter

#include <iostream>
#include <memory>
#include "cedar/dtx/partition.h"
#include "cedar/dtx/partition_config.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;
using namespace cedar::dtx;

void PrintHeader(const std::string& title) {
  std::cout << "\n" << std::string(50, '=') << std::endl;
  std::cout << title << std::endl;
  std::cout << std::string(50, '=') << std::endl;
}

int main() {
  PrintHeader("Dual-Mode Partition Strategy Integration Example");
  
  // =======================================================================
  // Step 1: Load configuration from YAML file
  // =======================================================================
  PrintHeader("Step 1: Load Configuration");
  
  std::string config_file = "config/partition.yaml";
  DualModePartitionStrategy::Config config;
  PartitionID num_partitions = 0;
  
  Status status = PartitionConfigLoader::LoadFromFile(
      config_file, &config, &num_partitions);
  
  if (!status.ok()) {
    std::cout << "Config file not found, using default config" << std::endl;
    config = DualModePartitionStrategy::Config();
    num_partitions = 32768;
  } else {
    std::cout << "✓ Config loaded from: " << config_file << std::endl;
  }
  
  std::cout << "  num_partitions: " << num_partitions << std::endl;
  std::cout << "  default_mode: " << (config.mode == DualModePartitionStrategy::Mode::STATIC_HASH ? 
                                     "static_hash" : 
                                     config.mode == DualModePartitionStrategy::Mode::MTH_STREAM ?
                                     "mth_stream" : "auto") << std::endl;
  
  // =======================================================================
  // Step 2: Initialize PartitionManager with DualMode
  // =======================================================================
  PrintHeader("Step 2: Initialize PartitionManager");
  
  DTxConfig dtx_config;
  auto partition_manager = std::make_unique<PartitionManager>(dtx_config);
  
  status = partition_manager->InitializeDualMode(config);
  if (!status.ok()) {
    std::cerr << "Failed to initialize partition manager: " << status.ToString() << std::endl;
    return 1;
  }
  
  std::cout << "✓ PartitionManager initialized with dual-mode strategy" << std::endl;
  std::cout << "  Strategy: " << partition_manager->GetDualModeStrategy()->Name() << std::endl;
  
  // =======================================================================
  // Step 3: Test partition routing with different modes
  // =======================================================================
  PrintHeader("Step 3: Test Partition Routing");
  
  // Test with StaticHash mode (default)
  std::cout << "\n--- StaticHash Mode ---" << std::endl;
  
  CedarKey key1 = CedarKey::Vertex(42, 0_vcol, Timestamp::Now());
  PartitionID pid1 = partition_manager->GetPartition(key1);
  std::cout << "Vertex 42 -> Partition " << pid1 << std::endl;
  
  CedarKey key2 = CedarKey::Vertex(1000, 0_vcol, Timestamp::Now());
  PartitionID pid2 = partition_manager->GetPartition(key2);
  std::cout << "Vertex 1000 -> Partition " << pid2 << std::endl;
  
  // =======================================================================
  // Step 4: Switch to MTH mode
  // =======================================================================
  PrintHeader("Step 4: Switch to MTHStream Mode");
  
  status = partition_manager->SetPartitionMode(DualModePartitionStrategy::Mode::MTH_STREAM);
  if (!status.ok()) {
    std::cerr << "Failed to switch mode: " << status.ToString() << std::endl;
    return 1;
  }
  
  std::cout << "✓ Mode switched to: " << partition_manager->GetDualModeStrategy()->Name() << std::endl;
  
  // Test with MTH mode
  std::cout << "\n--- MTHStream Mode (with temporal routing) ---" << std::endl;
  
  uint64_t ts = 1712563200000000ULL;
  for (int i = 0; i < 5; ++i) {
    CedarKey key = CedarKey::Vertex(i, 0_vcol, Timestamp(ts + i * 1000));
    PartitionID pid = partition_manager->GetPartition(key);
    std::cout << "Vertex " << i << " @ " << (ts + i * 1000) << " -> Partition " << pid << std::endl;
  }
  
  // =======================================================================
  // Step 5: Report query statistics
  // =======================================================================
  PrintHeader("Step 5: Query Statistics (for AUTO mode)");
  
  // Simulate query workload
  for (int i = 0; i < 10; ++i) {
    bool is_temporal = (i % 2 == 0);  // 50% temporal queries
    bool has_locality = (i % 3 == 0); // 33% locality queries
    partition_manager->ReportQueryStats(is_temporal, has_locality);
  }
  
  std::cout << "✓ Reported 10 query statistics" << std::endl;
  
  // Get statistics
  auto* dual_mode = partition_manager->GetDualModeStrategy();
  if (dual_mode) {
    std::cout << "\n--- Statistics ---" << std::endl;
    std::cout << dual_mode->GetStats() << std::endl;
  }
  
  // =======================================================================
  // Step 6: Switch to AUTO mode
  // =======================================================================
  PrintHeader("Step 6: Switch to AUTO Mode");
  
  status = partition_manager->SetPartitionMode(DualModePartitionStrategy::Mode::AUTO);
  if (!status.ok()) {
    std::cerr << "Failed to switch mode: " << status.ToString() << std::endl;
    return 1;
  }
  
  std::cout << "✓ Mode switched to: " << partition_manager->GetDualModeStrategy()->Name() << std::endl;
  std::cout << "  AUTO mode will automatically select between StaticHash and MTHStream" << std::endl;
  std::cout << "  based on query statistics (temporal_query_threshold: " 
            << config.temporal_query_threshold << ")" << std::endl;
  
  // =======================================================================
  // Step 7: Edge routing (demonstrates Event Split)
  // =======================================================================
  PrintHeader("Step 7: Edge Routing (Event Split)");
  
  // Create an edge (src -> dst)
  CedarKey edge_out = CedarKey::EdgeOut(10, 20, 1_etype, Timestamp::Now());
  CedarKey edge_in = CedarKey::EdgeIn(20, 10, 1_etype, Timestamp::Now());
  
  PartitionID src_partition = partition_manager->GetPartition(edge_out);
  PartitionID dst_partition = partition_manager->GetPartition(edge_in);
  
  std::cout << "Edge (10 -> 20):" << std::endl;
  std::cout << "  EdgeOut (src=10) -> Partition " << src_partition << std::endl;
  std::cout << "  EdgeIn (dst=20)  -> Partition " << dst_partition << std::endl;
  std::cout << "  (Note: EdgeOut and EdgeIn are routed independently)" << std::endl;
  
  // =======================================================================
  // Summary
  // =======================================================================
  PrintHeader("Summary");
  
  std::cout << "✓ Dual-mode partition strategy successfully integrated" << std::endl;
  std::cout << "✓ StaticHash mode: O(1) hash-based routing" << std::endl;
  std::cout << "✓ MTHStream mode: Temporal-aware sketch-based routing" << std::endl;
  std::cout << "✓ AUTO mode: Automatic mode selection based on workload" << std::endl;
  std::cout << "✓ Runtime mode switching supported" << std::endl;
  std::cout << "✓ YAML configuration supported" << std::endl;
  
  return 0;
}
