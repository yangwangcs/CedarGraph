// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
//
// Example: Using Dual-Mode Partitioning in CedarGraph

#include <iostream>
#include <memory>
#include "partition/partition_strategy_manager.h"
#include "partition/strategies/static_hash_strategy.h"
#include "partition/strategies/mth_stream_strategy.h"

using namespace cedar::partition;

int main() {
  std::cout << "=== CedarGraph Dual-Mode Partitioning Example ===" << std::endl;

  // Step 1: Create the strategy manager
  PartitionStrategyManager manager;
  
  StrategySelectionConfig config;
  config.default_strategy = StrategyType::STATIC_HASH;
  manager.Initialize(config);
  
  // Step 2: Register StaticHash strategy (for general workloads)
  auto static_hash = std::make_unique<StaticHashStrategy>(100);
  if (manager.RegisterStrategy(std::move(static_hash)).ok()) {
    std::cout << "✓ Registered StaticHash strategy" << std::endl;
  }
  
  // Step 3: Register MTHStream strategy (for temporal workloads)
  MTHStreamStrategy::Config mth_config;
  mth_config.num_partitions = 100;
  mth_config.sketch_depth = 3;
  mth_config.sketch_width = 64;
  mth_config.fast_path_threshold = 0.6;
  mth_config.temporal_alpha = 0.01;
  
  auto mth_stream = std::make_unique<MTHStreamStrategy>(mth_config);
  if (manager.RegisterStrategy(std::move(mth_stream)).ok()) {
    std::cout << "✓ Registered MTHStream strategy" << std::endl;
  }
  
  // Step 4: Use StaticHash for simple lookups
  std::cout << "\n--- Using StaticHash Strategy ---" << std::endl;
  manager.SetActiveStrategy(StrategyType::STATIC_HASH);
  
  std::vector<uint64_t> vertices = {1, 2, 3, 100, 200, 300};
  for (auto vid : vertices) {
    auto assign = manager.RouteVertex(vid);
    std::cout << "Vertex " << vid << " -> Partition " << assign.partition_id 
              << " [" << assign.strategy_tag << "]" << std::endl;
  }
  
  // Step 5: Use MTHStream for temporal graph analysis
  std::cout << "\n--- Using MTHStream Strategy ---" << std::endl;
  manager.SetActiveStrategy(StrategyType::MTH_STREAM);
  
  // Simulate temporal events (vertex creations and edge insertions)
  std::vector<GraphEvent> events;
  uint64_t base_time = 1712563200000000ULL;  // Base timestamp
  
  // Create vertices
  events.push_back(GraphEvent(1, 0, base_time, 0, 0, 0));        // CREATE Vertex 1
  events.push_back(GraphEvent(2, 0, base_time + 1000, 0, 0, 0)); // CREATE Vertex 2
  events.push_back(GraphEvent(3, 0, base_time + 2000, 0, 0, 0)); // CREATE Vertex 3
  
  // Create edges (Event Split: EdgeOut and EdgeIn are routed independently)
  events.push_back(GraphEvent(1, 2, base_time + 3000, 1, 1, 0)); // CREATE EdgeOut 1->2
  events.push_back(GraphEvent(2, 1, base_time + 3000, 1, 2, 0)); // CREATE EdgeIn 2<-1
  events.push_back(GraphEvent(2, 3, base_time + 4000, 1, 1, 0)); // CREATE EdgeOut 2->3
  events.push_back(GraphEvent(3, 2, base_time + 4000, 1, 2, 0)); // CREATE EdgeIn 3<-2
  
  // Process events through MTH partitioner
  auto status = manager.ProcessEventStream(events);
  if (status.ok()) {
    std::cout << "✓ Processed " << events.size() << " events" << std::endl;
  }
  
  // Query partitions after event processing
  for (auto vid : vertices) {
    auto assign = manager.RouteVertexTemporal(vid, base_time + 5000);
    std::cout << "Vertex " << vid << " -> Partition " << assign.partition_id 
              << " (confidence: " << assign.confidence << ")" << std::endl;
  }
  
  // Step 6: Edge routing with MTH
  std::cout << "\n--- Edge Routing (MTH) ---" << std::endl;
  auto [src_assign, dst_assign] = manager.RouteEdge(1, 2);
  std::cout << "Edge (1->2): src_partition=" << src_assign.partition_id 
            << ", dst_partition=" << dst_assign.partition_id << std::endl;
  
  // Step 7: Get strategy statistics
  std::cout << "\n--- Strategy Statistics ---" << std::endl;
  std::cout << manager.GetAllStats() << std::endl;
  
  // Step 8: Strategy switching based on workload
  std::cout << "\n--- Dynamic Strategy Selection ---" << std::endl;
  
  // Simulate query monitoring
  for (int i = 0; i < 10; ++i) {
    bool is_temporal = (i % 2 == 0);  // 50% temporal queries
    bool has_locality = (i % 3 == 0); // 33% locality queries
    manager.UpdateQueryStats(is_temporal, has_locality);
  }
  
  // Try auto-switch (would need more queries to trigger)
  manager.MaybeAutoSwitchStrategy();
  
  auto* active_strategy = manager.GetActiveStrategy();
  std::cout << "Active strategy: " << (active_strategy ? active_strategy->Name() : "None") 
            << std::endl;
  
  std::cout << "\n=== Example Complete ===" << std::endl;
  return 0;
}
