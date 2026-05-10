// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
//
// Test: Dual-mode partitioning (StaticHash + MTHStream)

#include <iostream>
#include <cassert>
#include <chrono>

#include "cedar/partition/partition_strategy_manager.h"
#include "cedar/partition/strategies/static_hash_strategy.h"
#include "cedar/partition/strategies/mth_stream_strategy.h"

using namespace cedar::partition;

void TestStaticHashStrategy() {
  std::cout << "=== Test StaticHash Strategy ===" << std::endl;
  
  StaticHashStrategy strategy(100);  // 100 partitions
  
  // Test vertex routing
  auto assign1 = strategy.RouteVertex(42);
  assert(assign1.partition_id == 42);  // 42 % 100 = 42
  assert(assign1.confidence == 1.0);
  assert(assign1.strategy_tag == "StaticHash");
  
  auto assign2 = strategy.RouteVertex(150);
  assert(assign2.partition_id == 50);  // 150 % 100 = 50
  
  // Test edge routing
  auto [src_assign, dst_assign] = strategy.RouteEdge(42, 150);
  assert(src_assign.partition_id == 42);
  assert(dst_assign.partition_id == 50);
  
  // Test stats
  auto stats = strategy.GetStats();
  assert(stats.ok());
  std::cout << stats.ValueOrDie() << std::endl;
  
  std::cout << "StaticHash tests passed!" << std::endl;
}

void TestMTHStreamStrategy() {
  std::cout << "\n=== Test MTHStream Strategy ===" << std::endl;
  
  MTHStreamStrategy::Config config;
  config.num_partitions = 10;
  config.sketch_depth = 3;
  config.sketch_width = 64;
  config.fast_path_threshold = 0.5;
  
  MTHStreamStrategy strategy(config);
  
  // Test vertex routing (without temporal)
  auto assign1 = strategy.RouteVertex(1);
  std::cout << "Vertex 1 -> Partition " << assign1.partition_id 
            << " (confidence: " << assign1.confidence << ")" << std::endl;
  
  // Test temporal routing
  uint64_t ts = 1712563200000000ULL;  // microseconds
  auto assign2 = strategy.RouteVertexTemporal(1, ts);
  std::cout << "Vertex 1 @ " << ts << " -> Partition " << assign2.partition_id 
            << " (confidence: " << assign2.confidence << ")" << std::endl;
  
  // Test edge routing
  auto [src_assign, dst_assign] = strategy.RouteEdge(1, 2);
  std::cout << "Edge (1->2): src_partition=" << src_assign.partition_id 
            << ", dst_partition=" << dst_assign.partition_id << std::endl;
  
  // Test event stream processing
  std::vector<GraphEvent> events;
  events.push_back(GraphEvent(1, 0, ts, 0, 0, 0));  // CREATE Vertex 1
  events.push_back(GraphEvent(2, 0, ts + 1000, 0, 0, 0));  // CREATE Vertex 2
  events.push_back(GraphEvent(1, 2, ts + 2000, 1, 1, 0));  // CREATE EdgeOut from 1 to 2
  
  auto status = strategy.ProcessEventStream(events);
  assert(status.ok());
  std::cout << "Event stream processed successfully" << std::endl;
  
  // Test stats
  auto stats = strategy.GetStats();
  assert(stats.ok());
  std::cout << stats.ValueOrDie() << std::endl;
  
  std::cout << "MTHStream tests passed!" << std::endl;
}

void TestStrategyManager() {
  std::cout << "\n=== Test Strategy Manager ===" << std::endl;
  
  PartitionStrategyManager manager;
  
  StrategySelectionConfig config;
  config.default_strategy = StrategyType::STATIC_HASH;
  auto status = manager.Initialize(config);
  assert(status.ok());
  
  // Register StaticHash strategy
  auto static_hash = std::make_unique<StaticHashStrategy>(100);
  status = manager.RegisterStrategy(std::move(static_hash));
  assert(status.ok());
  
  // Register MTHStream strategy
  MTHStreamStrategy::Config mth_config;
  mth_config.num_partitions = 10;
  auto mth_stream = std::make_unique<MTHStreamStrategy>(mth_config);
  status = manager.RegisterStrategy(std::move(mth_stream));
  assert(status.ok());
  
  // Set active strategy to StaticHash
  status = manager.SetActiveStrategy(StrategyType::STATIC_HASH);
  assert(status.ok());
  
  // Route using StaticHash
  auto assign1 = manager.RouteVertex(42);
  std::cout << "StaticHash: Vertex 42 -> Partition " << assign1.partition_id << std::endl;
  assert(assign1.partition_id == 42);
  
  // Switch to MTHStream
  status = manager.SetActiveStrategy("MTHStream");
  assert(status.ok());
  
  // Route using MTHStream
  auto assign2 = manager.RouteVertex(42);
  std::cout << "MTHStream: Vertex 42 -> Partition " << assign2.partition_id << std::endl;
  
  // Get all stats
  std::cout << "\n--- All Stats ---" << std::endl;
  std::cout << manager.GetAllStats() << std::endl;
  
  std::cout << "Strategy Manager tests passed!" << std::endl;
}

void TestPerformanceComparison() {
  std::cout << "\n=== Performance Comparison ===" << std::endl;
  
  const int kNumOperations = 100000;
  const uint32_t kNumPartitions = 1000;
  
  // StaticHash performance
  {
    StaticHashStrategy strategy(kNumPartitions);
    
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < kNumOperations; ++i) {
      strategy.RouteVertex(i);
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    double ops_per_sec = static_cast<double>(kNumOperations) / (duration.count() / 1e6);
    std::cout << "StaticHash: " << kNumOperations << " ops in " << duration.count() 
              << " us (" << static_cast<uint64_t>(ops_per_sec) << " ops/s)" << std::endl;
  }
  
  // MTHStream performance
  {
    MTHStreamStrategy::Config config;
    config.num_partitions = kNumPartitions;
    MTHStreamStrategy strategy(config);
    
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < kNumOperations; ++i) {
      strategy.RouteVertex(i);
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    double ops_per_sec = static_cast<double>(kNumOperations) / (duration.count() / 1e6);
    std::cout << "MTHStream: " << kNumOperations << " ops in " << duration.count() 
              << " us (" << static_cast<uint64_t>(ops_per_sec) << " ops/s)" << std::endl;
  }
}

int main() {
  std::cout << "======================================" << std::endl;
  std::cout << "Dual-Mode Partitioning Test Suite" << std::endl;
  std::cout << "======================================" << std::endl;
  
  TestStaticHashStrategy();
  TestMTHStreamStrategy();
  TestStrategyManager();
  TestPerformanceComparison();
  
  std::cout << "\n======================================" << std::endl;
  std::cout << "All tests passed!" << std::endl;
  std::cout << "======================================" << std::endl;
  
  return 0;
}
