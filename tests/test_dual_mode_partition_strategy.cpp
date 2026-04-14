// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
//
// Test: DualModePartitionStrategy using CedarGraph CedarKey

#include <iostream>
#include <cassert>
#include "cedar/dtx/partition.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;
using namespace cedar::dtx;

void TestStaticHashMode() {
  std::cout << "=== Test StaticHash Mode ===" << std::endl;
  
  DualModePartitionStrategy::Config config;
  config.mode = DualModePartitionStrategy::Mode::STATIC_HASH;
  config.num_partitions = 100;
  
  DualModePartitionStrategy strategy(config);
  
  // Test vertex routing
  CedarKey key = CedarKey::Vertex(42, 0_vcol, Timestamp::Now());
  PartitionID pid = strategy.ComputePartition(key, 100);
  
  assert(pid == 42);  // 42 % 100 = 42
  std::cout << "✓ Vertex 42 -> Partition " << pid << std::endl;
  
  // Test edge routing (uses source vertex)
  CedarKey edge_key = CedarKey::EdgeOut(55, 100, 1_etype, Timestamp::Now());
  PartitionID edge_pid = strategy.ComputePartition(edge_key, 100);
  
  assert(edge_pid == 55);  // 55 % 100 = 55
  std::cout << "✓ Edge from 55 -> Partition " << edge_pid << std::endl;
  
  std::cout << "StaticHash mode tests passed!" << std::endl;
}

void TestMTHStreamMode() {
  std::cout << "\n=== Test MTHStream Mode ===" << std::endl;
  
  DualModePartitionStrategy::Config config;
  config.mode = DualModePartitionStrategy::Mode::MTH_STREAM;
  config.num_partitions = 10;
  
  DualModePartitionStrategy strategy(config);
  
  // Test vertex routing with temporal
  uint64_t ts = 1712563200000000ULL;
  CedarKey key = CedarKey::Vertex(1, 0_vcol, Timestamp(ts));
  PartitionID pid = strategy.ComputePartition(key, 10);
  
  std::cout << "✓ Vertex 1 @ " << ts << " -> Partition " << pid << std::endl;
  
  // Process multiple events
  for (int i = 0; i < 5; ++i) {
    CedarKey k = CedarKey::Vertex(i, 0_vcol, Timestamp(ts + i * 1000));
    strategy.ComputePartition(k, 10);
  }
  
  std::cout << "✓ Processed 5 temporal events" << std::endl;
  
  // Get stats
  std::cout << "\n--- Stats ---" << std::endl;
  std::cout << strategy.GetStats() << std::endl;
  
  std::cout << "MTHStream mode tests passed!" << std::endl;
}

void TestModeSwitching() {
  std::cout << "\n=== Test Mode Switching ===" << std::endl;
  
  DualModePartitionStrategy::Config config;
  config.mode = DualModePartitionStrategy::Mode::STATIC_HASH;
  config.num_partitions = 100;
  
  DualModePartitionStrategy strategy(config);
  
  // Check initial mode
  assert(strategy.GetMode() == DualModePartitionStrategy::Mode::STATIC_HASH);
  std::cout << "✓ Initial mode: StaticHash" << std::endl;
  
  // Switch to MTH_STREAM
  strategy.SetMode(DualModePartitionStrategy::Mode::MTH_STREAM);
  assert(strategy.GetMode() == DualModePartitionStrategy::Mode::MTH_STREAM);
  std::cout << "✓ Switched to MTH_STREAM" << std::endl;
  
  // Switch to AUTO
  strategy.SetMode(DualModePartitionStrategy::Mode::AUTO);
  assert(strategy.GetMode() == DualModePartitionStrategy::Mode::AUTO);
  std::cout << "✓ Switched to AUTO" << std::endl;
  
  // Verify name changes
  assert(strategy.Name() == "DualMode(Auto)");
  std::cout << "✓ Name reflects mode: " << strategy.Name() << std::endl;
  
  std::cout << "Mode switching tests passed!" << std::endl;
}

void TestQueryStats() {
  std::cout << "\n=== Test Query Stats ===" << std::endl;
  
  DualModePartitionStrategy::Config config;
  config.mode = DualModePartitionStrategy::Mode::AUTO;
  config.num_partitions = 100;
  config.temporal_query_threshold = 5;
  
  DualModePartitionStrategy strategy(config);
  
  // Update stats
  for (int i = 0; i < 10; ++i) {
    bool is_temporal = (i % 2 == 0);
    bool has_locality = (i % 3 == 0);
    strategy.UpdateQueryStats(is_temporal, has_locality);
  }
  
  std::cout << "✓ Updated query stats (10 queries)" << std::endl;
  
  // Get stats
  std::cout << "\n--- Stats ---" << std::endl;
  std::cout << strategy.GetStats() << std::endl;
  
  std::cout << "Query stats tests passed!" << std::endl;
}

void TestCedarKeyIntegration() {
  std::cout << "\n=== Test CedarKey Integration ===" << std::endl;
  
  DualModePartitionStrategy::Config config;
  config.mode = DualModePartitionStrategy::Mode::STATIC_HASH;
  config.num_partitions = 100;
  
  DualModePartitionStrategy strategy(config);
  
  // Test with pre-set part_id
  CedarKey key = CedarKey::Vertex(42, 0_vcol, Timestamp::Now(), 0, 50);
  assert(key.part_id() == 50);
  key.AddFlags(key_flags::kIsDistributed);
  
  PartitionID pid = strategy.ComputePartition(key, 100);
  assert(pid == 50);  // Should use existing part_id
  std::cout << "✓ Respects existing part_id: " << pid << std::endl;
  
  // Test without distributed flag (should compute)
  CedarKey key2 = CedarKey::Vertex(42, 0_vcol, Timestamp::Now());
  PartitionID pid2 = strategy.ComputePartition(key2, 100);
  assert(pid2 == 42);  // Should compute
  std::cout << "✓ Computes partition when no part_id: " << pid2 << std::endl;
  
  std::cout << "CedarKey integration tests passed!" << std::endl;
}

int main() {
  std::cout << "==============================================" << std::endl;
  std::cout << "DualModePartitionStrategy Test Suite" << std::endl;
  std::cout << "Using CedarGraph CedarKey (32B)" << std::endl;
  std::cout << "==============================================" << std::endl;
  
  TestStaticHashMode();
  TestMTHStreamMode();
  TestModeSwitching();
  TestQueryStats();
  TestCedarKeyIntegration();
  
  std::cout << "\n==============================================" << std::endl;
  std::cout << "All tests passed!" << std::endl;
  std::cout << "==============================================" << std::endl;
  
  return 0;
}
