# End-to-End Partition Strategy Test Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Validate the complete data flow of temporal graph data through the dual-mode partition strategy before production deployment.

**Architecture:** Simulate a 3-node cluster (MetaD + GraphD + StorageD), ingest temporal graph data, verify correct partitioning, routing, and persistence.

**Tech Stack:** CedarGraph, DualModePartitionStrategy, CedarKey, gRPC, LSM-Tree storage

---

## Test Scenarios

### Scenario 1: StaticHash Mode - Basic Data Flow
- **Input:** 1000 vertices, 5000 edges with timestamps
- **Expected:** Data distributed evenly using hash(vid) % n
- **Verify:** Each partition receives ~same amount of data

### Scenario 2: MTHStream Mode - Temporal Locality
- **Input:** Time-series graph data (events in bursts)
- **Expected:** Related temporal events co-located in same partition
- **Verify:** High temporal locality score

### Scenario 3: AUTO Mode - Dynamic Switching
- **Phase 1:** Static workload (random access)
- **Phase 2:** Temporal workload (time-range queries)
- **Expected:** Strategy auto-switches from StaticHash to MTH
- **Verify:** Query stats trigger mode switch

### Scenario 4: Edge Split - Independent Routing
- **Input:** Edges with different src/dst partition assignments
- **Expected:** EdgeOut routed by src, EdgeIn routed by dst
- **Verify:** Both endpoints stored in correct partitions

### Scenario 5: Failover and Recovery
- **Input:** Write data, kill a node, restart
- **Expected:** Data persists, can be read back
- **Verify:** SST files contain correct data

---

## Task 1: Create End-to-End Test Framework

**Files:**
- Create: `tests/test_end_to_end_partition.cc`

- [ ] **Step 1: Create test framework with cluster simulation**

```cpp
// End-to-end test for dual-mode partition strategy
#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <cassert>
#include "cedar/dtx/partition.h"
#include "cedar/types/cedar_key.h"
#include "cedar/core/status.h"

using namespace cedar;
using namespace cedar::dtx;

// Test configuration
struct TestConfig {
  PartitionID num_partitions = 16;  // Small for testing
  uint64_t num_vertices = 1000;
  uint64_t num_edges = 5000;
  uint64_t time_range_micros = 3600 * 1000000;  // 1 hour
};

// Mock storage for testing
class MockPartitionStorage {
 public:
  struct StoredKey {
    CedarKey key;
    PartitionID partition_id;
    int64_t timestamp;
  };
  
  std::vector<StoredKey> stored_keys;
  
  Status Store(const CedarKey& key, PartitionID pid) {
    stored_keys.push_back({key, pid, 
      std::chrono::system_clock::now().time_since_epoch().count()});
    return Status::OK();
  }
  
  size_t CountInPartition(PartitionID pid) const {
    return std::count_if(stored_keys.begin(), stored_keys.end(),
                         [pid](const auto& sk) { return sk.partition_id == pid; });
  }
  
  void Clear() { stored_keys.clear(); }
};

// Cluster simulator
class TestCluster {
 public:
  std::unique_ptr<PartitionManager> partition_manager;
  std::vector<std::unique_ptr<MockPartitionStorage>> storages;
  
  Status Initialize(const DualModePartitionStrategy::Config& config) {
    DTxConfig dtx_config;
    partition_manager = std::make_unique<PartitionManager>(dtx_config);
    auto status = partition_manager->InitializeDualMode(config);
    if (!status.ok()) return status;
    
    // Create mock storages for each partition
    for (PartitionID i = 0; i < config.num_partitions; ++i) {
      storages.push_back(std::make_unique<MockPartitionStorage>());
    }
    
    return Status::OK();
  }
  
  Status WriteVertex(uint64_t vid, Timestamp ts) {
    CedarKey key = CedarKey::Vertex(vid, 0_vcol, ts);
    PartitionID pid = partition_manager->GetPartition(key);
    return storages[pid]->Store(key, pid);
  }
  
  Status WriteEdge(uint64_t src, uint64_t dst, uint16_t type, Timestamp ts) {
    // EdgeOut routed by src
    CedarKey edge_out = CedarKey::EdgeOut(src, dst, EdgeTypeId(type), ts);
    PartitionID src_pid = partition_manager->GetPartition(edge_out);
    auto status = storages[src_pid]->Store(edge_out, src_pid);
    if (!status.ok()) return status;
    
    // EdgeIn routed by dst
    CedarKey edge_in = CedarKey::EdgeIn(dst, src, EdgeTypeId(type), ts);
    PartitionID dst_pid = partition_manager->GetPartition(edge_in);
    return storages[dst_pid]->Store(edge_in, dst_pid);
  }
  
  void PrintStats() const {
    std::cout << "\n=== Partition Distribution ===" << std::endl;
    for (size_t i = 0; i < storages.size(); ++i) {
      std::cout << "Partition " << i << ": " 
                << storages[i]->stored_keys.size() << " keys" << std::endl;
    }
  }
};

// Test runner
int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "End-to-End Partition Strategy Test" << std::endl;
  std::cout << "========================================" << std::endl;
  
  TestConfig config;
  
  // Run tests
  TestStaticHashMode(config);
  TestMTHStreamMode(config);
  TestAutoMode(config);
  TestEdgeSplit(config);
  
  std::cout << "\n========================================" << std::endl;
  std::cout << "All tests passed!" << std::endl;
  std::cout << "========================================" << std::endl;
  
  return 0;
}
```

- [ ] **Step 2: Implement StaticHash mode test**

```cpp
void TestStaticHashMode(const TestConfig& config) {
  std::cout << "\n=== Test 1: StaticHash Mode ===" << std::endl;
  
  // Initialize cluster with StaticHash mode
  DualModePartitionStrategy::Config partition_config;
  partition_config.mode = DualModePartitionStrategy::Mode::STATIC_HASH;
  partition_config.num_partitions = config.num_partitions;
  
  TestCluster cluster;
  auto status = cluster.Initialize(partition_config);
  assert(status.ok());
  
  // Generate test data
  std::mt19937_64 rng(42);
  std::uniform_int_distribution<uint64_t> vid_dist(1, config.num_vertices);
  std::uniform_int_distribution<uint64_t> ts_dist(0, config.time_range_micros);
  
  // Write vertices
  std::cout << "Writing " << config.num_vertices << " vertices..." << std::endl;
  for (uint64_t i = 0; i < config.num_vertices; ++i) {
    uint64_t vid = vid_dist(rng);
    Timestamp ts(ts_dist(rng));
    status = cluster.WriteVertex(vid, ts);
    assert(status.ok());
  }
  
  // Write edges
  std::cout << "Writing " << config.num_edges << " edges..." << std::endl;
  for (uint64_t i = 0; i < config.num_edges; ++i) {
    uint64_t src = vid_dist(rng);
    uint64_t dst = vid_dist(rng);
    if (src == dst) continue;  // Skip self-loops
    
    Timestamp ts(ts_dist(rng));
    status = cluster.WriteEdge(src, dst, 1, ts);
    assert(status.ok());
  }
  
  // Verify distribution
  cluster.PrintStats();
  
  // Check load balance (each partition should have ~ (vertices + edges*2) / n )
  size_t expected_per_partition = (config.num_vertices + config.num_edges * 2) / config.num_partitions;
  size_t tolerance = expected_per_partition / 4;  // 25% tolerance
  
  for (size_t i = 0; i < cluster.storages.size(); ++i) {
    size_t count = cluster.storages[i]->stored_keys.size();
    // StaticHash should be fairly balanced
    assert(count >= expected_per_partition - tolerance);
    assert(count <= expected_per_partition + tolerance);
  }
  
  std::cout << "✓ StaticHash mode test passed" << std::endl;
}
```

- [ ] **Step 3: Implement MTHStream mode test**

```cpp
void TestMTHStreamMode(const TestConfig& config) {
  std::cout << "\n=== Test 2: MTHStream Mode ===" << std::endl;
  
  // Initialize cluster with MTH mode
  DualModePartitionStrategy::Config partition_config;
  partition_config.mode = DualModePartitionStrategy::Mode::MTH_STREAM;
  partition_config.num_partitions = config.num_partitions;
  partition_config.sketch_depth = 3;
  partition_config.sketch_width = 64;
  
  TestCluster cluster;
  auto status = cluster.Initialize(partition_config);
  assert(status.ok());
  
  // Generate temporal-burst data (simulating real-world patterns)
  std::mt19937_64 rng(42);
  
  // Simulate 10 "bursts" of activity
  int num_bursts = 10;
  uint64_t burst_interval = config.time_range_micros / num_bursts;
  
  std::cout << "Writing temporal burst data..." << std::endl;
  
  for (int burst = 0; burst < num_bursts; ++burst) {
    // Each burst has a set of "hot" vertices
    std::vector<uint64_t> hot_vertices;
    for (int i = 0; i < 10; ++i) {
      hot_vertices.push_back(rng() % config.num_vertices + 1);
    }
    
    // Generate events within this burst
    uint64_t burst_start = burst * burst_interval;
    std::uniform_int_distribution<uint64_t> ts_dist(burst_start, 
        burst_start + burst_interval / 2);
    
    // Each hot vertex has multiple events
    for (uint64_t vid : hot_vertices) {
      for (int event = 0; event < 5; ++event) {
        Timestamp ts(ts_dist(rng));
        status = cluster.WriteVertex(vid, ts);
        assert(status.ok());
        
        // Add some edges between hot vertices
        if (event < 3) {
          uint64_t dst = hot_vertices[rng() % hot_vertices.size()];
          if (vid != dst) {
            status = cluster.WriteEdge(vid, dst, 1, ts);
            assert(status.ok());
          }
        }
      }
    }
  }
  
  cluster.PrintStats();
  
  // Get MTH statistics
  auto* dual_mode = cluster.partition_manager->GetDualModeStrategy();
  if (dual_mode) {
    std::cout << "\n--- MTH Statistics ---" << std::endl;
    std::cout << dual_mode->GetStats() << std::endl;
  }
  
  std::cout << "✓ MTHStream mode test passed" << std::endl;
}
```

- [ ] **Step 4: Implement AUTO mode test**

```cpp
void TestAutoMode(const TestConfig& config) {
  std::cout << "\n=== Test 3: AUTO Mode with Dynamic Switching ===" << std::endl;
  
  // Initialize with AUTO mode
  DualModePartitionStrategy::Config partition_config;
  partition_config.mode = DualModePartitionStrategy::Mode::AUTO;
  partition_config.num_partitions = config.num_partitions;
  partition_config.temporal_query_threshold = 50;
  partition_config.locality_ratio_threshold = 0.6;
  
  TestCluster cluster;
  auto status = cluster.Initialize(partition_config);
  assert(status.ok());
  
  // Phase 1: Random access (should use StaticHash)
  std::cout << "\nPhase 1: Random access workload..." << std::endl;
  std::mt19937_64 rng(42);
  
  for (int i = 0; i < 30; ++i) {
    uint64_t vid = rng() % config.num_vertices;
    Timestamp ts(rng() % config.time_range_micros);
    status = cluster.WriteVertex(vid, ts);
    assert(status.ok());
    
    // Report as non-temporal query
    cluster.partition_manager->ReportQueryStats(false, false);
  }
  
  auto* dual_mode = cluster.partition_manager->GetDualModeStrategy();
  std::cout << "After Phase 1: " << dual_mode->Name() << std::endl;
  
  // Phase 2: Temporal workload (should trigger switch to MTH)
  std::cout << "\nPhase 2: Temporal workload..." << std::endl;
  
  for (int i = 0; i < 100; ++i) {
    // Write with temporal locality
    uint64_t vid = 100 + (i % 10);  // Hot vertices
    Timestamp ts(1000000 + i * 1000);  // Sequential timestamps
    status = cluster.WriteVertex(vid, ts);
    assert(status.ok());
    
    // Report as temporal query
    cluster.partition_manager->ReportQueryStats(true, true);
  }
  
  std::cout << "After Phase 2: " << dual_mode->Name() << std::endl;
  
  cluster.PrintStats();
  std::cout << dual_mode->GetStats() << std::endl;
  
  std::cout << "✓ AUTO mode test passed" << std::endl;
}
```

- [ ] **Step 5: Implement Edge Split test**

```cpp
void TestEdgeSplit(const TestConfig& config) {
  std::cout << "\n=== Test 4: Edge Split (Independent Routing) ===" << std::endl;
  
  DualModePartitionStrategy::Config partition_config;
  partition_config.mode = DualModePartitionStrategy::Mode::STATIC_HASH;
  partition_config.num_partitions = config.num_partitions;
  
  TestCluster cluster;
  auto status = cluster.Initialize(partition_config);
  assert(status.ok());
  
  // Create edge between two specific vertices
  uint64_t src_vid = 42;
  uint64_t dst_vid = 100;
  Timestamp ts(1234567890);
  
  // Calculate expected partitions
  CedarKey src_key = CedarKey::Vertex(src_vid, 0_vcol, ts);
  CedarKey dst_key = CedarKey::Vertex(dst_vid, 0_vcol, ts);
  
  PartitionID src_partition = cluster.partition_manager->GetPartition(src_key);
  PartitionID dst_partition = cluster.partition_manager->GetPartition(dst_key);
  
  std::cout << "Source vertex " << src_vid << " -> Partition " << src_partition << std::endl;
  std::cout << "Dest vertex " << dst_vid << " -> Partition " << dst_partition << std::endl;
  
  // Write edge
  status = cluster.WriteEdge(src_vid, dst_vid, 1, ts);
  assert(status.ok());
  
  // Verify EdgeOut is in src partition
  bool found_edge_out = false;
  bool found_edge_in = false;
  
  for (const auto& stored : cluster.storages[src_partition]->stored_keys) {
    if (stored.key.IsEdgeOut() && stored.key.entity_id() == src_vid) {
      found_edge_out = true;
      std::cout << "EdgeOut stored in Partition " << src_partition << " ✓" << std::endl;
      break;
    }
  }
  
  for (const auto& stored : cluster.storages[dst_partition]->stored_keys) {
    if (stored.key.IsEdgeIn() && stored.key.entity_id() == dst_vid) {
      found_edge_in = true;
      std::cout << "EdgeIn stored in Partition " << dst_partition << " ✓" << std::endl;
      break;
    }
  }
  
  assert(found_edge_out);
  assert(found_edge_in);
  
  std::cout << "✓ Edge Split test passed" << std::endl;
}
```

---

## Task 2: Build and Run Tests

**Files:**
- Modify: `CMakeLists.txt`

- [ ] **Step 6: Add test target to CMakeLists.txt**

```cmake
# End-to-End Partition Test
add_executable(test_end_to_end_partition
    tests/test_end_to_end_partition.cc
)
target_link_libraries(test_end_to_end_partition cedar ${LZ4_LIBRARIES})
target_include_directories(test_end_to_end_partition PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)
```

- [ ] **Step 7: Build and run the test**

Run:
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build
cmake --build . --target test_end_to_end_partition -j4
./test_end_to_end_partition
```

Expected output:
```
========================================
End-to-End Partition Strategy Test
========================================

=== Test 1: StaticHash Mode ===
Writing 1000 vertices...
Writing 5000 edges...
=== Partition Distribution ===
Partition 0: 625 keys
Partition 1: 631 keys
...
✓ StaticHash mode test passed

=== Test 2: MTHStream Mode ===
Writing temporal burst data...
=== MTH Statistics ===
DualModePartitionStrategy Stats:
  Mode: DualMode(MTHStream)
  Fast Path Ratio: 0.85
...
✓ MTHStream mode test passed

=== Test 3: AUTO Mode ===
Phase 1: Random access...
After Phase 1: DualMode(StaticHash)
Phase 2: Temporal workload...
After Phase 2: DualMode(MTHStream)
✓ AUTO mode test passed

=== Test 4: Edge Split ===
✓ Edge Split test passed

========================================
All tests passed!
========================================
```

---

## Success Criteria

- [ ] All 4 test scenarios pass
- [ ] StaticHash shows balanced distribution
- [ ] MTHStream shows temporal locality
- [ ] AUTO mode switches based on workload
- [ ] Edge Split correctly routes EdgeOut/EdgeIn independently
- [ ] No memory leaks or crashes

---

## Production Readiness Checklist

- [ ] Code compiles without warnings
- [ ] All unit tests pass
- [ ] End-to-end tests pass
- [ ] Configuration file validates
- [ ] Performance benchmarks meet targets
- [ ] Documentation complete
