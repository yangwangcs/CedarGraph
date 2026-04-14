# Dual-Mode Partition Strategy Integration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate subgraph2's MTH partitioning algorithm into CedarGraph using CedarGraph's native CedarKey (32B), supporting dual-mode: StaticHash and MTHStream.

**Architecture:** Create a `DualModePartitionStrategy` class that inherits from CedarGraph's `PartitionStrategy` interface. It wraps both `StaticHashStrategy` (simple hash) and `MTHStreamStrategy` (temporal-aware sketch-based), allowing runtime switching. All code uses CedarGraph's `cedar::CedarKey` from `include/cedar/types/cedar_key.h`.

**Tech Stack:** C++17, CedarGraph CedarKey, PartitionStrategy interface, MTH (Count-Min Sketch + Temporal Affinity)

---

## Key Design Decision

**Use CedarGraph CedarKey ONLY** - The 32B CedarKey in `include/cedar/types/cedar_key.h` is the sole key format. Remove/replace all subgraph2 CedarKey usage.

Key differences to handle:
- `timestamp()` returns `Timestamp` object (use `.value()` for microseconds)
- `entity_type()` returns `EntityType` enum (Vertex/EdgeOut/EdgeIn)
- `GetOpType()` returns operation type (Create/Update/Delete)
- `part_id()` is already a field in CedarGraph CedarKey

---

## Task 1: Create DualModePartitionStrategy Header

**Files:**
- Create: `include/cedar/dtx/dual_mode_partition_strategy.h`

- [x] **Step 1: Write the header file**

```cpp
// Copyright 2025 The Cedar Authors
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_DTX_DUAL_MODE_PARTITION_STRATEGY_H_
#define CEDAR_DTX_DUAL_MODE_PARTITION_STRATEGY_H_

#include <memory>
#include <atomic>
#include "cedar/dtx/partition.h"
#include "partition/partition_strategy.h"
#include "partition/strategies/static_hash_strategy.h"
#include "partition/strategies/mth_stream_strategy.h"

namespace cedar {
namespace dtx {

/**
 * @brief Dual-mode partition strategy supporting StaticHash and MTHStream
 * 
 * Uses CedarGraph's native CedarKey (32B fixed length).
 */
class DualModePartitionStrategy : public PartitionStrategy {
 public:
  enum class Mode {
    STATIC_HASH,    // Simple hash(vid) % n
    MTH_STREAM,     // Temporal-aware sketch-based
    AUTO            // Auto-select based on workload
  };
  
  struct Config {
    Mode mode = Mode::STATIC_HASH;
    PartitionID num_partitions = 65536;
    
    // MTH-specific configuration
    size_t sketch_capacity = 1000000;
    double mth_alpha = 1.0;
    double mth_beta = 1.0;
    double mth_gamma = 0.0;
    double mth_eta = 0.0;
    double temporal_alpha = 0.01;
    int sketch_depth = 3;
    int sketch_width = 64;
    double fast_path_threshold = 0.6;
    double load_relaxation = 0.0;
    int decay_interval = 0;
    double decay_factor = 0.95;
    
    // Auto-switch thresholds
    uint64_t temporal_query_threshold = 100;
    double locality_ratio_threshold = 0.7;
  };
  
  explicit DualModePartitionStrategy(const Config& config);
  ~DualModePartitionStrategy() override = default;
  
  // PartitionStrategy interface
  PartitionID ComputePartition(const CedarKey& key, 
                                PartitionID num_partitions) override;
  std::string Name() const override;
  
  // Mode management
  void SetMode(Mode mode);
  Mode GetMode() const { return mode_.load(); }
  
  // Stats for AUTO mode
  void UpdateQueryStats(bool is_temporal_query, bool has_locality);
  
  // Get statistics
  std::string GetStats() const;

 private:
  Config config_;
  std::atomic<Mode> mode_;
  
  // Sub-strategies
  std::unique_ptr<cedar::partition::StaticHashStrategy> static_hash_;
  std::unique_ptr<cedar::partition::MTHStreamStrategy> mth_stream_;
  
  // Query statistics for AUTO mode
  struct QueryStats {
    std::atomic<uint64_t> total{0};
    std::atomic<uint64_t> temporal{0};
    std::atomic<uint64_t> locality{0};
  } stats_;
  
  // Convert CedarKey to GraphEvent for MTH processing
  cedar::partition::GraphEvent ConvertToGraphEvent(const CedarKey& key);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_DUAL_MODE_PARTITION_STRATEGY_H_
```

- [x] **Step 2: Verify file compiles**

Run: `cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar -j4 2>&1 | head -30`
Expected: No compilation errors

---

## Task 2: Implement DualModePartitionStrategy

**Files:**
- Create: `src/dtx/dual_mode_partition_strategy.cc`

- [x] **Step 3: Write the implementation**

```cpp
// Copyright 2025 The Cedar Authors
// Licensed under the Apache License, Version 2.0

#include "cedar/dtx/dual_mode_partition_strategy.h"
#include <sstream>

namespace cedar {
namespace dtx {

DualModePartitionStrategy::DualModePartitionStrategy(const Config& config)
    : config_(config),
      mode_(config.mode) {
  
  // Initialize StaticHash strategy
  static_hash_ = std::make_unique<cedar::partition::StaticHashStrategy>(
      config.num_partitions);
  
  // Initialize MTHStream strategy
  cedar::partition::MTHStreamStrategy::Config mth_config;
  mth_config.num_partitions = config.num_partitions;
  mth_config.capacity = config.sketch_capacity;
  mth_config.alpha = config.mth_alpha;
  mth_config.beta = config.mth_beta;
  mth_config.gamma = config.mth_gamma;
  mth_config.eta = config.mth_eta;
  mth_config.temporal_alpha = config.temporal_alpha;
  mth_config.sketch_depth = config.sketch_depth;
  mth_config.sketch_width = config.sketch_width;
  mth_config.fast_path_threshold = config.fast_path_threshold;
  mth_config.load_relaxation = config.load_relaxation;
  mth_config.decay_interval = config.decay_interval;
  mth_config.decay_factor = config.decay_factor;
  
  mth_stream_ = std::make_unique<cedar::partition::MTHStreamStrategy>(mth_config);
}

PartitionID DualModePartitionStrategy::ComputePartition(const CedarKey& key, 
                                                         PartitionID num_partitions) {
  // If key already has distributed flag and part_id set, use it
  if (key.IsDistributed() && key.part_id() != 0) {
    return key.part_id();
  }
  
  Mode current_mode = mode_.load();
  
  switch (current_mode) {
    case Mode::STATIC_HASH: {
      // Simple hash-based routing
      auto assign = static_hash_->RouteVertex(key.entity_id());
      return static_cast<PartitionID>(assign.partition_id);
    }
    
    case Mode::MTH_STREAM: {
      // Process as event first (updates sketch)
      auto event = ConvertToGraphEvent(key);
      std::vector<cedar::partition::GraphEvent> events = {event};
      mth_stream_->ProcessEventStream(events);
      
      // Route using temporal-aware algorithm
      auto assign = mth_stream_->RouteVertexTemporal(
          key.entity_id(), 
          key.timestamp().value());
      return static_cast<PartitionID>(assign.partition_id);
    }
    
    case Mode::AUTO: {
      // Auto-select based on statistics
      if (stats_.total > config_.temporal_query_threshold) {
        double temporal_ratio = static_cast<double>(stats_.temporal.load()) 
                                / stats_.total.load();
        if (temporal_ratio > 0.5) {
          auto event = ConvertToGraphEvent(key);
          std::vector<cedar::partition::GraphEvent> events = {event};
          mth_stream_->ProcessEventStream(events);
          
          auto assign = mth_stream_->RouteVertexTemporal(
              key.entity_id(), 
              key.timestamp().value());
          return static_cast<PartitionID>(assign.partition_id);
        }
      }
      
      auto assign = static_hash_->RouteVertex(key.entity_id());
      return static_cast<PartitionID>(assign.partition_id);
    }
  }
  
  // Fallback to simple hash
  return static_cast<PartitionID>(key.entity_id() % num_partitions);
}

std::string DualModePartitionStrategy::Name() const {
  switch (mode_.load()) {
    case Mode::STATIC_HASH: return "DualMode(StaticHash)";
    case Mode::MTH_STREAM: return "DualMode(MTHStream)";
    case Mode::AUTO: return "DualMode(Auto)";
  }
  return "DualMode(Unknown)";
}

void DualModePartitionStrategy::SetMode(Mode mode) {
  mode_.store(mode);
}

void DualModePartitionStrategy::UpdateQueryStats(bool is_temporal_query, 
                                                  bool has_locality) {
  stats_.total++;
  if (is_temporal_query) stats_.temporal++;
  if (has_locality) stats_.locality++;
}

std::string DualModePartitionStrategy::GetStats() const {
  std::ostringstream oss;
  oss << "DualModePartitionStrategy Stats:\n"
      << "  Mode: " << Name() << "\n"
      << "  Total Queries: " << stats_.total.load() << "\n"
      << "  Temporal Queries: " << stats_.temporal.load() << "\n"
      << "  Locality Queries: " << stats_.locality.load() << "\n";
  
  auto mth_stats = mth_stream_->GetStats();
  if (mth_stats.ok()) {
    oss << "\n" << mth_stats.ValueOrDie() << "\n";
  }
  
  return oss.str();
}

 cedar::partition::GraphEvent DualModePartitionStrategy::ConvertToGraphEvent(
    const CedarKey& key) {
  cedar::partition::GraphEvent event;
  event.entity_id = key.entity_id();
  event.target_id = key.target_id();
  event.timestamp = key.timestamp().value();
  event.type_id = key.column_id();
  
  // Convert EntityType to uint8
  switch (key.entity_type()) {
    case EntityType::Vertex:
      event.entity_type = 0;
      break;
    case EntityType::EdgeOut:
      event.entity_type = 1;
      break;
    case EntityType::EdgeIn:
      event.entity_type = 2;
      break;
  }
  
  // Get op type from flags
  event.op_type = key.GetOpType();
  
  return event;
}

}  // namespace dtx
}  // namespace cedar
```

- [x] **Step 4: Verify implementation compiles**

Run: `cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar -j4 2>&1 | tail -30`
Expected: No errors, libcedar.a built successfully

---

## Task 3: Update CMakeLists.txt

**Files:**
- Modify: `CMakeLists.txt`

- [x] **Step 5: Add new source file to CEDAR_DTX_SOURCES**

Add `src/dtx/dual_mode_partition_strategy.cc` to the `CEDAR_DTX_SOURCES` list in CMakeLists.txt.

---

## Task 4: Create Unit Test

**Files:**
- Create: `tests/test_dual_mode_partition_strategy.cpp`

- [x] **Step 6: Write basic test**

```cpp
// Test DualModePartitionStrategy
#include <iostream>
#include <cassert>
#include "cedar/dtx/dual_mode_partition_strategy.h"
#include "cedar/types/cedar_key.h"

using namespace cedar;
using namespace cedar::dtx;

int main() {
  std::cout << "=== DualModePartitionStrategy Test ===" << std::endl;
  
  // Test 1: StaticHash mode
  {
    DualModePartitionStrategy::Config config;
    config.mode = DualModePartitionStrategy::Mode::STATIC_HASH;
    config.num_partitions = 100;
    
    DualModePartitionStrategy strategy(config);
    
    CedarKey key = CedarKey::Vertex(42, 0_vcol, Timestamp::Now());
    PartitionID pid = strategy.ComputePartition(key, 100);
    
    assert(pid == 42);  // 42 % 100 = 42
    std::cout << "✓ StaticHash mode: Vertex 42 -> Partition " << pid << std::endl;
  }
  
  // Test 2: MTHStream mode
  {
    DualModePartitionStrategy::Config config;
    config.mode = DualModePartitionStrategy::Mode::MTH_STREAM;
    config.num_partitions = 10;
    
    DualModePartitionStrategy strategy(config);
    
    CedarKey key = CedarKey::Vertex(1, 0_vcol, Timestamp::Now());
    PartitionID pid = strategy.ComputePartition(key, 10);
    
    std::cout << "✓ MTHStream mode: Vertex 1 -> Partition " << pid << std::endl;
  }
  
  // Test 3: Mode switching
  {
    DualModePartitionStrategy::Config config;
    config.mode = DualModePartitionStrategy::Mode::STATIC_HASH;
    config.num_partitions = 100;
    
    DualModePartitionStrategy strategy(config);
    
    assert(strategy.GetMode() == DualModePartitionStrategy::Mode::STATIC_HASH);
    
    strategy.SetMode(DualModePartitionStrategy::Mode::MTH_STREAM);
    assert(strategy.GetMode() == DualModePartitionStrategy::Mode::MTH_STREAM);
    
    std::cout << "✓ Mode switching works" << std::endl;
  }
  
  std::cout << "\nAll tests passed!" << std::endl;
  return 0;
}
```

- [x] **Step 7: Build and run test**

Run: 
```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build
cmake --build . --target test_dual_mode_partition_strategy -j4
./test_dual_mode_partition_strategy
```
Expected: All tests pass

---

## Success Criteria

- [ ] `DualModePartitionStrategy` compiles without errors
- [ ] Unit tests pass
- [ ] Both StaticHash and MTHStream modes work
- [ ] Mode switching works at runtime
- [ ] Uses CedarGraph CedarKey exclusively (no subgraph2 CedarKey)
