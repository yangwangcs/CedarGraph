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
// 综合读写性能测试
// =============================================================================

#include <gtest/gtest.h>
#include <chrono>
#include <random>
#include <vector>
#include "cedar/storage/lsm_engine.h"
#include "cedar/query/cedar_scan.h"
#include "cedar/storage/skeleton_cache.h"

using namespace cedar;

class RWPerformanceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/cedar_rw_perf_test_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = true;
    options.skeleton_cache_shards = 1024;
    options.skeleton_cache_entries_per_shard = 2048;
    
    engine_ = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
    EXPECT_TRUE(engine_->Open().ok());
  }

  void TearDown() override {
    engine_->Close();
    std::system(("rm -rf " + test_dir_).c_str());
  }

  std::string test_dir_;
  std::unique_ptr<LsmEngine> engine_;
};

TEST_F(RWPerformanceTest, WriteThroughput) {
  constexpr int NUM_ENTITIES = 10000;
  constexpr int COLUMNS_PER_ENTITY = 10;
  constexpr int TOTAL_RECORDS = NUM_ENTITIES * COLUMNS_PER_ENTITY;
  
  std::cout << "\n=== Write Performance Test ===" << std::endl;
  std::cout << "Entities: " << NUM_ENTITIES << ", Columns per entity: " << COLUMNS_PER_ENTITY << std::endl;
  
  // Write records
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < NUM_ENTITIES; i++) {
    uint64_t entity_id = 10000 + i;
    for (int j = 0; j < COLUMNS_PER_ENTITY; j++) {
      CedarKey key;
      key.SetEntityId(entity_id);
      key.SetTimestamp(Timestamp(1000000 + j));
      key.SetColumnId(j);
      key.SetEntityType(static_cast<uint8_t>(EntityType::Vertex));
      key.SetSequence(0);
      
      Descriptor desc = Descriptor::InlineInt(0, i * 1000 + j);
      
      auto status = engine_->Put(key, desc, Timestamp(1));
      EXPECT_TRUE(status.ok());
    }
  }
  auto write_end = std::chrono::steady_clock::now();
  
  // Force flush
  engine_->ForceFlush();
  auto flush_end = std::chrono::steady_clock::now();
  
  double write_time = std::chrono::duration<double>(write_end - start).count();
  double flush_time = std::chrono::duration<double>(flush_end - write_end).count();
  
  std::cout << "Write time: " << write_time << "s (" << TOTAL_RECORDS / write_time / 1000 << " kops/s)" << std::endl;
  std::cout << "ForceFlush: " << flush_time << "s" << std::endl;
  std::cout << "Total throughput: " << TOTAL_RECORDS / (write_time + flush_time) / 1000 << " kops/s" << std::endl;
}

TEST_F(RWPerformanceTest, ReadThroughput) {
  constexpr int NUM_ENTITIES = 5000;
  constexpr int COLUMNS_PER_ENTITY = 10;
  
  // Setup: write data
  for (int i = 0; i < NUM_ENTITIES; i++) {
    uint64_t entity_id = 10000 + i;
    for (int j = 0; j < COLUMNS_PER_ENTITY; j++) {
      CedarKey key;
      key.SetEntityId(entity_id);
      key.SetTimestamp(Timestamp(1000000 + j));
      key.SetColumnId(j);
      key.SetEntityType(static_cast<uint8_t>(EntityType::Vertex));
      key.SetSequence(0);
      
      Descriptor desc = Descriptor::InlineInt(0, i * 1000 + j);
      
      engine_->Put(key, desc, Timestamp(1));
    }
  }
  engine_->ForceFlush();
  
  std::cout << "\n=== Read Performance Test ===" << std::endl;
  std::cout << "Dataset: " << NUM_ENTITIES << " entities, " << NUM_ENTITIES * COLUMNS_PER_ENTITY << " records" << std::endl;
  
  // Test 1: Sequential read (CedarScan::GetNode)
  {
    CedarScan scan = CedarScan::Now(engine_.get());
    
    auto start = std::chrono::steady_clock::now();
    int found = 0;
    for (int i = 0; i < NUM_ENTITIES; i++) {
      auto node = scan.GetNode(10000 + i);
      if (node.has_value()) found++;
    }
    auto end = std::chrono::steady_clock::now();
    
    double time = std::chrono::duration<double>(end - start).count();
    std::cout << "\n--- Sequential Read (CedarScan::GetNode) ---" << std::endl;
    std::cout << "Found: " << found << "/" << NUM_ENTITIES << std::endl;
    std::cout << "Time: " << time << "s" << std::endl;
    std::cout << "Throughput: " << NUM_ENTITIES / time / 1000 << " kops/s" << std::endl;
  }
  
  // Test 2: Random read
  {
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, NUM_ENTITIES - 1);
    
    CedarScan scan = CedarScan::Now(engine_.get());
    
    auto start = std::chrono::steady_clock::now();
    int found = 0;
    for (int i = 0; i < NUM_ENTITIES; i++) {
      int idx = dist(rng);
      auto node = scan.GetNode(10000 + idx);
      if (node.has_value()) found++;
    }
    auto end = std::chrono::steady_clock::now();
    
    double time = std::chrono::duration<double>(end - start).count();
    std::cout << "\n--- Random Read (CedarScan::GetNode) ---" << std::endl;
    std::cout << "Found: " << found << "/" << NUM_ENTITIES << std::endl;
    std::cout << "Time: " << time << "s" << std::endl;
    std::cout << "Throughput: " << NUM_ENTITIES / time / 1000 << " kops/s" << std::endl;
  }
  
  // Test 3: Column-based read
  {
    CedarScan scan = CedarScan::Now(engine_.get());
    
    auto start = std::chrono::steady_clock::now();
    int total_cols = 0;
    for (int i = 0; i < 1000; i++) {
      uint64_t entity_id = 10000 + i;
      auto node = scan.GetNode(entity_id);
      if (node.has_value()) total_cols++;
    }
    auto end = std::chrono::steady_clock::now();
    
    double time = std::chrono::duration<double>(end - start).count();
    std::cout << "\n--- Column Read (CedarScan::GetNodeColumns) ---" << std::endl;
    std::cout << "Scanned: 1000 entities, found " << total_cols << " columns" << std::endl;
    std::cout << "Time: " << time << "s" << std::endl;
    std::cout << "Throughput: " << 1000 / time << " ops/s" << std::endl;
  }
}

TEST_F(RWPerformanceTest, CachePerformance) {
  constexpr int NUM_ENTITIES = 10000;
  constexpr int COLUMNS_PER_ENTITY = 10;
  
  // Setup: write data
  for (int i = 0; i < NUM_ENTITIES; i++) {
    uint64_t entity_id = 10000 + i;
    for (int j = 0; j < COLUMNS_PER_ENTITY; j++) {
      CedarKey key;
      key.SetEntityId(entity_id);
      key.SetTimestamp(Timestamp(1000000 + j));
      key.SetColumnId(j);
      key.SetEntityType(static_cast<uint8_t>(EntityType::Vertex));
      key.SetSequence(0);
      
      Descriptor desc = Descriptor::InlineInt(0, i * 1000 + j);
      
      engine_->Put(key, desc, Timestamp(1));
    }
  }
  engine_->ForceFlush();
  
  std::cout << "\n=== Cache Performance Test ===" << std::endl;
  
  // First pass - cold cache
  {
    CedarScan scan = CedarScan::Now(engine_.get());
    
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < NUM_ENTITIES; i++) {
      scan.GetNode(10000 + i);
    }
    auto end = std::chrono::steady_clock::now();
    
    double time = std::chrono::duration<double>(end - start).count();
    std::cout << "\n--- First Pass (Cold Cache) ---" << std::endl;
    std::cout << "Time: " << time << "s (" << NUM_ENTITIES / time / 1000 << " kops/s)" << std::endl;
  }
  
  // Second pass - warm cache
  {
    CedarScan scan = CedarScan::Now(engine_.get());
    
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < NUM_ENTITIES; i++) {
      scan.GetNode(10000 + i);
    }
    auto end = std::chrono::steady_clock::now();
    
    double time = std::chrono::duration<double>(end - start).count();
    std::cout << "\n--- Second Pass (Warm Cache) ---" << std::endl;
    std::cout << "Time: " << time << "s (" << NUM_ENTITIES / time / 1000 << " kops/s)" << std::endl;
  }
  
  // Third pass - hot cache
  {
    CedarScan scan = CedarScan::Now(engine_.get());
    
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < NUM_ENTITIES; i++) {
      scan.GetNode(10000 + i);
    }
    auto end = std::chrono::steady_clock::now();
    
    double time = std::chrono::duration<double>(end - start).count();
    std::cout << "\n--- Third Pass (Hot Cache) ---" << std::endl;
    std::cout << "Time: " << time << "s (" << NUM_ENTITIES / time / 1000 << " kops/s)" << std::endl;
  }
  
  // Get cache stats
  auto* cache = engine_->GetSkeletonCache();
  if (cache) {
    auto stats = cache->GetStats();
    std::cout << "\n--- SkeletonCache Stats ---" << std::endl;
    std::cout << "Total Entries: " << stats.TotalEntries() << std::endl;
    std::cout << "Hits: " << stats.hits << std::endl;
    std::cout << "Misses: " << stats.misses << std::endl;
    std::cout << "Hit rate: " << (stats.hits * 100.0 / (stats.hits + stats.misses)) << "%" << std::endl;
    std::cout << "Hit Rate: " << stats.HitRate() * 100 << "%" << std::endl;
  }
}

TEST_F(RWPerformanceTest, PointQueryPerformance) {
  constexpr int NUM_ENTITIES = 5000;
  
  // Setup: write data (one record per entity)
  for (int i = 0; i < NUM_ENTITIES; i++) {
    uint64_t entity_id = 10000 + i;
    CedarKey key;
    key.SetEntityId(entity_id);
    key.SetTimestamp(Timestamp(1000000));
    key.SetColumnId(0);
    key.SetEntityType(static_cast<uint8_t>(EntityType::Vertex));
    key.SetSequence(0);
    
    Descriptor desc = Descriptor::InlineInt(0, i);
    
    engine_->Put(key, desc, Timestamp(1));
  }
  engine_->ForceFlush();
  
  std::cout << "\n=== Point Query Performance Test ===" << std::endl;
  std::cout << "Dataset: " << NUM_ENTITIES << " entities" << std::endl;
  
  // Point query via LsmEngine::GetRecordAtTime
  {
    auto start = std::chrono::steady_clock::now();
    int found = 0;
    for (int i = 0; i < NUM_ENTITIES; i++) {
      auto result = engine_->GetRecordAtTime(10000 + i, EntityType::Vertex, 0, Timestamp(1000001));
      if (result.has_value()) found++;
    }
    auto end = std::chrono::steady_clock::now();
    
    double time = std::chrono::duration<double>(end - start).count();
    std::cout << "\n--- Point Query (GetRecordAtTime) ---" << std::endl;
    std::cout << "Found: " << found << "/" << NUM_ENTITIES << std::endl;
    std::cout << "Time: " << time << "s" << std::endl;
    std::cout << "Throughput: " << NUM_ENTITIES / time / 1000 << " kops/s" << std::endl;
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
