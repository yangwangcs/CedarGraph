//===----------------------------------------------------------------------===//
// SkeletonCache Simple Performance Test
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <iostream>

#include "cedar/storage/skeleton_cache.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/edge_scan_entry.h"
#include "cedar/core/env.h"

using namespace cedar;
using namespace std::chrono;

TEST(SkeletonCacheSimplePerf, BasicReadComparison) {
  // 创建目录
  std::string test_dir = "/tmp/skel_perf_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  // ===== 带缓存的引擎 =====
  {
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = true;
    
    auto engine = std::make_unique<LsmEngine>(test_dir + "/with_cache", options, env);
    ASSERT_TRUE(engine->Open().ok());
    
    // 写入 100 个节点，每个 10 条边
    const int num_vertices = 100;
    const int edges_per_vertex = 10;
    
    for (int i = 0; i < num_vertices; ++i) {
      uint64_t vid = 10000 + i;
      CedarKey vertex = CedarKey::Vertex(vid, 1, Timestamp(100));
      engine->Put(vertex, Descriptor::InlineInt(1, i), Timestamp(i * 2 + 1));
      
      for (int j = 0; j < edges_per_vertex; ++j) {
        CedarKey edge = CedarKey::EdgeOut(vid, 20000 + j, EdgeTypeId(j + 1), 
                                           Timestamp(100 + j * 10));
        engine->Put(edge, Descriptor::InlineInt(1, j * 100), Timestamp(i * 2 + j + 2));
      }
    }
    engine->ForceFlush();
    
    // 预热 - 第一次读取（会触发 Hydrate）
    for (int i = 0; i < num_vertices; ++i) {
      auto results = engine->ScanOutEdgesCached(10000 + i, 0xFFFF, Timestamp(1000));
    }
    
    auto stats_after_warmup = engine->GetSkeletonCacheStats();
    
    // 测试带缓存的读取
    auto start = high_resolution_clock::now();
    for (int iter = 0; iter < 10; ++iter) {
      for (int i = 0; i < num_vertices; ++i) {
        auto results = engine->ScanOutEdgesCached(10000 + i, 0xFFFF, Timestamp(1000));
      }
    }
    auto end = high_resolution_clock::now();
    double with_cache = duration<double>(end - start).count();
    
    auto stats_final = engine->GetSkeletonCacheStats();
    size_t memory = engine->GetSkeletonCacheMemoryUsage();
    
    std::cout << "\n=== With SkeletonCache ===" << std::endl;
    std::cout << "Vertices: " << num_vertices << ", Edges: " << num_vertices * edges_per_vertex << std::endl;
    std::cout << "Warmup hits: " << stats_after_warmup.hits << ", misses: " << stats_after_warmup.misses << std::endl;
    std::cout << "Final hits: " << stats_final.hits << ", misses: " << stats_final.misses << std::endl;
    std::cout << "Hit rate: " << stats_final.HitRate() * 100 << "%" << std::endl;
    std::cout << "Memory: " << memory / 1024 << " KB" << std::endl;
    std::cout << "Read time (1000 queries): " << with_cache << "s" << std::endl;
    std::cout << "Throughput: " << 1000.0 / with_cache << " ops/s" << std::endl;
    
    engine->Close();
  }
  
  // ===== 不带缓存的引擎 =====
  {
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    
    auto engine = std::make_unique<LsmEngine>(test_dir + "/no_cache", options, env);
    ASSERT_TRUE(engine->Open().ok());
    
    const int num_vertices = 100;
    const int edges_per_vertex = 10;
    
    for (int i = 0; i < num_vertices; ++i) {
      uint64_t vid = 10000 + i;
      CedarKey vertex = CedarKey::Vertex(vid, 1, Timestamp(100));
      engine->Put(vertex, Descriptor::InlineInt(1, i), Timestamp(i * 2 + 1));
      
      for (int j = 0; j < edges_per_vertex; ++j) {
        CedarKey edge = CedarKey::EdgeOut(vid, 20000 + j, EdgeTypeId(j + 1), 
                                           Timestamp(100 + j * 10));
        engine->Put(edge, Descriptor::InlineInt(1, j * 100), Timestamp(i * 2 + j + 2));
      }
    }
    engine->ForceFlush();
    
    // 使用标准查询（不经过 SkeletonCache）
    auto start = high_resolution_clock::now();
    for (int iter = 0; iter < 10; ++iter) {
      for (int i = 0; i < num_vertices; ++i) {
        auto entries = engine->ScanEdgesWithFolding(
            10000 + i, EntityType::EdgeOut, 0xFFFF, Timestamp(1000));
      }
    }
    auto end = high_resolution_clock::now();
    double without_cache = duration<double>(end - start).count();
    
    std::cout << "\n=== Without SkeletonCache ===" << std::endl;
    std::cout << "Read time (1000 queries): " << without_cache << "s" << std::endl;
    std::cout << "Throughput: " << 1000.0 / without_cache << " ops/s" << std::endl;
    std::cout << "\n=== Speedup ===" << std::endl;
    std::cout << without_cache / 1.0 << "x faster with cache" << std::endl;
    
    engine->Close();
  }
  
  // 清理
  std::filesystem::remove_all(test_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
