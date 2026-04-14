//===----------------------------------------------------------------------===//
// SkeletonCache Integration Test
// 测试 SkeletonCache 与 LsmEngine 的集成
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <iostream>

#include "cedar/storage/skeleton_cache.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/env.h"

using namespace cedar;
using namespace std::chrono;

class SkeletonCacheIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/skel_int_test_" + 
        std::to_string(system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);
    env_ = cedar::Env::Default();
  }
  
  void TearDown() override {
    if (engine_) {
      engine_->Close();
    }
    std::filesystem::remove_all(test_dir_);
  }
  
  std::string test_dir_;
  cedar::Env* env_;
  std::unique_ptr<LsmEngine> engine_;
};

// 基础集成测试
TEST_F(SkeletonCacheIntegrationTest, BasicIntegration) {
  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = true;
  
  engine_ = std::make_unique<LsmEngine>(test_dir_, options, env_);
  ASSERT_TRUE(engine_->Open().ok());
  
  // 写入一些数据
  const int num_vertices = 10;
  const int edges_per_vertex = 5;
  
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t vid = 10000 + i;
    
    // 写入节点
    CedarKey vertex = CedarKey::Vertex(vid, 1, Timestamp(100));
    ASSERT_TRUE(engine_->Put(vertex, Descriptor::InlineInt(1, i), Timestamp(1)).ok());
    
    // 写入边
    for (int j = 0; j < edges_per_vertex; ++j) {
      CedarKey edge = CedarKey::EdgeOut(vid, 20000 + j, EdgeTypeId(1), Timestamp(100 + j));
      ASSERT_TRUE(engine_->Put(edge, Descriptor::InlineInt(1, j), Timestamp(2)).ok());
    }
  }
  
  // 强制刷盘
  ASSERT_TRUE(engine_->ForceFlush().ok());
  
  // 等待一下确保数据写入完成
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  // 使用 SkeletonCache 读取
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t vid = 10000 + i;
    auto results = engine_->ScanOutEdgesCached(vid, 0xFFFF, Timestamp(1000));
    
    // 验证读取到边
    EXPECT_EQ(results.size(), edges_per_vertex) << "Failed for vid=" << vid;
  }
  
  // 验证缓存统计
  auto stats = engine_->GetSkeletonCacheStats();
  EXPECT_GE(stats.misses, num_vertices);  // 至少每个节点 miss 一次
  
  size_t memory = engine_->GetSkeletonCacheMemoryUsage();
  EXPECT_GT(memory, 0);
  
  std::cout << "\n=== Integration Test Results ===" << std::endl;
  std::cout << "Cached vertices: " << stats.TotalEntries() << std::endl;
  std::cout << "Cache hits: " << stats.hits << std::endl;
  std::cout << "Cache misses: " << stats.misses << std::endl;
  std::cout << "Hit rate: " << stats.HitRate() * 100 << "%" << std::endl;
  std::cout << "Memory used: " << memory / 1024 << " KB" << std::endl;
}

// 性能测试
TEST_F(SkeletonCacheIntegrationTest, PerformanceTest) {
  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = true;
  
  engine_ = std::make_unique<LsmEngine>(test_dir_, options, env_);
  ASSERT_TRUE(engine_->Open().ok());
  
  const int num_vertices = 50;
  const int edges_per_vertex = 10;
  
  // 写入数据
  auto write_start = high_resolution_clock::now();
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t vid = 10000 + i;
    CedarKey vertex = CedarKey::Vertex(vid, 1, Timestamp(100));
    engine_->Put(vertex, Descriptor::InlineInt(1, i), Timestamp(1));
    
    for (int j = 0; j < edges_per_vertex; ++j) {
      CedarKey edge = CedarKey::EdgeOut(vid, 20000 + j, EdgeTypeId(1), Timestamp(100 + j));
      engine_->Put(edge, Descriptor::InlineInt(1, j), Timestamp(2));
    }
  }
  engine_->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto write_end = high_resolution_clock::now();
  double write_time = duration<double>(write_end - write_start).count();
  
  // 预热 - 第一次读取
  for (int i = 0; i < num_vertices; ++i) {
    engine_->ScanOutEdgesCached(10000 + i, 0xFFFF, Timestamp(1000));
  }
  auto warmup_stats = engine_->GetSkeletonCacheStats();
  
  // 性能测试 - 缓存读取
  const int iterations = 10;
  auto read_start = high_resolution_clock::now();
  for (int iter = 0; iter < iterations; ++iter) {
    for (int i = 0; i < num_vertices; ++i) {
      engine_->ScanOutEdgesCached(10000 + i, 0xFFFF, Timestamp(1000));
    }
  }
  auto read_end = high_resolution_clock::now();
  double read_time = duration<double>(read_end - read_start).count();
  
  auto final_stats = engine_->GetSkeletonCacheStats();
  size_t memory = engine_->GetSkeletonCacheMemoryUsage();
  
  std::cout << "\n=== Performance Test Results ===" << std::endl;
  std::cout << "Vertices: " << num_vertices << ", Edges: " << num_vertices * edges_per_vertex << std::endl;
  std::cout << "Write time: " << write_time << "s" << std::endl;
  std::cout << "Warmup - Hits: " << warmup_stats.hits << ", Misses: " << warmup_stats.misses << std::endl;
  std::cout << "Read time (" << iterations * num_vertices << " queries): " << read_time << "s" << std::endl;
  std::cout << "Read throughput: " << (iterations * num_vertices) / read_time << " ops/sec" << std::endl;
  std::cout << "Final hit rate: " << final_stats.HitRate() * 100 << "%" << std::endl;
  std::cout << "Memory used: " << memory / 1024 << " KB" << std::endl;
  
  // 验证性能
  EXPECT_GT((iterations * num_vertices) / read_time, 1000);  // 至少 1000 ops/sec
}

// 缓存失效测试
TEST_F(SkeletonCacheIntegrationTest, CacheInvalidationTest) {
  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = true;
  
  engine_ = std::make_unique<LsmEngine>(test_dir_, options, env_);
  ASSERT_TRUE(engine_->Open().ok());
  
  uint64_t vid = 10000;
  
  // 写入节点和边
  CedarKey vertex = CedarKey::Vertex(vid, 1, Timestamp(100));
  engine_->Put(vertex, Descriptor::InlineInt(1, 42), Timestamp(1));
  
  CedarKey edge = CedarKey::EdgeOut(vid, 20000, EdgeTypeId(1), Timestamp(110));
  engine_->Put(edge, Descriptor::InlineInt(1, 100), Timestamp(2));
  
  engine_->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  // 第一次读取 - 缓存 miss
  auto results1 = engine_->ScanOutEdgesCached(vid, 0xFFFF, Timestamp(200));
  EXPECT_EQ(results1.size(), 1);
  
  auto stats1 = engine_->GetSkeletonCacheStats();
  EXPECT_EQ(stats1.misses, 1);
  
  // 第二次读取 - 缓存 hit
  auto results2 = engine_->ScanOutEdgesCached(vid, 0xFFFF, Timestamp(200));
  EXPECT_EQ(results2.size(), 1);
  
  auto stats2 = engine_->GetSkeletonCacheStats();
  EXPECT_EQ(stats2.hits, 1);
  
  // 标记删除 - 使缓存失效
  engine_->MarkEntityActive(vid);
  
  // 第三次读取 - 应该重新加载
  auto results3 = engine_->ScanOutEdgesCached(vid, 0xFFFF, Timestamp(200));
  // 注意：MarkEntityActive 会使缓存失效，但数据仍在 SST，所以会重新 Hydrate
  
  std::cout << "\n=== Invalidation Test Results ===" << std::endl;
  std::cout << "First read (miss): size=" << results1.size() << std::endl;
  std::cout << "Second read (hit): size=" << results2.size() << std::endl;
  std::cout << "After invalidation: size=" << results3.size() << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
