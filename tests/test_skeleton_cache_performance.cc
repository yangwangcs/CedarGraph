//===----------------------------------------------------------------------===//
// SkeletonCache Performance Tests
// 测试 SkeletonCache 在不同场景下的性能表现
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <thread>
#include <vector>
#include <random>

#include "cedar/storage/skeleton_cache.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/edge_scan_entry.h"
#include "cedar/core/env.h"

using namespace cedar;
using namespace std::chrono;

class SkeletonCachePerfTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/skeleton_perf_test_" + 
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
  
  // 创建带 SkeletonCache 的引擎
  void CreateEngineWithCache() {
    CedarOptions options;
    options.create_if_missing = true;
    options.write_buffer_size = 16 * 1024 * 1024;  // 16MB
    options.enable_skeleton_cache = true;
    options.skeleton_cache_shards = 1024;
    options.skeleton_cache_entries_per_shard = 1024;
    
    engine_ = std::make_unique<LsmEngine>(test_dir_ + "_with_cache", options, env_);
    ASSERT_TRUE(engine_->Open().ok());
  }
  
  // 创建不带 SkeletonCache 的引擎
  void CreateEngineWithoutCache() {
    CedarOptions options;
    options.create_if_missing = true;
    options.write_buffer_size = 16 * 1024 * 1024;  // 16MB
    options.enable_skeleton_cache = false;  // 禁用缓存
    
    engine_ = std::make_unique<LsmEngine>(test_dir_ + "_no_cache", options, env_);
    ASSERT_TRUE(engine_->Open().ok());
  }
  
  // 批量写入节点和边
  void WriteGraphData(int num_vertices, int edges_per_vertex) {
    for (int i = 0; i < num_vertices; ++i) {
      uint64_t vid = 10000 + i;
      
      // 写入节点
      CedarKey vertex = CedarKey::Vertex(vid, 1, Timestamp(100));
      engine_->Put(vertex, Descriptor::InlineInt(1, i), Timestamp(i * 2 + 1));
      
      // 写入边
      for (int j = 0; j < edges_per_vertex; ++j) {
        uint64_t dst_id = 20000 + (i * edges_per_vertex + j) % 10000;
        CedarKey edge = CedarKey::EdgeOut(vid, dst_id, EdgeTypeId(j % 10 + 1), 
                                           Timestamp(100 + j * 10));
        engine_->Put(edge, Descriptor::InlineInt(1, j * 100), Timestamp(i * 2 + j + 2));
      }
    }
    
    // 强制刷盘
    engine_->ForceFlush();
  }
  
  // 使用 CedarScan 进行标准查询（不依赖 SkeletonCache）
  std::vector<std::tuple<uint64_t, Timestamp, Descriptor, uint16_t>> 
  ScanEdgesStandard(uint64_t vertex_id, Timestamp snapshot_ts) {
    // 使用 LsmEngine 的 ScanEdgesWithFolding
    auto entries = engine_->ScanEdgesWithFolding(
        vertex_id, EntityType::EdgeOut, 0xFFFF, snapshot_ts);
    
    std::vector<std::tuple<uint64_t, Timestamp, Descriptor, uint16_t>> results;
    for (const auto& entry : entries) {
      results.emplace_back(entry.target_id, entry.timestamp, entry.descriptor, entry.edge_type);
    }
    return results;
  }
  
  // 顺序读取测试
  double SequentialReadTest(int num_vertices, int iterations) {
    auto start = high_resolution_clock::now();
    
    for (int iter = 0; iter < iterations; ++iter) {
      for (int i = 0; i < num_vertices; ++i) {
        uint64_t vid = 10000 + i;
        auto results = engine_->ScanOutEdgesCached(vid, 0xFFFF, Timestamp(1000));
        // 如果缓存未启用或返回空，使用标准查询
        if (results.empty()) {
          results = ScanEdgesStandard(vid, Timestamp(1000));
        }
        // 防止编译器优化
        volatile size_t count = results.size();
        (void)count;
      }
    }
    
    auto end = high_resolution_clock::now();
    duration<double> elapsed = end - start;
    return elapsed.count();
  }
  
  // 随机读取测试
  double RandomReadTest(int num_vertices, int iterations) {
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, num_vertices - 1);
    
    auto start = high_resolution_clock::now();
    
    for (int iter = 0; iter < iterations; ++iter) {
      for (int i = 0; i < num_vertices; ++i) {
        uint64_t vid = 10000 + dist(rng);
        auto results = engine_->ScanOutEdgesCached(vid, 0xFFFF, Timestamp(1000));
        if (results.empty()) {
          results = ScanEdgesStandard(vid, Timestamp(1000));
        }
        volatile size_t count = results.size();
        (void)count;
      }
    }
    
    auto end = high_resolution_clock::now();
    duration<double> elapsed = end - start;
    return elapsed.count();
  }
  
  // 并发读取测试
  double ConcurrentReadTest(int num_vertices, int num_threads, int ops_per_thread) {
    std::vector<std::thread> threads;
    std::atomic<int64_t> total_results{0};
    
    auto start = high_resolution_clock::now();
    
    for (int t = 0; t < num_threads; ++t) {
      threads.emplace_back([this, t, num_vertices, ops_per_thread, &total_results]() {
        std::mt19937 rng(42 + t);
        std::uniform_int_distribution<int> dist(0, num_vertices - 1);
        
        for (int i = 0; i < ops_per_thread; ++i) {
          uint64_t vid = 10000 + dist(rng);
          auto results = engine_->ScanOutEdgesCached(vid, 0xFFFF, Timestamp(1000));
          if (results.empty()) {
            results = ScanEdgesStandard(vid, Timestamp(1000));
          }
          total_results += results.size();
        }
      });
    }
    
    for (auto& t : threads) {
      t.join();
    }
    
    auto end = high_resolution_clock::now();
    duration<double> elapsed = end - start;
    return elapsed.count();
  }
  
  std::string test_dir_;
  cedar::Env* env_;
  std::unique_ptr<LsmEngine> engine_;
};

// ========== 测试 1: 写入性能对比 ==========
TEST_F(SkeletonCachePerfTest, DISABLED_WritePerformanceComparison) {
  const int num_vertices = 1000;
  const int edges_per_vertex = 10;
  
  // 带缓存的写入
  CreateEngineWithCache();
  auto start = high_resolution_clock::now();
  WriteGraphData(num_vertices, edges_per_vertex);
  auto end = high_resolution_clock::now();
  double with_cache = duration<double>(end - start).count();
  engine_->Close();
  engine_.reset();
  
  // 不带缓存的写入
  CreateEngineWithoutCache();
  start = high_resolution_clock::now();
  WriteGraphData(num_vertices, edges_per_vertex);
  end = high_resolution_clock::now();
  double without_cache = duration<double>(end - start).count();
  
  std::cout << "\n=== Write Performance ===" << std::endl;
  std::cout << "Vertices: " << num_vertices << ", Edges: " << num_vertices * edges_per_vertex << std::endl;
  std::cout << "With SkeletonCache: " << with_cache << "s" << std::endl;
  std::cout << "Without SkeletonCache: " << without_cache << "s" << std::endl;
  std::cout << "Overhead: " << (with_cache / without_cache - 1) * 100 << "%" << std::endl;
}

// ========== 测试 2: 顺序读取性能对比 ==========
TEST_F(SkeletonCachePerfTest, SequentialReadPerformance) {
  const int num_vertices = 500;
  const int edges_per_vertex = 10;
  const int iterations = 5;
  
  // 带缓存的读取
  CreateEngineWithCache();
  WriteGraphData(num_vertices, edges_per_vertex);
  
  // 预热缓存
  SequentialReadTest(num_vertices, 1);
  
  // 正式测试
  double with_cache = SequentialReadTest(num_vertices, iterations);
  auto stats_with = engine_->GetSkeletonCacheStats();
  size_t memory_with = engine_->GetSkeletonCacheMemoryUsage();
  
  engine_->Close();
  engine_.reset();
  
  // 不带缓存的读取
  CreateEngineWithoutCache();
  WriteGraphData(num_vertices, edges_per_vertex);
  double without_cache = SequentialReadTest(num_vertices, iterations);
  
  int64_t total_reads = num_vertices * iterations;
  std::cout << "\n=== Sequential Read Performance ===" << std::endl;
  std::cout << "Total reads: " << total_reads << std::endl;
  std::cout << "With SkeletonCache: " << with_cache << "s (" 
            << total_reads / with_cache / 1000 << " kops/s)" << std::endl;
  std::cout << "  - Cache hits: " << stats_with.hits << std::endl;
  std::cout << "  - Cache misses: " << stats_with.misses << std::endl;
  std::cout << "  - Hit rate: " << stats_with.HitRate() * 100 << "%" << std::endl;
  std::cout << "  - Memory: " << memory_with / 1024 << " KB" << std::endl;
  std::cout << "Without SkeletonCache: " << without_cache << "s ("
            << total_reads / without_cache / 1000 << " kops/s)" << std::endl;
  std::cout << "Speedup: " << without_cache / with_cache << "x" << std::endl;
  
  // 验证性能提升
  EXPECT_GT(without_cache / with_cache, 1.5);  // 至少 1.5 倍提升
}

// ========== 测试 3: 随机读取性能对比 ==========
TEST_F(SkeletonCachePerfTest, RandomReadPerformance) {
  const int num_vertices = 500;
  const int edges_per_vertex = 10;
  const int iterations = 5;
  
  // 带缓存的读取
  CreateEngineWithCache();
  WriteGraphData(num_vertices, edges_per_vertex);
  
  // 预热
  RandomReadTest(num_vertices, 1);
  
  // 正式测试
  double with_cache = RandomReadTest(num_vertices, iterations);
  auto stats_with = engine_->GetSkeletonCacheStats();
  
  engine_->Close();
  engine_.reset();
  
  // 不带缓存的读取
  CreateEngineWithoutCache();
  WriteGraphData(num_vertices, edges_per_vertex);
  double without_cache = RandomReadTest(num_vertices, iterations);
  
  int64_t total_reads = num_vertices * iterations;
  std::cout << "\n=== Random Read Performance ===" << std::endl;
  std::cout << "Total reads: " << total_reads << std::endl;
  std::cout << "With SkeletonCache: " << with_cache << "s ("
            << total_reads / with_cache / 1000 << " kops/s)" << std::endl;
  std::cout << "  - Cache hit rate: " << stats_with.HitRate() * 100 << "%" << std::endl;
  std::cout << "Without SkeletonCache: " << without_cache << "s ("
            << total_reads / without_cache / 1000 << " kops/s)" << std::endl;
  std::cout << "Speedup: " << without_cache / with_cache << "x" << std::endl;
  
  // 随机访问缓存命中率可能较低，但仍应有提升
  EXPECT_GT(without_cache / with_cache, 1.2);
}

// ========== 测试 4: 并发读取性能 ==========
TEST_F(SkeletonCachePerfTest, ConcurrentReadPerformance) {
  const int num_vertices = 500;
  const int edges_per_vertex = 10;
  const int num_threads = 4;
  const int ops_per_thread = 500;
  
  CreateEngineWithCache();
  WriteGraphData(num_vertices, edges_per_vertex);
  
  // 预热
  for (int i = 0; i < num_vertices; ++i) {
    engine_->ScanOutEdgesCached(10000 + i, 0xFFFF, Timestamp(1000));
  }
  
  // 并发测试
  double elapsed = ConcurrentReadTest(num_vertices, num_threads, ops_per_thread);
  auto stats = engine_->GetSkeletonCacheStats();
  
  int64_t total_ops = num_threads * ops_per_thread;
  std::cout << "\n=== Concurrent Read Performance ===" << std::endl;
  std::cout << "Threads: " << num_threads << std::endl;
  std::cout << "Ops per thread: " << ops_per_thread << std::endl;
  std::cout << "Total ops: " << total_ops << std::endl;
  std::cout << "Time: " << elapsed << "s" << std::endl;
  std::cout << "Throughput: " << total_ops / elapsed / 1000 << " kops/s" << std::endl;
  std::cout << "Cache hit rate: " << stats.HitRate() * 100 << "%" << std::endl;
  
  // 验证并发性能
  EXPECT_GT(total_ops / elapsed, 10000);  // 至少 10k ops/s
}

// ========== 测试 5: 不同缓存大小性能对比 ==========
TEST_F(SkeletonCachePerfTest, CacheSizeComparison) {
  const int num_vertices = 1000;
  const int edges_per_vertex = 10;
  const int cache_sizes[] = {128, 256, 512, 1024};
  
  std::cout << "\n=== Cache Size Comparison ===" << std::endl;
  std::cout << "Vertices: " << num_vertices << ", Edges per vertex: " << edges_per_vertex << std::endl;
  
  for (int cache_size : cache_sizes) {
    // 创建特定大小的缓存
    CedarOptions options;
    options.create_if_missing = true;
    options.write_buffer_size = 16 * 1024 * 1024;
    options.enable_skeleton_cache = true;
    options.skeleton_cache_shards = 64;
    options.skeleton_cache_entries_per_shard = cache_size / 64;
    
    std::string dir = test_dir_ + "_cache_" + std::to_string(cache_size);
    std::filesystem::create_directories(dir);
    
    auto engine = std::make_unique<LsmEngine>(dir, options, env_);
    ASSERT_TRUE(engine->Open().ok());
    
    // 写入数据
    for (int i = 0; i < num_vertices; ++i) {
      uint64_t vid = 10000 + i;
      CedarKey vertex = CedarKey::Vertex(vid, 1, Timestamp(100));
      engine->Put(vertex, Descriptor::InlineInt(1, i), Timestamp(i * 2 + 1));
      
      for (int j = 0; j < edges_per_vertex; ++j) {
        CedarKey edge = CedarKey::EdgeOut(vid, 20000 + j, EdgeTypeId(j + 1), 
                                           Timestamp(100 + j * 10));
        engine->Put(edge, Descriptor::InlineInt(1, j), Timestamp(i * 2 + j + 2));
      }
    }
    engine->ForceFlush();
    
    // 顺序访问所有节点
    auto start = high_resolution_clock::now();
    for (int iter = 0; iter < 3; ++iter) {
      for (int i = 0; i < num_vertices; ++i) {
        engine->ScanOutEdgesCached(10000 + i, 0xFFFF, Timestamp(1000));
      }
    }
    auto end = high_resolution_clock::now();
    double elapsed = duration<double>(end - start).count();
    
    auto stats = engine->GetSkeletonCacheStats();
    size_t memory = engine->GetSkeletonCacheMemoryUsage();
    
    std::cout << "Cache size " << cache_size << ": " << elapsed << "s, "
              << "Hit rate: " << stats.HitRate() * 100 << "%, "
              << "Memory: " << memory / 1024 << " KB" << std::endl;
    
    engine->Close();
    std::filesystem::remove_all(dir);
  }
}

// ========== 测试 6: 内存占用分析 ==========
TEST_F(SkeletonCachePerfTest, MemoryUsageAnalysis) {
  const int num_vertices = 1000;
  const int edges_per_vertex = 10;
  
  CreateEngineWithCache();
  WriteGraphData(num_vertices, edges_per_vertex);
  
  // 加载所有节点到缓存
  for (int i = 0; i < num_vertices; ++i) {
    engine_->ScanOutEdgesCached(10000 + i, 0xFFFF, Timestamp(1000));
  }
  
  size_t memory_used = engine_->GetSkeletonCacheMemoryUsage();
  auto stats = engine_->GetSkeletonCacheStats();
  
  // 计算理论内存占用
  size_t theoretical = stats.TotalEntries() * (sizeof(VertexSkeleton) + edges_per_vertex * sizeof(EdgeEntry12B));
  
  std::cout << "\n=== Memory Usage Analysis ===" << std::endl;
  std::cout << "Cached vertices: " << stats.TotalEntries() << std::endl;
  std::cout << "Total edges: " << num_vertices * edges_per_vertex << std::endl;
  std::cout << "Actual memory: " << memory_used / 1024 << " KB" << std::endl;
  std::cout << "Theoretical minimum: " << theoretical / 1024 << " KB" << std::endl;
  std::cout << "Overhead: " << (double)memory_used / theoretical * 100 - 100 << "%" << std::endl;
  std::cout << "Bytes per edge: " << (double)memory_used / (num_vertices * edges_per_vertex) << std::endl;
  
  // 验证内存效率 (应该小于 100B 每边)
  EXPECT_LT((double)memory_used / (num_vertices * edges_per_vertex), 100);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
