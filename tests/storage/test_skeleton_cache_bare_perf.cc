//===----------------------------------------------------------------------===//
// SkeletonCache Bare Performance Test (独立于 LsmEngine)
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <random>
#include <thread>

#include "cedar/storage/skeleton_cache.h"

using namespace cedar;
using namespace std::chrono;

TEST(SkeletonCacheBarePerf, InsertAndLookup) {
  const int num_entries = 10000;
  const int edges_per_entry = 10;
  
  ShardedSkeletonCache cache(1024, 1024);
  
  std::cout << "\n=== SkeletonCache Performance Test ===" << std::endl;
  std::cout << "Entries: " << num_entries << ", Edges per entry: " << edges_per_entry << std::endl;
  
  // ===== 插入性能测试 =====
  auto start = high_resolution_clock::now();
  
  for (int i = 0; i < num_entries; ++i) {
    VertexSkeleton skeleton;
    skeleton.base_timestamp = 1000;
    skeleton.out_count = edges_per_entry;
    skeleton.out_edges = new EdgeEntry12B[edges_per_entry];
    
    for (int j = 0; j < edges_per_entry; ++j) {
      skeleton.out_edges[j].dst_id = 20000 + j;
      skeleton.out_edges[j].bits.label_id = j % 128;
      skeleton.out_edges[j].bits.timestamp_offset = j * 10;
    }
    skeleton.SetStatus(1);
    
    cache.Put(i, std::move(skeleton));
  }
  
  auto end = high_resolution_clock::now();
  double insert_time = duration<double>(end - start).count();
  
  std::cout << "\n--- Insert Performance ---" << std::endl;
  std::cout << "Total time: " << insert_time << "s" << std::endl;
  std::cout << "Inserts/sec: " << num_entries / insert_time << std::endl;
  
  // ===== 顺序查找性能测试 =====
  start = high_resolution_clock::now();
  
  int hits = 0;
  for (int iter = 0; iter < 10; ++iter) {
    for (int i = 0; i < num_entries; ++i) {
      auto [skeleton, found] = cache.Get(i);
      if (found) hits++;
    }
  }
  
  end = high_resolution_clock::now();
  double lookup_time = duration<double>(end - start).count();
  
  std::cout << "\n--- Sequential Lookup Performance ---" << std::endl;
  std::cout << "Total lookups: " << num_entries * 10 << std::endl;
  std::cout << "Total time: " << lookup_time << "s" << std::endl;
  std::cout << "Lookups/sec: " << (num_entries * 10) / lookup_time << std::endl;
  std::cout << "Hits: " << hits << std::endl;
  
  // ===== 随机查找性能测试 =====
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> dist(0, num_entries - 1);
  
  start = high_resolution_clock::now();
  
  hits = 0;
  for (int iter = 0; iter < 10; ++iter) {
    for (int i = 0; i < num_entries; ++i) {
      int idx = dist(rng);
      auto [skeleton, found] = cache.Get(idx);
      if (found) hits++;
    }
  }
  
  end = high_resolution_clock::now();
  double random_lookup_time = duration<double>(end - start).count();
  
  std::cout << "\n--- Random Lookup Performance ---" << std::endl;
  std::cout << "Total lookups: " << num_entries * 10 << std::endl;
  std::cout << "Total time: " << random_lookup_time << "s" << std::endl;
  std::cout << "Lookups/sec: " << (num_entries * 10) / random_lookup_time << std::endl;
  
  // ===== 内存占用统计 =====
  size_t memory = cache.MemoryUsage();
  auto stats = cache.GetStats();
  
  std::cout << "\n--- Memory Usage ---" << std::endl;
  std::cout << "Total entries: " << stats.TotalEntries() << std::endl;
  std::cout << "Memory used: " << memory / 1024 << " KB" << std::endl;
  std::cout << "Bytes per entry: " << (double)memory / num_entries << std::endl;
  std::cout << "Bytes per edge: " << (double)memory / (num_entries * edges_per_entry) << std::endl;
  
  // 验证性能指标
  EXPECT_GT(num_entries / insert_time, 10000);  // 至少 10k inserts/sec
  EXPECT_GT((num_entries * 10) / lookup_time, 100000);  // 至少 100k lookups/sec
}

TEST(SkeletonCacheBarePerf, ConcurrentRead) {
  const int num_entries = 5000;
  const int edges_per_entry = 10;
  const int num_threads = 4;
  const int ops_per_thread = 10000;
  
  ShardedSkeletonCache cache(256, 256);  // 更小的分片数
  
  // 预填充数据
  for (int i = 0; i < num_entries; ++i) {
    VertexSkeleton skeleton;
    skeleton.base_timestamp = 1000;
    skeleton.out_count = edges_per_entry;
    skeleton.out_edges = new EdgeEntry12B[edges_per_entry];
    for (int j = 0; j < edges_per_entry; ++j) {
      skeleton.out_edges[j].dst_id = 20000 + j;
      skeleton.out_edges[j].bits.label_id = j % 128;
    }
    skeleton.SetStatus(1);
    cache.Put(i, std::move(skeleton));
  }
  
  std::cout << "\n=== Concurrent Read Test ===" << std::endl;
  std::cout << "Threads: " << num_threads << ", Ops per thread: " << ops_per_thread << std::endl;
  
  // 并发读取
  std::vector<std::thread> threads;
  std::atomic<int64_t> total_hits{0};
  
  auto start = high_resolution_clock::now();
  
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&cache, t, num_entries, ops_per_thread, &total_hits]() {
      std::mt19937 rng(42 + t);
      std::uniform_int_distribution<int> dist(0, num_entries - 1);
      
      for (int i = 0; i < ops_per_thread; ++i) {
        int idx = dist(rng);
        auto [skeleton, found] = cache.Get(idx);
        if (found) total_hits++;
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  auto end = high_resolution_clock::now();
  double elapsed = duration<double>(end - start).count();
  
  auto stats = cache.GetStats();
  
  std::cout << "Total ops: " << num_threads * ops_per_thread << std::endl;
  std::cout << "Time: " << elapsed << "s" << std::endl;
  std::cout << "Throughput: " << (num_threads * ops_per_thread) / elapsed / 1000 << " kops/s" << std::endl;
  std::cout << "Hit rate: " << stats.HitRate() * 100 << "%" << std::endl;
  
  EXPECT_EQ(total_hits.load(), num_threads * ops_per_thread);
  EXPECT_GT((num_threads * ops_per_thread) / elapsed, 50000);  // 至少 50k ops/sec
}

TEST(SkeletonCacheBarePerf, WarmupPerformance) {
  const int num_entries = 1000;
  const int edges_per_entry = 10;
  
  ShardedSkeletonCache cache(256, 1024);
  
  // 准备 ID 列表
  std::vector<uint64_t> ids;
  for (int i = 0; i < num_entries; ++i) {
    ids.push_back(i);
  }
  
  std::cout << "\n=== Warmup Performance Test ===" << std::endl;
  
  // 预热性能测试
  auto start = high_resolution_clock::now();
  
  size_t warmed = cache.Warmup(ids, [](uint64_t vid) {
    VertexSkeleton skeleton;
    skeleton.base_timestamp = 1000;
    skeleton.out_count = 10;
    skeleton.out_edges = new EdgeEntry12B[10];
    for (int j = 0; j < 10; ++j) {
      skeleton.out_edges[j].dst_id = 20000 + j;
      skeleton.out_edges[j].bits.label_id = j % 128;
    }
    skeleton.SetStatus(1);
    return skeleton;
  });
  
  auto end = high_resolution_clock::now();
  double elapsed = duration<double>(end - start).count();
  
  std::cout << "Warmed entries: " << warmed << std::endl;
  std::cout << "Time: " << elapsed << "s" << std::endl;
  std::cout << "Warmup speed: " << warmed / elapsed << " entries/sec" << std::endl;
  
  auto stats = cache.GetStats();
  EXPECT_EQ(stats.insertions, num_entries);
  EXPECT_EQ(warmed, num_entries);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
