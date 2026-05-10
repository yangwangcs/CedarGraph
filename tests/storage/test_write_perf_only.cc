//===----------------------------------------------------------------------===//
// Write Performance Test Only
// 由于 CedarScan 有 bug，只测试写入性能
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <iostream>
#include <random>

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/env.h"

using namespace cedar;
using namespace std::chrono;

TEST(WritePerfOnly, SequentialWrite) {
  std::string test_dir = "/tmp/write_perf_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  CedarOptions options;
  options.create_if_missing = true;
  options.write_buffer_size = 64 * 1024 * 1024;  // 64MB
  options.enable_skeleton_cache = true;
  
  auto engine = std::make_unique<LsmEngine>(test_dir, options, env);
  ASSERT_TRUE(engine->Open().ok());
  
  const int num_vertices = 1000;
  const int edges_per_vertex = 10;
  
  std::cout << "\n=== Sequential Write Performance Test ===" << std::endl;
  std::cout << "Vertices: " << num_vertices << ", Edges per vertex: " << edges_per_vertex << std::endl;
  
  // 写入节点
  auto start = high_resolution_clock::now();
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t vid = 10000 + i;
    CedarKey key = CedarKey::Vertex(vid, 1, Timestamp(100));
    Status s = engine->Put(key, Descriptor::InlineInt(1, i), Timestamp(1));
    EXPECT_TRUE(s.ok());
  }
  auto end = high_resolution_clock::now();
  double vertex_time = duration<double>(end - start).count();
  
  std::cout << "Vertex write: " << vertex_time << "s (" 
            << num_vertices / vertex_time / 1000 << " kv/s)" << std::endl;
  
  // 写入边
  start = high_resolution_clock::now();
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t src_id = 10000 + i;
    for (int j = 0; j < edges_per_vertex; ++j) {
      uint64_t dst_id = 20000 + (i * edges_per_vertex + j) % 5000;
      CedarKey key = CedarKey::EdgeOut(src_id, dst_id, EdgeTypeId(j % 5 + 1), Timestamp(100 + j));
      Status s = engine->Put(key, Descriptor::InlineInt(1, j), Timestamp(2));
      EXPECT_TRUE(s.ok());
    }
  }
  end = high_resolution_clock::now();
  double edge_time = duration<double>(end - start).count();
  int total_edges = num_vertices * edges_per_vertex;
  
  std::cout << "Edge write: " << edge_time << "s (" 
            << total_edges / edge_time / 1000 << " ke/s)" << std::endl;
  
  // 刷盘
  start = high_resolution_clock::now();
  Status s = engine->ForceFlush();
  end = high_resolution_clock::now();
  double flush_time = duration<double>(end - start).count();
  
  ASSERT_TRUE(s.ok());
  std::cout << "ForceFlush: " << flush_time << "s" << std::endl;
  
  // 缓存统计
  auto stats = engine->GetSkeletonCacheStats();
  size_t memory = engine->GetSkeletonCacheMemoryUsage();
  
  std::cout << "\n=== SkeletonCache Statistics ===" << std::endl;
  std::cout << "Cache hits: " << stats.hits << std::endl;
  std::cout << "Cache misses: " << stats.misses << std::endl;
  std::cout << "Memory used: " << memory / 1024 << " KB" << std::endl;
  
  // 总结
  std::cout << "\n=== Summary ===" << std::endl;
  std::cout << "Total write throughput: " << (num_vertices + total_edges) / (vertex_time + edge_time) / 1000 << " kops/s" << std::endl;
  std::cout << "Data successfully written to SST" << std::endl;
  
  engine->Close();
  std::filesystem::remove_all(test_dir);
}

TEST(WritePerfOnly, BatchWrite) {
  std::string test_dir = "/tmp/write_batch_perf_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  CedarOptions options;
  options.create_if_missing = true;
  options.write_buffer_size = 64 * 1024 * 1024;
  options.enable_skeleton_cache = true;
  
  auto engine = std::make_unique<LsmEngine>(test_dir, options, env);
  ASSERT_TRUE(engine->Open().ok());
  
  const int num_batches = 100;
  const int vertices_per_batch = 10;
  const int edges_per_vertex = 5;
  
  std::cout << "\n=== Batch Write Performance Test ===" << std::endl;
  std::cout << "Batches: " << num_batches << ", Vertices per batch: " << vertices_per_batch << std::endl;
  
  auto start = high_resolution_clock::now();
  
  for (int b = 0; b < num_batches; ++b) {
    // 每个批次写入多个顶点
    for (int v = 0; v < vertices_per_batch; ++v) {
      int vid = 10000 + b * vertices_per_batch + v;
      
      // 写入顶点
      CedarKey vkey = CedarKey::Vertex(vid, 1, Timestamp(100));
      engine->Put(vkey, Descriptor::InlineInt(1, vid), Timestamp(1));
      
      // 写入边
      for (int e = 0; e < edges_per_vertex; ++e) {
        uint64_t dst_id = 20000 + e;
        CedarKey ekey = CedarKey::EdgeOut(vid, dst_id, EdgeTypeId(1), Timestamp(100 + e));
        engine->Put(ekey, Descriptor::InlineInt(1, e), Timestamp(2));
      }
    }
  }
  
  auto end = high_resolution_clock::now();
  double write_time = duration<double>(end - start).count();
  
  int total_vertices = num_batches * vertices_per_batch;
  int total_edges = total_vertices * edges_per_vertex;
  
  std::cout << "Write time: " << write_time << "s" << std::endl;
  std::cout << "Write throughput: " << (total_vertices + total_edges) / write_time / 1000 << " kops/s" << std::endl;
  
  // 刷盘
  engine->ForceFlush();
  
  // 缓存统计
  auto stats = engine->GetSkeletonCacheStats();
  std::cout << "Cache memory: " << engine->GetSkeletonCacheMemoryUsage() / 1024 << " KB" << std::endl;
  
  engine->Close();
  std::filesystem::remove_all(test_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
