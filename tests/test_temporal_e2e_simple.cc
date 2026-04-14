//===----------------------------------------------------------------------===//
// Temporal Graph Simple E2E Test
// 简化的端到端测试，使用底层 API 直接写入和读取
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <iostream>

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/query/cedar_scan.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/env.h"

using namespace cedar;
using namespace std::chrono;

TEST(TemporalE2ESimple, DirectWriteRead) {
  std::string test_dir = "/tmp/temporal_e2e_simple_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  // 创建引擎
  CedarOptions options;
  options.create_if_missing = true;
  options.write_buffer_size = 4 * 1024 * 1024;
  options.enable_skeleton_cache = true;
  
  auto engine = std::make_unique<LsmEngine>(test_dir, options, env);
  ASSERT_TRUE(engine->Open().ok());
  
  const int num_vertices = 20;
  const int edges_per_vertex = 3;
  
  std::cout << "\n=== Direct Write-Read Test ===" << std::endl;
  std::cout << "Vertices: " << num_vertices << ", Edges per vertex: " << edges_per_vertex << std::endl;
  
  // 1. 直接使用 LsmEngine::Put 写入节点
  auto write_start = high_resolution_clock::now();
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t vid = 10000 + i;
    CedarKey key = CedarKey::Vertex(vid, 1, Timestamp(100));
    Status s = engine->Put(key, Descriptor::InlineInt(1, i), Timestamp(1));
    EXPECT_TRUE(s.ok()) << "Failed to write vertex " << vid << ": " << s.ToString();
  }
  auto write_end = high_resolution_clock::now();
  double vertex_write_time = duration<double>(write_end - write_start).count();
  
  std::cout << "Vertex write: " << vertex_write_time << "s (" 
            << num_vertices / vertex_write_time << " v/s)" << std::endl;
  
  // 2. 写入边
  write_start = high_resolution_clock::now();
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t src_id = 10000 + i;
    for (int j = 0; j < edges_per_vertex; ++j) {
      uint64_t dst_id = 20000 + j;
      CedarKey key = CedarKey::EdgeOut(src_id, dst_id, EdgeTypeId(1), Timestamp(100 + j));
      Status s = engine->Put(key, Descriptor::InlineInt(1, j * 100), Timestamp(2));
      EXPECT_TRUE(s.ok()) << "Failed to write edge " << src_id << "->" << dst_id;
    }
  }
  write_end = high_resolution_clock::now();
  double edge_write_time = duration<double>(write_end - write_start).count();
  int total_edges = num_vertices * edges_per_vertex;
  
  std::cout << "Edge write: " << edge_write_time << "s (" 
            << total_edges / edge_write_time << " e/s)" << std::endl;
  
  // 3. 强制刷盘并 sync
  Status s = engine->ForceFlush();
  ASSERT_TRUE(s.ok()) << "ForceFlush failed: " << s.ToString();
  
  // 等待数据稳定
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  std::cout << "Data flushed to SST" << std::endl;
  
  // 4. 使用 CedarScan 读取节点
  auto read_start = high_resolution_clock::now();
  int found_vertices = 0;
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t vid = 10000 + i;
    auto scan = CedarScan::At(Timestamp(150), engine.get());
    auto node = scan.GetNode(vid);
    if (node.has_value()) {
      found_vertices++;
    } else {
      std::cout << "  Vertex " << vid << " not found at t=150" << std::endl;
    }
  }
  auto read_end = high_resolution_clock::now();
  double vertex_read_time = duration<double>(read_end - read_start).count();
  
  std::cout << "Vertex read: " << vertex_read_time << "s ("
            << num_vertices / vertex_read_time << " v/s)" << std::endl;
  std::cout << "Found vertices: " << found_vertices << "/" << num_vertices << std::endl;
  
  // 5. 读取边
  read_start = high_resolution_clock::now();
  int found_edges = 0;
  for (int i = 0; i < num_vertices; ++i) {
    uint64_t vid = 10000 + i;
    auto scan = CedarScan::At(Timestamp(150), engine.get());
    auto edge_iter = scan.OutEdges(vid);
    int count = 0;
    while (edge_iter.Valid()) {
      count++;
      edge_iter.Next();
    }
    if (count == edges_per_vertex) {
      found_edges += count;
    } else {
      std::cout << "  Vertex " << vid << " has " << count << " edges, expected " << edges_per_vertex << std::endl;
    }
  }
  read_end = high_resolution_clock::now();
  double edge_read_time = duration<double>(read_end - read_start).count();
  
  std::cout << "Edge read: " << edge_read_time << "s ("
            << num_vertices / edge_read_time << " v/s)" << std::endl;
  std::cout << "Found edges: " << found_edges << "/" << total_edges << std::endl;
  
  // 6. 缓存统计
  auto stats = engine->GetSkeletonCacheStats();
  size_t memory = engine->GetSkeletonCacheMemoryUsage();
  
  std::cout << "\n=== Cache Statistics ===" << std::endl;
  std::cout << "Cache hits: " << stats.hits << std::endl;
  std::cout << "Cache misses: " << stats.misses << std::endl;
  std::cout << "Hit rate: " << stats.HitRate() * 100 << "%" << std::endl;
  std::cout << "Memory used: " << memory / 1024 << " KB" << std::endl;
  
  // 7. 性能总结
  std::cout << "\n=== Performance Summary ===" << std::endl;
  std::cout << "Write throughput: " << (num_vertices + total_edges) / (vertex_write_time + edge_write_time) / 1000 << " kops/s" << std::endl;
  std::cout << "Read throughput: " << (num_vertices * 2) / (vertex_read_time + edge_read_time) / 1000 << " kops/s" << std::endl;
  
  // 验证
  EXPECT_EQ(found_vertices, num_vertices);
  EXPECT_EQ(found_edges, total_edges);
  
  // 关闭引擎
  engine->Close();
  std::filesystem::remove_all(test_dir);
  
  std::cout << "\nData consistency: " << (found_vertices == num_vertices && found_edges == total_edges ? "VERIFIED" : "FAILED") << std::endl;
}

TEST(TemporalE2ESimple, TemporalQuery) {
  std::string test_dir = "/tmp/temporal_e2e_temporal_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = true;
  
  auto engine = std::make_unique<LsmEngine>(test_dir, options, env);
  ASSERT_TRUE(engine->Open().ok());
  
  const int num_vertices = 10;
  
  std::cout << "\n=== Temporal Query Test ===" << std::endl;
  
  // 写入多版本节点 (v1 at t=100, v2 at t=200, v3 at t=300)
  for (int version = 0; version < 3; ++version) {
    for (int i = 0; i < num_vertices; ++i) {
      uint64_t vid = 10000 + i;
      CedarKey key = CedarKey::Vertex(vid, 1, Timestamp(100 + version * 100));
      engine->Put(key, Descriptor::InlineInt(1, i + version * 100), Timestamp(version + 1));
    }
  }
  
  // 刷盘
  engine->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  // 查询不同时态点
  std::vector<Timestamp> query_times = {Timestamp(150), Timestamp(250), Timestamp(350)};
  
  for (auto ts : query_times) {
    int found = 0;
    for (int i = 0; i < num_vertices; ++i) {
      uint64_t vid = 10000 + i;
      auto scan = CedarScan::At(ts, engine.get());
      auto node = scan.GetNode(vid);
      if (node.has_value()) {
        found++;
      }
    }
    std::cout << "Query at t=" << ts.value() << ": found " << found << "/" << num_vertices << " vertices" << std::endl;
    EXPECT_EQ(found, num_vertices);
  }
  
  engine->Close();
  std::filesystem::remove_all(test_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
