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
// Real Temporal Graph Performance Benchmark
// 真实时态图性能基准测试 - 接入真实 CedarGraph 系统
// =============================================================================

#ifndef CEDAR_DTX_REAL_TEMPORAL_BENCHMARK_H_
#define CEDAR_DTX_REAL_TEMPORAL_BENCHMARK_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <queue>
#include <functional>

#include "cedar/core/status.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/optimized_2pc_engine.h"
#include "cedar/dtx/storage_service_impl.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Real Benchmark Configuration
// =============================================================================

struct RealBenchmarkConfig {
  // 集群配置
  int node_count = 3;
  std::vector<std::string> endpoints;  // gRPC endpoints: "host:port"
  
  // 数据目录 (本地存储)
  std::string data_dir = "/tmp/cedar_benchmark";
  
  // 图数据规模
  uint64_t vertex_count = 100000;
  uint64_t edge_count = 500000;
  uint64_t time_range_seconds = 86400;
  
  // 查询混合比例
  int point_query_ratio = 30;
  int range_query_ratio = 30;
  int graph_analytics_ratio = 20;
  int write_ratio = 20;
  
  // 测试参数
  int duration_seconds = 60;
  int concurrent_clients = 16;
  int operations_per_client = 10000;
  
  // 批量参数
  size_t batch_size = 100;
  bool enable_batch_write = true;
  
  // 时态参数
  uint64_t range_query_duration = 3600; // 1 hour in seconds
};

// =============================================================================
// Performance Metrics
// =============================================================================

struct RealPerformanceMetrics {
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> point_queries{0};
  std::atomic<uint64_t> range_queries{0};
  std::atomic<uint64_t> analytics_ops{0};
  std::atomic<uint64_t> write_ops{0};
  std::atomic<uint64_t> failed_ops{0};
  
  // 存储引擎统计
  std::atomic<uint64_t> cache_hits{0};
  std::atomic<uint64_t> cache_misses{0};
  std::atomic<uint64_t> disk_reads{0};
  std::atomic<uint64_t> bytes_read{0};
  std::atomic<uint64_t> bytes_written{0};
  
  // 延迟统计 (微秒)
  std::vector<uint64_t> point_latencies;
  std::vector<uint64_t> range_latencies;
  std::vector<uint64_t> analytics_latencies;
  std::vector<uint64_t> write_latencies;
  mutable std::mutex latency_mutex;
  
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point end_time;
  
  void Start();
  void Stop();
  double GetDurationSeconds() const;
  double GetThroughput() const;
  double GetCacheHitRate() const;
  
  void RecordPointLatency(uint64_t latency_us);
  void RecordRangeLatency(uint64_t latency_us);
  void RecordAnalyticsLatency(uint64_t latency_us);
  void RecordWriteLatency(uint64_t latency_us);
  
  void PrintReport() const;
};

// =============================================================================
// Connection Pool for Real Storage Clients
// =============================================================================

class BenchmarkStorageClientPool {
 public:
  explicit BenchmarkStorageClientPool(size_t pool_size);
  ~BenchmarkStorageClientPool();
  
  // 初始化连接池
  Status Initialize(const std::vector<std::string>& endpoints);
  
  // 获取客户端 (轮询)
  std::shared_ptr<StorageClient> GetClient();
  
  // 获取特定地址的客户端
  std::shared_ptr<StorageClient> GetClient(const std::string& address);
  
  // 统计
  size_t PoolSize() const { return clients_.size(); }
  
 private:
  size_t pool_size_;
  std::vector<std::shared_ptr<StorageClient>> clients_;
  std::atomic<size_t> next_index_{0};
  mutable std::mutex mutex_;
};

// =============================================================================
// Real Benchmark Runner - 接入真实系统
// =============================================================================

class RealTemporalBenchmark {
 public:
  explicit RealTemporalBenchmark(const RealBenchmarkConfig& config);
  ~RealTemporalBenchmark();
  
  // 初始化真实存储引擎和客户端
  Status Initialize();
  
  // 准备测试数据
  Status PrepareData();
  
  // 运行基准测试
  Status Run();
  
  // 清理数据
  Status Cleanup();
  
  const RealPerformanceMetrics& GetMetrics() const { return metrics_; }
  
 private:
  // 工作线程
  void WorkerThread(int client_id);
  
  // 真实存储操作
  Status RealPointQuery(uint64_t vertex_id, uint64_t timestamp, uint64_t& latency_us);
  Status RealRangeQuery(uint64_t start_time, uint64_t end_time, uint64_t& latency_us);
  Status RealGraphAnalytics(uint64_t& latency_us);
  Status RealWrite(uint64_t vertex_id, uint64_t timestamp, const std::string& data, 
                   uint64_t& latency_us);
  Status RealBatchWrite(const std::vector<std::tuple<uint64_t, uint64_t, std::string>>& writes,
                        uint64_t& latency_us);
  
  // 图分析算法 - 使用真实存储
  Status RealPageRank(uint64_t& latency_us);
  Status RealShortestPath(uint64_t from, uint64_t to, uint64_t& latency_us);
  Status RealTemporalNeighborhood(uint64_t vertex_id, uint64_t timestamp, 
                                  uint64_t& latency_us);
  
  // 辅助方法
  std::string GenerateVertexKey(uint64_t vertex_id, uint64_t timestamp);
  std::string GenerateEdgeKey(uint64_t from, uint64_t to, uint64_t timestamp);
  std::string GenerateVertexData(uint64_t vertex_id);
  std::string GenerateEdgeData(uint64_t edge_id);
  uint64_t GenerateRandomTimestamp();
  
  RealBenchmarkConfig config_;
  RealPerformanceMetrics metrics_;
  std::atomic<bool> running_{false};
  std::vector<std::thread> workers_;
  
  // 真实存储组件
  std::shared_ptr<CedarGraphStorage> local_storage_;  // 本地存储引擎
  std::unique_ptr<BenchmarkStorageClientPool> client_pool_;    // gRPC 客户端连接池
  std::unique_ptr<Optimized2PCEngine> txn_engine_;    // 事务引擎 (可选)
  
  // 随机数生成
  std::mt19937 rng_{std::random_device{}()};
  mutable std::mutex rng_mutex_;
  
  // 测试数据
  std::vector<uint64_t> vertex_ids_;
  std::vector<std::pair<uint64_t, uint64_t>> edge_pairs_;
  
  // 批量写入缓冲
  std::vector<std::tuple<uint64_t, uint64_t, std::string>> write_buffer_;
  mutable std::mutex write_buffer_mutex_;
};

// =============================================================================
// Utility Functions
// =============================================================================

// 序列化时态顶点
std::string SerializeTemporalVertex(uint64_t vertex_id, uint64_t timestamp, 
                                    const std::string& properties);

// 序列化时态边
std::string SerializeTemporalEdge(uint64_t edge_id, uint64_t from, uint64_t to,
                                  uint64_t timestamp, const std::string& type);

// 反序列化
bool DeserializeTemporalData(const std::string& data, uint64_t& id, 
                             uint64_t& timestamp, std::string& properties);

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_REAL_TEMPORAL_BENCHMARK_H_
