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
// Temporal Graph Performance Benchmark
// 时态图性能基准测试
// =============================================================================

#ifndef CEDAR_DTX_TEMPORAL_GRAPH_BENCHMARK_H_
#define CEDAR_DTX_TEMPORAL_GRAPH_BENCHMARK_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <map>
#include <unordered_map>

#include "cedar/core/status.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Temporal Graph Data Types
// =============================================================================

// 时态顶点：带有效时间戳的顶点
struct TemporalVertex {
  uint64_t vertex_id;
  uint64_t valid_from;  // 开始时间戳 (微秒级)
  uint64_t valid_until; // 结束时间戳 (微秒级), UINT64_MAX表示至今有效
  std::string properties; // JSON格式的属性
  
  bool IsValidAt(uint64_t timestamp) const {
    return timestamp >= valid_from && timestamp < valid_until;
  }
};

// 时态边：带有效时间戳的边
struct TemporalEdge {
  uint64_t edge_id;
  uint64_t from_vertex;
  uint64_t to_vertex;
  uint64_t valid_from;
  uint64_t valid_until;
  std::string edge_type;
  std::string properties;
  
  bool IsValidAt(uint64_t timestamp) const {
    return timestamp >= valid_from && timestamp < valid_until;
  }
};

// =============================================================================
// Benchmark Configuration
// =============================================================================

struct TemporalBenchmarkConfig {
  // 集群配置
  int node_count = 3;
  std::vector<std::string> endpoints;
  
  // 图数据规模
  uint64_t vertex_count = 100000;    // 顶点数量
  uint64_t edge_count = 500000;      // 边数量
  uint64_t time_range_seconds = 86400; // 时间范围 (24小时)
  
  // 查询混合比例
  int point_query_ratio = 30;        // 时态点查询比例 %
  int range_query_ratio = 30;        // 时态范围查询比例 %
  int graph_analytics_ratio = 20;    // 图分析算法比例 %
  int write_ratio = 20;              // 写入比例 %
  
  // 测试参数
  int duration_seconds = 60;
  int concurrent_clients = 16;
  
  // 范围查询参数
  uint64_t range_query_duration = 3600; // 范围查询默认跨度 (1小时)
  
  // 分析算法类型
  enum class AnalyticsType {
    kPageRank = 0,        // PageRank
    kShortestPath = 1,    // 最短路径
    kConnectedComponents = 2, // 连通分量
    kTemporalPattern = 3, // 时态模式匹配
    kRandom = 4           // 随机选择
  };
  AnalyticsType analytics_type = AnalyticsType::kRandom;
};

// =============================================================================
// Performance Metrics
// =============================================================================

struct TemporalQueryMetrics {
  std::atomic<uint64_t> total_queries{0};
  std::atomic<uint64_t> point_queries{0};
  std::atomic<uint64_t> range_queries{0};
  std::atomic<uint64_t> analytics_queries{0};
  std::atomic<uint64_t> write_ops{0};
  std::atomic<uint64_t> failed_queries{0};
  
  // 延迟统计 (微秒)
  std::vector<uint64_t> point_query_latencies;
  std::vector<uint64_t> range_query_latencies;
  std::vector<uint64_t> analytics_latencies;
  std::vector<uint64_t> write_latencies;
  mutable std::mutex latency_mutex;
  
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point end_time;
  
  void Start() { start_time = std::chrono::steady_clock::now(); }
  void Stop() { end_time = std::chrono::steady_clock::now(); }
  
  double GetDurationSeconds() const {
    return std::chrono::duration<double>(end_time - start_time).count();
  }
  
  void RecordPointQueryLatency(uint64_t latency_us);
  void RecordRangeQueryLatency(uint64_t latency_us);
  void RecordAnalyticsLatency(uint64_t latency_us);
  void RecordWriteLatency(uint64_t latency_us);
  
  void PrintReport() const;
};

// =============================================================================
// Temporal Graph Benchmark Runner
// =============================================================================

class TemporalGraphBenchmark {
 public:
  explicit TemporalGraphBenchmark(const TemporalBenchmarkConfig& config);
  ~TemporalGraphBenchmark();
  
  // 初始化测试数据
  Status Initialize();
  
  // 运行基准测试
  Status Run();
  
  // 获取测试指标
  const TemporalQueryMetrics& GetMetrics() const { return metrics_; }
  
 private:
  // 工作线程
  void WorkerThread(int client_id);
  
  // 各种查询操作
  Status SimulateTemporalPointQuery(uint64_t& latency_us);
  Status SimulateTemporalRangeQuery(uint64_t& latency_us);
  Status SimulateGraphAnalytics(uint64_t& latency_us);
  Status SimulateTemporalWrite(uint64_t& latency_us);
  
  // 图分析算法
  Status RunPageRank(uint64_t& latency_us);
  Status RunShortestPath(uint64_t& latency_us);
  Status RunConnectedComponents(uint64_t& latency_us);
  Status RunTemporalPatternMatch(uint64_t& latency_us);
  
  // 数据生成
  TemporalVertex GenerateRandomVertex(uint64_t timestamp);
  TemporalEdge GenerateRandomEdge(uint64_t timestamp);
  uint64_t GenerateRandomTimestamp();
  
  TemporalBenchmarkConfig config_;
  TemporalQueryMetrics metrics_;
  std::atomic<bool> running_{false};
  std::vector<std::thread> workers_;
  
  // 随机数生成
  std::mt19937 rng_{std::random_device{}()};
  mutable std::mutex rng_mutex_;
  
  // 模拟的图数据缓存
  std::vector<uint64_t> vertex_ids_;
  std::vector<std::pair<uint64_t, uint64_t>> edge_pairs_;
};

// =============================================================================
// Utility Functions
// =============================================================================

// 格式化时间戳为人类可读格式
std::string FormatTimestamp(uint64_t microseconds);

// 计算给定时间戳所在的存储分区
int GetPartitionForTimestamp(uint64_t timestamp, int num_partitions);

// 生成时态查询键
std::string GenerateTemporalKey(uint64_t vertex_id, uint64_t timestamp);

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_TEMPORAL_GRAPH_BENCHMARK_H_
