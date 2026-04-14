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
// Temporal Graph Performance Benchmark Implementation
// =============================================================================

#include "cedar/dtx/temporal_graph_benchmark.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <numeric>

namespace cedar {
namespace dtx {

// =============================================================================
// TemporalQueryMetrics Implementation
// =============================================================================

void TemporalQueryMetrics::RecordPointQueryLatency(uint64_t latency_us) {
  std::lock_guard<std::mutex> lock(latency_mutex);
  point_query_latencies.push_back(latency_us);
}

void TemporalQueryMetrics::RecordRangeQueryLatency(uint64_t latency_us) {
  std::lock_guard<std::mutex> lock(latency_mutex);
  range_query_latencies.push_back(latency_us);
}

void TemporalQueryMetrics::RecordAnalyticsLatency(uint64_t latency_us) {
  std::lock_guard<std::mutex> lock(latency_mutex);
  analytics_latencies.push_back(latency_us);
}

void TemporalQueryMetrics::RecordWriteLatency(uint64_t latency_us) {
  std::lock_guard<std::mutex> lock(latency_mutex);
  write_latencies.push_back(latency_us);
}

static uint64_t CalculateP50(const std::vector<uint64_t>& latencies) {
  if (latencies.empty()) return 0;
  auto sorted = latencies;
  std::sort(sorted.begin(), sorted.end());
  return sorted[sorted.size() * 0.5];
}

static uint64_t CalculateP99(const std::vector<uint64_t>& latencies) {
  if (latencies.empty()) return 0;
  auto sorted = latencies;
  std::sort(sorted.begin(), sorted.end());
  return sorted[std::min(sorted.size() - 1, (size_t)(sorted.size() * 0.99))];
}

static double CalculateAvg(const std::vector<uint64_t>& latencies) {
  if (latencies.empty()) return 0.0;
  return std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
}

void TemporalQueryMetrics::PrintReport() const {
  double duration = GetDurationSeconds();
  
  std::cout << "\n╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Temporal Graph Performance Results                     ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  std::cout << "Duration: " << std::fixed << std::setprecision(2) << duration << " seconds" << std::endl;
  std::cout << std::endl;
  
  // 总体统计
  std::cout << "Query Statistics:" << std::endl;
  std::cout << "  Total Queries: " << total_queries.load() << std::endl;
  std::cout << "  Point Queries: " << point_queries.load() << std::endl;
  std::cout << "  Range Queries: " << range_queries.load() << std::endl;
  std::cout << "  Analytics:     " << analytics_queries.load() << std::endl;
  std::cout << "  Write Ops:     " << write_ops.load() << std::endl;
  std::cout << "  Failed:        " << failed_queries.load() << std::endl;
  std::cout << std::endl;
  
  // 吞吐量
  std::cout << "Throughput:" << std::endl;
  std::cout << "  Total:         " << std::fixed << std::setprecision(1) 
            << (total_queries.load() / duration) << " queries/sec" << std::endl;
  std::cout << "  Point Query:   " << (point_queries.load() / duration) << " queries/sec" << std::endl;
  std::cout << "  Range Query:   " << (range_queries.load() / duration) << " queries/sec" << std::endl;
  std::cout << "  Analytics:     " << (analytics_queries.load() / duration) << " queries/sec" << std::endl;
  std::cout << "  Write:         " << (write_ops.load() / duration) << " ops/sec" << std::endl;
  std::cout << std::endl;
  
  // 延迟统计
  std::cout << "Latency Statistics (microseconds):" << std::endl;
  
  auto print_latency = [](const std::string& name, const std::vector<uint64_t>& latencies) {
    if (!latencies.empty()) {
      std::cout << "  " << name << ":" << std::endl;
      std::cout << "    P50: " << CalculateP50(latencies) << " µs" << std::endl;
      std::cout << "    P99: " << CalculateP99(latencies) << " µs" << std::endl;
      std::cout << "    Avg: " << std::fixed << std::setprecision(1) 
                << CalculateAvg(latencies) << " µs" << std::endl;
    }
  };
  
  print_latency("Point Query", point_query_latencies);
  print_latency("Range Query", range_query_latencies);
  print_latency("Analytics", analytics_latencies);
  print_latency("Write", write_latencies);
  
  std::cout << std::endl;
}

// =============================================================================
// TemporalGraphBenchmark Implementation
// =============================================================================

TemporalGraphBenchmark::TemporalGraphBenchmark(const TemporalBenchmarkConfig& config)
    : config_(config) {
  // 设置 endpoints
  if (config_.endpoints.empty()) {
    for (int i = 1; i <= config_.node_count; ++i) {
      std::stringstream ss;
      ss << "storaged" << i << ":700" << (i - 1);
      config_.endpoints.push_back(ss.str());
    }
  }
}

TemporalGraphBenchmark::~TemporalGraphBenchmark() {
  if (running_.exchange(false)) {
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }
}

Status TemporalGraphBenchmark::Initialize() {
  std::cout << "Initializing temporal graph benchmark data..." << std::endl;
  
  // 预生成顶点ID
  vertex_ids_.reserve(config_.vertex_count);
  for (uint64_t i = 0; i < config_.vertex_count; ++i) {
    vertex_ids_.push_back(i);
  }
  
  // 预生成边连接关系
  edge_pairs_.reserve(config_.edge_count);
  std::uniform_int_distribution<uint64_t> vertex_dist(0, config_.vertex_count - 1);
  
  for (uint64_t i = 0; i < config_.edge_count; ++i) {
    uint64_t from = vertex_dist(rng_);
    uint64_t to = vertex_dist(rng_);
    if (from != to) {
      edge_pairs_.push_back({from, to});
    }
  }
  
  std::cout << "Generated " << vertex_ids_.size() << " vertices, " 
            << edge_pairs_.size() << " edges" << std::endl;
  
  return Status::OK();
}

Status TemporalGraphBenchmark::Run() {
  std::cout << "\n╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Temporal Graph Benchmark                               ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Nodes: " << config_.node_count << std::endl;
  std::cout << "  Vertices: " << config_.vertex_count << std::endl;
  std::cout << "  Edges: " << config_.edge_count << std::endl;
  std::cout << "  Time Range: " << config_.time_range_seconds << " seconds" << std::endl;
  std::cout << "  Duration: " << config_.duration_seconds << " seconds" << std::endl;
  std::cout << "  Clients: " << config_.concurrent_clients << std::endl;
  std::cout << "  Query Mix: Point=" << config_.point_query_ratio 
            << "%, Range=" << config_.range_query_ratio
            << "%, Analytics=" << config_.graph_analytics_ratio
            << "%, Write=" << config_.write_ratio << "%" << std::endl;
  std::cout << std::endl;
  
  // 启动工作线程
  running_ = true;
  metrics_.Start();
  
  for (int i = 0; i < config_.concurrent_clients; ++i) {
    workers_.emplace_back([this, i]() { WorkerThread(i); });
  }
  
  // 进度报告
  std::thread progress_thread([this]() {
    auto start = std::chrono::steady_clock::now();
    while (running_) {
      std::this_thread::sleep_for(std::chrono::seconds(5));
      auto now = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
      
      if (elapsed >= config_.duration_seconds) break;
      
      auto queries = metrics_.total_queries.load();
      auto throughput = queries / std::max(1.0, (double)elapsed);
      
      std::cout << "[Progress] " << elapsed << "s / " << config_.duration_seconds 
                << "s | Queries: " << queries 
                << " | Throughput: " << std::fixed << std::setprecision(1) 
                << throughput << " qps" << std::endl;
    }
  });
  
  // 等待测试完成
  std::this_thread::sleep_for(std::chrono::seconds(config_.duration_seconds));
  running_ = false;
  
  // 停止工作线程
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  
  metrics_.Stop();
  progress_thread.join();
  
  // 输出报告
  metrics_.PrintReport();
  
  return Status::OK();
}

void TemporalGraphBenchmark::WorkerThread(int client_id) {
  std::mt19937 local_rng(std::random_device{}() + client_id);
  std::uniform_int_distribution<int> op_dist(0, 99);
  
  while (running_) {
    uint64_t latency_us = 0;
    Status status;
    
    int op_type = op_dist(local_rng);
    
    if (op_type < config_.point_query_ratio) {
      // 时态点查询
      status = SimulateTemporalPointQuery(latency_us);
      if (status.ok()) {
        metrics_.point_queries.fetch_add(1);
        metrics_.RecordPointQueryLatency(latency_us);
      }
    } else if (op_type < config_.point_query_ratio + config_.range_query_ratio) {
      // 时态范围查询
      status = SimulateTemporalRangeQuery(latency_us);
      if (status.ok()) {
        metrics_.range_queries.fetch_add(1);
        metrics_.RecordRangeQueryLatency(latency_us);
      }
    } else if (op_type < config_.point_query_ratio + config_.range_query_ratio + config_.graph_analytics_ratio) {
      // 图分析算法
      status = SimulateGraphAnalytics(latency_us);
      if (status.ok()) {
        metrics_.analytics_queries.fetch_add(1);
        metrics_.RecordAnalyticsLatency(latency_us);
      }
    } else {
      // 写入操作
      status = SimulateTemporalWrite(latency_us);
      if (status.ok()) {
        metrics_.write_ops.fetch_add(1);
        metrics_.RecordWriteLatency(latency_us);
      }
    }
    
    if (status.ok()) {
      metrics_.total_queries.fetch_add(1);
    } else {
      metrics_.failed_queries.fetch_add(1);
    }
    
    // 小延迟防止CPU过载
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
}

Status TemporalGraphBenchmark::SimulateTemporalPointQuery(uint64_t& latency_us) {
  auto start = std::chrono::high_resolution_clock::now();
  
  // 模拟时态点查询：查询特定顶点在特定时间的状态
  std::uniform_int_distribution<size_t> vertex_idx_dist(0, vertex_ids_.size() - 1);
  uint64_t timestamp = GenerateRandomTimestamp();
  size_t vertex_idx = vertex_idx_dist(rng_);
  
  // 模拟查询延迟 (时态查询需要检查时间有效性)
  std::uniform_int_distribution<int> latency_dist(500, 2000);  // 0.5-2ms
  std::this_thread::sleep_for(std::chrono::microseconds(latency_dist(rng_)));
  
  auto end = std::chrono::high_resolution_clock::now();
  latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  
  // 模拟低失败率 (1%)
  std::uniform_int_distribution<int> fail_dist(0, 99);
  if (fail_dist(rng_) < 1) {
    return Status::NotFound("Vertex not found at timestamp");
  }
  
  return Status::OK();
}

Status TemporalGraphBenchmark::SimulateTemporalRangeQuery(uint64_t& latency_us) {
  auto start = std::chrono::high_resolution_clock::now();
  
  // 模拟时态范围查询：查询时间范围内的所有变化
  uint64_t start_time = GenerateRandomTimestamp();
  uint64_t end_time = start_time + config_.range_query_duration;
  
  // 范围查询通常更慢，涉及更多数据
  std::uniform_int_distribution<int> latency_dist(2000, 10000);  // 2-10ms
  std::this_thread::sleep_for(std::chrono::microseconds(latency_dist(rng_)));
  
  auto end = std::chrono::high_resolution_clock::now();
  latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  
  return Status::OK();
}

Status TemporalGraphBenchmark::SimulateGraphAnalytics(uint64_t& latency_us) {
  auto start = std::chrono::high_resolution_clock::now();
  
  // 根据配置选择分析算法
  switch (config_.analytics_type) {
    case TemporalBenchmarkConfig::AnalyticsType::kPageRank:
      return RunPageRank(latency_us);
    case TemporalBenchmarkConfig::AnalyticsType::kShortestPath:
      return RunShortestPath(latency_us);
    case TemporalBenchmarkConfig::AnalyticsType::kConnectedComponents:
      return RunConnectedComponents(latency_us);
    case TemporalBenchmarkConfig::AnalyticsType::kTemporalPattern:
      return RunTemporalPatternMatch(latency_us);
    case TemporalBenchmarkConfig::AnalyticsType::kRandom:
    default:
      // 随机选择
      std::uniform_int_distribution<int> algo_dist(0, 3);
      switch (algo_dist(rng_)) {
        case 0: return RunPageRank(latency_us);
        case 1: return RunShortestPath(latency_us);
        case 2: return RunConnectedComponents(latency_us);
        case 3: return RunTemporalPatternMatch(latency_us);
      }
  }
  return Status::OK();
}

Status TemporalGraphBenchmark::RunPageRank(uint64_t& latency_us) {
  auto start = std::chrono::high_resolution_clock::now();
  
  // PageRank 计算需要多轮迭代，比较耗时
  // 模拟 10-50ms 的计算延迟
  std::uniform_int_distribution<int> latency_dist(10000, 50000);
  std::this_thread::sleep_for(std::chrono::microseconds(latency_dist(rng_)));
  
  auto end = std::chrono::high_resolution_clock::now();
  latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  
  return Status::OK();
}

Status TemporalGraphBenchmark::RunShortestPath(uint64_t& latency_us) {
  auto start = std::chrono::high_resolution_clock::now();
  
  // 最短路径查询 (BFS/Dijkstra)
  std::uniform_int_distribution<int> latency_dist(5000, 30000);  // 5-30ms
  std::this_thread::sleep_for(std::chrono::microseconds(latency_dist(rng_)));
  
  auto end = std::chrono::high_resolution_clock::now();
  latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  
  return Status::OK();
}

Status TemporalGraphBenchmark::RunConnectedComponents(uint64_t& latency_us) {
  auto start = std::chrono::high_resolution_clock::now();
  
  // 连通分量计算需要遍历全图
  std::uniform_int_distribution<int> latency_dist(20000, 80000);  // 20-80ms
  std::this_thread::sleep_for(std::chrono::microseconds(latency_dist(rng_)));
  
  auto end = std::chrono::high_resolution_clock::now();
  latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  
  return Status::OK();
}

Status TemporalGraphBenchmark::RunTemporalPatternMatch(uint64_t& latency_us) {
  auto start = std::chrono::high_resolution_clock::now();
  
  // 时态模式匹配（如：查找在特定时间窗口内形成三角形的顶点）
  std::uniform_int_distribution<int> latency_dist(15000, 60000);  // 15-60ms
  std::this_thread::sleep_for(std::chrono::microseconds(latency_dist(rng_)));
  
  auto end = std::chrono::high_resolution_clock::now();
  latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  
  return Status::OK();
}

Status TemporalGraphBenchmark::SimulateTemporalWrite(uint64_t& latency_us) {
  auto start = std::chrono::high_resolution_clock::now();
  
  // 模拟时态数据写入（带时间戳的顶点或边）
  std::uniform_int_distribution<int> latency_dist(800, 3000);  // 0.8-3ms
  std::this_thread::sleep_for(std::chrono::microseconds(latency_dist(rng_)));
  
  auto end = std::chrono::high_resolution_clock::now();
  latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  
  return Status::OK();
}

uint64_t TemporalGraphBenchmark::GenerateRandomTimestamp() {
  std::uniform_int_distribution<uint64_t> time_dist(0, config_.time_range_seconds * 1000000);
  return time_dist(rng_);
}

TemporalVertex TemporalGraphBenchmark::GenerateRandomVertex(uint64_t timestamp) {
  TemporalVertex v;
  v.vertex_id = vertex_ids_.size();
  v.valid_from = timestamp;
  v.valid_until = UINT64_MAX;
  v.properties = "{}";
  return v;
}

TemporalEdge TemporalGraphBenchmark::GenerateRandomEdge(uint64_t timestamp) {
  TemporalEdge e;
  e.edge_id = edge_pairs_.size();
  std::uniform_int_distribution<size_t> idx_dist(0, edge_pairs_.size() - 1);
  auto pair = edge_pairs_[idx_dist(rng_)];
  e.from_vertex = pair.first;
  e.to_vertex = pair.second;
  e.valid_from = timestamp;
  e.valid_until = UINT64_MAX;
  e.edge_type = "KNOWS";
  return e;
}

// =============================================================================
// Utility Functions
// =============================================================================

std::string FormatTimestamp(uint64_t microseconds) {
  auto seconds = microseconds / 1000000;
  auto usec = microseconds % 1000000;
  
  std::time_t time_val = seconds;
  auto tm = *std::localtime(&time_val);
  
  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  oss << "." << std::setfill('0') << std::setw(6) << usec;
  return oss.str();
}

int GetPartitionForTimestamp(uint64_t timestamp, int num_partitions) {
  return (timestamp / 3600000000ULL) % num_partitions;  // 按小时分区
}

std::string GenerateTemporalKey(uint64_t vertex_id, uint64_t timestamp) {
  std::ostringstream oss;
  oss << "v" << vertex_id << "_t" << timestamp;
  return oss.str();
}

}  // namespace dtx
}  // namespace cedar
