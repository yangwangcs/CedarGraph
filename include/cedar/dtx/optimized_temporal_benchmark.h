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
// Optimized Temporal Graph Performance Benchmark
// 优化的时态图性能基准测试
// =============================================================================

#ifndef CEDAR_DTX_OPTIMIZED_TEMPORAL_BENCHMARK_H_
#define CEDAR_DTX_OPTIMIZED_TEMPORAL_BENCHMARK_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>

#include "cedar/core/status.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Connection Pool for gRPC Clients
// =============================================================================

template<typename ClientType>
class ConnectionPool {
 public:
  ConnectionPool(size_t pool_size, std::function<std::unique_ptr<ClientType>()> factory);
  ~ConnectionPool();
  
  // 获取连接
  std::unique_ptr<ClientType, std::function<void(ClientType*)>> Acquire();
  
  // 连接池统计
  size_t AvailableCount() const;
  size_t InUseCount() const;
  
 private:
  void Release(ClientType* client);
  
  std::function<std::unique_ptr<ClientType>()> factory_;
  std::queue<std::unique_ptr<ClientType>> pool_;
  std::unordered_map<ClientType*, std::unique_ptr<ClientType>> in_use_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  size_t max_size_;
};

// =============================================================================
// Batch Operation Buffer
// =============================================================================

template<typename T>
class BatchBuffer {
 public:
  explicit BatchBuffer(size_t batch_size, 
                       std::chrono::microseconds flush_interval,
                       std::function<void(std::vector<T>&)> flush_callback);
  ~BatchBuffer();
  
  // 添加操作到批次
  void Add(const T& item);
  void Add(T&& item);
  
  // 强制刷新
  void Flush();
  
  // 统计
  size_t BatchCount() const;
  size_t TotalFlushed() const;
  
 private:
  void FlushLoop();
  
  size_t batch_size_;
  std::chrono::microseconds flush_interval_;
  std::function<void(std::vector<T>&)> flush_callback_;
  
  std::vector<T> buffer_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::thread flush_thread_;
  std::atomic<bool> running_{true};
  std::atomic<size_t> total_flushed_{0};
};

// =============================================================================
// Thread Pool for Parallel Execution
// =============================================================================

class ThreadPool {
 public:
  explicit ThreadPool(size_t num_threads);
  ~ThreadPool();
  
  // 提交任务
  template<typename F, typename... Args>
  auto Submit(F&& f, Args&&... args) -> std::future<decltype(f(args...))>;
  
  // 并行 for 循环
  template<typename Func>
  void ParallelFor(size_t start, size_t end, Func&& func);
  
  // 统计
  size_t TaskCount() const;
  void WaitForAll();
  
 private:
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> stop_{false};
  std::atomic<size_t> task_count_{0};
};

// =============================================================================
// Optimized Configuration
// =============================================================================

struct OptimizedTemporalConfig {
  // 基础配置
  int node_count = 3;
  std::vector<std::string> endpoints;
  
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
  
  // 优化参数 - 新增
  size_t connection_pool_size = 32;           // 连接池大小
  size_t write_batch_size = 100;              // 写入批量大小
  size_t query_batch_size = 10;               // 查询批量大小
  std::chrono::microseconds batch_flush_interval{1000}; // 批量刷新间隔
  bool enable_parallel_scan = true;           // 启用并行扫描
  bool enable_skeleton_cache = true;          // 启用 Skeleton Cache
  int parallel_scan_threads = 4;              // 并行扫描线程数
  bool enable_query_caching = true;           // 启用查询缓存
  size_t query_cache_size = 10000;            // 查询缓存大小
  bool enable_async_write = true;             // 启用异步写入
  bool enable_prefetch = true;                // 启用预读取
};

// =============================================================================
// Optimized Metrics
// =============================================================================

struct OptimizedMetrics {
  // 基础计数器
  std::atomic<uint64_t> total_queries{0};
  std::atomic<uint64_t> point_queries{0};
  std::atomic<uint64_t> range_queries{0};
  std::atomic<uint64_t> analytics_queries{0};
  std::atomic<uint64_t> write_ops{0};
  std::atomic<uint64_t> failed_queries{0};
  
  // 批量操作统计
  std::atomic<uint64_t> batched_writes{0};
  std::atomic<uint64_t> batched_queries{0};
  std::atomic<uint64_t> cache_hits{0};
  std::atomic<uint64_t> cache_misses{0};
  std::atomic<uint64_t> parallel_scans{0};
  
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
  
  double GetDurationSeconds() const;
  double GetThroughput() const;
  double GetCacheHitRate() const;
  
  void RecordPointQueryLatency(uint64_t latency_us);
  void RecordRangeQueryLatency(uint64_t latency_us);
  void RecordAnalyticsLatency(uint64_t latency_us);
  void RecordWriteLatency(uint64_t latency_us);
  
  void PrintOptimizedReport() const;
};

// =============================================================================
// Optimized Benchmark Runner
// =============================================================================

class OptimizedTemporalBenchmark {
 public:
  explicit OptimizedTemporalBenchmark(const OptimizedTemporalConfig& config);
  ~OptimizedTemporalBenchmark();
  
  Status Initialize();
  Status Run();
  const OptimizedMetrics& GetMetrics() const { return metrics_; }
  
 private:
  // 工作线程
  void WorkerThread(int client_id);
  
  // 优化的查询实现
  Status OptimizedPointQuery(uint64_t vertex_id, uint64_t timestamp, uint64_t& latency_us);
  Status OptimizedRangeQuery(uint64_t start_time, uint64_t end_time, uint64_t& latency_us);
  Status OptimizedAnalytics(uint64_t& latency_us);
  Status OptimizedBatchWrite(const std::vector<std::pair<uint64_t, uint64_t>>& writes, 
                             uint64_t& latency_us);
  
  // 并行扫描
  Status ParallelRangeScan(uint64_t start_time, uint64_t end_time, 
                           std::vector<uint64_t>& results);
  
  // 缓存操作
  bool CheckQueryCache(uint64_t key, std::string& result);
  void AddToQueryCache(uint64_t key, const std::string& result);
  
  // 批量刷新回调
  void FlushWriteBatch(std::vector<std::pair<uint64_t, uint64_t>>& batch);
  void FlushQueryBatch(std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& batch);
  
  OptimizedTemporalConfig config_;
  OptimizedMetrics metrics_;
  std::atomic<bool> running_{false};
  std::vector<std::thread> workers_;
  
  // 线程池
  std::unique_ptr<ThreadPool> thread_pool_;
  
  // 批量缓冲区
  std::unique_ptr<BatchBuffer<std::pair<uint64_t, uint64_t>>> write_buffer_;
  std::unique_ptr<BatchBuffer<std::tuple<uint64_t, uint64_t, uint64_t>>> query_buffer_;
  
  // 查询缓存
  std::unordered_map<uint64_t, std::string> query_cache_;
  mutable std::mutex cache_mutex_;
  
  // 随机数生成
  std::mt19937 rng_{std::random_device{}()};
  mutable std::mutex rng_mutex_;
  
  // 图数据
  std::vector<uint64_t> vertex_ids_;
  std::vector<std::pair<uint64_t, uint64_t>> edge_pairs_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_OPTIMIZED_TEMPORAL_BENCHMARK_H_
