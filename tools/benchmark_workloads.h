// tools/benchmark_workloads.h
// Temporal graph performance benchmark workloads

#ifndef CEDAR_BENCHMARK_WORKLOADS_H_
#define CEDAR_BENCHMARK_WORKLOADS_H_

#include "benchmark_metrics.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/raft/partition_router.h"

#include <thread>
#include <vector>
#include <random>
#include <atomic>
#include <queue>
#include <condition_variable>

namespace cedar {
namespace benchmark {

// =============================================================================
// Basic Read/Write Workload
// =============================================================================

class BasicReadWriteWorkload : public BenchmarkRunner {
 public:
  struct Config {
    uint64_t num_operations = 100000;
    uint64_t num_vertices = 10000;
    uint32_t num_threads = 8;
    uint32_t write_ratio = 20;  // 20% writes, 80% reads
    uint32_t value_size = 100;  // bytes
  };
  
  explicit BasicReadWriteWorkload(Config config, CedarGraphStorage* storage);
  void Run(BenchmarkReport& report) override;
  std::string GetName() const override { return "Basic Read/Write"; }
  
 private:
  void WorkerThread(uint32_t thread_id, uint64_t ops_per_thread, 
                    BenchmarkReport& report, std::atomic<uint64_t>& progress);
  
  Config config_;
  CedarGraphStorage* storage_;
};

// =============================================================================
// Temporal Point Query Workload
// =============================================================================

class TemporalPointQueryWorkload : public BenchmarkRunner {
 public:
  struct Config {
    uint64_t num_queries = 50000;
    uint32_t num_threads = 8;
    uint64_t num_timestamps = 1000;  // Number of historical versions
    uint64_t time_range_seconds = 86400;  // 1 day
  };
  
  explicit TemporalPointQueryWorkload(Config config, CedarGraphStorage* storage);
  void Run(BenchmarkReport& report) override;
  std::string GetName() const override { return "Temporal Point Query"; }
  
 private:
  void QueryByExactTime(BenchmarkReport& report);
  void QueryByVersion(BenchmarkReport& report);
  
  Config config_;
  CedarGraphStorage* storage_;
};

// =============================================================================
// Temporal Range Query Workload
// =============================================================================

class TemporalRangeQueryWorkload : public BenchmarkRunner {
 public:
  struct Config {
    uint64_t num_queries = 10000;
    uint32_t num_threads = 4;
    // Range size distribution
    uint64_t small_range_seconds = 3600;   // 1 hour
    uint64_t medium_range_seconds = 86400;  // 1 day
    uint64_t large_range_seconds = 604800;  // 1 week
    uint64_t time_range_seconds = 86400;    // Total time range for data generation
  };
  
  explicit TemporalRangeQueryWorkload(Config config, CedarGraphStorage* storage);
  void Run(BenchmarkReport& report) override;
  std::string GetName() const override { return "Temporal Range Query"; }
  
 private:
  void RunRangeQuery(BenchmarkReport& report, uint64_t range_seconds, 
                     OperationMetrics& metrics, const std::string& name);
  
  Config config_;
  CedarGraphStorage* storage_;
};

// =============================================================================
// Temporal Analytics Workload
// =============================================================================

class TemporalAnalyticsWorkload : public BenchmarkRunner {
 public:
  struct Config {
    uint64_t num_snapshots = 100;      // Historical snapshot queries
    uint64_t num_diff_queries = 50;    // Difference analysis
    uint64_t num_lifecycle_queries = 200;  // Lifecycle queries
    uint64_t entity_range = 10000;
  };
  
  explicit TemporalAnalyticsWorkload(Config config, CedarGraphStorage* storage);
  void Run(BenchmarkReport& report) override;
  std::string GetName() const override { return "Temporal Analytics"; }
  
 private:
  void SnapshotQuery(BenchmarkReport& report);
  void DiffQuery(BenchmarkReport& report);
  void LifecycleQuery(BenchmarkReport& report);
  
  Config config_;
  CedarGraphStorage* storage_;
};

// =============================================================================
// Graph Analytics Workload
// =============================================================================

class GraphAnalyticsWorkload : public BenchmarkRunner {
 public:
  struct Config {
    uint64_t num_neighbor_queries = 50000;
    uint64_t num_bfs_queries = 100;
    uint64_t num_shortest_path_queries = 50;
    uint32_t bfs_depth = 3;
    uint32_t num_threads = 4;
  };
  
  explicit GraphAnalyticsWorkload(Config config, CedarGraphStorage* storage);
  void Run(BenchmarkReport& report) override;
  std::string GetName() const override { return "Graph Analytics"; }
  
 private:
  void NeighborQuery(BenchmarkReport& report);
  void BFSTraversal(BenchmarkReport& report);
  void ShortestPath(BenchmarkReport& report);
  
  Config config_;
  CedarGraphStorage* storage_;
};

// =============================================================================
// Real-time Latency Workload (Latency Test)
// =============================================================================

class RealtimeLatencyWorkload : public BenchmarkRunner {
 public:
  struct Config {
    uint64_t duration_seconds = 60;  // Test duration
    uint32_t target_qps = 1000;      // Target QPS
    uint32_t warmup_seconds = 10;    // Warmup time
  };
  
  explicit RealtimeLatencyWorkload(Config config, CedarGraphStorage* storage);
  void Run(BenchmarkReport& report) override;
  std::string GetName() const override { return "Realtime Latency"; }
  
 private:
  void MeasureLatencyDistribution(BenchmarkReport& report);
  void CalculatePercentiles(BenchmarkReport& report);
  
  Config config_;
  CedarGraphStorage* storage_;
  std::vector<uint64_t> latency_samples_;  // For P99/P999 calculation
  std::mutex samples_mutex_;
};

}  // namespace benchmark
}  // namespace cedar

#endif  // CEDAR_BENCHMARK_WORKLOADS_H_
