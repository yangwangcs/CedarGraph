// tools/benchmark_metrics.h
// Performance metrics definitions for 3-node cluster temporal graph benchmarking

#ifndef CEDAR_BENCHMARK_METRICS_H_
#define CEDAR_BENCHMARK_METRICS_H_

#include <cstdint>
#include <chrono>
#include <atomic>
#include <vector>
#include <string>
#include <mutex>

namespace cedar {
namespace benchmark {

// =============================================================================
// Basic Performance Metrics
// =============================================================================

struct OperationMetrics {
  std::atomic<uint64_t> count{0};
  std::atomic<uint64_t> total_latency_us{0};
  std::atomic<uint64_t> min_latency_us{UINT64_MAX};
  std::atomic<uint64_t> max_latency_us{0};
  std::atomic<uint64_t> bytes_processed{0};
  std::atomic<uint64_t> errors{0};
  
  void Record(uint64_t latency_us, uint64_t bytes = 0, bool success = true);
  double GetAvgLatency() const;
  double GetThroughput() const;  // ops/sec
};

// =============================================================================
// Temporal Query Specific Metrics
// =============================================================================

struct TemporalQueryMetrics {
  // Temporal point queries
  OperationMetrics point_query_by_time;      // Exact time point query
  OperationMetrics point_query_by_version;   // Version query
  
  // Temporal range queries
  OperationMetrics range_query_small;        // Small range (< 1 hour)
  OperationMetrics range_query_medium;       // Medium range (1 hour - 1 day)
  OperationMetrics range_query_large;        // Large range (> 1 day)
  
  // Temporal analysis queries
  OperationMetrics temporal_snapshot;        // Historical snapshot query
  OperationMetrics temporal_diff;            // Temporal difference analysis
  OperationMetrics entity_lifecycle;         // Entity lifecycle query
};

// =============================================================================
// Graph Analytics Metrics
// =============================================================================

struct GraphAnalyticsMetrics {
  OperationMetrics bfs_traversal;            // BFS traversal
  OperationMetrics shortest_path;            // Shortest path
  OperationMetrics neighbor_query;           // Neighbor query
  OperationMetrics triangle_count;           // Triangle counting
  OperationMetrics page_rank_iteration;      // PageRank iteration
  OperationMetrics community_detection;      // Community detection
};

// =============================================================================
// System Resource Metrics
// =============================================================================

struct SystemMetrics {
  double cpu_usage_percent{0.0};
  double memory_usage_mb{0.0};
  double disk_read_mbps{0.0};
  double disk_write_mbps{0.0};
  double network_in_mbps{0.0};
  double network_out_mbps{0.0};
};

// =============================================================================
// Disk Usage Metrics
// =============================================================================

struct DiskUsageMetrics {
  uint64_t total_data_size_bytes{0};         // Total data size
  uint64_t sst_files_size_bytes{0};          // SST file size
  uint64_t wal_size_bytes{0};                // WAL log size
  uint64_t blob_storage_size_bytes{0};       // Blob storage size
  uint64_t metadata_size_bytes{0};           // Metadata size
  uint64_t compaction_overhead_bytes{0};     // Compaction overhead
  
  uint64_t num_sst_files{0};                 // Number of SST files
  uint64_t num_levels{0};                    // LSM level count
  double amplification_ratio{0.0};           // Write amplification ratio
  
  void Measure(const std::string& data_dir);
  void Print() const;
};

// =============================================================================
// Comprehensive Performance Report
// =============================================================================

struct BenchmarkReport {
  std::string benchmark_name;
  std::string cluster_config;  // "3-node (9779,9780,9781)"
  std::chrono::system_clock::time_point start_time;
  std::chrono::system_clock::time_point end_time;
  
  // Basic performance
  OperationMetrics write_metrics;
  OperationMetrics read_metrics;
  OperationMetrics batch_write_metrics;
  OperationMetrics scan_metrics;
  
  // Temporal queries
  TemporalQueryMetrics temporal_metrics;
  
  // Graph analytics
  GraphAnalyticsMetrics analytics_metrics;
  
  // Resource usage
  SystemMetrics system_metrics;
  DiskUsageMetrics disk_metrics;
  
  // Real-time response metrics
  double p50_latency_ms{0.0};
  double p99_latency_ms{0.0};
  double p999_latency_ms{0.0};
  
  void PrintConsole() const;
  void SaveToJson(const std::string& filename) const;
  void SaveToCsv(const std::string& filename) const;
};

// =============================================================================
// Benchmark Runner Base Class
// =============================================================================

class BenchmarkRunner {
 public:
  virtual ~BenchmarkRunner() = default;
  virtual void Run(BenchmarkReport& report) = 0;
  virtual std::string GetName() const = 0;
};

}  // namespace benchmark
}  // namespace cedar

#endif  // CEDAR_BENCHMARK_METRICS_H_
