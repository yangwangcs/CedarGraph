// Copyright 2025 The Cedar Authors
//
// Metrics Collector for collecting and reporting metrics

#ifndef CEDAR_CLIENT_METRICS_COLLECTOR_H_
#define CEDAR_CLIENT_METRICS_COLLECTOR_H_

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>

namespace cedar {
namespace client {

// Query metrics
struct QueryMetrics {
  int64_t total_queries = 0;
  int64_t successful_queries = 0;
  int64_t failed_queries = 0;
  int64_t total_latency_ms = 0;
  int64_t min_latency_ms = INT64_MAX;
  int64_t max_latency_ms = 0;
  int64_t timeout_queries = 0;
};

// Connection metrics
struct ConnectionMetrics {
  int64_t total_connections = 0;
  int64_t active_connections = 0;
  int64_t connection_errors = 0;
  int64_t reconnections = 0;
};

// Node metrics
struct NodeMetrics {
  std::string node_id;
  int64_t requests = 0;
  int64_t errors = 0;
  int64_t total_latency_ms = 0;
  bool healthy = true;
};

// Metrics Collector
class MetricsCollector {
 public:
  MetricsCollector();
  ~MetricsCollector();

  // Record query execution
  void RecordQuery(const std::string& query_type, 
                   int64_t latency_ms, 
                   bool success,
                   bool timeout = false);

  // Record connection event
  void RecordConnection(const std::string& event_type);

  // Record node request
  void RecordNodeRequest(const std::string& node_id, 
                         int64_t latency_ms, 
                         bool success);

  // Get query metrics
  QueryMetrics GetQueryMetrics() const;

  // Get connection metrics
  ConnectionMetrics GetConnectionMetrics() const;

  // Get node metrics
  std::unordered_map<std::string, NodeMetrics> GetNodeMetrics() const;

  // Reset all metrics
  void Reset();

  // Get uptime in seconds
  int64_t GetUptimeSeconds() const;

 private:
  mutable std::mutex mutex_;
  
  QueryMetrics query_metrics_;
  ConnectionMetrics connection_metrics_;
  std::unordered_map<std::string, NodeMetrics> node_metrics_;
  
  std::chrono::steady_clock::time_point start_time_;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_METRICS_COLLECTOR_H_
