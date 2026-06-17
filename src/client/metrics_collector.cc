// Copyright 2025 The Cedar Authors
//
// Metrics Collector implementation

#include "cedar/client/metrics_collector.h"

namespace cedar {
namespace client {

MetricsCollector::MetricsCollector() 
    : start_time_(std::chrono::steady_clock::now()) {}

MetricsCollector::~MetricsCollector() = default;

void MetricsCollector::RecordQuery(const std::string& query_type,
                                    int64_t latency_ms,
                                    bool success,
                                    bool timeout) {
  std::lock_guard<std::mutex> lock(mutex_);

  query_metrics_.total_queries++;
  query_metrics_.total_latency_ms += latency_ms;

  if (success) {
    query_metrics_.successful_queries++;
  } else {
    query_metrics_.failed_queries++;
    if (timeout) {
      query_metrics_.timeout_queries++;
    }
  }

  if (latency_ms < query_metrics_.min_latency_ms) {
    query_metrics_.min_latency_ms = latency_ms;
  }
  if (latency_ms > query_metrics_.max_latency_ms) {
    query_metrics_.max_latency_ms = latency_ms;
  }
}

void MetricsCollector::RecordConnection(const std::string& event_type) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (event_type == "create") {
    connection_metrics_.total_connections++;
    connection_metrics_.active_connections++;
  } else if (event_type == "close") {
    if (connection_metrics_.active_connections > 0) {
      connection_metrics_.active_connections--;
    }
  } else if (event_type == "error") {
    connection_metrics_.connection_errors++;
  } else if (event_type == "reconnect") {
    connection_metrics_.reconnections++;
  }
}

void MetricsCollector::RecordNodeRequest(const std::string& node_id,
                                          int64_t latency_ms,
                                          bool success) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto& node = node_metrics_[node_id];
  node.node_id = node_id;
  node.requests++;
  node.total_latency_ms += latency_ms;

  if (!success) {
    node.errors++;
  }
}

QueryMetrics MetricsCollector::GetQueryMetrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return query_metrics_;
}

ConnectionMetrics MetricsCollector::GetConnectionMetrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return connection_metrics_;
}

std::unordered_map<std::string, NodeMetrics> MetricsCollector::GetNodeMetrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return node_metrics_;
}

void MetricsCollector::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);

  query_metrics_ = QueryMetrics();
  connection_metrics_ = ConnectionMetrics();
  node_metrics_.clear();
  start_time_ = std::chrono::steady_clock::now();
}

int64_t MetricsCollector::GetUptimeSeconds() const {
  auto now = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
}

}  // namespace client
}  // namespace cedar
