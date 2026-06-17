// Copyright 2025 The Cedar Authors
//
// Cluster Monitor - Prometheus/Grafana integration

#ifndef CEDAR_CLIENT_CLUSTER_MONITOR_H_
#define CEDAR_CLIENT_CLUSTER_MONITOR_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace cedar {
namespace client {

// Metric types
enum class MetricType {
  COUNTER,
  GAUGE,
  HISTOGRAM,
  SUMMARY
};

// Metric value
struct MetricValue {
  std::string name;
  double value;
  std::unordered_map<std::string, std::string> labels;
  int64_t timestamp;
};

// Cluster metrics
struct ClusterMetrics {
  // CPU metrics
  double cpu_usage_avg;
  double cpu_usage_max;
  
  // Memory metrics
  double memory_usage_avg;
  double memory_usage_max;
  
  // Storage metrics
  double disk_usage_avg;
  double disk_usage_max;
  
  // Network metrics
  double network_in_bytes;
  double network_out_bytes;
  
  // Query metrics
  double qps;
  double latency_p50;
  double latency_p95;
  double latency_p99;
  
  // Node metrics
  int total_nodes;
  int healthy_nodes;
  int unhealthy_nodes;
};

// Alert rule
struct AlertRule {
  std::string name;
  std::string metric;
  std::string condition;  // >, <, >=, <=, ==, !=
  double threshold;
  int duration_seconds;
  std::string severity;  // critical, warning, info
  std::string message;
};

// Alert
struct Alert {
  std::string name;
  std::string severity;
  std::string message;
  std::string component;
  double value;
  double threshold;
  int64_t triggered_at;
  bool resolved;
};

// Alert callback
using AlertCallback = std::function<void(const Alert&)>;

// Cluster Monitor
class ClusterMonitor {
 public:
  ClusterMonitor();
  ~ClusterMonitor();

  // Initialize monitor
  bool Initialize(const std::string& prometheus_url = "http://localhost:9090",
                  const std::string& grafana_url = "http://localhost:3000");

  // Start monitoring
  bool Start(int interval_seconds = 30);

  // Stop monitoring
  void Stop();

  // Get cluster metrics
  ClusterMetrics GetClusterMetrics();

  // Get node metrics
  std::unordered_map<std::string, double> GetNodeMetrics(const std::string& node_id);

  // Query Prometheus
  double QueryPrometheus(const std::string& query);

  // Alert management
  void AddAlertRule(const AlertRule& rule);
  void RemoveAlertRule(const std::string& name);
  std::vector<AlertRule> GetAlertRules();
  std::vector<Alert> GetActiveAlerts();
  void SetAlertCallback(AlertCallback callback);

  // Grafana integration
  std::string GetGrafanaDashboardUrl(const std::string& dashboard_name);
  bool CreateGrafanaDashboard(const std::string& dashboard_json);

  // Export metrics
  std::string ExportPrometheusMetrics();

 private:
  std::string prometheus_url_;
  std::string grafana_url_;
  std::atomic<bool> running_{false};
  std::thread monitor_thread_;
  mutable std::mutex mutex_;
  
  std::vector<AlertRule> alert_rules_;
  std::vector<Alert> active_alerts_;
  AlertCallback alert_callback_;
  
  ClusterMetrics current_metrics_;

  // Monitor loop
  void MonitorLoop();

  // Collect metrics from Prometheus
  void CollectMetrics();

  // Check alert rules
  void CheckAlerts();

  // Query Prometheus API
  std::string QueryPrometheusApi(const std::string& query);

  // HTTP request helper
  std::string HttpRequest(const std::string& url);

  // Parse Prometheus response
  double ParsePrometheusResponse(const std::string& response);
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CLUSTER_MONITOR_H_
