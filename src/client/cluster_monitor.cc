// Copyright 2025 The Cedar Authors
//
// Cluster Monitor implementation

#include "cedar/client/cluster_monitor.h"

#include <chrono>
#include <iostream>
#include <sstream>

namespace cedar {
namespace client {

ClusterMonitor::ClusterMonitor() = default;

ClusterMonitor::~ClusterMonitor() {
  Stop();
}

bool ClusterMonitor::Initialize(const std::string& prometheus_url,
                                  const std::string& grafana_url) {
  prometheus_url_ = prometheus_url;
  grafana_url_ = grafana_url;
  return true;
}

bool ClusterMonitor::Start(int interval_seconds) {
  if (running_) {
    return true;
  }

  running_ = true;
  monitor_thread_ = std::thread([this, interval_seconds]() {
    while (running_) {
      CollectMetrics();
      CheckAlerts();
      std::this_thread::sleep_for(std::chrono::seconds(interval_seconds));
    }
  });

  return true;
}

void ClusterMonitor::Stop() {
  running_ = false;
  if (monitor_thread_.joinable()) {
    monitor_thread_.join();
  }
}

ClusterMetrics ClusterMonitor::GetClusterMetrics() {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_metrics_;
}

std::unordered_map<std::string, double> ClusterMonitor::GetNodeMetrics(
    const std::string& node_id) {
  std::unordered_map<std::string, double> metrics;

  // Query CPU usage
  std::string cpu_query = "node_cpu_usage{instance=\"" + node_id + "\"}";
  metrics["cpu_usage"] = QueryPrometheus(cpu_query);

  // Query memory usage
  std::string memory_query = "node_memory_usage{instance=\"" + node_id + "\"}";
  metrics["memory_usage"] = QueryPrometheus(memory_query);

  // Query disk usage
  std::string disk_query = "node_disk_usage{instance=\"" + node_id + "\"}";
  metrics["disk_usage"] = QueryPrometheus(disk_query);

  return metrics;
}

double ClusterMonitor::QueryPrometheus(const std::string& query) {
  std::string response = QueryPrometheusApi(query);
  return ParsePrometheusResponse(response);
}

void ClusterMonitor::AddAlertRule(const AlertRule& rule) {
  std::lock_guard<std::mutex> lock(mutex_);
  alert_rules_.push_back(rule);
}

void ClusterMonitor::RemoveAlertRule(const std::string& name) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = alert_rules_.begin(); it != alert_rules_.end(); ++it) {
    if (it->name == name) {
      alert_rules_.erase(it);
      break;
    }
  }
}

std::vector<AlertRule> ClusterMonitor::GetAlertRules() {
  std::lock_guard<std::mutex> lock(mutex_);
  return alert_rules_;
}

std::vector<Alert> ClusterMonitor::GetActiveAlerts() {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_alerts_;
}

void ClusterMonitor::SetAlertCallback(AlertCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  alert_callback_ = callback;
}

std::string ClusterMonitor::GetGrafanaDashboardUrl(const std::string& dashboard_name) {
  return grafana_url_ + "/d/" + dashboard_name;
}

bool ClusterMonitor::CreateGrafanaDashboard(const std::string& dashboard_json) {
  // TODO: Implement Grafana dashboard creation via API
  return true;
}

std::string ClusterMonitor::ExportPrometheusMetrics() {
  std::lock_guard<std::mutex> lock(mutex_);

  std::stringstream ss;

  // Export CPU metrics
  ss << "# HELP cedar_cpu_usage CPU usage percentage\n";
  ss << "# TYPE cedar_cpu_usage gauge\n";
  ss << "cedar_cpu_usage " << current_metrics_.cpu_usage_avg << "\n";

  // Export memory metrics
  ss << "# HELP cedar_memory_usage Memory usage percentage\n";
  ss << "# TYPE cedar_memory_usage gauge\n";
  ss << "cedar_memory_usage " << current_metrics_.memory_usage_avg << "\n";

  // Export QPS metrics
  ss << "# HELP cedar_qps Queries per second\n";
  ss << "# TYPE cedar_qps gauge\n";
  ss << "cedar_qps " << current_metrics_.qps << "\n";

  // Export latency metrics
  ss << "# HELP cedar_latency_p50 P50 latency in milliseconds\n";
  ss << "# TYPE cedar_latency_p50 gauge\n";
  ss << "cedar_latency_p50 " << current_metrics_.latency_p50 << "\n";

  ss << "# HELP cedar_latency_p95 P95 latency in milliseconds\n";
  ss << "# TYPE cedar_latency_p95 gauge\n";
  ss << "cedar_latency_p95 " << current_metrics_.latency_p95 << "\n";

  ss << "# HELP cedar_latency_p99 P99 latency in milliseconds\n";
  ss << "# TYPE cedar_latency_p99 gauge\n";
  ss << "cedar_latency_p99 " << current_metrics_.latency_p99 << "\n";

  // Export node metrics
  ss << "# HELP cedar_nodes_total Total number of nodes\n";
  ss << "# TYPE cedar_nodes_total gauge\n";
  ss << "cedar_nodes_total " << current_metrics_.total_nodes << "\n";

  ss << "# HELP cedar_nodes_healthy Number of healthy nodes\n";
  ss << "# TYPE cedar_nodes_healthy gauge\n";
  ss << "cedar_nodes_healthy " << current_metrics_.healthy_nodes << "\n";

  return ss.str();
}

// ============================================================================
// Private methods
// ============================================================================

void ClusterMonitor::MonitorLoop() {
  while (running_) {
    CollectMetrics();
    CheckAlerts();
    std::this_thread::sleep_for(std::chrono::seconds(30));
  }
}

void ClusterMonitor::CollectMetrics() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Query CPU metrics
  current_metrics_.cpu_usage_avg = QueryPrometheus("avg(node_cpu_usage)");
  current_metrics_.cpu_usage_max = QueryPrometheus("max(node_cpu_usage)");

  // Query memory metrics
  current_metrics_.memory_usage_avg = QueryPrometheus("avg(node_memory_usage)");
  current_metrics_.memory_usage_max = QueryPrometheus("max(node_memory_usage)");

  // Query disk metrics
  current_metrics_.disk_usage_avg = QueryPrometheus("avg(node_disk_usage)");
  current_metrics_.disk_usage_max = QueryPrometheus("max(node_disk_usage)");

  // Query network metrics
  current_metrics_.network_in_bytes = QueryPrometheus("sum(node_network_receive_bytes)");
  current_metrics_.network_out_bytes = QueryPrometheus("sum(node_network_transmit_bytes)");

  // Query QPS metrics
  current_metrics_.qps = QueryPrometheus("sum(rate(cedar_queries_total[1m]))");

  // Query latency metrics
  current_metrics_.latency_p50 = QueryPrometheus("histogram_quantile(0.5, cedar_latency_bucket)");
  current_metrics_.latency_p95 = QueryPrometheus("histogram_quantile(0.95, cedar_latency_bucket)");
  current_metrics_.latency_p99 = QueryPrometheus("histogram_quantile(0.99, cedar_latency_bucket)");

  // Query node metrics
  current_metrics_.total_nodes = static_cast<int>(QueryPrometheus("count(up)"));
  current_metrics_.healthy_nodes = static_cast<int>(QueryPrometheus("count(up == 1)"));
  current_metrics_.unhealthy_nodes = current_metrics_.total_nodes - current_metrics_.healthy_nodes;
}

void ClusterMonitor::CheckAlerts() {
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& rule : alert_rules_) {
    double value = 0;

    // Get metric value
    if (rule.metric == "cpu_usage") {
      value = current_metrics_.cpu_usage_avg;
    } else if (rule.metric == "memory_usage") {
      value = current_metrics_.memory_usage_avg;
    } else if (rule.metric == "disk_usage") {
      value = current_metrics_.disk_usage_avg;
    } else if (rule.metric == "qps") {
      value = current_metrics_.qps;
    } else if (rule.metric == "latency_p95") {
      value = current_metrics_.latency_p95;
    }

    // Check condition
    bool triggered = false;
    if (rule.condition == ">") {
      triggered = value > rule.threshold;
    } else if (rule.condition == "<") {
      triggered = value < rule.threshold;
    } else if (rule.condition == ">=") {
      triggered = value >= rule.threshold;
    } else if (rule.condition == "<=") {
      triggered = value <= rule.threshold;
    } else if (rule.condition == "==") {
      triggered = value == rule.threshold;
    } else if (rule.condition == "!=") {
      triggered = value != rule.threshold;
    }

    if (triggered) {
      Alert alert;
      alert.name = rule.name;
      alert.severity = rule.severity;
      alert.message = rule.message;
      alert.value = value;
      alert.threshold = rule.threshold;
      alert.triggered_at = std::chrono::system_clock::now().time_since_epoch().count();
      alert.resolved = false;

      active_alerts_.push_back(alert);

      if (alert_callback_) {
        alert_callback_(alert);
      }
    }
  }
}

std::string ClusterMonitor::QueryPrometheusApi(const std::string& query) {
  std::string url = prometheus_url_ + "/api/v1/query?query=" + query;
  return HttpRequest(url);
}

std::string ClusterMonitor::HttpRequest(const std::string& url) {
  // TODO: Implement HTTP request using libcurl or similar
  // For now, return empty string
  return "";
}

double ClusterMonitor::ParsePrometheusResponse(const std::string& response) {
  // TODO: Parse Prometheus JSON response
  // For now, return 0
  return 0.0;
}

}  // namespace client
}  // namespace cedar
