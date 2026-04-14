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
// Metrics Collector - Production Monitoring
// =============================================================================
// Collects and exposes metrics for monitoring and alerting

#ifndef CEDAR_DTX_STORAGE_METRICS_COLLECTOR_H_
#define CEDAR_DTX_STORAGE_METRICS_COLLECTOR_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// Metric Types
// =============================================================================

enum class MetricType : uint8_t {
  kCounter = 0,   // Monotonically increasing
  kGauge = 1,     // Can go up and down
  kHistogram = 2, // Distribution of values
  kSummary = 3,   // Sliding time window statistics
};

// =============================================================================
// Metric Value
// =============================================================================

struct MetricValue {
  double value = 0.0;
  std::chrono::system_clock::time_point timestamp;
  std::map<std::string, std::string> labels;
};

// =============================================================================
// Histogram Bucket
// =============================================================================

struct HistogramBucket {
  double upper_bound = 0.0;
  uint64_t count = 0;
};

// =============================================================================
// Base Metric
// =============================================================================

class Metric {
 public:
  Metric(const std::string& name, MetricType type, 
         const std::string& description);
  virtual ~Metric() = default;
  
  const std::string& GetName() const { return name_; }
  MetricType GetType() const { return type_; }
  const std::string& GetDescription() const { return description_; }
  
  virtual std::string ToPrometheusFormat() const = 0;

 protected:
  std::string name_;
  MetricType type_;
  std::string description_;
};

// =============================================================================
// Counter Metric
// =============================================================================

class Counter : public Metric {
 public:
  Counter(const std::string& name, const std::string& description);
  
  void Increment(double value = 1.0);
  double GetValue() const { return value_.load(); }
  void Reset() { value_.store(0.0); }
  
  std::string ToPrometheusFormat() const override;

 private:
  std::atomic<double> value_{0.0};
};

// =============================================================================
// Gauge Metric
// =============================================================================

class Gauge : public Metric {
 public:
  Gauge(const std::string& name, const std::string& description);
  
  void Set(double value);
  void Increment(double value = 1.0);
  void Decrement(double value = 1.0);
  double GetValue() const { return value_.load(); }
  
  std::string ToPrometheusFormat() const override;

 private:
  std::atomic<double> value_{0.0};
};

// =============================================================================
// Histogram Metric
// =============================================================================

class Histogram : public Metric {
 public:
  Histogram(const std::string& name, const std::string& description,
            std::vector<double> buckets);
  
  void Observe(double value);
  
  uint64_t GetTotalCount() const { return total_count_.load(); }
  double GetTotalSum() const { return total_sum_.load(); }
  
  std::string ToPrometheusFormat() const override;

 private:
  std::vector<double> bucket_bounds_;
  std::vector<std::unique_ptr<std::atomic<uint64_t>>> bucket_counts_;
  std::atomic<uint64_t> total_count_{0};
  std::atomic<double> total_sum_{0.0};
  
  mutable std::shared_mutex mutex_;
};

// =============================================================================
// Metric Family (with labels)
// =============================================================================

class MetricFamily {
 public:
  MetricFamily(const std::string& name, MetricType type,
               const std::string& description,
               const std::vector<std::string>& label_names);
  
  // Get or create metric with specific labels
  template<typename T>
  T* GetMetric(const std::map<std::string, std::string>& labels);
  
  std::string ToPrometheusFormat() const;

 private:
  std::string name_;
  MetricType type_;
  std::string description_;
  std::vector<std::string> label_names_;
  
  mutable std::shared_mutex mutex_;
  std::map<std::string, std::unique_ptr<Metric>> metrics_;
};

// =============================================================================
// Storage Node Metrics
// =============================================================================

struct StorageNodeMetrics {
  // Operation counters
  std::unique_ptr<Counter> put_ops_total;
  std::unique_ptr<Counter> get_ops_total;
  std::unique_ptr<Counter> scan_ops_total;
  std::unique_ptr<Counter> delete_ops_total;
  std::unique_ptr<Counter> batch_ops_total;
  
  // Latency histograms
  std::unique_ptr<Histogram> put_latency_seconds;
  std::unique_ptr<Histogram> get_latency_seconds;
  std::unique_ptr<Histogram> scan_latency_seconds;
  
  // Storage gauges
  std::unique_ptr<Gauge> storage_size_bytes;
  std::unique_ptr<Gauge> storage_keys_total;
  std::unique_ptr<Gauge> memtable_size_bytes;
  std::unique_ptr<Gauge> sst_file_count;
  
  // Raft metrics
  std::unique_ptr<Gauge> raft_role;  // 0=follower, 1=candidate, 2=leader
  std::unique_ptr<Counter> raft_leader_changes_total;
  std::unique_ptr<Counter> raft_proposals_failed_total;
  std::unique_ptr<Gauge> raft_commit_index;
  std::unique_ptr<Gauge> raft_applied_index;
  
  // Replication metrics
  std::unique_ptr<Counter> replication_lag_updates_total;
  std::unique_ptr<Gauge> replication_lag_seconds;
  
  StorageNodeMetrics();
  void Initialize(const std::string& prefix);
};

// =============================================================================
// Metrics Registry
// =============================================================================

class MetricsRegistry {
 public:
  static MetricsRegistry& Instance();
  
  // Register a metric
  void Register(std::unique_ptr<Metric> metric);
  void RegisterFamily(std::unique_ptr<MetricFamily> family);
  
  // Get or create counter
  Counter* GetCounter(const std::string& name,
                      const std::string& description = "");
  
  // Get or create gauge
  Gauge* GetGauge(const std::string& name,
                  const std::string& description = "");
  
  // Get or create histogram
  Histogram* GetHistogram(const std::string& name,
                          const std::string& description,
                          std::vector<double> buckets);
  
  // Export all metrics in Prometheus format
  std::string ExportPrometheus() const;
  
  // Export as JSON
  std::string ExportJSON() const;
  
  // Clear all metrics
  void Clear();

 private:
  MetricsRegistry() = default;
  
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<Metric>> metrics_;
  std::unordered_map<std::string, std::unique_ptr<MetricFamily>> families_;
};

// =============================================================================
// Metrics Collector
// =============================================================================

class MetricsCollector {
 public:
  struct Config {
    std::chrono::milliseconds collection_interval{60000};  // 1 minute
    std::string endpoint = ":9090";  // Prometheus scrape endpoint
    bool enable_http_server = true;
  };
  
  MetricsCollector();
  ~MetricsCollector();
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // Start HTTP server for Prometheus scraping
  Status StartHttpServer();
  
  // Manual collection trigger
  void Collect();
  
  // Register storage node for monitoring
  void RegisterStorageNode(PartitionID pid, const std::string& node_addr);
  void UnregisterStorageNode(PartitionID pid);
  
  // Get node metrics
  StorageNodeMetrics* GetNodeMetrics(PartitionID pid);

 private:
  void CollectionLoop();
  void CollectSystemMetrics();
  void CollectStorageMetrics();
  
  std::atomic<bool> running_{false};
  Config config_;
  
  std::unique_ptr<std::thread> collection_thread_;
  std::unique_ptr<std::thread> http_thread_;
  
  mutable std::shared_mutex nodes_mutex_;
  std::unordered_map<PartitionID, std::unique_ptr<StorageNodeMetrics>> node_metrics_;
};

// =============================================================================
// Alert Manager
// =============================================================================

enum class AlertSeverity : uint8_t {
  kInfo = 0,
  kWarning = 1,
  kCritical = 2,
  kEmergency = 3,
};

struct AlertRule {
  std::string name;
  std::string expression;  // Metric expression
  AlertSeverity severity;
  std::chrono::seconds duration;  // Duration before firing
  std::string summary;
  std::string description;
  
  // Threshold values
  double threshold;
  enum class Comparison : uint8_t {
    kGreaterThan = 0,
    kLessThan = 1,
    kEqual = 2,
    kNotEqual = 3,
  } comparison;
};

struct Alert {
  std::string name;
  AlertSeverity severity;
  std::chrono::system_clock::time_point fired_at;
  std::string summary;
  std::string description;
  std::map<std::string, std::string> labels;
};

class AlertManager {
 public:
  using AlertCallback = std::function<void(const Alert&)>;
  
  AlertManager();
  ~AlertManager();
  
  Status Initialize();
  void Shutdown();
  
  // Add alert rule
  void AddRule(const AlertRule& rule);
  void RemoveRule(const std::string& name);
  
  // Set callback for alert notifications
  void SetAlertCallback(AlertCallback cb) { alert_callback_ = std::move(cb); }
  
  // Evaluate rules and fire alerts
  void EvaluateRules();
  
  // Get active alerts
  std::vector<Alert> GetActiveAlerts() const;
  
  // Silence an alert
  void SilenceAlert(const std::string& name, std::chrono::minutes duration);

 private:
  void EvaluationLoop();
  bool EvaluateRule(const AlertRule& rule);
  void FireAlert(const AlertRule& rule);
  void ResolveAlert(const std::string& name);
  
  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> eval_thread_;
  
  mutable std::shared_mutex rules_mutex_;
  std::vector<AlertRule> rules_;
  
  mutable std::shared_mutex alerts_mutex_;
  std::unordered_map<std::string, Alert> active_alerts_;
  std::unordered_map<std::string, std::chrono::system_clock::time_point> silenced_until_;
  
  AlertCallback alert_callback_;
};

// =============================================================================
// Predefined Alert Rules
// =============================================================================

namespace alerts {

// Storage alerts
constexpr const char* kStorageHighLatency = "StorageHighLatency";
constexpr const char* kStorageLowSpace = "StorageLowSpace";
constexpr const char* kStorageHighErrorRate = "StorageHighErrorRate";

// Raft alerts
constexpr const char* kRaftLeaderNotElected = "RaftLeaderNotElected";
constexpr const char* kRaftHighReplicationLag = "RaftHighReplicationLag";
constexpr const char* kRaftSplitBrain = "RaftSplitBrain";

// Node alerts
constexpr const char* kNodeDown = "NodeDown";
constexpr const char* kNodeHighCPU = "NodeHighCPU";
constexpr const char* kNodeHighMemory = "NodeHighMemory";

std::vector<AlertRule> GetDefaultAlertRules();

}  // namespace alerts

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_METRICS_COLLECTOR_H_
