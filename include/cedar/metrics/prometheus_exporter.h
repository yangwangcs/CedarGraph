// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Prometheus Metrics Exporter

#pragma once

#include <string>
#include <map>
#include <vector>
#include <atomic>
#include <mutex>
#include <chrono>
#include <sstream>

namespace cedar {

class PrometheusExporter {
 public:
  static PrometheusExporter& Instance() {
    static PrometheusExporter instance;
    return instance;
  }

  // Counter metric (monotonically increasing)
  void IncrementCounter(const std::string& name, 
                        const std::map<std::string, std::string>& labels = {},
                        double value = 1.0) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto key = MakeKey(name, labels);
    counters_[key] += value;
  }

  // Gauge metric (can go up and down)
  void SetGauge(const std::string& name, double value,
                const std::map<std::string, std::string>& labels = {}) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto key = MakeKey(name, labels);
    gauges_[key] = value;
  }

  // Histogram metric (for latency, size distributions)
  void ObserveHistogram(const std::string& name, double value,
                        const std::map<std::string, std::string>& labels = {}) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto key = MakeKey(name, labels);
    auto& hist = histograms_[key];
    hist.sum += value;
    hist.count++;
    hist.min = std::min(hist.min, value);
    hist.max = std::max(hist.max, value);
    
    // Update buckets
    for (auto& bucket : hist.buckets) {
      if (value <= bucket.upper_bound) {
        bucket.count++;
      }
    }
  }

  // Export all metrics in Prometheus text format
  std::string Export() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ostringstream ss;
    
    // Export counters
    for (const auto& [key, value] : counters_) {
      ss << "# TYPE " << key.name << " counter\n";
      ss << key.name << FormatLabels(key.labels) << " " << value << "\n";
    }
    
    // Export gauges
    for (const auto& [key, value] : gauges_) {
      ss << "# TYPE " << key.name << " gauge\n";
      ss << key.name << FormatLabels(key.labels) << " " << value << "\n";
    }
    
    // Export histograms
    for (const auto& [key, hist] : histograms_) {
      ss << "# TYPE " << key.name << " histogram\n";
      for (const auto& bucket : hist.buckets) {
        auto labels = key.labels;
        labels["le"] = std::to_string(bucket.upper_bound);
        ss << key.name << "_bucket" << FormatLabels(labels) << " " << bucket.count << "\n";
      }
      ss << key.name << "_sum" << FormatLabels(key.labels) << " " << hist.sum << "\n";
      ss << key.name << "_count" << FormatLabels(key.labels) << " " << hist.count << "\n";
    }
    
    return ss.str();
  }

  // Reset all metrics
  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    counters_.clear();
    gauges_.clear();
    histograms_.clear();
  }

 private:
  PrometheusExporter() = default;

  struct MetricKey {
    std::string name;
    std::map<std::string, std::string> labels;
    
    bool operator<(const MetricKey& other) const {
      if (name != other.name) return name < other.name;
      return labels < other.labels;
    }
  };

  struct HistogramBucket {
    double upper_bound;
    uint64_t count;
  };

  struct HistogramData {
    double sum = 0.0;
    uint64_t count = 0;
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::lowest();
    std::vector<HistogramBucket> buckets = {
      {0.001, 0}, {0.005, 0}, {0.01, 0}, {0.05, 0}, {0.1, 0},
      {0.5, 0}, {1.0, 0}, {5.0, 0}, {10.0, 0}, {30.0, 0}, {60.0, 0}
    };
  };

  MetricKey MakeKey(const std::string& name, 
                    const std::map<std::string, std::string>& labels) const {
    return {name, labels};
  }

  std::string FormatLabels(const std::map<std::string, std::string>& labels) const {
    if (labels.empty()) return "";
    std::ostringstream ss;
    ss << "{";
    bool first = true;
    for (const auto& [key, value] : labels) {
      if (!first) ss << ",";
      ss << key << "=\"" << EscapeLabel(value) << "\"";
      first = false;
    }
    ss << "}";
    return ss.str();
  }

  std::string EscapeLabel(const std::string& value) const {
    std::string result;
    for (char c : value) {
      switch (c) {
        case '\\': result += "\\\\"; break;
        case '"': result += "\\\""; break;
        case '\n': result += "\\n"; break;
        default: result += c;
      }
    }
    return result;
  }

  mutable std::mutex mutex_;
  std::map<MetricKey, double> counters_;
  std::map<MetricKey, double> gauges_;
  std::map<MetricKey, HistogramData> histograms_;
};

// Helper macros for easy metric collection
#define CEDAR_COUNTER_INC(name, ...) \
  cedar::PrometheusExporter::Instance().IncrementCounter(name, ##__VA_ARGS__)

#define CEDAR_GAUGE_SET(name, value, ...) \
  cedar::PrometheusExporter::Instance().SetGauge(name, value, ##__VA_ARGS__)

#define CEDAR_HISTOGRAM_OBSERVE(name, value, ...) \
  cedar::PrometheusExporter::Instance().ObserveHistogram(name, value, ##__VA_ARGS__)

}  // namespace cedar
