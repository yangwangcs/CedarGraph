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

#ifndef CEDAR_METRICS_METRICS_REGISTRY_H_
#define CEDAR_METRICS_METRICS_REGISTRY_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace cedar {
namespace metrics {

using LabelSet = std::map<std::string, std::string>;

/// Simple atomic counter (Prometheus-style, monotonically increasing).
class Counter {
 public:
  void Increment() { value_.fetch_add(1, std::memory_order_relaxed); }
  void Increment(double amount) {
    // For integer-only backend we accumulate into a 64-bit atomic
    int64_t inc = static_cast<int64_t>(amount);
    value_.fetch_add(inc, std::memory_order_relaxed);
  }
  int64_t Value() const { return value_.load(std::memory_order_relaxed); }

 private:
  std::atomic<int64_t> value_{0};
};

/// Thread-safe histogram with fixed buckets (Prometheus-style).
class Histogram {
 public:
  explicit Histogram(std::vector<double> buckets);

  void Observe(double value);
  int64_t TotalCount() const;
  double TotalSum() const;
  const std::vector<double>& Buckets() const { return buckets_; }
  std::map<double, int64_t> BucketCounts() const;

 private:
  std::vector<double> buckets_;
  std::vector<std::unique_ptr<std::atomic<int64_t>>> bucket_counts_;
  std::atomic<int64_t> total_count_{0};
  std::atomic<int64_t> total_sum_micros_{0};  // Store sum as micros for atomic
};

/// Process-wide metrics registry (singleton).
/// Thread-safe for registration; individual metrics are lock-free.
class MetricsRegistry {
 public:
  static MetricsRegistry& Instance();

  Counter* GetOrCreateCounter(const std::string& name,
                              const std::string& help);
  Histogram* GetOrCreateHistogram(const std::string& name,
                                  const std::string& help,
                                  std::vector<double> buckets);

  // NEW: with labels
  Counter* GetOrCreateCounter(const std::string& name, const std::string& help,
                              const LabelSet& labels);
  Histogram* GetOrCreateHistogram(const std::string& name,
                                  const std::string& help,
                                  std::vector<double> buckets,
                                  const LabelSet& labels);

  // Serialize all metrics in Prometheus text exposition format
  std::string SerializeMetrics() const;

  // For testing
  void Clear();

 private:
  MetricsRegistry() = default;

  struct MetricKey {
    std::string name;
    LabelSet labels;
    bool operator<(const MetricKey& o) const {
      if (name != o.name) return name < o.name;
      return labels < o.labels;
    }
  };

  mutable std::mutex mutex_;
  std::map<MetricKey, std::unique_ptr<Counter>> counters_;
  std::map<MetricKey, std::unique_ptr<Histogram>> histograms_;
  std::map<MetricKey, std::string> counter_help_;
  std::map<MetricKey, std::string> histogram_help_;
};

}  // namespace metrics
}  // namespace cedar

#endif  // CEDAR_METRICS_METRICS_REGISTRY_H_
