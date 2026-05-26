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

#include "cedar/metrics/metrics_registry.h"

#include <iomanip>
#include <sstream>

namespace cedar {
namespace metrics {

// ============================================================================
// Histogram
// ============================================================================

Histogram::Histogram(std::vector<double> buckets) : buckets_(std::move(buckets)) {
  size_t n = buckets_.size() + 1;  // +1 for +Inf bucket
  bucket_counts_.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    bucket_counts_.emplace_back(std::make_unique<std::atomic<int64_t>>(0));
  }
}

void Histogram::Observe(double value) {
  size_t idx = buckets_.size();  // default to +Inf bucket
  if (buckets_.size() > 20) {
    // Use binary search for large bucket counts
    auto it = std::lower_bound(buckets_.begin(), buckets_.end(), value);
    idx = static_cast<size_t>(it - buckets_.begin());
  } else {
    for (size_t i = 0; i < buckets_.size(); ++i) {
      if (value <= buckets_[i]) {
        idx = i;
        break;
      }
    }
  }
  bucket_counts_[idx]->fetch_add(1, std::memory_order_relaxed);
  total_count_.fetch_add(1, std::memory_order_relaxed);
  total_sum_micros_.fetch_add(static_cast<int64_t>(value * 1e6),
                               std::memory_order_relaxed);
}

int64_t Histogram::TotalCount() const {
  return total_count_.load(std::memory_order_relaxed);
}

double Histogram::TotalSum() const {
  return total_sum_micros_.load(std::memory_order_relaxed) / 1e6;
}

namespace {
constexpr double kInfMarker = std::numeric_limits<double>::max();

std::string PrometheusEscapeLabel(const std::string& label) {
  std::string out;
  out.reserve(label.size());
  for (char c : label) {
    switch (c) {
      case '\\': out += "\\\\"; break;
      case '"': out += "\\\""; break;
      case '\n': out += "\\n"; break;
      default: out += c;
    }
  }
  return out;
}

std::string FormatLabels(const LabelSet& labels) {
  if (labels.empty()) return "";
  std::string out = "{";
  bool first = true;
  for (const auto& [k, v] : labels) {
    if (!first) out += ",";
    first = false;
    out += PrometheusEscapeLabel(k) + "=\"" + PrometheusEscapeLabel(v) + "\"";
  }
  out += "}";
  return out;
}

std::string FormatBucketLabels(const LabelSet& labels, double le) {
  std::string out = "{";
  bool first = true;
  for (const auto& [k, v] : labels) {
    if (!first) out += ",";
    first = false;
    out += PrometheusEscapeLabel(k) + "=\"" + PrometheusEscapeLabel(v) + "\"";
  }
  if (!first) out += ",";
  if (le == kInfMarker) {
    out += "le=\"+Inf\"";
  } else {
    std::ostringstream le_str;
    le_str << std::fixed << std::setprecision(6) << le;
    out += "le=\"" + le_str.str() + "\"";
  }
  out += "}";
  return out;
}
}  // namespace

std::map<double, int64_t> Histogram::BucketCounts() const {
  std::map<double, int64_t> result;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    result[buckets_[i]] = bucket_counts_[i]->load(std::memory_order_relaxed);
  }
  result[kInfMarker] = bucket_counts_[buckets_.size()]->load(std::memory_order_relaxed);
  return result;
}

// ============================================================================
// MetricsRegistry
// ============================================================================

MetricsRegistry& MetricsRegistry::Instance() {
  static MetricsRegistry instance;
  return instance;
}

Counter* MetricsRegistry::GetOrCreateCounter(const std::string& name,
                                             const std::string& help) {
  return GetOrCreateCounter(name, help, {});
}

Histogram* MetricsRegistry::GetOrCreateHistogram(
    const std::string& name,
    const std::string& help,
    std::vector<double> buckets) {
  return GetOrCreateHistogram(name, help, std::move(buckets), {});
}

Counter* MetricsRegistry::GetOrCreateCounter(const std::string& name,
                                             const std::string& help,
                                             const LabelSet& labels) {
  std::lock_guard<std::mutex> lock(mutex_);
  MetricKey key{name, labels};
  auto it = counters_.find(key);
  if (it != counters_.end()) {
    return it->second.get();
  }
  auto counter = std::make_unique<Counter>();
  Counter* ptr = counter.get();
  counters_[key] = std::move(counter);
  counter_help_[key] = help;
  return ptr;
}

Histogram* MetricsRegistry::GetOrCreateHistogram(
    const std::string& name,
    const std::string& help,
    std::vector<double> buckets,
    const LabelSet& labels) {
  std::lock_guard<std::mutex> lock(mutex_);
  MetricKey key{name, labels};
  auto it = histograms_.find(key);
  if (it != histograms_.end()) {
    return it->second.get();
  }
  auto hist = std::make_unique<Histogram>(std::move(buckets));
  Histogram* ptr = hist.get();
  histograms_[key] = std::move(hist);
  histogram_help_[key] = help;
  return ptr;
}

std::string MetricsRegistry::SerializeMetrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::ostringstream out;

  // Counters
  for (const auto& [key, counter] : counters_) {
    auto help_it = counter_help_.find(key);
    if (help_it != counter_help_.end()) {
      out << "# HELP " << PrometheusEscapeLabel(key.name) << " "
          << PrometheusEscapeLabel(help_it->second) << "\n";
    }
    out << "# TYPE " << PrometheusEscapeLabel(key.name) << " counter\n";
    out << PrometheusEscapeLabel(key.name) << FormatLabels(key.labels)
        << " " << counter->Value() << "\n";
  }

  // Histograms
  for (const auto& [key, hist] : histograms_) {
    auto help_it = histogram_help_.find(key);
    if (help_it != histogram_help_.end()) {
      out << "# HELP " << PrometheusEscapeLabel(key.name) << " "
          << PrometheusEscapeLabel(help_it->second) << "\n";
    }
    out << "# TYPE " << PrometheusEscapeLabel(key.name) << " histogram\n";

    auto bucket_counts = hist->BucketCounts();
    int64_t cumulative = 0;
    for (const auto& [le, count] : bucket_counts) {
      cumulative += count;
      out << PrometheusEscapeLabel(key.name) << "_bucket"
          << FormatBucketLabels(key.labels, le) << " "
          << cumulative << "\n";
    }
    out << PrometheusEscapeLabel(key.name) << "_sum"
        << FormatLabels(key.labels) << " "
        << std::fixed << std::setprecision(6) << hist->TotalSum() << "\n";
    out << PrometheusEscapeLabel(key.name) << "_count"
        << FormatLabels(key.labels) << " " << hist->TotalCount() << "\n";
  }

  return out.str();
}

void MetricsRegistry::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  counters_.clear();
  counter_help_.clear();
  histograms_.clear();
  histogram_help_.clear();
}

}  // namespace metrics
}  // namespace cedar
