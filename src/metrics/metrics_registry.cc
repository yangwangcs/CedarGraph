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
}

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
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = counters_.find(name);
  if (it != counters_.end()) {
    return it->second.get();
  }
  auto counter = std::make_unique<Counter>();
  Counter* ptr = counter.get();
  counters_[name] = std::move(counter);
  counter_help_[name] = help;
  return ptr;
}

Histogram* MetricsRegistry::GetOrCreateHistogram(
    const std::string& name,
    const std::string& help,
    std::vector<double> buckets) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = histograms_.find(name);
  if (it != histograms_.end()) {
    return it->second.get();
  }
  auto hist = std::make_unique<Histogram>(std::move(buckets));
  Histogram* ptr = hist.get();
  histograms_[name] = std::move(hist);
  histogram_help_[name] = help;
  return ptr;
}

std::string MetricsRegistry::SerializeMetrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::ostringstream out;

  // Counters
  for (const auto& [name, counter] : counters_) {
    auto help_it = counter_help_.find(name);
    if (help_it != counter_help_.end()) {
      out << "# HELP " << name << " " << help_it->second << "\n";
    }
    out << "# TYPE " << name << " counter\n";
    out << name << " " << counter->Value() << "\n";
  }

  // Histograms
  for (const auto& [name, hist] : histograms_) {
    auto help_it = histogram_help_.find(name);
    if (help_it != histogram_help_.end()) {
      out << "# HELP " << name << " " << help_it->second << "\n";
    }
    out << "# TYPE " << name << " histogram\n";

    auto bucket_counts = hist->BucketCounts();
    int64_t cumulative = 0;
    for (const auto& [le, count] : bucket_counts) {
      cumulative += count;
      if (le == kInfMarker) {
        out << name << "_bucket{le=\"+Inf\"} ";
      } else {
        out << name << "_bucket{le=\"" << std::fixed << std::setprecision(6)
            << le << "\"} ";
      }
      out << cumulative << "\n";
    }
    out << name << "_sum " << std::fixed << std::setprecision(6)
        << hist->TotalSum() << "\n";
    out << name << "_count " << hist->TotalCount() << "\n";
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
