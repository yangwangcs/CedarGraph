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
// Metrics Registry Test — Prometheus-style exposition without external deps
// =============================================================================

#include <gtest/gtest.h>

#include "cedar/metrics/metrics_registry.h"

using namespace cedar::metrics;

class MetricsRegistryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MetricsRegistry::Instance().Clear();
  }
};

TEST_F(MetricsRegistryTest, CounterIncrements) {
  auto* counter = MetricsRegistry::Instance().GetOrCreateCounter(
      "test_counter", "A test counter");
  EXPECT_EQ(counter->Value(), 0);
  counter->Increment();
  EXPECT_EQ(counter->Value(), 1);
  counter->Increment(5);
  EXPECT_EQ(counter->Value(), 6);
}

TEST_F(MetricsRegistryTest, CounterReturnsSameInstance) {
  auto* c1 = MetricsRegistry::Instance().GetOrCreateCounter(
      "same_counter", "Help text");
  auto* c2 = MetricsRegistry::Instance().GetOrCreateCounter(
      "same_counter", "Help text");
  EXPECT_EQ(c1, c2);
}

TEST_F(MetricsRegistryTest, HistogramObservesAndBuckets) {
  auto* hist = MetricsRegistry::Instance().GetOrCreateHistogram(
      "test_hist", "A test histogram", {10, 50, 100});
  hist->Observe(5);
  hist->Observe(25);
  hist->Observe(75);
  hist->Observe(150);

  EXPECT_EQ(hist->TotalCount(), 4);
  auto counts = hist->BucketCounts();
  EXPECT_EQ(counts[10], 1);    // 5
  EXPECT_EQ(counts[50], 1);    // 25
  EXPECT_EQ(counts[100], 1);   // 75
  EXPECT_EQ(hist->TotalCount(), 4);
}

TEST_F(MetricsRegistryTest, SerializeMetricsIncludesCounter) {
  auto* counter = MetricsRegistry::Instance().GetOrCreateCounter(
      "queries_total", "Total queries");
  counter->Increment(42);

  std::string text = MetricsRegistry::Instance().SerializeMetrics();
  EXPECT_NE(text.find("# HELP queries_total Total queries"), std::string::npos);
  EXPECT_NE(text.find("# TYPE queries_total counter"), std::string::npos);
  EXPECT_NE(text.find("queries_total 42"), std::string::npos);
}

TEST_F(MetricsRegistryTest, SerializeMetricsIncludesHistogram) {
  auto* hist = MetricsRegistry::Instance().GetOrCreateHistogram(
      "latency_us", "Latency", {1000, 5000});
  hist->Observe(500);
  hist->Observe(3000);
  hist->Observe(7000);

  std::string text = MetricsRegistry::Instance().SerializeMetrics();
  EXPECT_NE(text.find("# HELP latency_us Latency"), std::string::npos);
  EXPECT_NE(text.find("# TYPE latency_us histogram"), std::string::npos);
  EXPECT_NE(text.find("latency_us_bucket{le=\"1000.000000\"} 1"), std::string::npos);
  EXPECT_NE(text.find("latency_us_bucket{le=\"5000.000000\"} 2"), std::string::npos);
  EXPECT_NE(text.find("latency_us_bucket{le=\"+Inf\"}"), std::string::npos);
  EXPECT_NE(text.find("latency_us_count 3"), std::string::npos);
}
