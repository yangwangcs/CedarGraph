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

#include <gtest/gtest.h>
#include "cedar/metrics/metrics_registry.h"

TEST(MetricsRegistry, LabelsAppearInSerialization) {
  auto& reg = cedar::metrics::MetricsRegistry::Instance();
  reg.Clear();
  auto* c = reg.GetOrCreateCounter("requests_total", "Total requests",
                                    {{"partition", "p1"}});
  c->Increment();
  std::string out = reg.SerializeMetrics();
  EXPECT_NE(out.find("partition=\"p1\""), std::string::npos);
  EXPECT_NE(out.find("requests_total"), std::string::npos);
}

TEST(MetricsRegistry, UnlabeledMetricsStillWork) {
  auto& reg = cedar::metrics::MetricsRegistry::Instance();
  reg.Clear();
  auto* c = reg.GetOrCreateCounter("simple_counter", "A simple counter");
  c->Increment();
  std::string out = reg.SerializeMetrics();
  EXPECT_NE(out.find("simple_counter"), std::string::npos);
}

TEST(MetricsRegistry, HistogramWithLabels) {
  auto& reg = cedar::metrics::MetricsRegistry::Instance();
  reg.Clear();
  auto* h = reg.GetOrCreateHistogram("latency_seconds", "Request latency",
                                      std::vector<double>{0.1, 0.5, 1.0},
                                      {{"node", "n1"}});
  h->Observe(0.3);
  std::string out = reg.SerializeMetrics();
  EXPECT_NE(out.find("node=\"n1\""), std::string::npos);
}
