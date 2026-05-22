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
// Prometheus Escape Test — verifies label/help text escaping
// =============================================================================

#include <gtest/gtest.h>

#include "cedar/metrics/metrics_registry.h"

using namespace cedar::metrics;

class PrometheusEscapeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MetricsRegistry::Instance().Clear();
  }
};

TEST_F(PrometheusEscapeTest, EscapesQuotesAndBackslashes) {
  auto* counter = MetricsRegistry::Instance().GetOrCreateCounter(
      "bad\"name", "Help with \\ backslash and \" quote");
  counter->Increment(1);

  std::string text = MetricsRegistry::Instance().SerializeMetrics();
  EXPECT_NE(text.find("# HELP bad\\\"name Help with \\\\ backslash and \\\" quote"), std::string::npos);
  EXPECT_NE(text.find("# TYPE bad\\\"name counter"), std::string::npos);
  EXPECT_NE(text.find("bad\\\"name 1"), std::string::npos);
}
