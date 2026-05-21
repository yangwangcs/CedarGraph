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
#include <thread>
#include <vector>
#include <atomic>

#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/execution_plan.h"

using namespace cedar;
using namespace cedar::cypher;

class MockPhysicalOperator : public PhysicalOperator {
 public:
  bool Init(ExecutionContext* ctx) override { return true; }
  std::shared_ptr<Record> Next() override { return nullptr; }
  std::string GetName() const override { return "MockOp"; }
};

TEST(PlanCacheThreadSafety, ConcurrentReadWrite) {
  CypherEngine engine(nullptr);

  constexpr int kNumThreads = 8;
  constexpr int kOpsPerThread = 1000;

  std::atomic<int> successes{0};
  std::atomic<int> failures{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&engine, &successes, &failures, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        std::string fp = "fp_" + std::to_string(t) + "_" + std::to_string(i % 10);

        if (i % 3 == 0) {
          // Writer: cache a dummy plan
          auto plan = std::make_unique<ExecutionPlan>(
              std::make_shared<MockPhysicalOperator>());
          engine.CachePlan(fp, std::move(plan));
        } else if (i % 3 == 1) {
          // Reader
          auto cached = engine.GetCachedPlan(fp);
          if (cached) {
            successes.fetch_add(1);
          }
        } else {
          // Mixed: read then maybe clear
          auto cached = engine.GetCachedPlan(fp);
          (void)cached;
          if (i % 7 == 0) {
            engine.ClearCache();
          }
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  // The test passes if no crash or data race is detected under TSan/ASan
  EXPECT_GE(engine.GetCacheSize(), 0u);
}

TEST(PlanCacheThreadSafety, CacheSizeIsConsistent) {
  CypherEngine engine(nullptr);

  for (int i = 0; i < 100; ++i) {
    auto plan = std::make_unique<ExecutionPlan>(
        std::make_shared<MockPhysicalOperator>());
    engine.CachePlan("fp_" + std::to_string(i), std::move(plan));
  }

  EXPECT_EQ(engine.GetCacheSize(), 100u);

  engine.ClearCache();
  EXPECT_EQ(engine.GetCacheSize(), 0u);
}
