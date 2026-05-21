// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/value.h"

using namespace cedar::cypher;

// Mock source operator that produces a fixed number of integer records
class MockSource : public PhysicalOperator {
 public:
  int total_records;
  int current = 0;

  explicit MockSource(int total) : total_records(total) {}

  bool Init(ExecutionContext*) override {
    current = 0;
    return true;
  }

  std::shared_ptr<Record> Next() override {
    if (current >= total_records) return nullptr;
    auto rec = std::make_shared<Record>();
    rec->Set("n", Value(current++));
    return rec;
  }

  std::string GetName() const override { return "MockSource"; }

  std::unique_ptr<PhysicalOperator> Clone() const override {
    auto clone = std::make_unique<MockSource>(total_records);
    for (const auto& child : children_) {
      clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
    }
    clone->current = 0;
    return clone;
  }
};

TEST(ConcurrentPlanExecution, LimitOperatorStressTest) {
  // Build plan: Limit(5) over MockSource(10)
  auto source = std::make_shared<MockSource>(10);
  auto limit = std::make_shared<Limit>(5);
  limit->AddChild(source);

  std::vector<std::string> columns = {"n"};
  auto produce = std::make_shared<ProduceResults>(columns);
  produce->AddChild(limit);

  auto plan = std::make_unique<ExecutionPlan>(produce);

  constexpr int kNumThreads = 8;
  constexpr int kIterations = 1000;

  std::atomic<int> success_count{0};
  std::atomic<int> wrong_count{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kIterations; ++i) {
        ExecutionContext ctx;
        ctx.get_all_entities_fn = [](uint64_t, uint64_t, uint64_t) {
          return std::vector<uint64_t>{};
        };
        auto result = plan->Clone()->Execute(&ctx);

        if (result.error.has_value()) {
          wrong_count.fetch_add(1);
        } else if (result.records.size() == 5) {
          success_count.fetch_add(1);
        } else {
          wrong_count.fetch_add(1);
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(success_count.load(), kNumThreads * kIterations);
  EXPECT_EQ(wrong_count.load(), 0);
}

TEST(ConcurrentPlanExecution, DistinctOperatorStressTest) {
  // Build plan: Distinct over MockSource(3) producing 0, 1, 2
  auto source = std::make_shared<MockSource>(3);

  std::vector<std::shared_ptr<Expression>> keys;
  keys.push_back(std::make_shared<VariableExpr>("n"));

  auto distinct = std::make_shared<Distinct>(keys);
  distinct->AddChild(source);

  std::vector<std::string> columns = {"n"};
  auto produce = std::make_shared<ProduceResults>(columns);
  produce->AddChild(distinct);

  auto plan = std::make_unique<ExecutionPlan>(produce);

  constexpr int kNumThreads = 8;
  constexpr int kIterations = 1000;

  std::atomic<int> success_count{0};
  std::atomic<int> wrong_count{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kIterations; ++i) {
        ExecutionContext ctx;
        ctx.get_all_entities_fn = [](uint64_t, uint64_t, uint64_t) {
          return std::vector<uint64_t>{};
        };
        auto result = plan->Clone()->Execute(&ctx);

        if (result.error.has_value()) {
          wrong_count.fetch_add(1);
        } else if (result.records.size() == 3) {
          success_count.fetch_add(1);
        } else {
          wrong_count.fetch_add(1);
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(success_count.load(), kNumThreads * kIterations);
  EXPECT_EQ(wrong_count.load(), 0);
}

TEST(ConcurrentPlanExecution, SortOperatorStressTest) {
  // Build plan: Sort over MockSource(3)
  auto source = std::make_shared<MockSource>(3);

  std::vector<std::pair<std::shared_ptr<Expression>, bool>> sort_items;
  sort_items.push_back({std::make_shared<VariableExpr>("n"), true});

  auto sort_op = std::make_shared<Sort>(sort_items);
  sort_op->AddChild(source);

  std::vector<std::string> columns = {"n"};
  auto produce = std::make_shared<ProduceResults>(columns);
  produce->AddChild(sort_op);

  auto plan = std::make_unique<ExecutionPlan>(produce);

  constexpr int kNumThreads = 8;
  constexpr int kIterations = 1000;

  std::atomic<int> success_count{0};
  std::atomic<int> wrong_count{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kIterations; ++i) {
        ExecutionContext ctx;
        ctx.get_all_entities_fn = [](uint64_t, uint64_t, uint64_t) {
          return std::vector<uint64_t>{};
        };
        auto result = plan->Clone()->Execute(&ctx);

        if (result.error.has_value()) {
          wrong_count.fetch_add(1);
        } else if (result.records.size() == 3) {
          success_count.fetch_add(1);
        } else {
          wrong_count.fetch_add(1);
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(success_count.load(), kNumThreads * kIterations);
  EXPECT_EQ(wrong_count.load(), 0);
}
