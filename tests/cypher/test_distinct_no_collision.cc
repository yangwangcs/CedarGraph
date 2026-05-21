// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Distinct hash-collision regression test.

#include <gtest/gtest.h>
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/ast.h"

using namespace cedar::cypher;

// ---------------------------------------------------------------------------
// Mock source operator that yields a fixed set of records.
// ---------------------------------------------------------------------------
class MockSource : public PhysicalOperator {
 public:
  std::vector<Record> records;
  size_t index = 0;

  bool Init(ExecutionContext* ctx) override {
    (void)ctx;
    index = 0;
    return true;
  }

  std::shared_ptr<Record> Next() override {
    if (index < records.size()) {
      return std::make_shared<Record>(records[index++]);
    }
    return nullptr;
  }

  std::string GetName() const override { return "MockSource"; }

  std::unique_ptr<PhysicalOperator> Clone() const override {
    auto clone = std::make_unique<MockSource>();
    clone->records = records;
    clone->index = 0;
    return clone;
  }
};

// ---------------------------------------------------------------------------
// Verify that two records with the same combined hash but different values
// are both preserved by Distinct (i.e. no false duplicates caused by hash
// collisions).
//
// Distinct combined hash: h = h * 31 + val.Hash()
//   Value(0).Hash()  == 2 ^ 0  == 2
//   Value(1).Hash()  == 2 ^ 1  == 3
//   Value(29).Hash() == 2 ^ 29 == 31
//   Value(2).Hash()  == 2 ^ 2  == 0
//
// Record A keys [0, 29] -> 2 * 31 + 31 == 93
// Record B keys [1, 2]  -> 3 * 31 + 0  == 93
// ---------------------------------------------------------------------------
TEST(DistinctNoCollision, HashCollisionPreservesBothRecords) {
  auto source = std::make_shared<MockSource>();

  Record rec_a;
  rec_a.Set("a", Value(static_cast<int64_t>(0)));
  rec_a.Set("b", Value(static_cast<int64_t>(29)));
  source->records.push_back(rec_a);

  Record rec_b;
  rec_b.Set("a", Value(static_cast<int64_t>(1)));
  rec_b.Set("b", Value(static_cast<int64_t>(2)));
  source->records.push_back(rec_b);

  std::vector<std::shared_ptr<Expression>> keys;
  keys.push_back(std::make_shared<VariableExpr>("a"));
  keys.push_back(std::make_shared<VariableExpr>("b"));

  auto distinct = std::make_shared<Distinct>(keys);
  distinct->AddChild(source);

  ExecutionContext ctx;
  ASSERT_TRUE(distinct->Init(&ctx));

  auto out1 = distinct->Next();
  ASSERT_NE(out1, nullptr);

  auto out2 = distinct->Next();
  ASSERT_NE(out2, nullptr);

  auto out3 = distinct->Next();
  EXPECT_EQ(out3, nullptr);

  // The two returned records must have different actual values.
  EXPECT_NE(out1->Get("a"), out2->Get("a"));
  EXPECT_NE(out1->Get("b"), out2->Get("b"));
}

// ---------------------------------------------------------------------------
// Verify that truly identical records are still deduplicated.
// ---------------------------------------------------------------------------
TEST(DistinctNoCollision, IdenticalRecordsAreDeduped) {
  auto source = std::make_shared<MockSource>();

  Record rec;
  rec.Set("a", Value(static_cast<int64_t>(42)));
  source->records.push_back(rec);
  source->records.push_back(rec);

  std::vector<std::shared_ptr<Expression>> keys;
  keys.push_back(std::make_shared<VariableExpr>("a"));

  auto distinct = std::make_shared<Distinct>(keys);
  distinct->AddChild(source);

  ExecutionContext ctx;
  ASSERT_TRUE(distinct->Init(&ctx));

  auto out1 = distinct->Next();
  ASSERT_NE(out1, nullptr);
  EXPECT_EQ(out1->Get("a"), Value(static_cast<int64_t>(42)));

  auto out2 = distinct->Next();
  EXPECT_EQ(out2, nullptr);
}
