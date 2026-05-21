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
// PushdownPredicate Test — Verify IsTimeOnly logic
// =============================================================================

#include <gtest/gtest.h>

#include "cedar/graph/pushdown_predicate.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

TEST(PushdownPredicateTest, IsTimeOnly_BothTimeFieldsSet_ReturnsTrue) {
  PushdownPredicate pred;
  pred.time_start = Timestamp(1000);
  pred.time_end = Timestamp(2000);
  EXPECT_TRUE(pred.IsTimeOnly());
}

TEST(PushdownPredicateTest, IsTimeOnly_MissingTimeStart_ReturnsFalse) {
  PushdownPredicate pred;
  pred.time_end = Timestamp(2000);
  EXPECT_FALSE(pred.IsTimeOnly());
}

TEST(PushdownPredicateTest, IsTimeOnly_MissingTimeEnd_ReturnsFalse) {
  PushdownPredicate pred;
  pred.time_start = Timestamp(1000);
  EXPECT_FALSE(pred.IsTimeOnly());
}

TEST(PushdownPredicateTest, IsTimeOnly_WithPredicate_ReturnsFalse) {
  PushdownPredicate pred;
  pred.time_start = Timestamp(1000);
  pred.time_end = Timestamp(2000);
  pred.property_filters.emplace_back("age", FilterOp::GT, Descriptor::InlineInt(0, 18));
  EXPECT_FALSE(pred.IsTimeOnly());
}

TEST(PushdownPredicateTest, IsTimeOnly_WithLabel_ReturnsFalse) {
  PushdownPredicate pred;
  pred.time_start = Timestamp(1000);
  pred.time_end = Timestamp(2000);
  pred.projected_columns.push_back(1);
  EXPECT_FALSE(pred.IsTimeOnly());
}
