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
// Expression Evaluator Null Safety Test
// =============================================================================
// Verifies that EvaluateLogical and EvaluateNot handle null values safely
// without throwing std::bad_variant_access.
// =============================================================================

#include <gtest/gtest.h>

#include "cedar/cypher/ast.h"
#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/cypher/value.h"

using namespace cedar::cypher;

TEST(ExpressionNullSafetyTest, NullAndFalse) {
  ExecutionContext ctx;
  Record record;
  ExpressionEvaluator evaluator(&ctx);

  auto null_lit = std::make_shared<LiteralExpr>(Value::Null());
  auto false_lit = std::make_shared<LiteralExpr>(Value(false));
  auto expr = std::make_shared<LogicalExpr>(LogicalExpr::Op::AND, null_lit, false_lit);

  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsBool());
  EXPECT_FALSE(result.GetBool());
}

TEST(ExpressionNullSafetyTest, NullOrTrue) {
  ExecutionContext ctx;
  Record record;
  ExpressionEvaluator evaluator(&ctx);

  auto null_lit = std::make_shared<LiteralExpr>(Value::Null());
  auto true_lit = std::make_shared<LiteralExpr>(Value(true));
  auto expr = std::make_shared<LogicalExpr>(LogicalExpr::Op::OR, null_lit, true_lit);

  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.GetBool());
}

TEST(ExpressionNullSafetyTest, NotNull) {
  ExecutionContext ctx;
  Record record;
  ExpressionEvaluator evaluator(&ctx);

  auto null_lit = std::make_shared<LiteralExpr>(Value::Null());
  auto expr = std::make_shared<NotExpr>(null_lit);

  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.GetBool());
}

TEST(ExpressionNullSafetyTest, NullAndNull) {
  ExecutionContext ctx;
  Record record;
  ExpressionEvaluator evaluator(&ctx);

  auto null_lit = std::make_shared<LiteralExpr>(Value::Null());
  auto expr = std::make_shared<LogicalExpr>(LogicalExpr::Op::AND, null_lit, null_lit);

  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsBool());
  EXPECT_FALSE(result.GetBool());
}

TEST(ExpressionNullSafetyTest, NullOrNull) {
  ExecutionContext ctx;
  Record record;
  ExpressionEvaluator evaluator(&ctx);

  auto null_lit = std::make_shared<LiteralExpr>(Value::Null());
  auto expr = std::make_shared<LogicalExpr>(LogicalExpr::Op::OR, null_lit, null_lit);

  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsBool());
  EXPECT_FALSE(result.GetBool());
}

TEST(ExpressionNullSafetyTest, TrueAndNull) {
  ExecutionContext ctx;
  Record record;
  ExpressionEvaluator evaluator(&ctx);

  auto true_lit = std::make_shared<LiteralExpr>(Value(true));
  auto null_lit = std::make_shared<LiteralExpr>(Value::Null());
  auto expr = std::make_shared<LogicalExpr>(LogicalExpr::Op::AND, true_lit, null_lit);

  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsBool());
  EXPECT_FALSE(result.GetBool());
}

TEST(ExpressionNullSafetyTest, FalseOrNull) {
  ExecutionContext ctx;
  Record record;
  ExpressionEvaluator evaluator(&ctx);

  auto false_lit = std::make_shared<LiteralExpr>(Value(false));
  auto null_lit = std::make_shared<LiteralExpr>(Value::Null());
  auto expr = std::make_shared<LogicalExpr>(LogicalExpr::Op::OR, false_lit, null_lit);

  auto result = evaluator.Evaluate(*expr, record);
  EXPECT_TRUE(result.IsBool());
  EXPECT_FALSE(result.GetBool());
}

TEST(ExpressionNullSafetyTest, GetBoolOnNullReturnsFalse) {
  Value v = Value::Null();
  EXPECT_FALSE(v.GetBool());
}

TEST(ExpressionNullSafetyTest, GetBoolOnIntReturnsFalse) {
  Value v(42);
  EXPECT_FALSE(v.GetBool());
}
