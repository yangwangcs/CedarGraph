// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Expression Evaluator for Cypher Query Engine

#ifndef CEDAR_CYPHER_EXPRESSION_EVALUATOR_H_
#define CEDAR_CYPHER_EXPRESSION_EVALUATOR_H_

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "cedar/cypher/ast.h"
#include "cedar/cypher/value.h"

namespace cedar {
namespace cypher {

class ExecutionContext;

/**
 * @brief Expression evaluator that handles parameter substitution
 * 
 * Evaluates Cypher expressions against a record with optional
 * parameter values for parameterized queries.
 */
class ExpressionEvaluator {
 public:
  explicit ExpressionEvaluator(const ExecutionContext* context);

  /**
   * @brief Evaluate an expression and return the result value
   * @param expr The expression to evaluate
   * @param record The record containing variable bindings
   * @return The evaluated value
   */
  Value Evaluate(const Expression& expr, const Record& record);

  /**
   * @brief Build a predicate function from an expression for filtering records
   * @param expr The expression to evaluate (must return bool-ish)
   * @param params Optional parameter values for parameterized queries
   * @return A function that evaluates the expression against a record
   */
  static std::function<bool(const Record&)> BuildPredicate(
      const Expression* expr,
      const std::unordered_map<std::string, Value>& params = {});

  /**
   * @brief Set parameter values for parameterized queries
   * @param params Map of parameter name to value
   */
  void SetParameters(const std::unordered_map<std::string, Value>& params) {
    parameters_ = params;
  }

  /**
   * @brief Add a single parameter
   */
  void AddParameter(const std::string& name, const Value& value) {
    parameters_[name] = value;
  }

 private:
  const ExecutionContext* context_;
  std::unordered_map<std::string, Value> parameters_;

  // Helper methods for evaluating specific expression types
  Value EvaluateLiteral(const LiteralExpr& expr);
  Value EvaluateVariable(const VariableExpr& expr, const Record& record);
  Value EvaluateProperty(const PropertyExpr& expr, const Record& record);
  Value EvaluateComparison(const ComparisonExpr& expr, const Record& record);
  Value EvaluateLogical(const LogicalExpr& expr, const Record& record);
  Value EvaluateNot(const NotExpr& expr, const Record& record);
  Value EvaluateArithmetic(const ArithmeticExpr& expr, const Record& record);
  Value EvaluateFunctionCall(const FunctionCallExpr& expr, const Record& record);
  Value EvaluateParameter(const ParameterExpr& expr);
  Value EvaluateListLiteral(const ListLiteralExpr& expr, const Record& record);
  Value EvaluateMapLiteral(const MapLiteralExpr& expr, const Record& record);

  // Helper for type conversion
  Value CoerceToComparable(const Value& a, const Value& b);

  // String functions
  Value EvaluateStringFunction(const std::string& func_name,
                               const std::vector<Value>& args,
                               const Record& record);

  // Comparison helpers
  int CompareValues(const Value& a, const Value& b);
};

}  // namespace cypher
}  // namespace cedar

#endif  // CEDAR_CYPHER_EXPRESSION_EVALUATOR_H_
