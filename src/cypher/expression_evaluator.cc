// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Expression Evaluator Implementation

#include "cedar/cypher/expression_evaluator.h"
#include "cedar/cypher/execution_plan.h"
#include <algorithm>
#include <cctype>
#include <cmath>

namespace cedar {
namespace cypher {

ExpressionEvaluator::ExpressionEvaluator(const ExecutionContext* context)
    : context_(context) {}

std::function<bool(const Record&)> ExpressionEvaluator::BuildPredicate(
    const Expression* expr,
    const std::unordered_map<std::string, Value>& params) {
  if (!expr) {
    return [](const Record&) { return true; };
  }
  return [expr, params](const Record& record) -> bool {
    ExpressionEvaluator evaluator(nullptr);
    evaluator.SetParameters(params);
    Value result = evaluator.Evaluate(*expr, record);
    return result.GetBool();
  };
}

Value ExpressionEvaluator::Evaluate(const Expression& expr, const Record& record) {
  switch (expr.expr_type) {
    case ExprType::LITERAL:
      return EvaluateLiteral(static_cast<const LiteralExpr&>(expr));
    case ExprType::VARIABLE:
      return EvaluateVariable(static_cast<const VariableExpr&>(expr), record);
    case ExprType::PROPERTY:
      return EvaluateProperty(static_cast<const PropertyExpr&>(expr), record);
    case ExprType::COMPARISON:
      return EvaluateComparison(static_cast<const ComparisonExpr&>(expr), record);
    case ExprType::AND:
    case ExprType::OR:
      return EvaluateLogical(static_cast<const LogicalExpr&>(expr), record);
    case ExprType::NOT:
      return EvaluateNot(static_cast<const NotExpr&>(expr), record);
    case ExprType::ARITHMETIC:
      return EvaluateArithmetic(static_cast<const ArithmeticExpr&>(expr), record);
    case ExprType::FUNCTION_CALL:
      return EvaluateFunctionCall(static_cast<const FunctionCallExpr&>(expr), record);
    case ExprType::PARAMETER:
      return EvaluateParameter(static_cast<const ParameterExpr&>(expr));
    case ExprType::LIST_LITERAL:
      return EvaluateListLiteral(static_cast<const ListLiteralExpr&>(expr), record);
    case ExprType::MAP_LITERAL:
      return EvaluateMapLiteral(static_cast<const MapLiteralExpr&>(expr), record);
    default:
      return Value();
  }
}

Value ExpressionEvaluator::EvaluateLiteral(const LiteralExpr& expr) {
  return expr.value;
}

Value ExpressionEvaluator::EvaluateVariable(const VariableExpr& expr,
                                            const Record& record) {
  // First check record
  if (auto val = record.Get(expr.name)) {
    return *val;
  }

  // Then check context variables
  if (context_ && context_->variables.find(expr.name) != context_->variables.end()) {
    return context_->variables.at(expr.name);
  }

  // Parameter fallback
  if (parameters_.find(expr.name) != parameters_.end()) {
    return parameters_.at(expr.name);
  }

  return Value();  // Null
}

Value ExpressionEvaluator::EvaluateProperty(const PropertyExpr& expr,
                                            const Record& record) {
  // Get the variable value
  auto var_val = EvaluateVariable(VariableExpr(expr.variable), record);

  // Property access on Node or Map
  if (var_val.IsNode() || var_val.IsMap()) {
    auto prop = var_val.GetProperty(expr.property);
    if (prop) return *prop;
    return Value();  // Null if property not found
  }

  // Property access on string (builtins)
  if (var_val.IsString()) {
    const std::string& str = var_val.GetString();
    if (expr.property == "length" || expr.property == "size") {
      return Value(static_cast<int64_t>(str.size()));
    }
    if (expr.property == "toUpper") {
      std::string upper = str;
      std::transform(upper.begin(), upper.end(), upper.begin(),
                     [](unsigned char c) { return std::toupper(c); });
      return Value(upper);
    }
    if (expr.property == "toLower") {
      std::string lower = str;
      std::transform(lower.begin(), lower.end(), lower.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      return Value(lower);
    }
  }

  return Value();
}

Value ExpressionEvaluator::EvaluateComparison(const ComparisonExpr& expr,
                                               const Record& record) {
  auto left_val = Evaluate(*expr.left, record);
  auto right_val = Evaluate(*expr.right, record);

  int cmp = CompareValues(left_val, right_val);

  switch (expr.op) {
    case ComparisonExpr::EQ:
      return Value(cmp == 0);
    case ComparisonExpr::NE:
      return Value(cmp != 0);
    case ComparisonExpr::LT:
      return Value(cmp < 0);
    case ComparisonExpr::GT:
      return Value(cmp > 0);
    case ComparisonExpr::LE:
      return Value(cmp <= 0);
    case ComparisonExpr::GE:
      return Value(cmp >= 0);
    default:
      return Value();
  }
}

Value ExpressionEvaluator::EvaluateLogical(const LogicalExpr& expr,
                                            const Record& record) {
  auto left_val = Evaluate(*expr.left, record);
  bool left_bool = left_val.IsNull() ? false : left_val.GetBool();

  if (expr.op == LogicalExpr::Op::AND) {
    if (!left_bool) {
      return Value(false);  // Short-circuit
    }
    auto right_val = Evaluate(*expr.right, record);
    bool right_bool = right_val.IsNull() ? false : right_val.GetBool();
    return Value(right_bool);
  } else {  // OR
    if (left_bool) {
      return Value(true);  // Short-circuit
    }
    auto right_val = Evaluate(*expr.right, record);
    bool right_bool = right_val.IsNull() ? false : right_val.GetBool();
    return Value(right_bool);
  }
}

Value ExpressionEvaluator::EvaluateNot(const NotExpr& expr, const Record& record) {
  auto val = Evaluate(*expr.operand, record);
  bool bool_val = val.IsNull() ? false : val.GetBool();
  return Value(!bool_val);
}

Value ExpressionEvaluator::EvaluateArithmetic(const ArithmeticExpr& expr,
                                             const Record& record) {
  auto left_val = Evaluate(*expr.left, record);
  auto right_val = Evaluate(*expr.right, record);

  // Numeric operations
  if (left_val.IsInt() && right_val.IsInt()) {
    int64_t a = left_val.GetInt();
    int64_t b = right_val.GetInt();

    switch (expr.op) {
      case ArithmeticExpr::ADD: return Value(a + b);
      case ArithmeticExpr::SUB: return Value(a - b);
      case ArithmeticExpr::MUL: return Value(a * b);
      case ArithmeticExpr::DIV:
        if (b != 0) return Value(a / b);
        return Value();
      case ArithmeticExpr::MOD:
        if (b != 0) return Value(a % b);
        return Value();
      default:
        std::cerr << "[ExpressionEvaluator] Unknown arithmetic op" << std::endl;
        return Value();
    }
  }

  // Floating point
  double a = left_val.IsInt() ? static_cast<double>(left_val.GetInt()) :
           (left_val.IsFloat() ? left_val.GetFloat() : 0.0);
  double b = right_val.IsInt() ? static_cast<double>(right_val.GetInt()) :
           (right_val.IsFloat() ? right_val.GetFloat() : 0.0);

  switch (expr.op) {
    case ArithmeticExpr::ADD: return Value(a + b);
    case ArithmeticExpr::SUB: return Value(a - b);
    case ArithmeticExpr::MUL: return Value(a * b);
    case ArithmeticExpr::DIV: return Value(b != 0 ? a / b : 0.0);
    case ArithmeticExpr::MOD: return Value(std::fmod(a, b));
    default:
      std::cerr << "[ExpressionEvaluator] Unknown arithmetic op" << std::endl;
      return Value();
  }

  return Value();
}

Value ExpressionEvaluator::EvaluateFunctionCall(const FunctionCallExpr& expr,
                                                 const Record& record) {
  std::string lower_name = expr.name;
  std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  // Evaluate arguments
  std::vector<Value> args;
  for (const auto& arg : expr.arguments) {
    args.push_back(Evaluate(*arg, record));
  }

  // String functions
  if (lower_name == "starts_with") {
    if (args.size() >= 2 && args[0].IsString() && args[1].IsString()) {
      const std::string& str = args[0].GetString();
      const std::string& prefix = args[1].GetString();
      return Value(str.find(prefix) == 0);
    }
    return Value(false);
  }

  if (lower_name == "ends_with") {
    if (args.size() >= 2 && args[0].IsString() && args[1].IsString()) {
      const std::string& str = args[0].GetString();
      const std::string& suffix = args[1].GetString();
      return Value(str.size() >= suffix.size() &&
                  str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0);
    }
    return Value(false);
  }

  if (lower_name == "contains") {
    if (args.size() >= 2 && args[0].IsString() && args[1].IsString()) {
      return Value(args[0].GetString().find(args[1].GetString()) != std::string::npos);
    }
    return Value(false);
  }

  if (lower_name == "tostring" || lower_name == "string") {
    if (args.size() >= 1) {
      return Value(args[0].ToString());
    }
    return Value("");
  }

  if (lower_name == "tolower") {
    if (args.size() >= 1 && args[0].IsString()) {
      std::string result = args[0].GetString();
      std::transform(result.begin(), result.end(), result.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      return Value(result);
    }
    return Value();
  }

  if (lower_name == "toupper") {
    if (args.size() >= 1 && args[0].IsString()) {
      std::string result = args[0].GetString();
      std::transform(result.begin(), result.end(), result.begin(),
                     [](unsigned char c) { return std::toupper(c); });
      return Value(result);
    }
    return Value();
  }

  if (lower_name == "trim" || lower_name == "ltrim" || lower_name == "rtrim") {
    if (args.size() >= 1 && args[0].IsString()) {
      std::string result = args[0].GetString();
      if (lower_name == "trim") {
        // Remove leading and trailing whitespace
        auto start = result.find_first_not_of(" \t\n\r");
        auto end = result.find_last_not_of(" \t\n\r");
        if (start != std::string::npos) {
          result = result.substr(start, end - start + 1);
        } else {
          result = "";
        }
      } else if (lower_name == "ltrim") {
        auto start = result.find_first_not_of(" \t\n\r");
        result = (start != std::string::npos) ? result.substr(start) : "";
      } else {
        auto end = result.find_last_not_of(" \t\n\r");
        result = (end != std::string::npos) ? result.substr(0, end + 1) : "";
      }
      return Value(result);
    }
    return Value();
  }

  if (lower_name == "substring" || lower_name == "substr") {
    if (args.size() >= 2 && args[0].IsString()) {
      const std::string& str = args[0].GetString();
      int64_t start = args[1].IsInt() ? args[1].GetInt() : 0;
      if (start < 0) start = 0;
      if (start >= static_cast<int64_t>(str.size())) return Value("");

      if (args.size() >= 3 && args[2].IsInt()) {
        int64_t len = args[2].GetInt();
        if (len < 0) len = 0;
        return Value(str.substr(start, len));
      }
      return Value(str.substr(start));
    }
    return Value();
  }

  if (lower_name == "replace") {
    if (args.size() >= 3 && args[0].IsString() &&
        args[1].IsString() && args[2].IsString()) {
      std::string str = args[0].GetString();
      std::string from = args[1].GetString();
      std::string to = args[2].GetString();
      size_t pos = 0;
      while ((pos = str.find(from, pos)) != std::string::npos) {
        str.replace(pos, from.length(), to);
        pos += to.length();
      }
      return Value(str);
    }
    return Value();
  }

  if (lower_name == "size" || lower_name == "length") {
    if (args.size() >= 1) {
      if (args[0].IsString()) {
        return Value(static_cast<int64_t>(args[0].GetString().size()));
      }
      if (args[0].IsList()) {
        return Value(static_cast<int64_t>(args[0].GetList().size()));
      }
    }
    return Value(0);
  }

  if (lower_name == "coalesce") {
    for (const auto& arg : args) {
      if (!arg.IsNull()) {
        return arg;
      }
    }
    return Value();
  }

  if (lower_name == "head") {
    if (args.size() >= 1 && args[0].IsList() && !args[0].GetList().empty()) {
      return args[0].GetList()[0];
    }
    return Value();
  }

  if (lower_name == "last") {
    if (args.size() >= 1 && args[0].IsList() && !args[0].GetList().empty()) {
      return args[0].GetList().back();
    }
    return Value();
  }

  if (lower_name == "abs") {
    if (args.size() >= 1 && args[0].IsInt()) {
      return Value(std::abs(args[0].GetInt()));
    }
    if (args.size() >= 1 && args[0].IsFloat()) {
      return Value(std::abs(args[0].GetFloat()));
    }
    return Value();
  }

  if (lower_name == "round" || lower_name == "floor" || lower_name == "ceil") {
    if (args.size() >= 1 && args[0].IsFloat()) {
      double val = args[0].GetFloat();
      if (lower_name == "round") {
        return Value(static_cast<int64_t>(std::round(val)));
      }
      if (lower_name == "floor") {
        return Value(static_cast<int64_t>(std::floor(val)));
      }
      return Value(static_cast<int64_t>(std::ceil(val)));
    }
    return Value();
  }

  // Unknown function - return null
  return Value();
}

Value ExpressionEvaluator::EvaluateParameter(const ParameterExpr& expr) {
  auto it = parameters_.find(expr.name);
  if (it != parameters_.end()) {
    return it->second;
  }
  // Fallback to context variables
  if (context_ && context_->variables.find(expr.name) != context_->variables.end()) {
    return context_->variables.at(expr.name);
  }
  return Value();  // Null if parameter not provided
}

Value ExpressionEvaluator::EvaluateListLiteral(const ListLiteralExpr& expr,
                                               const Record& record) {
  std::vector<Value> values;
  for (const auto& elem : expr.elements) {
    values.push_back(Evaluate(*elem, record));
  }
  return Value(values);
}

Value ExpressionEvaluator::EvaluateMapLiteral(const MapLiteralExpr& expr,
                                              const Record& record) {
  std::map<std::string, Value> entries;
  for (const auto& [key, val_expr] : expr.entries) {
    entries[key] = Evaluate(*val_expr, record);
  }
  return Value(entries);
}

int ExpressionEvaluator::CompareValues(const Value& a, const Value& b) {
  // Handle nulls
  if (a.IsNull() && b.IsNull()) return 0;
  if (a.IsNull()) return -1;
  if (b.IsNull()) return 1;

  // Compare based on types
  if (a.IsInt() && b.IsInt()) {
    return (a.GetInt() < b.GetInt()) ? -1 : (a.GetInt() > b.GetInt()) ? 1 : 0;
  }

  if (a.IsFloat() || b.IsFloat()) {
    double a_val = a.IsFloat() ? a.GetFloat() : static_cast<double>(a.GetInt());
    double b_val = b.IsFloat() ? b.GetFloat() : static_cast<double>(b.GetInt());
    return (a_val < b_val) ? -1 : (a_val > b_val) ? 1 : 0;
  }

  if (a.IsString() && b.IsString()) {
    return a.GetString().compare(b.GetString());
  }

  if (a.IsBool() && b.IsBool()) {
    return (a.GetBool() == b.GetBool()) ? 0 : (a.GetBool() ? 1 : -1);
  }

  // Fallback to string comparison
  return a.ToString().compare(b.ToString());
}

}  // namespace cypher
}  // namespace cedar