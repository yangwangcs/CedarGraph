// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/cypher/validator.h"


namespace cedar::cypher {

QueryValidator::QueryValidator(const queryd::GraphSchema* schema)
    : schema_(schema) {}

 cedar::Status QueryValidator::Validate(const QueryStatement& stmt) {
  scope_.clear();
  if (ValidateQueryStatement(stmt)) {
    return cedar::Status::OK();
  }
  return cedar::Status::InvalidArgument(last_error_);
}

bool QueryValidator::ValidateQueryStatement(const QueryStatement& stmt) {
  std::vector<std::string> all_pushed_vars;
  for (const auto& clause : stmt.clauses) {
    std::vector<std::string> pushed_vars;
    if (auto* match = dynamic_cast<const MatchClause*>(clause.get())) {
      if (!ValidateMatchClause(*match, &pushed_vars)) return false;
    } else if (auto* where = dynamic_cast<const WhereClause*>(clause.get())) {
      if (!ValidateWhereClause(*where)) return false;
    } else if (auto* ret = dynamic_cast<const ReturnClause*>(clause.get())) {
      if (!ValidateReturnClause(*ret)) return false;
    } else if (auto* set_clause = dynamic_cast<const SetClause*>(clause.get())) {
      for (const auto& item : set_clause->items) {
        if (!ValidateExpression(*item.target)) return false;
        if (!ValidateExpression(*item.value)) return false;
      }
    } else if (auto* del_clause = dynamic_cast<const DeleteClause*>(clause.get())) {
      for (const auto& expr : del_clause->expressions) {
        if (!ValidateExpression(*expr)) return false;
      }
    } else if (auto* merge_clause = dynamic_cast<const MergeClause*>(clause.get())) {
      for (const auto& pattern : merge_clause->patterns) {
        for (const auto& elem : pattern.elements) {
          if (std::holds_alternative<NodePattern>(elem)) {
            const auto& node = std::get<NodePattern>(elem);
            if (!node.variable.empty()) {
              pushed_vars.push_back(node.variable);
            }
          } else if (std::holds_alternative<RelationshipPattern>(elem)) {
            const auto& rel = std::get<RelationshipPattern>(elem);
            if (!rel.variable.empty()) {
              pushed_vars.push_back(rel.variable);
            }
          }
        }
      }
    } else if (auto* with_clause = dynamic_cast<const WithClause*>(clause.get())) {
      for (const auto& item : with_clause->items) {
        if (!ValidateExpression(*item.expression)) return false;
      }
      // Reset scope: downstream clauses only see WITH projections
      scope_.clear();
      for (const auto& item : with_clause->items) {
        if (item.alias.has_value()) {
          PushScope(*item.alias, {});
        }
      }
    } else if (auto* unwind_clause = dynamic_cast<const UnwindClause*>(clause.get())) {
      if (!ValidateExpression(*unwind_clause->expression)) return false;
      if (!unwind_clause->alias.empty()) {
        pushed_vars.push_back(unwind_clause->alias);
      }
    }
    all_pushed_vars.insert(all_pushed_vars.end(),
                           pushed_vars.begin(), pushed_vars.end());
  }
  for (const auto& var : all_pushed_vars) {
    PopScope(var);
  }
  return true;
}

bool QueryValidator::ValidateMatchClause(const MatchClause& clause,
                                           std::vector<std::string>* pushed_vars) {
  for (const auto& pattern : clause.patterns) {
    for (const auto& elem : pattern.elements) {
      if (std::holds_alternative<NodePattern>(elem)) {
        const auto& node = std::get<NodePattern>(elem);
        if (!ValidateNodePattern(node)) return false;
        if (!node.variable.empty()) {
          if (pushed_vars) pushed_vars->push_back(node.variable);
        }
      } else if (std::holds_alternative<RelationshipPattern>(elem)) {
        const auto& rel = std::get<RelationshipPattern>(elem);
        if (!ValidateRelationshipPattern(rel)) return false;
        if (!rel.variable.empty()) {
          if (pushed_vars) pushed_vars->push_back(rel.variable);
        }
      }
    }
  }
  return true;
}

bool QueryValidator::ValidateNodePattern(const NodePattern& node) {
  if (!node.variable.empty()) {
    std::vector<std::string> props;
    for (const auto& label : node.labels) {
      if (!ValidateLabel(label, true)) return false;
      const auto* ls = schema_ ? schema_->GetNodeLabel(label) : nullptr;
      if (ls) {
        for (const auto& p : ls->properties) {
          props.push_back(p.name);
        }
      }
    }
    PushScope(node.variable, props);
  }
  return true;
}

bool QueryValidator::ValidateRelationshipPattern(const RelationshipPattern& rel) {
  for (const auto& type : rel.types) {
    if (!ValidateLabel(type, false)) return false;
  }
  if (!rel.variable.empty()) {
    std::vector<std::string> props;
    for (const auto& type : rel.types) {
      const auto* es = schema_ ? schema_->GetEdgeType(type) : nullptr;
      if (es) {
        for (const auto& p : es->properties) {
          props.push_back(p.name);
        }
      }
    }
    PushScope(rel.variable, props);
  }
  return true;
}

bool QueryValidator::ValidateWhereClause(const WhereClause& clause) {
  auto inferred = InferExpressionType(*clause.condition);
  // nullopt means type unknown (e.g., variable/property) — accept as possibly valid
  if (inferred.has_value() && inferred.value() != ValueType::kBool) {
    last_error_ = "WHERE clause must evaluate to a boolean";
    return false;
  }
  return ValidateExpression(*clause.condition);
}

bool QueryValidator::ValidateReturnClause(const ReturnClause& clause) {
  for (const auto& item : clause.items) {
    if (!ValidateExpression(*item.expression)) return false;
  }
  return true;
}

bool QueryValidator::ValidateExpression(const Expression& expr) {
  switch (expr.expr_type) {
    case ExprType::LITERAL:
    case ExprType::PARAMETER:
      return true;
    case ExprType::VARIABLE: {
      const auto& v = static_cast<const VariableExpr&>(expr);
      if (scope_.find(v.name) == scope_.end()) {
        last_error_ = "Undefined variable: " + v.name;
        return false;
      }
      return true;
    }
    case ExprType::PROPERTY: {
      return ValidatePropertyAccess(static_cast<const PropertyExpr&>(expr));
    }
    case ExprType::COMPARISON: {
      const auto& c = static_cast<const ComparisonExpr&>(expr);
      return ValidateExpression(*c.left) && ValidateExpression(*c.right);
    }
    case ExprType::AND:
    case ExprType::OR: {
      const auto& l = static_cast<const LogicalExpr&>(expr);
      return ValidateExpression(*l.left) && ValidateExpression(*l.right);
    }
    case ExprType::NOT: {
      const auto& n = static_cast<const NotExpr&>(expr);
      return ValidateExpression(*n.operand);
    }
    case ExprType::ARITHMETIC: {
      const auto& a = static_cast<const ArithmeticExpr&>(expr);
      return ValidateExpression(*a.left) && ValidateExpression(*a.right);
    }
    case ExprType::FUNCTION_CALL: {
      const auto& f = static_cast<const FunctionCallExpr&>(expr);
      for (const auto& arg : f.arguments) {
        if (!ValidateExpression(*arg)) return false;
      }
      return true;
    }
    case ExprType::LIST_LITERAL:
    case ExprType::MAP_LITERAL:
      return true;
    default:
      std::cerr << "[QueryValidator] Unknown expression type" << std::endl;
      return true;
  }
  return true;
}

bool QueryValidator::ValidatePropertyAccess(const PropertyExpr& prop) {
  auto it = scope_.find(prop.variable);
  if (it == scope_.end()) {
    last_error_ = "Undefined variable in property access: " + prop.variable;
    return false;
  }
  return true;
}

bool QueryValidator::ValidateLabel(const std::string& label, bool is_node) {
  if (!schema_) return true;
  if (is_node) {
    if (schema_->GetNodeLabel(label) == nullptr) {
      // Dynamic schema: allow unknown labels
      return true;
    }
  } else {
    if (schema_->GetEdgeType(label) == nullptr) {
      // Dynamic schema: allow unknown edge types
      return true;
    }
  }
  return true;
}

void QueryValidator::PushScope(const std::string& var,
                               const std::vector<std::string>& props) {
  scope_[var] = props;
}

void QueryValidator::PopScope(const std::string& var) {
  scope_.erase(var);
}

std::optional<ValueType> QueryValidator::InferExpressionType(const Expression& expr) {
  switch (expr.expr_type) {
    case ExprType::LITERAL:
      return static_cast<const LiteralExpr&>(expr).value.Type();
    case ExprType::VARIABLE:
    case ExprType::PROPERTY:
      return std::nullopt;  // Type unknown at validation time
    case ExprType::COMPARISON:
      return ValueType::kBool;
    case ExprType::AND:
    case ExprType::OR:
    case ExprType::NOT:
      return ValueType::kBool;
    case ExprType::ARITHMETIC:
      return ValueType::kInt;
    case ExprType::FUNCTION_CALL:
      return ValueType::kString;
    case ExprType::LIST_LITERAL:
      return ValueType::kList;
    case ExprType::MAP_LITERAL:
      return ValueType::kMap;
    case ExprType::PARAMETER:
      return std::nullopt;
    default:
      std::cerr << "[QueryValidator] Unknown expression type" << std::endl;
      return std::nullopt;
  }
  return std::nullopt;
}

}  // namespace cedar::cypher
