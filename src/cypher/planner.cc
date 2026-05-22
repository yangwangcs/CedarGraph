// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Cypher Query Planner Implementation

#include "cedar/cypher/planner.h"

#include <sstream>

#include "cedar/cypher/expression_evaluator.h"

namespace cedar::cypher {

// ============================================================================
// QueryPlanner::Plan - main entry point
// ============================================================================

std::unique_ptr<ExecutionPlan> QueryPlanner::Plan(
    const QueryStatement& query) {
  // Delegate to ExecutionPlanBuilder for consistency
  auto stmt = std::make_shared<QueryStatement>(query);
  auto root = ExecutionPlanBuilder::Build(stmt);
  if (!root) {
    last_error_ = "Failed to build execution plan";
    return nullptr;
  }
  return std::make_unique<ExecutionPlan>(root);
}

// ============================================================================
// Clause-level planners
// ============================================================================

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanClause(
    const QueryClause& clause,
    std::unique_ptr<PhysicalOperator> input) {
  switch (clause.clause_type) {
    case ClauseType::MATCH:
      return PlanMatch(static_cast<const MatchClause&>(clause),
                       std::move(input));
    case ClauseType::WHERE:
      return PlanWhere(static_cast<const WhereClause&>(clause),
                       std::move(input));
    case ClauseType::RETURN:
      return PlanReturn(static_cast<const ReturnClause&>(clause),
                        std::move(input));
    case ClauseType::ORDER_BY:
      return PlanOrderBy(static_cast<const OrderByClause&>(clause),
                         std::move(input));
    case ClauseType::LIMIT:
      return PlanLimit(static_cast<const LimitClause&>(clause),
                       std::move(input));
    case ClauseType::SKIP:
      return PlanSkip(static_cast<const SkipClause&>(clause),
                      std::move(input));
    default:
      last_error_ = "Unsupported clause type";
      return input;
  }
}

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanMatch(
    const MatchClause& match,
    std::unique_ptr<PhysicalOperator> input) {
  (void)input;  // MATCH is typically the leaf/source
  if (match.patterns.empty()) {
    last_error_ = "MATCH clause has no patterns";
    return nullptr;
  }
  return PlanPathPattern(match.patterns[0], nullptr);
}

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanWhere(
    const WhereClause& where,
    std::unique_ptr<PhysicalOperator> input) {
  if (!where.condition || !input) {
    return input;
  }
  auto filter = std::make_unique<Filter>(where.condition);
  filter->AddChild(std::shared_ptr<PhysicalOperator>(input.release()));
  return filter;
}

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanReturn(
    const ReturnClause& ret,
    std::unique_ptr<PhysicalOperator> input) {
  std::vector<std::string> columns;
  std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections;

  for (const auto& item : ret.items) {
    std::string col_name = item.alias.value_or("column");
    columns.push_back(col_name);
    projections.push_back({col_name, item.expression});
  }

  std::unique_ptr<PhysicalOperator> root;
  if (input) {
    auto project = std::make_unique<Project>(projections);
    project->AddChild(std::shared_ptr<PhysicalOperator>(input.release()));
    root = std::move(project);
  }

  if (ret.distinct && root) {
    std::vector<std::shared_ptr<Expression>> distinct_keys;
    for (const auto& item : ret.items) {
      distinct_keys.push_back(item.expression);
    }
    auto distinct = std::make_unique<Distinct>(distinct_keys);
    distinct->AddChild(std::shared_ptr<PhysicalOperator>(root.release()));
    root = std::move(distinct);
  }

  auto produce = std::make_unique<ProduceResults>(columns);
  if (root) {
    produce->AddChild(std::shared_ptr<PhysicalOperator>(root.release()));
  }
  return produce;
}

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanOrderBy(
    const OrderByClause& order_by,
    std::unique_ptr<PhysicalOperator> input) {
  if (order_by.items.empty() || !input) {
    return input;
  }
  std::vector<std::pair<std::shared_ptr<Expression>, bool>> sort_items;
  for (const auto& item : order_by.items) {
    sort_items.push_back({item.expression, item.ascending});
  }
  auto sort = std::make_unique<Sort>(sort_items);
  sort->AddChild(std::shared_ptr<PhysicalOperator>(input.release()));
  return sort;
}

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanLimit(
    const LimitClause& limit,
    std::unique_ptr<PhysicalOperator> input) {
  if (!limit.expression || !input) {
    return input;
  }
  int64_t limit_count = 0;
  if (auto lit = std::dynamic_pointer_cast<LiteralExpr>(limit.expression)) {
    if (lit->value.IsInt()) limit_count = lit->value.GetInt();
  }
  if (limit_count <= 0) {
    return input;
  }
  auto limit_op = std::make_unique<Limit>(static_cast<size_t>(limit_count));
  limit_op->AddChild(std::shared_ptr<PhysicalOperator>(input.release()));
  return limit_op;
}

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanSkip(
    const SkipClause& skip,
    std::unique_ptr<PhysicalOperator> input) {
  if (!skip.expression || !input) {
    return input;
  }
  int64_t skip_count = 0;
  if (auto lit = std::dynamic_pointer_cast<LiteralExpr>(skip.expression)) {
    if (lit->value.IsInt()) skip_count = lit->value.GetInt();
  }
  if (skip_count <= 0) {
    return input;
  }
  auto skip_op = std::make_unique<Skip>(static_cast<size_t>(skip_count));
  skip_op->AddChild(std::shared_ptr<PhysicalOperator>(input.release()));
  return skip_op;
}

// ============================================================================
// Pattern and scan planners
// ============================================================================

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanPathPattern(
    const PathPattern& pattern,
    std::unique_ptr<PhysicalOperator> input) {
  (void)input;
  if (pattern.elements.empty()) {
    last_error_ = "Empty path pattern";
    return nullptr;
  }

  std::unique_ptr<PhysicalOperator> current;

  // First element must be a node
  if (std::holds_alternative<NodePattern>(pattern.elements[0])) {
    const auto& node = std::get<NodePattern>(pattern.elements[0]);
    current = std::make_unique<NodeScan>(
        node.variable,
        node.labels.empty() ? std::nullopt : std::optional(node.labels[0]));
  } else {
    last_error_ = "Path must start with a node";
    return nullptr;
  }

  // Build expand chain
  size_t i = 1;
  while (i < pattern.elements.size()) {
    if (std::holds_alternative<RelationshipPattern>(pattern.elements[i])) {
      const auto& rel = std::get<RelationshipPattern>(pattern.elements[i]);
      if (i + 1 < pattern.elements.size() &&
          std::holds_alternative<NodePattern>(pattern.elements[i + 1])) {
        const auto& next_node = std::get<NodePattern>(pattern.elements[i + 1]);
        auto expand = std::make_unique<Expand>(
            std::get<NodePattern>(pattern.elements[i - 1]).variable,
            rel.variable,
            next_node.variable,
            rel.direction,
            rel.types.empty() ? std::nullopt : std::optional(rel.types[0]));
        expand->AddChild(
            std::shared_ptr<PhysicalOperator>(current.release()));
        current = std::move(expand);
        i += 2;
      } else {
        i++;
      }
    } else {
      i++;
    }
  }

  return current;
}

std::unique_ptr<PhysicalOperator> QueryPlanner::PlanNodeScan(
    const NodePattern& node) {
  return std::make_unique<NodeScan>(
      node.variable,
      node.labels.empty() ? std::nullopt : std::optional(node.labels[0]));
}

// ============================================================================
// Expression helpers
// ============================================================================

std::function<bool(const Record&)> QueryPlanner::CreateFilterPredicate(
    const Expression& expr) {
  return ExpressionEvaluator::BuildPredicate(&expr);
}

Value QueryPlanner::EvaluateExpression(
    const Expression& expr,
    const Record& record) {
  ExpressionEvaluator evaluator(nullptr);
  return evaluator.Evaluate(expr, record);
}

std::string QueryPlanner::ExpressionToString(const Expression& expr) {
  switch (expr.expr_type) {
    case ExprType::LITERAL: {
      auto& lit = static_cast<const LiteralExpr&>(expr);
      return lit.value.ToString();
    }
    case ExprType::VARIABLE: {
      auto& var = static_cast<const VariableExpr&>(expr);
      return var.name;
    }
    case ExprType::PROPERTY: {
      auto& prop = static_cast<const PropertyExpr&>(expr);
      return prop.variable + "." + prop.property;
    }
    case ExprType::COMPARISON: {
      auto& cmp = static_cast<const ComparisonExpr&>(expr);
      std::string op;
      switch (cmp.op) {
        case ComparisonExpr::EQ: op = "="; break;
        case ComparisonExpr::NE: op = "<>"; break;
        case ComparisonExpr::LT: op = "<"; break;
        case ComparisonExpr::GT: op = ">"; break;
        case ComparisonExpr::LE: op = "<="; break;
        case ComparisonExpr::GE: op = ">="; break;
        default:
          std::cerr << "[Planner] Unknown comparison op" << std::endl;
          op = "=";
          break;
      }
      return ExpressionToString(*cmp.left) + " " + op + " " +
             ExpressionToString(*cmp.right);
    }
    case ExprType::FUNCTION_CALL: {
      auto& fn = static_cast<const FunctionCallExpr&>(expr);
      std::string result = fn.name + "(";
      for (size_t i = 0; i < fn.arguments.size(); ++i) {
        if (i > 0) result += ", ";
        result += ExpressionToString(*fn.arguments[i]);
      }
      result += ")";
      return result;
    }
    default:
      return "<expression>";
  }
}

}  // namespace cedar::cypher
