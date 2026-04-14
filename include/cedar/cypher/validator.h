// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Cypher Query Validator - Semantic validation against schema

#ifndef CEDAR_CYPHER_VALIDATOR_H_
#define CEDAR_CYPHER_VALIDATOR_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/cypher/ast.h"
#include "cedar/cypher/value.h"
#include "cedar/queryd/meta_client.h"

namespace cedar::cypher {

class ValidationError {
 public:
  explicit ValidationError(std::string msg) : message_(std::move(msg)) {}
  const std::string& message() const { return message_; }
 private:
  std::string message_;
};

class QueryValidator {
 public:
  explicit QueryValidator(const queryd::GraphSchema* schema);

  // Validates a QueryStatement AST. Returns OK on success.
  cedar::Status Validate(const QueryStatement& stmt);

  // Returns the last validation error message.
  std::string GetLastError() const { return last_error_; }

 private:
  const queryd::GraphSchema* schema_;
  std::string last_error_;

  // Scoped variable bindings: variable_name -> inferred properties
  std::unordered_map<std::string, std::vector<std::string>> scope_;

  bool ValidateQueryStatement(const QueryStatement& stmt);
  bool ValidateMatchClause(const MatchClause& clause,
                           std::vector<std::string>* pushed_vars);
  bool ValidateWhereClause(const WhereClause& clause);
  bool ValidateReturnClause(const ReturnClause& clause);
  bool ValidateExpression(const Expression& expr);
  bool ValidateNodePattern(const NodePattern& node);
  bool ValidateRelationshipPattern(const RelationshipPattern& rel);
  bool ValidatePropertyAccess(const PropertyExpr& prop);
  bool ValidateLabel(const std::string& label, bool is_node);

  void PushScope(const std::string& var, const std::vector<std::string>& props);
  void PopScope(const std::string& var);

  std::optional<ValueType> InferExpressionType(const Expression& expr);
};

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_VALIDATOR_H_
