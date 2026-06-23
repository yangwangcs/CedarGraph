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
// Cypher Query Fingerprint Implementation
// =============================================================================

#include "cedar/cypher/fingerprint.h"

#include <algorithm>
#include <cctype>
#include <iostream>
#include <cstring>
#include <regex>
#include <sstream>

#include "cedar/cypher/ast.h"

namespace cedar {
namespace cypher {

// ============================================================================
// String-based fingerprint (fast path, no parser needed)
// ============================================================================
std::string ComputeFingerprint(const std::string& query) {
  std::string fp;
  fp.reserve(query.size());

  auto is_id_char = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
  };

  // Replaces string literals, numeric literals, boolean literals, and null with '?'
  size_t i = 0;
  while (i < query.size()) {
    char c = query[i];

    // Skip whitespace (will normalize later)
    if (std::isspace(static_cast<unsigned char>(c))) {
      fp.push_back(' ');
      ++i;
      continue;
    }

    // String literal: '...' or "..."
    if (c == '\'' || c == '"') {
      char quote = c;
      ++i;
      while (i < query.size() && query[i] != quote) {
        if (query[i] == '\\' && i + 1 < query.size()) i += 2;
        else ++i;
      }
      if (i < query.size()) ++i;  // skip closing quote
      fp.push_back('?');
      continue;
    }

    // Number literal (possibly negative, float, scientific)
    if ((c >= '0' && c <= '9') ||
        (c == '-' && i + 1 < query.size() &&
         query[i + 1] >= '0' && query[i + 1] <= '9' &&
         (i == 0 || !is_id_char(query[i - 1])))) {
      size_t start = i;
      if (query[i] == '-') ++i;
      while (i < query.size() && query[i] >= '0' && query[i] <= '9') ++i;
      if (i < query.size() && query[i] == '.') {
        ++i;
        while (i < query.size() && query[i] >= '0' && query[i] <= '9') ++i;
      }
      if (i < query.size() && (query[i] == 'e' || query[i] == 'E')) {
        size_t e_pos = i;
        ++i;
        if (i < query.size() && (query[i] == '+' || query[i] == '-')) ++i;
        bool has_digits = false;
        while (i < query.size() && query[i] >= '0' && query[i] <= '9') {
          has_digits = true;
          ++i;
        }
        if (!has_digits) i = e_pos;  // rollback, not scientific notation
      }
      // Only replace if not part of an identifier (e.g. "n42")
      if (i >= query.size() || !is_id_char(query[i])) {
        fp.push_back('?');
        continue;
      }
      // It was part of an identifier, copy as-is (lowercased)
      for (size_t k = start; k < i; ++k) {
        fp.push_back(static_cast<char>(
            std::tolower(static_cast<unsigned char>(query[k]))));
      }
      continue;
    }

    // Boolean / null literals as whole words
    auto check_word = [&](const char* word) -> bool {
      size_t len = std::strlen(word);
      if (query.size() - i < len) return false;
      for (size_t k = 0; k < len; ++k) {
        if (std::tolower(static_cast<unsigned char>(query[i + k])) != word[k])
          return false;
      }
      bool left_ok = (i == 0) || !is_id_char(query[i - 1]);
      bool right_ok = (i + len >= query.size()) || !is_id_char(query[i + len]);
      return left_ok && right_ok;
    };

    if (check_word("true") || check_word("false") || check_word("null")) {
      while (i < query.size() && is_id_char(query[i])) ++i;
      fp.push_back('?');
      continue;
    }

    // Default: copy character lowercased
    fp.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    ++i;
  }

  // Normalize whitespace: collapse multiple spaces to single space
  std::string normalized;
  normalized.reserve(fp.size());
  bool last_was_space = false;
  for (char ch : fp) {
    if (ch == ' ') {
      if (!last_was_space) {
        normalized.push_back(' ');
        last_was_space = true;
      }
    } else {
      normalized.push_back(ch);
      last_was_space = false;
    }
  }

  // Trim
  size_t start = normalized.find_first_not_of(' ');
  if (start == std::string::npos) return "";
  size_t end = normalized.find_last_not_of(' ');
  return normalized.substr(start, end - start + 1);
}

// ============================================================================
// AST-based fingerprint (exact, parser-level)
// ============================================================================
namespace {

class FingerprintWriter {
 public:
  explicit FingerprintWriter(const FingerprintOptions& options = {}) : options_(options) {}

  std::string Result() const { return oss_.str(); }

  void Write(const QueryStatement& stmt) {
    for (size_t i = 0; i < stmt.clauses.size(); ++i) {
      if (i > 0) oss_ << " ";
      WriteClause(*stmt.clauses[i]);
    }
  }

 private:
  FingerprintOptions options_;
  std::ostringstream oss_;

  void WriteLiteralValue(const LiteralExpr& literal) {
    oss_ << literal.value.ToString();
  }

  void WriteClause(const QueryClause& clause) {
    switch (clause.clause_type) {
      case ClauseType::MATCH:
        WriteMatch(static_cast<const MatchClause&>(clause));
        break;
      case ClauseType::WHERE:
        WriteWhere(static_cast<const WhereClause&>(clause));
        break;
      case ClauseType::RETURN:
        WriteReturn(static_cast<const ReturnClause&>(clause));
        break;
      case ClauseType::ORDER_BY:
        WriteOrderBy(static_cast<const OrderByClause&>(clause));
        break;
      case ClauseType::LIMIT:
        WriteLimit(static_cast<const LimitClause&>(clause));
        break;
      case ClauseType::SKIP:
        WriteSkip(static_cast<const SkipClause&>(clause));
        break;
      case ClauseType::CREATE:
        WriteCreate(static_cast<const CreateClause&>(clause));
        break;
      case ClauseType::SET:
        WriteSet(static_cast<const SetClause&>(clause));
        break;
      case ClauseType::DELETE:
        WriteDelete(static_cast<const DeleteClause&>(clause));
        break;
      default:
        std::cerr << "[Fingerprint] Unknown clause type" << std::endl;
        break;
    }
  }

  void WriteMatch(const MatchClause& match) {
    oss_ << "match ";
    for (size_t i = 0; i < match.patterns.size(); ++i) {
      if (i > 0) oss_ << ",";
      WritePattern(match.patterns[i]);
    }
  }

  void WritePattern(const PathPattern& pattern) {
    for (const auto& elem : pattern.elements) {
      std::visit([this](const auto& e) { WritePatternElement(e); }, elem);
    }
  }

  void WritePatternElement(const NodePattern& node) {
    oss_ << "(";
    if (!node.variable.empty()) oss_ << node.variable;
    for (const auto& label : node.labels) oss_ << ":" << label;
    if (!node.properties.empty()) {
      oss_ << "{";
      bool first = true;
      for (const auto& kv : node.properties) {
        if (!first) oss_ << ",";
        first = false;
        oss_ << kv.first << ":";
        if (options_.preserve_property_keys.count(kv.first) > 0 &&
            kv.second->expr_type == ExprType::LITERAL) {
          WriteLiteralValue(static_cast<const LiteralExpr&>(*kv.second));
        } else {
          WriteExpression(*kv.second);
        }
      }
      oss_ << "}";
    }
    oss_ << ")";
  }

  void WritePatternElement(const RelationshipPattern& rel) {
    if (rel.direction == Direction::OUTGOING) {
      oss_ << "-[";
    } else if (rel.direction == Direction::INCOMING) {
      oss_ << "<-[";
    } else {
      oss_ << "-[";
    }
    if (!rel.variable.empty()) oss_ << rel.variable;
    for (const auto& type : rel.types) oss_ << ":" << type;
    if (!rel.properties.empty()) {
      oss_ << "{";
      bool first = true;
      for (const auto& kv : rel.properties) {
        if (!first) oss_ << ",";
        first = false;
        oss_ << kv.first << ":";
        if (options_.preserve_property_keys.count(kv.first) > 0 &&
            kv.second->expr_type == ExprType::LITERAL) {
          WriteLiteralValue(static_cast<const LiteralExpr&>(*kv.second));
        } else {
          WriteExpression(*kv.second);
        }
      }
      oss_ << "}";
    }
    if (rel.min_hops.has_value() || rel.max_hops.has_value()) {
      oss_ << "*";
      if (rel.min_hops.has_value()) oss_ << rel.min_hops.value();
      oss_ << "..";
      if (rel.max_hops.has_value()) oss_ << rel.max_hops.value();
    }
    if (rel.direction == Direction::OUTGOING) {
      oss_ << "]->";
    } else if (rel.direction == Direction::INCOMING) {
      oss_ << "]-";
    } else {
      oss_ << "]-";
    }
  }

  void WriteWhere(const WhereClause& where) {
    oss_ << "where ";
    WriteExpression(*where.condition);
  }

  void WriteReturn(const ReturnClause& ret) {
    oss_ << "return ";
    if (ret.distinct) oss_ << "distinct ";
    if (ret.all) {
      oss_ << "*";
    } else {
      for (size_t i = 0; i < ret.items.size(); ++i) {
        if (i > 0) oss_ << ",";
        WriteExpression(*ret.items[i].expression);
        if (ret.items[i].alias.has_value()) oss_ << " as " << ret.items[i].alias.value();
      }
    }
  }

  void WriteOrderBy(const OrderByClause& ob) {
    oss_ << "order by ";
    for (size_t i = 0; i < ob.items.size(); ++i) {
      if (i > 0) oss_ << ",";
      WriteExpression(*ob.items[i].expression);
      oss_ << (ob.items[i].ascending ? " asc" : " desc");
    }
  }

  void WriteLimit(const LimitClause& limit) {
    oss_ << "limit ";
    WriteExpression(*limit.expression);
  }

  void WriteSkip(const SkipClause& skip) {
    oss_ << "skip ";
    WriteExpression(*skip.expression);
  }

  void WriteCreate(const CreateClause& create) {
    oss_ << "create ";
    for (size_t i = 0; i < create.patterns.size(); ++i) {
      if (i > 0) oss_ << ",";
      WritePattern(create.patterns[i]);
    }
  }

  void WriteSet(const SetClause& set) {
    oss_ << "set ";
    for (size_t i = 0; i < set.items.size(); ++i) {
      if (i > 0) oss_ << ",";
      WriteExpression(*set.items[i].target);
      oss_ << "=";
      WriteExpression(*set.items[i].value);
    }
  }

  void WriteDelete(const DeleteClause& del) {
    if (del.detach) oss_ << "detach ";
    oss_ << "delete ";
    for (size_t i = 0; i < del.expressions.size(); ++i) {
      if (i > 0) oss_ << ",";
      WriteExpression(*del.expressions[i]);
    }
  }

  void WriteExpression(const Expression& expr) {
    switch (expr.expr_type) {
      case ExprType::LITERAL:
      case ExprType::PARAMETER:
        oss_ << "?";
        break;
      case ExprType::VARIABLE:
        oss_ << static_cast<const VariableExpr&>(expr).name;
        break;
      case ExprType::PROPERTY: {
        auto& p = static_cast<const PropertyExpr&>(expr);
        oss_ << p.variable << "." << p.property;
        break;
      }
      case ExprType::COMPARISON: {
        auto& cmp = static_cast<const ComparisonExpr&>(expr);
        WriteExpression(*cmp.left);
        oss_ << ComparisonOp(cmp.op);
        WriteExpression(*cmp.right);
        break;
      }
      case ExprType::AND:
      case ExprType::OR: {
        auto& logical = static_cast<const LogicalExpr&>(expr);
        WriteExpression(*logical.left);
        oss_ << (logical.op == LogicalExpr::AND ? " and " : " or ");
        WriteExpression(*logical.right);
        break;
      }
      case ExprType::NOT: {
        auto& n = static_cast<const NotExpr&>(expr);
        oss_ << "not ";
        WriteExpression(*n.operand);
        break;
      }
      case ExprType::ARITHMETIC: {
        auto& arith = static_cast<const ArithmeticExpr&>(expr);
        WriteExpression(*arith.left);
        oss_ << ArithmeticOp(arith.op);
        WriteExpression(*arith.right);
        break;
      }
      case ExprType::FUNCTION_CALL: {
        auto& fc = static_cast<const FunctionCallExpr&>(expr);
        oss_ << fc.name << "(";
        for (size_t i = 0; i < fc.arguments.size(); ++i) {
          if (i > 0) oss_ << ",";
          WriteExpression(*fc.arguments[i]);
        }
        oss_ << ")";
        break;
      }
      case ExprType::LIST_LITERAL: {
        auto& list = static_cast<const ListLiteralExpr&>(expr);
        oss_ << "[";
        for (size_t i = 0; i < list.elements.size(); ++i) {
          if (i > 0) oss_ << ",";
          WriteExpression(*list.elements[i]);
        }
        oss_ << "]";
        break;
      }
      case ExprType::MAP_LITERAL: {
        auto& map = static_cast<const MapLiteralExpr&>(expr);
        oss_ << "{";
        bool first = true;
        for (const auto& kv : map.entries) {
          if (!first) oss_ << ",";
          first = false;
          oss_ << kv.first << ":";
          WriteExpression(*kv.second);
        }
        oss_ << "}";
        break;
      }
      default:
        std::cerr << "[Fingerprint] Unknown expression type" << std::endl;
        break;
    }
  }

  static const char* ComparisonOp(ComparisonExpr::Op op) {
    switch (op) {
      case ComparisonExpr::EQ: return "=";
      case ComparisonExpr::NE: return "<>";
      case ComparisonExpr::LT: return "<";
      case ComparisonExpr::GT: return ">";
      case ComparisonExpr::LE: return "<=";
      case ComparisonExpr::GE: return ">=";
      default:
        std::cerr << "[Fingerprint] Unknown comparison op" << std::endl;
        return "=";
    }
    return "=";
  }

  static const char* ArithmeticOp(ArithmeticExpr::Op op) {
    switch (op) {
      case ArithmeticExpr::ADD: return "+";
      case ArithmeticExpr::SUB: return "-";
      case ArithmeticExpr::MUL: return "*";
      case ArithmeticExpr::DIV: return "/";
      case ArithmeticExpr::MOD: return "%";
      default:
        std::cerr << "[Fingerprint] Unknown arithmetic op" << std::endl;
        return "+";
    }
    return "+";
  }
};

}  // namespace

std::string ComputeFingerprint(const QueryStatement& ast,
                               const FingerprintOptions& options) {
  FingerprintWriter writer(options);
  writer.Write(ast);
  std::string fp = writer.Result();
  std::regex ws(R"(\s+)");
  fp = std::regex_replace(fp, ws, " ");
  size_t start = fp.find_first_not_of(" \t\n\r");
  if (start == std::string::npos) return "";
  size_t end = fp.find_last_not_of(" \t\n\r");
  return fp.substr(start, end - start + 1);
}

std::string ComputeFingerprint(const QueryStatement& ast) {
  return ComputeFingerprint(ast, FingerprintOptions{});
}

}  // namespace cypher
}  // namespace cedar
