// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/cypher/parser.h"
#include <cctype>
#include <algorithm>
#include <limits>

namespace cedar {
namespace cypher {

CypherParser::CypherParser(const std::string& input) 
    : input_(input), pos_(0) {}

std::shared_ptr<QueryStatement> CypherParser::ParseStatement() {
  SkipWhitespaceAndComments();
  
  // Parse temporal clause first
  temporal_clause_ = ParseTemporalClause();
  
  SkipWhitespaceAndComments();
  
  auto stmt = std::make_shared<QueryStatement>();
  
  // Parse main query clauses
  while (!IsAtEnd()) {
    SkipWhitespaceAndComments();
    
    if (MatchKeyword("match")) {
      auto match = ParseMatchClause();
      if (match) {
        stmt->clauses.push_back(match);
      }
    } else if (MatchKeyword("where")) {
      auto where = ParseWhereClause();
      if (where) {
        stmt->clauses.push_back(where);
      }
    } else if (MatchKeyword("return")) {
      auto ret = ParseReturnClause();
      if (ret) {
        stmt->clauses.push_back(ret);
      }
    } else if (MatchKeyword("order")) {
      if (!MatchKeyword("by")) {
        error_ = "Expected BY after ORDER";
        break;
      }
      auto order = ParseOrderByClause();
      if (order) {
        stmt->clauses.push_back(order);
      }
    } else if (MatchKeyword("limit")) {
      auto limit_expr = ParseExpression();
      if (limit_expr) {
        stmt->clauses.emplace_back(std::make_shared<LimitClause>(limit_expr));
      }
    } else if (MatchKeyword("skip")) {
      auto skip_expr = ParseExpression();
      if (skip_expr) {
        stmt->clauses.emplace_back(std::make_shared<SkipClause>(skip_expr));
      }
    } else if (MatchKeyword("create")) {
      auto create = ParseCreateClause();
      if (create) {
        stmt->clauses.push_back(create);
      }
    } else if (MatchKeyword("set")) {
      auto set_clause = ParseSetClause();
      if (set_clause) {
        stmt->clauses.push_back(set_clause);
      }
    } else if (MatchKeyword("delete")) {
      auto del = ParseDeleteClause();
      if (del) {
        stmt->clauses.push_back(del);
      }
    } else {
      // Unknown clause — report error instead of silent truncation
      error_ = "Unexpected token at position " + std::to_string(pos_);
      break;
    }
    
    SkipWhitespaceAndComments();
  }
  
  // Store temporal modifier in statement
  if (temporal_clause_) {
    stmt->temporal_modifier.type = temporal_clause_->modifier;
    stmt->temporal_modifier.timestamp = nullptr;
    // Full expression conversion requires integrating the expression parser
    // with temporal_clause_->expression.
    stmt->temporal_modifier.version_number = temporal_clause_->version_number.value_or(0);
  }
  
  return stmt;
}

bool CypherParser::IsValid() {
  size_t save_pos = pos_;
  auto stmt = ParseStatement();
  pos_ = save_pos;
  return stmt != nullptr && error_.empty();
}

// ============================================================================
// Token handling
// ============================================================================

void CypherParser::SkipWhitespace() {
  while (!IsAtEnd() && std::isspace(Peek())) {
    Advance();
  }
}

void CypherParser::SkipWhitespaceAndComments() {
  SkipWhitespace();
  // Handle single-line comments (//...)
  while (Peek() == '/' && Lookahead(1) == '/') {
    while (!IsAtEnd() && Peek() != '\n') {
      Advance();
    }
    SkipWhitespace();
  }
}

bool CypherParser::MatchKeyword(const std::string& keyword) {
  SkipWhitespaceAndComments();
  
  size_t save_pos = pos_;
  std::string word;
  
  while (!IsAtEnd() && std::isalpha(Peek())) {
    word += std::tolower(Advance());
  }
  
  if (word == keyword) {
    return true;
  }
  
  pos_ = save_pos;
  return false;
}

bool CypherParser::ExpectKeyword(const std::string& keyword) {
  if (!MatchKeyword(keyword)) {
    error_ = "Expected keyword: " + keyword;
    return false;
  }
  return true;
}

bool CypherParser::MatchSymbol(char symbol) {
  SkipWhitespaceAndComments();
  if (!IsAtEnd() && Peek() == symbol) {
    Advance();
    return true;
  }
  return false;
}

bool CypherParser::ExpectSymbol(char symbol) {
  if (!MatchSymbol(symbol)) {
    error_ = std::string("Expected symbol: ") + symbol;
    return false;
  }
  return true;
}

std::string CypherParser::ParseIdentifier() {
  SkipWhitespaceAndComments();
  
  std::string ident;
  if (!IsAtEnd() && (std::isalpha(Peek()) || Peek() == '_')) {
    ident += Advance();
    while (!IsAtEnd() && (std::isalnum(Peek()) || Peek() == '_')) {
      ident += Advance();
    }
  }
  
  return ident;
}

std::string CypherParser::ParseStringLiteral() {
  SkipWhitespaceAndComments();
  
  char quote = Peek();
  if (quote != '\'' && quote != '\"') {
    return "";
  }
  Advance();  // Consume opening quote
  
  std::string value;
  // Heuristic reserve: remaining length up to next quote
  size_t remaining = input_.size() - pos_;
  value.reserve(remaining);
  while (!IsAtEnd() && Peek() != quote) {
    if (Peek() == '\\') {
      Advance();  // Skip escape char
      if (!IsAtEnd()) {
        char escaped = Advance();
        switch (escaped) {
          case 'n': value += '\n'; break;
          case 't': value += '\t'; break;
          case 'r': value += '\r'; break;
          case '\\': value += '\\'; break;
          case '\'': value += '\''; break;
          case '\"': value += '\"'; break;
          default: value += escaped; break;
        }
      }
    } else {
      value += Advance();
    }
  }
  
  if (!IsAtEnd() && Peek() == quote) {
    Advance();  // Consume closing quote
  } else {
    error_ = "Unclosed string literal";
  }

  return value;
}

int64_t CypherParser::ParseInteger() {
  SkipWhitespaceAndComments();
  
  int64_t value = 0;
  bool negative = false;
  
  if (!IsAtEnd() && Peek() == '-') {
    negative = true;
    Advance();
  }
  
  while (!IsAtEnd() && std::isdigit(Peek())) {
    int digit = Advance() - '0';
    if (value > (INT64_MAX - digit) / 10) {
      error_ = "Integer literal overflows int64_t";
      return negative ? INT64_MIN : INT64_MAX;
    }
    value = value * 10 + digit;
  }
  
  return negative ? -value : value;
}

char CypherParser::Peek() const {
  if (pos_ < input_.size()) {
    return input_[pos_];
  }
  return '\0';
}

char CypherParser::Advance() {
  if (pos_ < input_.size()) {
    return input_[pos_++];
  }
  return '\0';
}

char CypherParser::Lookahead(size_t offset) const {
  if (pos_ + offset < input_.size()) {
    return input_[pos_ + offset];
  }
  return '\0';
}

bool CypherParser::IsAtEnd() const {
  return pos_ >= input_.size();
}

// ============================================================================
// Clause parsers
// ============================================================================

std::shared_ptr<MatchClause> CypherParser::ParseMatchClause() {
  SkipWhitespaceAndComments();
  
  auto match = std::make_shared<MatchClause>();
  
  // Parse patterns
  while (!IsAtEnd()) {
    auto pattern = ParsePattern();
    if (!pattern.elements.empty()) {
      match->patterns.push_back(pattern);
    }
    
    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  
  return match;
}

std::shared_ptr<CreateClause> CypherParser::ParseCreateClause() {
  SkipWhitespaceAndComments();
  
  auto create = std::make_shared<CreateClause>();
  
  // Parse patterns
  while (!IsAtEnd()) {
    auto pattern = ParsePattern();
    if (!pattern.elements.empty()) {
      create->patterns.push_back(pattern);
    }
    
    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  
  return create;
}

std::shared_ptr<ReturnClause> CypherParser::ParseReturnClause() {
  SkipWhitespaceAndComments();
  
  auto ret = std::make_shared<ReturnClause>();
  
  // Check for DISTINCT
  if (MatchKeyword("distinct")) {
    ret->distinct = true;
  }
  
  // Parse return items
  while (!IsAtEnd()) {
    ReturnItem item;
    
    item.expression = ParseExpression();
    if (!item.expression) {
      error_ = "Failed to parse RETURN expression";
      break;
    }
    
    // Check for AS alias
    SkipWhitespaceAndComments();
    if (MatchKeyword("as")) {
      item.alias = ParseIdentifier();
    }
    
    // Derive alias from expression if not explicitly set
    if (!item.alias.has_value()) {
      if (auto var = std::dynamic_pointer_cast<VariableExpr>(item.expression)) {
        item.alias = var->name;
      } else if (auto prop = std::dynamic_pointer_cast<PropertyExpr>(item.expression)) {
        item.alias = prop->property;
      }
    }
    
    ret->items.push_back(item);
    
    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  
  return ret;
}

std::shared_ptr<WhereClause> CypherParser::ParseWhereClause() {
  SkipWhitespaceAndComments();
  
  auto where = std::make_shared<WhereClause>();
  where->condition = ParseExpression();
  if (!where->condition) {
    error_ = "Failed to parse WHERE condition";
  }
  
  return where;
}

// ============================================================================
// Pattern parsers
// ============================================================================

PathPattern CypherParser::ParsePattern() {
  SkipWhitespaceAndComments();
  
  PathPattern pattern;
  
  // Parse alternating nodes and relationships
  while (!IsAtEnd()) {
    // Expect node pattern
    if (MatchSymbol('(')) {
      NodePattern node;
      
      // Variable
      SkipWhitespaceAndComments();
      std::string var = ParseIdentifier();
      if (!var.empty()) {
        node.variable = var;
      }
      
      // Labels
      SkipWhitespaceAndComments();
      while (MatchSymbol(':')) {
        std::string label = ParseIdentifier();
        if (!label.empty()) {
          node.labels.push_back(label);
        }
      }
      
      // Properties
      SkipWhitespaceAndComments();
      if (MatchSymbol('{')) {
        while (!IsAtEnd() && Peek() != '}') {
          SkipWhitespaceAndComments();
          std::string prop_name = ParseIdentifier();
          if (prop_name.empty()) {
            error_ = "Expected property name in {";
            break;
          }
          SkipWhitespaceAndComments();
          if (!MatchSymbol(':')) {
            error_ = "Expected ':' after property name";
            break;
          }
          auto expr = ParseExpression();
          if (!expr) {
            error_ = "Expected expression after ':' in property";
            break;
          }
          node.properties[prop_name] = std::move(expr);
          SkipWhitespaceAndComments();
          if (Peek() == ',') {
            Advance();
          } else {
            break;
          }
        }
        ExpectSymbol('}');
      }
      
      ExpectSymbol(')');
      pattern.elements.push_back(node);
    }
    
    SkipWhitespaceAndComments();
    
    // Optional relationship pattern
    if (Peek() == '-' || Peek() == '<') {
      RelationshipPattern rel;
      
      if (MatchSymbol('<')) {
        rel.direction = Direction::INCOMING;
        ExpectSymbol('-');
      } else {
        rel.direction = Direction::OUTGOING;
        ExpectSymbol('-');
      }
      
      // Relationship details in brackets
      if (MatchSymbol('[')) {
        SkipWhitespaceAndComments();
        std::string rel_var = ParseIdentifier();
        if (!rel_var.empty()) {
          rel.variable = rel_var;
        }
        
        // Type
        SkipWhitespaceAndComments();
        if (MatchSymbol(':')) {
          std::string type;
          if (!IsAtEnd() && std::isdigit(Peek())) {
            while (!IsAtEnd() && std::isdigit(Peek())) {
              type += Advance();
            }
          } else {
            type = ParseIdentifier();
          }
          if (!type.empty()) {
            rel.types.push_back(type);
          }
        }
        
        SkipWhitespaceAndComments();
        // Optional hop range: *1..3
        if (MatchSymbol('*')) {
          SkipWhitespaceAndComments();
          // Parse optional min hops
          if (!IsAtEnd() && std::isdigit(Peek())) {
            uint64_t min_hops = 0;
            while (!IsAtEnd() && std::isdigit(Peek())) {
              min_hops = min_hops * 10 + (Advance() - '0');
            }
            rel.min_hops = min_hops;
          }
          SkipWhitespaceAndComments();
          if (MatchSymbol('.') && MatchSymbol('.')) {
            SkipWhitespaceAndComments();
            if (!IsAtEnd() && std::isdigit(Peek())) {
              uint64_t max_hops = 0;
              while (!IsAtEnd() && std::isdigit(Peek())) {
                max_hops = max_hops * 10 + (Advance() - '0');
              }
              rel.max_hops = max_hops;
            }
          }
        }

        SkipWhitespaceAndComments();
        ExpectSymbol(']');
      }

      // Arrow end
      if (rel.direction == Direction::OUTGOING) {
        if (MatchSymbol('-')) {
          if (MatchSymbol('>')) {
            // Already outgoing
          }
        }
      } else {
        ExpectSymbol('-');
      }
      
      pattern.elements.push_back(rel);
    } else {
      break;
    }
    
    SkipWhitespaceAndComments();
  }
  
  return pattern;
}

// ============================================================================
// ORDER BY, SET, DELETE clause parsers
// ============================================================================

std::shared_ptr<OrderByClause> CypherParser::ParseOrderByClause() {
  SkipWhitespaceAndComments();
  auto order = std::make_shared<OrderByClause>();
  
  while (!IsAtEnd()) {
    SortItem item;
    item.expression = ParseExpression();
    if (!item.expression) {
      error_ = "Failed to parse ORDER BY expression";
      break;
    }
    
    SkipWhitespaceAndComments();
    if (MatchKeyword("asc")) {
      item.ascending = true;
    } else if (MatchKeyword("desc")) {
      item.ascending = false;
    }
    
    order->items.push_back(item);
    
    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  
  return order;
}

std::shared_ptr<SetClause> CypherParser::ParseSetClause() {
  auto clause = std::make_shared<SetClause>();
  do {
    SkipWhitespaceAndComments();
    auto target = ParsePrimaryExpression();
    if (!target) {
      error_ = "Expected target expression in SET clause";
      return nullptr;
    }
    if (target->expr_type != ExprType::VARIABLE &&
        target->expr_type != ExprType::PROPERTY) {
      error_ = "SET target must be a variable or property access";
      return nullptr;
    }
    SkipWhitespaceAndComments();
    if (!ExpectSymbol('=')) {
      return nullptr;
    }
    auto value = ParseExpression();
    if (!value) {
      error_ = "Expected expression after = in SET clause";
      return nullptr;
    }
    SetClause::SetItem item;
    item.target = target;
    item.value = value;
    clause->items.push_back(item);
    SkipWhitespaceAndComments();
  } while (MatchSymbol(','));
  return clause;
}

std::shared_ptr<DeleteClause> CypherParser::ParseDeleteClause() {
  auto clause = std::make_shared<DeleteClause>();
  do {
    SkipWhitespaceAndComments();
    std::string ident = ParseIdentifier();
    if (ident.empty()) {
      error_ = "Expected variable name in DELETE clause";
      return nullptr;
    }
    clause->expressions.emplace_back(std::make_shared<VariableExpr>(ident));
    SkipWhitespaceAndComments();
  } while (MatchSymbol(','));
  return clause;
}

// ============================================================================
// Expression parsers (recursive descent with precedence climbing)
// ============================================================================

std::shared_ptr<Expression> CypherParser::ParseExpression() {
  return ParseOrExpression();
}

std::shared_ptr<Expression> CypherParser::ParseOrExpression() {
  auto left = ParseAndExpression();
  while (left && MatchKeyword("or")) {
    auto right = ParseAndExpression();
    if (!right) {
      error_ = "Expected expression after OR";
      return nullptr;
    }
    left = std::make_shared<LogicalExpr>(LogicalExpr::Op::OR, left, right);
  }
  return left;
}

std::shared_ptr<Expression> CypherParser::ParseAndExpression() {
  auto left = ParseComparisonExpression();
  while (left && MatchKeyword("and")) {
    auto right = ParseComparisonExpression();
    if (!right) {
      error_ = "Expected expression after AND";
      return nullptr;
    }
    left = std::make_shared<LogicalExpr>(LogicalExpr::Op::AND, left, right);
  }
  return left;
}

std::shared_ptr<Expression> CypherParser::ParseComparisonExpression() {
  auto left = ParseAdditiveExpression();
  if (!left) return nullptr;
  
  SkipWhitespaceAndComments();
  
  // = (equality)
  if (MatchSymbol('=')) {
    auto right = ParseAdditiveExpression();
    if (!right) {
      error_ = "Expected expression after =";
      return nullptr;
    }
    return std::make_shared<ComparisonExpr>(ComparisonExpr::EQ, left, right);
  }
  
  // <> (inequality) or <= (less than or equal)
  if (Peek() == '<') {
    Advance();
    if (Peek() == '>') {
      Advance();
      auto right = ParseAdditiveExpression();
      if (!right) {
        error_ = "Expected expression after <>";
        return nullptr;
      }
      return std::make_shared<ComparisonExpr>(ComparisonExpr::NE, left, right);
    }
    if (Peek() == '=') {
      Advance();
      auto right = ParseAdditiveExpression();
      if (!right) {
        error_ = "Expected expression after <=";
        return nullptr;
      }
      return std::make_shared<ComparisonExpr>(ComparisonExpr::LE, left, right);
    }
    auto right = ParseAdditiveExpression();
    if (!right) {
      error_ = "Expected expression after <";
      return nullptr;
    }
    return std::make_shared<ComparisonExpr>(ComparisonExpr::LT, left, right);
  }
  
  // >= (greater than or equal) or > (greater than)
  if (Peek() == '>') {
    Advance();
    if (Peek() == '=') {
      Advance();
      auto right = ParseAdditiveExpression();
      if (!right) {
        error_ = "Expected expression after >=";
        return nullptr;
      }
      return std::make_shared<ComparisonExpr>(ComparisonExpr::GE, left, right);
    }
    auto right = ParseAdditiveExpression();
    if (!right) {
      error_ = "Expected expression after >";
      return nullptr;
    }
    return std::make_shared<ComparisonExpr>(ComparisonExpr::GT, left, right);
  }
  
  // STARTS WITH
  if (MatchKeyword("starts")) {
    if (!MatchKeyword("with")) {
      error_ = "Expected WITH after STARTS";
      return nullptr;
    }
    auto right = ParseAdditiveExpression();
    if (!right) {
      error_ = "Expected expression after STARTS WITH";
      return nullptr;
    }
    return std::make_shared<FunctionCallExpr>("STARTS_WITH",
        std::vector<std::shared_ptr<Expression>>{left, right});
  }
  
  // ENDS WITH
  if (MatchKeyword("ends")) {
    if (!MatchKeyword("with")) {
      error_ = "Expected WITH after ENDS";
      return nullptr;
    }
    auto right = ParseAdditiveExpression();
    if (!right) {
      error_ = "Expected expression after ENDS WITH";
      return nullptr;
    }
    return std::make_shared<FunctionCallExpr>("ENDS_WITH",
        std::vector<std::shared_ptr<Expression>>{left, right});
  }
  
  // CONTAINS
  if (MatchKeyword("contains")) {
    auto right = ParseAdditiveExpression();
    if (!right) {
      error_ = "Expected expression after CONTAINS";
      return nullptr;
    }
    return std::make_shared<FunctionCallExpr>("CONTAINS",
        std::vector<std::shared_ptr<Expression>>{left, right});
  }
  
  // IS NULL / IS NOT NULL
  if (MatchKeyword("is")) {
    bool is_not = MatchKeyword("not");
    if (!MatchKeyword("null")) {
      error_ = "Expected NULL after IS";
      return nullptr;
    }
    auto null_lit = std::make_shared<LiteralExpr>(Value());
    if (is_not) {
      return std::make_shared<ComparisonExpr>(ComparisonExpr::NE, left, null_lit);
    }
    return std::make_shared<ComparisonExpr>(ComparisonExpr::EQ, left, null_lit);
  }
  
  return left;
}

std::shared_ptr<Expression> CypherParser::ParseAdditiveExpression() {
  auto left = ParseMultiplicativeExpression();
  while (left) {
    SkipWhitespaceAndComments();
    if (MatchSymbol('+')) {
      auto right = ParseMultiplicativeExpression();
      if (!right) {
        error_ = "Expected expression after +";
        return nullptr;
      }
      left = std::make_shared<ArithmeticExpr>(ArithmeticExpr::ADD, left, right);
    } else if (MatchSymbol('-')) {
      auto right = ParseMultiplicativeExpression();
      if (!right) {
        error_ = "Expected expression after -";
        return nullptr;
      }
      left = std::make_shared<ArithmeticExpr>(ArithmeticExpr::SUB, left, right);
    } else {
      break;
    }
  }
  return left;
}

std::shared_ptr<Expression> CypherParser::ParseMultiplicativeExpression() {
  auto left = ParseUnaryExpression();
  while (left) {
    SkipWhitespaceAndComments();
    if (MatchSymbol('*')) {
      auto right = ParseUnaryExpression();
      if (!right) {
        error_ = "Expected expression after *";
        return nullptr;
      }
      left = std::make_shared<ArithmeticExpr>(ArithmeticExpr::MUL, left, right);
    } else if (MatchSymbol('/')) {
      auto right = ParseUnaryExpression();
      if (!right) {
        error_ = "Expected expression after /";
        return nullptr;
      }
      left = std::make_shared<ArithmeticExpr>(ArithmeticExpr::DIV, left, right);
    } else if (MatchSymbol('%')) {
      auto right = ParseUnaryExpression();
      if (!right) {
        error_ = "Expected expression after %";
        return nullptr;
      }
      left = std::make_shared<ArithmeticExpr>(ArithmeticExpr::MOD, left, right);
    } else {
      break;
    }
  }
  return left;
}

std::shared_ptr<Expression> CypherParser::ParseUnaryExpression() {
  SkipWhitespaceAndComments();
  
  if (MatchKeyword("not")) {
    auto operand = ParseUnaryExpression();
    if (!operand) {
      error_ = "Expected expression after NOT";
      return nullptr;
    }
    return std::make_shared<NotExpr>(operand);
  }
  
  if (MatchSymbol('-')) {
    auto operand = ParseUnaryExpression();
    if (!operand) {
      error_ = "Expected expression after -";
      return nullptr;
    }
    auto zero = std::make_shared<LiteralExpr>(Value(static_cast<int64_t>(0)));
    return std::make_shared<ArithmeticExpr>(ArithmeticExpr::SUB, zero, operand);
  }
  
  if (MatchSymbol('+')) {
    // Unary plus is a no-op
    return ParseUnaryExpression();
  }
  
  return ParsePrimaryExpression();
}

std::shared_ptr<Expression> CypherParser::ParsePrimaryExpression() {
  SkipWhitespaceAndComments();
  
  // Parenthesized expression
  if (MatchSymbol('(')) {
    auto expr = ParseExpression();
    if (!expr) return nullptr;
    if (!ExpectSymbol(')')) return nullptr;
    return expr;
  }
  
  // Parameter ($name)
  if (MatchSymbol('$')) {
    std::string name = ParseIdentifier();
    if (name.empty()) {
      error_ = "Expected parameter name after $";
      return nullptr;
    }
    return std::make_shared<ParameterExpr>(name);
  }
  
  // String literal
  char c = Peek();
  if (c == '\'' || c == '"') {
    std::string str = ParseStringLiteral();
    return std::make_shared<LiteralExpr>(Value(str));
  }
  
  // Number literal (integer or float)
  if (std::isdigit(c) ||
      (c == '-' && pos_ + 1 < input_.size() && std::isdigit(input_[pos_ + 1]))) {
    bool negative = false;
    if (c == '-') {
      negative = true;
      Advance();
    }
    int64_t int_val = 0;
    bool overflow = false;
    while (!IsAtEnd() && std::isdigit(Peek())) {
      int64_t digit = Advance() - '0';
      if (int_val > (std::numeric_limits<int64_t>::max() - digit) / 10) {
        overflow = true;
        break;
      }
      int_val = int_val * 10 + digit;
    }
    if (overflow) {
      error_ = "Integer literal overflow";
      return std::make_shared<LiteralExpr>(Value::Null());
    }
    if (!IsAtEnd() && Peek() == '.') {
      Advance();  // consume '.'
      double frac = 0.0;
      double div = 1.0;
      while (!IsAtEnd() && std::isdigit(Peek())) {
        frac = frac * 10 + (Advance() - '0');
        div *= 10;
      }
      double float_val = static_cast<double>(int_val) + frac / div;
      if (negative) float_val = -float_val;
      return std::make_shared<LiteralExpr>(Value(float_val));
    }
    if (negative) int_val = -int_val;
    return std::make_shared<LiteralExpr>(Value(int_val));
  }
  
  // Boolean and NULL literals (check before generic identifier)
  size_t save_pos = pos_;
  std::string word;
  while (!IsAtEnd() && std::isalpha(Peek())) {
    word += std::tolower(Advance());
  }
  
  if (word == "true") {
    return std::make_shared<LiteralExpr>(Value(true));
  }
  if (word == "false") {
    return std::make_shared<LiteralExpr>(Value(false));
  }
  if (word == "null") {
    return std::make_shared<LiteralExpr>(Value());
  }
  
  // Restore position if not a special literal
  if (!word.empty()) {
    pos_ = save_pos;
  }
  
  // Identifier (variable, property access, function call, list subscript)
  std::string ident = ParseIdentifier();
  if (!ident.empty()) {
    std::shared_ptr<Expression> expr = std::make_shared<VariableExpr>(ident);
    
    // Handle postfix operators: .property and [index]
    while (true) {
      SkipWhitespaceAndComments();
      if (MatchSymbol('.')) {
        std::string prop = ParseIdentifier();
        if (prop.empty()) {
          error_ = "Expected property name after .";
          return nullptr;
        }
        if (auto var = std::dynamic_pointer_cast<VariableExpr>(expr)) {
          expr = std::make_shared<PropertyExpr>(var->name, prop);
        } else if (auto prop_expr = std::dynamic_pointer_cast<PropertyExpr>(expr)) {
          // Chained properties: use the original variable
          expr = std::make_shared<PropertyExpr>(prop_expr->variable, prop);
        } else {
          // For complex expressions, fall back to original identifier
          expr = std::make_shared<PropertyExpr>(ident, prop);
        }
      } else if (MatchSymbol('[')) {
        auto index_expr = ParseExpression();
        if (!index_expr) {
          error_ = "Expected expression after [";
          return nullptr;
        }
        if (!ExpectSymbol(']')) return nullptr;
        expr = std::make_shared<FunctionCallExpr>("element_at",
            std::vector<std::shared_ptr<Expression>>{expr, index_expr});
      } else {
        break;
      }
    }
    
    // Check for function call: identifier(...)
    SkipWhitespaceAndComments();
    if (MatchSymbol('(')) {
      std::vector<std::shared_ptr<Expression>> args;
      if (!MatchSymbol(')')) {
        while (true) {
          auto arg = ParseExpression();
          if (!arg) {
            error_ = "Expected argument in function call";
            return nullptr;
          }
          args.push_back(arg);
          if (MatchSymbol(')')) break;
          if (!ExpectSymbol(',')) return nullptr;
        }
      }
      return std::make_shared<FunctionCallExpr>(ident, args);
    }
    
    return expr;
  }
  
  error_ = "Unexpected token in expression";
  return nullptr;
}

}  // namespace cypher
}  // namespace cedar
