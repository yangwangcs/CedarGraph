# Sub-Plan F: Cypher MERGE, WITH, UNWIND Clauses

| | |
|---|---|
| **Goal** | Add `MERGE`, `WITH`, and `UNWIND` clause support to CedarGraph-Core's Cypher engine: parse → AST → physical operator → execution. |
| **Architecture** | Hand-written recursive-descent parser (`CypherParser`) → AST (`ast.h`) → `ExecutionPlanBuilder` → physical operators (`execution_plan.h` / `write_operators.cc`) → `ExecutionContext`. |
| **Tech Stack** | C++17, CMake, gtest, CedarGraph storage (`CedarGraphStorage`). |
| **Target branch** | `main` |
| **Saved to** | `/Users/wangyang/Desktop/CedarGraph-Core/docs/superpowers/plans/2026-06-11-subplan-f-cypher-clauses.md` |

---

## File Map

| # | File | Role | Lines of interest |
|---|------|------|-------------------|
| 1 | `include/cedar/cypher/ast.h` | AST definitions: `ClauseType`, clause structs | 178–188 (enum), 190–258 (clauses) |
| 2 | `include/cedar/cypher/parser.h` | Parser declarations | 111–118 (clause parsers) |
| 3 | `include/cedar/cypher/execution_plan.h` | Physical operator classes + `ExecutionPlanBuilder` | 606–736 (write ops + builder) |
| 4 | `src/cypher/parser.cc` | Parser implementation | 24–84 (main clause dispatch) |
| 5 | `src/cypher/execution_plan.cc` | Plan builder + standard operators | 993–1181 (`Build`), 1218–1337 (`Build*Plan`) |
| 6 | `src/cypher/operators/write_operators.cc` | Write operators (`CreateOperator`, `SetOperator`, `DeleteOperator`) | 1–479 |
| 7 | `src/cypher/validator.cc` | Query validator | 1–236 |
| 8 | `tests/cypher/CMakeLists.txt` | Cypher test targets | all |
| 9 | `tests/CMakeLists.txt` | Top-level test registration | 39–52 (cypher tests) |
| 10 | `CMakeLists.txt` | `cedar_cypher` library source list | 414–426 (`CEDAR_CYPHER_SOURCES`) |

---

## Design Summary

| Clause | Design |
|--------|--------|
| **MERGE** | `MergeClause` holds `PathPattern`s like `MatchClause`. `MergeOperator` wraps `NodeScan` + `CreateOperator`: scans for the pattern; if found, returns the matched record; if not found, creates it and returns the created record. |
| **WITH** | `WithClause` holds `ReturnItem`s like `ReturnClause`. Execution reuses the existing `Project` operator. The validator resets scope after `WITH` so downstream clauses only see projected variables. |
| **UNWIND** | `UnwindClause` holds an `expression` (the list) and an `alias` (the element variable). `UnwindOperator` pulls records from its child, evaluates the list expression, and emits one output record per list element with the alias bound. |

---

## Task Checklist

> Each task is designed to be 2–5 minutes. Run the exact command shown, verify the expected output, then apply the code change.

---

### Phase 1: AST Extensions

#### 1.1 Add `MERGE`, `WITH`, `UNWIND` to `ClauseType` enum

**File:** `include/cedar/cypher/ast.h`  
**Action:** Append three new enum values after `DELETE`.

```cpp
// Before (line 178-188):
enum class ClauseType {
  MATCH,
  WHERE,
  RETURN,
  ORDER_BY,
  LIMIT,
  SKIP,
  CREATE,
  SET,
  DELETE
};

// After:
enum class ClauseType {
  MATCH,
  WHERE,
  RETURN,
  ORDER_BY,
  LIMIT,
  SKIP,
  CREATE,
  SET,
  DELETE,
  MERGE,
  WITH,
  UNWIND
};
```

**Verify:**
```bash
grep -n "MERGE,\|WITH,\|UNWIND" include/cedar/cypher/ast.h
```
**Expected:**
```
189:  MERGE,
190:  WITH,
191:  UNWIND
```

---

#### 1.2 Add `MergeClause` struct

**File:** `include/cedar/cypher/ast.h`  
**Action:** Insert after `DeleteClause` (after line 258).

```cpp
// MERGE clause
struct MergeClause : QueryClause {
  std::vector<PathPattern> patterns;
  MergeClause() : QueryClause(ClauseType::MERGE) {}
};
```

---

#### 1.3 Add `WithClause` struct

**File:** `include/cedar/cypher/ast.h`  
**Action:** Insert after `MergeClause`.

```cpp
// WITH clause
struct WithClause : QueryClause {
  bool distinct = false;
  std::vector<ReturnItem> items;
  bool all = false;  // WITH *
  WithClause() : QueryClause(ClauseType::WITH) {}
};
```

---

#### 1.4 Add `UnwindClause` struct

**File:** `include/cedar/cypher/ast.h`  
**Action:** Insert after `WithClause`.

```cpp
// UNWIND clause
struct UnwindClause : QueryClause {
  std::shared_ptr<Expression> expression;
  std::string alias;
  UnwindClause() : QueryClause(ClauseType::UNWIND) {}
};
```

**Verify:**
```bash
grep -n "struct MergeClause\|struct WithClause\|struct UnwindClause" include/cedar/cypher/ast.h
```
**Expected:**
```
260:struct MergeClause : QueryClause {
267:struct WithClause : QueryClause {
275:struct UnwindClause : QueryClause {
```

---

### Phase 2: Parser Extensions

#### 2.1 Declare new clause parsers in `parser.h`

**File:** `include/cedar/cypher/parser.h`  
**Action:** Add declarations after `ParseDeleteClause` (around line 116).

```cpp
  std::shared_ptr<MergeClause> ParseMergeClause();
  std::shared_ptr<WithClause> ParseWithClause();
  std::shared_ptr<UnwindClause> ParseUnwindClause();
```

**Verify:**
```bash
grep -n "ParseMergeClause\|ParseWithClause\|ParseUnwindClause" include/cedar/cypher/parser.h
```

---

#### 2.2 Add keyword entries for `merge`, `unwind`

**File:** `include/cedar/cypher/parser.h`  
**Action:** Add `"merge"` and `"unwind"` to the `keywords_` map (around line 169).

```cpp
    {"merge", true},
    {"unwind", true},
```

> Note: `with` is already in the map (line 174) because it is used by `STARTS WITH` / `ENDS WITH`. We must be careful in the main dispatch that `WITH` as a clause keyword is matched before any expression parsing consumes it.

---

#### 2.3 Add clause dispatch in `ParseStatement`

**File:** `src/cypher/parser.cc`  
**Action:** Insert three new `else if` branches before the final `else` block in `ParseStatement` (around line 77, before `error_ = "Unexpected token..."`).

```cpp
    } else if (MatchKeyword("merge")) {
      auto merge = ParseMergeClause();
      if (merge) {
        stmt->clauses.push_back(merge);
      }
    } else if (MatchKeyword("with")) {
      auto with_clause = ParseWithClause();
      if (with_clause) {
        stmt->clauses.push_back(with_clause);
      }
    } else if (MatchKeyword("unwind")) {
      auto unwind = ParseUnwindClause();
      if (unwind) {
        stmt->clauses.push_back(unwind);
      }
```

**Verify (compile check):**
```bash
cd build && make cedar_cypher 2>&1 | head -n 20
```
**Expected:** Linker errors for undefined `ParseMergeClause` etc. (parser.cc still compiles).

---

#### 2.4 Implement `ParseMergeClause`

**File:** `src/cypher/parser.cc`  
**Action:** Add after `ParseCreateClause` (around line 317).

```cpp
std::shared_ptr<MergeClause> CypherParser::ParseMergeClause() {
  SkipWhitespaceAndComments();
  auto merge = std::make_shared<MergeClause>();
  while (!IsAtEnd()) {
    auto pattern = ParsePattern();
    if (!pattern.elements.empty()) {
      merge->patterns.push_back(pattern);
    }
    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  return merge;
}
```

---

#### 2.5 Implement `ParseWithClause`

**File:** `src/cypher/parser.cc`  
**Action:** Add after `ParseMergeClause`.

```cpp
std::shared_ptr<WithClause> CypherParser::ParseWithClause() {
  SkipWhitespaceAndComments();
  auto with_clause = std::make_shared<WithClause>();

  // Check for DISTINCT
  if (MatchKeyword("distinct")) {
    with_clause->distinct = true;
  }

  // Parse projection items
  while (!IsAtEnd()) {
    ReturnItem item;
    item.expression = ParseExpression();
    if (!item.expression) {
      error_ = "Failed to parse WITH expression";
      break;
    }

    SkipWhitespaceAndComments();
    if (MatchKeyword("as")) {
      item.alias = ParseIdentifier();
    }

    if (!item.alias.has_value()) {
      if (auto var = std::dynamic_pointer_cast<VariableExpr>(item.expression)) {
        item.alias = var->name;
      } else if (auto prop = std::dynamic_pointer_cast<PropertyExpr>(item.expression)) {
        item.alias = prop->property;
      }
    }

    with_clause->items.push_back(item);

    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  return with_clause;
}
```

---

#### 2.6 Implement `ParseUnwindClause`

**File:** `src/cypher/parser.cc`  
**Action:** Add after `ParseWithClause`.

```cpp
std::shared_ptr<UnwindClause> CypherParser::ParseUnwindClause() {
  SkipWhitespaceAndComments();
  auto unwind = std::make_shared<UnwindClause>();

  unwind->expression = ParseExpression();
  if (!unwind->expression) {
    error_ = "Failed to parse UNWIND expression";
    return nullptr;
  }

  SkipWhitespaceAndComments();
  if (!MatchKeyword("as")) {
    error_ = "Expected AS after UNWIND expression";
    return nullptr;
  }

  unwind->alias = ParseIdentifier();
  if (unwind->alias.empty()) {
    error_ = "Expected identifier after AS in UNWIND";
    return nullptr;
  }

  return unwind;
}
```

**Verify (compile check):**
```bash
cd build && make cedar_cypher 2>&1 | tail -n 5
```
**Expected:** `Linking CXX static library libcedar_cypher.a` (or no errors if already linked).

---

### Phase 3: Physical Operators

#### 3.1 Declare `MergeOperator` in `execution_plan.h`

**File:** `include/cedar/cypher/execution_plan.h`  
**Action:** Insert after `DeleteOperator` (before the `ExecutionPlanBuilder` class, around line 681).

```cpp
/**
 * @brief Merge operator — MATCH pattern, CREATE if not found
 */
class MergeOperator : public PhysicalOperator {
 public:
  explicit MergeOperator(std::shared_ptr<MergeClause> merge_clause);

  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Merge"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  bool RequiresGraph() const override { return false; }

 private:
  std::shared_ptr<MergeClause> merge_clause_;
  bool initialized_ = false;
  bool done_ = false;
  std::shared_ptr<Record> result_record_;
  uint64_t id_counter_ = 0;

  uint64_t GenerateId();
  cedar::Status MergeNode(const NodePattern& node, Record* record);
  cedar::Status MergeEdge(const RelationshipPattern& rel, const Record& record);
  uint16_t PropertyNameToColumnId(const std::string& name) const;
  cedar::Descriptor ValueToDescriptor(const Value& value, uint16_t col_id) const;
};
```

---

#### 3.2 Declare `UnwindOperator` in `execution_plan.h`

**File:** `include/cedar/cypher/execution_plan.h`  
**Action:** Insert after `MergeOperator`.

```cpp
/**
 * @brief Unwind operator — emit one record per list element
 */
class UnwindOperator : public PhysicalOperator {
 public:
  UnwindOperator(std::shared_ptr<Expression> list_expr, std::string alias);

  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Unwind"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  bool RequiresGraph() const override { return false; }

 private:
  std::shared_ptr<Expression> list_expr_;
  std::string alias_;

  std::shared_ptr<Record> current_record_;
  std::vector<Value> current_list_;
  size_t list_index_ = 0;
  bool initialized_ = false;
};
```

---

#### 3.3 Add builder methods to `ExecutionPlanBuilder`

**File:** `include/cedar/cypher/execution_plan.h`  
**Action:** Insert inside the `ExecutionPlanBuilder` private section (around line 717).

```cpp
  static std::shared_ptr<PhysicalOperator> BuildMergePlan(
      std::shared_ptr<MergeClause> merge);

  static std::shared_ptr<PhysicalOperator> BuildWithPlan(
      std::shared_ptr<WithClause> with_clause);

  static std::shared_ptr<PhysicalOperator> BuildUnwindPlan(
      std::shared_ptr<UnwindClause> unwind);
```

**Verify:**
```bash
grep -n "BuildMergePlan\|BuildWithPlan\|BuildUnwindPlan\|MergeOperator\|UnwindOperator" \
  include/cedar/cypher/execution_plan.h
```

---

#### 3.4 Implement `MergeOperator`

**File:** `src/cypher/operators/write_operators.cc`  
**Action:** Append at the end of the file (after `DeleteOperator`, before the closing namespace braces).

```cpp
// ============================================================================
// MergeOperator
// ============================================================================

MergeOperator::MergeOperator(std::shared_ptr<MergeClause> merge_clause)
    : merge_clause_(std::move(merge_clause)),
      initialized_(false),
      done_(false),
      id_counter_(0) {}

uint64_t MergeOperator::GenerateId() {
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
  return static_cast<uint64_t>(ns) + (++id_counter_);
}

bool MergeOperator::Init(ExecutionContext* ctx) {
  context_ = ctx;
  initialized_ = true;
  done_ = false;
  result_record_ = std::make_shared<Record>();
  return true;
}

std::shared_ptr<Record> MergeOperator::Next() {
  if (!initialized_ || done_) {
    return nullptr;
  }

  if (!merge_clause_ || merge_clause_->patterns.empty()) {
    done_ = true;
    return nullptr;
  }

  // Process all patterns in one call (MERGE produces a single result record)
  for (const auto& pattern : merge_clause_->patterns) {
    for (const auto& element : pattern.elements) {
      if (std::holds_alternative<NodePattern>(element)) {
        const auto& node = std::get<NodePattern>(element);
        // Attempt to find existing node by properties (id literal optimisation)
        bool found = false;
        auto id_it = node.properties.find("id");
        if (id_it != node.properties.end() && id_it->second &&
            id_it->second->expr_type == ExprType::LITERAL) {
          auto* literal = static_cast<LiteralExpr*>(id_it->second.get());
          if (literal->value.IsInt() && literal->value.GetInt() > 0) {
            uint64_t node_id = static_cast<uint64_t>(literal->value.GetInt());
            if (context_->graph && context_->graph->HasVertex(node_id)) {
              found = true;
              Node n;
              n.id = node_id;
              n.labels = node.labels.empty() ? std::vector<std::string>{"Node"} : node.labels;
              n.properties["id"] = Value(static_cast<int64_t>(node_id));
              result_record_->Set(node.variable, Value(n));
            } else if (context_->storage) {
              auto versions = context_->storage->Scan(node_id, Timestamp(0), Timestamp::Max());
              if (!versions.empty()) {
                found = true;
                Node n;
                n.id = node_id;
                n.labels = node.labels.empty() ? std::vector<std::string>{"Node"} : node.labels;
                n.properties["id"] = Value(static_cast<int64_t>(node_id));
                result_record_->Set(node.variable, Value(n));
              }
            }
          }
        }
        if (!found) {
          auto status = MergeNode(node, result_record_.get());
          if (!status.ok()) {
            CEDAR_LOG_WARN() << "MergeOperator: failed to merge node: " << status.ToString();
          }
        }
      } else if (std::holds_alternative<RelationshipPattern>(element)) {
        const auto& rel = std::get<RelationshipPattern>(element);
        auto status = MergeEdge(rel, *result_record_);
        if (!status.ok()) {
          CEDAR_LOG_WARN() << "MergeOperator: failed to merge edge: " << status.ToString();
        }
      }
    }
  }

  done_ = true;
  return result_record_;
}

cedar::Status MergeOperator::MergeNode(const NodePattern& node, Record* record) {
  if (!context_ || !context_->storage) {
    return cedar::Status::InvalidArgument("No storage available for MERGE");
  }

  uint64_t node_id = GenerateId();
  Node created_node;
  created_node.id = node_id;
  created_node.labels = node.labels;

  std::vector<CedarGraphStorage::BatchWriteItem> items;
  ExpressionEvaluator evaluator(context_);
  Record dummy_record;

  for (const auto& [prop_name, expr] : node.properties) {
    Value prop_value = Value::Null();
    if (expr) {
      prop_value = evaluator.Evaluate(*expr, dummy_record);
    }
    created_node.properties[prop_name] = prop_value;
    uint16_t col_id = PropertyNameToColumnId(prop_name);
    Descriptor desc = ValueToDescriptor(prop_value, col_id);
    items.emplace_back(node_id, EntityType::Vertex, col_id, desc, Timestamp::Static(), 0);
  }

  if (items.empty()) {
    items.emplace_back(node_id, EntityType::Vertex, 0,
                       Descriptor::InlineInt(0, 0), Timestamp::Static(), 0);
  }

  auto status = context_->storage->BatchWrite(items);
  if (!status.ok()) {
    return status;
  }

  context_->storage->MarkEntityCreated(node_id, EntityType::Vertex, Timestamp::Now());
  record->Set(node.variable, Value(created_node));
  return cedar::Status::OK();
}

cedar::Status MergeOperator::MergeEdge(const RelationshipPattern& rel,
                                       const Record& record) {
  if (!context_ || !context_->storage) {
    return cedar::Status::InvalidArgument("No storage available for MERGE edge");
  }

  uint64_t start_id = 0;
  uint64_t end_id = 0;
  for (const auto& [key, val] : record.values) {
    if (val.IsNode()) {
      if (start_id == 0) {
        start_id = val.GetNode().id;
      } else {
        end_id = val.GetNode().id;
      }
    }
  }

  if (start_id == 0 || end_id == 0) {
    return cedar::Status::InvalidArgument("MERGE edge requires both endpoints in record");
  }

  uint16_t edge_type = 0;
  if (!rel.types.empty()) {
    try {
      edge_type = static_cast<uint16_t>(std::stoi(rel.types[0]));
    } catch (...) {
      edge_type = static_cast<uint16_t>(std::hash<std::string>{}(rel.types[0]) & 0xFFFF);
    }
  }

  std::map<std::string, Value> edge_props;
  ExpressionEvaluator evaluator(context_);
  Record dummy_record;
  for (const auto& [prop_name, expr] : rel.properties) {
    Value prop_value = Value::Null();
    if (expr) {
      prop_value = evaluator.Evaluate(*expr, dummy_record);
    }
    edge_props[prop_name] = prop_value;
  }

  Descriptor edge_desc = Descriptor::InlineInt(0, 0);
  if (!rel.properties.empty()) {
    edge_desc = ValueToDescriptor(edge_props.begin()->second, 0);
  }

  auto status = context_->storage->PutEdge(
      start_id, end_id, edge_type, Timestamp::Now(), edge_desc, Timestamp(0));
  if (!status.ok()) {
    return status;
  }

  Relationship relationship;
  relationship.id = std::hash<std::string>{}(
      std::to_string(start_id) + ":" + std::to_string(end_id));
  relationship.start_id = start_id;
  relationship.end_id = end_id;
  relationship.type = rel.types.empty() ? "CONNECTED_TO" : rel.types[0];
  relationship.properties = std::move(edge_props);

  result_record_->Set(rel.variable, Value(relationship));
  return cedar::Status::OK();
}

std::string MergeOperator::GetDetails() const {
  if (!merge_clause_) return "0 patterns";
  return std::to_string(merge_clause_->patterns.size()) + " patterns";
}

std::unique_ptr<PhysicalOperator> MergeOperator::Clone() const {
  auto clone = std::make_unique<MergeOperator>(merge_clause_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->initialized_ = false;
  clone->done_ = false;
  clone->id_counter_ = 0;
  clone->result_record_.reset();
  return clone;
}

uint16_t MergeOperator::PropertyNameToColumnId(const std::string& name) const {
  return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
}

cedar::Descriptor MergeOperator::ValueToDescriptor(const Value& value,
                                                   uint16_t col_id) const {
  if (value.IsInt()) {
    return Descriptor::InlineInt(col_id, static_cast<int32_t>(value.GetInt()));
  }
  if (value.IsFloat()) {
    return Descriptor::InlineFloat(col_id, static_cast<float>(value.GetFloat()));
  }
  if (value.IsString()) {
    const std::string& s = value.GetString();
    if (s.size() <= 4) {
      auto opt = Descriptor::InlineShortStr(col_id, Slice(s));
      if (opt) return *opt;
    }
    return Descriptor::InlineInt(col_id, 0);
  }
  if (value.IsBool()) {
    return Descriptor::InlineInt(col_id, value.GetBool() ? 1 : 0);
  }
  return Descriptor::InlineInt(col_id, 0);
}
```

---

#### 3.5 Implement `UnwindOperator`

**File:** `src/cypher/execution_plan.cc`  
**Action:** Append before the `ExecutionPlanBuilder::Build` function (before line 993).

```cpp
// ============================================================================
// UnwindOperator Implementation
// ============================================================================

UnwindOperator::UnwindOperator(std::shared_ptr<Expression> list_expr,
                               std::string alias)
    : list_expr_(std::move(list_expr)), alias_(std::move(alias)) {}

bool UnwindOperator::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    if (!children_[0]->Init(ctx)) {
      return false;
    }
  }
  current_record_.reset();
  current_list_.clear();
  list_index_ = 0;
  initialized_ = true;
  return true;
}

std::shared_ptr<Record> UnwindOperator::Next() {
  if (!initialized_) {
    return nullptr;
  }

  while (true) {
    // If we have pending list elements, emit the next one
    if (list_index_ < current_list_.size()) {
      auto record = std::make_shared<Record>(*current_record_);
      record->Set(alias_, current_list_[list_index_]);
      ++list_index_;
      return record;
    }

    // Need next input record
    if (children_.empty()) {
      return nullptr;
    }

    current_record_ = children_[0]->Next();
    if (!current_record_) {
      return nullptr;
    }

    // Evaluate the list expression
    ExpressionEvaluator evaluator(context_);
    Value list_val = evaluator.Evaluate(*list_expr_, *current_record_);

    if (!list_val.IsList()) {
      CEDAR_LOG_WARN() << "UnwindOperator: expression did not evaluate to a list";
      continue;  // Skip non-list records
    }

    current_list_ = list_val.GetList();
    list_index_ = 0;
  }
}

std::string UnwindOperator::GetDetails() const {
  return "AS " + alias_;
}

std::unique_ptr<PhysicalOperator> UnwindOperator::Clone() const {
  auto clone = std::make_unique<UnwindOperator>(list_expr_, alias_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_record_.reset();
  clone->current_list_.clear();
  clone->list_index_ = 0;
  clone->initialized_ = false;
  return clone;
}
```

**Verify (compile check):**
```bash
cd build && make cedar_cypher 2>&1 | tail -n 10
```
**Expected:** No compilation errors.

---

### Phase 4: ExecutionPlanBuilder Wiring

#### 4.1 Collect new clauses in `Build`

**File:** `src/cypher/execution_plan.cc`  
**Action:** Add three new clause variables after `delete_clause` (around line 1010).

```cpp
  std::shared_ptr<MergeClause> merge_clause;
  std::shared_ptr<WithClause> with_clause;
  std::shared_ptr<UnwindClause> unwind_clause;
```

---

#### 4.2 Add switch cases in the clause collection loop

**File:** `src/cypher/execution_plan.cc`  
**Action:** Add cases inside the `for (const auto& clause : stmt->clauses)` switch (before `default:`).

```cpp
      case ClauseType::MERGE:
        merge_clause = std::static_pointer_cast<MergeClause>(clause);
        break;
      case ClauseType::WITH:
        with_clause = std::static_pointer_cast<WithClause>(clause);
        break;
      case ClauseType::UNWIND:
        unwind_clause = std::static_pointer_cast<UnwindClause>(clause);
        break;
```

---

#### 4.3 Wire MERGE, WITH, UNWIND into the build pipeline

**File:** `src/cypher/execution_plan.cc`  
**Action:** Insert after the DELETE block (around line 1085) and before the WHERE block.

```cpp
  // 1e. MERGE → MergeOperator (like CREATE but with existence check)
  if (merge_clause) {
    auto merge_op = BuildMergePlan(merge_clause);
    if (merge_op) {
      if (root) {
        merge_op->AddChild(root);
      }
      root = merge_op;
    }
  }

  // 1f. WITH → Project (reuses existing Project operator)
  if (with_clause) {
    auto with_op = BuildWithPlan(with_clause);
    if (with_op) {
      if (root) {
        with_op->AddChild(root);
      }
      root = with_op;
    }
  }

  // 1g. UNWIND → UnwindOperator
  if (unwind_clause) {
    auto unwind_op = BuildUnwindPlan(unwind_clause);
    if (unwind_op) {
      if (root) {
        unwind_op->AddChild(root);
      }
      root = unwind_op;
    }
  }
```

---

#### 4.4 Implement `BuildMergePlan`

**File:** `src/cypher/execution_plan.cc`  
**Action:** Add after `BuildDeletePlan` (around line 1252).

```cpp
std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildMergePlan(
    std::shared_ptr<MergeClause> merge) {
  if (!merge || merge->patterns.empty()) {
    return nullptr;
  }
  return std::make_shared<MergeOperator>(merge);
}
```

---

#### 4.5 Implement `BuildWithPlan`

**File:** `src/cypher/execution_plan.cc`  
**Action:** Add after `BuildMergePlan`.

```cpp
std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildWithPlan(
    std::shared_ptr<WithClause> with_clause) {
  if (!with_clause || with_clause->items.empty()) {
    return nullptr;
  }

  std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections;
  for (const auto& item : with_clause->items) {
    std::string col_name = item.alias.value_or("column");
    projections.push_back({col_name, item.expression});
  }

  auto project = std::make_shared<Project>(projections);

  // DISTINCT
  if (with_clause->distinct) {
    std::vector<std::shared_ptr<Expression>> distinct_keys;
    for (const auto& item : with_clause->items) {
      distinct_keys.push_back(item.expression);
    }
    auto distinct_op = std::make_shared<Distinct>(distinct_keys);
    distinct_op->AddChild(project);
    return distinct_op;
  }

  return project;
}
```

---

#### 4.6 Implement `BuildUnwindPlan`

**File:** `src/cypher/execution_plan.cc`  
**Action:** Add after `BuildWithPlan`.

```cpp
std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildUnwindPlan(
    std::shared_ptr<UnwindClause> unwind) {
  if (!unwind || !unwind->expression || unwind->alias.empty()) {
    return nullptr;
  }
  return std::make_shared<UnwindOperator>(unwind->expression, unwind->alias);
}
```

**Verify (compile check):**
```bash
cd build && make cedar_cypher 2>&1 | tail -n 5
```
**Expected:** Clean build.

---

### Phase 5: Validator Updates

#### 5.1 Add validator cases for new clauses

**File:** `src/cypher/validator.cc`  
**Action:** Add inside `ValidateQueryStatement` before the scope pop loop (around line 38).

```cpp
    } else if (auto* merge_clause = dynamic_cast<const MergeClause*>(clause.get())) {
      for (const auto& pattern : merge_clause->patterns) {
        for (const auto& elem : pattern.elements) {
          if (std::holds_alternative<NodePattern>(elem)) {
            const auto& node = std::get<NodePattern>(elem);
            if (!node.variable.empty()) {
              if (pushed_vars) pushed_vars->push_back(node.variable);
            }
          } else if (std::holds_alternative<RelationshipPattern>(elem)) {
            const auto& rel = std::get<RelationshipPattern>(elem);
            if (!rel.variable.empty()) {
              if (pushed_vars) pushed_vars->push_back(rel.variable);
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
        if (pushed_vars) pushed_vars->push_back(unwind_clause->alias);
      }
    }
```

**Verify (compile check):**
```bash
cd build && make cedar_cypher 2>&1 | tail -n 5
```

---

### Phase 6: Tests (TDD Loop)

#### 6.1 Create parser test file

**File:** `tests/cypher/test_merge_with_unwind_parsing.cc`

```cpp
// Copyright 2025 The Cedar Authors

#include <gtest/gtest.h>
#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

// ============================================================================
// MERGE Parsing Tests
// ============================================================================

TEST(MergeClauseTest, ParseSingleNodeMerge) {
  CypherParser parser("MERGE (n:Person {name: 'Alice'}) RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  ASSERT_EQ(stmt->clauses.size(), 2);
  auto merge = std::dynamic_pointer_cast<MergeClause>(stmt->clauses[0]);
  ASSERT_NE(merge, nullptr);
  ASSERT_EQ(merge->patterns.size(), 1);
  auto* node = std::get_if<NodePattern>(&merge->patterns[0].elements[0]);
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->variable, "n");
  EXPECT_EQ(node->labels[0], "Person");
}

TEST(MergeClauseTest, ParseMergePatternWithRel) {
  CypherParser parser("MERGE (a)-[r:KNOWS]->(b) RETURN a, b");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << parser.GetError();
  auto merge = std::dynamic_pointer_cast<MergeClause>(stmt->clauses[0]);
  ASSERT_NE(merge, nullptr);
  ASSERT_EQ(merge->patterns[0].elements.size(), 3);
}

// ============================================================================
// WITH Parsing Tests
// ============================================================================

TEST(WithClauseTest, ParseSimpleWith) {
  CypherParser parser("MATCH (n) WITH n.name AS personName RETURN personName");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  ASSERT_EQ(stmt->clauses.size(), 3);
  auto with_clause = std::dynamic_pointer_cast<WithClause>(stmt->clauses[1]);
  ASSERT_NE(with_clause, nullptr);
  ASSERT_EQ(with_clause->items.size(), 1);
  EXPECT_EQ(with_clause->items[0].alias, "personName");
}

TEST(WithClauseTest, ParseWithDistinct) {
  CypherParser parser("MATCH (n) WITH DISTINCT n.age AS age RETURN age");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  auto with_clause = std::dynamic_pointer_cast<WithClause>(stmt->clauses[1]);
  ASSERT_NE(with_clause, nullptr);
  EXPECT_TRUE(with_clause->distinct);
}

// ============================================================================
// UNWIND Parsing Tests
// ============================================================================

TEST(UnwindClauseTest, ParseUnwindLiteralList) {
  CypherParser parser("UNWIND [1, 2, 3] AS x RETURN x");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  ASSERT_EQ(stmt->clauses.size(), 2);
  auto unwind = std::dynamic_pointer_cast<UnwindClause>(stmt->clauses[0]);
  ASSERT_NE(unwind, nullptr);
  EXPECT_EQ(unwind->alias, "x");
  ASSERT_NE(unwind->expression, nullptr);
  EXPECT_EQ(unwind->expression->expr_type, ExprType::LIST_LITERAL);
}

TEST(UnwindClauseTest, ParseUnwindVariable) {
  CypherParser parser("MATCH (n) UNWIND n.tags AS tag RETURN tag");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();
  auto unwind = std::dynamic_pointer_cast<UnwindClause>(stmt->clauses[1]);
  ASSERT_NE(unwind, nullptr);
  EXPECT_EQ(unwind->alias, "tag");
  EXPECT_EQ(unwind->expression->expr_type, ExprType::PROPERTY);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

---

#### 6.2 Register parser test in CMake

**File:** `tests/CMakeLists.txt`  
**Action:** Add after the `test_set_delete_parsing` block (around line 44).

```cmake
# MERGE/WITH/UNWIND parsing test
add_executable(test_merge_with_unwind_parsing
    cypher/test_merge_with_unwind_parsing.cc
)
target_link_libraries(test_merge_with_unwind_parsing cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_merge_with_unwind_parsing)
```

**Run parser test (expect compile + pass):**
```bash
cd build && cmake .. -DBUILD_TESTS=ON && make test_merge_with_unwind_parsing -j$(sysctl -n hw.ncpu)
```
**Expected:** Binary `test_merge_with_unwind_parsing` builds.

```bash
cd build && ./tests/test_merge_with_unwind_parsing
```
**Expected:**
```
[==========] Running 6 tests from 3 test suites
[----------] Global test environment set-up
[----------] 2 tests from MergeClauseTest
[ RUN      ] MergeClauseTest.ParseSingleNodeMerge
[       OK ] MergeClauseTest.ParseSingleNodeMerge (0 ms)
[ RUN      ] MergeClauseTest.ParseMergePatternWithRel
[       OK ] MergeClauseTest.ParseMergePatternWithRel (0 ms)
[----------] 2 tests from WithClauseTest
[ RUN      ] WithClauseTest.ParseSimpleWith
[       OK ] WithClauseTest.ParseSimpleWith (0 ms)
[ RUN      ] WithClauseTest.ParseWithDistinct
[       OK ] WithClauseTest.ParseWithDistinct (0 ms)
[----------] 2 tests from UnwindClauseTest
[ RUN      ] UnwindClauseTest.ParseUnwindLiteralList
[       OK ] UnwindClauseTest.ParseUnwindLiteralList (0 ms)
[ RUN      ] UnwindClauseTest.ParseUnwindVariable
[       OK ] UnwindClauseTest.ParseUnwindVariable (0 ms)
[==========] 6 tests from 3 test suites ran (2 ms total)
[  PASSED  ] 6 tests.
```

---

#### 6.3 Create execution test file

**File:** `tests/cypher/test_merge_with_unwind_execution.cc`

```cpp
// Copyright 2025 The Cedar Authors

#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/value.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"

using namespace cedar::cypher;
using namespace cedar;

static std::string GetTempDbPath() {
  static std::atomic<int> counter{0};
  auto pid = getpid();
  auto seq = counter.fetch_add(1);
  auto tmp = std::filesystem::temp_directory_path() /
             ("cedar_merge_test_" + std::to_string(pid) + "_" + std::to_string(seq));
  std::filesystem::remove_all(tmp);
  std::filesystem::create_directories(tmp);
  return tmp.string();
}

// ============================================================================
// MergeOperator Tests
// ============================================================================

class MergeOperatorTest : public ::testing::Test {
 protected:
  CedarGraphStorage* storage_ = nullptr;
  std::string db_path_;

  void SetUp() override {
    db_path_ = GetTempDbPath();
    CedarOptions options;
    options.create_if_missing = true;
    auto s = CedarGraphStorage::Open(options, db_path_, &storage_);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  void TearDown() override {
    delete storage_;
    std::filesystem::remove_all(db_path_);
  }
};

TEST_F(MergeOperatorTest, MergeCreatesNodeWhenNotFound) {
  auto merge_clause = std::make_shared<MergeClause>();
  PathPattern pattern;
  NodePattern node;
  node.variable = "n";
  node.labels = {"Person"};
  node.properties["name"] = std::make_shared<LiteralExpr>(Value("Alice"));
  pattern.elements.push_back(node);
  merge_clause->patterns.push_back(std::move(pattern));

  MergeOperator op(merge_clause);
  ExecutionContext ctx;
  ctx.storage = storage_;

  ASSERT_TRUE(op.Init(&ctx));
  auto record = op.Next();
  ASSERT_NE(record, nullptr);
  EXPECT_TRUE(record->Get("n")->IsNode());
  EXPECT_EQ(record->Get("n")->GetNode().labels[0], "Person");
  EXPECT_EQ(op.Next(), nullptr);
}

// ============================================================================
// UnwindOperator Tests
// ============================================================================

class UnwindOperatorTest : public ::testing::Test {};

TEST_F(UnwindOperatorTest, UnwindLiteralList) {
  // Build a mock child that returns a single empty record
  class MockChild : public PhysicalOperator {
   public:
    bool emitted_ = false;
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override {
      if (emitted_) return nullptr;
      emitted_ = true;
      return std::make_shared<Record>();
    }
    std::string GetName() const override { return "MockChild"; }
    std::unique_ptr<PhysicalOperator> Clone() const override {
      return std::make_unique<MockChild>();
    }
  };

  auto mock = std::make_shared<MockChild>();

  // UNWIND [10, 20, 30] AS x
  std::vector<std::shared_ptr<Expression>> elems;
  elems.push_back(std::make_shared<LiteralExpr>(Value(10)));
  elems.push_back(std::make_shared<LiteralExpr>(Value(20)));
  elems.push_back(std::make_shared<LiteralExpr>(Value(30)));
  auto list_expr = std::make_shared<ListLiteralExpr>(elems);

  UnwindOperator op(list_expr, "x");
  op.AddChild(mock);

  ExecutionContext ctx;
  ASSERT_TRUE(op.Init(&ctx));

  auto r1 = op.Next();
  ASSERT_NE(r1, nullptr);
  EXPECT_EQ(r1->Get("x")->GetInt(), 10);

  auto r2 = op.Next();
  ASSERT_NE(r2, nullptr);
  EXPECT_EQ(r2->Get("x")->GetInt(), 20);

  auto r3 = op.Next();
  ASSERT_NE(r3, nullptr);
  EXPECT_EQ(r3->Get("x")->GetInt(), 30);

  EXPECT_EQ(op.Next(), nullptr);
}

TEST_F(UnwindOperatorTest, UnwindExhaustsAfterList) {
  class EmptyChild : public PhysicalOperator {
   public:
    bool Init(ExecutionContext*) override { return true; }
    std::shared_ptr<Record> Next() override { return nullptr; }
    std::string GetName() const override { return "EmptyChild"; }
    std::unique_ptr<PhysicalOperator> Clone() const override {
      return std::make_unique<EmptyChild>();
    }
  };

  auto mock = std::make_shared<EmptyChild>();
  std::vector<std::shared_ptr<Expression>> elems;
  auto list_expr = std::make_shared<ListLiteralExpr>(elems);

  UnwindOperator op(list_expr, "x");
  op.AddChild(mock);

  ExecutionContext ctx;
  ASSERT_TRUE(op.Init(&ctx));
  EXPECT_EQ(op.Next(), nullptr);
}

// ============================================================================
// ExecutionPlanBuilder Tests
// ============================================================================

TEST(ExecutionPlanBuilderMergeWithUnwind, BuildPlanWithMergeClause) {
  auto stmt = std::make_shared<QueryStatement>();
  auto merge_clause = std::make_shared<MergeClause>();
  PathPattern pattern;
  NodePattern node;
  node.variable = "n";
  node.labels = {"TestLabel"};
  pattern.elements.push_back(node);
  merge_clause->patterns.push_back(std::move(pattern));
  stmt->clauses.push_back(merge_clause);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  EXPECT_NE(plan->Explain().find("Merge"), std::string::npos);
}

TEST(ExecutionPlanBuilderMergeWithUnwind, BuildPlanWithWithClause) {
  auto stmt = std::make_shared<QueryStatement>();

  auto match_clause = std::make_shared<MatchClause>();
  PathPattern pattern;
  NodePattern node;
  node.variable = "n";
  pattern.elements.push_back(node);
  match_clause->patterns.push_back(std::move(pattern));
  stmt->clauses.push_back(match_clause);

  auto with_clause = std::make_shared<WithClause>();
  ReturnItem item;
  item.expression = std::make_shared<PropertyExpr>("n", "name");
  item.alias = "personName";
  with_clause->items.push_back(item);
  stmt->clauses.push_back(with_clause);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  auto explain = plan->Explain();
  EXPECT_NE(explain.find("Project"), std::string::npos);
  EXPECT_NE(explain.find("NodeScan"), std::string::npos);
}

TEST(ExecutionPlanBuilderMergeWithUnwind, BuildPlanWithUnwindClause) {
  auto stmt = std::make_shared<QueryStatement>();

  auto unwind_clause = std::make_shared<UnwindClause>();
  std::vector<std::shared_ptr<Expression>> elems;
  elems.push_back(std::make_shared<LiteralExpr>(Value(1)));
  elems.push_back(std::make_shared<LiteralExpr>(Value(2)));
  unwind_clause->expression = std::make_shared<ListLiteralExpr>(elems);
  unwind_clause->alias = "x";
  stmt->clauses.push_back(unwind_clause);

  auto plan = ExecutionPlanBuilder::Build(stmt, nullptr);
  ASSERT_NE(plan, nullptr);
  EXPECT_NE(plan->Explain().find("Unwind"), std::string::npos);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

---

#### 6.4 Register execution test in CMake

**File:** `tests/CMakeLists.txt`  
**Action:** Add after the parser test block.

```cmake
# MERGE/WITH/UNWIND execution test
add_executable(test_merge_with_unwind_execution
    cypher/test_merge_with_unwind_execution.cc
)
target_link_libraries(test_merge_with_unwind_execution cedar cedar_cypher gtest pthread)
gtest_discover_tests(test_merge_with_unwind_execution)
```

**Run execution tests (expect compile + pass):**
```bash
cd build && make test_merge_with_unwind_execution -j$(sysctl -n hw.ncpu)
```

```bash
cd build && ./tests/test_merge_with_unwind_execution
```
**Expected:**
```
[==========] Running 6 tests from 3 test suites
[----------] 1 test from MergeOperatorTest
[ RUN      ] MergeOperatorTest.MergeCreatesNodeWhenNotFound
[       OK ] MergeOperatorTest.MergeCreatesNodeWhenNotFound (5 ms)
[----------] 2 tests from UnwindOperatorTest
[ RUN      ] UnwindOperatorTest.UnwindLiteralList
[       OK ] UnwindOperatorTest.UnwindLiteralList (0 ms)
[ RUN      ] UnwindOperatorTest.UnwindExhaustsAfterList
[       OK ] UnwindOperatorTest.UnwindExhaustsAfterList (0 ms)
[----------] 3 tests from ExecutionPlanBuilderMergeWithUnwind
[ RUN      ] ExecutionPlanBuilderMergeWithUnwind.BuildPlanWithMergeClause
[       OK ] ... (0 ms)
[ RUN      ] ExecutionPlanBuilderMergeWithUnwind.BuildPlanWithWithClause
[       OK ] ... (0 ms)
[ RUN      ] ExecutionPlanBuilderMergeWithUnwind.BuildPlanWithUnwindClause
[       OK ] ... (0 ms)
[==========] 6 tests ran (10 ms total)
[  PASSED  ] 6 tests.
```

---

#### 6.5 End-to-end planner test

**File:** `tests/cypher/test_planner.cc`  
**Action:** Append new test cases at the end of the file (before `main`).

```cpp
TEST(QueryPlanner, PlanWithClause) {
  QueryPlanner planner;
  CypherParser parser("MATCH (n:Person) WITH n.name AS name RETURN name");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();

  auto plan = planner.Plan(*stmt);
  ASSERT_NE(plan, nullptr) << "Plan failed: " << planner.GetLastError();

  std::string explain = plan->Explain();
  EXPECT_NE(explain.find("Project"), std::string::npos);
  EXPECT_NE(explain.find("NodeScan"), std::string::npos);
  EXPECT_NE(explain.find("ProduceResults"), std::string::npos);
}

TEST(QueryPlanner, PlanUnwindClause) {
  QueryPlanner planner;
  CypherParser parser("UNWIND [1, 2, 3] AS x RETURN x");
  auto stmt = parser.ParseStatement();
  ASSERT_NE(stmt, nullptr) << "Parse failed: " << parser.GetError();

  auto plan = planner.Plan(*stmt);
  ASSERT_NE(plan, nullptr) << "Plan failed: " << planner.GetLastError();

  std::string explain = plan->Explain();
  EXPECT_NE(explain.find("Unwind"), std::string::npos);
  EXPECT_NE(explain.find("ProduceResults"), std::string::npos);
}
```

**Run existing planner test to confirm no regressions:**
```bash
cd build && make test_planner -j$(sysctl -n hw.ncpu) && ./tests/test_planner
```
**Expected:** All tests pass including the two new ones.

---

### Phase 7: Final Verification & Commit

#### 7.1 Full cypher test suite

```bash
cd build && cmake .. -DBUILD_TESTS=ON && make -j$(sysctl -n hw.ncpu)
```

**Expected:** `cedar_cypher` links cleanly.

```bash
cd build && ctest -R "test_merge_with_unwind_parsing|test_merge_with_unwind_execution|test_planner|test_write_operators|test_set_delete_parsing|test_parser" --output-on-failure
```
**Expected:**
```
Test project /Users/wangyang/Desktop/CedarGraph-Core/build
    Start 1: test_merge_with_unwind_parsing
1/6 Test #1: test_merge_with_unwind_parsing ... Passed
    Start 2: test_merge_with_unwind_execution
2/6 Test #2: test_merge_with_unwind_execution ... Passed
    Start 3: test_planner
3/6 Test #3: test_planner .................... Passed
    Start 4: test_write_operators
4/6 Test #4: test_write_operators ............ Passed
    Start 5: test_set_delete_parsing
5/6 Test #5: test_set_delete_parsing ......... Passed
    Start 6: test_parser
6/6 Test #6: test_parser ..................... Passed

100% tests passed, 0 tests failed
```

#### 7.2 Git commit

```bash
git add -A
git commit -m "feat(cypher): implement MERGE, WITH, UNWIND clauses

- AST: add MergeClause, WithClause, UnwindClause to ClauseType enum
- Parser: parse MERGE patterns, WITH projections, UNWIND list expressions
- Execution: MergeOperator (MATCH-or-CREATE), UnwindOperator (list expansion)
- Planner: wire MERGE/WITH/UNWIND into ExecutionPlanBuilder::Build
- Validator: handle new clauses, reset scope on WITH
- Tests: parser + execution + builder tests for all three clauses

Refs: Sub-Plan F"
```

---

## Summary of Changes

| File | What changed |
|------|-------------|
| `include/cedar/cypher/ast.h` | Added `MERGE`, `WITH`, `UNWIND` to `ClauseType`; added `MergeClause`, `WithClause`, `UnwindClause` structs |
| `include/cedar/cypher/parser.h` | Declared `ParseMergeClause`, `ParseWithClause`, `ParseUnwindClause`; added keywords |
| `include/cedar/cypher/execution_plan.h` | Declared `MergeOperator`, `UnwindOperator`, builder methods |
| `src/cypher/parser.cc` | Implemented dispatch + 3 clause parsers |
| `src/cypher/execution_plan.cc` | Wired clauses into `Build`; implemented `BuildMergePlan`, `BuildWithPlan`, `BuildUnwindPlan`; added `UnwindOperator` impl |
| `src/cypher/operators/write_operators.cc` | Implemented `MergeOperator` (scan-then-create logic) |
| `src/cypher/validator.cc` | Added validation + scope management for new clauses |
| `tests/cypher/test_merge_with_unwind_parsing.cc` | Parser TDD tests (6 cases) |
| `tests/cypher/test_merge_with_unwind_execution.cc` | Operator + builder TDD tests (6 cases) |
| `tests/cypher/test_planner.cc` | 2 additional planner E2E tests |
| `tests/CMakeLists.txt` | Registered 2 new test executables |
