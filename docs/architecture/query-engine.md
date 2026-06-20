# Query Engine Documentation

## Overview

The query engine is responsible for parsing, planning, and executing Cypher queries against the graph database. It supports OpenCypher syntax with temporal extensions for time-travel queries.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Query Engine                          │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   Parser    │  │   Planner   │  │  Executor   │     │
│  │             │  │             │  │             │     │
│  │  Cypher →   │  │  AST →      │  │  Plan →     │     │
│  │  AST        │  │  Plan       │  │  Results    │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │              Plan Cache                          │   │
│  │  - Fingerprint-based caching                    │   │
│  │  - Thread-safe Clone()                          │   │
│  │  - LRU eviction                                 │   │
│  └─────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │              Expression Evaluator                │   │
│  │  - Arithmetic operations                        │   │
│  │  - Logical operations (3VL)                     │   │
│  │  - Function calls                               │   │
│  │  - Property access                              │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## Cypher Parser

### Supported Syntax

**Basic Queries:**
```cypher
-- Create
CREATE (n:Person {name: 'Alice', age: 30})
CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)

-- Match
MATCH (n:Person) RETURN n
MATCH (n:Person) WHERE n.age > 25 RETURN n.name, n.age
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name

-- Update
MATCH (n:Person {name: 'Alice'}) SET n.age = 31
MATCH (n:Person {name: 'Alice'}) DELETE n

-- Merge
MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = timestamp()
```

**Temporal Extensions:**
```cypher
-- Point-in-time query
MATCH (n:Person) FOR SYSTEM_TIME AS OF timestamp('2024-01-01') RETURN n

-- Time range query
MATCH (n:Person) FOR SYSTEM_TIME BETWEEN timestamp('2024-01-01') 
  AND timestamp('2024-12-31') RETURN n
```

**Clauses:**
- `MATCH`: Pattern matching
- `WHERE`: Filtering
- `RETURN`: Projection
- `ORDER BY`: Sorting
- `LIMIT`/`SKIP`: Pagination
- `WITH`: Intermediate results
- `UNWIND`: List expansion
- `CREATE`/`MERGE`/`SET`/`DELETE`: Updates

### Parser Implementation

The parser uses recursive descent:

```cpp
class CypherParser {
  std::string query_;
  size_t pos_;
  std::string error_;
  
  std::unique_ptr<QueryStatement> ParseStatement();
  std::unique_ptr<MatchClause> ParseMatchClause();
  std::unique_ptr<WhereClause> ParseWhereClause();
  std::unique_ptr<ReturnClause> ParseReturnClause();
  // ...
};
```

**Error Handling:**
- Detailed error messages with position
- `IsValid()` for quick validation
- `GetError()` for error details

### AST Node Types

```cpp
enum class NodeType {
  QUERY,
  MATCH_CLAUSE,
  WHERE_CLAUSE,
  RETURN_CLAUSE,
  CREATE_CLAUSE,
  SET_CLAUSE,
  DELETE_CLAUSE,
  MERGE_CLAUSE,
  WITH_CLAUSE,
  UNWIND_CLAUSE,
  ORDER_BY_CLAUSE,
  LIMIT_CLAUSE,
  SKIP_CLAUSE,
  // Expressions
  LITERAL,
  VARIABLE,
  PROPERTY,
  ARITHMETIC,
  COMPARISON,
  LOGICAL,
  FUNCTION_CALL,
  // Patterns
  NODE_PATTERN,
  EDGE_PATTERN,
  PATH_PATTERN,
};
```

## Query Planner

### Planning Process

1. **Parse**: Convert query string to AST
2. **Validate**: Check semantic correctness
3. **Optimize**: Apply optimization rules
4. **Plan**: Generate execution plan

### Optimization Rules

**Predicate Pushdown:**
```cpp
// Before: Filter after scan
NodeScan → Filter(age > 25)

// After: Push filter into scan
NodeScan(age > 25)
```

**Projection Pushdown:**
```cpp
// Before: Scan all properties
NodeScan → Project(name, age)

// After: Scan only needed properties
NodeScan(name, age) → Project
```

**Index Selection:**
```cpp
// Before: Full scan
NodeScan → Filter(name = 'Alice')

// After: Index lookup
IndexScan(name = 'Alice')
```

### Execution Plan Operators

**NodeScan:**
```cpp
class NodeScan : public PhysicalOperator {
  std::string variable_;
  std::optional<std::string> label_;
  std::map<std::string, Expression*> properties_;
  std::vector<uint64_t> node_ids_;
  size_t current_index_;
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
};
```

**Expand:**
```cpp
class Expand : public PhysicalOperator {
  std::string src_variable_;
  std::string dst_variable_;
  std::string edge_variable_;
  Direction direction_;
  std::optional<uint16_t> edge_type_;
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
};
```

**Filter:**
```cpp
class Filter : public PhysicalOperator {
  std::shared_ptr<Expression> predicate_;
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
};
```

**ProduceResults:**
```cpp
class ProduceResults : public PhysicalOperator {
  std::vector<std::string> columns_;
  ResultSet result_set_;
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  ResultSet GetResultSet();
};
```

### Temporal Operators

**TemporalNodeScan:**
```cpp
class TemporalNodeScan : public PhysicalOperator {
  Timestamp query_timestamp_;
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
};
```

**TemporalExpand:**
```cpp
class TemporalExpand : public PhysicalOperator {
  Timestamp query_timestamp_;
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
};
```

## Execution Engine

### Execution Context

```cpp
struct ExecutionContext {
  CedarGraph* graph;
  CedarGraphStorage* storage;
  Timestamp query_timestamp;
  
  // Variable bindings
  std::unordered_map<std::string, Value> variables;
  
  // GCN callback for edge traversal
  std::function<std::vector<uint64_t>(uint64_t, uint32_t, uint64_t)> 
    gcn_traversal_callback;
  
  // Statistics
  struct Stats {
    std::atomic<uint64_t> rows_scanned{0};
    std::atomic<uint64_t> rows_returned{0};
    std::atomic<uint32_t> storage_nodes_accessed{0};
    std::atomic<uint32_t> network_roundtrips{0};
  } stats;
};
```

### Record Format

```cpp
struct Record {
  std::map<std::string, Value> values;
  
  std::optional<Value> Get(const std::string& key) const;
  void Set(const std::string& key, const Value& value);
};
```

### Value Types

```cpp
class Value {
  enum Type {
    NULL_VALUE,
    BOOL,
    INT,
    FLOAT,
    STRING,
    NODE,
    RELATIONSHIP,
    PATH,
    LIST,
    MAP,
  };
  
  // Type-specific accessors
  bool IsNull() const;
  bool GetBool() const;
  int64_t GetInt() const;
  double GetFloat() const;
  std::string GetString() const;
  Node GetNode() const;
  Relationship GetRelationship() const;
};
```

### Execution Flow

```cpp
ResultSet Execute(ExecutionContext* ctx) {
  // 1. Initialize operators
  if (!root_->Init(ctx)) {
    return ResultSet("Failed to initialize");
  }
  
  // 2. Pull records through pipeline
  ResultSet result;
  while (auto record = root_->Next()) {
    result.records.push_back(*record);
  }
  
  return result;
}
```

## Expression Evaluator

### Supported Operations

**Arithmetic:**
```cpp
Value EvaluateArithmetic(const ArithmeticExpr& expr, const Record& record) {
  auto left = Evaluate(*expr.left, record);
  auto right = Evaluate(*expr.right, record);
  
  switch (expr.op) {
    case ADD: return Value(left.GetFloat() + right.GetFloat());
    case SUB: return Value(left.GetFloat() - right.GetFloat());
    case MUL: return Value(left.GetFloat() * right.GetFloat());
    case DIV: return right.GetFloat() != 0 ? 
               Value(left.GetFloat() / right.GetFloat()) : Value();
    case MOD: return right.GetFloat() != 0 ? 
               Value(std::fmod(left.GetFloat(), right.GetFloat())) : Value();
  }
}
```

**Comparison:**
```cpp
Value EvaluateComparison(const ComparisonExpr& expr, const Record& record) {
  auto left = Evaluate(*expr.left, record);
  auto right = Evaluate(*expr.right, record);
  
  switch (expr.op) {
    case EQ: return Value(left == right);
    case NE: return Value(left != right);
    case LT: return Value(left < right);
    case LE: return Value(left <= right);
    case GT: return Value(left > right);
    case GE: return Value(left >= right);
  }
}
```

**Logical (3-Valued Logic):**
```cpp
Value EvaluateLogical(const LogicalExpr& expr, const Record& record) {
  auto left = Evaluate(*expr.left, record);
  
  // Short-circuit evaluation
  if (expr.op == AND) {
    if (!left.IsNull() && !left.GetBool()) return Value(false);
  } else if (expr.op == OR) {
    if (!left.IsNull() && left.GetBool()) return Value(true);
  }
  
  auto right = Evaluate(*expr.right, record);
  
  // Handle NULL
  if (left.IsNull() || right.IsNull()) {
    return Value();  // NULL
  }
  
  switch (expr.op) {
    case AND: return Value(left.GetBool() && right.GetBool());
    case OR: return Value(left.GetBool() || right.GetBool());
    case NOT: return Value(!left.GetBool());
  }
}
```

### NULL Handling

CedarGraph implements SQL NULL semantics (3-valued logic):

| Expression | Result |
|------------|--------|
| `NULL AND true` | `NULL` |
| `NULL AND false` | `false` |
| `NULL OR true` | `true` |
| `NULL OR false` | `NULL` |
| `NOT NULL` | `NULL` |
| `NULL = NULL` | `NULL` |
| `NULL + 1` | `NULL` |

## Plan Cache

### Cache Structure

```cpp
class CypherEngine {
  std::unordered_map<std::string, std::shared_ptr<ExecutionPlan>> plan_cache_;
  std::shared_mutex plan_cache_mutex_;
  
  static constexpr size_t kMaxPlanCacheSize = 1000;
};
```

### Cache Key (Fingerprint)

```cpp
std::string ComputeFingerprint(const std::string& query) {
  // Normalize query
  std::string normalized = NormalizeQuery(query);
  
  // Compute hash
  return std::to_string(std::hash<std::string>{}(normalized));
}
```

### Cache Operations

**Get Cached Plan:**
```cpp
std::shared_ptr<ExecutionPlan> GetCachedPlan(const std::string& fingerprint) {
  std::shared_lock<std::shared_mutex> lock(plan_cache_mutex_);
  auto it = plan_cache_.find(fingerprint);
  if (it != plan_cache_.end()) {
    return it->second;
  }
  return nullptr;
}
```

**Cache Plan:**
```cpp
void CachePlan(const std::string& fingerprint, 
               std::shared_ptr<ExecutionPlan> plan) {
  std::unique_lock<std::shared_mutex> lock(plan_cache_mutex_);
  
  // Evict if full
  if (plan_cache_.size() >= kMaxPlanCacheSize) {
    plan_cache_.erase(plan_cache_.begin());
  }
  
  plan_cache_[fingerprint] = plan;
}
```

### Thread Safety

Plans are executed via `Clone()` to ensure thread safety:

```cpp
auto cached = GetCachedPlan(fingerprint);
if (cached) {
  auto clone = cached->Clone();
  return clone->Execute(&ctx);
}
```

## Storage Integration

### Node Scan

```cpp
bool NodeScan::Init(ExecutionContext* ctx) {
  // 1. Check for point lookup
  if (HasIdProperty()) {
    node_ids_ = {GetIdValue()};
    return true;
  }
  
  // 2. Label-index-based scan
  if (label_ && ctx->storage) {
    auto* engine = ctx->storage->GetLsmEngine();
    auto entity_ids = engine->LookupLabelIndex(*label_);
    if (!entity_ids.empty()) {
      node_ids_ = std::move(entity_ids);
      return true;
    }
  }
  
  // 3. Full scan with limit
  for (uint64_t i = 1; i <= max_entity_id; ++i) {
    auto versions = ctx->storage->Scan(i, Timestamp(0), Timestamp::Max());
    if (!versions.empty()) {
      node_ids_.push_back(i);
    }
  }
  
  return true;
}
```

### Property Retrieval

```cpp
std::shared_ptr<Record> NodeScan::Next() {
  uint64_t node_id = node_ids_[current_index_++];
  
  // Create node
  Node node;
  node.id = node_id;
  node.labels = {*label_};
  
  // Fetch properties from storage
  if (context_->storage) {
    auto versions = context_->storage->ScanLimit(
        node_id, Timestamp(0), Timestamp::Max(), 100);
    
    for (const auto& [ts, desc] : versions) {
      uint16_t col_id = desc.GetColumnId();
      std::string name = context_->storage->GetPropertyName(col_id);
      node.properties[name] = DescriptorToValue(desc);
    }
  }
  
  return MakeRecord(node);
}
```

### Edge Traversal

```cpp
std::shared_ptr<Record> Expand::Next() {
  // Get source node
  auto src_record = GetSourceRecord();
  uint64_t src_id = src_record->GetNode().id;
  
  // Get neighbors
  std::vector<Neighbor> neighbors;
  if (context_->storage) {
    neighbors = context_->storage->GetNeighbors(
        src_id, direction_, edge_type_);
  }
  
  // Create records for each neighbor
  for (const auto& neighbor : neighbors) {
    auto record = CloneRecord(src_record);
    record->Set(dst_variable_, neighbor.node);
    record->Set(edge_variable_, neighbor.edge);
    result_buffer_.push_back(record);
  }
  
  return GetNextFromBuffer();
}
```

## Performance Optimization

### Predicate Pushdown

Push filters closer to data source:

```cpp
void PushDownPredicates(PhysicalOperator* root) {
  for (auto* child : root->GetChildren()) {
    if (auto* filter = dynamic_cast<Filter*>(child)) {
      if (auto* scan = dynamic_cast<NodeScan*>(filter->GetChild())) {
        // Push filter into scan
        scan->AddPredicate(filter->GetPredicate());
        filter->RemoveSelf();
      }
    }
  }
}
```

### Projection Pushdown

Only fetch needed columns:

```cpp
void PushDownProjections(PhysicalOperator* root) {
  std::set<std::string> required_columns;
  
  // Collect required columns from RETURN clause
  for (const auto& column : return_columns) {
    required_columns.insert(column);
  }
  
  // Push to scans
  for (auto* scan : GetScans(root)) {
    scan->SetRequiredColumns(required_columns);
  }
}
```

### Index Selection

Use indexes when available:

```cpp
bool CanUseIndex(const NodeScan& scan, const Expression& predicate) {
  if (auto* comp = dynamic_cast<const ComparisonExpr*>(&predicate)) {
    if (comp->op == EQ && IsPropertyAccess(*comp->left) && 
        IsLiteral(*comp->right)) {
      auto property = GetPropertyName(*comp->left);
      return HasIndex(scan.GetLabel(), property);
    }
  }
  return false;
}
```

## Error Handling

### Parse Errors

```cpp
ResultSet CypherEngine::Execute(const std::string& query) {
  auto plan = ParseAndPlan(query);
  if (!plan) {
    ResultSet result;
    result.SetError(last_error_);
    return result;
  }
  // ...
}
```

### Runtime Errors

```cpp
ResultSet ExecutionPlan::Execute(ExecutionContext* ctx) {
  try {
    // Execute plan
    auto result = ExecuteInternal(ctx);
    return result;
  } catch (const std::exception& e) {
    ResultSet result;
    result.SetError(std::string("Runtime error: ") + e.what());
    return result;
  }
}
```

### Validation Errors

```cpp
bool NodeScan::Init(ExecutionContext* ctx) {
  if (!ctx->storage) {
    // No storage available
    return false;
  }
  
  if (label_ && !LabelExists(*label_)) {
    // Label doesn't exist
    return false;
  }
  
  return true;
}
```

## Monitoring

### Query Statistics

```cpp
struct QueryStats {
  uint64_t rows_scanned;
  uint64_t rows_returned;
  uint64_t execution_time_us;
  uint32_t storage_accesses;
  uint32_t network_roundtrips;
};
```

### Metrics

- `cypher_queries_total`: Total queries executed
- `cypher_query_latency_us`: Query latency histogram
- `plan_cache_hits`: Plan cache hit count
- `plan_cache_misses`: Plan cache miss count

## Best Practices

1. **Use Indexes**: Create indexes for frequently queried properties
2. **Limit Results**: Use LIMIT to avoid large result sets
3. **Filter Early**: Push predicates as close to data source as possible
4. **Use Parameters**: Use parameterized queries for plan cache
5. **Monitor Performance**: Track query latency and cache hit rate
6. **Avoid Deep Traversals**: Limit variable-length path depth
7. **Use Labels**: Always specify labels when possible
8. **Batch Operations**: Use UNWIND for bulk operations
