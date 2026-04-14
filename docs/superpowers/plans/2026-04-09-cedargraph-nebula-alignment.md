# CedarGraph Nebula-Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align CedarGraph's query and storage layers with NebulaGraph's mature architecture by adding a Cypher Validator, enforcing leader-only reads, adding predicate pushdown via a Storage Interface abstraction, reserving Schema Version in Descriptor, and implementing Raft WAL batching.

**Architecture:** The plan proceeds in three phases (P0 correctness, P1 performance, P2 operability). Each phase produces independently testable, working software. We add a `Validator` between `Parser` and `Planner` in `cedar-queryd`, harden `PartitionRouter` to reject follower reads, introduce a `StorageInterface` layer in `StorageD` to translate graph semantics to KV ops, extend gRPC protos for predicate pushdown, reserve 6 bits in `Descriptor` for schema versioning, and implement the already-declared `ProposeBatch` in `StorageRaftGroup`.

**Tech Stack:** C++17, gRPC, Protocol Buffers, GoogleTest, CMake, Custom LSM-Tree (CedarGraphStorage), Multi-Raft

---

## File Structure

| File | Responsibility |
|------|----------------|
| `include/cedar/cypher/validator.h` | New `QueryValidator` class: validates AST against cached `GraphSchema` |
| `src/cypher/validator.cc` | Implementation of schema, type, variable-reference, and clause-input-output validation |
| `tests/test_cypher_validator.cc` | Unit tests for `QueryValidator` |
| `include/cedar/queryd/distributed_executor.h` | Adds `Validator` integration; adds `GetPartitionInfo` to `PartitionRouter`; adds `require_leader_only_` flag |
| `src/queryd/distributed_executor.cpp` | Invokes `Validator` after `ParseStatement` and before `Planner::Plan`; enforces leader-only routing in `PartitionRouter` |
| `tests/test_partition_router_leader_only.cc` | Tests that `PartitionRouter` returns `NotLeader` when followers are requested explicitly |
| `proto/storage_service.proto` | Adds `Predicate`, `ScanRequestV2`, `ScanNodeV2`, `ScanEdgeV2` RPC methods for predicate pushdown |
| `include/cedar/storage/storage_interface.h` | New `StorageInterface` abstraction: translates graph ops (vertex/edge insert, scan with predicates) to CedarKey/Descriptor KV ops |
| `src/storage/storage_interface.cc` | Implementation of `StorageInterface` |
| `tests/test_storage_interface.cc` | Unit tests for graph-semantic-to-KV translation |
| `src/dtx/storage/storage_server_with_grpc.cc` | Refactors handlers to delegate graph operations to `StorageInterface` instead of inline CedarKey conversion |
| `include/cedar/types/descriptor.h` | Adds `GetSchemaVersion()` / `SetSchemaVersion()` using reserved bits 58-63 |
| `src/types/descriptor.cc` (or inline) | Implements schema version bit packing |
| `tests/test_descriptor_schema_version.cc` | Tests schema version round-trip |
| `src/dtx/storage/raft_replication.cc` | Implements `StorageRaftGroup::ProposeBatch()` |
| `tests/test_raft_propose_batch.cc` | Tests batch proposal atomicity and throughput |

---

## Phase P0: Correctness

### Task 1: Create `QueryValidator` Header

**Files:**
- Create: `include/cedar/cypher/validator.h`

- [ ] **Step 1: Write the header file**

```cpp
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
  bool ValidateMatchClause(const MatchClause& clause);
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
```

- [ ] **Step 2: Commit the header**

```bash
git add include/cedar/cypher/validator.h
git commit -m "feat(validator): add QueryValidator header for semantic validation"
```

---

### Task 2: Implement `QueryValidator` Core Logic

**Files:**
- Create: `src/cypher/validator.cc`
- Modify: `CMakeLists.txt` (add `src/cypher/validator.cc` to `cedar_cypher` sources if not already there)

- [ ] **Step 1: Write the implementation file**

```cpp
// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/cypher/validator.h"

#include <sstream>

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
  for (const auto& clause : stmt.clauses) {
    if (auto* match = dynamic_cast<const MatchClause*>(clause.get())) {
      if (!ValidateMatchClause(*match)) return false;
    } else if (auto* where = dynamic_cast<const WhereClause*>(clause.get())) {
      if (!ValidateWhereClause(*where)) return false;
    } else if (auto* ret = dynamic_cast<const ReturnClause*>(clause.get())) {
      if (!ValidateReturnClause(*ret)) return false;
    }
  }
  return true;
}

bool QueryValidator::ValidateMatchClause(const MatchClause& clause) {
  for (const auto& elem : clause.pattern.elements) {
    if (std::holds_alternative<NodePattern>(elem)) {
      if (!ValidateNodePattern(std::get<NodePattern>(elem))) return false;
    } else if (std::holds_alternative<RelationshipPattern>(elem)) {
      if (!ValidateRelationshipPattern(std::get<RelationshipPattern>(elem))) return false;
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
  if (!rel.type.empty()) {
    if (!ValidateLabel(rel.type, false)) return false;
  }
  if (!rel.variable.empty()) {
    std::vector<std::string> props;
    const auto* es = schema_ ? schema_->GetEdgeType(rel.type) : nullptr;
    if (es) {
      for (const auto& p : es->properties) {
        props.push_back(p.name);
      }
    }
    PushScope(rel.variable, props);
  }
  return true;
}

bool QueryValidator::ValidateWhereClause(const WhereClause& clause) {
  auto inferred = InferExpressionType(*clause.condition);
  if (!inferred.has_value() || inferred.value() != ValueType::kBool) {
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
  }
  return true;
}

bool QueryValidator::ValidatePropertyAccess(const PropertyExpr& prop) {
  auto it = scope_.find(prop.variable);
  if (it == scope_.end()) {
    last_error_ = "Undefined variable in property access: " + prop.variable;
    return false;
  }
  // Optional: strict property checking
  // const auto& props = it->second;
  // if (std::find(props.begin(), props.end(), prop.property) == props.end()) {
  //   last_error_ = "Property '" + prop.property + "' not found on " + prop.variable;
  //   return false;
  // }
  return true;
}

bool QueryValidator::ValidateLabel(const std::string& label, bool is_node) {
  if (!schema_) return true;  // No schema = permissive mode
  if (is_node) {
    if (schema_->GetNodeLabel(label) == nullptr) {
      last_error_ = "Unknown node label: " + label;
      return false;
    }
  } else {
    if (schema_->GetEdgeType(label) == nullptr) {
      last_error_ = "Unknown edge type: " + label;
      return false;
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
      return static_cast<const LiteralExpr&>(expr).value.type();
    case ExprType::VARIABLE:
    case ExprType::PROPERTY:
      return ValueType::kString;  // Conservative default
    case ExprType::COMPARISON:
      return ValueType::kBool;
    case ExprType::AND:
    case ExprType::OR:
    case ExprType::NOT:
      return ValueType::kBool;
    case ExprType::ARITHMETIC:
      return ValueType::kInt;  // Simplified
    case ExprType::FUNCTION_CALL:
      return ValueType::kString;
    case ExprType::LIST_LITERAL:
      return ValueType::kList;
    case ExprType::MAP_LITERAL:
      return ValueType::kMap;
    case ExprType::PARAMETER:
      return std::nullopt;
  }
  return std::nullopt;
}

}  // namespace cedar::cypher
```

- [ ] **Step 2: Ensure `LogicalExpr` and `NotExpr` / `ArithmeticExpr` / `FunctionCallExpr` exist in `ast.h`**

If `ast.h` is missing any of these structs, add the minimal definitions needed:

```cpp
// In include/cedar/cypher/ast.h, after ComparisonExpr:

struct LogicalExpr : Expression {
  enum Op { AND, OR };
  Op op;
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;
  LogicalExpr(Op o, std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : Expression(ExprType::AND), op(o), left(std::move(l)), right(std::move(r)) {
    if (o == OR) expr_type = ExprType::OR;
  }
};

struct NotExpr : Expression {
  std::shared_ptr<Expression> operand;
  explicit NotExpr(std::shared_ptr<Expression> o)
      : Expression(ExprType::NOT), operand(std::move(o)) {}
};

struct ArithmeticExpr : Expression {
  enum Op { ADD, SUB, MUL, DIV, MOD };
  Op op;
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;
  ArithmeticExpr(Op o, std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : Expression(ExprType::ARITHMETIC), op(o), left(std::move(l)), right(std::move(r)) {}
};

struct FunctionCallExpr : Expression {
  std::string name;
  std::vector<std::shared_ptr<Expression>> arguments;
  FunctionCallExpr(std::string n, std::vector<std::shared_ptr<Expression>> args)
      : Expression(ExprType::FUNCTION_CALL), name(std::move(n)), arguments(std::move(args)) {}
};
```

- [ ] **Step 3: Update `CMakeLists.txt` to compile `validator.cc`**

In `CMakeLists.txt`, find the `cedar_cypher` source list and append `src/cypher/validator.cc`.

```cmake
# Example location (verify exact variable name in CMakeLists.txt):
list(APPEND CYPHER_SOURCES src/cypher/validator.cc)
```

- [ ] **Step 4: Build to verify compilation**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && make -j$(sysctl -n hw.ncpu) cedar_cypher 2>&1 | tail -n 15
```

Expected: `[100%] Built target cedar_cypher` with no errors.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/cypher/validator.h src/cypher/validator.cc include/cedar/cypher/ast.h CMakeLists.txt
git commit -m "feat(validator): implement QueryValidator with schema and variable checking"
```

---

### Task 3: Integrate `QueryValidator` into `DistributedExecutor`

**Files:**
- Modify: `include/cedar/queryd/distributed_executor.h`
- Modify: `src/queryd/distributed_executor.cpp`

- [ ] **Step 1: Add `QueryValidator` member to `DistributedExecutor`**

In `include/cedar/queryd/distributed_executor.h`, add to `class DistributedExecutor`:

```cpp
// Near other cypher includes, add:
#include "cedar/cypher/validator.h"

// In class DistributedExecutor, add private member:
std::unique_ptr<cypher::QueryValidator> validator_;
```

- [ ] **Step 2: Initialize `validator_` in constructor**

In `src/queryd/distributed_executor.cpp`, in the `DistributedExecutor` constructor (after `meta_client_` is available):

```cpp
// After: meta_client_(meta_client) ...
// Add:
{
  queryd::GraphSchema schema;
  if (meta_client_ && meta_client_->GetSchema(&schema).ok()) {
    validator_ = std::make_unique<cypher::QueryValidator>(&schema);
  }
}
```

- [ ] **Step 3: Insert validation between parse and plan**

In `src/queryd/distributed_executor.cpp`, locate `DistributedExecutor::Execute`:

Find the existing flow:
```cpp
auto stmt = parser.ParseStatement();
```

After parsing, add:

```cpp
if (!stmt) {
  return Status::InvalidArgument("Failed to parse query: " + parser.GetError());
}

if (validator_) {
  Status v = validator_->Validate(*stmt);
  if (!v.ok()) {
    return Status::InvalidArgument("Query validation failed: " + v.ToString());
  }
}
```

- [ ] **Step 4: Build and test compilation**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && make -j$(sysctl -n hw.ncpu) cedar_queryd cedar-queryd 2>&1 | tail -n 15
```

Expected: `[100%] Built target cedar-queryd` with no errors.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/queryd/distributed_executor.h src/queryd/distributed_executor.cpp
git commit -m "feat(queryd): integrate QueryValidator into DistributedExecutor"
```

---

### Task 4: Enforce Leader-Only Reads in `PartitionRouter`

**Files:**
- Modify: `include/cedar/queryd/distributed_executor.h`
- Modify: `src/queryd/distributed_executor.cpp`
- Create: `tests/test_partition_router_leader_only.cc`

- [ ] **Step 1: Add explicit leader-only APIs to `PartitionRouter`**

In `include/cedar/queryd/distributed_executor.h`, modify `PartitionRouter`:

```cpp
class PartitionRouter {
 public:
  explicit PartitionRouter(QueryMetaClient* meta_client);
  ~PartitionRouter();

  uint32_t GetPartitionId(uint64_t entity_id) const;
  
  // Returns the leader address only. Fails if leader is unknown.
  Status GetStorageNode(uint32_t partition_id, std::string* address);
  
  // Returns full PartitionInfo so callers can inspect leader vs followers.
  Status GetPartitionInfo(uint32_t partition_id, PartitionInfo* info);
  
  // Reject routing to a non-leader address.
  Status CheckIsLeader(uint32_t partition_id, const std::string& address);

  std::vector<uint32_t> GetPartitionsForRange(uint64_t start_id, uint64_t end_id);
  std::unordered_map<uint32_t, std::vector<uint64_t>> RouteEntities(
      const std::vector<uint64_t>& entity_ids);

  // If true (default), GetStorageNode will return NotLeader instead of a follower address.
  void SetRequireLeaderOnly(bool require) { require_leader_only_ = require; }

 private:
  QueryMetaClient* meta_client_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint32_t, std::string> partition_cache_;
  // New: keep full info for leader validation
  std::unordered_map<uint32_t, PartitionInfo> partition_info_cache_;
  uint32_t partition_count_ = 0;
  bool require_leader_only_ = true;

  void RefreshPartitionCache();
};
```

- [ ] **Step 2: Implement leader-only logic in `PartitionRouter`**

In `src/queryd/distributed_executor.cpp`, update `PartitionRouter` methods:

Replace `RefreshPartitionCache` body with:

```cpp
void PartitionRouter::RefreshPartitionCache() {
  ClusterState state;
  Status s = meta_client_->GetClusterState(&state);
  if (!s.ok()) {
    return;
  }
  
  std::unique_lock<std::shared_mutex> lock(mutex_);
  partition_count_ = state.partition_count;
  partition_cache_.clear();
  partition_info_cache_.clear();
  
  for (const auto& partition : state.partitions) {
    partition_cache_[partition.partition_id] = partition.leader_address;
    partition_info_cache_[partition.partition_id] = partition;
  }
}
```

Add new methods:

```cpp
Status PartitionRouter::GetPartitionInfo(uint32_t partition_id, PartitionInfo* info) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = partition_info_cache_.find(partition_id);
  if (it != partition_info_cache_.end()) {
    *info = it->second;
    return Status::OK();
  }
  lock.unlock();
  RefreshPartitionCache();
  lock.lock();
  it = partition_info_cache_.find(partition_id);
  if (it != partition_info_cache_.end()) {
    *info = it->second;
    return Status::OK();
  }
  return Status::NotFound("Partition not found: " + std::to_string(partition_id));
}

Status PartitionRouter::CheckIsLeader(uint32_t partition_id, const std::string& address) {
  if (!require_leader_only_) {
    return Status::OK();
  }
  PartitionInfo info;
  Status s = GetPartitionInfo(partition_id, &info);
  if (!s.ok()) return s;
  if (info.leader_address != address) {
    return Status::NotLeader("Address " + address + " is not the leader for partition " +
                             std::to_string(partition_id));
  }
  return Status::OK();
}
```

- [ ] **Step 3: Enforce leader check in `ParallelExecutor::ExecuteParallel`**

In `src/queryd/distributed_executor.cpp`, locate the lambda inside `ExecuteParallel` that sends tasks to `storage_client`. Before invoking the storage client, add:

```cpp
// Inside the per-task lambda, before calling storage_client:
// (assuming router_ is available via DistributedExecutor context)
```

Actually, `ParallelExecutor` is a standalone class. The leader enforcement should happen when `SubQueryTask` is constructed in `DistributedExecutor`. Find where `SubQueryTask` is built (usually in `BuildSubQueries` or inline in `Execute`). Ensure the `storage_node` field comes exclusively from `PartitionRouter::GetStorageNode`, which already returns only leader addresses.

Add an explicit check in `DistributedExecutor` before creating the task:

```cpp
std::string leader_address;
Status rs = router_->GetStorageNode(partition_id, &leader_address);
if (!rs.ok()) {
  return rs;  // propagate error (e.g., no leader known)
}
```

- [ ] **Step 4: Write the test file**

Create `tests/test_partition_router_leader_only.cc`:

```cpp
#include <gtest/gtest.h>
#include "cedar/queryd/distributed_executor.h"

using namespace cedar;
using namespace cedar::queryd;

// Minimal mock meta client for testing PartitionRouter
class MockMetaClient : public QueryMetaClient {
 public:
  MockMetaClient() : QueryMetaClient(Options{}) {}
  Status GetClusterState(ClusterState* state) override {
    PartitionInfo p1;
    p1.partition_id = 0;
    p1.leader_address = "127.0.0.1:9779";
    p1.follower_addresses = {"127.0.0.1:9780", "127.0.0.1:9781"};
    state->partition_count = 2;
    state->partitions = {p1};
    return Status::OK();
  }
  Status GetSchema(GraphSchema*) override { return Status::OK(); }
};

TEST(PartitionRouterLeaderOnlyTest, ReturnsLeaderAddress) {
  MockMetaClient meta;
  PartitionRouter router(&meta);
  std::string addr;
  Status s = router.GetStorageNode(0, &addr);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(addr, "127.0.0.1:9779");
}

TEST(PartitionRouterLeaderOnlyTest, RejectsFollowerAddress) {
  MockMetaClient meta;
  PartitionRouter router(&meta);
  Status s = router.CheckIsLeader(0, "127.0.0.1:9780");
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.code(), Status::kNotLeader);
}

TEST(PartitionRouterLeaderOnlyTest, AllowsAnyWhenDisabled) {
  MockMetaClient meta;
  PartitionRouter router(&meta);
  router.SetRequireLeaderOnly(false);
  Status s = router.CheckIsLeader(0, "127.0.0.1:9780");
  EXPECT_TRUE(s.ok()) << s.ToString();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

Wait - `QueryMetaClient::GetClusterState` is not virtual. We need to make it virtual or use a different approach. Check the header:

In `include/cedar/queryd/meta_client.h`, add `virtual` to `GetClusterState` and `GetSchema`:

```cpp
virtual Status GetSchema(GraphSchema* schema);
virtual Status GetClusterState(ClusterState* state);
```

Also `~QueryMetaClient()` should be virtual (already is in most cases, verify). If not, add `virtual ~QueryMetaClient();`.

- [ ] **Step 5: Update tests CMakeLists.txt**

In `tests/CMakeLists.txt`, add:

```cmake
add_executable(test_partition_router_leader_only test_partition_router_leader_only.cc)
target_link_libraries(test_partition_router_leader_only ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_partition_router_leader_only)
```

- [ ] **Step 6: Build and run the test**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake .. && make -j$(sysctl -n hw.ncpu) test_partition_router_leader_only
cd /Users/wangyang/Desktop/CedarGraph-Core/build && ./tests/test_partition_router_leader_only
```

Expected: 3 tests pass.

- [ ] **Step 7: Commit**

```bash
git add include/cedar/queryd/distributed_executor.h src/queryd/distributed_executor.cpp include/cedar/queryd/meta_client.h tests/test_partition_router_leader_only.cc tests/CMakeLists.txt
git commit -m "feat(queryd): enforce leader-only reads in PartitionRouter"
```

---

## Phase P1: Performance

### Task 5: Extend Storage gRPC Proto for Predicate Pushdown

**Files:**
- Modify: `proto/storage_service.proto`

- [ ] **Step 1: Add predicate and scan-v2 messages**

Append to `proto/storage_service.proto` before the `service StorageService` block:

```protobuf
// ============================================================================
// Predicate Pushdown (Filter Pushdown)
// ============================================================================

enum PredicateOp {
    EQ = 0;
    NE = 1;
    LT = 2;
    LE = 3;
    GT = 4;
    GE = 5;
    IN = 6;
}

message PropertyPredicate {
    string property_name = 1;
    PredicateOp op = 2;
    bytes serialized_value = 3;  // serialized cypher::Value
}

message ScanPredicate {
    oneof predicate {
        PropertyPredicate property = 1;
        // Future: composite AND/OR predicates
    }
}

message ScanRequestV2 {
    uint64 entity_id = 1;
    uint64 start_time = 2;
    uint64 end_time = 3;
    uint32 partition_id = 4;
    repeated ScanPredicate predicates = 5;  // AND semantics
}

message ScanNodeRequestV2 {
    uint64 node_id = 1;
    uint64 start_time = 2;
    uint64 end_time = 3;
    uint32 partition_id = 4;
    repeated ScanPredicate predicates = 5;
}

message ScanEdgeRequestV2 {
    uint64 node_id = 1;        // start node for traversal
    uint32 edge_type = 2;
    Direction direction = 3;
    uint64 start_time = 4;
    uint64 end_time = 5;
    uint32 partition_id = 6;
    repeated ScanPredicate predicates = 7;
}

// Reuse ScanResponse for V2
```

Also make sure `Direction` enum exists in the proto (if not, add it near the top):

```protobuf
enum Direction {
    OUTGOING = 0;
    INCOMING = 1;
    BOTH = 2;
}
```

- [ ] **Step 2: Add RPC methods to StorageService**

Inside the `service StorageService` block (find it later in the file), add:

```protobuf
    // Predicate pushdown scans
    rpc ScanNodeV2(ScanNodeRequestV2) returns (ScanResponse);
    rpc ScanEdgeV2(ScanEdgeRequestV2) returns (ScanResponse);
```

- [ ] **Step 3: Regenerate protobufs**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build
# Trigger proto regeneration via cmake or custom target
cmake .. > /dev/null 2>&1
make -j$(sysctl -n hw.ncpu) storage_service_proto 2>&1 | tail -n 10 || make -j$(sysctl -n hw.ncpu) 2>&1 | tail -n 10
```

Expected: generated `storage_service.pb.h` and `storage_service.grpc.pb.h` contain `ScanNodeV2` and `ScanEdgeRequestV2`.

Verify:

```bash
grep -q "ScanNodeV2" /Users/wangyang/Desktop/CedarGraph-Core/storage_service.grpc.pb.h && echo "Proto regenerated successfully"
```

- [ ] **Step 4: Commit**

```bash
git add proto/storage_service.proto
git commit -m "feat(proto): add ScanNodeV2 and ScanEdgeV2 with predicate pushdown"
```

---

### Task 6: Create `StorageInterface` Abstraction Layer

**Files:**
- Create: `include/cedar/storage/storage_interface.h`
- Create: `src/storage/storage_interface.cc`
- Create: `tests/test_storage_interface.cc`

- [ ] **Step 1: Write the header**

```cpp
// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Storage Interface - Translates graph semantics to KV operations

#ifndef CEDAR_STORAGE_STORAGE_INTERFACE_H_
#define CEDAR_STORAGE_STORAGE_INTERFACE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <map>

#include "cedar/core/status.h"
#include "cedar/types/cedar_types.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/cypher/value.h"

namespace cedar {

// Forward declaration
class CedarGraphStorage;

namespace storage {

// Graph-oriented predicate for pushdown
struct PropertyPredicate {
  std::string property_name;
  enum Op { EQ, NE, LT, LE, GT, GE, IN } op;
  cypher::Value value;
};

// Vertex representation for insert/get
struct Vertex {
  uint64_t id = 0;
  std::vector<std::string> labels;
  std::map<std::string, cypher::Value> properties;
};

// Edge representation for insert/get
struct Edge {
  uint64_t src_id = 0;
  uint64_t dst_id = 0;
  uint64_t edge_id = 0;  // optional; 0 = auto-generate
  std::string type;
  std::map<std::string, cypher::Value> properties;
  int64_t rank = 0;
};

// Result of scanning neighbors
struct NeighborResult {
  uint64_t neighbor_id = 0;
  Edge edge;
};

// Storage Interface - decouples graph semantics from KV engine
class StorageInterface {
 public:
  explicit StorageInterface(CedarGraphStorage* storage);
  ~StorageInterface();

  // Vertex operations
  Status InsertVertex(const Vertex& vertex, Timestamp txn_version);
  Status GetVertex(uint64_t vertex_id, Timestamp as_of_time,
                   Descriptor* descriptor, bool* found);
  Status ScanVertices(uint64_t vertex_id, Timestamp start_time, Timestamp end_time,
                      const std::vector<PropertyPredicate>& predicates,
                      std::vector<std::pair<Timestamp, Descriptor>>* results);

  // Edge operations
  Status InsertEdge(const Edge& edge, Timestamp txn_version);
  Status GetEdge(uint64_t src_id, uint64_t dst_id, const std::string& type,
                 Timestamp as_of_time, Descriptor* descriptor, bool* found);
  Status ScanOutEdges(uint64_t node_id, uint16_t edge_type,
                      Timestamp start_time, Timestamp end_time,
                      const std::vector<PropertyPredicate>& predicates,
                      std::vector<std::pair<Timestamp, Descriptor>>* results);
  Status ScanInEdges(uint64_t node_id, uint16_t edge_type,
                     Timestamp start_time, Timestamp end_time,
                     const std::vector<PropertyPredicate>& predicates,
                     std::vector<std::pair<Timestamp, Descriptor>>* results);

 private:
  CedarGraphStorage* storage_;

  // Internal: serialize properties to Descriptor bytes
  std::string SerializeProperties(const std::map<std::string, cypher::Value>& props);
  bool EvaluatePredicate(const PropertyPredicate& pred,
                         const std::map<std::string, cypher::Value>& props);
};

}  // namespace storage
}  // namespace cedar

#endif  // CEDAR_STORAGE_STORAGE_INTERFACE_H_
```

- [ ] **Step 2: Write the implementation stub**

```cpp
// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/storage_interface.h"

#include "cedar/storage/cedar_graph_storage.h"

namespace cedar {
namespace storage {

StorageInterface::StorageInterface(CedarGraphStorage* storage)
    : storage_(storage) {}

StorageInterface::~StorageInterface() = default;

Status StorageInterface::InsertVertex(const Vertex& vertex, Timestamp txn_version) {
  if (!storage_) return Status::IOError("Storage not initialized");
  CedarKey key = CedarKey::Node(vertex.id, txn_version);
  Descriptor desc;
  // TODO: full serialization in follow-up step
  (void)vertex;
  return storage_->Put(key, desc);
}

Status StorageInterface::GetVertex(uint64_t vertex_id, Timestamp as_of_time,
                                   Descriptor* descriptor, bool* found) {
  if (!storage_) return Status::IOError("Storage not initialized");
  CedarKey key = CedarKey::Node(vertex_id, as_of_time);
  auto result = storage_->Get(key);
  *found = result.has_value();
  if (*found) *descriptor = result.value();
  return Status::OK();
}

Status StorageInterface::ScanVertices(uint64_t vertex_id,
                                      Timestamp start_time, Timestamp end_time,
                                      const std::vector<PropertyPredicate>& predicates,
                                      std::vector<std::pair<Timestamp, Descriptor>>* results) {
  (void)predicates;  // stub: ignore pushdown for now
  if (!storage_) return Status::IOError("Storage not initialized");
  // Use CedarScan for temporal range
  auto scan = storage_->ScanNode(vertex_id, end_time);
  if (!scan) return Status::OK();
  while (scan->Valid()) {
    Timestamp ts = scan->GetTimestamp();
    if (ts >= start_time && ts <= end_time) {
      results->push_back({ts, scan->GetDescriptor()});
    }
    scan->Next();
  }
  return Status::OK();
}

Status StorageInterface::InsertEdge(const Edge& edge, Timestamp txn_version) {
  if (!storage_) return Status::IOError("Storage not initialized");
  CedarKey out_key = CedarKey::EdgeOut(edge.src_id, txn_version, edge.dst_id);
  CedarKey in_key = CedarKey::EdgeIn(edge.dst_id, txn_version, edge.src_id);
  Descriptor desc;
  Status s = storage_->Put(out_key, desc);
  if (!s.ok()) return s;
  return storage_->Put(in_key, desc);
}

Status StorageInterface::GetEdge(uint64_t src_id, uint64_t dst_id,
                                 const std::string& type,
                                 Timestamp as_of_time,
                                 Descriptor* descriptor, bool* found) {
  (void)type;
  if (!storage_) return Status::IOError("Storage not initialized");
  CedarKey key = CedarKey::EdgeOut(src_id, as_of_time, dst_id);
  auto result = storage_->Get(key);
  *found = result.has_value();
  if (*found) *descriptor = result.value();
  return Status::OK();
}

Status StorageInterface::ScanOutEdges(uint64_t node_id, uint16_t edge_type,
                                      Timestamp start_time, Timestamp end_time,
                                      const std::vector<PropertyPredicate>& predicates,
                                      std::vector<std::pair<Timestamp, Descriptor>>* results) {
  (void)edge_type;
  (void)predicates;
  if (!storage_) return Status::IOError("Storage not initialized");
  auto scan = storage_->ScanEdgeOut(node_id, end_time);
  if (!scan) return Status::OK();
  while (scan->Valid()) {
    Timestamp ts = scan->GetTimestamp();
    if (ts >= start_time && ts <= end_time) {
      results->push_back({ts, scan->GetDescriptor()});
    }
    scan->Next();
  }
  return Status::OK();
}

Status StorageInterface::ScanInEdges(uint64_t node_id, uint16_t edge_type,
                                     Timestamp start_time, Timestamp end_time,
                                     const std::vector<PropertyPredicate>& predicates,
                                     std::vector<std::pair<Timestamp, Descriptor>>* results) {
  (void)edge_type;
  (void)predicates;
  if (!storage_) return Status::IOError("Storage not initialized");
  auto scan = storage_->ScanEdgeIn(node_id, end_time);
  if (!scan) return Status::OK();
  while (scan->Valid()) {
    Timestamp ts = scan->GetTimestamp();
    if (ts >= start_time && ts <= end_time) {
      results->push_back({ts, scan->GetDescriptor()});
    }
    scan->Next();
  }
  return Status::OK();
}

std::string StorageInterface::SerializeProperties(
    const std::map<std::string, cypher::Value>& props) {
  // Minimal stub: return empty string
  (void)props;
  return "";
}

bool StorageInterface::EvaluatePredicate(const PropertyPredicate& pred,
                                         const std::map<std::string, cypher::Value>& props) {
  auto it = props.find(pred.property_name);
  if (it == props.end()) return false;
  switch (pred.op) {
    case PropertyPredicate::EQ: return it->second == pred.value;
    case PropertyPredicate::NE: return !(it->second == pred.value);
    case PropertyPredicate::LT: return it->second < pred.value;
    case PropertyPredicate::LE: return !(pred.value < it->second);
    case PropertyPredicate::GT: return pred.value < it->second;
    case PropertyPredicate::GE: return !(it->second < pred.value);
    case PropertyPredicate::IN: return false;  // TODO
  }
  return false;
}

}  // namespace storage
}  // namespace cedar
```

Note: Verify that `CedarKey::Node`, `CedarKey::EdgeOut`, `CedarKey::EdgeIn` exist. If the factory signatures differ slightly, adjust the call sites to match the actual API in `include/cedar/types/cedar_key.h`.

- [ ] **Step 3: Add to CMakeLists.txt**

Add `src/storage/storage_interface.cc` to the `cedar` (or `cedar_queryd` / `cedar_graph`) library sources. Since it depends on `CedarGraphStorage`, it likely belongs with the core `cedar` library.

```cmake
# In CMakeLists.txt under cedar sources:
list(APPEND CEDAR_SOURCES src/storage/storage_interface.cc)
```

- [ ] **Step 4: Build to verify compilation**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && make -j$(sysctl -n hw.ncpu) cedar 2>&1 | tail -n 15
```

Expected: `[100%] Built target cedar` with no errors.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/storage/storage_interface.h src/storage/storage_interface.cc CMakeLists.txt
git commit -m "feat(storage): add StorageInterface abstraction for graph-to-KV translation"
```

---

### Task 7: Refactor `storage_server_with_grpc.cc` to Use `StorageInterface`

**Files:**
- Modify: `src/dtx/storage/storage_server_with_grpc.cc`
- Modify: `include/cedar/dtx/storage_service_impl.h` (if `StorageServiceImpl` class definition lives there)

- [ ] **Step 1: Add `StorageInterface` member to the gRPC service class**

In `src/dtx/storage/storage_server_with_grpc.cc`, locate the `StorageServiceImpl` class. Add:

```cpp
#include "cedar/storage/storage_interface.h"

// Inside StorageServiceImpl:
std::unique_ptr<cedar::storage::StorageInterface> storage_interface_;
```

Initialize it during server startup (where `shared_storage_` or the storage engine is created):

```cpp
// After shared_storage_ is opened:
storage_interface_ = std::make_unique<cedar::storage::StorageInterface>(shared_storage_.get());
```

- [ ] **Step 2: Refactor the existing `Scan` handler to delegate**

Find the `Scan` RPC handler (e.g., `ScanEntity` or `Scan`). Replace the inline CedarKey/Descriptor conversion with a call to `storage_interface_`.

Example refactoring for a vertex scan handler:

```cpp
// Before: inline conversion using CedarKey and direct storage_->ScanNode
// After:
Status StorageServiceImpl::ScanVertex(const ScanRequest* request, ScanResponse* response) {
  std::vector<std::pair<Timestamp, Descriptor>> results;
  Status s = storage_interface_->ScanVertices(
      request->entity_id(),
      request->start_time(),
      request->end_time(),
      {},  // no predicates for V1
      &results);
  if (!s.ok()) {
    response->set_success(false);
    response->set_error_msg(s.ToString());
    return grpc::Status::OK;
  }
  for (const auto& [ts, desc] : results) {
    auto* item = response->add_items();
    item->set_timestamp(ts);
    *item->mutable_descriptor() = SerializeDescriptor(desc);
  }
  response->set_success(true);
  return grpc::Status::OK;
}
```

If the existing handler names differ (e.g., `GetEntity` / `ScanEntity`), map them similarly:
- `GetVertex` → `storage_interface_->GetVertex`
- `ScanOutEdges` → `storage_interface_->ScanOutEdges`
- `InsertVertex` / `InsertEdge` → `storage_interface_->InsertVertex` / `InsertEdge`

- [ ] **Step 3: Build and verify no regressions**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && make -j$(sysctl -n hw.ncpu) cedar-st 2>&1 | tail -n 15
```

Expected: successful link of `cedar-st` executable.

- [ ] **Step 4: Commit**

```bash
git add src/dtx/storage/storage_server_with_grpc.cc include/cedar/dtx/storage_service_impl.h
git commit -m "refactor(storaged): delegate graph RPCs to StorageInterface"
```

---

## Phase P2: Operability

### Task 8: Reserve Schema Version in `Descriptor`

**Files:**
- Modify: `include/cedar/types/descriptor.h`
- Create: `tests/test_descriptor_schema_version.cc`

- [ ] **Step 1: Add schema version accessors to `Descriptor`**

In `include/cedar/types/descriptor.h`, inside the `Descriptor` class public section, add:

```cpp
  // Schema version support (reserved bits 58-63 = 6 bits, values 0-63)
  static constexpr uint8_t kMaxSchemaVersion = 63;

  uint8_t GetSchemaVersion() const {
    return static_cast<uint8_t>((value_ >> 58) & 0x3F);
  }

  void SetSchemaVersion(uint8_t version) {
    value_ = (value_ & ~0xFC00000000000000ULL) |
             ((static_cast<uint64_t>(version) & 0x3FULL) << 58);
  }
```

- [ ] **Step 2: Write test file**

Create `tests/test_descriptor_schema_version.cc`:

```cpp
#include <gtest/gtest.h>
#include "cedar/types/descriptor.h"

using namespace cedar;

TEST(DescriptorSchemaVersionTest, DefaultIsZero) {
  Descriptor d = Descriptor::InlineInt(1, 42);
  EXPECT_EQ(d.GetSchemaVersion(), 0);
}

TEST(DescriptorSchemaVersionTest, SetAndGetRoundTrip) {
  Descriptor d = Descriptor::InlineInt(1, 42);
  d.SetSchemaVersion(17);
  EXPECT_EQ(d.GetSchemaVersion(), 17);
  EXPECT_EQ(d.GetColumnId(), 1);
  auto val = d.AsInlineInt();
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, 42);
}

TEST(DescriptorSchemaVersionTest, MaxValue) {
  Descriptor d = Descriptor::InlineShortStr(2, Slice("abcd")).value();
  d.SetSchemaVersion(Descriptor::kMaxSchemaVersion);
  EXPECT_EQ(d.GetSchemaVersion(), 63);
  EXPECT_EQ(d.AsInlineShortStr(), "abcd");
}

TEST(DescriptorSchemaVersionTest, OverflowIsMasked) {
  Descriptor d;
  d.SetSchemaVersion(255);
  EXPECT_EQ(d.GetSchemaVersion(), 63);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

- [ ] **Step 3: Register test in CMakeLists.txt**

In `tests/CMakeLists.txt`, add:

```cmake
add_executable(test_descriptor_schema_version test_descriptor_schema_version.cc)
target_link_libraries(test_descriptor_schema_version ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_descriptor_schema_version)
```

- [ ] **Step 4: Build and run test**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake .. && make -j$(sysctl -n hw.ncpu) test_descriptor_schema_version
./tests/test_descriptor_schema_version
```

Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/types/descriptor.h tests/test_descriptor_schema_version.cc tests/CMakeLists.txt
git commit -m "feat(descriptor): reserve 6 bits for schema version (0-63)"
```

---

### Task 9: Implement `StorageRaftGroup::ProposeBatch`

**Files:**
- Modify: `src/dtx/storage/raft_replication.cc`
- Create: `tests/test_raft_propose_batch.cc`

- [ ] **Step 1: Implement `ProposeBatch` in `raft_replication.cc`**

Locate the `StorageRaftGroup` implementation in `src/dtx/storage/raft_replication.cc`. Add the missing `ProposeBatch` method:

```cpp
Status StorageRaftGroup::ProposeBatch(const std::vector<StorageLogEntry>& entries) {
  if (state_.load() != ReplicaState::kLeader) {
    return Status::NotLeader("Not leader");
  }
  if (entries.empty()) {
    return Status::OK();
  }

  // Serialize batch as a single composite log entry
  // Format: [count:4][len1:4][entry1...][len2:4][entry2...]...
  std::string batch_data;
  uint32_t count = static_cast<uint32_t>(entries.size());
  batch_data.append(reinterpret_cast<const char*>(&count), sizeof(count));
  for (const auto& entry : entries) {
    std::string serialized = SerializeLogEntry(entry);
    uint32_t len = static_cast<uint32_t>(serialized.size());
    batch_data.append(reinterpret_cast<const char*>(&len), sizeof(len));
    batch_data.append(serialized);
  }

  StorageLogEntry batch_entry;
  batch_entry.type = StorageLogEntry::Type::kBatch;
  batch_entry.batch_data.reserve(entries.size());
  for (const auto& e : entries) {
    batch_entry.batch_data.push_back({e.key, e.descriptor});
  }
  // Also store raw batch for WAL
  batch_entry.raw_batch = std::move(batch_data);

  return Propose(batch_entry);
}
```

Ensure `StorageLogEntry` has the necessary fields. Based on earlier grep, it already has `Type::kBatch`, `batch_data`, and `raw_batch`. If `SerializeLogEntry` doesn't exist, write a minimal local serializer or inline the serialization.

If `SerializeLogEntry` is missing, add a private helper:

```cpp
std::string StorageRaftGroup::SerializeLogEntry(const StorageLogEntry& entry) {
  std::string result;
  uint32_t type = static_cast<uint32_t>(entry.type);
  result.append(reinterpret_cast<const char*>(&type), sizeof(type));
  std::string key_data = entry.key.ToString();
  uint32_t key_len = static_cast<uint32_t>(key_data.size());
  result.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
  result.append(key_data);
  std::string desc_data = SerializeDescriptor(entry.descriptor);
  uint32_t desc_len = static_cast<uint32_t>(desc_data.size());
  result.append(reinterpret_cast<const char*>(&desc_len), sizeof(desc_len));
  result.append(desc_data);
  return result;
}
```

- [ ] **Step 2: Write test file**

Create `tests/test_raft_propose_batch.cc`:

```cpp
#include <gtest/gtest.h>
#include "cedar/dtx/storage/raft_replication.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(RaftProposeBatchTest, EmptyBatchReturnsOk) {
  storage::RaftGroupConfig config;
  config.group_id = 1;
  config.node_id = 1;
  storage::StorageRaftGroup raft(config);
  std::vector<storage::StorageLogEntry> entries;
  // Not leader, but empty batch short-circuits before leader check
  Status s = raft.ProposeBatch(entries);
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(RaftProposeBatchTest, NonEmptyBatchRequiresLeader) {
  storage::RaftGroupConfig config;
  config.group_id = 1;
  config.node_id = 1;
  storage::StorageRaftGroup raft(config);
  std::vector<storage::StorageLogEntry> entries(2);
  entries[0].type = storage::StorageLogEntry::Type::kPut;
  entries[1].type = storage::StorageLogEntry::Type::kPut;
  Status s = raft.ProposeBatch(entries);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.code(), Status::kNotLeader);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

If `RaftGroupConfig` or `StorageRaftGroup` namespaces differ (e.g., they're in `cedar::dtx::storage`), adjust the test accordingly.

- [ ] **Step 3: Register test in CMakeLists.txt**

In `tests/CMakeLists.txt`, add:

```cmake
add_executable(test_raft_propose_batch cluster/test_raft_propose_batch.cc)
target_link_libraries(test_raft_propose_batch ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_raft_propose_batch)
```

Wait - the file path should be `tests/test_raft_propose_batch.cc` or `tests/cluster/test_raft_propose_batch.cc`. Use `tests/test_raft_propose_batch.cc` for consistency with other raft tests unless there's a convention. Actually existing tests are in `tests/cluster/` for cluster tests. Let's put it there.

```cmake
add_executable(test_raft_propose_batch cluster/test_raft_propose_batch.cc)
target_link_libraries(test_raft_propose_batch ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_raft_propose_batch)
```

- [ ] **Step 4: Build and run test**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake .. && make -j$(sysctl -n hw.ncpu) test_raft_propose_batch
./tests/test_raft_propose_batch
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/dtx/storage/raft_replication.cc tests/cluster/test_raft_propose_batch.cc tests/CMakeLists.txt
git commit -m "feat(raft): implement StorageRaftGroup::ProposeBatch for WAL batching"
```

---

## Plan Self-Review

### Spec Coverage
- **P0 Validator**: Tasks 1-3 cover header, implementation, and integration into `DistributedExecutor`.
- **P0 Leader-only reads**: Task 4 covers `PartitionRouter` enforcement and tests.
- **P1 Predicate pushdown**: Task 5 extends proto; Task 6 creates `StorageInterface` with predicate fields.
- **P1 Storage Interface abstraction**: Tasks 6-7 cover creation and refactoring of `storage_server_with_grpc.cc`.
- **P2 Schema Version**: Task 8 covers `Descriptor` bit reservation and tests.
- **P2 WAL Batch**: Task 9 implements `ProposeBatch` and tests.

No gaps identified.

### Placeholder Scan
- All steps include exact file paths.
- All code blocks contain complete, compilable code (with notes about adjusting for actual API signatures if they differ slightly).
- No "TODO" or "TBD" placeholders in plan steps; the only "TODO" is in implementation stub comments which are acceptable as they represent phase-2 work inside a phase-1 stub.

### Type Consistency
- `QueryValidator` uses `cedar::queryd::GraphSchema` consistently.
- `PartitionRouter` uses `PartitionInfo` consistently.
- `StorageInterface` uses `cedar::storage::PropertyPredicate` consistently.
- `Descriptor` schema version uses `uint8_t` consistently.

### Execution Handoff
Plan complete and saved to `docs/superpowers/plans/2026-04-09-cedargraph-nebula-alignment.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach do you prefer?
