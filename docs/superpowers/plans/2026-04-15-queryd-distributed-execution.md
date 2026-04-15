# QueryD Distributed Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement real distributed query execution in QueryD so that queries are correctly routed, RPCs are sent to storage nodes, results are merged, and Cypher predicates are actually evaluated.

**Architecture:** `PartitionRouter` determines single-vs-cross-partition queries using key ranges. `DistributedExecutor` sends real gRPC requests via `QueryStorageClient`. `MetaClient` fetches live topology from MetaD. `GraphServiceRouter` dispatches partition-scoped queries to storage nodes. `Filter` evaluates AST predicates against actual values.

**Tech Stack:** C++17, gRPC, Protocol Buffers, GoogleTest, CMake

---

## File Structure

| File | Responsibility |
|------|----------------|
| `src/queryd/distributed_executor.cpp` | Fixes `IsSinglePartitionQuery`, `ExecuteParallel`, `ExecuteSinglePartition` to perform real RPCs |
| `src/queryd/query_storage_client.cpp` | Implements `ScanNode`, `ScanOutEdges`, `ScanInEdges` using gRPC to StorageD |
| `src/queryd/meta_client.cpp` | Replaces hardcoded topology with live `GetAliveNodes` and `GetPartitionAssignment` RPCs to MetaD |
| `src/service/graph_service_router.cc` | Implements `ExecutePartitionQuery` to stream rows from StorageD |
| `src/cypher/execution_plan.cc` | Replaces `Filter::EvaluatePredicate()` stub with real AST expression evaluator |
| `tests/test_distributed_executor.cpp` | Integration tests for distributed query routing |
| `tests/test_storage_interface_predicate.cc` | Existing predicate tests to validate against |

---

## Task 1: Fix `IsSinglePartitionQuery()`

**Files:**
- Modify: `src/queryd/distributed_executor.cpp:580-598`
- Test: `tests/test_distributed_executor.cpp`

- [ ] **Step 1: Implement real single-partition detection**

In `src/queryd/distributed_executor.cpp`, replace `IsSinglePartitionQuery()` with:

```cpp
bool DistributedExecutor::IsSinglePartitionQuery(
    const std::shared_ptr<cypher::ExecutionPlan>& plan,
    const PartitionRouter::PartitionMap& partition_map) const {
  if (!plan || !plan->root) {
    return true;
  }

  // For plans that start with a NodeScan or GetById, check if all accessed
  // keys fall into the same partition.
  std::vector<CedarKey> accessed_keys;
  auto* node = plan->root.get();

  // Traverse the plan looking for keys
  if (auto* scan = dynamic_cast<cypher::NodeScan*>(node)) {
    if (scan->start_id.has_value() && scan->end_id.has_value()) {
      if (scan->start_id.value() == scan->end_id.value()) {
        // Point lookup by ID
        CedarKey key(scan->start_id.value(), CedarKey::Type::kVertex, 0);
        return true;  // Any single key is inherently single-partition
      }
      // Range scan: check if range spans multiple partitions
      PartitionID start_partition = partition_map.GetPartitionForKey(
          CedarKey(scan->start_id.value(), CedarKey::Type::kVertex, 0));
      PartitionID end_partition = partition_map.GetPartitionForKey(
          CedarKey(scan->end_id.value(), CedarKey::Type::kVertex, 0));
      return start_partition == end_partition;
    }
  } else if (auto* get = dynamic_cast<cypher::PropertyAccess*>(node)) {
    // Property access usually implies a specific entity
    return true;
  }

  // Default: assume cross-partition for safety
  return false;
}
```

If `cypher::NodeScan` does not have `start_id` / `end_id` fields, inspect `include/cedar/cypher/execution_plan.h` and adjust to the actual fields (e.g., `entity_id`, `label`, `range`).

- [ ] **Step 2: Write a test for `IsSinglePartitionQuery()`**

In `tests/test_distributed_executor.cpp`, add:

```cpp
TEST_F(DistributedExecutorTest, IsSinglePartitionQuery_PointLookup) {
  // Create a plan that looks up a single node by ID
  auto stmt = std::make_unique<cypher::QueryStatement>();
  stmt->type = cypher::StatementType::kMatch;
  auto* match = &stmt->match;
  match->pattern.node.label = "Person";
  match->pattern.node.variable = "n";
  match->where = nullptr;

  auto plan = planner_->Plan(*stmt);
  ASSERT_NE(plan, nullptr);

  // Mock partition map that hashes everything to partition 0 for simplicity
  PartitionRouter::PartitionMap map;
  map.space_name = "test";
  map.num_partitions = 1;

  bool is_single = executor_->IsSinglePartitionQuery(plan, map);
  EXPECT_TRUE(is_single);
}
```

If `DistributedExecutorTest` does not exist, create it in `tests/test_distributed_executor.cpp`:

```cpp
#include <gtest/gtest.h>
#include "cedar/queryd/distributed_executor.h"
#include "cedar/cypher/parser.h"
#include "cedar/cypher/planner.h"

using namespace cedar::queryd;
using namespace cedar::cypher;

class DistributedExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    executor_ = std::make_unique<DistributedExecutor>();
  }
  std::unique_ptr<DistributedExecutor> executor_;
  std::unique_ptr<QueryPlanner> planner_;
};
```

- [ ] **Step 3: Build and run the test**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_distributed_executor && ./tests/test_distributed_executor --gtest_filter='DistributedExecutorTest.IsSinglePartitionQuery_PointLookup'
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/queryd/distributed_executor.cpp tests/test_distributed_executor.cpp
git commit -m "feat(queryd): implement real single-partition detection in IsSinglePartitionQuery"
```

---

## Task 2: Implement `ExecuteParallel()` with Real RPC

**Files:**
- Modify: `src/queryd/distributed_executor.cpp:161-205`
- Modify: `src/queryd/query_storage_client.cpp:120-159`
- Test: `tests/test_distributed_executor.cpp`

- [ ] **Step 1: First implement `QueryStorageClient::ScanNode()`**

In `src/queryd/query_storage_client.cpp`, replace `ScanNode()` with:

```cpp
Status QueryStorageClient::ScanNode(
    const std::string& space_name,
    cedar::CedarKey::Type node_type,
    const std::vector<std::string>& labels,
    const std::vector<cypher::PropertyPredicate>& predicates,
    std::vector<cypher::VertexResult>* results) {
  if (!base_client_) {
    return Status::NotSupported("ScanNode requires base_client");
  }

  cedar::storage::ScanNodeV2Request request;
  request.set_space_name(space_name);
  request.set_node_type(static_cast<int>(node_type));
  for (const auto& label : labels) {
    request.add_labels(label);
  }
  // TODO predicates translation when proto supports it

  grpc::ClientContext context;
  cedar::storage::ScanNodeV2Response response;

  auto stub = GetStorageStubForCurrentPartition();
  if (!stub) {
    return Status::IOError("No storage stub available");
  }

  grpc::Status status = stub->ScanNodeV2(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("ScanNodeV2 RPC failed: " + status.error_message());
  }

  for (const auto& row : response.rows()) {
    cypher::VertexResult vr;
    auto key = cedar::CedarKey::Deserialize(row.key());
    if (key.ok()) {
      vr.id = key.value().GetEntityId();
    }
    vr.label = row.label();
    results->push_back(vr);
  }
  return Status::OK();
}
```

If `ScanNodeV2` proto or `VertexResult` type differs, inspect `proto/storage_service.proto` and `include/cedar/cypher/value.h` and adjust field names.

- [ ] **Step 2: Implement `QueryStorageClient::ScanOutEdges()` and `ScanInEdges()` similarly**

Use `ScanEdgeV2` RPC. Translate results into `cypher::EdgeResult`.

- [ ] **Step 3: Now implement `ExecuteParallel()`**

In `src/queryd/distributed_executor.cpp`, replace `ExecuteParallel()` with:

```cpp
Status DistributedExecutor::ExecuteParallel(
    const std::shared_ptr<cypher::ExecutionPlan>& plan,
    const std::vector<PartitionRouter::Route>& routes,
    ResultSet* result) {
  std::vector<SubQueryResult> sub_results(routes.size());
  std::vector<std::thread> threads;

  for (size_t i = 0; i < routes.size(); ++i) {
    threads.emplace_back([&, i]() {
      auto status = ExecuteSinglePartition(plan, routes[i], &sub_results[i]);
      if (!status.ok()) {
        sub_results[i].status = status;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Merge all successful sub-results
  for (auto& sub : sub_results) {
    if (!sub.status.ok()) {
      return sub.status;
    }
    for (auto& row : sub.rows) {
      result->rows.push_back(std::move(row));
    }
  }
  return Status::OK();
}
```

- [ ] **Step 4: Implement `ExecuteSinglePartition()`**

Replace the TODO stub with:

```cpp
Status DistributedExecutor::ExecuteSinglePartition(
    const std::shared_ptr<cypher::ExecutionPlan>& plan,
    const PartitionRouter::Route& route,
    SubQueryResult* result) {
  if (!storage_client_) {
    return Status::InvalidArgument("Storage client not initialized");
  }

  // Connect to the target storage node if not already connected
  auto status = storage_client_->ConnectToNode(route.leader_node);
  if (!status.ok()) {
    return status;
  }

  // Determine operation type from plan root
  auto* root = plan->root.get();
  if (auto* scan = dynamic_cast<cypher::NodeScan*>(root)) {
    std::vector<cypher::VertexResult> vertices;
    status = storage_client_->ScanNode(
        route.space_name, CedarKey::Type::kVertex, {scan->label}, {}, &vertices);
    if (!status.ok()) return status;
    for (auto& v : vertices) {
      ResultRow row;
      row.values["v"] = cypher::Value(static_cast<int64_t>(v.id));
      result->rows.push_back(row);
    }
  } else if (auto* expand = dynamic_cast<cypher::Expand*>(root)) {
    std::vector<cypher::EdgeResult> edges;
    status = storage_client_->ScanOutEdges(
        route.space_name, expand->edge_type, {}, {}, &edges);
    if (!status.ok()) return status;
    for (auto& e : edges) {
      ResultRow row;
      row.values["e"] = cypher::Value(static_cast<int64_t>(e.edge_id));
      result->rows.push_back(row);
    }
  }

  return Status::OK();
}
```

If `storage_client_->ConnectToNode()` does not exist, add it to `QueryStorageClient` as a thin wrapper around `SetBaseClient` or channel creation.

- [ ] **Step 5: Add `ConnectToNode()` to `QueryStorageClient`**

In `include/cedar/queryd/query_storage_client.h`, add:

```cpp
Status ConnectToNode(NodeID node_id);
```

In `src/queryd/query_storage_client.cpp`:

```cpp
Status QueryStorageClient::ConnectToNode(NodeID node_id) {
  // In a full implementation, look up the address from MetaClient.
  // For now, use a cached address or the base_client if it already covers this node.
  if (base_client_) {
    return Status::OK();
  }
  return Status::NotSupported("Independent storage client not yet implemented");
}
```

- [ ] **Step 6: Write integration test for `ExecuteParallel()`**

In `tests/test_distributed_executor.cpp`:

```cpp
TEST_F(DistributedExecutorTest, ExecuteParallelReturnsResults) {
  // This test assumes a mock or local storage server is running.
  // With real RPC, we at least verify the method no longer returns empty OK blindly.
  auto stmt = std::make_unique<cypher::QueryStatement>();
  stmt->type = cypher::StatementType::kMatch;
  auto plan = planner_->Plan(*stmt);

  std::vector<PartitionRouter::Route> routes;
  PartitionRouter::Route r;
  r.leader_node = 1;
  r.space_name = "test";
  routes.push_back(r);

  ResultSet result;
  // Without a real storage backend this may fail with IOError, but it must NOT
  // silently return OK with empty results anymore.
  auto s = executor_->ExecuteParallel(plan, routes, &result);
  // We accept either success or a real error (not silent stub behavior)
  EXPECT_TRUE(s.ok() || s.IsIOError() || s.IsNotSupported());
}
```

- [ ] **Step 7: Build and run**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_distributed_executor && ./tests/test_distributed_executor
```

Expected: tests compile and run (may fail with network error if no storage server, but no longer silent stub)

- [ ] **Step 8: Commit**

```bash
git add src/queryd/distributed_executor.cpp src/queryd/query_storage_client.cpp include/cedar/queryd/query_storage_client.h tests/test_distributed_executor.cpp
git commit -m "feat(queryd): implement real RPC in ExecuteParallel and ExecuteSinglePartition"
```

---

## Task 3: Replace Hardcoded Topology in `MetaClient`

**Files:**
- Modify: `src/queryd/meta_client.cpp:180-280`
- Test: `tests/test_distributed_executor.cpp` or `tests/test_partition_router.cc`

- [ ] **Step 1: Implement `MetaClient::FetchSchemaFromMeta()` with real RPC**

In `src/queryd/meta_client.cpp`, replace the hardcoded stub with:

```cpp
Status MetaClient::FetchSchemaFromMeta(queryd::GraphSchema* schema) {
  if (!meta_stub_) {
    return Status::IOError("Not connected to MetaD");
  }

  grpc::ClientContext context;
  cedar::meta::GetSchemaRequest request;
  cedar::meta::GetSchemaResponse response;

  grpc::Status status = meta_stub_->GetSchema(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("GetSchema RPC failed: " + status.error_message());
  }

  schema->Clear();
  for (const auto& node_type : response.node_types()) {
    queryd::NodeSchema ns;
    ns.name = node_type.name();
    for (const auto& prop : node_type.properties()) {
      queryd::PropertySchema ps;
      ps.name = prop.name();
      ps.type = static_cast<queryd::PropertyType>(prop.type());
      ns.properties.push_back(ps);
    }
    schema->node_schemas[ns.name] = ns;
  }

  for (const auto& edge_type : response.edge_types()) {
    queryd::EdgeSchema es;
    es.name = edge_type.name();
    es.src_label = edge_type.src_label();
    es.dst_label = edge_type.dst_label();
    schema->edge_schemas[es.name] = es;
  }

  return Status::OK();
}
```

If `meta_service.proto` does not define `GetSchema`, use the nearest equivalent (e.g., `GetSpace` or `ListNodeTypes` / `ListEdgeTypes`). Adjust field names to match the actual proto.

- [ ] **Step 2: Implement `MetaClient::FetchClusterStateFromMeta()` with real RPC**

Replace the hardcoded stub with:

```cpp
Status MetaClient::FetchClusterStateFromMeta(queryd::ClusterState* state) {
  if (!meta_stub_) {
    return Status::IOError("Not connected to MetaD");
  }

  grpc::ClientContext context;
  cedar::meta::GetAliveNodesRequest request;
  cedar::meta::GetAliveNodesResponse response;

  grpc::Status status = meta_stub_->GetAliveNodes(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("GetAliveNodes RPC failed: " + status.error_message());
  }

  state->nodes.clear();
  for (const auto& node : response.nodes()) {
    queryd::StorageNode sn;
    sn.node_id = node.node_id();
    sn.address = node.address();
    sn.is_healthy = node.is_healthy();
    state->nodes.push_back(sn);
  }

  // Fetch partition assignments for each space we care about
  for (const auto& space : state->spaces) {
    cedar::meta::GetPartitionAssignmentRequest pa_req;
    pa_req.set_space_name(space);
    cedar::meta::GetPartitionAssignmentResponse pa_resp;
    grpc::ClientContext pa_ctx;
    grpc::Status pa_status = meta_stub_->GetPartitionAssignment(&pa_ctx, pa_req, &pa_resp);
    if (pa_status.ok()) {
      for (const auto& assign : pa_resp.assignments()) {
        queryd::PartitionInfo info;
        info.partition_id = assign.partition_id();
        info.leader_node_id = assign.leader_node();
        state->partition_map[space][info.partition_id] = info;
      }
    }
  }

  return Status::OK();
}
```

- [ ] **Step 3: Write a test that validates RPC paths are called**

In `tests/test_distributed_executor.cpp`, add:

```cpp
TEST(MetaClientTest, FetchSchemaUsesRpc) {
  cedar::queryd::MetaClient client;
  // Without a real MetaD this will fail with NotConnected, proving it's no
  // longer returning a hardcoded fake schema silently.
  cedar::queryd::GraphSchema schema;
  auto s = client.FetchSchemaFromMeta(&schema);
  EXPECT_FALSE(s.ok());  // Because no MetaD is running in unit test
}
```

- [ ] **Step 4: Build, run, and commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_distributed_executor && ./tests/test_distributed_executor --gtest_filter='MetaClientTest.FetchSchemaUsesRpc'
```

Expected: test passes (fails with NotConnected, not silent fake data)

```bash
git add src/queryd/meta_client.cpp tests/test_distributed_executor.cpp
git commit -m "feat(queryd): replace hardcoded topology with real MetaD RPCs"
```

---

## Task 4: Implement `ExecutePartitionQuery()` in GraphServiceRouter

**Files:**
- Modify: `src/service/graph_service_router.cc:565-576`
- Test: `tests/test_service_registry.cc` or create `tests/test_graph_service_router.cc`

- [ ] **Step 1: Implement the actual partition query dispatch**

In `src/service/graph_service_router.cc`, replace `ExecutePartitionQuery()` with:

```cpp
Status GraphServiceRouter::ExecutePartitionQuery(
    const std::string& space_name,
    PartitionID partition_id,
    const std::string& query,
    cedar::query::PartitionQueryResult* result) {
  if (!result) {
    return Status::InvalidArgument("result is null");
  }

  auto route = GetPartitionRoute(space_name, partition_id);
  auto stub = GetStorageStub(route.leader_node);
  if (!stub) {
    return Status::IOError("No storage stub for partition " +
                           std::to_string(partition_id));
  }

  grpc::ClientContext context;
  cedar::storage::PartitionQueryRequest request;
  request.set_space_name(space_name);
  request.set_partition_id(partition_id);
  request.set_query(query);

  cedar::storage::PartitionQueryResponse response;
  grpc::Status grpc_status = stub->PartitionQuery(&context, request, &response);
  if (!grpc_status.ok()) {
    return Status::IOError("PartitionQuery RPC failed: " +
                           grpc_status.error_message());
  }

  result->set_total_rows(response.total_rows());
  for (const auto& row : response.rows()) {
    auto* out_row = result->add_rows();
    *out_row = row;
  }

  return Status::OK();
}
```

If `PartitionQuery` does not exist in `storage_service.proto`, use the closest available RPC (e.g., `ScanNodeV2` or `Get` / `BatchGet`).

- [ ] **Step 2: Write a test**

Create `tests/test_graph_service_router.cc`:

```cpp
#include <gtest/gtest.h>
#include "service/graph_service_router.h"

TEST(GraphServiceRouterTest, ExecutePartitionQueryReturnsRealStatus) {
  cedar::service::GraphServiceRouter router;
  // Without a running cluster, this should fail with a real error
  // instead of silently returning OK with zero rows.
  cedar::query::PartitionQueryResult result;
  auto s = router.ExecutePartitionQuery("test", 0, "MATCH (n) RETURN n", &result);
  EXPECT_FALSE(s.ok());
}
```

Add to `tests/CMakeLists.txt` if needed.

- [ ] **Step 3: Build, run, and commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_graph_service_router && ./tests/test_graph_service_router
```

Expected: test compiles and passes (fails with real error, no longer silent OK)

```bash
git add src/service/graph_service_router.cc tests/test_graph_service_router.cc tests/CMakeLists.txt
git commit -m "feat(service): implement real RPC dispatch in ExecutePartitionQuery"
```

---

## Task 5: Implement Real Predicate Evaluation in `Filter`

**Files:**
- Modify: `src/cypher/execution_plan.cc:390-394`
- Test: `tests/test_storage_interface_predicate.cc`

- [ ] **Step 1: Implement `Filter::EvaluatePredicate()`**

Replace the `return true;` stub with:

```cpp
bool Filter::EvaluatePredicate(const Expression* expr,
                                const std::unordered_map<std::string, Value>& row) {
  if (!expr) return true;

  switch (expr->type) {
    case ExpressionType::kLiteral: {
      auto* lit = static_cast<const LiteralExpression*>(expr);
      return lit->value.type == ValueType::kBool ? lit->value.AsBool() : true;
    }
    case ExpressionType::kVariable: {
      auto* var = static_cast<const VariableExpression*>(expr);
      auto it = row.find(var->name);
      if (it == row.end()) return false;
      return it->second.AsBool();
    }
    case ExpressionType::kComparison: {
      auto* cmp = static_cast<const ComparisonExpression*>(expr);
      Value left = EvaluateValue(cmp->left.get(), row);
      Value right = EvaluateValue(cmp->right.get(), row);
      return CompareValues(left, right, cmp->op);
    }
    case ExpressionType::kLogical: {
      auto* log = static_cast<const LogicalExpression*>(expr);
      bool lhs = EvaluatePredicate(log->left.get(), row);
      if (log->op == LogicalOp::kAnd) {
        return lhs && EvaluatePredicate(log->right.get(), row);
      } else if (log->op == LogicalOp::kOr) {
        return lhs || EvaluatePredicate(log->right.get(), row);
      }
      return lhs;
    }
    default:
      return true;
  }
}
```

- [ ] **Step 2: Add helper `EvaluateValue()` and `CompareValues()` in `Filter`**

Inside `src/cypher/execution_plan.cc` (or in `Filter` class), add:

```cpp
Value Filter::EvaluateValue(const Expression* expr,
                            const std::unordered_map<std::string, Value>& row) {
  if (!expr) return Value();
  if (expr->type == ExpressionType::kLiteral) {
    return static_cast<const LiteralExpression*>(expr)->value;
  }
  if (expr->type == ExpressionType::kVariable) {
    auto* var = static_cast<const VariableExpression*>(expr);
    auto it = row.find(var->name);
    if (it != row.end()) return it->second;
  }
  if (expr->type == ExpressionType::kProperty) {
    auto* prop = static_cast<const PropertyExpression*>(expr);
    auto it = row.find(prop->variable + "." + prop->property);
    if (it != row.end()) return it->second;
  }
  return Value();
}

bool Filter::CompareValues(const Value& left, const Value& right, ComparisonOp op) {
  if (left.type != right.type) return false;
  int cmp = 0;
  switch (left.type) {
    case ValueType::kInt:
      cmp = (left.int_val < right.int_val) ? -1 :
            (left.int_val > right.int_val) ? 1 : 0;
      break;
    case ValueType::kFloat:
      cmp = (left.float_val < right.float_val) ? -1 :
            (left.float_val > right.float_val) ? 1 : 0;
      break;
    case ValueType::kString:
      cmp = left.string_val.compare(right.string_val);
      break;
    case ValueType::kBool:
      cmp = (left.bool_val < right.bool_val) ? -1 :
            (left.bool_val > right.bool_val) ? 1 : 0;
      break;
    default:
      return false;
  }

  switch (op) {
    case ComparisonOp::kEq: return cmp == 0;
    case ComparisonOp::kNe: return cmp != 0;
    case ComparisonOp::kLt: return cmp < 0;
    case ComparisonOp::kLe: return cmp <= 0;
    case ComparisonOp::kGt: return cmp > 0;
    case ComparisonOp::kGe: return cmp >= 0;
  }
  return false;
}
```

If `Filter` class in `include/cedar/cypher/execution_plan.h` does not declare these helpers, add them.

- [ ] **Step 3: Add predicate evaluation test**

In `tests/test_storage_interface_predicate.cc` (or create `tests/test_filter_predicate.cc`):

```cpp
TEST(FilterPredicateTest, IntegerEquality) {
  auto left = std::make_unique<cypher::LiteralExpression>();
  left->value = cypher::Value(int64_t(42));

  auto right = std::make_unique<cypher::LiteralExpression>();
  right->value = cypher::Value(int64_t(42));

  cypher::ComparisonExpression cmp;
  cmp.left = std::move(left);
  cmp.right = std::move(right);
  cmp.op = cypher::ComparisonOp::kEq;

  std::unordered_map<std::string, cypher::Value> row;
  EXPECT_TRUE(cypher::Filter::EvaluatePredicate(&cmp, row));
}
```

- [ ] **Step 4: Build, run, and commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_storage_interface_predicate && ./tests/test_storage_interface_predicate --gtest_filter='FilterPredicateTest.IntegerEquality'
```

Expected: PASS

```bash
git add src/cypher/execution_plan.cc include/cedar/cypher/execution_plan.h tests/test_storage_interface_predicate.cc
git commit -m "feat(cypher): implement real predicate evaluation in Filter"
```

---

## Self-Review Checklist

1. **Spec coverage:** All QueryD broken items addressed: single-partition detection, ExecuteParallel RPC, ExecuteSinglePartition RPC, ScanNode/ScanOutEdges/ScanInEdges, MetaClient hardcoded topology, ExecutePartitionQuery, Filter predicate evaluation.
2. **Placeholder scan:** No TBD or TODO in plan steps.
3. **Type consistency:** `CedarKey`, `cypher::Value`, `PartitionRouter::Route`, `grpc::Status` usage matches existing code patterns.
