# Integration Test Follow-Up Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three follow-up issues discovered during integration test debugging: (1) query cache fingerprint collision for point lookups, (2) missing BatchGet RPC in standalone StorageD, and (3) NodeScan point lookup returning virtual nodes without storage verification.

**Architecture:**
1. Extend the AST-based `ComputeFingerprint` with `FingerprintOptions` to preserve literal values for designated property keys (e.g., `id`), eliminating the `:id=X` string-append workaround in `GraphServiceRouter`.
2. Add `BatchGet` to `tools/storaged.cc`'s standalone `StorageServiceImpl`, delegating to `CedarGraphStorage::BatchGet`.
3. Make `NodeScan::Init()` verify node existence via `CedarGraph::HasVertex()` / `CedarGraphStorage::Scan()` when in point-lookup mode.

**Tech Stack:** C++17, gRPC/Protobuf, CMake, gtest

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `include/cedar/cypher/fingerprint.h` | Modify | Add `FingerprintOptions` struct; overload `ComputeFingerprint` for AST with options |
| `src/cypher/fingerprint.cc` | Modify | Implement `FingerprintOptions` support in `FingerprintWriter`; add `WriteLiteralValue` |
| `src/service/graph_service_router.cc` | Modify | Replace `:id=X` workaround with AST-based `GenerateResultCacheFingerprint` using `preserve_property_keys={"id"}` |
| `tools/storaged.cc` | Modify | Add `BatchGet` RPC handler to standalone `StorageServiceImpl` |
| `include/cedar/graph/cedar_graph.h` | Modify | Add `HasVertex(uint64_t vertex_id)` public method declaration |
| `src/graph/cedar_graph.cc` | Modify | Implement `CedarGraph::HasVertex()` using `storage_->Scan()` |
| `src/cypher/execution_plan.cc` | Modify | `NodeScan::Init()` storage existence check for point lookups |
| `tests/cypher/test_fingerprint.cc` | Modify | Add tests for `FingerprintOptions` with `preserve_property_keys` |
| `tests/test_neighbor_only.cpp` | Modify | Remove `BatchGet` fallback loop; rely on native `BatchGet` |
| `tests/cypher/test_nodescan_real_data.cc` | Modify | Add test for point lookup on non-existent node returning empty |

---

## Task 1: FingerprintOptions Data Structure

**Files:**
- Modify: `include/cedar/cypher/fingerprint.h`

- [ ] **Step 1: Add `<unordered_set>` include and FingerprintOptions struct**

```cpp
// In include/cedar/cypher/fingerprint.h
// Add after existing includes:
#include <unordered_set>

namespace cedar {
namespace cypher {

// Options for controlling fingerprint generation behavior.
struct FingerprintOptions {
  // Property keys whose literal values should be preserved in the fingerprint
  // instead of being replaced with '?'. Empty set means all literals are
  // replaced (default behavior).
  std::unordered_set<std::string> preserve_property_keys;
};

// Existing declarations remain unchanged:
std::string ComputeFingerprint(const std::string& query);
std::string ComputeFingerprint(const QueryStatement& ast);

// New overload:
std::string ComputeFingerprint(const QueryStatement& ast,
                               const FingerprintOptions& options);

}  // namespace cypher
}  // namespace cedar
```

- [ ] **Step 2: Build to verify header compiles**

Run: `cd <repo-root>/build && make -j$(nproc) 2>&1 | tail -10`

Expected: Compilation proceeds (no syntax errors in the modified header).

- [ ] **Step 3: Commit**

```bash
cd <repo-root>
git add include/cedar/cypher/fingerprint.h
git commit -m "feat(fingerprint): add FingerprintOptions struct for literal preservation"
```

---

## Task 2: AST FingerprintWriter Support for Preserving Literals

**Files:**
- Modify: `src/cypher/fingerprint.cc`

- [ ] **Step 1: Modify FingerprintWriter constructor and add WriteLiteralValue**

```cpp
// In src/cypher/fingerprint.cc, inside the anonymous namespace

class FingerprintWriter {
 public:
  explicit FingerprintWriter(const FingerprintOptions& options = {})
      : options_(options) {}

  std::string Result() const { return oss_.str(); }

  void Write(const QueryStatement& stmt) {
    for (size_t i = 0; i < stmt.clauses.size(); ++i) {
      if (i > 0) oss_ << " ";
      WriteClause(*stmt.clauses[i]);
    }
  }

 private:
  std::ostringstream oss_;
  FingerprintOptions options_;

  // ... existing WriteClause, WriteMatch, WritePattern methods ...

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

  void WriteLiteralValue(const LiteralExpr& literal) {
    oss_ << literal.value.ToString();
  }

  // ... existing WriteWhere, WriteReturn, WriteExpression, etc. ...
};
```

- [ ] **Step 2: Add new ComputeFingerprint overload and keep default wrapper**

Replace the existing `ComputeFingerprint(const QueryStatement& ast)` at the bottom of the file:

```cpp
std::string ComputeFingerprint(const QueryStatement& ast,
                               const FingerprintOptions& options) {
  FingerprintWriter writer(options);
  writer.Write(ast);
  std::string fp = writer.Result();
  // Normalize whitespace (AST writer may produce double spaces)
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
```

- [ ] **Step 3: Build to verify implementation compiles**

Run: `cd <repo-root>/build && make -j$(nproc) cedar_cypher 2>&1 | tail -15`

Expected: `cedar_cypher` target builds successfully with no errors.

- [ ] **Step 4: Commit**

```bash
cd <repo-root>
git add src/cypher/fingerprint.cc
git commit -m "feat(fingerprint): implement FingerprintOptions in AST writer"
```

---

## Task 3: Fingerprint Options Unit Tests

**Files:**
- Modify: `tests/cypher/test_fingerprint.cc`

- [ ] **Step 1: Add tests for preserved property keys**

Append before `main()`:

```cpp
TEST(FingerprintTest, PreservePropertyKeys) {
  cedar::cypher::CypherParser parser("MATCH (n:Person {id: 100}) RETURN n");
  auto ast = parser.ParseStatement();
  ASSERT_TRUE(ast != nullptr);

  // Default: id replaced with ?
  std::string fp_default = ComputeFingerprint(*ast);
  EXPECT_EQ(fp_default, "match (n:Person {id:?}) return n");

  // With preserve_property_keys={"id"}: id value kept
  FingerprintOptions opts;
  opts.preserve_property_keys.insert("id");
  std::string fp_preserve = ComputeFingerprint(*ast, opts);
  EXPECT_EQ(fp_preserve, "match (n:Person {id:100}) return n");

  // Different id values produce different fingerprints
  cedar::cypher::CypherParser parser2("MATCH (n:Person {id: 200}) RETURN n");
  auto ast2 = parser2.ParseStatement();
  ASSERT_TRUE(ast2 != nullptr);
  std::string fp2 = ComputeFingerprint(*ast2, opts);
  EXPECT_NE(fp_preserve, fp2);
  EXPECT_EQ(fp2, "match (n:Person {id:200}) return n");
}

TEST(FingerprintTest, PreservePropertyKeysNonLiteral) {
  cedar::cypher::CypherParser parser("MATCH (n:Person {id: $param}) RETURN n");
  auto ast = parser.ParseStatement();
  ASSERT_TRUE(ast != nullptr);

  FingerprintOptions opts;
  opts.preserve_property_keys.insert("id");
  std::string fp = ComputeFingerprint(*ast, opts);
  // Parameters are still replaced with ? even when key is preserved
  EXPECT_EQ(fp, "match (n:Person {id:?}) return n");
}

TEST(FingerprintTest, PreservePropertyKeysOnlyForDesignatedKeys) {
  cedar::cypher::CypherParser parser(
      "MATCH (n:Person {id: 100, name: 'Alice'}) RETURN n");
  auto ast = parser.ParseStatement();
  ASSERT_TRUE(ast != nullptr);

  FingerprintOptions opts;
  opts.preserve_property_keys.insert("id");
  std::string fp = ComputeFingerprint(*ast, opts);
  // id is preserved, name is still replaced with ?
  EXPECT_EQ(fp, "match (n:Person {id:100,name:?}) return n");
}
```

- [ ] **Step 2: Build and run tests**

Run:
```bash
cd <repo-root>/build
make -j$(nproc) test_fingerprint 2>&1 | tail -10
./tests/cypher/test_fingerprint
```

Expected: All 10 tests pass (6 existing + 4 new).

- [ ] **Step 3: Commit**

```bash
cd <repo-root>
git add tests/cypher/test_fingerprint.cc
git commit -m "test(fingerprint): add tests for FingerprintOptions preserve_property_keys"
```

---

## Task 4: GraphServiceRouter Use New Fingerprint Options

**Files:**
- Modify: `src/service/graph_service_router.cc`

- [ ] **Step 1: Add GenerateResultCacheFingerprint helper method**

In `src/service/graph_service_router.cc`, add a new private helper in the `GraphServiceRouter` class (inside the anonymous namespace or as a private method). For simplicity, add it as a private method declaration in `include/cedar/service/graph_service_router.h` first, then implement it in `.cc`.

**In `include/cedar/service/graph_service_router.h`**, add after `GenerateQueryFingerprint`:

```cpp
  // Generates a fingerprint suitable for result caching.
  // Preserves 'id' property literals to avoid cache collisions
  // for point-lookup queries.
  std::string GenerateResultCacheFingerprint(const std::string& query);
```

**In `src/service/graph_service_router.cc`**, add the implementation before the existing `GenerateQueryFingerprint`:

```cpp
std::string GraphServiceRouter::GenerateResultCacheFingerprint(
    const std::string& query) {
  // Fast path: use string-based fingerprint for queries without AST
  cedar::cypher::CypherParser parser(query);
  auto ast = parser.ParseStatement();
  if (!ast) {
    return cedar::cypher::ComputeFingerprint(query);
  }

  // Check if any MATCH clause contains a node with {id: literal}
  bool has_literal_id = false;
  for (const auto& clause : ast->clauses) {
    if (clause->clause_type == cedar::cypher::ClauseType::MATCH) {
      auto* match = static_cast<cedar::cypher::MatchClause*>(clause.get());
      for (const auto& pattern : match->patterns) {
        for (const auto& elem : pattern.elements) {
          if (std::holds_alternative<cedar::cypher::NodePattern>(elem)) {
            const auto& node = std::get<cedar::cypher::NodePattern>(elem);
            auto it = node.properties.find("id");
            if (it != node.properties.end() &&
                it->second->expr_type == cedar::cypher::ExprType::LITERAL) {
              has_literal_id = true;
              break;
            }
          }
        }
        if (has_literal_id) break;
      }
    }
    if (has_literal_id) break;
  }

  if (has_literal_id) {
    cedar::cypher::FingerprintOptions opts;
    opts.preserve_property_keys.insert("id");
    return cedar::cypher::ComputeFingerprint(*ast, opts);
  }

  return cedar::cypher::ComputeFingerprint(*ast);
}
```

- [ ] **Step 2: Replace all three cache-key generation sites**

Find and replace all three occurrences in `ExecuteQuery` and `StreamQuery`:

**Occurrence 1** (~line 259, cache read):
```cpp
// BEFORE:
cache_key.query_fingerprint = GenerateQueryFingerprint(request->query());
if (!route_ctx.entity_ids.empty()) {
  cache_key.query_fingerprint += ":id=" + std::to_string(route_ctx.entity_ids[0]);
}

// AFTER:
cache_key.query_fingerprint = GenerateResultCacheFingerprint(request->query());
```

**Occurrence 2** (~line 376, cache write after DistributedExecutor):
```cpp
// BEFORE:
cache_key.query_fingerprint = GenerateQueryFingerprint(request->query());
if (!route_ctx.entity_ids.empty()) {
  cache_key.query_fingerprint += ":id=" + std::to_string(route_ctx.entity_ids[0]);
}

// AFTER:
cache_key.query_fingerprint = GenerateResultCacheFingerprint(request->query());
```

**Occurrence 3** (~line 578, cache write at end of ExecuteQuery):
```cpp
// BEFORE:
cache_key.query_fingerprint = GenerateQueryFingerprint(request->query());
if (!route_ctx.entity_ids.empty()) {
  cache_key.query_fingerprint += ":id=" + std::to_string(route_ctx.entity_ids[0]);
}

// AFTER:
cache_key.query_fingerprint = GenerateResultCacheFingerprint(request->query());
```

- [ ] **Step 3: Build to verify**

Run: `cd <repo-root>/build && make -j$(nproc) graph_service_router 2>&1 | tail -15`

Expected: Builds successfully.

- [ ] **Step 4: Run test_distributed_write to verify cache behavior**

Run:
```bash
cd <repo-root>/build
./tests/test_distributed_write
```

Expected: Test passes with 100% read hit rate. QPS remains ~60 (2PC writes) but cache now works correctly for reads without string concatenation workaround.

- [ ] **Step 5: Commit**

```bash
cd <repo-root>
git add include/cedar/service/graph_service_router.h src/service/graph_service_router.cc
git commit -m "feat(router): use AST-based result cache fingerprint with id literal preservation"
```

---

## Task 5: tools/storaged.cc Implement BatchGet

**Files:**
- Modify: `tools/storaged.cc`

- [ ] **Step 1: Add BatchGet method to StorageServiceImpl**

In `tools/storaged.cc`, add the following method inside `class StorageServiceImpl` (after the existing `Get` method, around line 327):

```cpp
  grpc::Status BatchGet(grpc::ServerContext* context,
                        const cedar::storage::BatchGetRequest* request,
                        cedar::storage::BatchGetResponse* response) override {
    (void)context;
    auto start = std::chrono::steady_clock::now();

    for (const auto& proto_key : request->keys()) {
      auto result = storage_->Get(proto_key.entity_id(), proto_key.timestamp());
      if (result.has_value()) {
        response->add_descriptors()->set_data(result->Encode());
        response->add_found(true);
      } else {
        response->add_descriptors();
        response->add_found(false);
      }
    }

    auto end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end - start).count();
    RecordStorageOp("batch_get", true, latency_us);
    response->set_success(true);
    return grpc::Status::OK;
  }
```

- [ ] **Step 2: Build to verify**

Run: `cd <repo-root>/build && make -j$(nproc) storaged 2>&1 | tail -15`

Expected: `storaged` target builds successfully.

- [ ] **Step 3: Commit**

```bash
cd <repo-root>
git add tools/storaged.cc
git commit -m "feat(storaged): implement BatchGet RPC in standalone StorageServiceImpl"
```

---

## Task 6: Verify BatchGet and Remove Fallback

**Files:**
- Modify: `tests/test_neighbor_only.cpp`

- [ ] **Step 1: Remove BatchGet fallback in test client**

In `tests/test_neighbor_only.cpp`, modify `BatchRead` to remove the fallback loop:

```cpp
// BEFORE (current code with fallback):
std::vector<std::optional<int32_t>> BatchRead(
    const std::vector<std::tuple<uint64_t, uint16_t, uint64_t>>& keys) {
  cedar::storage::BatchGetRequest request;
  // ... build request ...
  cedar::storage::BatchGetResponse response;
  grpc::ClientContext context;
  auto status = stub_->BatchGet(&context, request, &response);

  std::vector<std::optional<int32_t>> results;
  if (status.ok() && response.success()) {
    for (int i = 0; i < response.found_size(); ++i) {
      // ... decode descriptors ...
    }
    return results;
  }

  // Fallback: individual Get calls if BatchGet is not supported
  for (const auto& [entity_id, col_id, timestamp] : keys) {
    results.push_back(ReadVertex(entity_id, col_id, timestamp));
  }
  return results;
}

// AFTER (no fallback):
std::vector<std::optional<int32_t>> BatchRead(
    const std::vector<std::tuple<uint64_t, uint16_t, uint64_t>>& keys) {
  cedar::storage::BatchGetRequest request;

  for (const auto& [entity_id, col_id, timestamp] : keys) {
    auto* key = request.add_keys();
    key->set_entity_id(entity_id);
    key->set_timestamp(timestamp);
    key->set_column_id(col_id);
    key->set_type_flags((0 << 16));
    key->set_partition_id(0);
  }

  cedar::storage::BatchGetResponse response;
  grpc::ClientContext context;
  auto status = stub_->BatchGet(&context, request, &response);

  std::vector<std::optional<int32_t>> results;
  if (!status.ok() || !response.success()) {
    std::cerr << "BatchGet failed: " << (status.ok() ? response.error_msg() : status.error_message()) << std::endl;
    return results;  // Return empty on failure
  }

  for (int i = 0; i < response.found_size(); ++i) {
    if (response.found(i) && response.descriptors(i).data().size() >= 8) {
      auto opt_desc = cedar::Descriptor::Decode(
          cedar::Slice(response.descriptors(i).data().data(),
                       response.descriptors(i).data().size()));
      if (opt_desc.has_value()) {
        results.push_back(opt_desc.value().AsInlineInt());
      } else {
        results.push_back(std::nullopt);
      }
    } else {
      results.push_back(std::nullopt);
    }
  }
  return results;
}
```

- [ ] **Step 2: Build and run test**

Run:
```bash
cd <repo-root>/build
make -j$(nproc) test_neighbor_only 2>&1 | tail -10
./tests/test_neighbor_only
```

Expected: Test passes with 1000/1000 writes and reads OK. No fallback needed.

- [ ] **Step 3: Commit**

```bash
cd <repo-root>
git add tests/test_neighbor_only.cpp
git commit -m "test(neighbor): remove BatchGet fallback, use native BatchGet RPC"
```

---

## Task 7: CedarGraph::HasVertex Helper

**Files:**
- Modify: `include/cedar/graph/cedar_graph.h`
- Modify: `src/graph/cedar_graph.cc`

- [ ] **Step 1: Add HasVertex declaration to header**

In `include/cedar/graph/cedar_graph.h`, add after `ScanVertices` (~line 294):

```cpp
  // Check if a vertex has any data in storage
  bool HasVertex(uint64_t vertex_id);
```

- [ ] **Step 2: Implement HasVertex**

In `src/graph/cedar_graph.cc`, add after `ScanVertices` (~line 419):

```cpp
bool CedarGraph::HasVertex(uint64_t vertex_id) {
  if (!storage_) {
    return false;
  }
  auto versions = storage_->Scan(vertex_id, Timestamp(0), Timestamp::Max());
  return !versions.empty();
}
```

- [ ] **Step 3: Build to verify**

Run: `cd <repo-root>/build && make -j$(nproc) cedar_graph 2>&1 | tail -10`

Expected: Builds successfully.

- [ ] **Step 4: Commit**

```bash
cd <repo-root>
git add include/cedar/graph/cedar_graph.h src/graph/cedar_graph.cc
git commit -m "feat(graph): add HasVertex() for point-lookup existence check"
```

---

## Task 8: NodeScan Point Lookup Storage Verification

**Files:**
- Modify: `src/cypher/execution_plan.cc`

- [ ] **Step 1: Modify NodeScan::Init point-lookup branch to verify existence**

In `src/cypher/execution_plan.cc`, replace the point-lookup branch in `NodeScan::Init`:

```cpp
// BEFORE (lines 239-253):
  auto id_it = properties_.find("id");
  if (id_it != properties_.end() && id_it->second) {
    if (id_it->second->expr_type == ExprType::LITERAL) {
      auto* literal = static_cast<LiteralExpr*>(id_it->second.get());
      if (literal->value.IsInt()) {
        int64_t id_val = literal->value.GetInt();
        if (id_val > 0) {
          node_ids_.push_back(static_cast<uint64_t>(id_val));
          current_index_ = 0;
          return true;
        }
      }
    }
  }

// AFTER:
  auto id_it = properties_.find("id");
  if (id_it != properties_.end() && id_it->second) {
    if (id_it->second->expr_type == ExprType::LITERAL) {
      auto* literal = static_cast<LiteralExpr*>(id_it->second.get());
      if (literal->value.IsInt()) {
        int64_t id_val = literal->value.GetInt();
        if (id_val > 0) {
          uint64_t node_id = static_cast<uint64_t>(id_val);
          // If storage or graph is available, verify the node actually exists
          bool exists = true;
          if (ctx->graph) {
            exists = ctx->graph->HasVertex(node_id);
          } else if (ctx->storage) {
            auto versions = ctx->storage->Scan(node_id, Timestamp(0), Timestamp::Max());
            exists = !versions.empty();
          }
          if (exists) {
            node_ids_.push_back(node_id);
          }
          current_index_ = 0;
          return true;
        }
      }
    }
  }
```

- [ ] **Step 2: Build to verify**

Run: `cd <repo-root>/build && make -j$(nproc) cedar_cypher 2>&1 | tail -10`

Expected: Builds successfully.

- [ ] **Step 3: Commit**

```bash
cd <repo-root>
git add src/cypher/execution_plan.cc
git commit -m "fix(nodescan): verify node existence in storage for point lookups"
```

---

## Task 9: NodeScan Point Lookup Verification Test

**Files:**
- Modify: `tests/cypher/test_nodescan_real_data.cc`

- [ ] **Step 1: Add test for non-existent node point lookup**

Append after the existing test (before the closing brace of the file):

```cpp
TEST(NodeScanRealDataTest, PointLookupReturnsOnlyExistingVertices) {
  char buf[] = "/tmp/cedar_nodescan_pointlookup_test_XXXXXX";
  char* dir = mkdtemp(buf);
  ASSERT_NE(dir, nullptr);
  std::string db_path = dir;

  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path, &storage);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_NE(storage, nullptr);

  // Create only vertex 10 (vertex 999 does NOT exist)
  Descriptor desc = Descriptor::InlineInt(1, 42);
  s = storage->PutStaticVertex(10, 1, desc);
  ASSERT_TRUE(s.ok()) << s.ToString();

  CedarGraph graph(storage);

  // Test 1: Point lookup for existing vertex 10
  {
    ExecutionContext ctx;
    ctx.graph = &graph;

    // Build properties map with {id: 10}
    std::map<std::string, std::shared_ptr<Expression>> props;
    props["id"] = std::make_shared<LiteralExpr>(Value(static_cast<int64_t>(10)));

    NodeScan scan("n", std::nullopt, std::move(props));
    bool init_ok = scan.Init(&ctx);
    ASSERT_TRUE(init_ok);

    auto record = scan.Next();
    ASSERT_NE(record, nullptr);
    auto val = record->Get("n");
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val->Type(), ValueType::kNode);
    EXPECT_EQ(val->GetNode().id, 10u);

    // No more records
    EXPECT_EQ(scan.Next(), nullptr);
  }

  // Test 2: Point lookup for non-existent vertex 999
  {
    ExecutionContext ctx;
    ctx.graph = &graph;

    std::map<std::string, std::shared_ptr<Expression>> props;
    props["id"] = std::make_shared<LiteralExpr>(Value(static_cast<int64_t>(999)));

    NodeScan scan("n", std::nullopt, std::move(props));
    bool init_ok = scan.Init(&ctx);
    ASSERT_TRUE(init_ok);

    // Should return nullptr because vertex 999 does not exist
    EXPECT_EQ(scan.Next(), nullptr);
  }

  delete storage;
  std::filesystem::remove_all(db_path);
}
```

- [ ] **Step 2: Build and run test**

Run:
```bash
cd <repo-root>/build
make -j$(nproc) test_nodescan_real_data 2>&1 | tail -10
./tests/cypher/test_nodescan_real_data
```

Expected: Both tests pass (existing `ReturnsOnlyExistingVertices` + new `PointLookupReturnsOnlyExistingVertices`).

- [ ] **Step 3: Commit**

```bash
cd <repo-root>
git add tests/cypher/test_nodescan_real_data.cc
git commit -m "test(nodescan): verify point lookup skips non-existent nodes"
```

---

## Task 10: Final Integration Verification

**Files:**
- (no file changes — verification only)

- [ ] **Step 1: Build all affected targets**

Run:
```bash
cd <repo-root>/build
make -j$(nproc) test_distributed_write test_timestamp_debug test_neighbor_only test_fingerprint test_nodescan_real_data 2>&1 | tail -20
```

Expected: All targets build successfully.

- [ ] **Step 2: Run all three integration tests**

```bash
cd <repo-root>/build
./tests/test_timestamp_debug && echo "PASS: test_timestamp_debug"
./tests/test_neighbor_only && echo "PASS: test_neighbor_only"
./tests/test_distributed_write && echo "PASS: test_distributed_write"
```

Expected: All three tests pass.

- [ ] **Step 3: Run unit tests**

```bash
cd <repo-root>/build
./tests/cypher/test_fingerprint && echo "PASS: test_fingerprint"
./tests/cypher/test_nodescan_real_data && echo "PASS: test_nodescan_real_data"
```

Expected: All unit tests pass.

- [ ] **Step 4: Final commit**

```bash
cd <repo-root>
# No uncommitted changes at this point — all tasks committed individually
```

---

## Self-Review

**1. Spec coverage:**
- ✅ Cache fingerprint collision: `FingerprintOptions` + `preserve_property_keys` + `GenerateResultCacheFingerprint` + tests
- ✅ BatchGet missing: `BatchGet` RPC implementation in `tools/storaged.cc` + fallback removal + test verification
- ✅ NodeScan virtual nodes: `HasVertex()` + `NodeScan::Init()` existence check + test for non-existent node

**2. Placeholder scan:**
- ✅ No "TBD", "TODO", "implement later"
- ✅ All code blocks contain actual implementation
- ✅ All test code is complete with assertions
- ✅ All commands include expected output

**3. Type consistency:**
- ✅ `FingerprintOptions` used consistently across header, implementation, tests, and router
- ✅ `ComputeFingerprint(const QueryStatement&, const FingerprintOptions&)` signature matches declaration
- ✅ `HasVertex(uint64_t)` signature matches declaration
- ✅ `CypherParser` (not `Parser`) used consistently
- ✅ `std::holds_alternative` / `std::get` for `NodePattern` in `PathPattern::elements` matches AST definition
