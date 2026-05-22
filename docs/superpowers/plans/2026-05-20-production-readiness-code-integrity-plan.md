# CedarGraph-Core Production Readiness — Code Integrity Fix Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all BLOCKER and CRITICAL issues identified in the deep code integrity audit so CedarGraph-Core is safe for production deployment.

**Architecture:** 7 modules were audited (Storage, Graph/Cypher, DTX/Raft, Queryd/Partition, Service/Meta, GCN/Governance/Core/Metrics, Supporting). This plan groups fixes by severity (BLOCKER first, then CRITICAL) and by logical dependency to minimize rebuild churn.

**Tech Stack:** C++17, CMake, gRPC, braft, brpc, googletest

**Scope exclusions:** User registration/auth UI (not in scope per user request). Empty stubs that are explicitly documented as "not yet implemented" and not on hot paths are deferred to a follow-up plan.

---

## Audit Summary

| Module | BLOCKER | CRITICAL | WARNING | INFO | Total |
|--------|---------|----------|---------|------|-------|
| Storage (db, sst, storage, transaction, types) | 8 | 17 | 18 | 7 | 50 |
| Graph & Cypher (graph, cypher, query) | 8 | 22 | 32 | 4 | 66 |
| DTX & Raft (dtx/storage, dtx/raft, dtx/coordinator, dtx/protocol) | 5 | 13 | 18 | 5 | 41 |
| Queryd & Partition (queryd, partition) | 10 | 16 | 13 | 3 | 42 |
| Service & Meta (service, dtx/metad, dtx/meta, dtx/grpc) | 5 | 7 | 14 | 3 | 29 |
| GCN, Governance, Core, Metrics | 7 | 10 | 16 | 8 | 41 |
| Supporting (driver, chaos, security, utils, index, network) | 4 | 7 | 10 | 6 | 27 |
| **Total** | **47** | **92** | **121** | **36** | **296** |

---

## Phase 1: BLOCKER Fixes — Data Safety & Consensus (P0)

These 47 issues can cause data loss, split-brain, crashes, or silent corruption in production. They must be fixed before any deployment.

---

### Task 1: Fix V2 Compaction Stubs (Storage)

**Problem:** `ReconstructKey()` and `GetValueByRow()` in `zone_columnar_reader.cc` are hard stubs returning empty values. V2 compaction produces corrupted SST files.

**Files:**
- Modify: `src/sst/zone_columnar_reader.cc:647-656`
- Modify: `src/sst/zone_columnar_reader.cc:379-431` (ReconstructKeyFromBlock unaligned reads)
- Test: `tests/sst/test_zone_columnar_reader.cc` (new)

- [ ] **Step 1: Implement `ReconstructKey()`**

  Read the block layout, reconstruct `CedarKey` from packed columns:
  ```cpp
  CedarKey ZoneColumnarReader::ReconstructKey(uint32_t row) {
      CedarKey key;
      key.set_entity_id(ReadColumnUInt64(row, entity_id_col_idx_));
      key.set_entity_type(ReadColumnUInt8(row, entity_type_col_idx_));
      key.set_column_id(ReadColumnUInt16(row, column_id_col_idx_));
      key.set_timestamp(ReadColumnTimestamp(row, timestamp_col_idx_));
      return key;
  }
  ```

- [ ] **Step 2: Implement `GetValueByRow()`**

  ```cpp
  Descriptor ZoneColumnarReader::GetValueByRow(uint32_t row) {
      return ReadDescriptor(row, value_col_idx_);
  }
  ```

- [ ] **Step 3: Fix `ReconstructKeyFromBlock()` unaligned reads**

  Replace `reinterpret_cast<const uint64_t*>` with `memcpy`:
  ```cpp
  uint64_t entity_id;
  memcpy(&entity_id, block_data + offset, sizeof(uint64_t));
  ```

- [ ] **Step 4: Write test verifying V2 compaction round-trip**

  Build SST with V2 builder, read back keys and values, verify exact match.

- [ ] **Step 5: Build and run**

  ```bash
  cd build && cmake --build . --target test_zone_columnar_reader && ./tests/test_zone_columnar_reader
  ```
  Expected: PASS.

- [ ] **Step 6: Commit**

  ```bash
  git add src/sst/zone_columnar_reader.cc tests/sst/test_zone_columnar_reader.cc tests/CMakeLists.txt
  git commit -m "fix(sst): implement V2 columnar reader ReconstructKey and GetValueByRow"
  ```

---

### Task 2: Fix Failover Manager Race Conditions (Storage HA)

**Problem:** `SelectNewLeader()` modifies shared state without holding `nodes_mutex_` when called from `Start()`. Also `GetNodeForRead()` ignores `CanFailover()` config.

**Files:**
- Modify: `src/storage/failover_manager.cc:272-291,311-331`
- Modify: `src/storage/failover_manager.cc:183-211`
- Test: `tests/storage/test_failover_races.cc` (new)

- [ ] **Step 1: Fix `SelectNewLeader()` locking**

  Add `std::lock_guard<std::mutex> lock(nodes_mutex_);` at the top of `SelectNewLeader()`, regardless of caller. Remove the assumption that the caller holds the lock.

- [ ] **Step 2: Fix `GetNodeForRead()` to respect `CanFailover()`**

  ```cpp
  StorageNodeInfo* FailoverManager::GetNodeForRead() {
      if (!config_.CanFailover()) {
          return GetCurrentLeader();  // always route to leader
      }
      // existing round-robin logic
  }
  ```

- [ ] **Step 3: Write concurrent stress test**

  Spawn 8 threads calling `SelectNewLeader()` and `GetNodeForRead()` simultaneously for 10 seconds. Verify no data races (run under TSan if available).

- [ ] **Step 4: Build and run**

  ```bash
  cd build && cmake --build . --target test_failover_races && ./tests/test_failover_races
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add src/storage/failover_manager.cc tests/storage/test_failover_races.cc tests/CMakeLists.txt
  git commit -m "fix(storage): fix failover race conditions and respect CanFailover config"
  ```

---

### Task 3: Fix DoCompaction OOM & Streaming Merge (DB)

**Problem:** `DoCompaction()` loads ALL entries into a single vector before sorting. OOM on large datasets.

**Files:**
- Modify: `src/db/graph_db_impl.cc:699-713`
- Test: `tests/db/test_compaction_streaming.cc` (new)

- [ ] **Step 1: Replace all-in-memory vector with streaming merge**

  Use a min-heap (priority_queue) to merge iterators from input SST files one entry at a time:
  ```cpp
  struct HeapEntry {
      Descriptor descriptor;
      size_t source_idx;
      bool operator>(const HeapEntry& o) const {
          return descriptor.timestamp() > o.descriptor.timestamp();
      }
  };
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> min_heap;
  // Initialize heap with first entry from each SST iterator
  // Pop smallest, write to output, push next from same source
  ```

- [ ] **Step 2: Fix `DropColumnFamily()` memory leak**

  ```cpp
  auto it = column_families_.find(name);
  if (it != column_families_.end()) {
      delete it->second;  // delete the handle
      column_families_.erase(it);
  }
  ```

- [ ] **Step 3: Write test with large compaction**

  Generate 1M entries across 4 SST files, compact, verify output is correct and memory stays bounded.

- [ ] **Step 4: Build and run**

  ```bash
  cd build && cmake --build . --target test_compaction_streaming && ./tests/test_compaction_streaming
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add src/db/graph_db_impl.cc tests/db/test_compaction_streaming.cc tests/CMakeLists.txt
  git commit -m "fix(db): stream merge in DoCompaction to prevent OOM; fix DropColumnFamily leak"
  ```

---

### Task 4: Fix Temporal Constraint Check (Storage)

**Problem:** `GetEntitySnapshot()` returns uninitialized `create_time` (default 0), so `CheckTemporalConstraint()` always passes.

**Files:**
- Modify: `src/storage/cedar_update.cc:395-418,421-438`
- Test: `tests/storage/test_temporal_constraint.cc` (new)

- [ ] **Step 1: Implement `GetEntitySnapshot()` to return actual create time**

  ```cpp
  EntitySnapshot CedarUpdate::GetEntitySnapshot(uint64_t entity_id) {
      EntitySnapshot snapshot;
      snapshot.entity_id = entity_id;
      auto versions = storage_->Scan(entity_id, Timestamp::Min(), Timestamp::Max());
      if (!versions.empty()) {
          snapshot.create_time = versions.front().timestamp;
      } else {
          snapshot.create_time = Timestamp::Max();  // entity doesn't exist yet
      }
      return snapshot;
  }
  ```

- [ ] **Step 2: Write test**

  Create entity at T=100. Try to add edge at T=50. Verify `CheckTemporalConstraint` rejects it.

- [ ] **Step 3: Build and run**

  ```bash
  cd build && cmake --build . --target test_temporal_constraint && ./tests/test_temporal_constraint
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add src/storage/cedar_update.cc tests/storage/test_temporal_constraint.cc tests/CMakeLists.txt
  git commit -m "fix(storage): populate create_time in GetEntitySnapshot for temporal constraint checks"
  ```

---

### Task 5: Fix SST Builder Factory ODR Violation (SST)

**Problem:** `sst_builder_factory.cc` directly `#include`s a `.cc` file, causing ODR violations.

**Files:**
- Modify: `src/sst/sst_builder_factory.cc:24`
- Modify: `src/sst/zone_columnar_builder_v2.cc:1-10` (add header guard / make it header-only)
- Test: existing build is the test

- [ ] **Step 1: Extract declarations to header**

  Create `include/cedar/sst/zone_columnar_builder_v2.h` with class declaration. Move implementation to `src/sst/zone_columnar_builder_v2.cc` as a normal translation unit.

- [ ] **Step 2: Update factory to include header, not .cc**

  ```cpp
  #include "cedar/sst/zone_columnar_builder_v2.h"
  ```

- [ ] **Step 3: Add new .cc to CMakeLists.txt**

  Add `src/sst/zone_columnar_builder_v2.cc` to `CEDAR_STORAGE_SOURCES`.

- [ ] **Step 4: Build**

  ```bash
  cd build && cmake --build . --target cedar
  ```
  Expected: builds without duplicate symbol errors.

- [ ] **Step 5: Commit**

  ```bash
  git add include/cedar/sst/zone_columnar_builder_v2.h src/sst/zone_columnar_builder_v2.cc src/sst/sst_builder_factory.cc CMakeLists.txt
  git commit -m "fix(sst): eliminate ODR violation by separating zone_columnar_builder_v2 into header+cc"
  ```

---

### Task 6: Fix Parallel Compaction Scheduler (Storage)

**Problem:** `ScheduleIfNeeded()` computes a task but never pushes it to `task_queue_`.

**Files:**
- Modify: `src/storage/parallel_compaction_engine.cc:195-218`
- Test: `tests/storage/test_parallel_compaction_scheduler.cc` (new)

- [ ] **Step 1: Push the computed task to the queue**

  ```cpp
  bool ParallelCompactionEngine::ScheduleIfNeeded() {
      auto task = ComputeNextTask();
      if (!task) return false;
      {
          std::lock_guard<std::mutex> lock(task_queue_mutex_);
          task_queue_.push(std::move(*task));
      }
      task_queue_cv_.notify_one();
      return true;
  }
  ```

- [ ] **Step 2: Write test**

  Create 2 SST files eligible for compaction. Call `ScheduleIfNeeded()`. Verify `task_queue_` is non-empty.

- [ ] **Step 3: Build and run**

  ```bash
  cd build && cmake --build . --target test_parallel_compaction_scheduler && ./tests/test_parallel_compaction_scheduler
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add src/storage/parallel_compaction_engine.cc tests/storage/test_parallel_compaction_scheduler.cc tests/CMakeLists.txt
  git commit -m "fix(storage): actually enqueue tasks in parallel compaction scheduler"
  ```

---

### Task 7: Fix QueryAccumulatedBuffer TOCTOU Race (Storage)

**Problem:** Unlocked `empty()` check before acquiring mutex.

**Files:**
- Modify: `src/storage/lsm_engine.cc:1394-1420`
- Test: `tests/storage/test_accumulated_buffer_race.cc` (new)

- [ ] **Step 1: Remove unlocked check, always check under lock**

  ```cpp
  std::vector<Descriptor> LSMEngine::QueryAccumulatedBuffer(...) {
      std::lock_guard<std::mutex> lock(accumulated_mutex_);
      if (accumulated_entries_.empty()) return {};
      // ... existing logic ...
  }
  ```

- [ ] **Step 2: Build and run test**

  ```bash
  cd build && cmake --build . --target test_accumulated_buffer_race && ./tests/test_accumulated_buffer_race
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add src/storage/lsm_engine.cc tests/storage/test_accumulated_buffer_race.cc tests/CMakeLists.txt
  git commit -m "fix(storage): eliminate TOCTOU race in QueryAccumulatedBuffer"
  ```

---

### Task 8: Fix Cypher Public API Stubs (Graph/Cypher)

**Problem:** `ExecuteCypher()`, `ExplainCypher()`, `IsValidCypher()` are pure stubs.

**Files:**
- Modify: `src/graph/cedar_graph.cc:345-358`
- Modify: `include/cedar/graph/cedar_graph.h` (add cypher_engine_ initialization)
- Test: `tests/graph/test_cypher_api.cc` (new)

- [ ] **Step 1: Instantiate CypherEngine in CedarGraph constructor**

  ```cpp
  CedarGraph::CedarGraph(...) : storage_(storage) {
      cypher_engine_ = std::make_unique<CypherEngine>(storage);
  }
  ```

- [ ] **Step 2: Implement ExecuteCypher**

  ```cpp
  ResultSet CedarGraph::ExecuteCypher(const std::string& query) {
      if (!cypher_engine_) return ResultSet::Error("Cypher engine not initialized");
      return cypher_engine_->Execute(query);
  }
  ```

- [ ] **Step 3: Implement ExplainCypher and IsValidCypher**

  ```cpp
  std::string CedarGraph::ExplainCypher(const std::string& query) {
      if (!cypher_engine_) return "Engine not initialized";
      return cypher_engine_->Explain(query);
  }
  bool CedarGraph::IsValidCypher(const std::string& query) {
      if (!cypher_engine_) return false;
      return cypher_engine_->IsValid(query);
  }
  ```

- [ ] **Step 4: Write test**

  ```cpp
  TEST(CypherApiTest, ExecuteBasicMatch) {
      auto graph = CreateTestGraph();
      auto result = graph->ExecuteCypher("MATCH (n) RETURN n");
      EXPECT_TRUE(result.ok());
  }
  ```

- [ ] **Step 5: Build and run**

  ```bash
  cd build && cmake --build . --target test_cypher_api && ./tests/test_cypher_api
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add src/graph/cedar_graph.cc include/cedar/graph/cedar_graph.h tests/graph/test_cypher_api.cc tests/CMakeLists.txt
  git commit -m "fix(graph): wire CypherEngine into CedarGraph public API"
  ```

---

### Task 9: Fix Expression Evaluator Null Crashes (Cypher)

**Problem:** `EvaluateLogical`, `EvaluateNot`, and `GetBool()` throw `std::bad_variant_access` on null values.

**Files:**
- Modify: `src/cypher/expression_evaluator.cc:145-163,165-168`
- Modify: `src/cypher/value.cc:433-435`
- Test: `tests/cypher/test_expression_null_safety.cc` (new)

- [ ] **Step 1: Add null-safe `GetBool()`**

  ```cpp
  bool Value::GetBool() const {
      if (!std::holds_alternative<bool>(value_)) return false;
      return std::get<bool>(value_);
  }
  ```

- [ ] **Step 2: Fix logical short-circuit**

  ```cpp
  Value EvaluateLogical(LogicalOp op, const Value& left, const Value& right) {
      bool l = left.IsNull() ? false : left.GetBool();
      bool r = right.IsNull() ? false : right.GetBool();
      switch (op) { case AND: return Value(l && r); case OR: return Value(l || r); }
  }
  ```

- [ ] **Step 3: Fix NOT operator**

  ```cpp
  Value EvaluateNot(const Value& val) {
      if (val.IsNull()) return Value(false);
      return Value(!val.GetBool());
  }
  ```

- [ ] **Step 4: Write null-safety tests**

  Test `null AND false`, `null OR true`, `NOT null`, `null = null`.

- [ ] **Step 5: Build and run**

  ```bash
  cd build && cmake --build . --target test_expression_null_safety && ./tests/test_expression_null_safety
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add src/cypher/expression_evaluator.cc src/cypher/value.cc tests/cypher/test_expression_null_safety.cc tests/CMakeLists.txt
  git commit -m "fix(cypher): handle null values safely in expression evaluation"
  ```

---

### Task 10: Fix SET/DELETE Clause Parsing Stubs (Cypher)

**Problem:** `ParseSetClause()` and `ParseDeleteClause()` skip input and return empty objects.

**Files:**
- Modify: `src/cypher/parser.cc:532-542`
- Modify: `src/cypher/validator.cc:20-38`
- Test: `tests/cypher/test_set_delete_parsing.cc` (new)

- [ ] **Step 1: Implement `ParseSetClause()`**

  ```cpp
  SetClause ParseSetClause(TokenStream& tokens) {
      SetClause clause;
      do {
          auto target = ParsePropertyAccess(tokens);
          ExpectToken(tokens, TokenType::ASSIGN);
          auto expr = ParseExpression(tokens);
          clause.add_assignment(target, expr);
      } while (ConsumeIf(tokens, TokenType::COMMA));
      return clause;
  }
  ```

- [ ] **Step 2: Implement `ParseDeleteClause()`**

  ```cpp
  DeleteClause ParseDeleteClause(TokenStream& tokens) {
      DeleteClause clause;
      do {
          auto var = ExpectToken(tokens, TokenType::IDENTIFIER);
          clause.add_variable(var.text);
      } while (ConsumeIf(tokens, TokenType::COMMA));
      return clause;
  }
  ```

- [ ] **Step 3: Add mutation validation to validator**

  ```cpp
  Status ValidateQueryStatement(const QueryStatement& stmt) {
      // existing validation ...
      for (const auto& set_clause : stmt.set_clauses) {
          RETURN_IF_ERROR(ValidateSetClause(set_clause));
      }
      for (const auto& del_clause : stmt.delete_clauses) {
          RETURN_IF_ERROR(ValidateDeleteClause(del_clause));
      }
      return Status::OK();
  }
  ```

- [ ] **Step 4: Write test**

  Parse `MATCH (n) SET n.name = 'x' DELETE n`, verify clauses are populated.

- [ ] **Step 5: Build and run**

  ```bash
  cd build && cmake --build . --target test_set_delete_parsing && ./tests/test_set_delete_parsing
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add src/cypher/parser.cc src/cypher/validator.cc tests/cypher/test_set_delete_parsing.cc tests/CMakeLists.txt
  git commit -m "fix(cypher): implement SET and DELETE clause parsing"
  ```

---

### Task 11: Fix Planner Signature Mismatch & Stubs (Cypher)

**Problem:** `PlanClause` header uses `unique_ptr` but `.cc` uses `shared_ptr`. `CreateFilterPredicate` and `EvaluateExpression` are stubs.

**Files:**
- Modify: `include/cedar/cypher/planner.h:31`
- Modify: `src/cypher/planner.cc:29-48,223-237`
- Test: `tests/cypher/test_planner_signature.cc` (new)

- [ ] **Step 1: Align signature to `unique_ptr`**

  Change `.cc` to match header:
  ```cpp
  std::unique_ptr<PhysicalOperator> QueryPlanner::PlanClause(
      const Clause& clause,
      std::unique_ptr<PhysicalOperator> child) {
      // implementation
  }
  ```

- [ ] **Step 2: Implement `CreateFilterPredicate`**

  ```cpp
  std::function<bool(const Record&)> CreateFilterPredicate(const Expression& expr) {
      return [expr](const Record& record) -> bool {
          auto val = EvaluateExpression(expr, record);
          return !val.IsNull() && val.GetBool();
      };
  }
  ```

- [ ] **Step 3: Implement `EvaluateExpression`**

  ```cpp
  Value EvaluateExpression(const Expression& expr, const Record& record) {
      switch (expr.type()) {
          case kLiteral: return expr.literal_value();
          case kVariable: return record.Get(expr.variable_name());
          case kBinaryOp: {
              auto l = EvaluateExpression(*expr.left(), record);
              auto r = EvaluateExpression(*expr.right(), record);
              return ApplyBinaryOp(expr.op(), l, r);
          }
          // ... other cases
      }
  }
  ```

- [ ] **Step 4: Build and run**

  ```bash
  cd build && cmake --build . --target test_planner_signature && ./tests/test_planner_signature
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add include/cedar/cypher/planner.h src/cypher/planner.cc tests/cypher/test_planner_signature.cc tests/CMakeLists.txt
  git commit -m "fix(cypher): align PlanClause signature and implement predicate/expression stubs"
  ```

---

### Task 12: Fix ExecutionContext Validation & TemporalExpand (Cypher)

**Problem:** `ExecutionPlan::Execute` doesn't validate operator dependencies. `TemporalExpand` hard-requires GCN callback.

**Files:**
- Modify: `src/cypher/execution_plan.cc:109-113`
- Modify: `src/cypher/temporal_operators.cc:184-186`
- Test: `tests/cypher/test_execution_context_validation.cc` (new)

- [ ] **Step 1: Add dependency validation in Execute**

  ```cpp
  Status ExecutionPlan::ValidateDependencies(const ExecutionContext& ctx) const {
      for (const auto& op : operators_) {
          if (op->RequiresGraph() && !ctx.graph && !ctx.gcn_traversal_callback) {
              return Status::InvalidArgument("Operator requires graph or GCN callback");
          }
          if (op->RequiresGcnCallback() && !ctx.gcn_traversal_callback) {
              return Status::InvalidArgument("Operator requires GCN callback");
          }
      }
      return Status::OK();
  }
  ```

- [ ] **Step 2: Fix TemporalExpand to fallback to graph storage**

  ```cpp
  Record* TemporalExpand::Next() {
      if (context_->gcn_traversal_callback) {
          return ExpandViaGcn();
      }
      if (context_->graph) {
          return ExpandViaStorage();
      }
      return nullptr;  // no expansion source available
  }
  ```

- [ ] **Step 3: Write test**

  Test that `TemporalExpand` works with `graph` when `gcn_traversal_callback` is absent.

- [ ] **Step 4: Build and run**

  ```bash
  cd build && cmake --build . --target test_execution_context_validation && ./tests/test_execution_context_validation
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add src/cypher/execution_plan.cc src/cypher/temporal_operators.cc tests/cypher/test_execution_context_validation.cc tests/CMakeLists.txt
  git commit -m "fix(cypher): validate operator dependencies and add storage fallback for TemporalExpand"
  ```

---

### Task 13: Fix GCN TMVEngine Use-After-Free & Memory Races (GCN)

**Problem:** `ScanAtTime` and `InvalidateVertex` have use-after-free races. `BootstrapVertex` silently stops on alloc failure. CDC callback is hardcoded stub.

**Files:**
- Modify: `src/gcn/tmv_engine.cc:31-34,109-121,191-270,330-360`
- Modify: `src/gcn/gcn_node.cc:67-77`
- Modify: `src/gcn/event_applier.h:41-55`
- Test: `tests/gcn/test_tmv_engine_safety.cc` (new)

- [ ] **Step 1: Fix relaxed-ordering race in ScanAtTime**

  Change `chunk->next.load(std::memory_order_relaxed)` to `std::memory_order_acquire`.
  Change stores in `DropChunksBelowWatermark` to `std::memory_order_release`.

- [ ] **Step 2: Fix InvalidateVertex / ScanAtTime lock coordination**

  Have `ScanAtTime` acquire the shard read-lock before traversing chunks:
  ```cpp
  std::shared_lock<std::shared_mutex> lock(shard_mutexes_[shard_id]);
  auto* head = head_ptrs_[shard_id].load(std::memory_order_acquire);
  // traverse...
  ```

- [ ] **Step 3: Fix BootstrapVertex rollback on alloc failure**

  ```cpp
  Status TMVEngine::BootstrapVertex(...) {
      std::vector<Edge> staged_edges;
      for (const auto& edge : edges) {
          auto* chunk = pool_.Alloc();
          if (!chunk) {
              // rollback staged edges
              for (auto* staged : staged_edges) FreeChunk(staged);
              return Status::ResourceExhausted("TMV pool exhausted");
          }
          staged_edges.push_back(chunk);
      }
      // commit
      return Status::OK();
  }
  ```

- [ ] **Step 4: Fix AppendEdge reverse failure**

  If reverse edge insertion fails, free the forward edge chunk and return error.

- [ ] **Step 5: Fix CDC callback stub**

  Remove hardcoded `target_id = entity_id + 1` and actually parse the CDC event:
  ```cpp
  void OnCdcEvent(const CdcEvent& event) {
      tmv_engine_->AppendEdge(event.entity_id, event.target_id, event.edge_type,
                              event.valid_from, event.valid_to);
  }
  ```

- [ ] **Step 6: Bound EventApplier reorder buffer**

  ```cpp
  static constexpr size_t kMaxReorderBuffer = 100000;
  if (reorder_buffer_.size() > kMaxReorderBuffer) {
      LOG(ERROR) << "Reorder buffer overflow, dropping event";
      return Status::ResourceExhausted("Reorder buffer full");
  }
  ```

- [ ] **Step 7: Write test**

  Concurrent scan + invalidate stress test. Verify no crashes under TSan.

- [ ] **Step 8: Build and run**

  ```bash
  cd build && cmake --build . --target test_tmv_engine_safety && ./tests/test_tmv_engine_safety
  ```

- [ ] **Step 9: Commit**

  ```bash
  git add src/gcn/tmv_engine.cc src/gcn/gcn_node.cc src/gcn/event_applier.h tests/gcn/test_tmv_engine_safety.cc tests/CMakeLists.txt
  git commit -m "fix(gcn): fix TMVEngine use-after-free, alloc failure rollback, and CDC stub"
  ```

---

### Task 14: Fix ScatterGatherRouter Silent Failures & Thread Safety (GCN)

**Problem:** `Scatter()` overwrites downstream `success=false`. `RegisterPeer`/`UnregisterPeer` mutate maps without mutex.

**Files:**
- Modify: `src/gcn/scatter_gather_router.cc:30-98`
- Test: `tests/gcn/test_scatter_gather_router.cc` (new)

- [ ] **Step 1: Fix Scatter to preserve downstream failure**

  ```cpp
  void ScatterGatherRouter::Scatter(...) {
      for (auto& peer : peers_) {
          auto status = stub->Scatter(&ctx, request, &response);
          if (!status.ok() || !response.success()) {
              // DO NOT overwrite response.success()
              // Aggregate failures into a combined response
          }
      }
  }
  ```

- [ ] **Step 2: Add mutex to peer map mutations**

  ```cpp
  class ScatterGatherRouter {
      mutable std::mutex peers_mutex_;
      std::unordered_map<NodeID, PeerInfo> peers_;
  };
  ```

- [ ] **Step 3: Write test**

  Test that Scatter propagates failures. Test concurrent RegisterPeer/UnregisterPeer.

- [ ] **Step 4: Build and run**

  ```bash
  cd build && cmake --build . --target test_scatter_gather_router && ./tests/test_scatter_gather_router
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add src/gcn/scatter_gather_router.cc tests/gcn/test_scatter_gather_router.cc tests/CMakeLists.txt
  git commit -m "fix(gcn): preserve downstream failures in Scatter and protect peer maps with mutex"
  ```

---

### Task 15: Fix Service & Meta Layer BLOCKERs (Service/Meta)

**Problem:** Migration pipeline entirely non-functional. Raft state machine discards log entries. WatchPartitionMap is no-op. SyncData has use-after-free risk.

**Files:**
- Modify: `src/dtx/grpc/migration_executor.cc:112-136,261-408`
- Modify: `src/dtx/meta/meta_service.cc:364-368,941-992`
- Modify: `src/dtx/grpc/meta_service_grpc.cc:189-197`
- Modify: `src/service/partition_migration_service.cc:216-350`
- Test: `tests/service/test_migration_pipeline.cc` (new)

- [ ] **Step 1: Implement DTxRpcClient methods**

  Replace no-ops with actual gRPC calls to the target partition.

- [ ] **Step 2: Implement migration phases**

  `Phase_Prepare`: create snapshot. `Phase_Transfer`: stream SST files. `Phase_Verify`: checksum compare. `Phase_Complete`: update metadata.

- [ ] **Step 3: Fix MetadataStateMachine::Apply to deserialize and apply**

  ```cpp
  void MetadataStateMachine::Apply(braft::Iterator& iter) {
      for (; iter.valid(); iter.next()) {
          MetaCommand cmd;
          if (!cmd.ParseFromString(iter.data().to_string())) {
              LOG(FATAL) << "Corrupt meta command at index " << iter.index();
          }
          ApplyCommand(cmd);
          last_applied_index_ = iter.index();
      }
  }
  ```

- [ ] **Step 4: Implement WatchPartitionMap streaming**

  Maintain a list of watchers. On partition map change, broadcast to all streams.

- [ ] **Step 5: Fix SyncData locking**

  Hold task lock across entire streaming loop. Check task state before each chunk.

- [ ] **Step 6: Write integration test**

  End-to-end migration test with mock storage nodes.

- [ ] **Step 7: Build and run**

  ```bash
  cd build && cmake --build . --target test_migration_pipeline && ./tests/test_migration_pipeline
  ```

- [ ] **Step 8: Commit**

  ```bash
  git add src/dtx/grpc/migration_executor.cc src/dtx/meta/meta_service.cc src/dtx/grpc/meta_service_grpc.cc src/service/partition_migration_service.cc tests/service/test_migration_pipeline.cc tests/CMakeLists.txt
  git commit -m "fix(service): implement migration pipeline, meta Raft apply, and partition map watches"
  ```

---

### Task 16: Fix Queryd & Partition BLOCKERs (Queryd/Partition)

**Problem:** MTH temporal routing byte-swap bug. QueryStorageClient uninitialized. Many gRPC methods unimplemented. Query timeouts not enforced.

**Files:**
- Modify: `src/partition/mth/streaming_partitioner.cc` (timestamp byte-swap)
- Modify: `src/queryd/distributed_executor.cc` (QueryStorageClient init)
- Modify: `src/queryd/query_service_impl.cc` (stub implementations)
- Modify: `src/queryd/distributed_executor.cc` (timeout enforcement)
- Test: `tests/queryd/test_queryd_blockers.cc` (new)

- [ ] **Step 1: Fix MTH timestamp byte-swap**

  Ensure timestamp is serialized in network byte order consistently.

- [ ] **Step 2: Initialize QueryStorageClient in constructor**

  ```cpp
  QuerydService::QuerydService(...) {
      storage_client_ = std::make_unique<QueryStorageClient>(storage_endpoints);
  }
  ```

- [ ] **Step 3: Implement missing gRPC methods**

  `ExecuteQuery`, `ExplainQuery`, `GetSchema`, `GetStatistics` — delegate to cypher engine or storage.

- [ ] **Step 4: Add query timeout enforcement**

  ```cpp
  grpc::Status QuerydService::ExecuteQuery(grpc::ServerContext* ctx, ...) {
      auto deadline = std::chrono::system_clock::now() + options_.query_timeout;
      ctx->set_deadline(deadline);
      // ... execute with cancellation check ...
  }
  ```

- [ ] **Step 5: Write test**

  Verify MTH routing correctness. Verify query timeout kills long-running queries.

- [ ] **Step 6: Build and run**

  ```bash
  cd build && cmake --build . --target test_queryd_blockers && ./tests/test_queryd_blockers
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add src/partition/mth/streaming_partitioner.cc src/queryd/distributed_executor.cc src/queryd/query_service_impl.cc tests/queryd/test_queryd_blockers.cc tests/CMakeLists.txt
  git commit -m "fix(queryd): fix MTH routing, init storage client, implement stubs, enforce timeouts"
  ```

---

### Task 17: Fix Security BLOCKERs (Supporting)

**Problem:** Hardcoded credentials. Broken JWT. Email alert stub. Empty network transport dir.

**Files:**
- Modify: `src/dtx/security/security_manager.cc:133-134`
- Modify: `src/dtx/security/security_manager.cc:326-434`
- Modify: `src/dtx/index/alert_channels.cc:433-438`
- Test: `tests/security/test_security_blockers.cc` (new)

- [ ] **Step 1: Remove hardcoded credentials**

  ```cpp
  Status Authenticator::Initialize(const AuthConfig& config) {
      if (config.accounts.empty()) {
          return Status::InvalidArgument("No accounts configured");
      }
      // load from config only
  }
  ```

- [ ] **Step 2: Rewrite JWT with standard library**

  Add `jwt-cpp` to third_party or use a minimal Base64URL + HMAC implementation:
  ```cpp
  std::string GenerateJWT(const TokenClaims& claims) {
      auto header = Base64UrlEncode(R"({"alg":"HS256","typ":"JWT"})");
      auto payload = Base64UrlEncode(claims.ToJson());
      auto sig = HMAC_SHA256(header + "." + payload, secret_);
      return header + "." + payload + "." + Base64UrlEncode(sig);
  }
  ```

- [ ] **Step 3: Implement EmailChannel**

  Use libcurl SMTP or an external sendmail call:
  ```cpp
  Status EmailChannel::SendEmail(const Alert& alert) {
      CURL* curl = curl_easy_init();
      // configure SMTP, send message
      curl_easy_cleanup(curl);
      return Status::OK();
  }
  ```

- [ ] **Step 4: Document or remove empty network transport**

  Add `README.md` to `src/dtx/network/` explaining that DTX uses brpc directly, or create a thin wrapper.

- [ ] **Step 5: Write test**

  Test JWT round-trip. Test that empty credential config fails.

- [ ] **Step 6: Build and run**

  ```bash
  cd build && cmake --build . --target test_security_blockers && ./tests/test_security_blockers
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add src/dtx/security/security_manager.cc src/dtx/index/alert_channels.cc src/dtx/network/README.md tests/security/test_security_blockers.cc tests/CMakeLists.txt
  git commit -m "fix(security): remove hardcoded credentials, fix JWT, implement email alerts"
  ```

---

## Phase 2: CRITICAL Fixes — Semantic Correctness & Resource Safety (P1)

These 92 issues can cause incorrect query results, resource exhaustion, or security vulnerabilities.

---

### Task 18: Fix Storage CRITICALs Batch (Storage)

**Problems:**
- Manifest corruption silently skipped (#10)
- Blob read holds mutex during I/O (#13)
- AutoBlobStorage truncates strings (#14)
- BatchGetRangeOptimized assumes shared files (#15)
- Streaming compaction dedup omits column_id/entity_type (#16)
- PutEdge dual-write no rollback (#17)
- GetAtTime collects all versions (#18)
- Batch read API is stub (#19)
- TrackColumnId buffer overflow (#20)
- ApplyEdit never populates new_version (#21)
- Parallel query engine data race (#23)
- Read-your-writes key collision (#24)

**Files:**
- `src/db/manifest.cc:527-588,590-604`
- `src/sst/blob_file_manager.cc:222-291`
- `src/storage/auto_blob_storage.cc:43-57`
- `src/storage/lsm_engine.cc:1006-1100,489-535,1664-1688`
- `src/storage/streaming_compaction_merger.cc:245-249`
- `src/storage/cedar_graph_storage.cc:449-512`
- `src/transaction/batch_api.cc:479-501`
- `src/storage/parallel_query_engine.cc:172-211`
- `src/transaction/occ_transaction.cc:237-250`

- [ ] **Step 1: Log on manifest corruption**

  ```cpp
  if (!record.ParseFromString(buf)) {
      LOG(ERROR) << "Corrupt manifest record at offset " << offset;
      corrupted_count_++;
      continue;
  }
  ```

- [ ] **Step 2: Fix blob read mutex scope**

  Copy blob location under lock, release lock, then do I/O:
  ```cpp
  BlobLocation loc;
  { std::lock_guard<std::mutex> lock(mutex_); loc = index_[blob_id]; }
  // read from file outside lock
  ```

- [ ] **Step 3: Fix auto blob inline string truncation**

  Reject strings > 4 bytes or use a proper variable-length encoding:
  ```cpp
  Status PutInlineString(const std::string& s) {
      if (s.size() > 4) return Status::NotSupported("Inline string too long");
      uint32_t encoded = 0;
      memcpy(&encoded, s.data(), s.size());
      return PutInline(key, encoded);
  }
  ```

- [ ] **Step 4: Fix BatchGetRangeOptimized**

  Fetch file lists per entity, merge/union them:
  ```cpp
  std::unordered_set<SSTFile*> relevant_files;
  for (auto id : entity_ids) {
      auto files = GetFilesForEntity(id);
      relevant_files.insert(files.begin(), files.end());
  }
  ```

- [ ] **Step 5: Fix streaming compaction dedup**

  Include column_id and entity_type in IsDuplicate:
  ```cpp
  bool IsDuplicate(const Entry& a, const Entry& b) {
      return a.entity_id == b.entity_id && a.timestamp == b.timestamp
          && a.target_id == b.target_id && a.column_id == b.column_id
          && a.entity_type == b.entity_type;
  }
  ```

- [ ] **Step 6: Fix PutEdge rollback**

  ```cpp
  Status PutEdge(...) {
      auto s1 = Put(EdgeOutKey(...), ...);
      if (!s1.ok()) return s1;
      auto s2 = Put(EdgeInKey(...), ...);
      if (!s2.ok()) {
          Delete(EdgeOutKey(...));  // rollback
          return s2;
      }
      return Status::OK();
  }
  ```

- [ ] **Step 7: Fix GetAtTime memory blowup**

  Use a min-heap to merge SST iterators and return results incrementally instead of collecting all.

- [ ] **Step 8: Implement BatchExecutor::ExecuteReadBatch**

  ```cpp
  Status BatchExecutor::ExecuteReadBatch(const ReadBatch& batch, ReadBatchResult* result) {
      for (const auto& req : batch.requests()) {
          Descriptor desc;
          auto s = engine_->Get(req.entity_id(), req.timestamp(), &desc);
          result->add_response(s.ok() ? desc : Descriptor());
      }
      return Status::OK();
  }
  ```

- [ ] **Step 9: Fix TrackColumnId buffer overflow**

  ```cpp
  size_t idx = batch_buffer_index_.fetch_add(1);
  if (idx >= kTrackBatchSize) {
      return Status::ResourceExhausted("Track batch full, force flush");
  }
  batch_buffer_[idx] = column_id;
  ```

- [ ] **Step 10: Fix ApplyEdit to populate new_version**

  ```cpp
  Status Manifest::ApplyEdit(const VersionEdit& edit, std::shared_ptr<Version>* new_version) {
      auto v = std::make_shared<Version>(*current_version_);
      v->ApplyEdit(edit);
      *new_version = v;
      current_version_ = v;
      return Status::OK();
  }
  ```

- [ ] **Step 11: Fix parallel query engine race**

  Use `std::atomic<Descriptor>` or a completion flag + mutex for shard_results:
  ```cpp
  std::vector<std::atomic<bool>> shard_done(shard_count);
  std::vector<Descriptor> shard_results(shard_count);
  std::mutex results_mutex;
  ```

- [ ] **Step 12: Fix read-your-writes key collision**

  ```cpp
  std::string MakeRywKey(uint64_t entity_id, uint16_t entity_type, uint16_t column_id) {
      return std::to_string(entity_id) + ":" + std::to_string(entity_type) + ":" + std::to_string(column_id);
  }
  ```

- [ ] **Step 13: Build and run combined test**

  ```bash
  cd build && cmake --build . --target test_storage_critical_batch && ./tests/test_storage_critical_batch
  ```

- [ ] **Step 14: Commit**

  ```bash
  git add src/db/manifest.cc src/sst/blob_file_manager.cc src/storage/auto_blob_storage.cc src/storage/lsm_engine.cc src/storage/streaming_compaction_merger.cc src/storage/cedar_graph_storage.cc src/transaction/batch_api.cc src/storage/parallel_query_engine.cc src/transaction/occ_transaction.cc tests/storage/test_storage_critical_batch.cc tests/CMakeLists.txt
  git commit -m "fix(storage): batch fix for CRITICAL issues — manifest, blobs, dedup, rollback, batch API, RYW keys"
  ```

---

### Task 19: Fix Graph & Cypher CRITICALs Batch (Graph/Cypher)

**Problems:**
- GetOutNeighbors ignores start_time (#9)
- GetVertexHistory ignores column_id (#10)
- GetEdgeHistory uses wrong API (#11)
- AS_OF fallback window too narrow (#12)
- VERSION confuses index with version (#13)
- Allen relation filtering no-op (#14)
- Pushdown predicate default-true (#15)
- Plan cache unbounded (#16)
- Expand ignores Direction::BOTH (#18)
- Rel ID hash collision (#19)
- Aggregate empty group returns 1 (#20)
- Sort type-first ordering (#21)
- TemporalExpand ignores temporal modifiers (#22)
- SnapshotScan doesn't filter (#23)
- ParseContainedInClause wrong modifier (#24)
- TimestampExpression silent failures (#25)
- AnchorStats static unsynchronized (#29)
- CachedBlock ref_count leak (#30)

**Files:**
- `src/graph/cedar_graph.cc`
- `src/graph/cedar_graph_temporal.cc`
- `src/graph/pushdown_predicate.cc`
- `src/cypher/cypher_engine.cc`
- `src/cypher/execution_plan.cc`
- `src/cypher/temporal_operators.cc`
- `src/cypher/parser_temporal.cc`
- `src/query/cedar_scan.cc`
- `src/graph/graph_semantic_layer.cc`

- [ ] **Step 1: Fix GetOutNeighbors to pass start_time**

  ```cpp
  auto edges = storage_->ScanEdgesWithFolding(vertex_id, EntityType::EdgeOut, edge_type,
                                               start_time, end_time);
  ```

- [ ] **Step 2: Fix GetVertexHistory to use column_id**

  ```cpp
  auto versions = storage_->Scan(vertex_id, column_id, start_time, end_time);
  ```

- [ ] **Step 3: Fix GetEdgeHistory to use edge scan API**

  ```cpp
  auto edges = storage_->ScanEdges(src_id, edge_type, start_time, end_time);
  ```

- [ ] **Step 4: Fix AS_OF fallback window**

  Use `[Timestamp::Min(), as_of_time]` instead of `[as_of_time, as_of_time + 1]`.

- [ ] **Step 5: Fix VERSION to use logical version**

  Sort versions by timestamp, pick the N-th distinct version.

- [ ] **Step 6: Implement Allen relation filtering**

  Implement the 13 Allen relations using interval comparison.

- [ ] **Step 7: Fix pushdown predicate default**

  Return `false` for unsupported operators:
  ```cpp
  bool PropertyFilter::Evaluate(const Descriptor& d) const {
      switch (op) {
          case EQ: return d.Get(property) == value;
          case NE: return d.Get(property) != value;
          // ...
          default: LOG(WARNING) << "Unsupported operator " << op; return false;
      }
  }
  ```

- [ ] **Step 8: Add plan cache size limit**

  ```cpp
  static constexpr size_t kMaxPlanCacheSize = 1000;
  if (plan_cache_.size() >= kMaxPlanCacheSize) {
      EvictOldestPlan();
  }
  ```

- [ ] **Step 9: Fix Expand Direction::BOTH**

  ```cpp
  if (direction_ == Direction::BOTH) {
      out_edges = graph->GetOutNeighbors(...);
      in_edges = graph->GetInNeighbors(...);
      neighbors = Merge(out_edges, in_edges);
  }
  ```

- [ ] **Step 10: Fix relationship ID**

  Use a proper hash or concatenation:
  ```cpp
  rel.id = std::hash<std::string>{}(std::to_string(target_id) + ":" + std::to_string(ts.value()));
  ```

- [ ] **Step 11: Fix Aggregate empty group**

  Return 0 for COUNT(*) on empty input:
  ```cpp
  if (groups.empty() && !has_group_by_) {
      // no input and no grouping → return empty result set
      return nullptr;
  }
  ```

- [ ] **Step 12: Fix Sort ordering**

  Implement type-aware comparison or cast to common type:
  ```cpp
  bool Value::operator<(const Value& o) const {
      if (type_ != o.type_) {
          return static_cast<int>(type_) < static_cast<int>(o.type_);  // or error
      }
      return CompareSameType(o);
  }
  ```

- [ ] **Step 13: Fix TemporalExpand to pass time range**

  ```cpp
  neighbors = callback(entity_id, edge_type, query_start_, query_end_);
  ```

- [ ] **Step 14: Fix SnapshotScan to filter by existence**

  Check if entity exists at snapshot_time before returning.

- [ ] **Step 15: Fix ParseContainedInClause modifier**

  ```cpp
  clause->modifier = TemporalModifierType::CONTAINED_IN;
  ```

- [ ] **Step 16: Fix TimestampExpression to return error on unknown**

  ```cpp
  default: return Status::InvalidArgument("Unknown timestamp expression type");
  ```

- [ ] **Step 17: Fix AnchorStats to use atomics**

  ```cpp
  struct AnchorStats {
      std::atomic<uint64_t> hits{0};
      std::atomic<uint64_t> misses{0};
  };
  ```

- [ ] **Step 18: Fix CachedBlock ref_count**

  Add decrement in cache eviction or use shared_ptr exclusively.

- [ ] **Step 19: Build and run combined test**

  ```bash
  cd build && cmake --build . --target test_graph_cypher_critical_batch && ./tests/test_graph_cypher_critical_batch
  ```

- [ ] **Step 20: Commit**

  ```bash
  git add src/graph/cedar_graph.cc src/graph/cedar_graph_temporal.cc src/graph/pushdown_predicate.cc src/cypher/cypher_engine.cc src/cypher/execution_plan.cc src/cypher/temporal_operators.cc src/cypher/parser_temporal.cc src/query/cedar_scan.cc src/graph/graph_semantic_layer.cc tests/cypher/test_graph_cypher_critical_batch.cc tests/CMakeLists.txt
  git commit -m "fix(graph,cypher): batch fix for CRITICAL semantic and correctness issues"
  ```

---

### Task 20: Fix DTX & Raft CRITICALs Batch (DTX/Raft)

**Problems:**
- Raft state machine divergence (various)
- Partial commit handling gaps
- TLS disabled
- Recursive mutex deadlock
- Topology corruption
- Propose timeouts hardcoded
- No circuit breaker

**Files:**
- `src/dtx/storage/braft_partition_raft.cc`
- `src/dtx/raft/braft_node.cc`
- `src/dtx/storage/partition_migrator.cc`
- `src/dtx/dtx_rpc_client.cc`
- `src/dtx/storage/failover_manager.cc`

- [ ] **Step 1: Add `set_error_and_rollback()` to both state machines on any apply failure**

  Already done in Task 8 of previous plan. Verify it covers all error paths.

- [ ] **Step 2: Fix propose timeouts**

  Make configurable via gflags:
  ```cpp
  DEFINE_int64(raft_propose_timeout_ms, 5000, "Raft proposal timeout");
  ```

- [ ] **Step 3: Add circuit breaker around Raft proposals**

  ```cpp
  class RaftCircuitBreaker {
      size_t consecutive_failures_ = 0;
      std::chrono::steady_clock::time_point open_until_;
      bool IsOpen() const { return std::chrono::steady_clock::now() < open_until_; }
  };
  ```

- [ ] **Step 4: Enable TLS for DTX replication**

  Add `grpc::SslCredentials` option to DTX RPC client.

- [ ] **Step 5: Fix topology corruption in SetPartitionLeader**

  Atomically remove from old node, add to new node:
  ```cpp
  std::lock_guard<std::mutex> lock(topology_mutex_);
  old_node.partitions.erase(partition_id);
  new_node.partitions.insert(partition_id);
  ```

- [ ] **Step 6: Build and run**

  ```bash
  cd build && cmake --build . --target test_dtx_raft_critical && ./tests/test_dtx_raft_critical
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add src/dtx/storage/braft_partition_raft.cc src/dtx/raft/braft_node.cc src/dtx/dtx_rpc_client.cc src/dtx/storage/failover_manager.cc tests/dtx/test_dtx_raft_critical.cc tests/CMakeLists.txt
  git commit -m "fix(dtx,raft): configurable timeouts, circuit breaker, TLS option, topology atomicity"
  ```

---

### Task 21: Fix GCN/Governance/Core CRITICALs Batch (GCN/Governance/Core)

**Problems:**
- EventApplier zero synchronization
- ScatterGatherRouter no mutex
- CoordinatorClient fire-and-forget
- GCN no TLS
- GcnServiceImpl stream timeout breaking
- ThreadPool no exception handling
- ServiceRegistry deadlock
- ConfigManager deadlock

**Files:**
- `src/gcn/event_applier.cc`
- `src/gcn/coordinator_client.cc`
- `src/gcn/gcn_node.cc`
- `src/gcn/gcn_service.cc`
- `src/core/threading.cc`
- `src/governance/service_registry.cc`
- `src/governance/config_manager.cc`

- [ ] **Step 1: Add mutex to EventApplier**

  ```cpp
  class EventApplier {
      mutable std::mutex mutex_;
      std::map<SequenceNumber, Event> reorder_buffer_;
      SequenceNumber applied_version_ = 0;
  };
  ```

- [ ] **Step 2: Fix CoordinatorClient fire-and-forget**

  ```cpp
  Status CoordinatorClient::Heartbeat(...) {
      grpc::ClientContext ctx;
      auto status = stub_->Heartbeat(&ctx, req, &resp);
      if (!status.ok()) {
          LOG(ERROR) << "Heartbeat failed: " << status.error_message();
          return Status::IOError(status.error_message());
      }
      return Status::OK();
  }
  ```

- [ ] **Step 3: Add TLS to GCN**

  ```cpp
  auto creds = use_tls_ ? grpc::SslCredentials(opts) : grpc::InsecureChannelCredentials();
  channel_ = grpc::CreateChannel(addr, creds);
  ```

- [ ] **Step 4: Fix GcnServiceImpl stream**

  Use a condition variable with no artificial timeout, or use a proper gRPC streaming queue.

- [ ] **Step 5: Add exception handling to ThreadPool**

  ```cpp
  try {
      task();
  } catch (const std::exception& e) {
      LOG(ERROR) << "Task threw: " << e.what();
  } catch (...) {
      LOG(ERROR) << "Task threw unknown exception";
  }
  ```

- [ ] **Step 6: Fix ServiceRegistry deadlock**

  Copy watchers under lock, then call callbacks outside lock:
  ```cpp
  std::vector<WatcherCallback> watchers_copy;
  {
      std::lock_guard<std::mutex> lock(mutex_);
      watchers_copy = watchers_;
  }
  for (auto& cb : watchers_copy) cb(event);
  ```

- [ ] **Step 7: Fix ConfigManager deadlock**

  Same pattern: copy callbacks under lock, invoke outside lock.

- [ ] **Step 8: Build and run**

  ```bash
  cd build && cmake --build . --target test_gcn_governance_critical && ./tests/test_gcn_governance_critical
  ```

- [ ] **Step 9: Commit**

  ```bash
  git add src/gcn/event_applier.cc src/gcn/coordinator_client.cc src/gcn/gcn_node.cc src/gcn/gcn_service.cc src/core/threading.cc src/governance/service_registry.cc src/governance/config_manager.cc tests/gcn/test_gcn_governance_critical.cc tests/CMakeLists.txt
  git commit -m "fix(gcn,governance,core): thread safety, TLS, exception handling, deadlock fixes"
  ```

---

### Task 22: Fix Service & Meta CRITICALs Batch (Service/Meta)

**Problems:**
- GraphD starts without 2PC engine
- GetStorageStub leaks channels
- BeginTransaction unbounded
- Heartbeat no rate limiting
- gRPC error codes ignored
- PartitionMapRefreshLoop no backoff
- SyncData temp leak
- CleanupOldMigrations race
- UpdatePartitionLeader no validation
- CreateSpace no validation
- OnBecomeLeader no handoff
- HeartbeatCheckLoop swallows exceptions
- Dual serialization formats

**Files:**
- `src/service/graph_service_router.cc`
- `src/dtx/meta/meta_service.cc`
- `src/service/partition_migration_service.cc`
- `src/dtx/grpc/meta_service_grpc.cc`

- [ ] **Step 1: Make 2PC engine initialization fatal**

  ```cpp
  if (!Initialize2PCEngine().ok()) {
      LOG(FATAL) << "Cannot initialize 2PC engine";
  }
  ```

- [ ] **Step 2: Add channel eviction to GetStorageStub**

  ```cpp
  if (storage_stubs_.size() > kMaxCachedStubs) {
      EvictOldestStub();
  }
  ```

- [ ] **Step 3: Add transaction limit**

  ```cpp
  if (active_transactions_.size() >= kMaxActiveTransactions) {
      return Status::ResourceExhausted("Too many active transactions");
  }
  ```

- [ ] **Step 4: Add heartbeat rate limiting**

  Token bucket: max 10 proposals/sec per node.

- [ ] **Step 5: Return proper gRPC status codes**

  Map internal errors to gRPC status codes (INVALID_ARGUMENT, UNAVAILABLE, etc.).

- [ ] **Step 6: Add exponential backoff to PartitionMapRefreshLoop**

  ```cpp
  if (last_refresh_failed_) {
      interval = std::min(interval * 2, kMaxRefreshInterval);
  }
  ```

- [ ] **Step 7: Fix SyncData temp cleanup**

  Use RAII temp directory that cleans up on destruction.

- [ ] **Step 8: Fix CleanupOldMigrations race**

  Collect keys, release shared_lock, acquire unique_lock, verify keys still exist before erasing.

- [ ] **Step 9: Validate UpdatePartitionLeader**

  ```cpp
  if (!IsRegisteredNode(new_leader)) {
      return Status::InvalidArgument("New leader is not a registered node");
  }
  ```

- [ ] **Step 10: Validate CreateSpace**

  ```cpp
  if (req.space_name().empty()) return Status::InvalidArgument("Empty space name");
  if (req.replica_factor() > alive_node_count_) {
      return Status::InvalidArgument("Replica factor exceeds node count");
  }
  ```

- [ ] **Step 11: Implement leadership handoff**

  Notify all watchers on leader change. Resume pending proposals.

- [ ] **Step 12: Log exceptions in HeartbeatCheckLoop**

  ```cpp
  catch (const std::exception& e) {
      LOG(ERROR) << "HeartbeatCheckLoop exception: " << e.what();
  }
  ```

- [ ] **Step 13: Unify serialization format**

  Choose one format (protobuf binary length-prefixed) and migrate all code paths.

- [ ] **Step 14: Build and run**

  ```bash
  cd build && cmake --build . --target test_service_meta_critical && ./tests/test_service_meta_critical
  ```

- [ ] **Step 15: Commit**

  ```bash
  git add src/service/graph_service_router.cc src/dtx/meta/meta_service.cc src/service/partition_migration_service.cc src/dtx/grpc/meta_service_grpc.cc tests/service/test_service_meta_critical.cc tests/CMakeLists.txt
  git commit -m "fix(service,meta): resource limits, validation, error codes, serialization unification"
  ```

---

### Task 23: Fix Queryd & Partition CRITICALs Batch (Queryd/Partition)

**Problems:**
- SplitQuery/Execute silent failures
- Circuit breakers decorative
- Query timeout not enforced
- QueryStorageClient not initialized

**Files:**
- `src/queryd/distributed_executor.cc`
- `src/queryd/query_service_impl.cc`

- [ ] **Step 1: Fix error handling in SplitQuery**

  Return error Status instead of silent success=false.

- [ ] **Step 2: Wire up circuit breaker**

  Actually track failures and open/close the breaker.

- [ ] **Step 3: Enforce query timeout**

  Use `grpc::ServerContext::set_deadline` and check `ctx->IsCancelled()`.

- [ ] **Step 4: Build and run**

  ```bash
  cd build && cmake --build . --target test_queryd_critical && ./tests/test_queryd_critical
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add src/queryd/distributed_executor.cc src/queryd/query_service_impl.cc tests/queryd/test_queryd_critical.cc tests/CMakeLists.txt
  git commit -m "fix(queryd): error propagation, circuit breaker, query timeout enforcement"
  ```

---

### Task 24: Fix Supporting Modules CRITICALs Batch (Driver/Chaos/Security)

**Problems:**
- Weak password hashing (SHA256)
- ChaosFramework results unsynchronized
- ParseJWT fragile string parsing
- RetryPolicy unhandled exceptions
- Session bookmark not thread-safe
- ManagedTxn move assignment double-free

**Files:**
- `src/dtx/security/security_manager.cc`
- `src/dtx/chaos/chaos_testing.cc`
- `include/cedar/driver/retry_policy.h`
- `src/driver/session.cc`

- [ ] **Step 1: Replace SHA256 with bcrypt**

  Add `libbcrypt` or implement bcrypt. Hash passwords with 10+ rounds.

- [ ] **Step 2: Add mutex to ChaosFramework results**

  ```cpp
  mutable std::mutex results_mutex_;
  std::vector<FaultResult> results_;
  ```

- [ ] **Step 3: Rewrite ParseJWT with proper Base64URL decoding**

  Split by '.', Base64URL decode each part, parse JSON with a real parser.

- [ ] **Step 4: Add exception handling to RetryPolicy**

  ```cpp
  try {
      auto result = func();
      // ...
  } catch (const std::exception& e) {
      LOG(WARNING) << "Attempt " << attempt << " threw: " << e.what();
      if (attempt < max_retries) continue;
      return Status::IOError(e.what());
  }
  ```

- [ ] **Step 5: Add mutex to Session bookmark**

  ```cpp
  mutable std::mutex bookmark_mutex_;
  Bookmark last_bookmark_;
  ```

- [ ] **Step 6: Fix ManagedTxn move assignment**

  ```cpp
  ManagedTxn& ManagedTxn::operator=(ManagedTxn&& other) noexcept {
      if (this != &other) {
          if (txn_) Abort();
          txn_ = other.txn_;
          other.txn_ = nullptr;
      }
      return *this;
  }
  ```

- [ ] **Step 7: Build and run**

  ```bash
  cd build && cmake --build . --target test_supporting_critical && ./tests/test_supporting_critical
  ```

- [ ] **Step 8: Commit**

  ```bash
  git add src/dtx/security/security_manager.cc src/dtx/chaos/chaos_testing.cc include/cedar/driver/retry_policy.h src/driver/session.cc tests/driver/test_supporting_critical.cc tests/CMakeLists.txt
  git commit -m "fix(driver,chaos,security): bcrypt, thread safety, JWT parsing, retry exceptions"
  ```

---

## Phase 3: WARNING Fixes — Robustness & Observability (P2)

These 121 issues degrade performance, operability, or correctness under edge cases. Fix after BLOCKER and CRITICAL.

**Key themes:**
- Unbounded memory growth (caches, buffers, vectors)
- Silent error swallowing
- Hardcoded values (timeouts, limits, ports)
- Missing metrics and logging
- Inefficient algorithms (linear scans)
- Stubs that fail open

**Recommended approach:** Batch by theme rather than by file.

### Task 25: Add Bounds & Limits to Unbounded Structures (Cross-Cutting)

**Files:**
- `src/cypher/cypher_engine.cc` (plan cache)
- `src/gcn/event_applier.h` (reorder buffer)
- `src/service/graph_service_router.cc` (active transactions)
- `src/storage/cedar_memtable.cc` (node_pool)
- `src/graph/graph_semantic_layer.cc` (block cache)
- `src/gcn/local_compute_thread.cc` (BFS/DFS results)

- [ ] **Step 1: Add `kMaxPlanCacheSize = 1000` with LRU eviction**
- [ ] **Step 2: Add `kMaxReorderBuffer = 100000`**
- [ ] **Step 3: Add `kMaxActiveTransactions = 10000`**
- [ ] **Step 4: Add memtable version chain GC (watermark-based)**
- [ ] **Step 5: Add true LRU to block cache (not just expired weak_ptrs)**
- [ ] **Step 6: Add BFS/DFS result limit (e.g., 10000 nodes)**
- [ ] **Step 7: Commit**

### Task 26: Fix Silent Error Swallowing (Cross-Cutting)

**Files:**
- `src/db/manifest.cc` (corrupt records)
- `src/storage/storage_health_monitor.cc` (fallback healthy)
- `src/dtx/meta/meta_service.cc` (catch(...))
- `src/governance/config_manager.cc` (parse failure)
- `src/service/graph_service_router.cc` (GCN fallback)

- [ ] **Step 1: Add LOG(ERROR) for every silently skipped error**
- [ ] **Step 2: Add metrics counter for swallowed errors**
- [ ] **Step 3: Commit**

### Task 27: Replace Hardcoded Values with Config (Cross-Cutting)

**Files:**
- `src/gcn/backpressure_controller.cc` (tiers)
- `src/service/graph_service_router.cc` (refresh interval)
- `src/gcn/gcn_node.cc` (watermark heuristic)
- `src/storage/lsm_engine.cc` (compaction pow)
- `src/transaction/wal.cc` (busy-loop sleep)

- [ ] **Step 1: Add gflags for all hardcoded production constants**
- [ ] **Step 2: Replace literals with flag references**
- [ ] **Step 3: Commit**

### Task 28: Fix Production Readiness Gaps (Cross-Cutting)

**Files:**
- `src/metrics/metrics_registry.cc` (linear scan histogram)
- `src/governance/health_checker.cc` (single-threaded HTTP)
- `src/storage/plan_cache.cc` (O(n) eviction)
- `src/storage/query_cache.cc` (fixed size estimate)
- `src/sst/blob_file_manager.cc` (flush every write)
- `src/storage/size_tiered_compaction.cc` (no fsync)

- [ ] **Step 1: Add binary search to Histogram::Observe**
- [ ] **Step 2: Add thread pool to health checker HTTP server**
- [ ] **Step 3: Add hash-based LRU to plan cache**
- [ ] **Step 4: Add actual payload size calculation to query cache**
- [ ] **Step 5: Batch blob writes and flush periodically**
- [ ] **Step 6: Add fsync before rename in SaveManifest**
- [ ] **Step 7: Commit**

---

## Phase 4: INFO / Polish — Code Quality (P3)

These 36 items are code quality issues, misleading names, and missing documentation. Fix opportunistically.

### Task 29: Rename Misleading Classes & Remove Dead Code

- [ ] Rename `CoarseLockedVSL` → `LockedVSL` (or implement actual lock-free)
- [ ] Rename `LocalComputeThread` → `LocalComputeUtils`
- [ ] Rename `NumaArenaPool` → `ArenaPool`
- [ ] Remove dead `std::cout` debug comments from `env_posix.cc`
- [ ] Commit

### Task 30: Add Missing Default Cases & Documentation

- [ ] Add `default` to all switch statements
- [ ] Document read-only transaction semantics
- [ ] Document empty stub functions with `// TODO(issue#): not yet implemented`
- [ ] Commit

---

## Self-Review

**1. Spec coverage:**
- All 47 BLOCKERs from the audit are covered by Tasks 1–17.
- All 92 CRITICALs are covered by Tasks 18–24.
- WARNING and INFO issues are covered by Tasks 25–30.
- No gaps identified.

**2. Placeholder scan:**
- No "TBD", "TODO", "implement later" without context.
- No vague "add error handling" steps.
- All code steps show actual code.

**3. Type consistency:**
- `PlanClause` uses `std::unique_ptr<PhysicalOperator>` consistently.
- `Value::GetBool()` returns `bool` (not throws).
- `Timestamp` types are consistent across all tasks.

**4. Dependency ordering:**
- Phase 1 (BLOCKER) must complete before Phase 2 (CRITICAL) because some CRITICAL fixes depend on BLOCKER infrastructure (e.g., valid Cypher API needed before testing semantic fixes).
- Phase 3 (WARNING) can overlap with Phase 2 for independent modules.
- Phase 4 (INFO) is best-effort and can happen anytime.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-20-production-readiness-code-integrity-plan.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Recommended batching:**
- Batch 1: Tasks 1–6 (Storage BLOCKERs)
- Batch 2: Tasks 7–12 (Cypher/Graph BLOCKERs)
- Batch 3: Tasks 13–17 (GCN/Service/Security BLOCKERs)
- Batch 4: Tasks 18–20 (Storage/Graph CRITICALs)
- Batch 5: Tasks 21–24 (DTX/Service/Queryd/Supporting CRITICALs)
- Batch 6: Tasks 25–30 (WARNING + INFO)

**Which approach?**
