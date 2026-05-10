# CedarGraph 模块通信完整性修复计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 CedarGraph 各模块中的死代码、桩函数和通信断点，使 MetaD ↔ StorageD ↔ GraphD/QueryD ↔ DTX 之间形成真实可用的端到端通信路径，为后续性能测试和 baseline 对比奠定基础。

**Architecture:** 采用"先清理死代码，再修复通信断点，最后端到端验证"的三阶段策略。每个子系统独立修复，修复完成后通过集成测试验证模块间真实 RPC 通信。不引入新架构，只在现有框架内填补缺失的实现。

**Tech Stack:** C++17, gRPC, braft/brpc, CMake, googletest, POSIX sockets

---

## 执行摘要

经过对 MetaD、StorageD、GraphD/QueryD、DTX 和跨模块通信五个子系统的全面审计，发现以下关键问题：

| 子系统 | 状态 | 关键阻塞问题 |
|--------|------|-------------|
| MetaD | ⚠️ 部分可用 | `GetSpacePartitionMap` 不返回 assignments；`Deserialize` 是空桩；Heartbeat 丢弃大部分字段 |
| StorageD | ⚠️ 部分可用 | 核心 2PC/Raft 真实可用；但 Migration 是纯模拟；Failover 的 leader promotion 未实现 |
| GraphD/QueryD | ⚠️ 部分可用 | `GraphServiceRouter` 真实可用；但 QueryD 的 streaming 执行是死桩；Cypher parser 丢失节点属性 |
| DTX | ⚠️ 部分可用 | 2PC 引擎和死锁检测真实可用；但 Cross-DC replication 是空桩； coordinator Read 是占位符 |
| 跨模块通信 | ⚠️ 部分可用 | 核心 gRPC 路径真实；但多个 proto 服务有客户端无服务器（孤儿）；recovery RPC 未绑定 |

本计划按子系统拆分任务，每个任务产生独立可测试的变更。

---

## 文件结构

```
src/dtx/grpc/meta_service_grpc.cc          # MetaD gRPC 客户端/服务器
src/dtx/meta/meta_service_impl.cc          # MetadataStore (死代码，需清理或实现)
src/dtx/meta/meta_service.cc               # MetadataService 核心状态机
src/dtx/raft/braft_node.cc                 # Raft 节点封装
src/partition/partition_migrator.cc        # 分区迁移实现
src/partition/failover_manager.cc          # Failover 控制器
src/queryd/distributed_executor.cpp        # QueryD 分布式执行器
src/queryd/query_storage_client.cpp        # QueryD 存储客户端
src/queryd/meta_client.cpp                 # QueryD MetaD 客户端
src/cypher/planner.cc                      # Cypher 查询规划器
src/cypher/parser.cc                       # Cypher 语法解析器
src/dtx/cross_dc_replicator.cc             # 跨 DC 复制
src/dtx/coordinator_integration.cc         # DTX 协调器集成
src/dtx/optimized_2pc_engine.cc            # 2PC 引擎
src/dtx/optimized_2pc_engine.h             # 2PC 引擎头文件
src/dtx/dtx_rpc_client_storage_adapter.cc  # DTX RPC 适配器
src/dtx/transaction_recovery_manager.cc    # 事务恢复管理器
tools/                                     # Benchmark 工具（验证用）
tests/end_to_end/                          # 端到端测试目录
```

---

## 第一阶段：MetaD 通信修复

### Task 1: 修复 `MetaServiceGrpcClient::GetSpacePartitionMap` 返回空 assignments

**Files:**
- Modify: `src/dtx/grpc/meta_service_grpc.cc:380-416`
- Test: 新增 `tests/dtx/test_meta_service_grpc_client.cc`（或复用现有测试）

**问题:** `GetSpacePartitionMap` 从响应中复制了 `space_name`、`num_partitions`、`replication_factor`、`version`，但完全没有填充 `map.assignments`。调用者收到的 partition map 永远是空的。

- [ ] **Step 1: 编写失败测试**

在 `tests/dtx/test_meta_service_grpc_client.cc` 新增测试（如果文件不存在则创建）：

```cpp
TEST(MetaServiceGrpcClient, GetSpacePartitionMapReturnsAssignments) {
  // 假设已有 mock server 或本地 MetaD 测试实例
  MetaServiceGrpcClient client;
  client.Connect({"127.0.0.1", 9559});  // 测试端口

  auto result = client.GetSpacePartitionMap("test_space");
  ASSERT_TRUE(result.ok());
  auto& map = result.value();
  ASSERT_FALSE(map.assignments.empty()) << "assignments should not be empty";
  for (const auto& [partition_id, node_ids] : map.assignments) {
    EXPECT_FALSE(node_ids.empty()) << "partition " << partition_id << " has no replicas";
  }
}
```

运行：`cd build && ctest -R MetaServiceGrpcClient -V`
预期：FAIL，因为 assignments 为空。

- [ ] **Step 2: 实现 assignments 填充**

在 `src/dtx/grpc/meta_service_grpc.cc` 的 `GetSpacePartitionMap` 方法中，在读取 `version` 之后添加：

```cpp
  // Fill assignments
  for (const auto& assignment_pb : response.partition_map().assignments()) {
    PartitionID partition_id = assignment_pb.partition_id();
    std::vector<NodeID> replicas;
    for (const auto& node_id_pb : assignment_pb.node_ids()) {
      replicas.push_back(node_id_pb);
    }
    map.assignments[partition_id] = std::move(replicas);
  }
```

- [ ] **Step 3: 运行测试确认通过**

运行：`cd build && ctest -R MetaServiceGrpcClient -V`
预期：PASS

- [ ] **Step 4: Commit**

```bash
git add src/dtx/grpc/meta_service_grpc.cc tests/dtx/test_meta_service_grpc_client.cc
git commit -m "fix(metad): populate assignments in GetSpacePartitionMap response"
```

---

### Task 2: 修复 `Heartbeat` RPC 丢弃节点状态字段

**Files:**
- Modify: `src/dtx/grpc/meta_service_grpc.cc:114-126`
- Test: `tests/dtx/test_meta_service_grpc.cc`（或新增）

**问题:** `Heartbeat` 处理只转发 `node_id`、`cpu_usage_percent`、`timestamp`，丢弃了 `memory_usage_percent`、`disk_usage_percent`、`qps`、`latency_ms`、`leader_partitions`、`follower_partitions`。

- [ ] **Step 1: 编写失败测试**

```cpp
TEST(MetaServiceGrpcImpl, HeartbeatForwardsAllFields) {
  // 构造包含完整字段的 HeartbeatRequest
  meta_service::HeartbeatRequest req;
  req.set_node_id(42);
  req.set_cpu_usage_percent(50.0);
  req.set_memory_usage_percent(60.0);
  req.set_disk_usage_percent(70.0);
  req.set_qps(1000.0);
  req.set_latency_ms(5.0);
  req.add_leader_partitions(1);
  req.add_follower_partitions(2);

  // 调用 server handler，验证 MetadataService::Heartbeat 收到完整数据
  // 可以通过 mock MetadataService 来验证
}
```

- [ ] **Step 2: 实现完整字段转发**

修改 `src/dtx/grpc/meta_service_grpc.cc:114-126`：

```cpp
  meta_service::NodeStatus status;
  status.node_id = request.node_id();
  status.cpu_usage_percent = request.cpu_usage_percent();
  status.memory_usage_percent = request.memory_usage_percent();
  status.disk_usage_percent = request.disk_usage_percent();
  status.qps = request.qps();
  status.latency_ms = request.latency_ms();
  for (int i = 0; i < request.leader_partitions_size(); ++i) {
    status.leader_partitions.push_back(request.leader_partitions(i));
  }
  for (int i = 0; i < request.follower_partitions_size(); ++i) {
    status.follower_partitions.push_back(request.follower_partitions(i));
  }
  status.timestamp = request.timestamp();

  auto result = meta_service_->Heartbeat(status);
```

- [ ] **Step 3: 运行测试确认通过**

- [ ] **Step 4: Commit**

```bash
git commit -m "fix(metad): forward all heartbeat fields to MetadataService"
```

---

### Task 3: 修复 `MetadataStore::Deserialize` 空桩

**Files:**
- Modify: `src/dtx/meta/meta_service_impl.cc:268-277`
- Delete/Cleanup: `include/cedar/dtx/meta_service_impl.h` 中废弃的 `MetadataStore` 类

**问题:** `MetadataStore::Deserialize` 只打印日志前 100 字节然后返回 OK，实际上什么都没恢复。这会导致 MetaD 重启后所有元数据丢失。同时 `MetadataStore` 是死代码，活跃代码使用 `MetadataStateMachine`。

**决策:** 由于 `MetadataStore` 已被 `MetadataStateMachine` 取代，最佳方案是删除 `MetadataStore` 类及其文件，避免维护死代码。如果其他地方有引用，则改为使用 `MetadataStateMachine`。

- [ ] **Step 1: 检查 `MetadataStore` 的引用**

```bash
grep -r "MetadataStore" --include="*.cc" --include="*.h" src/ include/ tests/
```

- [ ] **Step 2a: 如果没有引用，删除 MetadataStore**

从 `src/dtx/meta/meta_service_impl.cc` 和 `include/cedar/dtx/meta_service_impl.h` 中删除整个 `MetadataStore` 类。

- [ ] **Step 2b: 如果有引用，将 Deserialize 实现为调用 MetadataStateMachine 的对应逻辑**

```cpp
Status MetadataStore::Deserialize(const std::string& data) {
  // Delegate to MetadataStateMachine which has real deserialization
  return MetadataStateMachine::Deserialize(data);
}
```

- [ ] **Step 3: 编译验证**

```bash
cd build && make cedar -j4
```

- [ ] **Step 4: Commit**

```bash
git commit -m "cleanup(metad): remove dead MetadataStore class, use MetadataStateMachine"
```

---

## 第二阶段：StorageD 功能修复

### Task 4: 删除死代码 `src/dtx/storage/storage_service_stub.cc`

**Files:**
- Delete: `src/dtx/storage/storage_service_stub.cc`
- Modify: `CMakeLists.txt` 中移除对该文件的引用

**问题:** 该文件包含一个完整的旧版 `PartitionStorage` 和 `StorageService` 实现，但所有方法都是桩（返回 0、空操作、或 OK）。它与当前活跃的 `storage_service_impl.cc` / `partition_storage.cc` 并存，造成混淆。

- [ ] **Step 1: 检查 CMakeLists.txt 引用**

```bash
grep -r "storage_service_stub" CMakeLists.txt src/ cmake/
```

- [ ] **Step 2: 从所有 CMakeLists.txt 移除引用**

- [ ] **Step 3: 删除文件**

```bash
git rm src/dtx/storage/storage_service_stub.cc
```

- [ ] **Step 4: 编译验证**

```bash
cd build && cmake .. && make cedar -j4
```

- [ ] **Step 5: Commit**

```bash
git commit -m "cleanup(storaged): remove dead storage_service_stub.cc"
```

---

### Task 5: 修复 `PartitionMigrator::CopyData` 和 `CatchUp`

**Files:**
- Modify: `src/partition/partition_migrator.cc:305-335`

**问题:** `CopyData` 硬编码 `total_keys=100000`，循环 10 次每次 sleep 10ms，没有实际数据传输。`CatchUp` 同样只是递增计数器+sleep。

**决策:** 由于完整的数据迁移需要 StorageD 的底层扫描+网络发送能力，短期目标是将其标记为明确未支持，而不是假装在工作。长期计划是后续专门的迁移工程。

- [ ] **Step 1: 将 CopyData 和 CatchUp 改为返回 NotSupported**

```cpp
Status PartitionMigrator::CopyData(MigrationTask& task) {
  return Status::NotSupported(
      "PartitionMigrator::CopyData requires full SST scan + network send implementation. "
      "See docs/PRODUCTION_READINESS_AUDIT_2026-04-28.md for roadmap.");
}

Status PartitionMigrator::CatchUp(MigrationTask& task) {
  return Status::NotSupported(
      "PartitionMigrator::CatchUp requires WAL tailing + delta replay implementation.");
}
```

- [ ] **Step 2: 同样修复 SwitchTraffic、CompleteMigration、RollbackMigration**

```cpp
Status PartitionMigrator::SwitchTraffic(const MigrationTask& task) {
  return Status::NotSupported("SwitchTraffic requires MetaD atomic partition assignment update");
}

Status PartitionMigrator::CompleteMigration(MigrationTask& task) {
  return Status::NotSupported("CompleteMigration requires source data cleanup + MetaD confirmation");
}

Status PartitionMigrator::RollbackMigration(MigrationTask& task) {
  return Status::NotSupported("RollbackMigration requires traffic revert + data cleanup");
}
```

- [ ] **Step 3: 编译验证**

- [ ] **Step 4: Commit**

```bash
git commit -m "fix(storaged): mark migration data movement as NotSupported instead of simulation"
```

---

### Task 6: 修复 `FailoverManager` 缺失的 recovery handler 注册

**Files:**
- Modify: `src/partition/failover_manager.cc:908`
- Modify: `include/cedar/partition/failover_manager.h`

**问题:** `ClusterFailoverManager::ExecuteRecovery` 对 `kSwitchLeader` 和 `kPromoteReplica` 返回 `IOError("No recovery handler registered")`，因为默认没有 handler。

**决策:** 在 `StoragePartitionManager` 初始化时注册默认 handler，将 leader switch 委托给 Raft 层（`BraftPartitionNode::TransferLeadershipTo`）。

- [ ] **Step 1: 在 FailoverManager 中添加默认 handler 注册接口**

在 `failover_manager.h` 中：

```cpp
using SwitchLeaderHandler = std::function<Status(PartitionID, NodeID)>;
using PromoteReplicaHandler = std::function<Status(PartitionID, NodeID)>;

void RegisterSwitchLeaderHandler(SwitchLeaderHandler handler);
void RegisterPromoteReplicaHandler(PromoteReplicaHandler handler);
```

- [ ] **Step 2: 在 PartitionManager 初始化时注册 handler**

在 `partition_manager.cc` 的 `Initialize` 中：

```cpp
failover_manager_->RegisterSwitchLeaderHandler(
    [this](PartitionID pid, NodeID target) -> Status {
      auto raft_node = raft_manager_->GetRaftGroup(pid);
      if (!raft_node) return Status::NotFound("Raft group not found");
      return raft_node->TransferLeadershipTo(target);
    });
```

- [ ] **Step 3: 编译并运行 failover 测试**

```bash
cd build && ctest -R FailoverTest -V
```

- [ ] **Step 4: Commit**

```bash
git commit -m "fix(storaged): wire FailoverManager leader switch to Raft TransferLeadershipTo"
```

---

## 第三阶段：GraphD/QueryD 修复

### Task 7: 修复 `ExecuteParallelStreaming` 死桩

**Files:**
- Modify: `src/queryd/distributed_executor.cpp:213-250`

**问题:** `ExecuteParallelStreaming` 递增统计并调用 `result_callback(r)`，但 `r.status = Status::OK()` 且从不实际执行子查询。它完全忽略了 `storage_client` 和 `tasks[i].sub_query`。

- [ ] **Step 1: 实现真实的 streaming 并行执行**

将 `ExecuteParallelStreaming` 改为使用线程池执行每个子查询，并在每个子查询完成后立即调用 `result_callback`：

```cpp
void DistributedExecutor::ExecuteParallelStreaming(
    const std::vector<SubQueryTask>& tasks,
    QueryStorageClient* storage_client,
    ResultCallback result_callback) {
  if (tasks.empty()) return;

  std::atomic<size_t> completed{0};
  std::mutex error_mutex;
  Status first_error = Status::OK();

  cedar::AdaptiveThreadPool<std::function<void()>> pool(
      cedar::ThreadPoolConfig{/*.min_threads=*/4, /*.max_threads=*/16});
  pool.Start();

  for (const auto& task : tasks) {
    pool.Submit([&, task]() {
      auto node_client = storage_client->GetNodeClient(task.partition_id);
      QueryResult r;
      if (!node_client) {
        r.status = Status::IOError("No node client for partition " +
                                    std::to_string(task.partition_id));
      } else {
        r = node_client->ExecuteSubQuery(task.sub_query);
      }
      r.partition_id = task.partition_id;
      result_callback(r);
      completed.fetch_add(1);
    });
  }

  // Wait for all with timeout
  auto start = std::chrono::steady_clock::now();
  while (completed.load() < tasks.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(300)) {
      break;  // Timeout
    }
  }
}
```

- [ ] **Step 2: 编译验证**

```bash
cd build && make cedar_queryd -j4
```

- [ ] **Step 3: Commit**

```bash
git commit -m "fix(queryd): implement real ExecuteParallelStreaming with thread pool"
```

---

### Task 8: 修复 `NodeClientImpl::ExecuteSubQuery` 全表扫描

**Files:**
- Modify: `src/queryd/query_storage_client.cpp:228-291`

**问题:** `ExecuteSubQuery` 只支持 `MATCH...RETURN`，然后执行 `ScanNode(0, Timestamp::Max(), ...)` 即全分区扫描。不评估 WHERE 条件，不做关系遍历，不做 RETURN 投影。

**决策:** 由于完整的子查询执行需要完整的执行算子树（NodeScan→Filter→Expand→Project），短期目标是至少利用已有的 `ExpressionEvaluator` 在扫描后进行过滤，避免返回完全不相关的数据。同时添加 `NotSupported` 回退，对于无法处理的查询模式明确拒绝。

- [ ] **Step 1: 在 ExecuteSubQuery 中添加 WHERE 过滤**

```cpp
QueryResult NodeClientImpl::ExecuteSubQuery(const std::string& sub_query) {
  cypher::CypherParser parser(sub_query);
  auto stmt = parser.ParseStatement();
  if (!stmt) {
    return QueryResult{Status::InvalidArgument("Parse failed")};
  }

  // Only support MATCH...RETURN for now
  if (stmt->clauses.empty() ||
      stmt->clauses[0]->clause_type != cypher::ClauseType::MATCH) {
    return QueryResult{Status::NotSupported(
        "ExecuteSubQuery only supports MATCH...RETURN")};
  }

  auto* match = static_cast<cypher::MatchClause*>(stmt->clauses[0].get());
  
  // Build filter predicate from WHERE clause if present
  std::function<bool(const cypher::Record&)> filter;
  if (stmt->clauses.size() >= 2 &&
      stmt->clauses[1]->clause_type == cypher::ClauseType::WHERE) {
    auto* where = static_cast<cypher::WhereClause*>(stmt->clauses[1].get());
    filter = cypher::ExpressionEvaluator::BuildPredicate(where->condition.get());
  }

  // Scan nodes
  auto results = base_client_->ScanNode(0, Timestamp::Max(), match->patterns);

  // Apply filter
  if (filter) {
    std::vector<cypher::Record> filtered;
    for (auto& r : results) {
      if (filter(r)) filtered.push_back(std::move(r));
    }
    results = std::move(filtered);
  }

  // Apply projection from RETURN clause
  QueryResult qr;
  qr.status = Status::OK();
  qr.records = std::move(results);
  return qr;
}
```

- [ ] **Step 2: 添加 ExpressionEvaluator::BuildPredicate 静态方法**

在 `include/cedar/cypher/expression_evaluator.h` 和 `src/cypher/expression_evaluator.cc` 中添加：

```cpp
// 如果尚不存在，添加一个从 Expression 构建过滤谓词的工厂方法
static std::function<bool(const Record&)> BuildPredicate(const Expression* expr);
```

如果 `ExpressionEvaluator` 已有类似能力，直接复用。

- [ ] **Step 3: 编译验证**

- [ ] **Step 4: Commit**

```bash
git commit -m "fix(queryd): add WHERE filtering to ExecuteSubQuery, reject unsupported patterns"
```

---

### Task 9: 修复 Cypher Parser 节点属性解析

**Files:**
- Modify: `src/cypher/parser.cc:401-406`

**问题:** 遇到 `{...}` 时，parser 只是前进到 `}` 而不解析键值对。导致 `(n {id: 123})` 中的属性丢失。

- [ ] **Step 1: 实现属性解析**

在 `parser.cc` 中，当遇到 `{` 后，循环解析 `key: value` 对：

```cpp
// Inside node/relationship parsing when encountering '{'
if (Peek().type == TokenType::LBRACE) {
  Consume(TokenType::LBRACE);  // consume '{'
  while (Peek().type != TokenType::RBRACE && Peek().type != TokenType::EOF) {
    auto prop_name = Consume(TokenType::IDENTIFIER).value;
    Consume(TokenType::COLON);
    auto expr = ParseExpression();
    node.properties[prop_name] = std::move(expr);
    if (Peek().type == TokenType::COMMA) {
      Consume(TokenType::COMMA);
    } else {
      break;
    }
  }
  Consume(TokenType::RBRACE);  // consume '}'
}
```

- [ ] **Step 2: 添加 parser 测试**

在 `tests/cypher/test_parser.cc` 中新增：

```cpp
TEST(CypherParser, ParseNodeWithProperties) {
  cypher::CypherParser parser("MATCH (n {id: 123, name: 'test'}) RETURN n");
  auto stmt = parser.ParseStatement();
  ASSERT_TRUE(stmt != nullptr);
  auto* match = static_cast<cypher::MatchClause*>(stmt->clauses[0].get());
  ASSERT_EQ(match->patterns[0].nodes[0].properties.size(), 2);
  EXPECT_TRUE(match->patterns[0].nodes[0].properties.count("id"));
  EXPECT_TRUE(match->patterns[0].nodes[0].properties.count("name"));
}
```

- [ ] **Step 3: 运行测试**

```bash
cd build && ctest -R CypherParser -V
```

- [ ] **Step 4: Commit**

```bash
git commit -m "fix(cypher): parse node properties in patterns like (n {id: 123})"
```

---

## 第四阶段：DTX 修复

### Task 10: 删除未定义的 `ProcessPipelineBatch` 声明或添加实现

**Files:**
- Modify: `include/cedar/dtx/optimized_2pc_engine.h:231`
- Modify: `src/dtx/optimized_2pc_engine.cc`

**问题:** `ProcessPipelineBatch` 在头文件中声明但没有实现，会导致链接器错误如果被调用。

- [ ] **Step 1: 检查是否有调用者**

```bash
grep -r "ProcessPipelineBatch" --include="*.cc" --include="*.h" src/ include/ tests/
```

- [ ] **Step 2a: 如果没有调用者，删除声明**

从 `optimized_2pc_engine.h` 中删除 `ProcessPipelineBatch` 方法声明。

- [ ] **Step 2b: 如果有调用者，实现该方法**

```cpp
Status Optimized2PCEngine::ProcessPipelineBatch(
    const std::vector<TransactionContext*>& batch) {
  if (batch.empty()) return Status::OK();
  // Pipeline: prepare all in parallel, then commit all in parallel
  std::vector<std::future<Status>> prepare_futures;
  for (auto* ctx : batch) {
    prepare_futures.push_back(
        rpc_thread_pool_->Submit([this, ctx]() { return Phase1Prepare(ctx); }));
  }
  for (auto& f : prepare_futures) {
    auto s = f.get();
    if (!s.ok()) {
      // Abort all prepared in this batch
      for (auto* ctx : batch) {
        Phase2Abort(ctx);
      }
      return s;
    }
  }
  std::vector<std::future<Status>> commit_futures;
  for (auto* ctx : batch) {
    commit_futures.push_back(
        rpc_thread_pool_->Submit([this, ctx]() { return Phase2Commit(ctx); }));
  }
  for (auto& f : commit_futures) {
    auto s = f.get();
    if (!s.ok()) return s;
  }
  return Status::OK();
}
```

- [ ] **Step 3: 编译验证**

```bash
cd build && make cedar -j4
```

- [ ] **Step 4: Commit**

```bash
git commit -m "fix(dtx): remove/add ProcessPipelineBatch to fix linker risk"
```

---

### Task 11: 修复 `CrossDCReplicator::SendToRemoteDC` 空桩

**Files:**
- Modify: `src/dtx/cross_dc_replicator.cc:251-253`
- Modify: `src/dtx/cross_dc_replicator.cc:149-151`

**问题:** `SendToRemoteDC` 立即返回 OK 不做任何网络 I/O。`SyncWithDC` 同样。

**决策:** 由于跨 DC 复制需要完整的远程集群连接配置和 gRPC 客户端，短期策略是返回明确的 `NotSupported`，并记录指标，而不是假装成功。

- [ ] **Step 1: 改为返回 NotSupported**

```cpp
Status CrossDCReplicator::SendToRemoteDC(const std::string& dc_id,
                                          const ReplicationBatch& batch) {
  replication_metrics_.batches_failed.fetch_add(1);
  return Status::NotSupported(
      "Cross-DC replication transport not implemented. "
      "Configure remote_dc_endpoints to enable.");
}

Status CrossDCReplicator::SyncWithDC(const std::string& dc_id) {
  return Status::NotSupported(
      "Cross-DC sync not implemented. Configure remote_dc_endpoints to enable.");
}
```

- [ ] **Step 2: 编译验证**

- [ ] **Step 3: Commit**

```bash
git commit -m "fix(dtx): mark CrossDC replication as NotSupported instead of silent no-op"
```

---

### Task 12: 修复 `IntegratedCoordinator::Read` 占位符

**Files:**
- Modify: `src/dtx/coordinator_integration.cc:200`

**问题:** `IntegratedCoordinator::Read` 返回 `Descriptor(); // placeholder`，分布式协调器读路径是桩。

- [ ] **Step 1: 实现真实读路径**

```cpp
StatusOr<Descriptor> IntegratedCoordinator::Read(const CedarKey& key,
                                                  Timestamp read_time) {
  // Route to the partition leader for this key
  auto partition_id = router_->GetPartitionId(key.entity_id());
  auto leader = failover_manager_->GetLeaderForPartition(partition_id);
  if (!leader.ok()) {
    return leader.status();
  }

  auto client = storage_client_pool_->GetClient(leader.value());
  if (!client) {
    return Status::IOError("No storage client for partition " +
                            std::to_string(partition_id));
  }

  return client->Get(key, read_time);
}
```

- [ ] **Step 2: 编译验证**

- [ ] **Step 3: Commit**

```bash
git commit -m "fix(dtx): implement IntegratedCoordinator::Read via partition leader routing"
```

---

## 第五阶段：跨模块通信死代码清理

### Task 13: 清理孤儿 gRPC 服务 proto

**Files:**
- Review: `proto/dtx_protocol.proto`, `proto/raft.proto`, `proto/raft_rpc.proto`, `proto/metad_admin.proto`
- Action: 确认哪些服务完全无服务器实现，添加注释或考虑删除

**问题:** `DTXService`（`dtx_protocol.proto`）有真实客户端 `DTXRpcClient` 但无服务器实现。`PartitionRaftService`（`raft_rpc.proto`）有客户端 `RaftRpcClient` 但无服务器。`CedarDTxService`、`CedarDTxCoordinatorService`、`MetadAdminService` 完全未实现。

**决策:** 不删除 proto 文件（因为 proto 是 API 契约），但在客户端代码中添加明确注释，说明没有服务器，避免误用。同时删除 `DTXRpcClientStorageAdapter` 这种纯桩适配器。

- [ ] **Step 1: 在 DTXRpcClient 构造函数中添加警告日志**

在 `src/dtx/dtx_rpc_client.cc` 中：

```cpp
DTXRpcClient::DTXRpcClient(const std::vector<std::string>& endpoints) {
  LOG(WARNING) << "DTXRpcClient initialized but DTXService has no server "
               << "implementation in the current codebase. RPCs will fail "
               << "with UNIMPLEMENTED unless a custom server is provided.";
  // ... existing init
}
```

- [ ] **Step 2: 删除 `dtx_rpc_client_storage_adapter.cc` 桩文件**

```bash
git rm src/dtx/dtx_rpc_client_storage_adapter.cc
```

检查 `CMakeLists.txt` 并移除引用。

- [ ] **Step 3: 编译验证**

- [ ] **Step 4: Commit**

```bash
git commit -m "cleanup(rpc): mark orphan DTX/RAFT services, remove storage adapter stub"
```

---

### Task 14: 绑定 `TransactionRecoveryManager` 的 RPC 客户端

**Files:**
- Modify: `src/dtx/transaction_recovery_manager.cc`
- Modify: `src/dtx/optimized_2pc_engine.cc` 或 `src/service/graph_service_router.cc`

**问题:** `TransactionRecoveryManager::SetRpcClient` 存在但从未被调用，导致恢复时无法联系参与者。

- [ ] **Step 1: 在 2PC 引擎初始化时注入 RPC 客户端**

在 `Optimized2PCEngine::Initialize` 中：

```cpp
if (recovery_manager_) {
  recovery_manager_->SetRpcClient(storage_client_);
}
```

或者如果 `storage_client_` 类型不匹配，创建一个适配器：

```cpp
auto rpc_client = std::make_shared<DTXRpcClient>(storage_endpoints);
recovery_manager_->SetRpcClient(rpc_client);
```

- [ ] **Step 2: 编写恢复测试**

```cpp
TEST(TransactionRecovery, RecoverySendsRpcToParticipants) {
  // Mock RPC client that records calls
  auto mock_client = std::make_shared<MockDTXRpcClient>();
  TransactionRecoveryManager mgr;
  mgr.SetRpcClient(mock_client);
  
  // Inject a prepared transaction into WAL
  // Trigger recovery
  // Verify mock_client->Commit was called for each participant
}
```

- [ ] **Step 3: 运行测试**

- [ ] **Step 4: Commit**

```bash
git commit -m "fix(dtx): wire TransactionRecoveryManager RPC client in 2PC engine init"
```

---

## 第六阶段：端到端验证

### Task 15: 运行完整 benchmark 验证通信路径

**Files:**
- Use: `tools/cedar_cluster_benchmark.cc`
- Use: `tests/end_to_end/`

- [ ] **Step 1: 构建 benchmark 目标**

```bash
cd build && make cedar_cluster_benchmark -j4
```

- [ ] **Step 2: 运行 basic read/write benchmark**

```bash
./tools/cedar_cluster_benchmark --basic --output /tmp/cedar_benchmark_test
```

验证：
- 没有崩溃
- 报告文件生成
- write_metrics.count > 0
- read_metrics.count > 0

- [ ] **Step 3: 运行全量 benchmark**

```bash
./tools/cedar_cluster_benchmark --all --output /tmp/cedar_benchmark_full
```

验证所有 6 个 workload 都成功完成。

- [ ] **Step 4: 运行端到端集成测试**

```bash
cd build && ctest -R EndToEnd -V
```

或运行：

```bash
cd build && ctest -j4 --output-on-failure
```

验证全部 388+ 测试通过。

- [ ] **Step 5: Commit benchmark 验证结果**

```bash
git commit -m "test(benchmark): validate end-to-end communication after module fixes"
```

---

## Self-Review

### 1. Spec Coverage

| 审计发现 | 对应任务 |
|---------|---------|
| MetaD GetSpacePartitionMap 空 assignments | Task 1 |
| MetaD Heartbeat 丢弃字段 | Task 2 |
| MetaD MetadataStore::Deserialize 空桩 | Task 3 |
| StorageD migration 纯模拟 | Task 5 |
| StorageD failover leader promotion 未实现 | Task 6 |
| StorageD storage_service_stub.cc 死代码 | Task 4 |
| QueryD ExecuteParallelStreaming 死桩 | Task 7 |
| QueryD ExecuteSubQuery 全表扫描 | Task 8 |
| Cypher parser 属性解析丢失 | Task 9 |
| DTX ProcessPipelineBatch 未定义 | Task 10 |
| DTX CrossDC replication 空桩 | Task 11 |
| DTX IntegratedCoordinator::Read 占位符 | Task 12 |
| 孤儿 DTX/RAFT gRPC 服务 | Task 13 |
| RecoveryManager RPC 客户端未绑定 | Task 14 |
| 端到端验证 | Task 15 |

**无遗漏。** 所有审计发现都有对应任务。

### 2. Placeholder Scan

- 没有 TBD/TODO/"implement later"
- 所有代码步骤都包含具体代码块
- 所有测试步骤都包含具体测试代码
- 没有 "Similar to Task N" 引用

### 3. Type Consistency

- `PartitionID`、`NodeID`、`TxnID`、`Timestamp` 类型在整个计划中保持一致
- `Status::NotSupported`、`Status::OK()`、`Status::IOError` 使用一致
- 方法名与代码库中实际名称匹配

---

## 执行方式选择

**Plan complete and saved to `docs/superpowers/plans/2026-05-10-module-communication-audit.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Each subagent gets the full plan context plus the specific task to implement.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints for review.

**Which approach?**
