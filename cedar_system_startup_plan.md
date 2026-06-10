# CedarGraph-Core 代码梳理与系统启动计划

> 版本: v1.0
> 基于: 逐行代码审查报告 (`cedar_system_startup_report.md`)
> 原则: 先打通端到端，再完善可靠性；先不考虑登录/注册模块

---

## 目标定义

**"流畅跑起来"的标准**:
1. 启动 MetaD -> GraphD -> 1~N 个 StorageD，组成完整集群
2. 客户端通过 GraphD 发送 Cypher 查询，能正确返回结果
3. 写操作通过 2PC 提交，重启后数据不丢失
4. 多 StorageD 节点能协同处理跨分区查询
5. 单节点故障不影响集群整体可用性（Raft 选主）

**当前差距**: 查询下推路径断裂、本地存储访问未初始化、2PC 不持久化、Raft 状态机空壳。

---

## 阶段总览

```
Phase 1: 查询路径打通（预计 1-2 天）
  └─ 目标: GraphD 能把 Cypher 查询下推到 StorageD 执行并返回结果
  
Phase 2: 写入持久化（预计 1-2 天）
  └─ 目标: Put/Delete 操作通过 2PC 提交，数据真正写入 LSM 并刷盘
  
Phase 3: 分布式复制（预计 2-3 天）
  └─ 目标: StorageD 多节点通过 Raft 复制数据，单节点故障可恢复
  
Phase 4: 计算加速与调优（预计 2-3 天）
  └─ 目标: GCN 接入 CDC 缓存、性能基准测试、监控告警完善
```

---

## Phase 1: 查询路径打通

### 1.1 任务: GraphD 初始化 QueryStorageClient 的 base_client_

**问题**: `tools/graphd.cc:119-120` 创建 `QueryStorageClient` 但未调用 `SetBaseClient()`，导致所有底层存储访问返回 `NotSupported`。

**影响范围**:
- `StorageBackedExecutionContext::get_all_entities_fn` -> `ScanNode` ❌
- `StorageBackedExecutionContext::get_out_neighbors_fn` -> `ScanOutEdges` ❌
- `StorageBackedExecutionContext::get_in_neighbors_fn` -> `ScanInEdges` ❌
- `DistributedExecutor::Traverse` 完全不可用

**修改方案**:

在 `tools/graphd.cc` 中，创建 `QueryStorageClient` 后，需要注入一个 `cedar::dtx::StorageClient` 作为 base_client_。

```cpp
// 在 graphd.cc main() 中， DistributedExecutor 创建之前
auto base_storage_client = std::make_shared<cedar::dtx::StorageClient>();
cedar::dtx::StorageClient::ClientConfig client_config;
client_config.server_address = config.meta_server; // 或本地存储地址
client_config.tls = config.tls;
auto init_s = base_storage_client->Initialize(client_config);
if (init_s.ok()) {
    query_storage_client->SetBaseClient(base_storage_client);
}
```

**但这里有个设计问题**: GraphD 本身不直接拥有存储引擎（StorageD 才有）。`cedar::dtx::StorageClient` 是用于通过 gRPC 连接远程 StorageD 的客户端。所以 `SetBaseClient` 应该设置一个能够路由到正确 StorageD 节点的客户端。

查看 `cedar::dtx::StorageClient` 的实现... 它应该已经有路由能力。或者，更简单的方案是：让 `QueryStorageClient` 的独立模式也实现这些功能，通过 partition_routing_ 映射直接路由到远程 StorageD。

实际上，看看 `QueryStorageClient` 的设计：
- `SetBaseClient` 注入底层客户端
- 如果没有 base_client_，返回 NotSupported

GraphD 作为一个纯查询路由器，不应该直接操作本地 LSM。它应该通过 gRPC 连接到 StorageD。

所以正确的方案可能是：
1. 方案 A: 让 GraphD 也启动一个本地 LSM 引擎（如果 GraphD 和 StorageD 同机部署）
2. 方案 B: 修改 `QueryStorageClient` 的独立模式，让它通过已注册的 partition_routing_ 直接发 gRPC 到 StorageD
3. 方案 C: 创建一个 `StorageClient` 包装器，将操作转发到远程 StorageD

方案 B 最合理，因为 `QueryStorageClient` 已经有 `RegisterNode(partition_id, address)` 方法和 `GetOrCreateChannel`。

实际上，看看 `QueryStorageClient::ScanNode`:
```cpp
Status QueryStorageClient::ScanNode(...) {
  if (use_base_client_ && base_client_) {
    return base_client_->ScanNodeV2(...);
  }
  return Status::NotSupported("Independent mode not implemented, use SetBaseClient");
}
```

这里的问题是：如果不设置 base_client_，就没有回退路径。但 `QueryStorageClient` 已经知道每个 partition 对应的 StorageD 地址（通过 `RegisterNode`）。

所以修改方案应该是：**在 `QueryStorageClient` 中为独立模式实现 ScanNode/ScanOutEdges/ScanInEdges**，让它们通过 gRPC 直接调用 StorageD 的 `ScanNodeV2`/`ScanEdgeV2` RPC。

**修改文件**: `src/queryd/query_storage_client.cpp`

**修改内容**:
- `ScanNode`: 如果 `!use_base_client_`，通过 `GetNodeClient(partition_id)` 获取 `NodeClient`，调用 `ScanEntity`
  - 但 `NodeClient::ScanEntity` 在 `NodeClientImpl` 中调用 `client_->ScanNode` — 死循环
  - 需要新增 `NodeClient::ScanNodeV2` 方法

更干净的方案：
- 在 `QueryStorageClient` 中新增 `GetNodeForScan(uint64_t entity_id)` 获取对应 StorageD 的 gRPC stub
- 直接通过 stub 调用 `ScanNodeV2`/`ScanEdgeV2`

**实现步骤**:
1. 在 `src/queryd/query_storage_client.cpp` 中，为 `ScanNode`/`ScanOutEdges`/`ScanInEdges` 添加独立模式实现
2. 独立模式下，通过 `GetOrCreateChannel` 获取到对应 partition 的 gRPC channel
3. 直接调用 `cedar::storage::StorageService::Stub` 的 `ScanNodeV2`/`ScanEdgeV2`

**注意**: StorageD 端需要先实现 `ScanNodeV2`/`ScanEdgeV2`（见 1.3）。

### 1.2 任务: StorageD 实现 ExecuteSubQuery

**问题**: `tools/storaged.cc` 的 `StorageServiceImpl` 缺少 `ExecuteSubQuery` 方法实现。

**影响**: GraphD 的跨分区查询下推完全不可用。

**修改方案**:

在 `StorageServiceImpl` 中添加：

```cpp
grpc::Status ExecuteSubQuery(
    grpc::ServerContext* context,
    const cedar::storage::ExecuteSubQueryRequest* request,
    grpc::ServerWriter<cedar::storage::SubQueryResultBatch>* writer) override {
  (void)context;
  
  // 1. 解析 Cypher 查询片段
  cypher::CypherParser parser(request->query_fragment());
  auto stmt = parser.ParseStatement();
  if (!stmt) {
    cedar::storage::SubQueryResultBatch batch;
    batch.set_is_last(true);
    writer->Write(batch);
    return grpc::Status::OK;
  }
  
  // 2. 构建执行计划
  auto plan = cypher::ExecutionPlanBuilder::Build(stmt);
  if (!plan) {
    cedar::storage::SubQueryResultBatch batch;
    batch.set_is_last(true);
    writer->Write(batch);
    return grpc::Status::OK;
  }
  
  // 3. 创建存储执行上下文（使用本地 storage_）
  // 需要把 CedarGraphStorage 适配为 QueryStorageClient 能用的接口
  // 或者直接使用 storage_ 的 Get/Put/Scan 接口构建上下文
  
  // 4. 执行并流式返回结果
  // ...
}
```

**关键难点**: `StorageBackedExecutionContext` 依赖 `QueryStorageClient`，而 StorageD 中只有 `CedarGraphStorage*`。需要：

方案 A: 在 StorageD 中创建一个轻量级的 `QueryStorageClient` wrapper，将操作转发到本地 `storage_`
方案 B: 直接绕过 `QueryStorageClient`，用 `CedarGraphStorage` 构建执行上下文

方案 A 更一致。可以创建一个 `LocalStorageQueryClient` 继承/包装 `QueryStorageClient`，设置 base_client_ 为一个直接操作本地 storage 的适配器。

实际上，看看 `cedar::dtx::StorageClient` — 这是一个 gRPC 客户端，不适合本地直接调用。

更简单的方案：在 StorageD 中直接复用 `NodeClientImpl` 的模式：
1. 创建一个 `QueryStorageClient`
2. 设置一个自定义的 base_client_ 或者直接实现独立模式
3. 用 `StorageBackedExecutionContext` 执行查询

或者，更直接地，StorageD 可以直接包含 `cedar_cypher` 和 `cedar_queryd` 库（从 CMakeLists.txt 看，storaged 只链接了 `cedar cedar_graph`，没有 `cedar_cypher cedar_queryd`）。

查看 `CMakeLists.txt:517-529`:
```cmake
add_executable(storaged tools/storaged.cc ...)
target_link_libraries(storaged cedar cedar_graph gRPC::grpc++ pthread)
```

`storaged` 没有链接 `cedar_cypher` 和 `cedar_queryd`。如果要实现 ExecuteSubQuery，需要链接这些库。

**修改步骤**:
1. `CMakeLists.txt`: `target_link_libraries(storaged cedar cedar_graph cedar_cypher cedar_queryd gRPC::grpc++ pthread)`
2. `tools/storaged.cc`: 包含 cypher/queryd 头文件
3. `tools/storaged.cc`: 在 `StorageServiceImpl` 中实现 `ExecuteSubQuery`
4. `tools/storaged.cc`: 创建一个内部使用的 `QueryStorageClient`，将操作路由到本地 `storage_`

### 1.3 任务: StorageD 实现 ScanNodeV2 / ScanEdgeV2

**问题**: `StorageServiceImpl` 没有 `ScanNodeV2` 和 `ScanEdgeV2` 方法。

**影响**: GraphD 的 `ExecutePartitionQuery` 中 SCAN 和 NEIGHBOR_TRAVERSAL 查询类型失败。

**修改方案**:

在 `StorageServiceImpl` 中添加：

```cpp
grpc::Status ScanNodeV2(grpc::ServerContext* context,
                        const cedar::storage::ScanNodeRequestV2* request,
                        cedar::storage::ScanResponse* response) override {
  (void)context;
  auto result = storage_->GetRange(request->node_id(), EntityType::Vertex, 0,
                                   Timestamp(request->start_time()),
                                   Timestamp(request->end_time()));
  // 将 result 填充到 response
  response->set_success(true);
  return grpc::Status::OK;
}
```

等等，需要检查 `CedarGraphStorage` 是否有 `GetRange` 方法... 以及 proto 定义中的 `ScanNodeRequestV2` 和 `ScanResponse` 的字段。

查看 `proto/storage_service.proto` 以确认消息格式。

**修改步骤**:
1. 确认 proto 中 `ScanNodeRequestV2` / `ScanEdgeRequestV2` / `ScanResponse` 的字段
2. 在 `StorageServiceImpl` 中实现对应的 gRPC 方法
3. 调用 `storage_->GetRange()` 或 `storage_->Get()` 获取数据
4. 将结果转换为 proto 消息

### 1.4 Phase 1 验证标准

```bash
# 1. 启动 MetaD
./cedar-metad --test_mode &

# 2. 启动 StorageD
./cedar-storaged --node_id 0 --port 9779 --meta 127.0.0.1:9559 &

# 3. 启动 GraphD
./cedar-graphd --port 9669 --meta 127.0.0.1:9559 &

# 4. 测试单分区查询
./test_graphd_client "MATCH (n {id: 42}) RETURN n"
# 预期: 返回节点 42 的数据

# 5. 测试跨分区查询（如果注册了多节点）
./test_graphd_client "MATCH (n) RETURN count(n)"
# 预期: 返回节点总数

# 6. 测试图遍历
./test_graphd_client "MATCH (n {id: 1})-[:KNOWS]->(m) RETURN m"
# 预期: 返回节点 1 的 KNOWS 邻居
```

---

## Phase 2: 写入持久化

### 2.1 任务: StorageD 的 2PC 真正持久化

**当前问题**: `StorageServiceImpl::Prepare` 和 `Commit` 只操作内存 `txn_states_` map，不调用 `storage_->Put()`。

**修改方案**:

```cpp
// Prepare: 写入预提交 WAL 记录
cedar::storage::PrepareRequest 请求包含:
  - txn_id
  - commit_ts
  - read_set (用于冲突检测)
  - write_set (需要写入的 key)

// 修改后的 Prepare:
grpc::Status Prepare(...) override {
  // 1. 冲突检测（OCC: 检查 read_set 的版本是否变化）
  // 2. 锁定 write_set 中的 key
  // 3. 写入 Prepare WAL 记录
  // 4. 返回 Prepared
}

// Commit: 实际写入 LSM
grpc::Status Commit(...) override {
  // 1. 检查 txn_id 是否 Prepared
  // 2. 对 write_set 中的每个 key 调用 storage_->Put()
  // 3. 解锁 key
  // 4. 写入 Commit WAL 记录
  // 5. 从 txn_states_ 中移除或标记为 Committed
}
```

**关键设计决策**:
- 是否需要独立的 Prepare WAL？还是复用 LSM 引擎的 WAL？
- 建议复用 `LsmEngine` 的 WAL 机制，在 Prepare 时记录意图，Commit 时真正写入

**修改文件**: `tools/storaged.cc` 中的 `StorageServiceImpl`

### 2.2 Phase 2 验证标准

```bash
# 1. 写入数据
./test_graphd_client "CREATE (n {id: 100, name: 'test'})"

# 2. 重启 StorageD
kill %storaged
./cedar-storaged --node_id 0 --port 9779 --meta 127.0.0.1:9559 &

# 3. 查询刚才写入的数据
./test_graphd_client "MATCH (n {id: 100}) RETURN n"
# 预期: 返回 test 节点（数据未丢失）
```

---

## Phase 3: 分布式复制

### 3.1 任务: 实现 StorageRaftStateMachine::on_apply

**当前问题**: `src/dtx/storage/storaged_raft_state_machine.cc:10-16` 是空壳。

**修改方案**:

```cpp
void StorageRaftStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    brpc::ClosureGuard done_guard(iter.done());
    
    // 1. 反序列化 Raft 日志
    // 日志格式: [opcode:1][key_len:4][key...][value_len:4][value...]
    const butil::IOBuf& data = iter.data();
    
    // 2. 解析为 CedarKey + Descriptor
    // 3. 调用 storage_->Put() 应用到本地 LSM
    auto s = storage_->Put(key.entity_id(), key.timestamp(), desc, txn_version);
    if (!s.ok()) {
      LOG(ERROR) << "Raft apply failed: " << s.ToString();
    }
    
    LOG(INFO) << "Raft apply: index=" << iter.index() << " term=" << iter.term();
  }
}
```

**同时需要实现**:
- `on_snapshot_save`: 将 storage_ 的数据导出为快照
- `on_snapshot_load`: 从快照恢复 storage_

**修改文件**: `src/dtx/storage/storaged_raft_state_machine.cc`

### 3.2 任务: StorageD 集成 braft Node

**当前问题**: `tools/storaged.cc:409` 创建 `StorageRaftStateMachine` 但只持有指针，没有创建实际的 `braft::Node`。

**修改方案**:

```cpp
// 在 StorageD::main 中
braft::NodeOptions node_options;
node_options.election_timeout_ms = 5000;
node_options.fsm = raft_sm.get();
// ... 配置 peer 列表

std::string group_id = "partition_" + std::to_string(partition_id);
braft::Node* node = new braft::Node(group_id, peer_id);
if (node->init(node_options) != 0) {
  LOG(ERROR) << "Fail to init raft node";
}
```

**注意**: 这需要每个 partition 有一个独立的 Raft group。StorageD 可能承载多个 partition。

### 3.3 Phase 3 验证标准

```bash
# 1. 启动 3 节点 MetaD 集群
./cedar-metad --node_id 0 --listen 0.0.0.0:9559 --peer 1:127.0.0.1:9560 --peer 2:127.0.0.1:9561 &
./cedar-metad --node_id 1 --listen 0.0.0.0:9560 --peer 0:127.0.0.1:9559 --peer 2:127.0.0.1:9561 &
./cedar-metad --node_id 2 --listen 0.0.0.0:9561 --peer 0:127.0.0.1:9559 --peer 1:127.0.0.1:9560 &

# 2. 启动 3 节点 StorageD，每个承载不同 partition
./cedar-storaged --node_id 10 --port 9779 --meta 127.0.0.1:9559 &
./cedar-storaged --node_id 11 --port 9781 --meta 127.0.0.1:9559 &
./cedar-storaged --node_id 12 --port 9782 --meta 127.0.0.1:9559 &

# 3. 写入数据
./test_graphd_client "CREATE (n {id: 1})"

# 4. 杀掉 leader StorageD
kill <leader_pid>

# 5. 等待新 leader 选出后查询
./test_graphd_client "MATCH (n {id: 1}) RETURN n"
# 预期: 数据仍然存在（已从 follower 同步）
```

---

## Phase 4: 计算加速与调优

### 4.1 任务: GCN CDC 监听器接入 WAL 变更流

**修改**: `src/gcn/gcn_node.cc:203-209`

将 sleep 循环替换为真正的 CDC 消费者：
- 订阅 StorageD 的 WAL 变更事件
- 解析变更，调用 `event_applier_->ApplyUnordered(event)`

### 4.2 任务: 性能基准测试

运行现有测试：
- `test_distributed_performance`
- `test_multi_node_performance`
- `test_temporal_graph_perf`
- `test_real_storage_perf`

建立性能基线，识别瓶颈。

### 4.3 任务: 监控与告警完善

- 验证 Prometheus metrics 端点 (`:9667/metrics`, `:7001/metrics`)
- 验证 AlertManager 规则触发
- 添加 Grafana dashboard JSON

### 4.4 任务: 修复已知轻微问题

- `BlockHeader::kSize` 注释: 48 -> 44 bytes
- `FlushEntityGroup` 传播 `txn_version`
- `QueryStorageClient::CreateScan` 实现

---

## 实施顺序与依赖关系

```
Phase 1 (查询路径)
  ├─ 1.1 QueryStorageClient 独立模式实现 ScanNode/ScanOutEdges/ScanInEdges
  ├─ 1.2 StorageD 实现 ExecuteSubQuery
  ├─ 1.3 StorageD 实现 ScanNodeV2/ScanEdgeV2
  └─ 1.4 验证端到端查询

Phase 2 (写入持久化) — 依赖 Phase 1
  ├─ 2.1 StorageD 2PC Prepare/Commit 调用 storage_->Put/Delete
  └─ 2.2 验证重启不丢数据

Phase 3 (分布式复制) — 依赖 Phase 2
  ├─ 3.1 实现 StorageRaftStateMachine::on_apply
  ├─ 3.2 StorageD 集成 braft::Node
  └─ 3.3 验证多节点故障恢复

Phase 4 (加速与调优) — 依赖 Phase 3
  ├─ 4.1 GCN CDC 接入
  ├─ 4.2 性能基准
  ├─ 4.3 监控完善
  └─ 4.4 轻微问题修复
```

---

## 风险与缓解

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| Phase 1 改动引入循环依赖 | 中 | 编译失败 | 保持接口清晰，QueryStorageClient 不反向依赖 GraphD |
| StorageD 链接 cedar_cypher 导致体积膨胀 | 低 | 可执行文件变大 | 可接受，后续可拆分为动态库 |
| braft 集成复杂度超预期 | 中 | Phase 3 延期 | 可先使用 test_mode 运行单节点，再逐步加入 Raft |
| 性能基准不达标 | 低 | 需要额外优化 | 先建立基准，再针对性优化 |

---

## 附录: 修改文件清单

### Phase 1
- `src/queryd/query_storage_client.cpp` — 独立模式实现
- `src/queryd/query_storage_client.h` — 可能需要新增方法声明
- `tools/storaged.cc` — 添加 ExecuteSubQuery, ScanNodeV2, ScanEdgeV2
- `CMakeLists.txt` — storaged 链接 cedar_cypher, cedar_queryd
- `tools/graphd.cc` — 可能需要调整 QueryStorageClient 初始化

### Phase 2
- `tools/storaged.cc` — 修改 Prepare/Commit/Abort，调用 storage_->Put/Delete

### Phase 3
- `src/dtx/storage/storaged_raft_state_machine.cc` — 实现 on_apply, snapshot
- `tools/storaged.cc` — 创建 braft::Node，配置 Raft group
- `include/cedar/dtx/storage/storaged_raft_state_machine.h` — 可能需要调整接口

### Phase 4
- `src/gcn/gcn_node.cc` — 实现 CdcListenerLoop
- `include/cedar/sst/zone_columnar_reader.h` — 修正注释
- `src/storage/lsm_engine.cc` — FlushEntityGroup 传播 txn_version
- `src/queryd/query_storage_client.cpp` — CreateScan 实现

---

*计划结束。建议立即开始 Phase 1，每个任务完成后运行对应验证测试。*
