# CedarGraph 生产上线路线图

> **版本**: 2026-05-10  
> **基准审计**: docs/PRODUCTION_READINESS_AUDIT_2026-04-28.md  
> **目标**: 从当前状态到最小可上线（MVP）状态  
> **预估总工期**: 3-4 周（1-2 人）

---

## 目录

1. [执行摘要](#1-执行摘要)
2. [Phase 1: 阻塞修复（Week 1）](#2-phase-1-阻塞修复week-1)
3. [Phase 2: 核心功能补全（Week 1-2）](#3-phase-2-核心功能补全week-1-2)
4. [Phase 3: 端到端验证（Week 2-3）](#4-phase-3-端到端验证week-2-3)
5. [Phase 4: 生产加固（Week 3-4）](#5-phase-4-生产加固week-3-4)
6. [每日 Checklist](#6-每日-checklist)
7. [风险与应急预案](#7-风险与应急预案)

---

## 1. 执行摘要

### 1.1 当前状态

| 组件 | 完成度 | 关键风险 |
|------|--------|----------|
| MetaD (Raft + 元数据) | ~85% | 客户端缓存/流式 watch 缺失 |
| StorageD (Raft 复制 + 存储) | ~85% | 编译失败（Clang 17） |
| GraphD (查询网关) | ~75% | 查询路由已实现，但 QueryD 并行层仍为 stub |
| QueryD (分布式执行) | ~55% | `ParallelExecutor` 未实现真实 RPC |
| 故障检测与转移 | ~70% | 健康检查逻辑过于简单 |
| 测试覆盖 | ~80% | 4 个测试未构建，端到端读查询测试不足 |

### 1.2 目标定义（MVP 上线标准）

满足以下全部条件，方可进入生产环境：

- [ ] macOS (Apple Clang 17) 和 Linux (GCC 11+) 均可完整编译
- [ ] 486 个测试全部通过（含 4 个当前 NOT_BUILT 的测试）
- [ ] 3 节点 MetaD + 3 节点 StorageD + GraphD Docker Compose 一键部署成功
- [ ] 端到端：创建 Space → Put 写入 → Get 读回 → Cypher 查询返回正确结果
- [ ] 故障注入：kill StorageD leader 后，10 秒内完成故障转移，读/写不中断
- [ ] 快照恢复：MetaD/StorageD 重启后从快照恢复，数据零丢失
- [ ] 24 小时长稳测试：读写混合负载，无内存泄漏、无数据不一致

---

## 2. Phase 1: 阻塞修复（Week 1）

### 🔴 P0-1: 修复 Apple Clang 17 编译失败

**现状**: `src/dtx/optimized_2pc_engine.cc` 编译失败，错误位于 `third_party/brpc/src/bvar/detail/percentile.h:132`

```
DEFINE_SMALL_ARRAY(uint32_t, tmp, rhs._num_samples, 64);
// error: initializer of 'tmp_stack_array_size' is not a constant expression
```

**根因**: brpc 的 `DEFINE_SMALL_ARRAY` 宏使用了 VLA（变长数组）风格，Clang 17 在 C++ 模式下对此更严格。

**目标文件**:
- `third_party/brpc/src/butil/macros.h:425-426`
- `third_party/brpc/src/bvar/detail/percentile.h:132`

**实现方案**（二选一，推荐方案 A）:

**方案 A: 添加编译器兼容性补丁（推荐，侵入最小）**

在 `CMakeLists.txt` 中，针对 Apple Clang 17+ 添加 `-Wno-error` 或宏补丁：

```cmake
# 在 third_party/brpc 的编译选项中
if(APPLE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # Clang 17+ 对 VLA 更严格，brpc 的 DEFINE_SMALL_ARRAY 使用 const unsigned 而非 constexpr
    # 降级该错误为警告
    target_compile_options(brpc-static PRIVATE -Wno-error=vla)
    target_compile_options(brpc-shared PRIVATE -Wno-error=vla)
    # 或更宽松：
    target_compile_options(brpc-static PRIVATE -Wno-vla-extension)
endif()
```

**方案 B: 就地修改 brpc 宏（如果方案 A 无效）**

修改 `third_party/brpc/src/butil/macros.h:425`:

```cpp
// 修改前
const unsigned name##_stack_array_size = (name##_size <= (maxsize) ? name##_size : 0);

// 修改后 - 使用宏参数展开确保编译期常量
#define DEFINE_SMALL_ARRAY(Tp, name, name##_size, maxsize)                  \
    enum { name##_stack_array_size = (name##_size <= (maxsize) ? name##_size : 0) }; \
    char name##_stack_array[sizeof(Tp) * name##_stack_array_size];          \
    Tp* name = (name##_stack_array_size > 0)                                \
               ? reinterpret_cast<Tp*>(name##_stack_array)                  \
               : reinterpret_cast<Tp*>(malloc(sizeof(Tp) * name##_size));
```

> ⚠️ 注意：方案 B 改变了 `name##_stack_array_size` 的作用域（从 const unsigned 变为 enum），需验证 brpc 其他使用该变量的地方。

**验收标准**:
```bash
# 在 macOS 上
rm -rf build && mkdir build && cd build
cmake .. -DBUILD_TESTS=ON -DBUILD_EXAMPLES=ON
make -j$(nproc) cedar
# 应无错误完成
```

---

### 🔴 P0-2: 修复 4 个 NOT_BUILT 测试

**现状**: 以下测试在 CMakeLists.txt 中定义，但因编译/链接失败未生成二进制：

| 测试 | 路径 | 依赖 |
|------|------|------|
| `test_where_clause` | `tests/cypher/test_where_clause.cc` | `cedar_cypher` |
| `test_parameterized_query` | `tests/cypher/test_parameterized_queries.cc` | `cedar_cypher` |
| `test_cypher_gcn_routing` | `tests/gcn/test_cypher_gcn_routing.cc` | `cedar_cypher cedar_gcn` |
| `test_storage_extensions` | `tests/gcn/test_storage_extensions.cc` | `cedar_gcn` |

**诊断步骤**:
```bash
cd build
cmake --build . --target test_where_clause 2>&1 | tee /tmp/build_where_clause.log
cmake --build . --target test_parameterized_query 2>&1 | tee /tmp/build_param.log
cmake --build . --target test_cypher_gcn_routing 2>&1 | tee /tmp/build_gcn.log
cmake --build . --target test_storage_extensions 2>&1 | tee /tmp/build_storage_ext.log
```

**常见修复模式**:
1. **链接缺失库**: 在 `tests/CMakeLists.txt` 中为对应目标添加缺失的 `${CEDAR_TEST_LIBS}` 或 `gRPC::grpc++`
2. **头文件路径问题**: 检查是否缺少 `#include "cedar/xxx.h"`
3. **符号未定义**: 检查依赖库是否已编译（可能因 P0-1 的编译失败导致 `cedar` 库不完整）

**验收标准**:
```bash
cd build
ctest -R "test_where_clause|test_parameterized_query|test_cypher_gcn_routing|test_storage_extensions" --output-on-failure
# 4 个测试均应编译通过并执行
```

---

### 🟡 P0-3: 修复 flaky test `DistributedCrudTest.TombstoneWithColumnId`

**现状**: 该测试在 `ctest -j4` 并行运行时偶发失败，单独运行通过。

**可能原因**:
1. 测试使用了共享的临时目录 `/tmp/test_crud`，并行时冲突
2. 测试间状态未完全清理

**文件**: `tests/cluster/test_distributed_crud.cc`

**修复指导**:
- 在 `SetUp()` 中使用**唯一临时目录**（如 `mkdtemp` 或基于 PID + 时间戳的目录）
- 在 `TearDown()` 中确保完全清理

```cpp
// 在 test fixture 的 SetUp 中
char tmpdir[] = "/tmp/test_crud_XXXXXX";
mkdtemp(tmpdir);
data_dir_ = tmpdir;

// TearDown 中
std::filesystem::remove_all(data_dir_);
```

**验收标准**:
```bash
# 连续运行 10 次均应通过
for i in {1..10}; do
    ./build/tests/test_distributed_crud --gtest_filter=DistributedCrudTest.TombstoneWithColumnId
done
```

---

## 3. Phase 2: 核心功能补全（Week 1-2）

### 🟡 P1-1: 实现 QueryD ParallelExecutor 真实 RPC 调用

**现状**: `ParallelExecutor::ExecuteParallel` 提交任务到线程池，但 RPC 是模拟的。

**文件**:
- `src/queryd/distributed_executor.cpp:170-210`
- `src/queryd/distributed_executor.cpp:610-650`

**当前代码（stub）**:
```cpp
// TODO: Implement actual RPC call
r.status = Status::OK();
completed++;
promises[i].set_value();
```

**目标实现**:

```cpp
// ParallelExecutor::ExecuteParallel
for (size_t i = 0; i < tasks.size(); ++i) {
    auto task = [&tasks, &results, &promises, i, storage_client, ctx]() {
        const auto& t = tasks[i];
        auto& r = results[i];
        r.partition_id = t.partition_id;
        r.sequence = t.sequence;

        // 真实 RPC 调用
        auto status = storage_client->ExecuteSubQuery(
            t.partition_id, t.query_fragment, t.parameters, &r.rows);
        
        r.status = status;
        ctx->stats.storage_nodes_accessed++;
        ctx->stats.network_roundtrips++;
        
        promises[i].set_value();
    };
    // ... 提交到线程池
}
```

**需要新增/修改的接口**:
1. `src/queryd/query_storage_client.h`: 添加 `ExecuteSubQuery()` 方法
2. `src/queryd/query_storage_client.cpp`: 实现向 StorageD 发送 `ExecuteQueryRequest` RPC
3. `proto/query_service.proto`: 确认已有 `SubQueryRequest` / `SubQueryResponse`，或复用现有消息

**验收标准**:
- `ParallelExecutor` 的单元测试中，mock storage_client 验证 `ExecuteSubQuery` 被正确调用
- 端到端测试中，跨分区查询返回非空结果

---

### 🟡 P1-2: 实现 DistributedExecutor::ExecuteSinglePartition 真实 RPC

**现状**: `ExecuteSinglePartition` 获取 leader 地址后，注释 `// TODO: Implement RPC to storage node for query execution`

**文件**: `src/queryd/distributed_executor.cpp:610-650`

**实现指导**:
- 复用 `QueryStorageClient` 已有的 `ScanNodeV2` / `ScanEdgeV2` / `Get` 接口
- 根据查询类型（点查/扫描/遍历）选择对应的 RPC 方法
- 将查询结果转换为 `cypher::ResultSet`

**验收标准**:
- `test_query_dispatcher` 或新增测试中，单分区查询通过 RPC 返回正确数据

---

### 🟡 P1-3: 增强 FailoverManager 健康检查

**现状**: `CheckReplicaHealth()` 默认返回 `true`，无真实探活逻辑。

**文件**:
- `src/dtx/storage/failover_manager.cc:274-280`
- `src/dtx/storage/failover_manager.cc:388-430`

**当前代码**:
```cpp
bool PartitionFailoverController::CheckReplicaHealth(NodeID node_id) {
    std::lock_guard<std::mutex> lock(replica_health_mutex_);
    auto it = replica_health_.find(node_id);
    if (it != replica_health_.end()) {
        return it->second;
    }
    return true;  // 默认健康 - 生产风险！
}
```

**目标实现**:

```cpp
bool PartitionFailoverController::CheckReplicaHealth(NodeID node_id) {
    std::lock_guard<std::mutex> lock(replica_health_mutex_);
    auto it = replica_health_.find(node_id);
    if (it != replica_health_.end()) {
        return it->second;
    }
    // 无健康记录时，不盲目认为健康
    // 方案：触发一次主动探活
    return PerformActiveHealthCheck(node_id);
}

bool PartitionFailoverController::PerformActiveHealthCheck(NodeID node_id) {
    // 1. 获取节点地址
    auto addr = node_directory_->GetAddress(node_id);
    if (!addr.ok()) return false;
    
    // 2. TCP 连接探活（超时 500ms）
    auto sock = butil::tcp_connect(butil::EndPoint(addr.value()), 500);
    if (sock < 0) {
        replica_health_[node_id] = false;
        return false;
    }
    close(sock);
    
    // 3. 可选：发送轻量级 RPC（如 GetPartitionInfo）验证服务层健康
    // auto status = storage_client_->Ping(node_id);
    
    replica_health_[node_id] = true;
    return true;
}
```

**验收标准**:
- `test_failover` 中新增测试：模拟节点网络断开，`CheckReplicaHealth` 返回 false，触发故障转移
- 故障转移后，新 leader 可正常服务读写请求

---

### 🟡 P1-4: MetaServiceGrpcClient 连接池与故障转移

**现状**:
- `Connect()` 只连接第一个地址，无重试/重定向
- `TryReconnect()` 声明但未实现
- 缓存和 watch 流式 RPC 未实现

**文件**:
- `src/dtx/grpc/meta_service_grpc.cc`

**最小实现（MVP 标准）**:

```cpp
// Connect 实现轮询连接
Status MetaServiceGrpcClient::Connect(const std::vector<std::string>& addresses) {
    for (const auto& addr : addresses) {
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        auto stub = cedar::meta::MetaService::NewStub(channel);
        
        // 健康检查
        HealthCheckRequest req;
        HealthCheckResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        if (stub->HealthCheck(&ctx, req, &resp).ok()) {
            current_stub_ = std::move(stub);
            current_address_ = addr;
            return Status::OK();
        }
    }
    return Status::IOError("Failed to connect to any MetaD node");
}

// 定期重连背景线程
void MetaServiceGrpcClient::TryReconnect() {
    if (reconnecting_.exchange(true)) return;
    
    std::thread([this]() {
        for (int attempt = 0; attempt < max_retries_; ++attempt) {
            if (Connect(addresses_).ok()) {
                reconnecting_ = false;
                return;
            }
            std::this_thread::sleep_for(retry_interval_);
        }
        reconnecting_ = false;
    }).detach();
}
```

**验收标准**:
- 在 `test_cluster_connection` 或新增测试中：
  1. 连接第一个 MetaD 节点后将其 kill
  2. 客户端在 3 秒内自动切换到第二个节点
  3. 读写操作不中断

---

## 4. Phase 3: 端到端验证（Week 2-3）

### 🟢 E2E-1: Docker Compose 全链路测试

**目标**: 验证 3 节点集群的完整读写查询链路

**步骤**:

```bash
cd cedar-docker-compose
./scripts/quick-start.sh

# 等待集群就绪
sleep 10
./scripts/cedar-cli.sh -e "CREATE SPACE test_space (partition_num=8, replica_factor=3)"
./scripts/cedar-cli.sh -e "USE test_space"
./scripts/cedar-cli.sh -e "PUT (n:Person {id: 1, name: 'Alice'})"
./scripts/cedar-cli.sh -e "PUT (n:Person {id: 2, name: 'Bob'})"
./scripts/cedar-cli.sh -e "MATCH (n:Person) RETURN n.name"
# 期望返回 Alice, Bob
```

**文件**:
- `cedar-docker-compose/docker-compose.yml`
- `cedar-docker-compose/scripts/quick-start.sh`
- `cedar-docker-compose/scripts/cedar-cli.sh`

**如果 CLI 不存在或功能不全**: 使用 `build/cedar-cli.sh` 或 `curl`/`grpcurl` 直接调用 GraphD gRPC 接口

---

### 🟢 E2E-2: 故障注入测试

**目标**: 验证节点故障后的自动恢复能力

**测试脚本** (`scripts/test_failover_injection.sh`):

```bash
#!/bin/bash
set -e

CLUSTER_DIR=$(pwd)/test_cluster
mkdir -p $CLUSTER_DIR
cd $CLUSTER_DIR

# 1. 启动 3 节点集群
docker-compose -f ../cedar-docker-compose/docker-compose.yml up -d

# 2. 写入测试数据
for i in {1..100}; do
    ../scripts/cedar-cli.sh -e "PUT (n:Node {id: $i})"
done

# 3. 找到 StorageD leader 节点
LEADER=$(../scripts/cedar-cli.sh -e "SHOW HOSTS" | grep "LEADER" | head -1 | awk '{print $1}')
echo "Current leader: $LEADER"

# 4. Kill leader
docker kill $LEADER

# 5. 等待故障转移
echo "Waiting for failover..."
sleep 15

# 6. 验证读取不中断
RESULT=$(../scripts/cedar-cli.sh -e "MATCH (n:Node) RETURN count(n)")
echo "Query result after failover: $RESULT"

# 7. 验证写入恢复
../scripts/cedar-cli.sh -e "PUT (n:NewNode {id: 999})"

# 8. 重启被 kill 的节点
docker start $LEADER
sleep 10

# 9. 验证数据一致性（所有节点数据相同）
../scripts/cedar-cli.sh -e "MATCH (n:Node) RETURN count(n)"
# 期望: 101

docker-compose down
```

**验收标准**:
- kill leader 后，15 秒内查询/写入恢复正常
- 重启节点后自动加入集群并同步数据
- 数据无丢失、无重复

---

### 🟢 E2E-3: 快照恢复测试

**目标**: 验证重启后数据完整性

**测试脚本**:

```bash
# 1. 启动集群并写入数据
./scripts/quick-start.sh
for i in {1..50}; do
    ./scripts/cedar-cli.sh -e "PUT (n:Test {id: $i, val: $i})"
done

# 2. 触发快照（通过日志增长或 API）
./scripts/cedar-cli.sh -e "ADMIN SNAPSHOT"

# 3. 完全停止并删除容器
docker-compose down

# 4. 重新启动（数据卷保留）
docker-compose up -d

# 5. 验证数据
COUNT=$(./scripts/cedar-cli.sh -e "MATCH (n:Test) RETURN count(n)")
if [ "$COUNT" != "50" ]; then
    echo "FAIL: Expected 50, got $COUNT"
    exit 1
fi
echo "PASS: Snapshot recovery verified"
```

---

### 🟢 E2E-4: 24 小时长稳测试

**目标**: 发现内存泄漏、数据不一致、Raft 日志无限增长等问题

**工具**: `build/cedar_cluster_benchmark` 或 `build/test_docker_perf_benchmark`

**测试参数**:
- Duration: 86400s (24h)
- Concurrent Clients: 50
- Write Ratio: 30%
- Key Range: 1,000,000
- Value Size: 256 bytes

**监控指标**:
```bash
# 每小时记录
# - 内存 RSS
docker stats --no-stream | grep cedar

# - 磁盘使用
du -sh data/metad data/storaged

# - 查询成功率
# 从 benchmark 输出中提取

# - 数据一致性校验（每 4 小时）
# 对比各副本的 checksum
```

**验收标准**:
- 24 小时内无 OOM、无崩溃
- 查询成功率 > 99.9%
- 磁盘增长速率 < 10MB/小时（正常写入外）
- 一致性校验 100% 通过

---

## 5. Phase 4: 生产加固（Week 3-4）

### 🟢 OP-1: 完善 Helm Chart 生产配置

**文件**:
- `helm-chart/cedargraph/values.yaml`
- `helm-chart/cedargraph/templates/*.yaml`

**检查清单**:
- [ ] Resource limits/requests 设置合理（MetaD: 2CPU/4GB, StorageD: 4CPU/8GB, GraphD: 2CPU/4GB）
- [ ] 持久化卷声明（PVC）使用 SSD StorageClass
- [ ] PodDisruptionBudget 确保最小可用副本数
- [ ] NetworkPolicy 限制跨命名空间访问
- [ ] Liveness/Readiness Probe 配置（HTTP/gRPC 健康检查端点）
- [ ] Pod 拓扑分布约束（跨可用区部署）
- [ ] Init Container 检查依赖服务就绪

---

### 🟢 OP-2: 可观测性增强

**文件**:
- `config/prometheus.yml`
- `config/grafana/dashboards/`
- `src/dtx/transaction_metrics.cc`（已有基础）

**必须有的告警规则** (`config/cedar_alerts.yml`):

```yaml
groups:
  - name: cedar-critical
    rules:
      - alert: MetaDLeaderDown
        expr: up{job="metad"} < 2
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "MetaD quorum lost"

      - alert: StorageDLeaderMissing
        expr: cedar_storaged_leader_count == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "StorageD partition {{ $labels.partition }} has no leader"

      - alert: HighReplicationLag
        expr: cedar_raft_log_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Raft follower lag too high"

      - alert: QueryErrorRate
        expr: rate(cedar_query_errors_total[5m]) / rate(cedar_query_total[5m]) > 0.05
        for: 2m
        labels:
          severity: warning
```

**必须有的 Grafana Dashboard**:
- Cluster Overview（节点状态、QPS、延迟）
- Raft Status（各分区 leader/follower 分布、log lag）
- Storage Metrics（SST 文件数、compaction 速率、WAL 大小）
- Query Performance（P50/P99 延迟、错误率、活跃连接数）
- Resource Usage（CPU/Memory/磁盘/网络）

---

### 🟢 OP-3: 运维手册

**新建文件**: `docs/OPERATIONS.md`

**必须包含的章节**:
1. **部署**: 单节点开发 / Docker Compose / Kubernetes 生产
2. **扩缩容**: 增加 StorageD 节点、调整分区数（如支持）
3. **备份恢复**: 手动触发快照、从快照恢复、跨集群迁移
4. **故障排查**:
   - MetaD 无法选举 leader → 检查网络连通性、节点数是否为奇数
   - StorageD NotLeader 错误 → 检查分区 leader 分布、网络分区
   - 查询超时 → 检查热点分区、compaction 状态
   - 磁盘满 → 清理日志、触发手动 compaction
5. **升级流程**: 滚动升级顺序（MetaD → StorageD → GraphD）
6. **Emergency**: 紧急下线节点、强制 leader 切换、数据一致性修复

---

### 🟢 OP-4: 安全加固

**检查清单**:
- [ ] gRPC TLS 配置验证（`config/cedar.yaml` 中 tls.enabled=true 时正确加载证书）
- [ ] 节点间通信启用 mTLS
- [ ] Docker 镜像非 root 运行
- [ ] 敏感配置（如 TLS 私钥）使用 Kubernetes Secret 挂载
- [ ] Snyk/Trivy 扫描无 HIGH/CRITICAL 漏洞

---

## 6. 每日 Checklist

建议按以下节奏推进：

### Day 1-2: Phase 1
- [ ] 修复编译失败（P0-1）
- [ ] 诊断并修复 4 个 NOT_BUILT 测试（P0-2）
- [ ] 修复 flaky test（P0-3）
- [ ] 本地 `make -j && ctest --output-on-failure` 全绿

### Day 3-5: Phase 2
- [ ] 实现 ParallelExecutor RPC（P1-1）
- [ ] 实现 ExecuteSinglePartition RPC（P1-2）
- [ ] 增强 CheckReplicaHealth（P1-3）
- [ ] 实现 MetaServiceGrpcClient 故障转移（P1-4）
- [ ] 补充对应单元测试

### Day 6-10: Phase 3
- [ ] Docker Compose 一键部署成功（E2E-1）
- [ ] 故障注入测试通过（E2E-2）
- [ ] 快照恢复测试通过（E2E-3）
- [ ] 24 小时长稳测试通过（E2E-4）

### Day 11-14: Phase 4
- [ ] Helm Chart 生产化（OP-1）
- [ ] 告警规则 + Grafana Dashboard（OP-2）
- [ ] 运维手册完成（OP-3）
- [ ] 安全扫描通过（OP-4）

---

## 7. 风险与应急预案

| 风险 | 可能性 | 影响 | 应急措施 |
|------|--------|------|----------|
| brpc 编译补丁导致性能退化 | 低 | 高 | 准备纯 Linux 编译方案，macOS 仅用于开发 |
| 24h 长稳测试发现内存泄漏 | 中 | 高 | 使用 AddressSanitizer (`-fsanitize=address`) 快速定位 |
| 故障转移时间 > 15s | 中 | 中 | 调短 `heartbeat_timeout_sec` 和 `leader_lease_duration` |
| 数据一致性校验失败 | 低 | 极高 | 暂停上线，启用 `test_debug_storage` 和 `test_2pc_recovery` 深度排查 |
| Raft 日志无限增长 | 中 | 高 | 检查 `on_snapshot_save` 是否正常触发、snapshot_interval 配置 |
| 编译器升级再次破坏构建 | 中 | 中 | 在 CI 中锁定编译器版本，新增编译器兼容性测试 |

---

## 附录 A: 关键文件速查表

| 功能 | 文件 |
|------|------|
| MetaD 快照序列化 | `src/dtx/meta/meta_service.cc:474-600` |
| 分区分配 | `src/dtx/meta/meta_service.cc:309-356` |
| 心跳检测 | `src/dtx/meta/meta_service.cc:807-850` |
| StorageD 快照 | `src/dtx/storage/braft_partition_raft.cc:395-470` |
| 2PC Prepare | `src/dtx/storage_impl/storage_service_impl.cc:707-830` |
| GraphD 查询路由 | `src/service/graph_service_router.cc:1173-1400` |
| QueryD 并行执行 | `src/queryd/distributed_executor.cpp:170-210, 610-650` |
| 故障转移 | `src/dtx/storage/failover_manager.cc:255-430` |
| Raft 日志配置 | `src/dtx/storage/braft_partition_raft.cc:595-597` |
| MetaD gRPC 客户端 | `src/dtx/grpc/meta_service_grpc.cc` |
| CMake 主配置 | `CMakeLists.txt` |
| 测试配置 | `tests/CMakeLists.txt` |
| Docker Compose | `cedar-docker-compose/docker-compose.yml` |
| Helm Chart | `helm-chart/cedargraph/` |
| 监控配置 | `config/prometheus.yml`, `config/cedar_alerts.yml` |

---

## 附录 B: 快速验证命令

```bash
# 1. 完整编译
rm -rf build && mkdir build && cd build
cmake .. -DBUILD_TESTS=ON -DBUILD_EXAMPLES=ON -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# 2. 运行全部测试
ctest --output-on-failure -j$(nproc)

# 3. 运行特定测试
./tests/test_failover
./tests/test_distributed_crud --gtest_filter="*"
./tests/test_partition_raft

# 4. 启动本地集群（需先编译）
./scripts/start_local_cluster.sh  # 如不存在，参考 cedar-docker-compose

# 5. 基准测试
./build/cedar_cluster_benchmark --nodes=3 --duration=60 --clients=10

# 6. 检查内存泄漏（开发调试用）
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address"
make -j$(nproc)
./tests/test_distributed_crud
```

---

**文档维护**: 每完成一个 Phase，在此文件中打勾并记录实际耗时。遇到未预见的问题时，在此文档末尾追加 "Change Log"。
