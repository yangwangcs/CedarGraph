# CedarGraph 生产就绪性深度审计报告

**审计日期：** 2026-05-20  
**审计范围：** 全代码库（~515 个源码文件，~120k LOC）  
**审计方法：** 逐模块静态代码审查，TODO/FIXME/STUB 扫描，并发安全与资源管理分析

---

## 一、执行摘要

### 总体结论

**CedarGraph 当前不具备生产上线条件。** 代码库呈现典型的"研究原型→工程化"过渡状态：核心数据结构扎实，但分布式事务、查询引擎、存储引擎等关键子系统存在大量未实现功能（STUB）、数据竞争、资源泄漏和静默数据损坏风险。

| 维度 | 评分 | 说明 |
|------|------|------|
| 数据正确性 | 4/10 | 存在静默截断、类型损坏、日志未达多数确认等问题 |
| 并发安全 | 4/10 | 多处数据竞争（plan cache、anchor stats、streaming partitioner） |
| 容错恢复 | 3/10 | Manifest 回放缺失、WAL 迁移未实现、Raft 自定义实现不安全 |
| 可观测性 | 2/10 | 无 Prometheus 指标、假指标（hardcoded P99）、原始查询文本直接日志 |
| 资源管理 | 5/10 | 无界缓存、内存物化全部结果集、线程池缺失 |
| 安全 | 3/10 | 所有 gRPC 通道默认 Insecure，无 TLS/mTLS |

### 各模块评分一览

| 模块 | 评分 | 关键风险 |
|------|------|----------|
| `src/core/` (基础库) | 7/10 | mmap 泄漏、CRC32C 未对齐读取 |
| `src/types/` (类型系统) | 8/10 | system_clock 非单调、union type-punning UB |
| `src/db/` (DB 层) | 5/10 | Compaction 完全未实现、Manifest 回放缺失 |
| `src/graph/` (图抽象) | 4/10 | ref_count 泄漏、GetInNeighbors 全 stub |
| `src/storage/` (LSM 引擎) | 5/10 | "Lock-free" skiplist 实为全局互斥锁、热路径 debug print |
| `src/sst/` (SSTable) | 6/10 | 大数据静默截断、全文件内存构建 |
| `src/dtx/storage_impl/` (存储服务) | 5/10 | **Proto 反序列化损坏 entity_type** |
| `src/cypher/` (Cypher 引擎) | 4/10 | Plan cache 无线程安全、Value 类型访问崩溃 |
| `src/query/` (查询层) | 5/10 | 静态非原子统计竞争 |
| `src/queryd/` (查询调度) | 4/10 | 限流 TOCTOU 竞争、超时未强制执行 |
| `src/dtx/` (分布式事务) | 4/10 | DTXService 为透传 stub、线程-per-RPC |
| `src/partition/` (分区) | 5/10 | 迁移 checksum 为假、WAL catch-up 未实现 |
| `src/dtx/raft/` (Raft) | 5/10 | 自定义 Raft 不安全、LOG(FATAL) 崩溃进程 |
| `src/transaction/` (事务) | 6/10 | 事务池未清理状态、WAL 未 fsync |
| `src/service/` (服务层) | 5/10 | 无熔断限流、连接池实为泄漏缓存 |
| `src/gcn/` (图计算) | 4/10 | 全 Insecure 通道、水印 GC 可能误删活跃数据 |
| `src/governance/` (治理) | 6/10 | 无分布式共识、HTTP 单线程阻塞 |
| `src/driver/` (客户端) | 5/10 | 字符串匹配错误分类、无网络层 |

---

## 二、P0 生产阻断问题（必须修复才能上线）

### P0-1: Proto 反序列化静默损坏 entity_type
**位置：** `src/dtx/storage_impl/storage_service_impl.cc:44-55`

`CedarKeyToProto` 将 `key.flags()` 存入 `type_flags`，而 `ProtoToCedarKey` 将其直接 cast 为 `EntityType`。由于 `flags` 低位是 `OpType`（0=CREATE, 1=UPDATE, 2=DELETE），一个 Vertex Update 会被反序列化为 `EntityType::EdgeOut`。

**影响：** 数据在 RPC 传输后类型错乱，图遍历返回错误实体。  
**修复：** 在 protobuf 中增加独立的 `entity_type` 字段，或在序列化时使用明确的 bit-pack 格式。

---

### P0-2: 自定义 Raft 实现未达多数确认即返回 OK
**位置：** `src/dtx/storage/raft_replication.cc:476-506`

`StorageRaftGroup::Propose` 仅将日志追加到本地磁盘即返回 `Status::OK()`，不向 follower 复制，不等待 quorum。

**影响：** 写入被确认后若 leader 崩溃，数据丢失。严重违反 Raft 安全保证。  
**修复：** 立即废弃自定义 Raft，全部使用 braft；或实现完整的 AppendEntries + WaitForApplied。

---

### P0-3: Cypher 引擎 Plan Cache 无线程安全
**位置：** `src/cypher/cypher_engine.cc:139-150`

`plan_cache_` 是裸 `std::map`，无任何同步原语保护。多线程 gRPC 调用下会触发数据竞争，可能导致崩溃或返回损坏的执行计划。

**影响：** 任何并发查询场景下都可能崩溃或返回错误结果。  
**修复：** 使用 `std::shared_mutex` 保护读，或替换为线程安全的有界 LRU cache（如 `LRUCache<std::string, std::unique_ptr<ExecutionPlan>>`）。

---

### P0-4: `Value::GetXxx()` 使用裸 `std::get` 导致崩溃
**位置：** `src/cypher/value.cc:433-435`

```cpp
bool Value::GetBool() const {
  return std::get<bool>(value_);  // 类型不匹配时抛出 std::bad_variant_access
}
```

**影响：** 查询返回意外类型时进程 terminate。  
**修复：** 全部替换为 `std::get_if` + 错误返回，或增加类型守卫断言。

---

### P0-5: Compaction 完全未实现
**位置：** `src/db/graph_db_impl.cc:651-700`

`DoCompaction` 是空壳函数，仅递增计数器后返回 OK。SST 文件将无限增长。

**影响：** 磁盘空间无界膨胀、读放大无限增加、最终 OOM/磁盘满。  
**修复：** 实现 LSM 标准 compaction（Level 间 merge-sort + SST 重写）。

---

### P0-6: Manifest 回放缺失 → 重启后元数据丢失
**位置：** `src/db/manifest.cc:413-439`

数据库启动时检测到 CURRENT 文件存在，但注释明确说明 "Manifest file reading requires version state reconstruction from edit records (not yet implemented)"。

**影响：** 重启后无法识别已存在的 SST 文件，数据不可见或损坏。  
**修复：** 实现 `ManifestManager::LoadCurrentVersion`，回放 edit records 重建 `VersionSet`。

---

### P0-7: `GetInNeighbors` 完全未实现且静默返回空
**位置：** `src/graph/cedar_graph.cc:80-91`

反向边查询返回空 vector，调用方无法区分 "无邻居" 和 "功能未实现"。

**影响：** 任何依赖反向遍历的查询（如社交网络粉丝列表）返回错误结果。  
**修复：** 实现基于 EdgeIn 索引的反向遍历，或将返回类型改为 `StatusOr<vector<Neighbor>>` 以暴露未实现状态。

---

### P0-8: PartitionMigrator 迁移后数据一致性校验为假 Checksum
**位置：** `src/dtx/storage/partition_migrator.cc:546-551`

```cpp
Status PartitionMigrator::CalculateChecksum(...) {
  std::stringstream ss;
  ss << "chk_" << pid << "_" << std::chrono::system_clock::now().time_since_epoch().count();
  *checksum = ss.str();
  return Status::OK();
}
```

**影响：** 迁移后的静默数据损坏永远无法被检测。  
**修复：** 对所有 KV pair 计算 CRC32C 或 xxHash checksum。

---

### P0-9: WAL Catch-up 在迁移期间仅计数不复制
**位置：** `src/dtx/storage/partition_migrator.cc:435-455`

迁移期间的 WAL 操作只被计数，从未发送到目标节点。

**影响：** 迁移期间写入的数据在目标节点丢失。  
**修复：** 将 WAL entry 流式发送到目标节点并应用。

---

### P0-10: 事务池未清理状态 → 脏数据跨事务泄漏
**位置：** `src/transaction/transaction_pool.cc:41-56`

```cpp
OCCTransaction* TransactionPool::Acquire(...) {
  if (!pool_.empty()) {
    auto* txn = pool_.front();
    pool_.pop();
    // 注意：这里需要确保事务已结束状态  <-- 注释承认 bug
    return txn;  // 未清理 read_set_/write_set_
  }
}
```

**影响：** 旧事务的读写集泄漏到新事务，导致脏读、幻读或错误冲突检测。  
**修复：** 在 `Acquire` 返回前调用 `txn->Cleanup()`。

---

## 三、P1 严重问题（强烈建议修复）

### P1-1: QueryD 限流存在 TOCTOU 竞争
**位置：** `src/queryd/query_service_full.cpp:429-435`

```cpp
bool AcquireQuerySlot() {
  if (current_queries_.load() >= options_.max_concurrent_queries) return false;
  current_queries_++;  // 两个线程可能同时通过检查并同时递增
}
```

**修复：** 使用 `compare_exchange_weak` 或 `std::counting_semaphore`。

---

### P1-2: 查询超时存储但不强制执行
**位置：** `src/queryd/distributed_executor.cpp:164-214`

`ctx.timeout_ms` 被设置但 `DistributedExecutor::Execute` 和 `ParallelExecutor::ExecuteParallel` 从不检查。

**修复：** 在子查询分发前检查截止时间，使用 `std::future::wait_for` 替代无限 `wait()`。

---

### P1-3: Optimized2PCEngine 每个 participant 新建线程
**位置：** `src/dtx/optimized_2pc_engine.cc:476-500`

高并发下线程资源耗尽。

**修复：** 使用固定线程池 + 异步回调。

---

### P1-4: CRC32C 硬件路径存在未对齐读取 UB
**位置：** `src/core/crc32c.cc:117`

```cpp
l = _mm_crc32_u64(l, *reinterpret_cast<const uint64_t*>(p));
```

`p` 不保证 8 字节对齐。在 strict-alignment 架构（部分 ARM）上会崩溃。

**修复：** 使用 `memcpy` 到对齐临时变量后再调用 CRC 指令。

---

### P1-5: Storage 热路径存在 Debug Print
**位置：** `src/storage/cedar_memtable.cc:35-37`

```cpp
if (key.entity_id() == 344 && key.entity_type() == EntityType::EdgeOut) {
  std::cerr << "DEBUG: CedarMemTable::Put entity_id=344..." << std::endl;
}
```

**修复：** 删除所有热路径中的 `std::cout`/`std::cerr`。

---

### P1-6: SSTable 大数据静默截断
**位置：** `src/sst/zone_columnar_builder.cc:120-133`

超过 12 字节的数据被静默截断，未路由到 Blob 存储。

**修复：** 大值路由到 `BlobFileManager`，永不静默截断。

---

### P1-7: GraphServiceRouter 连接池无上限且无清理
**位置：** `src/service/graph_service_router.cc:1153-1164`

`storage_stubs_` 无限增长，死通道永不清理。

**修复：** 实现带最大连接数、空闲超时、健康检查的真实连接池。

---

### P1-8: GCN 所有对等/协调器通信使用 InsecureChannel
**位置：** `src/gcn/gcn_node.cc:84-91`

**修复：** 统一使用 `TlsCredentialFactory::CreateClientCredentials()`。

---

## 四、P2 改进建议（提升工程成熟度）

1. **建立结构化日志体系** — 禁止 `src/` 中使用 `std::cout`/`std::cerr`，统一使用 `spdlog` 或现有 `Logger` 接口。
2. **接入 Prometheus 指标** — 在存储 Put/Get/Flush/Compaction、查询执行、事务 2PC 等边界添加 Counter/Histogram。
3. **统一错误码体系** — 禁止字符串匹配错误分类（`retry_policy.cc`），扩展 `Status` 携带结构化错误码。
4. **文档化锁顺序** — `cf_mutex_`、`wal_mutex_`、`bg_mutex_`、`snapshot_mutex_`、`txn_mutex_` 等之间无文档化顺序，存在死锁风险。
5. **实现查询取消传播** — gRPC `IsCancelled()` 仅在入口检查，未传播到 `DistributedExecutor`、`ParallelExecutor` 和存储扫描。
6. **移除或完成所有 (void) 参数抑制** — 大量 `(void)param` 表示未实现功能，应附带 `TODO(#issue)` 或移除。
7. **查询计划缓存加界限** — `plan_cache_` 无最大尺寸、无 TTL，应替换为有界 LRU。
8. **使用单调时钟** — `Timestamp::Now()` 使用 `system_clock`，应改用 `steady_clock` + 原子计数器。

---

## 五、上线路径建议

### 阶段 1：数据正确性（ blocker ）
- [ ] 修复 P0-1（Proto 类型损坏）
- [ ] 修复 P0-2（Raft 未达多数）
- [ ] 修复 P0-5（Compaction 未实现）
- [ ] 修复 P0-6（Manifest 回放）
- [ ] 修复 P0-8（迁移 checksum 为假）
- [ ] 修复 P0-9（WAL catch-up）
- [ ] 修复 P0-10（事务池状态泄漏）

### 阶段 2：并发安全与稳定性
- [ ] 修复 P0-3（Plan cache 线程安全）
- [ ] 修复 P0-4（Value 类型崩溃）
- [ ] 修复 P0-7（GetInNeighbors）
- [ ] 修复 P1-1（限流 TOCTOU）
- [ ] 修复 P1-3（线程-per-RPC）

### 阶段 3：可观测性与运维
- [ ] 接入 Prometheus 指标导出
- [ ] 统一结构化日志
- [ ] 实现 gRPC TLS/mTLS
- [ ] 实现熔断、限流、连接池治理

### 阶段 4：性能优化
- [ ] 实现真正的 Lock-free skiplist 或移除误导性命名
- [ ] 查询内存预算与超时强制执行
- [ ] 分区哈希从顺序映射改为 MurmurHash

---

## 六、审计统计

| 统计项 | 数值 |
|--------|------|
| 源码文件总数 | ~515 |
| TODO/FIXME/STUB/UNIMPLEMENTED 总数 | **362** |
| dtx 模块标记数 | 215 |
| service 模块标记数 | 37 |
| queryd 模块标记数 | 17 |
| gcn 模块标记数 | 17 |
| P0 阻断问题 | 10 |
| P1 严重问题 | 8 |
| P2 改进建议 | 8 |

---

*本报告基于静态代码分析生成。建议后续配合压力测试、混沌测试（Chaos Testing）和故障注入进行动态验证。*
