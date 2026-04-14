# CedarGraph 分布式系统完整性检查清单

## 当前状态概览

| 组件 | 状态 | 完成度 | 优先级 |
|-----|------|--------|--------|
| MetaD (元数据服务) | 🟡 Partial | 70% | P0 |
| StorageD (存储服务) | 🟡 Partial | 75% | P0 |
| DTx Coordinator | 🟡 Partial | 60% | P0 |
| Load Balancer | 🟡 Partial | 65% | P1 |
| gRPC 通信层 | 🔴 Missing | 30% | P0 |
| Raft 共识 | 🔴 Stub | 40% | P0 |
| 服务发现 | 🟡 Partial | 50% | P1 |
| 数据迁移 | 🔴 Missing | 20% | P1 |
| 监控指标 | 🔴 Missing | 0% | P2 |

---

## 详细组件分析

### 1. MetaD 元数据服务

**已实现:**
- ✅ Space/Partition/Node 内存管理
- ✅ 基本的 Raft 接口 (MemoryRaft)
- ✅ 心跳机制框架
- ✅ gRPC 服务定义

**缺失/待完善:**
- 🔲 **真实 Raft 实现** (braft/etcd-raft 集成)
  - 当前: `MemoryRaft` (仅内存，无持久化)
  - 需要: 基于 RocksDB 的日志存储 + 快照
- 🔲 **Leader 选举和故障转移**
  - 当前: 手动指定 leader
  - 需要: 自动选举，脑裂处理
- 🔲 **多节点 Raft 集群支持**
  - 当前: 单节点运行
  - 需要: 3/5/7 节点集群配置

**实现建议:**
```cpp
// 集成 braft 或 etcd-raft
class BRaftNode : public RaftInterface {
    braft::Node* node_;
    RocksDBLogStorage* log_storage_;
    
    // 实现状态机复制
    void on_apply(const std::vector<LogEntry>& entries) override;
    void on_snapshot_save(braft::SnapshotWriter* writer) override;
    void on_snapshot_load(braft::SnapshotReader* reader) override;
};
```

---

### 2. StorageD 存储服务

**已实现:**
- ✅ 共享 LSM-Tree 架构
- ✅ PartitionStorage 逻辑分区
- ✅ 2PC 接口 (Prepare/Commit/Abort)
- ✅ 分区索引 (PartitionIndex)

**缺失/待完善:**
- 🔲 **真实 gRPC 服务启动**
  - 当前: stub 实现
  - 需要: 完整的 StorageService gRPC server
- 🔲 **Raft 日志应用**
  - 当前: 独立 WAL
  - 需要: 与 Raft 日志集成
- 🔲 **Snapshot 机制**
  - 当前: 无
  - 需要: 定期生成 SST 快照
- 🔲 **数据迁移接收端**
  - 当前: 接口定义
  - 需要: 实际的数据接收和验证

**实现建议:**
```cpp
class StorageRaftStateMachine : public RaftStateMachine {
    PartitionManager* partition_mgr_;
    
    void ApplyLog(const LogEntry& entry) override {
        switch (entry.type) {
            case kPut: partition_mgr_->GetPartition(entry.pid)->Put(...); break;
            case kDelete: ...;
            case kMigrateIn: ReceivePartitionData(entry.data); break;
        }
    }
};
```

---

### 3. DTx 协调器

**已实现:**
- ✅ TWCD 引擎 (时间窗口冲突检测)
- ✅ OCC 验证引擎
- ✅ 事务上下文管理
- ✅ 本地书签管理器

**缺失/待完善:**
- 🔲 **跨节点 RPC 调用实现**
  - 当前: 头文件定义，无实现
  - 需要: gRPC 客户端完整实现
- 🔲 **全局时钟同步**
  - 当前: 本地时钟
  - 需要: HLC (Hybrid Logical Clock) 或 TSO
- 🔲 **死锁检测与处理**
  - 当前: 超时回滚
  - 需要: 分布式死锁检测
- 🔲 **事务恢复**
  - 当前: 无
  - 需要: 协调者故障后的恢复流程

**实现建议:**
```cpp
class DTxCoordinator {
    // 1. 实现真实 RPC 调用
    RpcClient* rpc_client_;  // 需要完整实现
    
    // 2. 集成 HLC
    HybridLogicalClock hlc_;
    
    // 3. 事务日志持久化
    TxnLogStore* txn_log_;  // 用于协调者恢复
};
```

---

### 4. gRPC 通信层

**已实现:**
- ✅ proto 文件定义
- ✅ 部分 stub 代码

**缺失/待完善:**
- 🔲 **Protobuf 版本兼容性解决**
  - 当前: 生成代码与系统库版本冲突
  - 需要: 统一使用 protobuf 3.21+
- 🔲 **StorageService 完整实现**
  - 当前: 只有接口，无 gRPC 绑定
  - 需要: full gRPC service implementation
- 🔲 **DTxService 实现**
  - 当前: 头文件定义
  - 需要: 完整的跨节点事务 RPC
- 🔲 **连接池管理**
  - 当前: 基础框架
  - 需要: 健康检查、自动重连、负载均衡

**实现建议:**
```bash
# 1. 解决 protobuf 版本问题
brew install protobuf@3.21
# 或升级系统 protobuf

# 2. 重新生成代码
protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` *.proto

# 3. 实现服务端
class StorageServiceImpl : public StorageService::Service {
    PartitionManager* mgr_;
    Status Put(ServerContext* ctx, const PutRequest* req, PutResponse* resp) override;
    Status Get(ServerContext* ctx, const GetRequest* req, GetResponse* resp) override;
    Status Prepare(ServerContext* ctx, const PrepareRequest* req, PrepareResponse* resp) override;
    Status Commit(ServerContext* ctx, const CommitRequest* req, CommitResponse* resp) override;
};
```

---

### 5. Load Balancer 负载均衡器

**已实现:**
- ✅ 多种均衡策略 (Leader/Data/QPS/Composite)
- ✅ 统计信息收集框架
- ✅ 迁移计划生成

**缺失/待完善:**
- 🔲 **实际数据迁移执行**
  - 当前: 计划生成，无执行
  - 需要: 流式数据迁移实现
- 🔲 **迁移进度追踪**
  - 当前: 无
  - 需要: 实时进度、断点续传
- 🔲 **一致性校验**
  - 当前: 无
  - 需要: 迁移后数据校验
- 🔲 **自动触发策略**
  - 当前: 手动触发
  - 需要: 基于阈值自动触发

**实现建议:**
```cpp
class DataMigrationExecutor {
    // 1. 源节点读取数据
    void ReadPartitionFromSource(PartitionID pid, NodeID source_node);
    
    // 2. 流式发送到目标
    void StreamDataToTarget(const std::vector<KVPair>& batch, NodeID target_node);
    
    // 3. 双写阶段 (保证一致性)
    void DualWritePhase(PartitionID pid, NodeID old_node, NodeID new_node);
    
    // 4. 切换所有权
    void TransferOwnership(PartitionID pid, NodeID new_leader);
    
    // 5. 校验
    bool VerifyMigration(PartitionID pid, Checksum expected);
};
```

---

### 6. 服务发现与健康检查

**已实现:**
- ✅ 基础心跳框架
- ✅ MetaD 注册接口

**缺失/待完善:**
- 🔲 **Gossip 协议**
  - 当前: 中心化 MetaD
  - 需要: 节点间 gossip 传播状态
- 🔲 **故障检测**
  - 当前: 简单超时
  - 需要: Phi Accrual 故障检测
- 🔲 **网络分区处理**
  - 当前: 无
  - 需要: 脑裂检测与恢复
- 🔲 **动态配置更新**
  - 当前: 静态配置
  - 需要: 运行时配置热更新

**实现建议:**
```cpp
class GossipProtocol {
    // 随机选择节点交换状态
    void DoGossipRound();
    
    // 传播节点状态变更
    void PropagateNodeState(NodeID node, NodeState state);
    
    // 合并收到的状态
    void MergeStates(const std::vector<NodeState>& remote_states);
};

class PhiAccrualFailureDetector {
    // 维护心跳到达时间分布
    SlidingWindow<Duration> heartbeat_history_;
    
    // 计算 phi 值
    double ComputePhi(const Timestamp& last_heartbeat);
    
    bool IsSuspected(NodeID node) {
        return ComputePhi(last_heartbeat_[node]) > threshold_;
    }
};
```

---

### 7. 监控与可观测性

**已实现:**
- ✅ 基础统计接口

**缺失/待完善:**
- 🔲 **Metrics 导出**
  - Prometheus 格式支持
  - Grafana  Dashboard
- 🔲 **分布式追踪**
  - OpenTelemetry/Jaeger 集成
  - 事务全链路追踪
- 🔲 **日志聚合**
  - 结构化日志 (JSON)
  - 日志级别动态调整
- 🔲 **告警机制**
  - 基于规则的告警
  - 告警抑制与聚合

**实现建议:**
```cpp
// Prometheus 指标
class MetricsRegistry {
    Counter partition_reads_total_;
    Counter partition_writes_total_;
    Histogram txn_latency_seconds_;
    Gauge active_transactions_;
    
    void ExportPrometheus(std::ostream& out);
};

// 分布式追踪
class TxnTracer {
    Span* StartSpan(const std::string& name, TxnID txn_id);
    void AddEvent(Span* span, const std::string& event);
    void FinishSpan(Span* span);
};
```

---

### 8. 配置管理

**已实现:**
- ✅ 基础配置结构体

**缺失/待完善:**
- 🔲 **配置中心**
  - 集中式配置管理
  - 配置版本控制
- 🔲 **动态配置更新**
  - 无需重启生效
  - 配置热加载
- 🔲 **配置校验**
  -  schema 校验
  - 依赖关系检查

---

## 实施路线图

### Phase 1: 核心功能完善 (2-3周)
1. **解决 protobuf 版本问题**
   - 统一开发/测试环境 protobuf 版本
   - 重新生成 gRPC 代码

2. **完整 gRPC 实现**
   - StorageService gRPC server
   - MetaService gRPC server
   - DTxRpcClient 完整实现

3. **真实 Raft 集成**
   - 集成 braft 或 etcd-raft
   - 实现状态机复制
   - 多节点集群支持

### Phase 2: 数据迁移与一致性 (2周)
1. **数据迁移执行器**
   - 流式数据传输
   - 双写一致性保证
   - 迁移校验

2. **服务发现增强**
   - Gossip 协议
   - Phi Accrual 故障检测

### Phase 3: 生产就绪 (1-2周)
1. **监控指标**
   - Prometheus 导出
   - 基础 Dashboard

2. **配置管理**
   - 配置中心
   - 热更新

3. **混沌测试**
   - 网络分区测试
   - 节点故障测试
   - 数据一致性验证

---

## 关键决策点

### 1. Raft 实现选择
| 选项 | 优点 | 缺点 | 推荐 |
|-----|------|------|------|
| **braft** | 成熟、百度开源、C++ | 依赖 brpc | ✅ 推荐 |
| **etcd-raft** | 广泛测试 | Go 实现，需 cgo | |
| **自研** | 完全可控 | 工作量大、风险高 | ❌ 不推荐 |

### 2. 时钟同步方案
| 选项 | 精度 | 复杂度 | 推荐 |
|-----|------|--------|------|
| **HLC** | 毫秒级 | 中 | ✅ 推荐 |
| **TSO** | 微秒级 | 高 (需部署 PD) | |
| **NTP** | 毫秒级 | 低 (依赖外部) | ❌ 不推荐 |

### 3. 通信框架
| 选项 | 性能 | 易用性 | 推荐 |
|-----|------|--------|------|
| **gRPC** | 高 | 高 | ✅ 当前选择 |
| **brpc** | 极高 | 中 | 可与 braft 配合 |
| **seastar** | 极高 | 低 | 学习曲线陡峭 |

---

## 当前阻塞问题

### 🔴 Critical
1. **protobuf 版本冲突** - 阻止 gRPC 代码编译
2. **MemoryRaft 非生产级** - 元数据可能丢失

### 🟡 High
3. **StorageClient stub** - 无法实际跨节点通信
4. **缺少数据迁移实现** - 无法做负载均衡

### 🟢 Medium
5. 监控指标缺失
6. 配置管理不完善
