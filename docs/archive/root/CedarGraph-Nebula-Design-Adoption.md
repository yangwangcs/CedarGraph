# CedarGraph 采用 NebulaGraph 设计的详细分析

## 概述

本文深入分析 CedarGraph 应该/不应该采用 NebulaGraph 设计的具体部分，并提供详细的实现建议。

---

## 1. 存算分离（Query Engine 层）

### 1.1 为什么当前优先级低？

```
性能瓶颈分析:
┌─────────────────────────────────────────────────────────┐
│  CedarGraph 当前架构                                    │
│                                                          │
│  Client ──► StorageNode ──► 结果                       │
│                │                                         │
│                ├──► 查询解析 (Cypher)                   │
│                ├──► 执行计划生成                        │
│                ├──► 本地数据读取 (VSL - O(log N))       │
│                ├──► 事务协调 (DTx)                      │
│                └──► 结果返回                            │
│                                                          │
│  瓶颈在哪里?                                             │
│  ├── 如果是事务吞吐 ──► 需要优化 DTx/2PC               │
│  ├── 如果是查询延迟 ──► 需要优化 VSL/缓存              │
│  └── 如果是复杂查询 ──► 才需要存算分离                 │
└─────────────────────────────────────────────────────────┘
```

**当前瓶颈分析**:
| 指标 | 当前值 | 目标值 | 瓶颈 |
|------|--------|--------|------|
| 单点事务延迟 | ~0.5ms | <1ms | ✅ 已满足 |
| 分布式事务延迟 | ~15ms | <20ms | ✅ 已满足 |
| 简单查询延迟 | ~2ms | <5ms | ✅ 已满足 |
| 复杂图遍历 | ~500ms | ? | ⚠️ 可能需要优化 |

**结论**: 当前架构在事务和简单查询上性能良好，存算分离的紧迫性不高。

### 1.2 什么时候需要存算分离？

```
触发条件:
┌────────────────────────────────────────────────────────┐
│  1. 查询复杂度超过单机处理能力                          │
│     • 需要跨大量 Partition 的图遍历                    │
│     • 需要分布式 Join                                  │
│                                                          │
│  2. 查询负载与写入负载冲突                              │
│     • 写入吞吐受查询影响                               │
│     • 需要独立的查询扩展能力                           │
│                                                          │
│  3. 需要专门的查询优化                                 │
│     • 复杂的查询计划缓存                              │
│     • 专门的查询执行引擎                              │
└────────────────────────────────────────────────────────┘
```

### 1.3 未来实现方案（预留设计）

```cpp
// include/cedar/query/query_engine.h

namespace cedar {
namespace query {

/**
 * @brief Query Engine - 可选的查询层
 * 
 * 当需要时可以引入，与 StorageD 分离
 */
class QueryEngine {
public:
    Status Initialize(const QueryEngineConfig& config);
    
    // 执行 Cypher 查询
    ResultSet Execute(const std::string& cypher, const QueryOptions& options);
    
    // 执行时序查询
    ResultSet ExecuteTemporal(const TemporalQuery& query);
    
private:
    // 查询解析
    std::unique_ptr<CypherParser> parser_;
    
    // 执行计划生成器
    std::unique_ptr<ExecutionPlanGenerator> plan_generator_;
    
    // 到 StorageD 的连接池
    std::unordered_map<dtx::PartitionID, 
        std::unique_ptr<StorageService::Stub>> storage_clients_;
    
    // 查询缓存
    std::unique_ptr<QueryCache> cache_;
};

/**
 * @brief 分布式执行计划
 */
class DistributedExecutionPlan {
public:
    // 将计划拆分为子计划，每个子计划在特定 Partition 执行
    std::vector<SubPlan> Partition(const dtx::ClusterTopology& topology);
    
    // 聚合子计划结果
    ResultSet Aggregate(const std::vector<ResultSet>& partial_results);
};

} // namespace query
} // namespace cedar
```

---

## 2. MetaD（元数据服务）

### 2.1 为什么优先级高？

```
没有元数据服务的问题:
┌─────────────────────────────────────────────────────────┐
│  场景: 客户端要写入 Key = (entity=100, ts=now)         │
│                                                          │
│  Client ──► 怎么知道 Partition 0 在哪个节点?            │
│                                                          │
│  选项 1: 客户端直连所有节点询问                          │
│          - N 次网络往返                                │
│          - 客户端需要维护拓扑                            │
│                                                          │
│  选项 2: 固定的静态配置                                  │
│          - 无法动态扩缩容                                │
│          - 节点故障无法自动切换                          │
│                                                          │
│  选项 3: 元数据服务 (✅ 推荐)                            │
│          - 一次查询获取 Part 位置                        │
│          - 支持动态变更                                  │
│          - 客户端缓存 + Watch 机制                       │
└─────────────────────────────────────────────────────────┘
```

### 2.2 MetaD 详细设计

```cpp
// include/cedar/dtx/meta_service.h

namespace cedar {
namespace dtx {

/**
 * @brief 元数据服务 - 3-5 节点 Raft 集群
 * 
 * 职责:
 * 1. Schema 管理 (Space, Tag, Edge, Property)
 * 2. 分区到节点的映射 (Part Location)
 * 3. 节点心跳与发现
 * 4. 负载均衡决策
 * 5. 全局事务ID / HLC 管理
 */
class MetadataService {
public:
    // ===== 生命周期 =====
    
    // 初始化，加入 Raft 组
    Status Initialize(const MetaServiceConfig& config);
    
    // 关闭服务
    Status Shutdown();
    
    // ===== Schema 管理 =====
    
    // 创建 Space（类似数据库）
    Status CreateSpace(const SpaceDef& space);
    
    // 删除 Space
    Status DropSpace(const std::string& space_name);
    
    // 获取 Space 定义
    Result<SpaceDef> GetSpace(const std::string& space_name);
    
    // 创建 Tag（点类型）
    Status CreateTag(const std::string& space_name, const TagDef& tag);
    
    // 创建 Edge（边类型）
    Status CreateEdge(const std::string& space_name, const EdgeDef& edge);
    
    // ===== 分区管理（核心）=====
    
    /**
     * @brief 获取分区的当前分配
     * 
     * 这是客户端和协调器最常用的接口
     */
    Result<PartitionAssignment> GetPartitionAssignment(
        const std::string& space_name,
        PartitionID partition_id);
    
    /**
     * @brief 批量获取分区分配
     */
    Result<std::vector<PartitionAssignment>> GetPartitionAssignments(
        const std::string& space_name,
        const std::vector<PartitionID>& partitions);
    
    /**
     * @brief 获取 Space 的完整分区表
     */
    Result<SpacePartitionMap> GetSpacePartitionMap(
        const std::string& space_name);
    
    // ===== 节点管理 =====
    
    // 节点注册（新节点加入集群）
    Status RegisterNode(const NodeInfo& node_info);
    
    // 节点心跳（由 StorageD 定期调用）
    Status Heartbeat(NodeID node_id, const NodeStatus& status);
    
    // 获取所有存活节点
    std::vector<NodeInfo> GetAliveNodes();
    
    // 获取节点信息
    Result<NodeInfo> GetNode(NodeID node_id);
    
    // ===== 负载均衡 =====
    
    // 触发负载均衡检查
    Status TriggerLoadBalance();
    
    // 获取负载均衡任务状态
    Result<LoadBalanceStatus> GetLoadBalanceStatus();
    
    // ===== 事务支持 =====
    
    // 分配全局事务ID
    TxnID AllocateTxnID();
    
    // 获取当前 HLC（混合逻辑时钟）
    HybridLogicalClock GetCurrentHLC();
    
    // ===== 订阅/通知 =====
    
    // 订阅分区表变更（客户端缓存失效）
    void WatchPartitionMap(
        const std::string& space_name,
        std::function<void(const PartitionMapChange&)> callback);
    
    // 订阅节点变更
    void WatchNodes(
        std::function<void(const NodeChange&)> callback);

private:
    // Raft 状态机实现
    class MetadataStateMachine : public raft::StateMachine {
    public:
        void Apply(const LogEntry& entry) override;
        Snapshot CreateSnapshot() override;
        void RestoreSnapshot(const Snapshot& snapshot) override;
        
    private:
        // 内存中的元数据状态
        std::unordered_map<std::string, SpaceMeta> spaces_;
        std::unordered_map<NodeID, NodeMeta> nodes_;
        std::unordered_map<std::string, SpacePartitionMap> partition_maps_;
    };
    
    std::unique_ptr<raft::RaftNode> raft_node_;
    MetadataStateMachine state_machine_;
    
    // 配置
    MetaServiceConfig config_;
};

/**
 * @brief 分区分配信息
 */
struct PartitionAssignment {
    PartitionID partition_id;
    NodeID leader_node;                    // 当前 Leader
    std::vector<NodeID> follower_nodes;    // Follower 列表
    uint64_t version;                      // 版本号（用于缓存验证）
    std::chrono::system_clock::time_point last_updated;
    
    // 辅助方法
    bool IsLeaderOn(NodeID node_id) const {
        return leader_node == node_id;
    }
    
    bool IsReplicaOn(NodeID node_id) const {
        if (leader_node == node_id) return true;
        for (auto& nid : follower_nodes) {
            if (nid == node_id) return true;
        }
        return false;
    }
};

/**
 * @brief Space 的分区映射
 */
struct SpacePartitionMap {
    std::string space_name;
    uint32_t num_partitions;
    uint32_t replication_factor;
    
    // partition_id -> assignment
    std::unordered_map<PartitionID, PartitionAssignment> assignments;
    
    uint64_t version;  // 整体版本号
    
    // 获取 Key 对应的分区
    PartitionID GetPartitionForKey(const CedarKey& key) const;
    
    // 获取分区的 Leader 节点
    NodeID GetLeader(PartitionID pid) const;
};

/**
 * @brief 节点信息
 */
struct NodeInfo {
    NodeID node_id;
    std::string address;           // gRPC 地址，如 "10.0.0.1:50051"
    std::string data_path;         // 数据目录
    
    // 资源信息
    uint32_t num_cpu_cores{0};
    uint64_t total_memory_bytes{0};
    uint64_t total_disk_bytes{0};
    
    // 角色
    std::vector<PartitionID> leader_partitions;
    std::vector<PartitionID> follower_partitions;
};

} // namespace dtx
} // namespace cedar
```

### 2.3 MetaD 的 Raft 状态机

```
Raft Log Entry Types:
┌─────────────────────────────────────────────────────────┐
│  enum class MetaLogType : uint8_t {                     │
│      kCreateSpace = 1,                                  │
│      kDropSpace,                                        │
│      kCreateTag,                                        │
│      kCreateEdge,                                       │
│      kRegisterNode,                                     │
│      kUpdateNodeStatus,                                 │
│      kUpdatePartitionLeader,    // Leader 变更          │
│      kMigratePartition,          // 分区迁移            │
│      kUpdateSchema,                                       │
│  };                                                      │
└─────────────────────────────────────────────────────────┘

状态机应用流程:
1. Raft Leader 接收请求
2. 写入本地 Raft Log
3. 复制到 Follower
4. 多数派确认后，Apply 到状态机
5. 返回客户端成功
```

### 2.4 客户端与 MetaD 的交互

```cpp
// Client 使用 MetaD 的示例

class CedarClient {
public:
    Status Connect(const std::vector<std::string>& meta_addresses) {
        // 1. 连接到任意一个 MetaD 节点
        for (auto& addr : meta_addresses) {
            meta_client_ = MetaServiceClient::Create(addr);
            if (meta_client_->Ping().ok()) break;
        }
        
        // 2. 获取 Space 的分区表
        auto result = meta_client_->GetSpacePartitionMap("my_graph");
        if (!result.ok()) return result.status();
        
        partition_map_ = result.value();
        
        // 3. 订阅变更（Watch）
        meta_client_->WatchPartitionMap("my_graph", [this](const auto& change) {
            // 缓存失效，重新获取
            RefreshPartitionMap();
        });
        
        return Status::OK();
    }
    
    Status Put(const CedarKey& key, const Value& value) {
        // 1. 计算分区
        PartitionID pid = partition_map_.GetPartitionForKey(key);
        
        // 2. 获取 Leader 节点
        NodeID leader = partition_map_.GetLeader(pid);
        
        // 3. 发送到 Leader
        auto stub = GetStorageStub(leader);
        return stub->Put(key, value);
    }
    
private:
    std::unique_ptr<MetaServiceClient> meta_client_;
    SpacePartitionMap partition_map_;
    std::unordered_map<NodeID, std::unique_ptr<StorageService::Stub>> storage_stubs_;
};
```

---

## 3. Part 映射管理

### 3.1 为什么需要专门的 Part 映射管理？

```
场景 1: 节点故障
┌─────────────────────────────────────────────────────────┐
│  Before:                                                │
│    Partition 0: Leader=Node0, Followers=[Node1, Node2]  │
│                                                          │
│  Node0 故障                                              │
│                                                          │
│  After (Raft 自动处理):                                  │
│    Partition 0: Leader=Node1, Followers=[Node2]         │
│    (Node0 恢复后会变成 Follower)                        │
│                                                          │
│  问题: 客户端缓存的 "P0 Leader=Node0" 失效了！          │
│                                                          │
│  解决: MetaD Watch 机制通知客户端更新                    │
└─────────────────────────────────────────────────────────┘

场景 2: 分区迁移（负载均衡）
┌─────────────────────────────────────────────────────────┐
│  Before:                                                │
│    Node0: P0, P1, P2 (负载高)                           │
│    Node1: P3, P4, P5 (负载低)                           │
│                                                          │
│  负载均衡决策: 将 P2 从 Node0 迁移到 Node1               │
│                                                          │
│  After:                                                 │
│    Node0: P0, P1                                        │
│    Node1: P2, P3, P4, P5                                │
│                                                          │
│  需要: MetaD 协调迁移，通知所有客户端                    │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Part 映射的数据结构

```cpp
/**
 * @brief 分区位置缓存（客户端使用）
 */
class PartitionLocationCache {
public:
    // 获取 Key 的 Leader 节点（带缓存）
    Result<NodeID> GetLeaderForKey(const CedarKey& key) {
        PartitionID pid = GetPartitionID(key);
        
        // 检查缓存
        auto it = cache_.find(pid);
        if (it != cache_.end() && !it->second.IsExpired()) {
            return it->second.leader_node;
        }
        
        // 缓存未命中或过期，查询 MetaD
        auto result = meta_client_->GetPartitionAssignment(pid);
        if (!result.ok()) return result.status();
        
        // 更新缓存
        cache_[pid] = CachedAssignment{
            .assignment = result.value(),
            .cached_at = std::chrono::steady_clock::now()
        };
        
        return result.value().leader_node;
    }
    
    // 处理 Leader 变更（由 MetaD Watch 触发）
    void OnLeaderChange(PartitionID pid, NodeID new_leader) {
        auto it = cache_.find(pid);
        if (it != cache_.end()) {
            it->second.assignment.leader_node = new_leader;
            it->second.cached_at = std::chrono::steady_clock::now();
        }
    }
    
    // 处理分区迁移（清除缓存）
    void OnPartitionMigration(PartitionID pid) {
        cache_.erase(pid);
    }
    
private:
    struct CachedAssignment {
        PartitionAssignment assignment;
        std::chrono::steady_clock::time_point cached_at;
        
        bool IsExpired() const {
            auto now = std::chrono::steady_clock::now();
            return std::chrono::duration_cast<std::chrono::seconds>(
                now - cached_at).count() > 60;  // 60秒过期
        }
    };
    
    std::unordered_map<PartitionID, CachedAssignment> cache_;
    std::shared_mutex cache_mutex_;
    std::unique_ptr<MetaServiceClient> meta_client_;
};
```

### 3.3 Part 映射的版本控制

```cpp
/**
 * @brief 分区映射版本控制
 * 
 * 防止客户端使用过期的分区信息
 */
struct PartitionMapVersion {
    uint64_t global_version;      // 全局版本号（每次变更+1）
    uint64_t partition_version;   // 分区级别版本号
    
    bool operator>=(const PartitionMapVersion& other) const {
        return global_version >= other.global_version;
    }
};

// 使用示例
Status StorageService::Put(const PutRequest& request) {
    // 检查客户端的分区表版本
    if (request.partition_map_version < current_version_) {
        return Status::VersionExpired("Partition map expired, please refresh");
    }
    
    // 检查本节点是否还是 Leader
    if (!raft_node_->IsLeader()) {
        return Status::NotLeader("Not leader anymore", current_leader_);
    }
    
    // 执行写入
    return DoPut(request);
}
```

---

## 4. 负载均衡

### 4.1 负载均衡触发条件

```cpp
/**
 * @brief 负载均衡决策器
 */
class LoadBalancer {
public:
    // 检查是否需要负载均衡
    bool NeedsRebalance(const ClusterLoadReport& report) {
        // 条件 1: 节点间数据量差异超过阈值
        double max_data = GetMaxDataSize(report);
        double min_data = GetMinDataSize(report);
        if (max_data / min_data > kDataImbalanceThreshold) {
            return true;
        }
        
        // 条件 2: Leader 分布不均
        auto leader_distribution = GetLeaderDistribution(report);
        if (GetStandardDeviation(leader_distribution) > kLeaderImbalanceThreshold) {
            return true;
        }
        
        // 条件 3: 节点故障导致副本数不足
        for (auto& partition : report.partitions) {
            if (partition.replicas.size() < replication_factor_) {
                return true;
            }
        }
        
        // 条件 4: 热点 Partition
        for (auto& partition : report.partitions) {
            if (partition.qps > kHotPartitionThreshold) {
                return true;
            }
        }
        
        return false;
    }
    
private:
    static constexpr double kDataImbalanceThreshold = 1.5;  // 最大/最小比 > 1.5
    static constexpr double kLeaderImbalanceThreshold = 5.0;  // 标准差 > 5
    static constexpr uint32_t kHotPartitionThreshold = 10000;  // QPS > 10K
};
```

### 4.2 负载均衡策略

```cpp
/**
 * @brief 负载均衡策略
 */
enum class BalanceStrategy : uint8_t {
    kLeaderBalance = 1,    // 均衡 Leader 分布
    kDataBalance,          // 均衡数据量
    kQpsBalance,           // 均衡 QPS
    kAuto,                 // 自动选择
};

class LoadBalancePlanner {
public:
    // 生成负载均衡计划
    BalancePlan GeneratePlan(const ClusterLoadReport& report, 
                            BalanceStrategy strategy) {
        switch (strategy) {
            case BalanceStrategy::kLeaderBalance:
                return PlanLeaderBalance(report);
            case BalanceStrategy::kDataBalance:
                return PlanDataBalance(report);
            case BalanceStrategy::kQpsBalance:
                return PlanQpsBalance(report);
            case BalanceStrategy::kAuto:
                return PlanAuto(report);
        }
    }
    
private:
    // Leader 均衡策略
    BalancePlan PlanLeaderBalance(const ClusterLoadReport& report) {
        BalancePlan plan;
        
        // 计算平均 Leader 数
        size_t avg_leaders = report.total_partitions / report.nodes.size();
        
        // 找到 Leader 过多的节点
        for (auto& node : report.nodes) {
            if (node.leader_count > avg_leaders + 1) {
                // 转移部分 Leader 到其他节点
                size_t to_transfer = node.leader_count - avg_leaders;
                for (size_t i = 0; i < to_transfer; i++) {
                    auto target = FindNodeWithFewestLeaders(report);
                    plan.AddTask(TransferLeaderTask{
                        .partition = node.leader_partitions[i],
                        .from_node = node.node_id,
                        .to_node = target.node_id
                    });
                }
            }
        }
        
        return plan;
    }
    
    // 数据均衡策略
    BalancePlan PlanDataBalance(const ClusterLoadReport& report) {
        // 类似 Leader 均衡，但基于数据量
    }
};
```

### 4.3 分区迁移流程

```
分区迁移步骤 (Partition Migration):
┌─────────────────────────────────────────────────────────┐
│  1. MetaD 决策: 将 P0 从 Node0 迁移到 Node1              │
│                                                          │
│  2. 添加副本                                             │
│     MetaD ──► Node1: 创建 P0 副本（Follower）            │
│     Node1 ──► Node0: 同步数据（Raft Join）               │
│                                                          │
│  3. 等待同步完成                                         │
│     检查 Node1 的 P0 数据与 Node0 一致                   │
│                                                          │
│  4. Leader 切换（如果需要）                              │
│     如果 P0 Leader 在 Node0，切换到其他节点              │
│                                                          │
│  5. 移除旧副本                                           │
│     MetaD ──► Node0: 移除 P0 副本                        │
│     Node0: 删除本地 P0 数据                              │
│                                                          │
│  6. 更新分区表                                           │
│     MetaD 更新 P0 的副本列表                             │
│     通知所有客户端                                       │
└─────────────────────────────────────────────────────────┘
```

---

## 5. 实施路线图

### Phase 1: MetaD 基础（高优先级）

```
Week 1-2:
├── MetaD Raft 框架集成
├── 节点注册/心跳
└── 基础分区表管理

Week 3-4:
├── 客户端 MetaD 客户端
├── 分区表缓存 + Watch
└── 集成到 StorageD
```

### Phase 2: Part 映射优化（高优先级）

```
Week 5-6:
├── 版本控制机制
├── Leader 变更自动通知
└── 分区迁移基础
```

### Phase 3: 负载均衡（中优先级）

```
Week 7-8:
├── 负载收集/报告
├── 均衡决策算法
└── 分区迁移实现
```

### Phase 4: 存算分离（低优先级，未来）

```
Future:
├── Query Engine 设计
├── 分布式执行计划
└── 性能对比测试
```

---

## 6. 总结

| 设计 | 优先级 | 原因 | 实施复杂度 |
|------|--------|------|-----------|
| **MetaD** | 🔴 高 | 分布式基础，没有它集群无法正确路由 | 中 |
| **Part 映射** | 🔴 高 | 与 MetaD 一起实现，客户端必需 | 中 |
| **负载均衡** | 🟡 中 | 运维需求，初期可以手动处理 | 高 |
| **存算分离** | 🟢 低 | 性能优化，当前不是瓶颈 | 很高 |

**关键决策**:
1. **立即实施 MetaD**: 这是分布式化的基础设施
2. **借鉴 Nebula 的 MetaD 设计**: 成熟的分区管理方案
3. **暂不实施存算分离**: 当前架构已满足需求
4. **保持 DTx 强事务**: 这是 Cedar 的核心竞争力
