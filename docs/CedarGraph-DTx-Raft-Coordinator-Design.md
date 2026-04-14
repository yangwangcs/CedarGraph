# CedarGraph-DTx: Raft、协调器与元服务设计

## 1. 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CedarGraph-DTx 集群                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    元服务集群 (Metadata Service)                     │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │   │
│  │  │MetaNode0│  │MetaNode1│  │MetaNode2│  │MetaNode3│  │MetaNode4│   │   │
│  │  │(Leader) │◄►│(Follower│◄►│(Follower│◄►│(Follower│◄►│(Follower│   │   │
│  │  │         │  │   )     │  │   )     │  │   )     │  │   )     │   │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘   │   │
│  │       └─────────────┴─────────────┴─────────────┴─────────────┘      │   │
│  │                           Raft Consensus                              │   │
│  │                                                                       │   │
│  │  职责:                                                                │   │
│  │  • 分区到节点的映射 (Partition Map)                                    │   │
│  │  • 节点发现与心跳管理                                                  │   │
│  │  • 集群拓扑变更                                                        │   │
│  │  • 全局事务ID分配                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    存储节点集群 (Storage Nodes)                       │   │
│  │                                                                      │   │
│  │   Node 0              Node 1              Node 2                     │   │
│  │  ┌─────────┐         ┌─────────┐         ┌─────────┐                │   │
│  │  │Coordina-│         │Coordina-│         │Coordina-│   ...          │   │
│  │  │ tor    │         │ tor    │         │ tor    │                  │   │
│  │  │ (P0主) │         │ (P1主) │         │ (P2主) │                  │   │
│  │  ├─────────┤         ├─────────┤         ├─────────┤                │   │
│  │  │  P0     │◄───────►│  P1     │◄───────►│  P2     │                │   │
│  │  │ (Raft)  │         │ (Raft)  │         │ (Raft)  │                │   │
│  │  │ 主/从   │         │ 主/从   │         │ 主/从   │                │   │
│  │  ├─────────┤         ├─────────┤         ├─────────┤                │   │
│  │  │  P3     │◄───────►│  P4     │◄───────►│  P5     │                │   │
│  │  │ (Raft)  │         │ (Raft)  │         │ (Raft)  │                │   │
│  │  │ 从/主   │         │ 从/主   │         │ 从/主   │                │   │
│  │  └─────────┘         └─────────┘         └─────────┘                │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 2. 元服务 (Metadata Service)

### 2.1 职责

元服务是集群的"大脑"，但不参与实际数据存储：

1. **分区表 (Partition Map)**: 记录每个 Partition 的主副本分布
2. **节点发现**: 新节点加入、节点下线检测
3. **全局配置**: 集群级别参数管理
4. **事务ID分配**: 全局唯一 TxnID 分配（可选，可用 HLC 替代）

### 2.2 实现设计

```cpp
// include/cedar/dtx/metadata_service.h

namespace cedar {
namespace dtx {

/**
 * @brief 分区分配信息
 */
struct PartitionAssignment {
    PartitionID partition_id;
    NodeID primary_node;                    // 主节点
    std::vector<NodeID> replica_nodes;      // 副本节点列表
    uint64_t version;                       // 配置版本号（用于乐观并发）
    
    // 检查节点是否拥有此分区
    bool IsOnNode(NodeID node_id) const {
        if (primary_node == node_id) return true;
        for (auto& nid : replica_nodes) {
            if (nid == node_id) return true;
        }
        return false;
    }
};

/**
 * @brief 集群拓扑信息
 */
struct ClusterTopology {
    uint64_t version{0};                                    // 拓扑版本
    std::unordered_map<NodeID, NodeInfo> nodes;            // 所有节点
    std::unordered_map<PartitionID, PartitionAssignment> partitions;
    std::chrono::system_clock::time_point updated_at;
};

/**
 * @brief 元服务节点
 */
class MetadataService {
public:
    // 初始化（Raft 组）
    Status Initialize(const MetadataConfig& config);
    
    // ===== 分区管理 =====
    
    // 获取分区分配
    Result<PartitionAssignment> GetPartitionAssignment(PartitionID pid);
    
    // 获取节点的所有分区
    std::vector<PartitionID> GetPartitionsOnNode(NodeID node_id);
    
    // 更新分区分配（需要 Raft 共识）
    Status UpdatePartitionAssignment(const PartitionAssignment& assign);
    
    // ===== 节点管理 =====
    
    // 节点注册
    Status RegisterNode(const NodeInfo& info);
    
    // 节点心跳
    Status Heartbeat(NodeID node_id);
    
    // 获取存活节点列表
    std::vector<NodeID> GetAliveNodes();
    
    // 标记节点失效（由 Raft 触发）
    Status MarkNodeFailed(NodeID node_id);
    
    // ===== 拓扑管理 =====
    
    // 获取完整拓扑
    ClusterTopology GetTopology();
    
    // 订阅拓扑变更
    void SubscribeTopologyChange(std::function<void(const ClusterTopology&)> callback);
    
    // ===== 事务ID分配 =====
    
    // 分配全局唯一 TxnID
    TxnID AllocateTxnID();
    
private:
    // Raft 状态机
    class MetadataStateMachine : public RaftStateMachine {
    public:
        // 应用日志条目
        void Apply(const LogEntry& entry) override;
        
        // 创建快照
        Snapshot CreateSnapshot() override;
        
        // 恢复快照
        void RestoreSnapshot(const Snapshot& snapshot) override;
    };
    
    std::unique_ptr<RaftNode> raft_node_;  // Raft 节点
    MetadataStateMachine state_machine_;
    
    // 内存缓存（Raft 状态机的镜像）
    std::shared_mutex topo_mutex_;
    ClusterTopology cached_topology_;
};

}  // namespace dtx
}  // namespace cedar
```

### 2.3 为什么元服务需要 Raft？

**场景**: 3 个元服务节点，同时收到分区迁移请求

```
Node 0 提议: P0 从 N0 迁移到 N1
Node 1 提议: P0 从 N0 迁移到 N2

Raft 保证:
1. 只有一个提议会被提交
2. 所有节点看到相同的顺序
3. 已提交的提议不会丢失
```

**如果不使用 Raft**:
- 脑裂：两个节点认为自己是主
- 不一致：不同客户端看到不同分区分布
- 数据丢失：分区迁移决策冲突

## 3. 分区级别的 Raft

### 3.1 每个 Partition 是一个 Raft Group

```
Partition 0 Raft Group:
┌─────────────────────────────────────────┐
│              Raft Group 0               │
├─────────────────────────────────────────┤
│                                         │
│   Node 0 (Leader) ◄──────────────────┐  │
│      │                               │  │
│      │ Log Replication               │  │
│      ▼                               │  │
│   Node 1 (Follower) ────────────────┘  │
│      │                                 │
│      │ Log Replication                 │
│      ▼                                 │
│   Node 2 (Follower)                    │
│                                         │
│  Log Entry: [Write Batch 1] [Write Batch 2] ...
│                                         │
└─────────────────────────────────────────┘
```

### 3.2 实现设计

```cpp
// include/cedar/dtx/partition_raft.h

namespace cedar {
namespace dtx {

/**
 * @brief 分区日志条目类型
 */
enum class PartitionLogType : uint8_t {
    kWriteBatch = 1,        // 写批次（数据变更）
    kTransactionCommit,     // 事务提交记录
    kMetadataUpdate,        // 分区元数据更新
    kCheckpoint,            // 检查点
};

/**
 * @brief 分区日志条目
 */
struct PartitionLogEntry {
    uint64_t term;           // Raft Term
    uint64_t index;          // Log Index
    PartitionLogType type;
    std::string data;        // 序列化数据
    uint64_t timestamp;      // 时间戳
};

/**
 * @brief 分区 Raft 状态机
 * 
 * 负责实际的数据存储和复制
 */
class PartitionRaftStateMachine : public RaftStateMachine {
public:
    explicit PartitionRaftStateMachine(PartitionID pid);
    
    // 应用日志条目（由 Raft 库调用）
    void Apply(const LogEntry& entry) override {
        switch (entry.type) {
            case PartitionLogType::kWriteBatch:
                ApplyWriteBatch(entry.data);
                break;
            case PartitionLogType::kTransactionCommit:
                ApplyTransactionCommit(entry.data);
                break;
            case PartitionLogType::kMetadataUpdate:
                ApplyMetadataUpdate(entry.data);
                break;
            case PartitionLogType::kCheckpoint:
                ApplyCheckpoint(entry.data);
                break;
        }
    }
    
    // 创建快照（用于日志压缩）
    Snapshot CreateSnapshot() override {
        // 1. 暂停写操作
        // 2. 序列化当前状态（LSM-Tree SST 文件列表）
        // 3. 返回快照
    }
    
private:
    PartitionID partition_id_;
    std::unique_ptr<LsmEngine> storage_;  // 实际存储引擎
};

/**
 * @brief 分区 Raft 节点
 * 
 * 每个 Partition 在节点上有一个对应的 Raft 实例
 */
class PartitionRaftNode {
public:
    // 初始化
    Status Initialize(const RaftConfig& config,
                      PartitionID pid,
                      NodeID current_node,
                      const std::vector<NodeID>& peers);
    
    // 提议写操作（只有 Leader 能成功）
    Result<uint64_t> ProposeWrite(const WriteBatch& batch);
    
    // 提议事务提交
    Result<uint64_t> ProposeTransactionCommit(TxnID txn_id);
    
    // 读操作（Leader 或 Follower 都可）
    Result<Value> Read(const CedarKey& key);
    
    // 是否是 Leader
    bool IsLeader() const;
    
    // 获取当前 Leader
    NodeID GetLeader() const;
    
    // 处理 Raft 消息（来自其他节点）
    void OnRaftMessage(const RaftMessage& msg);
    
private:
    PartitionID partition_id_;
    std::unique_ptr<RaftImpl> raft_impl_;  // 底层 Raft 实现
    PartitionRaftStateMachine state_machine_;
};

/**
 * @brief 节点上的 Partition Raft 管理器
 */
class NodePartitionManager {
public:
    // 启动时加载所有本节点的 Partition
    Status LoadPartitions(const std::vector<PartitionID>& partitions);
    
    // 获取指定 Partition 的 Raft 节点
    PartitionRaftNode* GetPartitionRaft(PartitionID pid);
    
    // 创建新的 Partition（作为副本）
    Status CreatePartitionReplica(PartitionID pid, NodeID leader_node);
    
    // 删除 Partition 副本
    Status RemovePartitionReplica(PartitionID pid);
    
private:
    std::unordered_map<PartitionID, std::unique_ptr<PartitionRaftNode>> partitions_;
};

}  // namespace dtx
}  // namespace cedar
```

### 3.3 Raft 与 2PC 的关系

```
┌─────────────────────────────────────────────────────────────┐
│              跨分区事务 (跨 Raft Group)                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Client                                                     │
│    │                                                        │
│    ▼                                                        │
│  Coordinator (任意节点)                                      │
│    │                                                        │
│    │  2PC Phase 1: Prepare                                   │
│    ├───────────────────────────────────────────┐             │
│    │                                           │             │
│    ▼                                           ▼             │
│  Partition 0 Raft                           Partition 1 Raft │
│  ┌─────────────┐                            ┌─────────────┐ │
│  │ Leader (N0) │                            │ Leader (N1) │ │
│  │             │                            │             │ │
│  │ Log:Prepare │◄── Raft 复制 ──►│ Log:Prepare │ │
│  │ Vote: YES   │                            │ Vote: YES   │ │
│  └─────────────┘                            └─────────────┘ │
│         │                                           │        │
│         └─────────────────┬─────────────────────────┘        │
│                           ▼                                  │
│  2PC Phase 2: Commit (All votes YES)                         │
│                           │                                  │
│    ┌──────────────────────┴──────────────────────┐           │
│    ▼                                              ▼           │
│  Log:Commit                                  Log:Commit      │
│                                                             │
└─────────────────────────────────────────────────────────────┘

关键区别:
• Raft: 单个 Partition 内部的共识（复制）
• 2PC: 多个 Partition 之间的事务原子性
```

## 4. 事务协调器 (Transaction Coordinator)

### 4.1 协调器的类型

```cpp
/**
 * @brief 协调器类型
 */
enum class CoordinatorType : uint8_t {
    // 每个节点都有一个协调器
    kNodeLocal = 1,      // 节点本地协调器
    
    // 特定 Partition 的协调器
    kPartitionPinned,    // 绑定到 Partition 的协调器（该 Partition 的 Leader）
    
    // 动态选举
    kDynamicLeader,      // 动态选举（最小节点 ID 或负载最低）
};
```

### 4.2 协调器实现

```cpp
// include/cedar/dtx/coordinator.h

namespace cedar {
namespace dtx {

/**
 * @brief 分布式事务协调器
 * 
 * 负责:
 * 1. 接收客户端事务请求
 * 2. 路由到相关 Partition
 * 3. 执行 2PC 协议
 * 4. 处理故障恢复
 */
class TransactionCoordinator {
public:
    Status Initialize(const CoordinatorConfig& config,
                      NodeID node_id,
                      MetadataService* meta_service);
    
    // ===== 事务接口 =====
    
    // 开始事务
    Result<TxnID> BeginTransaction(const DistributedTxnOptions& options);
    
    // 执行读操作
    Result<Value> Read(TxnID txn_id, const CedarKey& key);
    
    // 执行写操作（缓冲）
    Status Write(TxnID txn_id, const CedarKey& key, const Value& value);
    
    // 提交事务
    Result<CommitResult> Commit(TxnID txn_id);
    
    // 回滚事务
    Status Abort(TxnID txn_id);
    
    // ===== 内部方法 =====
    
private:
    // 执行 2PC
    Result<CommitResult> ExecuteTwoPhaseCommit(DistributedTxnContext* ctx);
    
    // Phase 1: Prepare
    Result<std::vector<VoteResult>> Phase1Prepare(DistributedTxnContext* ctx);
    
    // Phase 2: Commit
    Status Phase2Commit(DistributedTxnContext* ctx);
    
    // Phase 2: Abort
    Status Phase2Abort(DistributedTxnContext* ctx);
    
    // 选择参与者的 Leader 节点
    Status DiscoverPartitionLeaders(DistributedTxnContext* ctx);
    
    // 故障恢复
    Status RecoverTransaction(TxnID txn_id);
    
private:
    NodeID node_id_;                                    // 本节点 ID
    MetadataService* meta_service_;                     // 元服务客户端
    
    // 活跃事务表
    std::shared_mutex active_txns_mutex_;
    std::unordered_map<TxnID, std::unique_ptr<DistributedTxnContext>> active_txns_;
    
    // RPC 客户端缓存
    std::unordered_map<NodeID, std::unique_ptr<PartitionService::Stub>> partition_stubs_;
};

}  // namespace dtx
}  // namespace cedar
```

### 4.3 协调器选择策略

```
场景 1: 单分区事务
┌──────────────────────────────────────────┐
│  Client                                  │
│    │                                     │
│    │ 1. Begin Txn                        │
│    ▼                                     │
│  Node 0 (任意节点)                        │
│    │                                     │
│    │ 2. 发现 P0 Leader 在 Node 0        │
│    │    本地执行，无需 RPC               │
│    ▼                                     │
│  Partition 0                             │
│  (本地 Raft)                             │
└──────────────────────────────────────────┘

场景 2: 跨分区事务
┌──────────────────────────────────────────┐
│  Client                                  │
│    │                                     │
│    │ 1. Begin Txn                        │
│    ▼                                     │
│  Node 0 (协调器)                          │
│    │                                     │
│    │ 2. 2PC Prepare ──────┬──────────► P0 (N0)
│    │                      ├──────────► P1 (N1)
│    │                      └──────────► P2 (N2)
│    │                                     │
│    │ 3. 2PC Commit ───────┬──────────► P0
│    │                      ├──────────► P1
│    │                      └──────────► P2
│    ▼                                     │
│  Return Result                           │
└──────────────────────────────────────────┘

协调器选择策略:
• 最小化跨节点通信
• 优先选择参与者最多的节点作为协调器
• 考虑节点负载
```

## 5. 故障恢复

### 5.1 协调器故障

```
场景: 协调器在 2PC 过程中崩溃

Before Crash:
  Coordinator (Node 0)       P0 (Node 1)       P1 (Node 2)
       │                         │                 │
       ├──── Prepare ───────────►│                 │
       │                         │ Prepared        │
       ├──── Prepare ────────────────────────────►│
       │                                           │ Prepared
       │ X (Crash here)                            │

After Recovery:
  1. P0 和 P1 的 Prepared 记录保留（超时机制）
  2. 新协调器选举（或通过元服务发现）
  3. 新协调器查询 P0 和 P1 的状态
  4. 根据投票结果决定 Commit 或 Abort

Decision Matrix:
┌────────────────┬────────────────┬─────────┐
│ P0 State       │ P1 State       │ Action  │
├────────────────┼────────────────┼─────────┤
│ Prepared       │ Prepared       │ Commit  │
│ Prepared       │ Committed      │ Commit  │
│ Committed      │ Prepared       │ Commit  │
│ Abort          │ *              │ Abort   │
│ *              │ Abort          │ Abort   │
│ Unknown        │ Unknown        │ Abort   │
└────────────────┴────────────────┴─────────┘
```

### 5.2 Partition Leader 故障

```
场景: Partition 0 的 Leader (Node 0) 崩溃

Before:
  P0 Raft Group
  ┌─────────┐
  │ N0 (L)  │── Leader
  │ N1 (F)  │── Follower
  │ N2 (F)  │── Follower
  └─────────┘

After (Raft 自动处理):
  1. N1 和 N2 检测到 Leader 失效（心跳超时）
  2. N1 成为 Candidate，发起选举
  3. N1 获得多数票，成为新 Leader
  4. 元服务更新 Partition Map
  5. 客户端重试，路由到新 Leader

After:
  P0 Raft Group
  ┌─────────┐
  │ N0 (X)  │── Dead
  │ N1 (L)  │── New Leader
  │ N2 (F)  │── Follower
  └─────────┘
```

## 6. 代码实现骨架

### 6.1 Raft 接口定义

```cpp
// include/cedar/dtx/raft/raft_interface.h

namespace cedar {
namespace dtx {
namespace raft {

/**
 * @brief Raft 状态机接口
 */
class StateMachine {
public:
    virtual ~StateMachine() = default;
    
    // 应用已提交的日志条目
    virtual void Apply(const LogEntry& entry) = 0;
    
    // 创建快照
    virtual Snapshot CreateSnapshot() = 0;
    
    // 从快照恢复
    virtual void RestoreSnapshot(const Snapshot& snapshot) = 0;
};

/**
 * @brief Raft 节点接口
 * 
 * 可以使用现有实现（如 etcd/raft, braft）或自研
 */
class RaftNode {
public:
    // 初始化
    virtual Status Initialize(const NodeConfig& config) = 0;
    
    // 提议条目（仅 Leader 能成功）
    virtual Result<uint64_t> Propose(const std::string& data) = 0;
    
    // 检查是否是 Leader
    virtual bool IsLeader() const = 0;
    
    // 获取当前 Leader ID
    virtual NodeID GetLeader() const = 0;
    
    // 添加节点到集群
    virtual Status AddNode(NodeID node_id, const std::string& address);
    
    // 移除节点
    virtual Status RemoveNode(NodeID node_id);
    
    // 处理来自其他节点的消息
    virtual void OnMessage(const RaftMessage& msg) = 0;
    
    // 注册状态机
    virtual void SetStateMachine(StateMachine* sm) = 0;
};

// 可以使用现成的 Raft 实现
using RaftImpl = etcd::raft::RaftNode;  // 或其他实现

}  // namespace raft
}  // namespace dtx
}  // namespace cedar
```

### 6.2 服务发现与心跳

```cpp
// include/cedar/dtx/discovery.h

namespace cedar {
namespace dtx {

/**
 * @brief 服务发现接口
 */
class ServiceDiscovery {
public:
    // 注册本节点
    virtual Status Register(const NodeInfo& info) = 0;
    
    // 发现其他节点
    virtual std::vector<NodeInfo> DiscoverNodes() = 0;
    
    // 发送心跳
    virtual Status Heartbeat() = 0;
    
    // 订阅节点变更
    virtual void WatchNodes(std::function<void(const NodeEvent&)> callback) = 0;
};

/**
 * @brief 基于元服务的发现实现
 */
class MetadataBasedDiscovery : public ServiceDiscovery {
public:
    Status Register(const NodeInfo& info) override {
        return meta_service_->RegisterNode(info);
    }
    
    std::vector<NodeInfo> DiscoverNodes() override {
        auto nodes = meta_service_->GetAliveNodes();
        // 转换为 NodeInfo
        return ConvertToNodeInfo(nodes);
    }
    
private:
    MetadataService* meta_service_;
};

}  // namespace dtx
}  // namespace cedar
```

## 7. 部署配置示例

### 7.1 3 节点集群配置

```yaml
# cluster-config.yaml

# 元服务配置
metadata_service:
  nodes:
    - id: meta-0
      address: 10.0.0.1:2379
    - id: meta-1
      address: 10.0.0.2:2379
    - id: meta-2
      address: 10.0.0.3:2379
  
# 存储节点配置
storage_nodes:
  - id: 0
    address: 10.0.0.1:50051
    data_dir: /data/cedar/node0
    partitions: [0, 3, 6, 9, ...]  # 本节点作为主的分区
    
  - id: 1
    address: 10.0.0.2:50051
    data_dir: /data/cedar/node1
    partitions: [1, 4, 7, 10, ...]
    
  - id: 2
    address: 10.0.0.3:50051
    data_dir: /data/cedar/node2
    partitions: [2, 5, 8, 11, ...]

# 全局配置
cluster:
  num_partitions: 256
  replication_factor: 3
  
  # Raft 配置
  raft:
    election_timeout_ms: 1000
    heartbeat_interval_ms: 100
    snapshot_interval_sec: 3600
    
  # 2PC 配置
  twopc:
    prepare_timeout_ms: 5000
    commit_timeout_ms: 10000
    coordinator_retry_count: 3
```

## 8. 总结

| 组件 | 职责 | 数量 | 关键技术 |
|------|------|------|---------|
| **Metadata Service** | 分区映射、节点发现、全局配置 | 3~5 节点 | Raft 共识 |
| **Partition Raft** | 数据复制、分区级别一致性 | 每个 Partition 3 副本 | Raft 组 |
| **Coordinator** | 2PC 协调、事务管理 | 每个节点 1 个 | 2PC + 故障恢复 |

**关键设计决策**:
1. **两层共识**: 元服务 Raft（全局配置）+ 分区 Raft（数据复制）
2. **无单点**: 协调器无状态，任意节点可协调任意事务
3. **自动恢复**: Raft 自动处理 Leader 故障，2PC 超时处理协调器故障
