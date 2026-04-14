# CedarGraph-DTx 详细实现规划文档

> **版本**: v1.0  
> **日期**: 2025-04-05  
> **状态**: 设计阶段  
> **目标**: VLDB/SIGMOD 2025 投稿 + 生产可用实现

---

## 目录

1. [架构总览](#1-架构总览)
2. [核心数据结构](#2-核心数据结构)
3. [模块详细设计](#3-模块详细设计)
4. [实施路线图](#4-实施路线图)
5. [依赖与接口](#5-依赖与接口)
6. [风险分析](#6-风险分析)
7. [测试策略](#7-测试策略)
8. [性能目标](#8-性能目标)

---

## 1. 架构总览

### 1.1 系统架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CedarGraph-DTx 架构                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Client Interface Layer                            │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │   │
│  │  │   Session   │  │   Batch     │  │   Cypher    │  │   gRPC      │ │   │
│  │  │   (Driver)  │  │    API      │  │    API      │  │    API      │ │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘ │   │
│  └─────────┼────────────────┼────────────────┼────────────────┼────────┘   │
│            │                │                │                │            │
│            └────────────────┴────────────────┴────────────────┘            │
│                                     │                                      │
│                                     ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                 Distributed Transaction Coordinator                  │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │   │
│  │  │   Transaction   │  │   Partition     │  │   Routing Manager   │  │   │
│  │  │   Manager       │  │   Manager       │  │   (GLTR)            │  │   │
│  │  │                 │  │                 │  │                     │  │   │
│  │  │ - Txn Lifecycle │  │ - Partition Map │  │ - Subgraph Cache    │  │   │
│  │  │ - 2PC Coord     │  │ - Rebalancing   │  │ - Locality Scoring  │  │   │
│  │  │ - Deadlock Det. │  │ - Migration     │  │ - Route Optimization│  │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                      │
│                                     ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Core Protocol Layer                               │   │
│  │                                                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │    TW-CD     │  │   DVC-Val    │  │   LND-OCC    │               │   │
│  │  │              │  │              │  │              │               │   │
│  │  │ Temporal Win │  │ Version      │  │ LSM-Native   │               │   │
│  │  │ Conflict Det │  │ Chain Val.   │  │ Coordination │               │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │   │
│  │                                                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐                                  │   │
│  │  │    GLTR      │  │    BBCC      │                                  │   │
│  │  │              │  │              │                                  │   │
│  │  │ Graph Local. │  │ Bookmark     │                                  │   │
│  │  │ Routing      │  │ Causal Cons. │                                  │   │
│  │  └──────────────┘  └──────────────┘                                  │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                      │
│                                     ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Storage Node Layer (Per Shard)                   │   │
│  │                                                                      │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │   │
│  │  │   Local OCC     │  │   Version       │  │   LSM-Tree Engine   │  │   │
│  │  │   Engine        │  │   Chain Store   │  │   (VSLMemTable)     │  │   │
│  │  │                 │  │                 │  │                     │  │   │
│  │  │ - Read/Write Set│  │ - Head Pointer  │  │ - MemTable          │  │   │
│  │  │ - Validation    │  │ - Chain Nodes   │  │ - Immutable Tables  │  │   │
│  │  │ - Local Commit  │  │ - GC Management │  │ - SST Files         │  │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────────┘  │   │
│  │                                                                      │   │
│  │  ┌─────────────────┐  ┌─────────────────┐                            │   │
│  │  │   WAL Manager   │  │   Lock Manager  │                            │   │
│  │  │   (Group Commit)│  │   (Temporal)    │                            │   │
│  │  └─────────────────┘  └─────────────────┘                            │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                      │
│                                     ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Network & Consensus Layer                        │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │   │
│  │  │   RPC Service   │  │   Raft Group    │  │   Failure Detector  │  │   │
│  │  │   (gRPC)        │  │   (Consensus)   │  │   (Gossip)          │  │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 模块依赖图

```
                    ┌─────────────────┐
                    │   Client API    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   Coordinator   │
                    │   (Singleton)   │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼───────┐   ┌────────▼────────┐   ┌──────▼──────┐
│     TW-CD     │   │      GLTR       │   │    BBCC     │
│   (Module)    │   │    (Module)     │   │  (Module)   │
└───────┬───────┘   └────────┬────────┘   └──────┬──────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   LND-OCC       │
                    │   (Core)        │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   DVC-Val       │
                    │   (Core)        │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼───────┐   ┌────────▼────────┐   ┌──────▼──────┐
│  Local OCC    │   │  Version Chain  │   │  LSM Engine │
└───────────────┘   └─────────────────┘   └─────────────┘
```

---

## 2. 核心数据结构

### 2.1 分区与路由

```cpp
// include/cedar/dtx/partition.h

namespace cedar {
namespace dtx {

// 分区ID (16bit，与reserved:2匹配，支持65536个分区)
using PartitionID = uint16_t;

// 节点ID
using NodeID = uint32_t;

// 子图ID（用于GLTR）
using SubgraphID = uint32_t;

/**
 * @brief 分区元数据
 * 
 * 存储分区的拓扑信息、负载状态和统计信息
 */
struct PartitionMeta {
  PartitionID partition_id;
  NodeID primary_node;           // 主节点
  std::vector<NodeID> replicas;  // 副本节点
  
  // 拓扑信息（用于GLTR）
  std::unordered_set<SubgraphID> subgraphs;  // 包含的子图
  uint64_t vertex_count{0};      // 顶点数量
  uint64_t edge_count{0};        // 边数量
  
  // 负载信息
  std::atomic<uint64_t> txn_rate{0};  // 当前事务率 (TPS)
  std::atomic<uint64_t> conflict_rate{0};  // 冲突率
  
  // 统计信息
  uint64_t hot_key_count{0};     // 热点Key数量
  double locality_score{0.0};    // 局部性评分 (0-1)
};

/**
 * @brief CedarKey分区操作辅助类
 * 
 * CedarKey已经包含part_id字段（Offset 30-31, 16 bits）：
 * [entity_id:8][timestamp:8][target_id:8][column:2][seq:2][type:1][flags:1][part_id:2]
 * 
 * 支持最多 65536 个分区，无需扩展Key大小
 */
class CedarKeyPartitionHelper {
 public:
  // 获取Key的分区ID
  static PartitionID GetPartitionID(const CedarKey& key) {
    return key.part_id();
  }
  
  // 设置Key的分区ID（返回修改后的Key）
  static CedarKey SetPartitionID(CedarKey key, PartitionID pid) {
    key.SetPartId(pid);
    return key;
  }
  
  // 计算Key的分区（基于图局部性）
  static PartitionID ComputePartition(const CedarKey& key,
                                       const GraphTopology& topo);
  
  // 检查Key是否属于指定分区
  static bool BelongsToPartition(const CedarKey& key, PartitionID pid) {
    return key.part_id() == pid;
  }
};

/**
 * @brief 子图事务边界
 * 
 * GLTR使用此结构快速判断事务是否需要跨分区协调
 */
struct SubgraphBoundary {
  SubgraphID subgraph_id;
  std::unordered_set<PartitionID> partitions;  // 子图涉及的分区
  std::unordered_set<CedarKey, CedarKeyHash> boundary_keys;  // 边界Key
  
  // 快速判断：Key是否在此子图内
  bool Contains(const CedarKey& key) const;
  
  // 判断事务是否完全在子图内（仅涉及一个分区）
  bool IsLocalTransaction(const std::vector<CedarKey>& keys) const;
};

}  // namespace dtx
}  // namespace cedar
```

### 2.2 时序窗口（TW-CD核心）

```cpp
// include/cedar/dtx/temporal_window.h

namespace cedar {
namespace dtx {

/**
 * @brief 时序窗口 - TW-CD的核心数据结构
 * 
 * 事务声明其感兴趣的时间范围，冲突检测只检查窗口重叠的写入
 */
struct TemporalWindow {
  Timestamp start{0};  // 窗口起点（包含）
  Timestamp end{0};    // 窗口终点（包含），0表示无上限
  
  // 默认构造函数（全时间范围）
  TemporalWindow() = default;
  
  // 指定范围
  TemporalWindow(Timestamp s, Timestamp e) : start(s), end(e) {}
  
  // 从单个时间点构造（精确查询）
  explicit TemporalWindow(Timestamp point) : start(point), end(point) {}
  
  // 检查两个窗口是否重叠
  bool Overlaps(const TemporalWindow& other) const {
    return (end == 0 || other.start <= end) && 
           (other.end == 0 || start <= other.end);
  }
  
  // 检查时间点是否在窗口内
  bool Contains(Timestamp ts) const {
    return ts >= start && (end == 0 || ts <= end);
  }
  
  // 合并两个窗口
  void Merge(const TemporalWindow& other) {
    start = std::min(start, other.start);
    if (end == 0 || other.end == 0) {
      end = 0;  // 无上限
    } else {
      end = std::max(end, other.end);
    }
  }
  
  // 序列化
  std::string Serialize() const {
    return std::to_string(start.ToUInt64()) + ":" + 
           (end.ToUInt64() == 0 ? "inf" : std::to_string(end.ToUInt64()));
  }
};

/**
 * @brief 带时序窗口的读写集
 * 
 * 扩展标准读写集，添加时间窗口信息
 */
struct TemporalWriteSet {
  CedarKey key;
  TemporalWindow window;
  uint64_t version{0};  // 写入版本
  
  // 序列化大小
  static constexpr size_t kSerializedSize = 
      sizeof(CedarKey) + sizeof(TemporalWindow) + sizeof(uint64_t);
};

struct TemporalReadSet {
  CedarKey key;
  TemporalWindow window;
  uint64_t read_version{0};  // 读取时的版本
  Timestamp read_timestamp{0};  // 读取时间戳
};

/**
 * @brief 时序锁记录
 * 
 * 存储在Lock Manager中，支持时序粒度加锁
 */
struct TemporalLock {
  uint64_t txn_id{0};
  TemporalWindow window;
  LockType type{LockType::kNone};  // READ, WRITE, INTENT
  
  // 检查是否与另一个锁冲突
  bool ConflictsWith(const TemporalLock& other) const {
    if (!window.Overlaps(other.window)) return false;  // 时间不重叠，无冲突
    
    // 标准锁冲突矩阵
    if (type == LockType::kWrite || other.type == LockType::kWrite) {
      return true;  // 写锁与任何锁冲突
    }
    return false;  // 读-读不冲突
  }
};

}  // namespace dtx
}  // namespace cedar
```

### 2.3 分布式事务上下文

```cpp
// include/cedar/dtx/txn_context.h

namespace cedar {
namespace dtx {

/**
 * @brief 事务类型（分层模型）
 */
enum class TxnType : uint8_t {
  kSinglePartition = 1,   // 单分区事务（Layer 1）
  kSameTemporalRange = 2, // 同时序范围跨分区（Layer 2）
  kCrossTemporalRange = 3,// 跨时序范围（Layer 3）
  kDeterministic = 4,     // 确定性批量事务（Layer 4）
};

/**
 * @brief 分布式事务状态
 */
enum class DistributedTxnState : uint8_t {
  kStarted = 0,
  kPreparing = 1,      // 准备阶段（2PC）
  kPrepared = 2,       // 已准备好
  kCommitting = 3,     // 提交中
  kCommitted = 4,      // 已提交
  kAborting = 5,       // 回滚中
  kAborted = 6,        // 已回滚
};

/**
 * @brief 分布式事务上下文
 * 
 * 扩展单机OCCTransaction，添加分布式协调所需信息
 */
struct DistributedTxnContext {
  // 基础信息
  uint64_t txn_id;
  uint64_t start_ts;      // 开始时间戳（由ShardedTimestampAllocator分配）
  uint64_t commit_ts{0};  // 提交时间戳（验证后分配）
  
  // 事务类型
  TxnType type{TxnType::kSinglePartition};
  DistributedTxnState state{DistributedTxnState::kStarted};
  
  // 时序窗口（TW-CD）
  TemporalWindow temporal_window;
  
  // 参与者信息
  NodeID coordinator_node;  // 协调者节点
  std::unordered_set<PartitionID> participant_partitions;  // 参与分区
  std::unordered_map<PartitionID, NodeID> partition_leaders;  // 分区Leader
  
  // 读写集（带时序窗口）
  std::vector<TemporalReadSet> read_set;
  std::vector<TemporalWriteSet> write_set;
  
  // 图局部性（GLTR）
  std::unordered_set<SubgraphID> touched_subgraphs;
  bool is_local_subgraph{false};  // 是否完全在一个子图内
  
  // 因果一致性（BBCC）
  std::vector<Bookmark> causal_dependencies;  // 依赖的Bookmark
  
  // 统计信息
  uint64_t execution_time_ms{0};
  uint64_t coord_time_ms{0};
  uint32_t retry_count{0};
  
  // 快速判断方法
  bool IsSinglePartition() const {
    return participant_partitions.size() == 1;
  }
  
  bool NeedsCoordination() const {
    return participant_partitions.size() > 1;
  }
  
  // 判断是否与另一个事务冲突（TW-CD）
  bool MayConflictWith(const DistributedTxnContext& other) const {
    // 首先检查时序窗口
    if (!temporal_window.Overlaps(other.temporal_window)) {
      return false;  // 时间不重叠，无冲突
    }
    
    // 检查分区重叠
    for (const auto& pid : participant_partitions) {
      if (other.participant_partitions.count(pid)) {
        return true;  // 时间和分区都重叠，可能冲突
      }
    }
    return false;
  }
};

/**
 * @brief 2PC投票结果
 */
struct VoteResult {
  PartitionID partition_id;
  bool prepared{false};  // true=Prepared, false=Abort
  std::string reason;    // 如果Abort，原因
  uint64_t prepared_ts{0};  // 准备时间戳（用于DVC-Val）
};

}  // namespace dtx
}  // namespace cedar
```

### 2.4 分布式版本链（DVC-Val核心）

```cpp
// include/cedar/dtx/version_chain.h

namespace cedar {
namespace dtx {

/**
 * @brief 分布式版本链节点
 * 
 * 每个存储节点维护本地版本链，协调器维护跨节点视图
 */
struct VersionChainNode {
  uint64_t txn_id;
  Timestamp commit_ts;
  uint64_t version{0};  // 单调递增版本号
  
  // 序列化数据位置（用于延迟加载）
  uint64_t wal_offset{0};  // WAL中的位置
  
  // 下一个版本（ older ）
  VersionChainNode* next{nullptr};
  
  // 原子操作接口
  std::atomic<bool> visible{false};  // 是否可见（提交后才可见）
};

/**
 * @brief 版本链头（每个Key一个）
 * 
 * 使用无锁结构保证高并发读取
 */
struct VersionChainHead {
  std::atomic<VersionChainNode*> head{nullptr};  // 最新版本
  std::atomic<uint64_t> reader_count{0};  // 当前读取者数量
  
  // 获取最新可见版本（O(1)）
  VersionChainNode* GetLatestVisible() const;
  
  // 插入新版本（CAS操作）
  bool InsertVersion(VersionChainNode* new_node);
  
  // 垃圾回收（删除旧版本）
  void GC(Timestamp before_ts);
};

/**
 * @brief 分布式版本链索引
 * 
 * 每个存储分片维护的本地索引
 */
class VersionChainIndex {
 public:
  // 获取Key的版本链头
  VersionChainHead* GetOrCreate(const CedarKey& key);
  
  // 读取特定版本
  Status ReadVersion(const CedarKey& key, 
                      uint64_t version,
                      Value* value);
  
  // 验证读集（O(1)检查版本链头）
  bool ValidateRead(const CedarKey& key, 
                     uint64_t read_version,
                     Timestamp commit_ts);
  
  // 提交新版本
  Status CommitVersion(const CedarKey& key,
                        uint64_t txn_id,
                        Timestamp commit_ts,
                        const Value& value);
  
  // 定期GC
  void RunGC(Timestamp global_safe_ts);
  
 private:
  // Key -> 版本链头的映射
  std::unordered_map<CedarKey, VersionChainHead*, CedarKeyHash> index_;
  std::shared_mutex mutex_;
};

/**
 * @brief 跨分片版本链视图
 * 
 * 协调器使用，聚合多个分片的版本信息
 */
class CrossShardVersionView {
 public:
  // 添加分片的版本信息
  void AddShardView(PartitionID pid, 
                     const std::vector<VersionInfo>& versions);
  
  // 全局验证：检查所有读集是否仍然有效
  bool GlobalValidate(const DistributedTxnContext& ctx);
  
  // 计算全局安全时间戳（用于GC）
  Timestamp ComputeGlobalSafeTimestamp() const;
  
 private:
  std::unordered_map<PartitionID, std::vector<VersionInfo>> shard_views_;
};

}  // namespace dtx
}  // namespace cedar
```

---

## 3. 模块详细设计

### 3.1 TW-CD: 时序窗口冲突检测

#### 3.1.1 算法流程

```
算法: TW-CD Conflict Detection
输入: 事务T的读写集 (RS_T, WS_T)，时序窗口TW_T
      并发事务集合CT
输出: 是否冲突

1. 对于CT中的每个事务C:
   a. 如果 TW_T 与 C.TW 不重叠:
      continue  // 时序窗口不重叠，无冲突
   
   b. 如果 RS_T 与 C.WS 有交集:
      // 读-写冲突（传统OCC检测）
      return CONFLICT
   
   c. 如果 WS_T 与 C.WS 有交集:
      // 写-写冲突
      return CONFLICT

2. return NO_CONFLICT
```

#### 3.1.2 接口定义

```cpp
// include/cedar/dtx/tw_cd.h

class TemporalWindowConflictDetector {
 public:
  /**
   * @brief 注册事务的时序窗口
   * 
   * 在事务开始时调用，用于后续冲突检测
   */
  Status RegisterWindow(uint64_t txn_id, const TemporalWindow& window);
  
  /**
   * @brief 注销事务窗口（事务结束时）
   */
  void UnregisterWindow(uint64_t txn_id);
  
  /**
   * @brief 检测冲突（验证阶段）
   * 
   * 核心方法，TW-CD的主要逻辑
   */
  ConflictCheckResult CheckConflict(
      uint64_t txn_id,
      const TemporalWindow& window,
      const std::vector<CedarKey>& read_set,
      const std::vector<CedarKey>& write_set);
  
  /**
   * @brief 获取与指定窗口重叠的所有事务
   * 
   * 用于调试和监控
   */
  std::vector<uint64_t> GetOverlappingTxns(const TemporalWindow& window);
  
  /**
   * @brief 获取当前活跃窗口统计
   */
  WindowStats GetStats() const;
  
 private:
  // 活跃事务窗口索引（按时间排序）
  // 使用区间树或线段树实现高效范围查询
  struct WindowIndex {
    std::map<Timestamp, std::set<uint64_t>> start_points_;
    std::map<Timestamp, std::set<uint64_t>> end_points_;
    
    // 插入窗口
    void Insert(uint64_t txn_id, const TemporalWindow& window);
    
    // 删除窗口
    void Remove(uint64_t txn_id, const TemporalWindow& window);
    
    // 查询重叠窗口
    std::set<uint64_t> QueryOverlapping(const TemporalWindow& window) const;
  };
  
  WindowIndex index_;
  std::shared_mutex mutex_;  // 读写锁
};
```

#### 3.1.3 优化：窗口合并

对于频繁的小范围查询，自动合并窗口以减少检测开销：

```cpp
class WindowMergeOptimizer {
 public:
  // 合并相邻小窗口
  static TemporalWindow MergeAdjacentWindows(
      const std::vector<TemporalWindow>& windows,
      Timestamp gap_threshold);  // 小于此阈值视为相邻
      
  // 分裂大窗口（对于热点Key）
  static std::vector<TemporalWindow> SplitLargeWindow(
      const TemporalWindow& window,
      size_t max_span);  // 最大时间跨度
};
```

### 3.2 GLTR: 图局部性感知路由

#### 3.2.1 数据放置策略

```cpp
// include/cedar/dtx/graph_partitioner.h

/**
 * @brief 图感知分区器
 * 
 * 实现基于METIS的图划分，优化遍历局部性
 */
class GraphAwarePartitioner {
 public:
  /**
   * @brief 初始化分区（离线计算）
   * 
   * 基于图拓扑结构计算最优分区
   */
  Status Initialize(const GraphTopology& graph, 
                     size_t num_partitions,
                     double imbalance_factor = 1.05);
  
  /**
   * @brief 获取Key的分区
   */
  PartitionID GetPartition(const CedarKey& key) const;
  
  /**
   * @brief 获取Key所属的子图
   */
  SubgraphID GetSubgraph(const CedarKey& key) const;
  
  /**
   * @brief 计算一组Key涉及的分区
   */
  std::set<PartitionID> GetPartitionsForKeys(
      const std::vector<CedarKey>& keys) const;
  
  /**
   * @brief 判断事务是否需要跨分区协调
   */
  bool NeedsCoordination(const std::vector<CedarKey>& keys) const {
    return GetPartitionsForKeys(keys).size() > 1;
  }
  
  /**
   * @brief 计算子图边界
   * 
   * 用于GLTR的子图事务优化
   */
  SubgraphBoundary ComputeSubgraphBoundary(SubgraphID sid) const;
  
  /**
   * @brief 动态重平衡（运行时调用）
   */
  Status Rebalance(const PartitionLoadStats& stats);
  
 private:
  // 顶点 -> 分区映射
  std::unordered_map<uint64_t, PartitionID> vertex_partition_map_;
  
  // 分区 -> 子图映射
  std::unordered_map<PartitionID, std::set<SubgraphID>> partition_subgraphs_;
  
  // METIS参数
  struct MetisParams {
    int ptype{0};      // 分区类型 (0=k-way, 1=recursive)
    int objtype{1};    // 目标 (0=cut, 1=vol)
    int ctype{1};      // 粗化类型
  } metis_params_;
};
```

#### 3.2.2 事务路由决策

```cpp
// include/cedar/dtx/transaction_router.h

/**
 * @brief 事务路由决策
 */
struct RoutingDecision {
  TxnType txn_type;
  std::vector<PartitionID> participants;
  NodeID coordinator;
  SubgraphID primary_subgraph{kInvalidSubgraphID};
  bool can_use_local_occ{false};
  bool needs_2pc{false};
  std::string reason;  // 决策原因（用于调试）
};

class GraphLocalityRouter {
 public:
  /**
   * @brief 路由决策（核心方法）
   * 
   * 根据Key集合和图拓扑决定事务类型和协调策略
   */
  RoutingDecision RouteTransaction(
      const std::vector<CedarKey>& keys,
      const TemporalWindow& window) const;
  
  /**
   * @brief 选择协调者节点
   * 
   * 策略：
   * 1. 优先选择包含最多Key的分区Leader
   * 2. 考虑节点负载
   * 3. 考虑网络拓扑（就近原则）
   */
  NodeID SelectCoordinator(const std::vector<PartitionID>& participants,
                            const std::vector<CedarKey>& keys) const;
  
  /**
   * @brief 更新局部性缓存
   * 
   * 运行时学习，优化路由决策
   */
  void UpdateLocalityCache(const CedarKey& key, 
                            PartitionID partition);
  
 private:
  GraphAwarePartitioner* partitioner_;
  
  // 局部性缓存（热点Key的快速查找）
  struct LocalityCache {
    CedarKey key;
    PartitionID partition;
    uint64_t access_count{0};
    std::chrono::steady_clock::time_point last_access;
  };
  
  LRUCache<CedarKey, LocalityCache> cache_{10000};  // 10K条目
};
```

### 3.3 LND-OCC: LSM-Tree原生分布式OCC

#### 3.3.1 分层事务提交

```cpp
// include/cedar/dtx/lsm_native_occ.h

/**
 * @brief LSM-Tree原生分布式OCC
 * 
 * 核心思想：利用LSM的不可变特性消除协调开销
 */
class LsmNativeDistributedOCC {
 public:
  /**
   * @brief 单分区事务提交（无协调）
   * 
   * 利用MemTable切换的原子性实现提交
   */
  Status SinglePartitionCommit(PartitionID pid,
                                const DistributedTxnContext& ctx);
  
  /**
   * @brief 同时序范围跨分区提交（轻量协调）
   * 
   * 仅验证时序窗口，不锁定数据
   */
  Status SameTemporalRangeCommit(
      const std::vector<PartitionID>& participants,
      const DistributedTxnContext& ctx);
  
  /**
   * @brief 完整2PC（跨时序范围）
   */
  Status FullTwoPhaseCommit(
      const std::vector<PartitionID>& participants,
      const DistributedTxnContext& ctx);
  
 private:
  /**
   * @brief MemTable级原子提交
   * 
   * 将事务数据写入MemTable，切换时自动提交
   */
  Status MemTableCommit(PartitionID pid,
                         const WriteBatch& batch,
                         uint64_t commit_ts);
  
  /**
   * @brief Zone-Columnar感知的事务分组
   * 
   * 按Zone分组写入，优化压缩
   */
  std::map<ZoneID, WriteBatch> GroupByZone(
      const std::vector<TemporalWriteSet>& write_set);
  
  /**
   * @brief 异步WAL写入（组提交优化）
   */
  Status AsyncWalWrite(const std::vector<WriteBatch>& batches,
                        uint64_t commit_ts);
};
```

#### 3.3.2 时序一致性级别

```cpp
/**
 * @brief 时序一致性级别
 * 
 * 允许应用在性能和一致性之间权衡
 */
enum class TemporalConsistencyLevel : uint8_t {
  // 严格一致性：读取必须看到所有已提交写入
  kStrict = 0,
  
  // 边界一致性：允许读取到N秒前的状态（降低延迟）
  kBounded = 1,
  
  // 会话一致性：保证会话内因果一致性
  kSession = 2,
  
  // 最终一致性：无保证，最高性能
  kEventual = 3,
};

class TemporalConsistencyManager {
 public:
  // 检查读取是否满足一致性级别
  bool SatisfiesConsistency(Timestamp read_ts,
                             Timestamp max_committed_ts,
                             TemporalConsistencyLevel level);
  
  // 获取指定级别允许的最大读取延迟
  Timestamp GetAllowedStaleness(TemporalConsistencyLevel level);
};
```

### 3.4 DVC-Val: 分布式版本链验证

#### 3.4.1 O(1) 验证算法

```cpp
// include/cedar/dtx/dvc_validator.h

/**
 * @brief 分布式版本链验证器
 */
class DistributedVersionChainValidator {
 public:
  /**
   * @brief O(1)快速验证（核心优化）
   * 
   * 只检查版本链头部，避免遍历
   */
  ValidationResult FastValidate(
      const CedarKey& key,
      uint64_t read_version,      // 事务读取时的版本
      Timestamp commit_ts,         // 事务计划提交时间
      const VersionChainHead& head);
  
  /**
   * @brief 完整验证（必要时回退）
   * 
   * 当FastValidate无法确定时，遍历版本链
   */
  ValidationResult FullValidate(
      const CedarKey& key,
      uint64_t read_version,
      Timestamp commit_ts,
      VersionChainNode* chain);
  
  /**
   * @brief 批量验证
   * 
   * 并行验证多个Key
   */
  std::vector<ValidationResult> BatchValidate(
      const std::vector<TemporalReadSet>& read_set,
      Timestamp commit_ts);
  
 private:
  // O(1)验证逻辑
  ValidationResult ValidateWithHead(
      const VersionChainHead& head,
      uint64_t read_version,
      Timestamp commit_ts) {
    
    auto* latest = head.head.load();
    if (!latest || !latest->visible.load()) {
      // 无可见版本，读的是空值
      return ValidationResult::kValid;
    }
    
    if (latest->version == read_version) {
      // 版本未变，验证通过
      return ValidationResult::kValid;
    }
    
    if (latest->commit_ts > commit_ts) {
      // 最新版本在事务开始后提交，不影响本事务
      return ValidationResult::kValid;
    }
    
    // 版本已变且可能影响本事务，需要完整检查
    return ValidationResult::kNeedFullCheck;
  }
};
```

#### 3.4.2 跨分片验证协调

```cpp
/**
 * @brief 跨分片验证协调器
 * 
 * 协调者角色：收集各分片验证结果，做出全局决策
 */
class CrossShardValidationCoordinator {
 public:
  /**
   * @brief 执行跨分片验证
   * 
   * 流程：
   * 1. 向各参与分片发送验证请求
   * 2. 收集验证结果
   * 3. 汇总决策
   */
  ValidationResult CoordinateValidation(
      const DistributedTxnContext& ctx);
  
  /**
   * @brief 处理分片验证请求（分片端）
   */
  ValidationResult HandleValidationRequest(
      PartitionID pid,
      const std::vector<TemporalReadSet>& read_set,
      Timestamp commit_ts);
  
 private:
  // RPC客户端
  std::unordered_map<NodeID, std::unique_ptr<ValidationService::Stub>> stubs_;
  
  // 超时配置
  std::chrono::milliseconds validation_timeout_{100};
};
```

### 3.5 BBCC: Bookmark因果一致性

#### 3.5.1 扩展Bookmark

```cpp
// include/cedar/dtx/bookmark_manager.h

/**
 * @brief 分布式Bookmark
 * 
 * 扩展单机Bookmark，支持分布式因果一致性
 */
struct DistributedBookmark {
  // 基础信息（继承单机Bookmark）
  uint64_t timestamp{0};
  uint64_t txn_id{0};
  
  // 分布式扩展
  std::vector<std::pair<PartitionID, uint64_t>> shard_watermarks;  // 各分片水位
  std::unordered_map<NodeID, uint64_t> node_clocks;  // 节点逻辑时钟（HLC）
  
  // 序列化格式: v2d:{timestamp}:{txn_id}:{shard_watermarks}:{node_clocks}
  std::string Serialize() const;
  static std::optional<DistributedBookmark> Deserialize(const std::string& str);
  
  // 合并多个Bookmark（取最大值）
  static DistributedBookmark Merge(
      const std::vector<DistributedBookmark>& bookmarks);
  
  // 因果序判断
  bool HappensBefore(const DistributedBookmark& other) const;
  bool IsConcurrentWith(const DistributedBookmark& other) const;
};

/**
 * @brief Bookmark管理器
 */
class BookmarkManager {
 public:
  /**
   * @brief 从事务上下文生成Bookmark
   */
  DistributedBookmark CreateBookmark(const DistributedTxnContext& ctx);
  
  /**
   * @brief 等待Bookmark满足（因果一致性读）
   * 
   * 阻塞直到本地状态至少达到Bookmark指定的时间点
   */
  Status WaitForBookmark(const DistributedBookmark& bookmark,
                          std::chrono::milliseconds timeout);
  
  /**
   * @brief 更新本地水位
   */
  void UpdateLocalWatermark(PartitionID pid, uint64_t watermark);
  
  /**
   * @brief 获取全局最小水位（用于GC）
   */
  uint64_t GetGlobalMinWatermark() const;
  
 private:
  // 各分片水位
  std::unordered_map<PartitionID, std::atomic<uint64_t>> watermarks_;
  
  // HLC混合逻辑时钟
  struct HybridLogicalClock {
    uint64_t wall_time{0};  // 物理时间
    uint64_t logical{0};    // 逻辑计数
  } hlc_;
  
  mutable std::mutex hlc_mutex_;
};
```

---

## 4. 实施路线图

### Phase 1: 基础框架（Week 1-8）

#### Week 1-2: 项目结构搭建
- [ ] 创建 `src/dtx/` 和 `include/cedar/dtx/` 目录结构
- [ ] 定义基础数据结构和接口
- [ ] 实现分区管理模块骨架
- [ ] **交付物**: 可编译的基础框架，通过基础单元测试

#### Week 3-4: 分区与路由
- [ ] 实现 `GraphAwarePartitioner`（METIS集成）
- [ ] 实现 `PartitionedCedarKey` 编码
- [ ] 实现 `GraphLocalityRouter`
- [ ] 实现子图边界检测
- [ ] **交付物**: 分区策略工作，路由决策正确

#### Week 5-6: 网络层
- [ ] 定义gRPC服务接口（DTxService）
- [ ] 实现节点间RPC通信
- [ ] 实现失败检测（Gossip协议）
- [ ] **交付物**: 多节点间可通信，失败检测工作

#### Week 7-8: 集成测试
- [ ] 单节点完整事务流程
- [ ] 多节点分区事务
- [ ] 性能基准测试
- [ ] **交付物**: Phase 1验收测试通过

### Phase 2: TW-CD与LND-OCC（Week 9-16）

#### Week 9-11: TW-CD实现
- [ ] 实现 `TemporalWindowConflictDetector`
- [ ] 实现窗口索引（区间树）
- [ ] 实现窗口合并优化
- [ ] 集成到事务验证流程
- [ ] **交付物**: TW-CD功能完整，单元测试覆盖>80%

#### Week 12-14: LND-OCC核心
- [ ] 实现 `LsmNativeDistributedOCC`
- [ ] MemTable级原子提交
- [ ] Zone感知事务分组
- [ ] 异步WAL写入
- [ ] **交付物**: 单分区事务无需协调

#### Week 15-16: 整合与优化
- [ ] TW-CD + LND-OCC整合
- [ ] 性能调优
- [ ] 压力测试
- [ ] **交付物**: Phase 2验收，性能目标达成

### Phase 3: DVC-Val（Week 17-22）

#### Week 17-19: 版本链实现
- [ ] 实现 `VersionChainIndex`
- [ ] 实现O(1)快速验证
- [ ] 实现跨分片验证协调
- [ ] GC机制
- [ ] **交付物**: 版本链验证工作

#### Week 20-21: 集成与优化
- [ ] 与LND-OCC整合
- [ ] 批量验证优化
- [ ] 性能对比测试
- [ ] **交付物**: DVC-Val性能达标

#### Week 22: 稳定性测试
- [ ] 长时运行测试
- [ ] 故障恢复测试
- [ ] **交付物**: Phase 3验收

### Phase 4: BBCC与GLTR优化（Week 23-28）

#### Week 23-25: BBCC实现
- [ ] 扩展Bookmark格式
- [ ] 实现 `BookmarkManager`
- [ ] HLC混合逻辑时钟
- [ ] 因果一致性读
- [ ] **交付物**: BBCC功能完整

#### Week 26-27: GLTR优化
- [ ] 实现局部性缓存
- [ ] 运行时学习优化
- [ ] 动态重平衡
- [ ] **交付物**: 90%事务本地执行

#### Week 28: 整合测试
- [ ] 全系统整合
- [ ] 端到端测试
- [ ] **交付物**: Phase 4验收

### Phase 5: 完整系统与论文实验（Week 29-36）

#### Week 29-32: Baseline实现
- [ ] Calvin确定性事务（对比）
- [ ] 标准2PC（对比）
- [ ] Percolator风格OCC（对比）
- [ ] **交付物**: 所有Baseline可运行

#### Week 33-35: 论文实验
- [ ] 吞吐量测试
- [ ] 延迟测试
- [ ] 可扩展性测试
- [ ] 冲突率分析
- [ ] **交付物**: 实验数据完整

#### Week 36: 论文初稿
- [ ] 撰写论文
- [ ] 内部评审
- [ ] **交付物**: 论文初稿提交

### Phase 6: 论文完善与投稿（Week 37-42）

#### Week 37-39: 论文修改
- [ ] 根据反馈修改
- [ ] 补充实验
- [ ] 图表优化
- [ ] **交付物**: 论文修改稿

#### Week 40-42: 投稿准备
- [ ] 最终校对
- [ ] 投稿材料准备
- [ ] 提交VLDB/SIGMOD
- [ ] **交付物**: 论文投稿

---

## 5. 依赖与接口

### 5.1 外部依赖

| 依赖 | 版本 | 用途 | 引入时机 |
|------|------|------|----------|
| gRPC | 1.60+ | 节点间通信 | Phase 1 |
| METIS | 5.1+ | 图分区 | Phase 1 |
| HdrHistogram | 最新 | 性能测量 | Phase 2 |
| Google Benchmark | 最新 | 微基准测试 | Phase 2 |

### 5.2 内部接口

#### 与现有OCCTransaction的集成

```cpp
// 扩展现有OCCTransaction以支持分布式
class DistributedOCCTransaction : public OCCTransaction {
 public:
  // 添加分布式上下文
  void SetDistributedContext(const DistributedTxnContext& ctx);
  
  // 重写验证方法，支持DVC-Val
  Status Validate() override;
  
  // 重写提交方法，支持LND-OCC
  Status Commit() override;
  
 private:
  std::unique_ptr<DistributedTxnContext> dtx_ctx_;
};
```

#### 与VSLMemTable的集成

```cpp
// 扩展VSLMemTable以支持版本链
class DistributedVSLMemTable : public VSLMemTable {
 public:
  // 添加版本链索引
  void SetVersionChainIndex(VersionChainIndex* index);
  
  // 支持MemTable级原子提交
  Status AtomicCommit(const WriteBatch& batch, uint64_t commit_ts);
  
  // 获取版本链头（用于DVC-Val）
  VersionChainHead* GetVersionChainHead(const CedarKey& key);
};
```

---

## 6. 风险分析

### 6.1 技术风险

| 风险 | 可能性 | 影响 | 缓解措施 |
|------|--------|------|----------|
| **TW-CD窗口重叠度过高** | 中 | 高 | 实现窗口分裂优化；提供回退到标准OCC的开关 |
| **GLTR分区不平衡** | 中 | 中 | 实现动态重平衡；监控热点并自动迁移 |
| **DVC-Val内存开销** | 高 | 中 | 积极GC策略；限制版本链长度；溢出到磁盘 |
| **网络分区一致性** | 低 | 高 | Raft共识；分区期间拒绝写；自动恢复 |
| **死锁** | 低 | 高 | 超时检测；事务优先级；死锁预防算法 |

### 6.2 进度风险

| 风险 | 可能性 | 影响 | 缓解措施 |
|------|--------|------|----------|
| **METIS集成复杂** | 中 | 中 | 准备备选方案（哈希分区）；简化初始实现 |
| **性能不达预期** | 中 | 高 | 早期原型验证；预留优化时间；调整目标 |
| **论文实验不充分** | 低 | 高 | 并行进行实验；准备简化版Storyline |

### 6.3 依赖风险

| 风险 | 可能性 | 影响 | 缓解措施 |
|------|--------|------|----------|
| **gRPC性能瓶颈** | 低 | 中 | 评估使用RDMA/DPDK；优化序列化 |
| **METIS许可问题** | 低 | 高 | 使用兼容许可证；备选自研分区器 |

---

## 7. 测试策略

### 7.1 单元测试

```cpp
// 测试TW-CD
TEST(TemporalWindowTest, OverlapDetection) {
  TemporalWindow w1(100, 200);
  TemporalWindow w2(150, 250);
  EXPECT_TRUE(w1.Overlaps(w2));
}

// 测试GLTR路由
TEST(GraphLocalityRouterTest, LocalTransactionRouting) {
  GraphLocalityRouter router;
  std::vector<CedarKey> keys = GenerateSameSubgraphKeys(10);
  auto decision = router.RouteTransaction(keys, TemporalWindow());
  EXPECT_EQ(decision.txn_type, TxnType::kSinglePartition);
}

// 测试DVC-Val
TEST(VersionChainValidatorTest, FastValidation) {
  DistributedVersionChainValidator validator;
  VersionChainHead head;
  // ... 构造版本链
  auto result = validator.FastValidate(key, read_version, commit_ts, head);
  EXPECT_EQ(result, ValidationResult::kValid);
}
```

### 7.2 集成测试

| 测试场景 | 描述 | 通过标准 |
|----------|------|----------|
| 单分区事务 | 单Key读写 | 正确性100%，延迟<1ms |
| 跨分区事务 | 多Key跨分区 | 原子性保证，无丢失更新 |
| 冲突检测 | 并发写同一Key | 正确检测冲突，至少一个成功 |
| 故障恢复 | 节点故障后恢复 | 数据一致性，自动重新路由 |
| 长事务 | 持有锁长时间 | 无死锁，正确超时 |

### 7.3 性能测试

```cpp
// 吞吐量基准
TEST_F(PerformanceTest, ThroughputBenchmark) {
  const int kNumThreads = 16;
  const int kDurationSec = 60;
  
  auto result = RunThroughputBenchmark(kNumThreads, kDurationSec);
  
  EXPECT_GT(result.tps, 100000);  // 目标: 10万TPS
  EXPECT_LT(result.p50_latency_ms, 1);  // P50 < 1ms
  EXPECT_LT(result.p99_latency_ms, 10);  // P99 < 10ms
}

// 可扩展性测试
TEST_F(ScalabilityTest, NodeScaling) {
  for (int nodes : {1, 2, 4, 8, 16}) {
    auto result = RunWithNodes(nodes);
    double efficiency = result.throughput / (baseline * nodes);
    EXPECT_GT(efficiency, 0.8);  // 扩展效率>80%
  }
}
```

### 7.4 混沌测试

- **网络延迟**: 注入100ms延迟，验证超时机制
- **节点故障**: 随机kill节点，验证故障转移
- **时钟偏移**: 模拟时钟漂移，验证HLC
- **分区**: 网络分区场景，验证一致性

---

## 8. 性能目标

### 8.1 核心指标

| 指标 | 目标 | 基准（传统2PC） | 提升倍数 |
|------|------|----------------|---------|
| **单分区吞吐** | 500K TPS | 100K TPS | 5x |
| **跨分区吞吐** | 100K TPS | 20K TPS | 5x |
| **单分区延迟(P50)** | 0.5 ms | 2 ms | 4x |
| **跨分区延迟(P50)** | 2 ms | 10 ms | 5x |
| **冲突检测时间** | <10 μs | N/A | - |
| **版本链验证** | O(1) avg | O(N) | - |

### 8.2 扩展性目标

| 节点数 | 目标吞吐 | 效率 |
|--------|----------|------|
| 1 | 500K TPS | 100% |
| 4 | 1.8M TPS | 90% |
| 16 | 6.4M TPS | 80% |
| 64 | 20M TPS | 60% |

### 8.3 对比目标（vs Baseline）

| 对比系统 | 吞吐量提升 | 延迟降低 | 冲突率降低 |
|----------|-----------|----------|-----------|
| **Calvin** | 2x | 3x | 40% |
| **Spanner** | 3x | 5x | 50% |
| **Percolator** | 2x | 2x | 60% |
| **AeonG** | 4x | 4x | 70% |

---

## 附录

### A. 术语表

| 术语 | 英文 | 说明 |
|------|------|------|
| TW-CD | Temporal-Window Conflict Detection | 时序窗口冲突检测 |
| GLTR | Graph-Locality-Aware Transaction Routing | 图局部性感知路由 |
| LND-OCC | LSM-Tree Native Distributed OCC | LSM-Tree原生分布式OCC |
| DVC-Val | Distributed Version-Chain Validation | 分布式版本链验证 |
| BBCC | Bookmark-Based Causal Consistency | Bookmark因果一致性 |
| HLC | Hybrid Logical Clock | 混合逻辑时钟 |

### B. 参考论文

1. Calvin: "Calvin: Fast Distributed Transactions for Partitioned Database Systems" (SIGMOD 2012)
2. Spanner: "Spanner: Google's Globally-Distributed Database" (OSDI 2012)
3. Percolator: "Large-scale Incremental Processing Using Distributed Transactions and Notifications" (OSDI 2010)
4. CockroachDB: "CockroachDB: The Resilient Geo-Distributed SQL Database" (SIGMOD 2020)
5. FaRM: "No Compromises: Distributed Transactions with Consistency, Availability, and Performance" (SOSP 2015)

### C. 代码仓库结构

```
cedar-graph-dtx/
├── src/dtx/
│   ├── coordinator/
│   │   ├── txn_coordinator.cc
│   │   ├── partition_manager.cc
│   │   └── router.cc
│   ├── protocol/
│   │   ├── tw_cd.cc
│   │   ├── lnd_occ.cc
│   │   ├── dvc_validator.cc
│   │   └── bbcc_manager.cc
│   ├── storage/
│   │   ├── version_chain_index.cc
│   │   ├── distributed_vsl_memtable.cc
│   │   └── lock_manager.cc
│   ├── network/
│   │   ├── rpc_service.cc
│   │   ├── failure_detector.cc
│   │   └── consensus.cc
│   └── utils/
│       ├── temporal_window.cc
│       ├── graph_partitioner.cc
│       └── hlc.cc
├── include/cedar/dtx/
│   └── (所有头文件)
├── tests/dtx/
│   ├── unit/
│   ├── integration/
│   └── benchmark/
└── docs/
    ├── DTx-Architecture.md
    ├── DTx-Protocol.md
    └── DTx-Evaluation.md
```

---

*本文档为 CedarGraph-DTx 的详细实施规划，指导从原型到生产系统的完整开发流程。建议定期更新进度和风险状态。*
