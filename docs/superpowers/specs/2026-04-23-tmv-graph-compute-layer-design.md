# Cedar 分布式图计算层（TMV）架构设计方案

> **状态：** 已确认（方案 B：完整重构）  
> **日期：** 2026-04-23  
> **作者：** Cedar Architecture Team  
> **关联文档：**
> - `docs/superpowers/plans/2026-04-16-epoch-based-block-linked-csr.md`（原型验证）
> - `docs/superpowers/plans/2026-04-16-distributed-temporal-graph-next-phase.md`（分布式加固计划）

---

## 1. 摘要与设计哲学

本方案提出构建 Cedar 的 **带状态的、事件驱动的时态物化视图（Stateful Temporal Materialized View, TMV）** 图计算层，彻底摒弃传统图数据库（如 NebulaGraph）"无状态查询翻译"的存算分离模式。

**核心哲学：**

- **底层存储负责极速吞吐（Write-Optimized）：** Storage Node（SN）基于 LSM-tree，只追加无冗余的 32B 原子事件，通过 Raft 保证分区一致性。
- **上层内存图层负责拓扑重构与时序折叠（Read-Optimized）：** Graph Compute Node（GCN）作为独立进程，在内存中维护基于 Epoch Chunk 的 Versioned CSR 拓扑，实现纳秒级多跳时态遍历。

**双重解耦：**

1. **物理拓扑与逻辑视图解耦：** SN 只存储单向事件日志，GCN 在内存中通过"内存反转构建"生成双向 CSR。
2. **属性与拓扑内存隔离：** GCN 的 `TMVEdge` 存储高频属性偏移指针，低频大属性存储在独立 `PropertyArena`，遍历时不污染缓存。

---

## 2. 总体架构图景（The Big Picture）

### 2.1 集群角色定义

在 Cedar 分布式集群中，图计算层引入三个核心角色：

| 角色 | 进程名 | 核心职责 | 状态特性 |
|------|--------|----------|----------|
| **Storage Node (SN)** | `storaged` | 基于 LSM-tree 的底层键值存储。数据按 `Hash(EntityID)` 分片，完全无冗余地存储 32B 原子事件。 | 无状态（存储状态外化于磁盘） |
| **Graph Compute Node (GCN)** | `graphcomputenode`（新建） | 系统的核心大脑。内存中维护基于 Epoch Chunk 的 Versioned CSR 拓扑。负责执行多跳图遍历和时态图算法。 | **带状态**（维护时间窗口拓扑视图，可容忍丢失） |
| **Coordinator** | `metad`（扩展） | 维护全局元数据，记录"哪个 GCN 当前在内存中缓存了哪个 EntityID 的哪个时间段及版本"。 | 强一致（基于 Raft） |

### 2.2 部署拓扑

```
┌─────────────────────────────────────────────────────────────┐
│                      Client / Cypher Engine                  │
└───────────────────────┬─────────────────────────────────────┘
                        │ TraversalRequest (brpc)
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
   ┌─────────┐    ┌─────────┐    ┌─────────┐
   │  GCN-1  │◄──►│  GCN-2  │◄──►│  GCN-N  │  ←── 带状态内存拓扑 (TMV)
   │ (TMV)   │    │ (TMV)   │    │ (TMV)   │
   └────┬────┘    └────┬────┘    └────┬────┘
        │ Bootstrap     │ Bootstrap     │     (按需拉取 or CDC 推送)
        └───────────────┼───────────────┘
                        ▼
        ┌───────────────────────────────┐
        │      Coordinator (metad)      │
        │   [VertexLocationTable Raft]  │
        └───────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
   ┌─────────┐    ┌─────────┐    ┌─────────┐
   │  SN-1   │    │  SN-2   │    │  SN-N   │  ←── 仅存储 32B 事件 (LSM-tree)
   │(LSM-tree│    │(LSM-tree│    │(LSM-tree│
   └─────────┘    └─────────┘    └─────────┘
```

### 2.3 与现有系统的切割声明

方案 B 采用**完整重构**策略：

1. **现有 `src/compute/` 全部视为原型并废弃：** `TemporalGraphView`、`EpochChunk`（原型版）、`ArenaPool`（原型版）、`TemporalQueryEngine` 等不再被新 GCN 使用。它们验证了数据结构可行性，但新 GCN 需要独立进程的内存模型、NUMA-aware 分配和跨核无锁访问。
2. **现有 `src/graph/cedar_graph_temporal.cc` 的查询接口被 GCN Client SDK 替代：** Cypher 引擎或其他查询发起方不再直接调用 `CedarGraph::GetOutNeighborsAsOf`，而是通过 brpc channel 向 GCN 发送 `TraversalRequest`。
3. **现有 `src/service/graph_service_router.cc` 被 Coordinator 动态路由表替代：** 不再基于静态配置的 graphd 列表做轮询，而是基于 Coordinator 维护的**动态顶点缓存目录**做精确路由。

---

## 3. 核心内存结构 —— 时态折叠引擎（TMV Engine）

GCN 的内存不存储线性的"事件日志"，而是将其折叠为具有生命周期 `T[from,to)` 的拓扑结构，并极致对齐硬件缓存。

### 3.1 物理布局（C++17，严格对齐）

#### 3.1.1 TMVEdge（32B 胖边）

```cpp
// === include/cedar/gcn/tmv_edge.h ===
// 32 Bytes: 一个 Cache Line (64B) 存 2 条，一次 AVX2 加载处理 2 条
struct alignas(32) TMVEdge {
    uint64_t target_id;        // 8B: 目标顶点 ID
    uint32_t valid_from;       // 4B: 关系生效时间 (CREATE 事件)
    uint32_t valid_to;         // 4B: 关系失效时间 (DELETE 事件，存活为 UINT32_MAX)
    uint64_t attr_offset;      // 8B: 指向 GCN PropertyArena 的偏移量
                               //     （非裸指针，支持 Arena 重定位）
    uint32_t edge_type;        // 4B: 边类型（过滤时避免查属性表）
    uint32_t reserved;         // 4B: 预留（权重、TTL 标记位等）
};
static_assert(sizeof(TMVEdge) == 32);
static_assert(alignof(TMVEdge) == 32);
```

**设计权衡：**
- **密度 vs 局部性：** 64B Cache Line 从存 4 条（16B 瘦边）降为存 2 条，遍历带宽减半。但高频属性可通过 `attr_offset` 在遍历时不跳表直接访问，避免一次额外的 HashMap lookup。
- **低频属性**（大文本、复杂 JSON）仍然存储在独立的 `PropertyArena`，`attr_offset` 可指向一个间接页。

#### 3.1.2 TMVChunk（纪元时间块）

```cpp
// === include/cedar/gcn/tmv_chunk.h ===
// ~1MB，匹配 HugePage (2MB 可容 2 个 chunk + 元数据)
struct alignas(4096) TMVChunk {
    static constexpr uint32_t kCapacity = 65536;  // 1MB = 65536 * 16B

    // 元数据区（64B，一个 Cache Line）
    uint64_t min_valid_from = UINT64_MAX;
    uint64_t max_valid_to   = 0;
    std::atomic<uint32_t> event_count{0};
    std::atomic<bool>     sealed{false};
    uint32_t pad[9];  // 补齐到 64B

    // 数据区（严格连续）
    TMVEdge edges[kCapacity];

    // 链表指针不在数据区内，避免污染预取
    TMVChunk* next = nullptr;          // 时间链表下一个 chunk
    TMVChunk* next_freelist = nullptr; // Arena 回收链表
};
```

#### 3.1.3 TMVVertexEntry（实体入口）

```cpp
// === include/cedar/gcn/tmv_vertex_entry.h ===
struct TMVVertexEntry {
    uint64_t entity_id;
    
    // 出边/入边各维护一个按时间升序排列的 chunk 链表
    std::atomic<TMVChunk*> out_chunk_head{nullptr};
    std::atomic<TMVChunk*> out_chunk_tail{nullptr};  // 活跃写入端
    std::atomic<TMVChunk*> in_chunk_head{nullptr};
    std::atomic<TMVChunk*> in_chunk_tail{nullptr};

    // 统计与水位线（用于 O(1) GC）
    std::atomic<uint64_t> out_edge_count{0};
    std::atomic<uint64_t> in_edge_count{0};
    std::atomic<uint64_t> earliest_chunk_timestamp{UINT64_MAX};
};
```

#### 3.1.4 TMVIndex（SwissTable + 分区锁）

```cpp
// === include/cedar/gcn/tmv_index.h ===
class TMVIndex {
 public:
  TMVVertexEntry* FindOrCreate(uint64_t entity_id);
  TMVVertexEntry* Find(uint64_t entity_id) const;
  void Reserve(uint64_t expected_vertices);

 private:
  static constexpr size_t kShardBits = 8;
  static constexpr size_t kNumShards = 1 << kShardBits;
  
  struct Shard {
    mutable absl::base_internal::SpinLock lock;
    absl::flat_hash_map<uint64_t, TMVVertexEntry> entries;
  };
  std::array<Shard, kNumShards> shards_;
};
```

**哈希表选型：** 选用 `absl::flat_hash_map`。理由：`third_party/brpc-master/CMakeLists.txt` 已声明 `find_package(absl REQUIRED CONFIG)`，absl 已是 Cedar 的传递依赖，无需引入新库。

### 3.2 内存反转构建（Memory Reversal）

这是方案的核心不变性（invariant）。当 GCN 从 Storage Node 拉取到单向事件流时：

```
事件: [CREATE, src=A, dst=B, type=KNOWS, t=1000]

动作 1: 在 A 的 out_chunk_tail 上追加 TMVEdge{B, 1000, MAX, ...}
动作 2: 在 B 的 in_chunk_tail  上追加 TMVEdge{A, 1000, MAX, ...}
        （这一步在 GCN 内存中完成，不写入 Storage）
```

**关键保证：** 入边遍历在 GCN 内部变成纯内存操作，无需跨网络。Storage Node 始终保持单向、无冗余的 32B 事件存储。

### 3.3 时态折叠（Temporal Folding）

GCN 内存中**不存储**线性的 CREATE/DELETE 事件对，而是在 `ScanAtTime(t_query)` 时实时折叠：

1. **Chunk 跳过：** 若 `chunk.max_valid_to < t_query` 或 `chunk.min_valid_from > t_query`，整 chunk 跳过。
2. **SIMD 区间判定：** 对候选 chunk，使用 AVX2 并行判定 `valid_from <= t_query && t_query < valid_to`。
3. **折叠去重：** 对同一 `target_id`，只保留最新的有效边（若其 `valid_to` 为 MAX 或 `> t_query`）。

折叠结果可缓存为 **Ephemeral CSR**（供当前查询使用），查询结束后丢弃。

---

## 4. GCN 进程模型与内部服务架构

### 4.1 线程架构

```
┌─────────────────────────────────────────────────────────────┐
│                     graphcomputenode 进程                    │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ IO Thread 1 │  │ IO Thread 2 │  │ IO Thread N (brpc)  │  │
│  │ (brpc bthread│  │             │  │                     │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         │                │                    │             │
│         ▼                ▼                    ▼             │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │            Query Dispatcher (bthread 协程池)             │ │
│  │   • 接收 TraversalRequest                                │ │
│  │   • 查询本地 TMVIndex（VertexEntry 是否在内存）            │ │
│  │   • 命中 → 提交给 Local Compute Thread                   │ │
│  │   • 未命中 → 提交给 Bootstrap Worker / Scatter-Gather    │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              │                              │
│         ┌────────────────────┼────────────────────┐         │
│         ▼                    ▼                    ▼         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │ Local Compute│    │Bootstrap    │    │ Scatter-Gather  │  │
│  │ Thread Pool │    │Worker Thread│    │  Router Thread  │  │
│  │ (per-NUMA)  │    │ (Chunked)   │    │  (Async RPC)    │  │
│  │             │    │             │    │                 │  │
│  │ • SIMD Scan │    │ • 向 SN 发  │    │ • 子查询打包    │  │
│  │ • BFS/DFS   │    │   RPC 拉取  │    │ • 向 GCN-2 发送 │  │
│  │ • 算法执行  │    │ • 内存反转  │    │ • 结果 Gather   │  │
│  └─────────────┘    └─────────────┘    └─────────────────┘  │
│                              │                              │
│         ┌────────────────────┼────────────────────┐         │
│         ▼                    ▼                    ▼         │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              TMV Engine (共享内存区)                     │ │
│  │   • TMVIndex (SwissTable)                               │ │
│  │   • NUMA Arena Pool (per-socket HugePages)              │ │
│  │   • Chunk Linkage (VertexEntry → TMVChunk)              │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              │                              │
│                              ▼                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │           CDC / Log Tail Thread (后台)                   │ │
│  │   • 监听 Raft Learner Stream 或 SN Push                 │ │
│  │   • 无锁 append 到 active TMVChunk                      │ │
│  └─────────────────────────────────────────────────────────┘ │
│                              │                              │
│                              ▼                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │           GC Thread (后台，可配置间隔)                    │ │
│  │   • 根据全局 Watermark 断开陈旧 Chunk 链表               │ │
│  │   • 归还 Arena Pool，无数据拷贝                          │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 GCN 服务接口（brpc）

```cpp
// === include/cedar/gcn/gcn_service.h ===
class GcnServiceImpl : public cedar::gcn::GcnService {
 public:
  // 主入口：客户端或 Cypher 引擎发起遍历请求
  void Traverse(::google::protobuf::RpcController* controller,
                const TraversalRequest* request,
                TraversalResponse* response,
                ::google::protobuf::Closure* done) override;

  // GCN 间内部协议：子查询派发（Scatter-Gather）
  void SubQuery(::google::protobuf::RpcController* controller,
                const SubQueryRequest* request,
                SubQueryResponse* response,
                ::google::protobuf::Closure* done) override;

  // Coordinator 推送：缓存失效或拓扑更新通知
  void OnCacheInvalidate(::google::protobuf::RpcController* controller,
                         const CacheInvalidateNotice* request,
                         Empty* response,
                         ::google::protobuf::Closure* done) override;

  // SN CDC 推送：增量事件（Raft Learner 模式）
  void OnEventStream(::google::protobuf::RpcController* controller,
                     const EventStream* request,
                     Ack* response,
                     ::google::protobuf::Closure* done) override;
};
```

### 4.3 内存预算与 NUMA 亲和

GCN 作为独立进程，启动时需向系统声明内存预算（如 `--memory_budget=256GB`）。TMV Engine 内部将预算按 NUMA Socket 分片：

```cpp
// === include/cedar/gcn/numa_arena.h ===
class NumaArenaPool {
 public:
  explicit NumaArenaPool(size_t total_budget_bytes);
  TMVChunk* AllocChunk(int numa_node);
  TMVVertexEntry* AllocVertexEntry(int preferred_numa_node);

 private:
  std::vector<std::unique_ptr<ArenaPool>> per_numa_pools_;
};
```

**绑定策略：** 查询调度器根据 `entity_id` 的哈希值将请求路由到固定的 Local Compute Thread，该线程绑定到特定 CPU core，其访问的 `TMVChunk` 优先从该 core 所属 NUMA node 的 Arena 分配，最大化本地内存访问。


---

## 5. 分布式 Scatter-Gather 协议与 Coordinator 元数据



---

## 5. 分布式 Scatter-Gather 协议与 Coordinator 元数据

### 5.1 Coordinator 的 VertexLocationTable

Coordinator（扩展 metad 或独立服务）维护一张全局表，记录哪个 EntityID 的哪个时间窗口当前缓存在哪个 GCN：

```cpp
// === include/cedar/coordinator/location_table.h ===
struct CacheWindow {
    uint64_t entity_id;
    uint64_t cached_from;      // 缓存的时间起点
    uint64_t cached_to;        // 缓存的时间终点（通常到 MAX/水位线）
    std::string gcn_node_id;   // 如 "gcn-192.168.1.10:9777"
    uint64_t version;          // 缓存版本号（用于强一致性校验）
    std::chrono::steady_clock::time_point expire_at;  // TTL
};

class VertexLocationTable {
 public:
  // 查询 entity_id 在指定时间点的缓存位置
  std::optional<CacheWindow> Locate(uint64_t entity_id, uint64_t query_time) const;
  
  // GCN 上报：我已完成 Bootstrap，缓存了 [from, to]
  Status ReportCache(const std::string& gcn_node_id, const CacheWindow& window);
  
  // GCN 心跳：续约或批量上报
  Status Heartbeat(const std::string& gcn_node_id, 
                   const std::vector<CacheWindow>& windows);

 private:
  absl::flat_hash_map<uint64_t, std::vector<CacheWindow>> table_;
  mutable absl::Mutex mutex_;
};
```

关键设计：Coordinator 不存储实际拓扑数据，只存储指针式元数据。表项可容忍丢失——若 Coordinator 重启或表项过期，GCN 直接回退到向 Storage Node 拉取（Bootstrap）。

### 5.2 查询路由决策流程

```
TraversalRequest 到达 GCN-1
        |
        V
+-------------------+
| 1. 解析起始点 A   |  （从请求中提取 root_entity_id + time_window）
|    和时间窗口 T   |
+---------+---------+
          V
+-------------------+
| 2. 检查本地缓存   |  TMVIndex::Find(A)
|    A 是否在内存？ |
+---------+---------+
          |
    +-----+-----+
    V           V
   命中        未命中
    |           |
    V           V
+---------+  +-----------------+
| 本地遍历 |  | 3. 问 Coordinator|
| A->B    |  |    Locate(A, T) |
+----+----+  +--------+--------+
     |                 |
     |            +----+----+
     |            V         V
     |         其他GCN    无缓存
     |         缓存了A    （Coordinator无记录）
     |            |         |
     |            V         V
     |    +----------+  +--------------+
     |    | 向 GCN-2 |  | 向 SN 发起   |
     |    | 请求 A   |  | Bootstrap    |
     |    | 的数据   |  | 拉取 A 的日志 |
     |    +----+-----+  +------+-------+
     |         |               |
     +---------+---------------+
                 |
                 V
     +-----------------------+
     | 4. 内存中完成 A->B    |
     |    （SIMD 区间判定）   |
     +-----------+-----------+
                 |
                 V
     +-----------------------+
     | 5. B 的下一跳？       |
     |    检查 B 是否在本地  |
     +-----------+-----------+
                 |
       +---------+---------+
       V                   V
      命中               未命中
       |                   |
       V                   V
   B->C 本地完成     重复步骤 3
   （继续递归）      （Scatter 到 GCN-N）
```

### 5.3 GCN 间 SubQuery 协议

```protobuf
// === proto/gcn_service.proto (新增) ===
message SubQueryRequest {
  uint64 trace_id = 1;           // 分布式追踪
  uint64 parent_gcn_id = 2;      // 发起方 GCN
  uint64 root_entity_id = 3;     // 遍历根节点
  uint64 current_entity_id = 4;  // 当前所在节点
  uint64 query_time = 5;         // 时态查询点
  uint32 remaining_hops = 6;     // 剩余跳数
  repeated uint64 visited_path = 7;  // 已访问路径（防环）
  bytes algorithm_context = 8;   // 算法上下文（BFS 队列 / DFS 栈 / PageRank 值）
}

message SubQueryResponse {
  repeated PathFragment paths = 1;
  uint32 processed_hops = 2;
  bool truncated = 3;            // 是否因跳数限制截断
}
```

协程挂起与恢复：GCN-1 在遍历到边界节点时，通过 brpc 的 bthread 机制异步发送 SubQuery，当前遍历协程挂起等待。GCN-2 在本地 TMV 上完成子遍历后返回结果，GCN-1 的协程恢复并继续汇聚。

### 5.4 强一致性模型（第一版核心）

GCN 缓存的拓扑必须与 Storage Node 的已提交状态保持一致：

1. **版本号传播：** 每个 Storage Node 的 Partition 维护一个单调递增的 commit_version（来自 Raft Log Index）。所有事件写入时携带该版本号。
2. **Bootstrap 带版本：** GCN 向 SN 拉取数据时，同时获取 max_committed_version。Coordinator 记录 (entity_id, cached_version, gcn_node_id)。
3. **路由检查：** Coordinator Locate() 返回的 CacheWindow 包含 cached_version。若客户端请求 required_version > cached_version，Coordinator 拒绝路由到该 GCN，触发 GCN 的增量同步（而非全量 Bootstrap）。
4. **CDC 顺序保证：** Storage Node 通过 Raft Learner 向 GCN 推送增量事件时，严格按 commit_version 顺序推送。GCN 维护 applied_version，只应用连续的版本，乱序到达的事件缓存在重排序缓冲区。


---

## 6. 增量保鲜、GC 与容错

### 6.1 Log Tailing & 增量同步（强一致性路径）

Storage Node 的每个 Partition 在 Raft 提交日志后，除了向 Follower 复制，还向订阅的 GCN 推送 CDC 事件流：

```cpp
// === include/cedar/gcn/event_applier.h ===
struct CDCEvent {
    uint64_t commit_version;   // Raft log index，单调递增
    uint64_t entity_id;
    uint64_t target_id;
    uint32_t valid_from;
    uint32_t valid_to;
    uint16_t edge_type;
    uint8_t  op;               // CREATE / DELETE
};

class EventApplier {
 public:
  // 按 commit_version 严格顺序应用
  Status ApplyOrdered(const CDCEvent& event);
  
  // 重排序缓冲区：处理网络乱序到达的事件
  Status ApplyUnordered(const CDCEvent& event);

 private:
  std::atomic<uint64_t> applied_version_{0};
  absl::Mutex reorder_mutex_;
  absl::flat_hash_map<uint64_t, CDCEvent> reorder_buffer_;
};
```

**应用路径（无锁追加）：**
1. 根据 entity_id 哈希定位到 TMVIndex 分片。
2. 找到 TMVVertexEntry 的 active_out_chunk_tail。
3. TMVChunk::Append() 使用 fetch_add 获取写入槽位，写入 TMVEdge。
4. 若 op == DELETE，查找对应 CREATE 边并回填 valid_to。

### 6.2 O(1) 水位线淘汰（Chunk-based Drop）

GC 线程周期性执行：

```cpp
class WatermarkGc {
 public:
  void SetGlobalWatermark(uint64_t watermark_time);
  size_t DropExpiredChunks(TMVIndex* index);

 private:
  std::atomic<uint64_t> watermark_{0};
};
```

**关键不变性：** 淘汰时**只修改指针**，不拷贝数据，不遍历边。被断开的 TMVChunk 直接归还 NumaArenaPool 的 free-list。

```cpp
// 伪代码：O(1) 淘汰
for each shard in TMVIndex:
    for each entry in shard:
        TMVChunk* head = entry.out_chunk_head.load();
        while (head && head->max_valid_to < watermark) {
            TMVChunk* next = head->next;
            entry.out_chunk_head.store(next);
            arena_pool_.Free(head);  // 归还，无拷贝
            head = next;
        }
```

### 6.3 GCN 容错与重建

| 故障场景 | 恢复策略 |
|----------|----------|
| **GCN 进程崩溃** | Coordinator 心跳超时（默认 5s），标记该 GCN 所有缓存为失效。新查询回退到 Bootstrap。GCN 重启后内存为空，按需重建。 |
| **GCN 网络分区** | Coordinator 通过 Raft 元数据集群判断 GCN 是否存活。分区 GCN 的缓存被隔离，避免脑读。 |
| **Storage Node 不可用** | GCN 依赖本地缓存继续服务读请求（若缓存窗口覆盖查询时间）。写请求阻塞直至 SN 恢复。 |
| **Coordinator 不可用** | GCN 本地缓存保留最近知道的元数据快照，进入**自治模式**：继续服务本地命中查询，未命中则广播查询所有已知 GCN（最后手段）。 |



---

## 7. 与现有 Cedar 系统的对接边界

方案 B 是完整重构，但必须明确与现有代码的**切割点**和**复用点**。

### 7.1 完全替换（现有代码废弃）

| 现有模块 | 处理方式 | 理由 |
|----------|----------|------|
| `src/compute/arena_pool.cc` | **废弃** | 新 GCN 使用 `NumaArenaPool` |
| `src/compute/temporal_graph_view.cc` | **废弃** | 替换为 `TMVEngine` |
| `src/compute/temporal_query_engine.cc` | **废弃** | SIMD 扫描内嵌到 `LocalComputeThread` |
| `src/compute/gc_thread.cc` | **废弃** | 替换为 `WatermarkGc` |
| `src/compute/request_coalescer.cc` | **部分复用** | 理念保留，但实现升级为跨 GCN RPC 去重 |

### 7.2 直接复用（保持不变或微小扩展）

| 现有模块 | 复用方式 |
|----------|----------|
| `src/storage/lsm_engine.cc` | **扩展** `GetRangeForCompute()` 批量端点，新增 `GetCommittedVersion()` 接口 |
| `src/storage/cedar_graph_storage.cc` | **复用** `CedarKey` 编码、`PutEdge`/`GetEdge` 作为 SN 内部实现 |
| `src/raft/partition_raft_group.cc` | **复用** Raft 核心，新增 Learner 角色向 GCN 推送 CDC |
| `proto/query_service.proto` | **扩展** 新增 `SubQuery` 消息，保留现有查询语义 |
| `src/cypher/cypher_engine.cc` | **适配** 不再直接调 `storage_`，改为通过 brpc 发送 `TraversalRequest` 到 GCN |

### 7.3 新增文件清单

```
include/cedar/gcn/
  tmv_edge.h
  tmv_chunk.h
  tmv_vertex_entry.h
  tmv_index.h
  tmv_engine.h
  numa_arena.h
  event_applier.h
  watermark_gc.h
  gcn_service.h
  gcn_node.h
  query_dispatcher.h
  local_compute_thread.h
  scatter_gather_router.h

src/gcn/
  gcn_service.cc
  gcn_node.cc
  tmv_engine.cc
  numa_arena.cc
  event_applier.cc
  watermark_gc.cc
  query_dispatcher.cc
  local_compute_thread.cc
  scatter_gather_router.cc

proto/
  gcn_service.proto

tools/
  graphcomputenode.cc
```

### 7.4 构建系统对接

在 `CMakeLists.txt` 中新增 `gcn` 库和 `graphcomputenode` 二进制目标。依赖：
- `brpc`（RPC 框架，已存在）
- `braft`（若 GCN 内部需要轻量 Raft 协调，可选）
- `absl::flat_hash_map`（通过 brpc 传递依赖）
- `libnuma`（NUMA 亲和，可选，运行时检测）



---

## 8. 对比传统方案（Why not NebulaGraph?）

在 VLDB 的叙事体系中，Cedar TMV 图计算层通过以下维度凸显先进性：

| 维度 | NebulaGraph（及类似系统） | Cedar TMV 图计算层 |
|------|---------------------------|-------------------|
| **计算节点状态** | Stateless（无状态，只做查询翻译和计算） | **Stateful**（带状态，维护时间窗口拓扑视图） |
| **底层存储模型** | 点边分离，冗余存储（出边存一份，入边存一份） | **单向追加**，无冗余 32B 事件流 |
| **时态查询代价** | 极高（需扫描多个版本，就地更新导致碎片） | **极低**（连续内存 SIMD 扫描，区间判定） |
| **高并发写入** | 容易触发锁和分布式事务瓶颈 | **纯粹的 LSM-tree Append**，写吞吐无上限 |
| **跨节点遍历** | 每次跳都需跨网络查 Storage | **内存反转构建**，反向遍历纯本地 |
| **GC 与淘汰** | 需要复杂的多版本压缩（Compaction） | **O(1) Chunk-based Drop**，无数据拷贝 |

**核心差异的本质：** NebulaGraph 将图计算视为"无状态的查询翻译层"，每次查询都要重新从存储层组装拓扑；而 Cedar TMV 将图计算视为"带状态的物化视图层"，拓扑在内存中被预先折叠为遍历友好的 CSR 结构，查询变为纯粹的内存计算。

---

## 9. 核心工作流场景

### 9.1 场景一：冷启动与按需物化（Bootstrap Materialization）

**触发：** 用户请求"变压器 TX-1 在过去一天的级联故障路径"。

1. **路由决策：** 请求到达任意 GCN（如 GCN-1），该 GCN 询问 Coordinator 发现 TX-1 的近期拓扑不在任何节点的内存中。
2. **批量拉取：** GCN-1 根据 `Hash(TX-1)` 向对应的 Storage Node 发起 RPC，要求拉取过去一天的 32B 事件块，并获取 `max_committed_version`。
3. **折叠建树：** GCN-1 收到连续的事件日志后，执行"内存反转构建"，将其物化为 `TMVVertexEntry` 和 `TMVChunk`。
4. **上报元数据：** GCN-1 向 Coordinator 上报 `CacheWindow{TX-1, t-24h, t_now, version, ...}`。

### 9.2 场景二：跨节点时态遍历（Distributed Scatter-Gather）

**触发：** 图遍历算法（如 BFS）在 GCN-1 内部沿时间线游走。

1. **本地穿梭：** 算法在 `TMVChunk` 数组中飞速迭代。利用 CPU AVX2 指令验证 `valid_from <= t_query < valid_to`，进行纳秒级的有效边判定。
2. **触发降级（Miss Fallback）：** 遍历到边界节点 C 时，发现 C 的下一跳拓扑不在 GCN-1 的内存中。
3. **子查询派发：** GCN-1 挂起当前协程，将查询上下文打包，向 Coordinator 查询 C 的位置。若 C 缓存在 GCN-2，则向 GCN-2 发送 `SubQuery`；若未缓存，则向 SN 发起 Bootstrap。
4. **结果汇聚：** GCN-2 在本地内存跑完后续路径，将结果（如故障影响范围）Gather 回 GCN-1。

### 9.3 场景三：增量保鲜与 O(1) 淘汰（Log Tailing & GC）

**保鲜：** 底层 Storage Node 发生新的事件追加时，利用 Raft Learner CDC 流，将 `[t_new, A, B, CREATE]` 推送给对应的 GCN。GCN 的 `EventApplier` 执行无锁的内存追加。

**淘汰：** 后台 `WatermarkGc` 线程根据全局水位线，直接断开陈旧 `TMVChunk` 的指针，归还 `NumaArenaPool`（Chunk-based Drop），没有任何数据拷贝。

---

## 10. 总结

这套方案通过双重解耦（物理拓扑与逻辑视图解耦、属性与拓扑内存隔离），让 Cedar 能够兼顾 Event Sourcing 的极致写吞吐与 In-Memory Graph 的极致读遍历。

**可作为 VLDB 论文 Core Architecture 章节的主打卖点：**
- **Temporal Folding Engine（时态折叠引擎）：** 将线性事件流实时折叠为具有生命周期的 CSR 拓扑。
- **Epoch-based CSR：** 基于 `TMVChunk` 的 O(1) 淘汰与 NUMA-aware 内存管理。
- **Memory Reversal：** 在 GCN 内存中无冗余地构建双向拓扑，彻底消除跨网络反向遍历。
- **Strongly-Consistent CDC:** 基于 Raft Learner 的增量保鲜，保证物化视图与存储层强一致。

---

## 附录 A：关键术语表

| 术语 | 定义 |
|------|------|
| **TMV** | Temporal Materialized View，时态物化视图 |
| **GCN** | Graph Compute Node，图计算节点（独立进程） |
| **SN** | Storage Node，存储节点（LSM-tree） |
| **TMVEdge** | 32B 时态边结构，包含 target_id、valid_from、valid_to、attr_offset |
| **TMVChunk** | 约 1MB 的连续内存块，存储 65536 条 TMVEdge，GC 的基本单元 |
| **Memory Reversal** | GCN 在内存中为单向事件主动构建反向边的机制 |
| **Scatter-Gather** | 跨 GCN 的分布式子查询派发与结果汇聚协议 |
| **Bootstrap** | GCN 从 SN 按需拉取事件日志并物化为内存拓扑的过程 |

