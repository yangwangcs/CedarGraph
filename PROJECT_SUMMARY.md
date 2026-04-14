# CedarGraph-Core 项目完整总结

> **分布式时序图存储系统** - 高性能、列式存储、乐观并发控制

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]() [![C++17](https://img.shields.io/badge/C++-17-blue.svg)](https://isocpp.org/) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

---

## 1. 项目概述

**CedarGraph-Core** 是一个专为**时序属性图**设计的高性能分布式存储系统。它将顶点和边统一编码为 32 字节定长事件流，采用 LSM-Tree 架构和乐观并发控制（OCC），实现了：

- **高吞吐写入**: 14μs 延迟，适合高频时序数据摄入
- **高效时序查询**: 原生支持 `AS OF` 点查询和 `BETWEEN` 范围扫描
- **极致存储效率**: 比传统方案节省 87% 存储空间
- **事务安全**: 快照隔离级别 + 自动重试机制

### 1.1 适用场景

- 物联网时序图谱（设备状态变化追踪）
- 金融交易网络（交易时序分析）
- 社交网络历史分析（用户行为时序）
- 供应链溯源（全生命周期追踪）

---

## 2. 核心架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API Layer                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  gRPC API   │  │ Cypher API  │  │ Driver API  │  │  Batch API  │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Query Engine                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ CypherParser│  │ExecutionPlan│  │ CedarScan   │  │ Pushdown    │        │
│  │             │  │             │  │ (AS OF /    │  │ Predicate   │        │
│  │             │  │             │  │ BETWEEN)    │  │             │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Transaction Layer (OCC)                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Session   │  │ManagedTxn   │  │OCCTransaction│  │  WAL Writer │        │
│  │             │  │  (RAII)     │  │             │  │ (Group      │        │
│  │ - Bookmark  │  │             │  │ - ReadSet   │  │  Commit)    │        │
│  │ - Retry     │  │             │  │ - WriteSet  │  │             │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │              TransactionManager (ShardedTimestampAllocator)          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Storage Engine (LSM-Tree)                            │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         Memory Layer                                  │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────┐  │  │
│  │  │  VSLMemTable │  │CedarMemTable │  │   TemporalStorageLayer     │  │  │
│  │  │ (Version     │  │ (Event       │  │   (Unified Interface)      │  │  │
│  │  │  Chain Skip  │  │  Buffer)     │  │                            │  │  │
│  │  │  List)       │  │              │  │                            │  │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                    │ Flush (64MB threshold)                 │
│                                    ▼                                       │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         Disk Layer (SST v2)                           │  │
│  │  ┌────────────────────────────────────────────────────────────────┐  │  │
│  │  │              Zone-Columnar Format                               │  │  │
│  │  │  ┌─────────┬─────────┬─────────┬─────────┬─────────────────┐   │  │  │
│  │  │  │ Zone 0  │ Zone 1  │ Zone 2  │ Zone 3  │    Zone 4       │   │  │  │
│  │  │  │EntityID │Timestamp│TargetID │Metadata │     Values      │   │  │  │
│  │  │  │  (RLE)  │(Delta²) │(Delta)  │(RLE+Bitmap)│(Dict+LZ4)    │   │  │  │
│  │  │  └─────────┴─────────┴─────────┴─────────┴─────────────────┘   │  │  │
│  │  │                    + Blob Files (>6B values)                    │  │  │
│  │  └────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                              │
│  │  Components: SSTBuilder, SSTReader, BlockCache, BloomFilter, ZoneMaps      │
│  │  Compaction: ZLM (Zone-Level Merge) - Differential processing              │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 数据模型层

### 3.1 CedarKey (32 字节定长)

统一编码格式，64B 缓存行对齐：

| 字段 | 大小 | 说明 |
|------|------|------|
| `entity_id` | 8B | 顶点 ID / 边源 ID |
| `timestamp` | 8B | 微秒级时间戳（降序存储） |
| `target_id` | 8B | 边目标 ID / 扩展数据 |
| `column_id` | 2B | 属性 ID / 边类型 ID |
| `sequence` | 2B | MVCC 版本序列号 |
| `type+flags` | 2B | 实体类型（顶点/边/属性）+ 标志位 |
| `reserved` | 2B | 保留用于未来扩展 |

### 3.2 Descriptor（值描述符）

支持多种数据类型和存储方式：

```cpp
enum class EntryKind : uint8_t {
  Tombstone = 0,      // 删除标记
  InlineInt = 1,      // 内联整数（6B）
  InlineShortStr = 2, // 内联短字符串（6B）
  InlineFloat = 3,    // 内联浮点数（6B）
  BlobPtr = 4,        // Blob 文件引用
};
```

**存储策略**:
- **Inline** (≤6B): 直接存储在 Key 中
- **Blob** (>6B): 存储在独立的 `.blob` 文件，Key 中存指针

### 3.3 EntityType

```cpp
enum class EntityType : uint8_t {
  Vertex = 0,   // 顶点
  Edge = 1,     // 边
  Property = 2, // 属性
};
```

---

## 4. 存储引擎层

### 4.1 内存组件

#### VSLMemTable (Version Chain SkipList)

```
┌─────────────────────────────────────────┐
│           SkipList Index Layer          │
│  ┌─────┐   ┌─────┐   ┌─────┐          │
│  │ L3  │──►│     │──►│     │          │
│  └─────┘   └─────┘   └─────┘          │
│  ┌─────┐   ┌─────┐   ┌─────┐          │
│  │ L2  │──►│     │──►│     │          │
│  └─────┘   └─────┘   └─────┘          │
│  ┌─────┐   ┌─────┐   ┌─────┐          │
│  │ L1  │──►│     │──►│     │          │
│  └─────┘   └─────┘   └─────┘          │
│  ┌─────┐   ┌─────┐   ┌─────┐          │
│  │ L0  │──►│     │──►│     │          │
│  └──┬──┘   └──┬──┘   └──┬──┘          │
│     │         │         │              │
│     ▼         ▼         ▼              │
│  ┌─────────────────────────────────┐  │
│  │      Version Data Layer         │  │
│  │  ┌──────┐    ┌──────┐          │  │
│  │  │Ver N │───►│VerN-1│───►...   │  │
│  │  │(New) │    │      │          │  │
│  │  └──────┘    └──────┘          │  │
│  └─────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

**特性**:
- **Lock-free**: 无锁跳表，高并发写入
- **Vertical Version Chain**: 同一实体的不同版本纵向链接
- **O(1) Latest Lookup**: 最新版本在链头，常数时间访问
- **O(log N + V') Time-Travel**: 时序查询复杂度

#### CedarMemTable

事件缓冲区，支持：
- 批量写入优化
- 自动 Flush 阈值（4MB）
- 版本链构建

### 4.2 磁盘格式 (SST v2)

#### Zone-Columnar 布局

每个 SST 文件分为 5 个语义区域：

| Zone | 内容 | 编码 | 用途 |
|------|------|------|------|
| Zone 0 | Entity IDs | RLE (Run-Length Encoding) | 快速实体查找 |
| Zone 1 | Timestamps | Delta-of-Delta | 时序范围查询 |
| Zone 2 | Target IDs | Delta / RLE | 图遍历优化 |
| Zone 3 | Metadata | RLE + Bitmap | 删除标记检查 |
| Zone 4 | Values | Dictionary + LZ4 | 延迟物化 |

**辅助结构**:
- **Zone Maps**: 每列的最小/最大值，用于文件裁剪
- **Bloom Filter**: 负查询优化
- **Restart Points**: 每 8192 行一个，支持二分查找
- **Index Pointer**: 指向 footer 和 meta block

#### Blob 存储

大值（>6B）分离存储：
- 独立的 `.blob` 文件
- 引用计数管理
- 垃圾回收（BlobGCManager）
- L0→L1 压缩时零拷贝转发

### 4.3 压缩策略 (ZLM)

**Zone-Level Merge (区域级合并)**:

```
L0 → L1 Compaction:
┌─────────────────────────────────────────────────────────┐
│  Zone 0,2 (Topology)    │ K-way Merge + RLE optimization│
│  Zone 1 (Temporal)      │ Delta-of-Delta re-encoding    │
│  Zone 3 (Metadata)      │ Bitmap consolidation          │
│  Zone 4 (Property)      │ Dictionary rebuild + LZ4      │
│  Blob Files             │ Reference forwarding          │
└─────────────────────────────────────────────────────────┘
```

**特性**:
- **语义感知**: 不同 Zone 采用差异化压缩策略
- **零拷贝 Blob**: 大值在压缩时通过引用转发，避免复制
- **并行压缩**: ParallelCompactionEngine 多线程处理

---

## 5. 事务系统层 (OCC)

### 5.1 乐观并发控制

```
Transaction Lifecycle:
┌─────────┐    ┌─────────┐    ┌───────────┐    ┌─────────┐
│  Begin  │───►│ Execute │───►│ Validate  │───►│ Commit  │
│         │    │ (R/W)   │    │ (OCC)     │    │         │
└─────────┘    └─────────┘    └───────────┘    └─────────┘
                                    │
                                    ▼ (conflict)
                              ┌─────────┐
                              │  Abort  │
                              └─────────┘
```

**OCC 三阶段**:
1. **执行阶段**: 记录读集（ReadSet）和写集（WriteSet）
2. **验证阶段**: 检查读集是否被其他事务修改
3. **提交阶段**: 写 WAL，更新 MemTable

### 5.2 隔离级别

```cpp
enum class IsolationLevel : uint8_t {
  kReadUncommitted = 0,  // 读未提交
  kReadCommitted = 1,    // 读已提交
  kSnapshot = 2,         // 快照隔离（默认）
  kSerializable = 3,     // 串行化
};
```

### 5.3 时间戳分配

**ShardedTimestampAllocator (分片式分配器)**:

```
┌──────────────────────────────────────────┐
│         Timestamp Allocator              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐    │
│  │ Shard 0 │ │ Shard 1 │ │ Shard N │    │
│  │ (Local) │ │ (Local) │ │ (Local) │    │
│  │ Counter │ │ Counter │ │ Counter │    │
│  └────┬────┘ └────┬────┘ └────┬────┘    │
│       └───────────┴───────────┘          │
│                   │                      │
│            Global Epoch                  │
└──────────────────────────────────────────┘
```

**特性**:
- **无锁分配**: 每个线程绑定到一个 shard，本地计数器
- **批量分配**: 减少原子操作次数
- **Epoch 管理**: 定期同步全局时间

### 5.4 WAL (Write-Ahead Logging)

**组提交优化**:
- 多个事务批量写入 WAL
- 减少 fsync 次数
- WalBatchWriter 异步处理

---

## 6. 驱动层 API (Neo4j-Style)

### 6.1 架构简化

```
Before (多层封装):                    After (简化):
┌─────────────┐                       ┌─────────────┐
│   Session   │                       │   Session   │
└──────┬──────┘                       └──────┬──────┘
       │                                     │
       ▼                                     ▼
┌─────────────────┐                   ┌─────────────┐
│ExplicitTransaction│                 │ ManagedTxn  │
│  (隐藏OCCTransaction)│               │ (RAII only) │
└────────┬────────┘                   └──────┬──────┘
         │                                   │
         ▼                                   ▼
┌─────────────────┐                   ┌─────────────┐
│  OCCTransaction │                   │OCCTransaction│
│   (Native API)  │                   │(Direct Access)│
└─────────────────┘                   └─────────────┘
```

### 6.2 Session (事务容器)

```cpp
class Session {
 public:
  // L3: 显式事务
  ManagedTxn BeginTransaction(const TransactionConfig& config = {});
  ManagedTxn BeginTransaction(const Bookmark& bookmark, 
                               const TransactionConfig& config = {});
  
  // L2: 托管事务（自动重试）
  template<typename Func>
  BookmarkResult ExecuteWrite(Func&& func, 
                               const TransactionConfig& config = {},
                               const RetryConfig& retry_config = {});
  
  // 书签管理（因果一致性）
  Bookmark GetLastBookmark() const;
  void UpdateBookmark(const Bookmark& bookmark);
};
```

### 6.3 ManagedTxn (RAII 包装)

```cpp
class ManagedTxn {
 public:
  // 直接暴露 OCCTransaction
  OCCTransaction* operator->() { return txn_.get(); }
  OCCTransaction& operator*() { return *txn_; }
  
  // 提交并返回书签
  BookmarkResult Commit();
  
  // 显式回滚
  void Rollback();
  
  // RAII：析构时自动回滚（如果未提交）
  ~ManagedTxn();
};
```

### 6.4 Bookmark (因果一致性)

**格式**: `v2:{timestamp}:{txn_id}`

```cpp
class Bookmark {
 public:
  Bookmark(uint64_t timestamp, uint64_t txn_id);
  
  // 序列化/反序列化
  std::string ToString() const;           // "v2:123456789:987654321"
  static std::optional<Bookmark> FromString(const std::string& str);
  
  // 因果排序（基于 timestamp）
  bool operator>(const Bookmark& other) const;  // IsAfter
  bool operator<(const Bookmark& other) const;
  
  // 从 OCCTransaction 创建
  static Bookmark FromOCCTransaction(const OCCTransaction& txn);
};
```

**用途**: 跨会话因果一致性保证
```cpp
Session s1(...);
auto txn1 = s1.BeginTransaction();
// ... 写入数据 ...
auto bm = txn1.Commit().value();  // 获取书签

Session s2(...);
auto txn2 = s2.BeginTransaction(bm);  // 保证看到 s1 的写入
```

### 6.5 RetryPolicy (自动重试)

```cpp
// 错误分类
class ErrorClassifier {
 public:
  static bool IsRetryable(const Status& status);
  // OCC Conflict, Lock Busy → Retryable
  // InvalidArgument, NotFound → Non-retryable
};

// 重试策略
struct RetryConfig {
  int max_attempts = 3;
  BackoffStrategy backoff = BackoffStrategy::kExponential;
  std::chrono::milliseconds base_delay{10};
  std::chrono::milliseconds max_delay{1000};
};

// 预置策略
namespace RetryPolicies {
  RetryConfig Aggressive();    // 快速重试（3次，指数退避）
  RetryConfig Conservative();  // 保守重试（5次，线性退避）
  RetryConfig NoRetry();       // 不重试
}
```

---

## 7. 查询引擎层

### 7.1 CedarScan (时序扫描)

支持两种核心查询模式：

```cpp
// AS OF: 查询特定时间点的状态
auto result = storage->Get(entity_id, Timestamp(1700000000000000));

// BETWEEN: 查询时间范围内的历史
auto results = scan->ScanRange(entity_id, start_ts, end_ts);
```

**实现机制**:
1. 利用 VCSL 的版本链垂直遍历
2. 结合 SST 的 Zone 1 (Timestamp) 进行文件裁剪
3. 使用 Zone Maps 进行范围过滤

### 7.2 Cypher 支持

```cpp
CypherEngine engine;

// 时序查询示例
auto result = engine.Execute(
  "MATCH (n:Device) WHERE n.status = 'active' AS OF 1700000000000000"
);

// 范围查询
auto result = engine.Execute(
  "MATCH (n) RETURN n BETWEEN 1700000000000000 AND 1700000100000000"
);
```

**执行流程**:
```
Cypher Query → Parser → ExecutionPlan → TemporalOperators → CedarScan → Results
```

---

## 8. 性能指标

### 8.1 与 Aion (Neo4j-based) 对比

测试数据集：674M 时序记录（RE-Europe 数据集）

| 指标 | Cedar | Aion | 提升倍数 |
|------|-------|------|---------|
| **存储空间** | 6.98 GB | 54.97 GB | **7.9x** |
| **写入延迟** | 14 μs | 200 μs | **14x** |
| **AS OF 查询** | 0.8 ms | 6 ms | **7.5x** |
| **BETWEEN 查询** | 56 ms | 65 ms | 1.2x |
| **时序 BFS (深度3)** | 270 ms | 890 ms | **3.3x** |

### 8.2 内部组件性能

| 组件 | 性能指标 |
|------|---------|
| VCSL 写入 | ~100ns / op (单线程) |
| 时间戳分配 | ~20ns / timestamp (无锁) |
| SST 读取 | ~5μs / block (缓存命中) |
| ZLM 压缩 | 200MB/s (多线程) |

---

## 9. 项目结构

```
CedarGraph-Core/
├── include/cedar/                  # 公共头文件
│   ├── api/                        # 外部 API（legacy）
│   ├── core/                       # 基础设施
│   │   ├── status.h                # Status 码
│   │   ├── slice.h                 # Slice（零拷贝字符串）
│   │   ├── env.h                   # 环境抽象
│   │   └── crc32c.h                # CRC32C 校验
│   ├── types/                      # 核心类型
│   │   ├── cedar_key.h             # CedarKey (32B)
│   │   ├── descriptor.h            # Descriptor 值描述符
│   │   └── cedar_types.h           # 基础类型定义
│   ├── storage/                    # 存储引擎
│   │   ├── lsm_engine.h            # LSM-Tree 主引擎
│   │   ├── vsl_memtable.h          # VCSL MemTable
│   │   ├── cedar_graph_storage.h   # 图存储接口
│   │   ├── temporal_storage_layer.h # 时序存储层
│   │   ├── sst_reader_cache.h      # SST 读取缓存
│   │   ├── block_cache.h           # 块缓存
│   │   ├── compaction_merger_v2.h  # ZLM 压缩
│   │   └── ...                     # 其他组件
│   ├── transaction/                # 事务系统
│   │   ├── occ_transaction.h       # OCC 事务
│   │   ├── transaction_manager_optimized.h # 事务管理器
│   │   ├── sharded_timestamp_allocator.h   # 时间戳分配器
│   │   ├── wal.h                   # 预写日志
│   │   └── batch_transaction.h     # 批量事务
│   ├── driver/                     # Neo4j 风格驱动
│   │   ├── session.h               # 会话管理
│   │   ├── bookmark.h              # 因果一致性书签
│   │   ├── retry_policy.h          # 重试策略
│   │   └── transaction_types.h     # 事务类型
│   ├── query/                      # 查询引擎
│   │   └── cedar_scan.h            # 时序扫描
│   ├── graph/                      # 图语义层
│   │   ├── cedar_graph.h           # 图接口
│   │   └── graph_semantic_layer.h  # 语义层
│   ├── cypher/                     # Cypher 查询
│   │   ├── cypher_engine.h         # Cypher 引擎
│   │   ├── parser.h                # 解析器
│   │   └── execution_plan.h        # 执行计划
│   ├── sst/                        # SST 格式
│   │   ├── zone_columnar_format.h  # Zone-Columnar 格式
│   │   ├── zone_columnar_builder.h # SST 构建器
│   │   ├── zone_columnar_reader.h  # SST 读取器
│   │   ├── bloom_filter.h          # 布隆过滤器
│   │   └── compression.h           # 压缩
│   ├── update/                     # 更新处理
│   │   └── cedar_update.h          # CedarUpdate API
│   ├── io/                         # IO 层
│   │   └── async_io.h              # 异步 IO
│   └── db/                         # 数据库层
│       ├── graph_db.h              # 图数据库
│       └── manifest.h              # Manifest 管理
├── src/                            # 实现源码（对应 include）
├── proto/                          # Protocol Buffers
│   └── cedar_graph.proto           # gRPC 服务定义
├── tests/                          # 单元测试
│   └── test_driver_transaction.cc  # 驱动层测试
├── examples/                       # 示例程序
├── docs/                           # 文档
├── cmake/                          # CMake 模块
├── CMakeLists.txt                  # 构建配置
└── README.md                       # 项目说明
```

---

## 10. 构建和使用

### 10.1 依赖

- C++17 编译器（GCC 7+, Clang 5+, MSVC 2017+）
- CMake 3.14+
- LZ4 压缩库
- Protocol Buffers + gRPC
- GoogleTest（测试可选）

**macOS**:
```bash
brew install cmake pkg-config lz4 grpc googletest
```

**Ubuntu/Debian**:
```bash
sudo apt-get install -y cmake pkg-config liblz4-dev \
    libprotobuf-dev protobuf-compiler libgrpc++-dev libgtest-dev
```

### 10.2 构建

```bash
mkdir build && cd build

# 基础构建
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# 带测试和示例
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DBUILD_TESTS=ON \
         -DBUILD_EXAMPLES=ON
make -j$(nproc)
ctest --output-on-failure
```

### 10.3 使用示例

**基础读写**:
```cpp
#include "cedar/storage/cedar_graph_storage.h"

// 打开存储
cedar::CedarOptions options;
options.create_if_missing = true;
cedar::CedarGraphStorage* storage = nullptr;
cedar::Status s = cedar::CedarGraphStorage::Open(options, "/data/cedar", &storage);

// 写入数据
cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, 42);
s = storage->Put(123ULL, 1700000000000000ULL, desc, cedar::Timestamp(1));

// AS OF 查询
auto result = storage->Get(123ULL, 1700000000000000ULL);
if (result.has_value()) {
    if (auto val = result->AsInlineInt()) {
        std::cout << "Value: " << *val << std::endl;
    }
}

delete storage;
```

**使用 Driver API（推荐）**:
```cpp
#include "cedar/driver/session.h"

// 创建会话
cedar::driver::SessionConfig config;
cedar::driver::Session session(txn_manager, memtable, lsm_engine, 
                                wal_writer, config);

// 执行写事务（自动重试）
auto result = session.ExecuteWrite([](OCCTransaction& txn) {
    Descriptor desc;
    desc.SetKind(EntryKind::InlineInt);
    desc.SetColumnId(0);
    desc.SetPayload(12345);
    
    txn.Put(1, EntityType::Vertex, 0, desc, Timestamp::Now());
    return true;  // 成功
});

if (result.ok()) {
    std::cout << "Committed at: " << result.value().ToString() << std::endl;
}

// 显式事务
auto txn = session.BeginTransaction();
txn->Put(2, EntityType::Edge, 1, desc, Timestamp::Now());
auto bm = txn.Commit();
```

---

## 11. gRPC 服务接口

```protobuf
service CedarGraphService {
    // 节点 CRUD
    rpc GetNode(GetNodeRequest) returns (Node);
    rpc PutNode(PutNodeRequest) returns (GrpcStatus);
    rpc DeleteNode(DeleteNodeRequest) returns (GrpcStatus);
    rpc QueryNodes(QueryNodesRequest) returns (NodeList);
    
    // 边 CRUD
    rpc GetEdge(GetEdgeRequest) returns (Edge);
    rpc PutEdge(PutEdgeRequest) returns (GrpcStatus);
    rpc DeleteEdge(DeleteEdgeRequest) returns (GrpcStatus);
    rpc QueryEdges(QueryEdgesRequest) returns (EdgeList);
    
    // 图遍历
    rpc GetNeighbors(GetNeighborsRequest) returns (GetNeighborsResponse);
    rpc GetInNeighbors(GetNeighborsRequest) returns (GetNeighborsResponse);
    rpc ShortestPath(ShortestPathRequest) returns (ShortestPathResponse);
    rpc Bfs(BfsRequest) returns (BfsResponse);
    
    // Cypher 查询
    rpc ExecuteCypher(CypherQueryRequest) returns (CypherQueryResponse);
    rpc ExplainCypher(CypherQueryRequest) returns (CypherQueryResponse);
    
    // 事务
    rpc BeginTransaction(BeginTransactionRequest) returns (Transaction);
    rpc Commit(CommitRequest) returns (GrpcStatus);
    rpc Rollback(RollbackRequest) returns (GrpcStatus);
    
    // 数据库管理
    rpc Flush(FlushRequest) returns (GrpcStatus);
    rpc Compact(CompactRequest) returns (GrpcStatus);
    rpc GetStats(GetStatsRequest) returns (DatabaseStats);
    
    // 服务器管理
    rpc HealthCheck(Empty) returns (HealthCheckResponse);
    rpc GetServerStats(Empty) returns (ServerStats);
}
```

---

## 12. 关键技术点总结

| 技术领域 | 核心设计 | 优势 |
|---------|---------|------|
| **数据模型** | 32B 定长 CedarKey | 统一顶点/边/属性编码，64B 缓存行对齐 |
| **内存结构** | VCSL (Version Chain SkipList) | O(1) 最新查询，O(log N + V') 时序查询 |
| **磁盘格式** | Zone-Columnar SST | 语义感知压缩，差异化处理不同数据类型 |
| **压缩策略** | ZLM (Zone-Level Merge) | 减少写放大，零拷贝 Blob 转发 |
| **事务并发** | OCC + 快照隔离 | 无锁读取，乐观写入，高吞吐 |
| **时间戳** | ShardedTimestampAllocator | 分片无锁分配，~20ns 延迟 |
| **驱动 API** | Neo4j-style Session + ManagedTxn | 简洁直观，RAII 安全，自动重试 |
| **一致性** | Bookmark (v2:timestamp:txn_id) | 跨会话因果一致性 |
| **查询优化** | Pushdown Predicate + Zone Maps | 减少 I/O，快速文件裁剪 |

---

## 13. 路线图

- [x] Core LSM-Tree 引擎
- [x] VCSL MemTable
- [x] Zone-Columnar SST v2
- [x] OCC 事务系统
- [x] Driver API (Neo4j-style)
- [x] Cypher 查询支持
- [x] gRPC 服务
- [ ] 分布式事务（2PC）
- [ ] 副本一致性（Raft）
- [ ] 查询优化器（Cost-based）
- [ ] 图算法库（PageRank, LPA 等）

---

## 14. 许可证

**Apache License 2.0**

---

*本文档总结了 CedarGraph-Core 项目的完整架构和技术实现。如需更详细的组件说明，请参考各模块的设计文档（docs/ 目录）。*
