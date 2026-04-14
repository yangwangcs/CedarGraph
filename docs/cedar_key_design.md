# CedarKey 设计规范

## 概述

**CedarKey** 是专为 CedarGraph 时序属性图存储设计的 32 字节定长键格式。它将顶点和边统一到一个键空间中，实现高效的 LSM-Tree 存储和时序查询。

在分布式场景下，通过将 `part_id`（分区 ID）引入 `reserved` 字段，CedarKey 从"单机时序索引"提升到了"分布式可扩展索引"的层面。

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **固定 32 字节长度** | 缓存行友好（每 64B 缓存行可放 2 个 Key） |
| **8 字节对齐** | SIMD 友好，支持 AVX2 优化的 `memcmp` |
| **降序时间戳** | 最新版本自然排在前面 |
| **双向边支持** | `EdgeOut` + `EdgeIn` 类型，支持 O(log N) 反向查找 |
| **MVCC 支持** | 通过 `sequence` 字段实现版本控制 |
| **分布式分区** | 2B `part_id` 支持 65536 个逻辑分片 |

---

## 内存布局

```
+--------+--------+--------+--------+--------+--------+--------+--------+
|                           entity_id (8B)                              |  +0
+--------+--------+--------+--------+--------+--------+--------+--------+
|                        timestamp_be (8B)                              |  +8
+--------+--------+--------+--------+--------+--------+--------+--------+
|                          target_id (8B)                               |  +16
+--------+--------+--------+--------+--------+--------+--------+--------+
| column_id | sequence  |entity_type| flags |        part_id           |  +24
+--------+--------+--------+--------+--------+--------+--------+--------+
    2B         2B          1B        1B            2B
```

### 字段详情

| 偏移量 | 字段 | 大小 | 说明 | 分布式语义 |
|--------|------|------|-------------|-----------|
| 0-7 | `entity_id` | 8B | 源点 ID $s$ | 路由键，决定数据所在物理节点 |
| 8-15 | `timestamp_be` | 8B | 大端序时间戳 $t$ | 降序存储，支持时序查询 |
| 16-23 | `target_id` | 8B | 目标点 ID $o$ / 扩展数据 | 边终点或内联值 |
| 24-25 | `column_id` | 2B | 属性/边类型 ID $\phi$ | Predicate ID |
| 26-27 | `sequence` | 2B | 版本序列号 $\kappa$ | 同一微秒内并发事件的全序编号 |
| 28 | `entity_type` | 1B | 实体类型 $\tau$ | NODE/EDGE/PROP 类型标识 |
| 29 | `flags` | 1B | 操作类型 $\delta$ + 状态位 | CREATE/UPDATE/DELETE 逻辑 + 分布式标记 |
| 30-31 | `part_id` | 2B | 分区 ID | 数据所属逻辑分片（0-65535）|

---

## 8B 元数据区：最终定义

在分布式架构中，偏移量 +24 到 +31 的 8 字节物理布局定义如下：

| 偏移量 | 字段 | 大小 | 符号 | 分布式语义说明 |
|--------|------|------|------|---------------|
| **24-25** | `column_id` | 2B | $\phi$ | Predicate ID：属性或边类型的唯一编码 |
| **26-27** | `sequence` | 2B | $\kappa$ | Sequence：同一微秒内并发事件的全序编号 |
| **28** | `entity_type` | 1B | $\tau$ | Event Class：NODE/EDGE/PROP 类型标识 |
| **29** | `flags` | 1B | $\delta$ | Op Type + 状态位：包含 CREATE/UPDATE/DELETE 逻辑 |
| **30-31** | `part_id` | **2B** | - | **Partition ID**：数据所属的逻辑分片（0-65535）|

---

## 为什么 `part_id` 放在 Key 的末尾？

在分布式存储设计中，CedarKey 选择将 `part_id` 放在末尾（+30 偏移量）具有特殊的**图拓扑优化**意义：

### A. 避免"路由前缀"导致的随机写

如果将 `part_id` 放在偏移量 0（Key 的最前面），虽然方便路由，但会导致同一个点（entity_id）的不同属性分散在不同的逻辑范围。

**CedarKey 的选择**：以 `entity_id` 为开头。这意味着**同一个实体的所有历史、所有属性、所有关联边，在物理存储上是高度聚簇的**。

### B. 分布式 Compaction 过滤 (Filter-on-the-fly)

当存储节点（Tablet/Shard）承载了多个 Partition 时，后台的 Compaction 线程可以利用末尾的 `part_id` 快速识别：

- **迁移支持**：在分片分裂（Partition Split）或迁移时，引擎可以直接根据最后 2 字节做物理流式剪裁，无需重新计算 `entity_id` 的 Hash。
- **数据一致性校验**：在读取时，如果发现 Key 中的 `part_id` 与当前存储桶不符，可以立即报错，防止分布式脑裂导致的数据污染。

### C. 比较顺序的精妙设计

由于 `part_id` 放在了比较顺序的最后一位：

```
比较顺序：entity_id → entity_type → column_id → target_id → timestamp → sequence → flags → part_id
```

这意味着：
- **单点逻辑连续性**：同一 `entity_id` 的所有数据在物理上连续存储
- **全局物理可管理性**：`part_id` 不影响排序，但可用于物理过滤

---

## Flags 字段深度枚举

为了支撑分布式和时态语义，`flags` (1B) 字段承载 $\delta$ (Operation Type)：

| Bit | 定义 | 描述 |
|-----|------|------|
| **0-1** | **`op_type`** | **$\delta$ 映射**：`00`: CREATE, `01`: UPDATE, `10`: DELETE |
| **2** | `is_distributed` | 标记位。若为 1，表示 `part_id` 字段当前生效 |
| **3** | `has_v_inline` | 标记 $v$ (Value) 是否直接存储在 `target_id` 或其它空隙中 |
| **4** | `is_compressed` | Value 部分是否经过压缩 |
| **5** | `is_locked` | **分布式事务支持**：标记该记录是否被某个事务锁住（用于实现类似 Percolator 的协议）|
| **6-7** | `reserved` | 预留 |

### Op Type 枚举

```cpp
namespace op_type {
  constexpr uint8_t kCreate  = 0x00;  // 00: 创建操作
  constexpr uint8_t kUpdate  = 0x01;  // 01: 更新操作
  constexpr uint8_t kDelete  = 0x02;  // 10: 删除操作
}
```

### 状态位掩码

```cpp
namespace key_flags {
  constexpr uint8_t kOpTypeMask      = 0x03;  // Bit 0-1: 操作类型
  constexpr uint8_t kIsDistributed   = 1 << 2; // Bit 2: 分布式标记
  constexpr uint8_t kHasVInline      = 1 << 3; // Bit 3: 内联值标记
  constexpr uint8_t kIsCompressed    = 1 << 4; // Bit 4: 压缩标记
  constexpr uint8_t kIsLocked        = 1 << 5; // Bit 5: 事务锁标记
}
```

---

## 实体类型

```cpp
enum class EntityType : uint8_t {
  Vertex  = 0,   // 点/节点 (NODE)
  EdgeOut = 1,   // 出边 (EDGE_OUT: src -> dst)
  EdgeIn  = 2,   // 入边 (EDGE_IN: dst <- src)，用于快速反向查询
};
```

### 各类型字段用法

#### Vertex (类型 = 0)
| 字段 | 用法 |
|-------|-------|
| `entity_id` | 顶点 ID $s$ |
| `target_id` | 扩展数据（如轻量权重）或内联值 |
| `column_id` | 属性 ID $\phi$ |

#### EdgeOut (类型 = 1)
| 字段 | 用法 |
|-------|-------|
| `entity_id` | 源顶点 ID $s$ |
| `target_id` | 目标顶点 ID $o$ |
| `column_id` | 边类型 ID $\phi$ |

#### EdgeIn (类型 = 2)
| 字段 | 用法 |
|-------|-------|
| `entity_id` | 目标顶点 ID $s$（查询入口）|
| `target_id` | 源顶点 ID $o$（邻居）|
| `column_id` | 边类型 ID $\phi$ |

---

## 映射到原子事件 $\epsilon$ 的完整视图

原子图事件 $\epsilon = \langle s, t, o, \phi, \tau, \delta, \kappa, v \rangle$ 与 32B CedarKey 的映射关系：

$$
\text{CedarKey} = \underbrace{s}_{\text{8B}} + \underbrace{t}_{\text{8B}} + \underbrace{o}_{\text{8B}} + \underbrace{\phi}_{\text{2B}} + \underbrace{\kappa}_{\text{2B}} + \underbrace{\tau}_{\text{1B}} + \underbrace{\delta}_{\text{1B}} + \underbrace{\text{part\_id}}_{\text{2B}}
$$

| 符号 | 字段 | 大小 | 含义 |
|------|------|------|------|
| $s$ | `entity_id` | 8B | Source vertex |
| $t$ | `timestamp_be` | 8B | Timestamp (descending) |
| $o$ | `target_id` | 8B | Target vertex or extension |
| $\phi$ | `column_id` | 2B | Predicate ID (label/property key) |
| $\kappa$ | `sequence` | 2B | Intra-microsecond ordering |
| $\tau$ | `entity_type` | 1B | Event class (NODE/EDGE/PROP) |
| $\delta$ | `flags` | 1B | Op type (CREATE/UPDATE/DELETE) + status |
| - | `part_id` | 2B | Distributed partition identifier |

---

## 分布式查询流程

在分布式架构下，查询流程如下：

```
┌─────────────────────────────────────────────────────────────┐
│  1. 路由层 (Routing Layer)                                   │
│     - 根据 s (entity_id) 计算 Hash                           │
│     - 定位到对应的物理节点 (Tablet/Shard)                    │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  2. 存储层 (Storage Layer)                                   │
│     - 节点根据 s 找到对应的 LSM-Tree 范围                    │
│     - 加载 SST 文件的 Zone Maps 和 Bloom Filter            │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  3. 过滤层 (Filter Layer)                                    │
│     - 通过 part_id (最后 2B) 确认分片归属                   │
│     - 校验 part_id 与当前存储桶是否匹配（脑裂防护）          │
│     - 利用 Zone Maps 进行范围过滤                            │
└──────────────────────┬──────────────────────┬───────────────┘
                       ▼                      ▼
┌─────────────────────────────────┐  ┌──────────────────────────┐
│  4. 时变分析 (Temporal Analysis) │  │  5. 分布式事务 (Tx)       │
│     - 利用 t (descending)        │  │     - 检查 is_locked 标志 │
│       提取该分片内 s 的演变轨迹  │  │     - Percolator 协议支持 │
└─────────────────────────────────┘  └──────────────────────────┘
```

### 详细步骤

1. **路由层**：根据 $s$ (source vertex) 计算 Hash，定位到对应的物理节点
2. **存储层**：节点根据 $s$ 找到对应的 LSM-Tree 范围
3. **过滤层**：
   - 通过 `part_id` (最后 2B) 确认分片归属
   - 校验 `part_id` 与当前存储桶是否匹配，防止分布式脑裂导致的数据污染
4. **时变分析**：利用 $t$ (descending) 提取该分片内 $s$ 的演变轨迹

---

## 时间戳编码

时间戳按**降序**存储，确保最新版本排在前面：

```cpp
uint64_t EncodeForStorage(uint64_t micros) {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    return htobe64(max - micros);  // 大端序用于字典序比较
}
```

**优点：**
- 迭代时最新数据排在前面
- 无需扫描旧版本即可找到最新数据
- 天然支持 "最新状态" 查询

---

## 分区容量评估

**65536 ($2^{16}$) 个分区的容量评估：**

| 指标 | 数值 | 说明 |
|------|------|------|
| 单分区容量 | ~100GB-1TB | LSM-Tree 推荐大小 |
| 总容量 | 6.5PB - 65PB | 65536 × 100GB-1TB |
| 单集群节点 | 100-1000 台 | 超大规模集群 |
| 每节点分区 | 65-655 个 | 合理范围 |

**结论**：65536 个分区完全足够，可以支撑：
- 百 PB 级数据量
- 千节点级集群
- 每节点 64 个分区（负载均衡良好）

---

## 编码/解码

```cpp
// 编码为 32 字节字符串
std::string Encode() const;

// 编码到缓冲区
void EncodeTo(void* buffer) const;

// 从 string_view 解码
static std::optional<CedarKey> Decode(std::string_view slice);

// 从指针解码
static CedarKey Decode(const void* ptr);
```

---

## C++ 结构体定义

```cpp
struct __attribute__((packed, aligned(8))) CedarKey {
    // 基础定位信息 (24B)
    uint64_t entity_id;      // s: source vertex
    uint64_t timestamp_be;   // t: timestamp (descending)
    uint64_t target_id;      // o: target vertex or extension

    // 8B 元数据区 (8B)
    uint16_t column_id;      // phi: label/property key
    uint16_t sequence;       // kappa: intra-microsecond ordering
    uint8_t  entity_type;    // tau: NODE/EDGE/PROP
    uint8_t  flags;          // delta: CREATE/UPDATE/DELETE + internal status
    uint16_t part_id;        // Distributed Partition Identifier (0-65535)
};

static_assert(sizeof(CedarKey) == 32, "CedarKey must be exactly 32 bytes");
static_assert(alignof(CedarKey) == 8, "CedarKey must be 8-byte aligned");
```

---

## 性能特征

| 操作 | 复杂度 | 说明 |
|-----------|------------|-------|
| 编码 | O(1) | 直接内存拷贝 |
| 解码 | O(1) | 直接内存拷贝 |
| 比较 | O(1) | 固定 32B 的 `memcmp` |
| 比较 UserKey | O(1) | 比较前 27B（不含 flags 和 part_id）|
| 提取实体 ID | O(1) | 直接字段访问 |
| 分区过滤 | O(1) | 直接读取最后 2B |

---

## 设计原理

### 为什么是 32 字节？

- **缓存行效率**：2 个 key 正好放入 64B 缓存行
- **内存对齐**：8B 对齐支持 SIMD 优化
- **定长优势**：消除 LSM-Tree 中的长度前缀开销
- **空间充足**：容纳 64 位 ID 和微秒时间戳

### 为什么是降序时间戳？

- **查询模式**：大多数查询请求最新版本
- **迭代效率**：无需跳过旧版本
- **自然排序**：时序旅行查询变为简单的前缀扫描

### 为什么 `part_id` 放在末尾？

- **物理聚簇**：同一 `entity_id` 的数据物理连续
- **逻辑分区**：不影响比较顺序，但支持物理过滤
- **Compaction 优化**：可直接按 `part_id` 流式剪裁

---

## 工厂方法

### Vertex
```cpp
static CedarKey Vertex(uint64_t vertex_id,
                       VertexColumnId col,
                       Timestamp ts,
                       uint16_t seq = 0,
                       uint16_t part_id = 0,
                       uint64_t extension = 0,
                       uint8_t flags = 0);
```

### EdgeOut（出边）
```cpp
static CedarKey EdgeOut(uint64_t src_id,
                        uint64_t dst_id,
                        EdgeTypeId edge_type,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint16_t part_id = 0,
                        uint8_t flags = 0);
```

### EdgeIn（入边 / 反向索引）
```cpp
static CedarKey EdgeIn(uint64_t dst_id,
                       uint64_t src_id,
                       EdgeTypeId edge_type,
                       Timestamp ts,
                       uint16_t seq = 0,
                       uint16_t part_id = 0,
                       uint8_t flags = 0);
```

### 创建双向边对
```cpp
static std::pair<CedarKey, CedarKey> MakeEdge(
    uint64_t src_id,
    uint64_t dst_id,
    EdgeTypeId edge_type,
    Timestamp ts,
    uint16_t seq = 0,
    uint16_t part_id = 0);
// 返回：{EdgeOut, EdgeIn}
```

---

## 相关类型

### InternalKey
排除时间戳/版本信息的 27 字节键（到 `entity_type` 为止），用于 MemTable 索引：
```cpp
struct InternalKey {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  uint64_t target_id;
};
```

### Timestamp
微秒精度时间戳，支持降序编码：
```cpp
class Timestamp {
  uint64_t value_;  // 自纪元起的微秒数
};
```

---

## 参考

- `include/cedar/types/cedar_key.h` - 头文件实现
- `include/cedar/types/descriptor.h` - 值（8B）设计
- `docs/sst_format_design.md` - 磁盘 SST 格式
