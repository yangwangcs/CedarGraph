# CedarGraph 事件映射规范

## 概述

在分布式时态图存储的设计中，**导入（Ingestion）**是将抽象的图事件 $\epsilon$ 转化为物理字节流的"炼金术"。当系统接收到 `CREATE`、`UPDATE` 或 `DELETE` 事件时，映射过程不仅要保证数据的完整性，还要为后续的**时序范围查询**和**分布式均衡**铺路。

---

## 1. 映射核心逻辑：从事件到字节

映射过程可以看作一个**编码流水线**。给定一个原子事件 $\epsilon = \langle s, t, o, \phi, \tau, \delta, \kappa, v \rangle$，映射逻辑如下：

### 1.1 基础标识映射 (Location)

| 字段 | 映射规则 | 说明 |
|------|---------|------|
| `entity_id` ($s$) | 直接填入 | 路由和聚簇的第一关键字 |
| `target_id` ($o$) | Vertex: 填充 `0`；Edge: 填充目标顶点 ID | 边终点或扩展数据 |
| `part_id` | `Hash(entity_id) % MaxPartitions` | 确保同一实体的所有历史版本落在同一物理分片 |

### 1.2 时态编码 (Temporal)

| 字段 | 映射规则 | 说明 |
|------|---------|------|
| `timestamp_be` ($t$) | `EncodeForStorage(t) = uint64_max - t` | 降序存储，最新事件排在最前面 |
| `sequence` ($\kappa$) | 同一微秒内多个事件时递增 | 确保全序性 |

### 1.3 语义与状态映射 (Metadata)

| 字段 | 映射规则 | 说明 |
|------|---------|------|
| `column_id` ($\phi$) | 从元数据缓存查找 Label/Property Key 对应的 `uint16_t` | Predicate ID |
| `entity_type` ($\tau$) | `Vertex(0)` / `EdgeOut(1)` / `EdgeIn(2)` | 实体类型 |
| `flags` ($\delta$) | 见下方位图定义 | 操作类型 + 状态位 |

---

## 2. Flags 位图定义 (1 Byte)

| Bit | 字段名 | 描述 |
|-----|--------|------|
| **0-1** | `delta_op` | `00`: CREATE (初始点/边/属性)<br>`01`: UPDATE (状态变更)<br>`10`: DELETE (从此时间点起逻辑消失) |
| **2** | `is_distributed` | `part_id` 字段是否生效（分布式标记） |
| **3** | `has_v_inline` | 值是否内联在 `target_id` 中 |
| **4** | `is_compressed` | Value 是否经过压缩 |
| **5** | `is_locked` | 分布式事务锁（Percolator 协议） |
| **6** | `has_extension` | `target_id` 是否包含扩展数据 |
| **7** | `is_tombstone` | 物理墓碑标记（仅用于 Compaction，见第5.4节） |

### 2.1 Column 时效性：is_static 设计

对于不随时间变化的属性（如点的创建时间、静态 Label、不可变标识符），可以在 `column_id` 的最高位（MSB）标记为 **静态字段**（`is_static = column_id & 0x8000`）。

**优化效果：**
- 扫描历史轨迹时，迭代器检测到 `is_static` 位可直接跳过，避免重复读取相同值
- Compaction 时，静态字段的旧版本可被安全清理（因为值永不变更）
- 减少存储膨胀，提升时态查询效率

**示例：**
```cpp
// 标记静态属性
column_id = Schema.GetId("created_at") | 0x8000;  // 设置 is_static 位

// 扫描时跳过静态字段
if (key.column_id & 0x8000) {
    // 静态字段，只在首次出现时读取，后续版本跳过
    continue;
}
```

---

## 3. 场景演示：创建一个"转账"边事件

假设事件：`Vertex A (ID: 101)` 在 `2026-04-02 10:00:00` 创建了一条指向 `Vertex B (ID: 202)` 的 `Transfer` 边。

### 3.1 生成 EdgeOut (正向边)

这是存储在源节点 A 所在分片的主数据。

| 字段 | 映射值 | 说明 |
|------|--------|------|
| `entity_id` | `101` | 源点 A |
| `timestamp_be` | `0xFF... - 17120...` | 降序时间戳 |
| `target_id` | `202` | 目标点 B |
| `column_id` | `5` (Transfer_ID) | 边类型 |
| `sequence` | `0` | 初始序列号 |
| `entity_type` | `1` (EdgeOut) | 出边类型 |
| `flags` | `0x04` | Bit 0-1: `00` (Create), Bit 2: `1` (Distributed) |
| `part_id` | `Hash(101) % N` | 分片 ID |

### 3.2 生成 EdgeIn (反向索引)

为了支持"谁转账给我"的查询，系统会自动生成一个镜像 Key。**注意：`entity_id` 和 `target_id` 对调。**

| 字段 | 映射值 | 说明 |
|------|--------|------|
| **`entity_id`** | **`202`** | **目标点 B 变为检索入口** |
| `timestamp_be` | `0xFF... - 17120...` | 保持一致 |
| **`target_id`** | **`101`** | **源点 A 变为邻居** |
| `column_id` | `5` | 保持一致 |
| `entity_type` | **`2` (EdgeIn)** | **标记为入边** |
| `part_id` | `Hash(202) % N` | **根据点 B 路由到对应节点** |

---

## 4. 更新操作 (UPDATE) 的映射设计

当属性 $\phi$ 在时间 $t_{new}$ 发生变更时，系统生成一个新的 Key-Value 对。

### 4.1 Key 映射逻辑

| 字段 | 映射规则 |
|------|---------|
| `entity_id`, `target_id`, `column_id` | 保持不变，确保属于同一逻辑实体 |
| `timestamp_be` | 使用新时间 $t_{new}$ 编码（降序），新 Key 物理排在旧版本之前 |
| `sequence` | 如果 $t_{new}$ 与上一条记录相同，则递增此值 |
| `flags` | 设置 `OpType` 为 `UPDATE (01)` |

### 4.2 Value 映射逻辑

存储新的属性值 $v_{new}$。

### 4.3 设计效果

- 查询"当前状态"时，`Seek` 操作会第一个命中这个 `UPDATE` 记录
- 查询"历史轨迹"时，向后迭代即可看到所有历史版本

---

## 5. 删除操作 (DELETE) 的映射设计

在全历史模式下，`DELETE` 不是真正的物理删除，而是一个**时态墓碑（Temporal Tombstone）**。

### 5.1 Key 映射逻辑

| 字段 | 映射规则 |
|------|---------|
| `timestamp_be` | 记录删除发生的时刻 $t_{del}$ |
| `flags` | 设置 `OpType` 为 `DELETE (10)` |

### 5.2 Value 映射逻辑

通常为空，或者存储轻量级的删除元数据（如：是谁执行了删除）。

### 5.3 时态语义

| 查询时间 | 结果 |
|---------|------|
| $T > t_{del}$ | 系统命中 `DELETE` 标记，返回"不存在" |
| $T < t_{del}$ | 系统跳过 `DELETE` 记录，找到更早的 `CREATE` 或 `UPDATE`，实现**"回到过去"** |

### 5.4 业务删除 vs. 物理墓碑

在 CedarGraph 的设计中，需要严格区分**业务逻辑删除**与**物理墓碑**两个概念：

| 维度 | 业务删除 (Logical DELETE) | 物理墓碑 (Physical Tombstone) |
|------|---------------------------|-------------------------------|
| **触发条件** | 用户/业务系统显式删除实体或属性 | LSM-Tree Compaction 清理中间版本时生成 |
| **flags 标记** | `delta_op = 10` (bit 0-1) | `is_tombstone = 1` (bit 7) |
| **语义** | "从此时间点起，该属性逻辑上不存在" | "此记录在物理存储上可被安全回收" |
| **存储位置** | MemTable → SST (全历史保留) | Compaction 输出文件的元数据标记 |

**关键设计原则：**

1. **业务 DELETE 不设置 `is_tombstone`**
   ```cpp
   // 正确的 DELETE 事件编码
   key.flags = 0x02 | (1 << 2);  // DELETE (10) + Distributed
   // 注意：不设置 bit 7 (is_tombstone)
   ```

2. **`is_tombstone` 专供 Compaction Filter 使用**
   - 当 LSM-Tree 执行合并时，对于被多个 UPDATE 覆盖的中间版本，Compaction Filter 可标记 `is_tombstone = 1`
   - 物理墓碑用于 MVCC 垃圾回收，在下一次 Compaction 时真正释放磁盘空间
   - 这允许系统支持**"保存全历史"**与**"TTL 自动清理"**两种模式的灵活切换

3. **全历史模式下的 DELETE 保留**
   - 在"保存全历史"模式下，业务 DELETE 记录**永久保留**（除非执行显式的 TTL 或物理归档）
   - 用户随时可以查询 $T < t_{del}$ 的历史状态，实现真正的"时间旅行"
   - 只有开启 TTL 策略或执行归档命令时，系统才通过设置 `is_tombstone` 标记可回收版本

---

## 6. 全历史下的物理排列示例

假设 `Vertex 1` 的 `status` 属性演变如下：

1. $t=100$: `CREATE` (value: "active")
2. $t=200$: `UPDATE` (value: "suspended")
3. $t=300$: `DELETE`

**磁盘上的物理排列顺序（从上到下，降序时间戳）：**

| 物理顺序 | Key 内容 | 映射的 $\delta$ | 说明 |
|---------|---------|----------------|------|
| **1** | `ID:1, TS:Max-300, Col:status` | **DELETE** | 最新状态：已删除 |
| **2** | `ID:1, TS:Max-200, Col:status` | **UPDATE** | 历史版本：曾是 suspended |
| **3** | `ID:1, TS:Max-100, Col:status` | **CREATE** | 初始版本：曾是 active |

---

## 7. 设计陈述：为什么这样设计能支持"全历史"？

### 7.1 Append-only 架构

所有的 `UPDATE` 和 `DELETE` 都是在 LSM-Tree 的最上层（MemTable/L0）追加新 Key。这不仅写入速度极快，而且天然保留了旧数据。

### 7.2 不可变性 (Immutability)

旧的 SST 文件一旦生成就不再修改。在全历史模式下，Compaction 策略配置为**"不丢弃旧版本"**。

### 7.3 时序对齐

由于 `part_id` 放在最后，且同一实体的 Key 前缀一致，所有的历史版本在物理上是连续存储的。这使得"回溯某节点的所有变迁史"变成了一次极高性能的**顺序扫描**。

---

## 8. 导入流程伪代码

```cpp
void IngestCreateEvent(const Event& e) {
    // 1. 构造基础 Key
    CedarKey key;
    key.entity_id = e.s;
    key.timestamp_be = EncodeTimestamp(e.t);
    key.target_id = e.o;
    key.column_id = Schema.GetId(e.phi);
    key.sequence = e.kappa;
    key.entity_type = (e.tau == NODE) ? EntityType::Vertex : EntityType::EdgeOut;
    key.flags = PackFlags(e.delta);  // bit 0-1: OpType
    key.part_id = ComputePartition(e.s);

    // 2. 写入存储引擎 (LSM-Tree)
    StorageEngine.Put(key, e.v);

    // 3. 如果是边，生成反向索引
    if (e.tau == EDGE) {
        CedarKey in_key = key;
        std::swap(in_key.entity_id, in_key.target_id);
        in_key.entity_type = EntityType::EdgeIn;
        in_key.part_id = ComputePartition(e.o);
        StorageEngine.Put(in_key, EmptyValue);
    }
}

void IngestUpdateEvent(const Event& e) {
    // UPDATE 也是追加新 Key
    CedarKey key;
    key.entity_id = e.s;
    key.timestamp_be = EncodeTimestamp(e.t);  // 新时间戳
    key.target_id = e.o;
    key.column_id = Schema.GetId(e.phi);
    key.sequence = GetNextSequence(e.s, e.t);  // 必要时递增
    key.entity_type = (e.tau == NODE) ? EntityType::Vertex : EntityType::EdgeOut;
    key.flags = 0x01 | (1 << 2);  // UPDATE (01) + Distributed
    key.part_id = ComputePartition(e.s);
    
    StorageEngine.Put(key, e.v);  // 新值
}

void IngestDeleteEvent(const Event& e) {
    // DELETE 生成业务逻辑墓碑（物理墓碑由 Compaction 管理）
    CedarKey key;
    key.entity_id = e.s;
    key.timestamp_be = EncodeTimestamp(e.t);  // 删除时间
    key.target_id = e.o;
    key.column_id = Schema.GetId(e.phi);
    key.sequence = GetNextSequence(e.s, e.t);
    key.entity_type = (e.tau == NODE) ? EntityType::Vertex : EntityType::EdgeOut;
    key.flags = 0x02 | (1 << 2);  // DELETE (10) + Distributed，不设置 is_tombstone
    key.part_id = ComputePartition(e.s);
    
    StorageEngine.Put(key, EmptyValue);  // 空值或删除元数据
}

void IngestStaticProperty(const Event& e) {
    // 创建静态属性（如创建时间、Label）
    CedarKey key;
    key.entity_id = e.s;
    key.timestamp_be = EncodeTimestamp(e.t);
    key.target_id = e.o;
    // 标记 column 为静态：高位置 1
    key.column_id = Schema.GetId(e.phi) | 0x8000;  // is_static = true
    key.sequence = e.kappa;
    key.entity_type = EntityType::Vertex;
    key.flags = 0x00 | (1 << 2);  // CREATE + Distributed
    key.part_id = ComputePartition(e.s);
    
    StorageEngine.Put(key, e.v);
}
```

---

## 9. 关键函数说明

```cpp
// 时间戳降序编码
uint64_t EncodeTimestamp(uint64_t micros) {
    return std::numeric_limits<uint64_t>::max() - micros;
}

// 计算分区 ID
uint16_t ComputePartition(uint64_t entity_id) {
    return static_cast<uint16_t>(Hash(entity_id) % MaxPartitions);
}

// 打包 flags
uint8_t PackFlags(OpType op, bool distributed, bool has_inline, 
                  bool compressed, bool locked) {
    uint8_t flags = static_cast<uint8_t>(op) & 0x03;  // bit 0-1
    if (distributed) flags |= (1 << 2);
    if (has_inline)  flags |= (1 << 3);
    if (compressed)  flags |= (1 << 4);
    if (locked)      flags |= (1 << 5);
    return flags;
}
```

---

## 参考

- `docs/cedar_key_design.md` - CedarKey 32B 结构详细定义
- `docs/sst_format_design.md` - SST 格式与 Zone 编码
- `include/cedar/types/cedar_key.h` - CedarKey C++ 实现
