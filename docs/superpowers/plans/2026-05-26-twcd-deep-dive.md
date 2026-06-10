# TWCD 深度解析：时序感知分布式事务验证机制

> **TWCD = Temporal-Window Conflict Detection（时序窗口冲突检测）**
>
> **定位**：TWCD 不是分布式提交协议，而是嵌入在分布式 OCC 验证路径中的一个**时序感知验证算子**。原子性仍由 DOL-2PC/2PC 保证。

## 1. 定位与核心思想

### 1.1 解决什么问题

传统分布式 OCC 的冲突判断是**一维 Key 空间**：

```
Conflict(T_i, T_j) ⇐ ReadSet_i ∩ WriteSet_j ≠ ∅
```

对于时序图数据库，这会产生大量**伪冲突**：

- **T1**: `MATCH (n {id:42, time:2024}) RETURN n.value`  
- **T2**: `SET n.value=100 WHERE id(n)=42 AND n.time=2025`

T1 读的是 2024 年的数据，T2 写的是 2025 年的数据。它们在业务逻辑上完全不相交，但传统 OCC 会因为读写集中出现了同一个 `entity_id=42` 而判定冲突，导致不必要的重试/中止。

### 1.2 TWCD 的核心思想：Key-Time 二维冲突域

TWCD 将冲突检测对象从一维 Key 空间提升到**二维 Key-Time 空间**：

```
Conflict(T_i, T_j) ⇐ TemporalOverlap(W_i, W_j)
                       ∧ ( RWOverlap(T_i, T_j)
                           ∨ WWOverlap(T_i, T_j)
                           ∨ PredicateOverlap(T_i, T_j) )
```

其中：
- `W_i`, `W_j` 是事务声明的时序访问窗口（保守超集）
- `TemporalOverlap` 检查时间窗口是否重叠
- `RWOverlap` / `WWOverlap` 检查 Key 级读写/写写重叠
- `PredicateOverlap` 检查谓词读与写入的范围重叠（幻读防护）

**关键洞察**：两个事务访问同一个 Key，但如果它们的时间窗口不重叠，就没有冲突。这是时序图数据的天然属性——同一实体的不同历史版本是独立的逻辑状态。

---

## 2. 正确性前提（必须在论文中明确声明）

### 2.1 窗口保守性

事务声明的时序窗口必须满足：

```
ActualAccessTimeRange(T) ⊆ W(T)
```

- **窗口可以更大**（会降低性能收益，但不破坏正确性）
- **窗口不能更小**（会漏检冲突，破坏隔离性）

**无显式时间谓词的查询**（如 `MATCH (n) RETURN n`）必须映射为：

```
W(T) = (-∞, +∞)
```

即退化为标准 Key-level OCC。这一点必须在正确性定义中写明，不能仅作为配置建议。

### 2.2 谓词读与幻读防护

仅记录读到的具体 Key 不够。时序范围查询必须记录**谓词读窗口**：

```cpp
struct PredicateRead {
  EntityType    entity_type;
  Label         label;
  PropertyFilter filter;
  TemporalWindow window;      // 时序谓词范围
};
```

**反例**：
- T1: `MATCH ()-[e:TRANSFER]->() WHERE e.time BETWEEN 2024-01-01 AND 2024-12-31 RETURN sum(e.amount)`
- T2: `CREATE EDGE TRANSFER {time: 2024-06-01, amount: 100}`

T1 的读集如果只记录已读到的边，T2 插入的新边会导致**幻读**。因此 TWCD 的 Key 级检测必须扩展为：

```
Conflict ⇐ TemporalOverlap ∧ (KeyOverlap ∨ PredicateWriteOverlap)
```

### 2.3 双重时间戳的正确性基础

CedarGraph 区分两种时间戳：

| 维度 | 名称 | 对应关系 | 用途 |
|------|------|----------|------|
| **业务时间** | `user_timestamp` | Valid Time / Application Time | 数据在时序图中的有效时间；编码进 CedarKey；用于 SST 排序和时序查询 |
| **事务版本** | `txn_version` | Transaction Time / System Time | MVCC 并发控制；**不编码进 Key**；仅存在于 MemTable 版本链节点中 |

可见性条件：

```
Visible(v, θ, rts) ⇔ business_time(v) ≤ θ  ∧  txn_version(v) ≤ rts
```

对于区间查询：

```
Visible(v, [t_s, t_e], rts) ⇔ business_time(v) ∈ [t_s, t_e]  ∧  txn_version(v) ≤ rts
```

**这保证了历史回填不会破坏快照隔离**：
- T1 在 2025-01-01 开始，read_timestamp = 100
- T2 在 2026-01-01 提交一条业务时间为 2024-01-01 的数据，commit_timestamp = 200
- T1 查询 `AS OF 2024` 时：业务时间满足，但 `txn_version=200 > read_timestamp=100`，因此不可见

---

## 3. 整体架构：三层事务模型

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 3: Deterministic Batch Commit（批量导入/回填优化路径）│
│         预排序输入序列，按序执行，无需运行时冲突检测          │
├─────────────────────────────────────────────────────────────┤
│ Layer 2: TWCD-Aware Distributed OCC + DOL-2PC               │
│         跨分区事务。TWCD 过滤时间不相交事务，减少验证范围；   │
│         DOL-2PC 保证跨分区原子性                              │
├─────────────────────────────────────────────────────────────┤
│ Layer 1: Local MVCC/OCC（单分区，零协调）                   │
│         利用 LSM-Tree 不可变性，单分区事务无需分布式协调      │
├─────────────────────────────────────────────────────────────┤
│  TW-CD Engine  │  DVC-Val       │  BBCC                     │
│  (Interval Tree │  (Version      │  (Bookmark-Based          │
│   + Key Index   │   Chain O(1)   │   Causal Consistency)     │
│   + Predicate   │   Validation)  │                           │
│     Index)      │                │                           │
├─────────────────────────────────────────────────────────────┤
│  HLC (Hybrid Logical Clock) + Sharded Timestamp Allocator   │
├─────────────────────────────────────────────────────────────┤
│  LSM-Tree Storage Layer                                     │
│  - MemTable: MVCC Version Chains (TemporalVersionNode)      │
│  - WAL: Write-Ahead Log (per-partition)                     │
│  - SST: Immutable Zone-Columnar Files (time-partitioned)    │
└─────────────────────────────────────────────────────────────┘
```

**事务分类逻辑**（`LndOccEngine::ClassifyTransaction`）：

| 条件 | 层级 | 协调成本 | 协议 |
|------|------|----------|------|
| 读写集在同一分区 | Layer 1 | **零** | 本地 MVCC/OCC |
| 跨分区，时间窗口可判定 | Layer 2 | **降低** | TWCD 过滤 + DOL-2PC |
| 批量导入/回填 | Layer 3 | **无运行时冲突** | 确定性排序执行 |

**注意**：Layer 2 的价值不是"完全不需要分布式协调"，而是：
- 减少 2PC 的参与者范围（仅时间窗口重叠的分区需要协调）
- 减少验证阶段的比较对象数
- 减少不必要的事务中止

---

## 4. 关键数据结构

### 4.1 TemporalWindow（时序窗口）

**建议改为半开区间** `[start, end)`：

```cpp
struct TemporalWindow {
  Timestamp start;   // 包含
  Timestamp end;     // 不包含，end=0 表示 +∞
};
```

半开区间的优势：
- 避免边界重复（`[10,20)` 与 `[20,30)` 不重叠）
- 更符合数据库范围查询惯例

重叠判断：

```cpp
bool Overlaps(const TemporalWindow& other) const {
  return start < other.end && other.start < end;
}
```

**无显式时序谓词的事务**：

```cpp
W(T) = [0, 0)  // 表示 (-∞, +∞)，退化为标准 OCC
```

### 4.2 两级索引结构（推荐实现）

```
Temporal Index:
  Interval Tree: temporal window → candidate txn ids
  
Key Index:
  write_key → {active writer txn ids}
  read_predicate_key/range → {active reader txn ids}
  
Txn Table:
  txn_id → {window, read_set, write_set, predicate_set, state}
```

冲突检测流程：

```
1. Interval Tree 找出时间窗口重叠事务 C_t
2. Key Index 找出 Key 重叠事务 C_k
3. Predicate Index 找出谓词读写冲突 C_p
4. 候选冲突集：C = C_t ∩ (C_k ∪ C_p)
5. 若 C 非空，进入 OCC/MVCC 验证或 abort
```

### 4.3 Interval Tree 节点

```cpp
struct IntervalTreeNode {
  Timestamp start;
  Timestamp end;
  std::set<TxnID> txns;        // 覆盖此区间的所有事务
  std::unique_ptr<IntervalTreeNode> left, right;
  Timestamp max_end;            // 子树最大 end（用于剪枝）
};
```

时间复杂度：
- 插入/删除：`O(log n)`
- 重叠查询：`O(log n + m)`，`m` = 重叠事务数（不是总事务数）

---

## 5. 读流程详解

### Step 1: 检查写集（Read-Your-Writes）

```cpp
if (write_set_keys_.find(target_key) != write_set_keys_.end()) {
  // 返回本事务未提交的写入值
  *descriptor = write_entry.descriptor;
  return Status::OK();
}
```

### Step 2-3: MemTable 版本链 + MVCC 选择

```cpp
auto chain_opt = memtable_->GetVersionChain(entity_id, entity_type, column_id);

for (const auto& entry : chain_opt) {
  if (entry.txn_version <= read_timestamp_) {  // MVCC 可见性判断
    *descriptor = entry.descriptor;
    *version_ts = entry.timestamp;  // 返回业务时间给用户
    
    // 记录到读集（用于验证阶段）
    read_set_.push_back({entity_id, entity_type, column_id, 
                         entry.timestamp, entry.txn_version});
    return Status::OK();
  }
}
```

### Step 4: SST 回退（已知限制）

```cpp
if (lsm_engine_) {
  auto all_versions = lsm_engine_->GetAll(entity_id, entity_type, column_id);
  if (!all_versions.empty()) {
    *descriptor = all_versions.front().descriptor;
    *version_ts = all_versions.front().timestamp;
    
    // SST 数据视为已提交且全局可见
    // 已知限制：当前 SST 在 Flush 时丢失了 txn_version，因此无法做 MVCC
    // 过滤。修复路径：SST Builder/Reader 需增加 txn_version 列。
    read_set_.push_back({entity_id, entity_type, column_id, 
                         Timestamp::Max(), Timestamp::Max()});
    return Status::OK();
  }
}
```

**已知限制说明**：

当前 SST 格式在 Flush 时丢失了 `txn_version`。`FlushMemTable()` 通过 `mem->Traverse()` 收集 `pair<CedarKey, Descriptor>`，然后传给 `SstBuilderInterface::Add(key, desc)`。`MemTableEntry` 中的 `txn_version` 在此过程中丢失。

这导致：
1. SST 数据被所有事务视为可见（`Timestamp::Max()`）
2. 在"当前状态"查询下是正确的
3. 在"历史回填"场景下可能破坏快照隔离（详见第 2.3 节反例）

**修复路径**：
- `SstBuilderInterface::Add()` 增加 `txn_version` 参数
- `ZoneColumnarSstBuilderV2` 增加 Zone-6（`TxnVersionColumn`）
- `FlushMemTable()` 传递 `txn_version`
- `SstReader::GetRange()` 返回 `(CedarKey, Descriptor, txn_version)`

---

## 6. 写流程详解

### Step 1: 本地缓冲（Put）

```cpp
Status OCCTransaction::Put(...) {
  Timestamp ts = user_timestamp.value() > 0 ? user_timestamp : Timestamp::Now();
  
  // txn_version 使用哨兵值 Timestamp(0)，在 Commit() 验证通过后更新
  Timestamp txn_version = Timestamp(0);
  
  CedarKey temp_key = MakeKey(entity_id, entity_type, column_id, ts, target_id);
  write_set_.push_back({entity_id, entity_type, column_id, descriptor, 
                        temp_key, ts, txn_version, target_id});
  
  // WAL batch 延迟到 Commit() 时构建（使用正确的 commit_timestamp_）
  return Status::OK();
}
```

### Step 2: 分配提交时间戳 + 更新写集

```cpp
Status OCCTransaction::Commit() {
  commit_timestamp_ = txn_manager_->AllocateTimestamp();
  
  // 修复：更新写集中的 txn_version 为 commit_timestamp_
  for (auto& entry : write_set_) {
    entry.txn_version = commit_timestamp_;
  }
  
  // 重建 WAL batch，使用正确的 commit_timestamp_
  if (wal_writer_) {
    wal_batch_.Clear();
    for (const auto& entry : write_set_) {
      wal_batch_.Put(entry.key, entry.descriptor, entry.txn_version);
    }
  }
  
  // 验证阶段（TWCD + OCC 验证）
  Status validation_status = Validate();
  ...
}
```

**为什么 `txn_version` 必须是 `commit_timestamp_` 而不是 `read_timestamp_`？**

反例：
- T1 在 t=100 开始，`read_timestamp_ = 100`
- T2 在 t=150 开始，`read_timestamp_ = 150`
- T1 执行写入，如果 `txn_version = read_timestamp_ = 100`
- T1 在 t=200 才提交
- T2 的验证：检查是否有事务在 T2 开始后提交了同一 Key 的写入
- 如果 T2 的验证只看 `txn_version`，它可能无法正确判断 T1 的写入是否在 T2 开始后才提交

正确的 MVCC 语义：写入版本必须是**提交时间戳**，这样其他事务可以用自己的 `read_timestamp` 准确判断可见性。

### Step 3-5: 验证 → WAL → MemTable

```cpp
// 验证通过后才写入 WAL 和 MemTable
if (wal_writer_) {
  wal_writer_->WriteBatch(wal_batch_);  // WAL 含 commit_timestamp
}

for (const auto& entry : write_set_) {
  memtable_->Put(entry.key, entry.descriptor, entry.txn_version);
}
```

### Step 6: 更新 MemTable 版本链

MemTable 维护双向链表，按**业务时间戳**降序排列：

```cpp
struct TemporalVersionNode {
  Timestamp timestamp;      // 业务时间戳（用于时序查询）
  Descriptor descriptor;
  Timestamp txn_version;    // 提交时间戳（用于 MVCC 可见性）
  TemporalVersionNode* older;
  TemporalVersionNode* newer;
};
```

---

## 7. TWCD 冲突检测算法

```cpp
ConflictCheckResult TwcdEngine::CheckConflict(TxnID txn_id,
    const TemporalWindow& window,
    const std::vector<CedarKey>& read_set,
    const std::vector<CedarKey>& write_set) {
  
  ++total_checks_;
  
  // ===== 第一阶段：时间窗口过滤 =====
  auto overlapping_txns = interval_tree_.QueryOverlapping(window);
  overlapping_txns.erase(txn_id);
  
  if (overlapping_txns.empty()) {
    return ConflictCheckResult::NoConflict();  // 无时间重叠 → 肯定无冲突
  }
  
  // ===== 第二阶段：Key 级冲突检测 =====
  auto rw_conflicts = DetectReadWriteConflicts(txn_id, read_set, overlapping_txns);
  if (!rw_conflicts.empty()) {
    return ConflictCheckResult::ReadWriteConflict(rw_conflicts, overlapping_txns);
  }
  
  auto ww_conflicts = DetectWriteWriteConflicts(txn_id, write_set, overlapping_txns);
  if (!ww_conflicts.empty()) {
    return ConflictCheckResult::WriteWriteConflict(ww_conflicts, overlapping_txns);
  }
  
  // ===== 第三阶段：谓词读冲突检测（幻读防护）=====
  auto phantom_conflicts = DetectPhantomConflicts(txn_id, predicate_reads_, overlapping_txns);
  if (!phantom_conflicts.empty()) {
    return ConflictCheckResult::PhantomConflict(phantom_conflicts, overlapping_txns);
  }
  
  return ConflictCheckResult::NoConflict();
}
```

---

## 8. 配置与开关

```cpp
struct DTxConfig {
  bool enable_twcd{true};                  // 启用 TWCD
  uint64_t default_temporal_window_us{0};  // 默认窗口（0=无限，保守回退）
  
  uint32_t max_retry_count{3};
  std::chrono::milliseconds retry_base_delay{10};
  
  uint32_t max_version_chain_length{100};
  uint32_t gc_interval_ms{1000};
};
```

### 调优建议

| 场景 | `default_temporal_window_us` | 效果 |
|------|------------------------------|------|
| 纯时序数据（IoT、金融 tick） | 较小（1ms ~ 1s） | 精确窗口，最小化冲突 |
| 混合负载（时序 + 当前状态） | 0（无限） | 退化为标准 OCC，保证正确性 |
| 批量历史导入 | 较大（1h ~ 1d） | 覆盖整个批量范围，避免内部冲突 |

---

## 9. 论文贡献表述建议

### 贡献点 1：Key-Time 二维冲突域

> 传统 OCC 的冲突域是 Key 空间的一维集合。TWCD 将其扩展为 Key × TimeWindow 的二维空间，利用时序图查询天然携带的时间谓词缩小冲突判定范围。

### 贡献点 2：业务时间与事务时间分离

> 通过将 CedarKey 中的业务时间戳与 MVCC 版本号分离，系统保证历史回填、时间旅行查询和快照隔离可以共存。形式化可见性条件为：
>
> `Visible(v, θ, rts) ⇔ business_time(v) ≤ θ ∧ txn_version(v) ≤ rts`

### 贡献点 3：基于区间树的两阶段过滤

> 先通过区间树进行时间窗口过滤（`O(log n + m)`），再在候选事务之间进行 Key/谓词级冲突验证。比纯 Key-level OCC 更适合时序负载。

### 贡献点 4：与 LSM 时序布局协同

> 业务时间编码进 Key 用于 SST 时序过滤；事务版本不编码进 Key 用于 MVCC 可见性。存储层与事务层协同设计。

### 论文小节建议

```
3.3 时序感知分布式事务处理
  3.3.1 双重时间戳模型
  3.3.2 Key-Time 冲突域
  3.3.3 Temporal-Window Conflict Detection
  3.3.4 跨分区原子提交
  3.3.5 正确性分析
```

---

## 10. 已知问题与修复路径

| 问题 | 影响 | 修复路径 | 优先级 |
|------|------|----------|--------|
| `txn_version` 原先用 `read_timestamp_` | 破坏 MVCC 语义 | ✅ 已修复：改为 `Commit()` 时赋值 `commit_timestamp_` | P0 |
| SST Flush 丢失 `txn_version` | 历史回填破坏快照隔离 | SST Builder/Reader 增加 `txn_version` 列 | P1 |
| 缺少谓词读/幻读防护 | 范围查询与插入可能幻读 | 增加 `PredicateRead` 结构和谓词索引 | P1 |
| `TemporalWindow [0,0]` 歧义 | 可能混淆"精确点 0"与"无限" | 改用半开区间 + 哨兵值 `kPosInf = UINT64_MAX` | P2 |
| `IntervalTreeNode::txns` 为 `std::set` | 大量事务时查询开销增加 | 可优化为哈希集合或跳表 | P3 |

---

## 11. 总结

TWCD 的思想是正确的，而且非常适合 CedarGraph 的时序图场景。但论文中必须把它从直觉提升为严谨的机制：

> TWCD 是一种**基于保守时序窗口、Key-Time 二维冲突域和谓词读保护**的分布式 OCC 验证机制。它通过区间树将传统 Key-level 冲突检测扩展为 Key × Temporal-Window 二维判定，在保持快照隔离语义的同时，有效过滤时序图中的伪冲突。

核心正确性条件：
1. `ActualAccessTime(T) ⊆ W(T)`（窗口保守性）
2. 无时间谓词 → `W(T) = (-∞, +∞)`（退化为标准 OCC）
3. `txn_version = commit_timestamp_`（MVCC 语义）
4. 谓词读必须参与冲突检测（幻读防护）
5. TWCD 不替代 2PC，而是减少验证成本
