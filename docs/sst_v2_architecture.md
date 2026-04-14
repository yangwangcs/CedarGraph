# CedarGraph SST V2 架构设计

## 1. 核心设计理念

### "逻辑行排序，物理列拆分"的混合存储

传统行式存储：
```
[Key1|Value1][Key2|Value2][Key3|Value3]...  // 按行存储
```

传统列式存储：
```
[Key1][Key2][Key3]... [Value1][Value2][Value3]...  // 完全按列存储
```

CedarGraph SST V2：
```
Zone 0: [entity_id1, entity_id1, entity_id1, ...]  (RLE: ID1×3)
Zone 1: [ts1, ts2, ts3, ...]                       (Delta-Delta)
Zone 2: [target1, target2, target3, ...]           (Delta/RLE)
Zone 3: [col_ids], [seqs], [types], [flags], [parts]  (分开编码)
Zone 4: [Value1, Value2, Value3, ...]              (Dictionary/LZ4)
```

**关键洞察**：
- 按 `entity_id` 排序后，Zone 0 会出现大量连续重复值 → RLE 压缩率极高
- 同一实体的属性连续存储 → CPU 缓存友好
- 列式拆分 → 查询时可跳过不需要的 Zone

## 2. 全局排序契约 (Global Sorting Contract)

```cpp
int CompareForSorting(const CedarKey& a, const CedarKey& b) {
  // 1. entity_id ASC (首要聚簇关键字)
  if (a.entity_id != b.entity_id) return a.entity_id < b.entity_id ? -1 : 1;
  
  // 2. entity_type ASC (点 vs 边)
  if (a.entity_type != b.entity_type) return a.entity_type < b.entity_type ? -1 : 1;
  
  // 3. column_id ASC (属性/边类型)
  if (a.column_id != b.column_id) return a.column_id < b.column_id ? -1 : 1;
  
  // 4. target_id ASC (邻居排序)
  if (a.target_id != b.target_id) return a.target_id < b.target_id ? -1 : 1;
  
  // 5. timestamp_be DESC (降序 - 最新版本在前)
  if (a.timestamp_be != b.timestamp_be) return a.timestamp_be > b.timestamp_be ? -1 : 1;
  
  // 6. sequence ASC (微秒内序列号)
  if (a.sequence != b.sequence) return a.sequence < b.sequence ? -1 : 1;
  
  return 0;
}
```

**效果**：
- 同一个 `entity_id` 的所有记录在 SST 中物理连续
- 版本链自然按时间降序排列
- 支持高效的范围扫描和点查

## 3. 实体对齐的块设计 (Entity-Aligned Block)

### 3.1 Block 结构

```
┌─────────────────────────────────────────────────────────────┐
│ Block Header                                                │
│   - start_row (全局行号)                                    │
│   - row_count (行数)                                        │
│   - first_entity_id, last_entity_id                         │
├─────────────────────────────────────────────────────────────┤
│ Zone 0: Entity IDs (RLE 编码)                               │
│   Format: [value:8B][run_length:varint]...                  │
│   Example: [(1001, 50), (1002, 30), ...]                    │
├─────────────────────────────────────────────────────────────┤
│ Zone 1: Timestamps (Delta-Delta)                            │
│   Format: [first_ts:8B][first_delta:varint][dod:varint]...  │
│   Example: 100, 99, 98, 95 → 100, -1, -1, -3, ...           │
├─────────────────────────────────────────────────────────────┤
│ Zone 2: Target IDs (Delta or RLE)                           │
│   Strategy: If mostly sequential, use Delta; else RLE       │
├─────────────────────────────────────────────────────────────┤
│ Zone 3: Key Metadata (8B split into 5 fields)               │
│   - column_id (2B): RLE (often same within block)           │
│   - sequence (2B): RLE (mostly 0)                           │
│   - entity_type (1B): Bitmap (2 bits per row)               │
│   - flags (1B): Bitmap (8 bits per row)                     │
│   - part_id (2B): RLE (often same within block)             │
├─────────────────────────────────────────────────────────────┤
│ Zone 4: Values (Dictionary or LZ4)                          │
│   - Dictionary if unique values < 1000                      │
│   - LZ4 otherwise                                           │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 块切割策略

**目标**：尽量避免将同一个 `entity_id` 的数据分割到不同 Block

**决策逻辑**：
```cpp
bool ShouldCutBlock() {
  // 硬性限制：块大小或行数
  if (block_size >= kMaxBlockSize) {
    // 软限制：检查是否适合切割
    if (!IsSameEntityAsNextRow()) return true;
    
    // 同一 entity 积累了足够多，允许切割
    if (SameEntityCount() >= kEntityAlignThreshold) return true;
    
    // 紧急情况：块大小超过 2 倍限制，强制切割
    if (block_size >= kMaxBlockSize * 2) return true;
    
    return false;  // 继续积累
  }
  return false;
}
```

**参数**：
- `kMaxBlockSize` = 64KB (默认)
- `kMaxRowsPerBlock` = 4096
- `kEntityAlignThreshold` = 256 (同一 entity 超过 256 行可切割)

## 4. 各 Zone 的压缩策略

### Zone 0: Entity IDs - RLE (Run-Length Encoding)

**为什么 RLE**：排序后同一 entity 连续出现

```
Input:  [1001, 1001, 1001, 1001, 1001, 1002, 1002, 1003, ...]
RLE:    [(1001, 5), (1002, 2), (1003, 1), ...]
压缩比：通常 10:1 到 100:1
```

**解码优化**：
- 每 8192 行建立 Restart Point
- 二分查找定位 entity_id

### Zone 1: Timestamps - Delta-Delta

**为什么 Delta-Delta**：时间戳降序排列，相邻差值小

```
Input:  [1000000, 999999, 999998, 999995, 999990, ...]
Delta:  [-1, -1, -3, -5, ...]
压缩：使用 Varint 编码小整数
```

### Zone 3: Key Metadata - 分开编码

**Field 1: column_id (2B)**
- 特点：同一 SST 内通常相同
- 编码：RLE，通常只需要 1 个条目

**Field 2: sequence (2B)**
- 特点：99% 为 0
- 编码：RLE

**Field 3: entity_type (1B)**
- 特点：每个 SST 通常只存储一种类型
- 编码：Bitmap (2 bits per row) 或 单字节重复

**Field 4: flags (1B)**
- 特点：包含 OpType (2 bits) + 状态位
- 编码：Bitmap (8 bits per row)

**Field 5: part_id (2B)**
- 特点：同一 SST 通常相同
- 编码：RLE

## 5. 读路径优化

### 5.1 谓词下推 (Predicate Pushdown)

```cpp
// 查询: GetNode(1001) at T=500
void GetNode(uint64_t entity_id, Timestamp ts) {
  // Step 1: Zone Map 过滤 (文件级)
  if (entity_id < sst.min_entity_id || entity_id > sst.max_entity_id)
    return;  // 跳过整个 SST
  
  // Step 2: Bloom Filter 过滤 (Block 级)
  if (!bloom_filter.MayContain(entity_id))
    return;  // 跳过所有 Blocks
  
  // Step 3: Zone 0 范围探测
  auto [start_row, end_row] = FindEntityRangeInZone0(entity_id);
  
  // Step 4: Zone 3 语义过滤
  for (row in [start_row, end_row]) {
    if (GetColumnId(row) != target_column) continue;
    if (GetTimestamp(row) > ts) continue;  // 降序，找第一个 ≤ ts
    
    // Step 5: Zone 4 物化
    return GetValue(row);
  }
}
```

### 5.2 延迟缝合 (Late Materialization)

```cpp
// 扫描: 获取 entity 1001 的所有属性
void ScanEntity(uint64_t entity_id) {
  // 1. 只读 Zone 0 找到范围
  auto range = zone0_decoder.FindRange(entity_id);
  
  // 2. 只读 Zone 3 获取 column_ids
  vector<uint16_t> columns = zone3_decoder.GetColumnIds(range);
  
  // 3. 根据 column_ids 决定读取哪些 Zone 4 条目
  for (size_t i = 0; i < range.count; ++i) {
    if (IsInterestingColumn(columns[i])) {
      Descriptor desc = zone4_decoder.Get(range.start + i);
      Process(desc);
    }
  }
}
```

## 6. Compaction 优化

### 6.1 Zone 同步写入

```cpp
// Compaction 过程中保持 Zone 同步
void CompactSSTs(vector<SST*> inputs, SST* output) {
  // 创建各 Zone 的 Writer
  Zone0Writer z0_writer;
  Zone1Writer z1_writer;
  Zone2Writer z2_writer;
  Zone3Writer z3_writer;
  Zone4Writer z4_writer;
  
  // 多路归并
  PriorityQueue<CedarKey> heap;
  for (auto* input : inputs) {
    heap.Push(input->GetMinKey());
  }
  
  while (!heap.Empty()) {
    auto [key, desc] = heap.Pop();
    
    // 同步写入所有 Zone
    z0_writer.Write(key.entity_id());
    z1_writer.Write(key.timestamp());
    z2_writer.Write(key.target_id());
    z3_writer.Write(key.column_id(), key.sequence(), 
                    key.entity_type(), key.flags(), key.part_id());
    z4_writer.Write(desc);
    
    // 补充堆
    if (input->HasMore()) {
      heap.Push(input->GetNextKey());
    }
  }
  
  // 完成并 Flush 各 Zone
  output->Finalize();
}
```

### 6.2 冷热分离

```cpp
CompactionDecision Filter(const CedarKey& key) {
  if (key.IsTombstone()) {
    // 墓碑：检查保留期
    if (key.timestamp() < Now() - kRetentionPeriod)
      return kRemove;  // 安全删除
    return kKeepAsTombstone;
  }
  
  if (key.timestamp() < Now() - kColdStorageThreshold) {
    // 冷数据迁移
    cold_storage.Write(key, desc);
    return kMoveToCold;
  }
  
  return kKeep;  // 保留在热存储
}
```

## 7. 性能预期

| 指标 | 行式存储 | 列式存储 (V1) | SST V2 (Entity-Aligned) |
|------|---------|--------------|------------------------|
| 压缩比 | 3:1 | 5:1 | 10:1 (RLE+Delta) |
| GetNode | 5ms | 3ms | 1ms (Zone Map+Bloom) |
| ScanEntity | 10ms | 8ms | 2ms (延迟物化) |
| Cache Hit | 60% | 70% | 85% (Entity 局部性) |
| Compaction | 100MB/s | 80MB/s | 120MB/s (Zone 并行) |

## 8. 实现状态

### 已完成 ✅
- [x] 全局排序契约 (`CedarKey::CompareForSorting`)
- [x] 实体对齐的块设计 (`ZoneColumnarSstBuilderV2`)
- [x] Zone 编码器 (RLE, Delta-Delta)
- [x] 大端序一致性 (`htobe16/htobe64`)

### 待实现 📝
- [ ] SST Reader V2 (谓词下推)
- [ ] Compaction Merger (Zone 同步)
- [ ] Cold Storage 集成
- [ ] 性能测试与调优
