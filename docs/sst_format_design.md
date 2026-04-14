# Zone-Columnar SST 格式设计

## 概述

**Zone-Columnar SST** (SST v2) 是专为 CedarGraph 时序属性图数据设计的列式存储格式。它将数据分区到语义化的 Zone 中，每个 Zone 使用针对其数据特性优化的编码算法。

---

## 文件结构

```
┌─────────────────────────────────────────────────────────────────┐
│  ZoneColumnarHeader (256 字节)                                  │
│  - 魔数、版本、标志位                                           │
│  - 列 ID、实体类型、行数                                        │
│  - Zone 信息（偏移量、大小、编码、压缩）                        │
│  - Zone Maps、Restart Points、Bloom Filter 偏移量             │
│  - 最小/最大时间戳和实体 ID（用于过滤）                         │
├─────────────────────────────────────────────────────────────────┤
│  数据 Zone（可变大小）                                          │
│  ├─ Zone 0: Entity IDs (RLE 编码)                               │
│  ├─ Zone 1: Timestamps (Delta-of-Delta 编码)                    │
│  ├─ Zone 2: Target IDs (Delta 或 RLE 编码)                      │
│  ├─ Zone 3: Key Metadata (8B: φ+κ+τ+δ+part_id)                │
│  └─ Zone 4: Values (Dictionary 或 LZ4 压缩)                     │
├─────────────────────────────────────────────────────────────────┤
│  Zone Maps (32 字节 × 5 个 zone)                                │
│  - 每个 zone 的 min/max 值、计数、不重复计数                    │
├─────────────────────────────────────────────────────────────────┤
│  Restart Points (16 字节 × N)                                   │
│  - 稀疏索引，支持二分查找（每 8192 行一个）                     │
├─────────────────────────────────────────────────────────────────┤
│  Bloom Filter（可变大小）                                       │
│  - 快速排除不存在的 key                                         │
├─────────────────────────────────────────────────────────────────┤
│  Entity Index（可选）                                           │
│  - 持久化倒排索引：entity_id → [positions]                      │
├─────────────────────────────────────────────────────────────────┤
│  ZoneColumnarFooter (128 字节)                                  │
│  - 校验和（数据、Header）                                       │
│  - 条目数、大小统计                                             │
│  - 文件号、层级、版本链                                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Header 结构（256 字节）

```cpp
struct ZoneColumnarHeader {
  // 魔数和版本（8 字节）
  uint32_t magic = 0x5A434F4C;  // "ZCOL"
  uint32_t version = 1;

  // 文件元信息（16 字节）
  uint32_t flags;           // 文件标志
  uint16_t column_id;       // 列 ID / 边类型 ID
  uint8_t  entity_type;     // 0=Vertex, 1=EdgeOut, 2=EdgeIn
  uint8_t  reserved1;
  uint32_t row_count;       // 总行数
  uint32_t block_size;      // 块大小（默认 64KB）

  // Zone 信息（变长，zone0-2,4 各 16 字节，zone3 扩展为 48 字节）
  // Zone 0,1,2,4 使用标准 16B 结构
  struct ZoneInfo {
    uint8_t  encoding_type;       // 编码算法
    uint8_t  compression_type;    // 压缩算法
    uint16_t reserved;
    uint32_t data_offset;         // 相对于文件头的偏移
    uint32_t data_size;           // 压缩后大小
    uint32_t uncompressed_size;   // 原始大小
  } zone0, zone1, zone2, zone4;
  
  // Zone 3: Key Metadata (8B 元数据区) - 扩展结构，包含 5 个独立字段
  struct ZoneInfo3 {
    uint8_t  encoding_type;       // 主编码类型
    uint8_t  compression_type;    // 压缩类型
    uint16_t reserved;
    
    // Column ID (φ) - 2B
    uint32_t column_rle_offset;
    uint32_t column_rle_size;
    
    // Sequence (κ) - 2B
    uint32_t seq_rle_offset;
    uint32_t seq_rle_size;
    
    // Entity Type (τ) - 1B
    uint32_t type_bitmap_offset;
    uint32_t type_bitmap_size;
    
    // Flags (δ) - 1B
    uint32_t flags_bitmap_offset;
    uint32_t flags_bitmap_size;
    
    // Part ID - 2B
    uint32_t part_rle_offset;
    uint32_t part_rle_size;
  } zone3;  // Key Metadata: φ+κ+τ+δ+part_id

  // 索引偏移量（24 字节）
  uint32_t zone_maps_offset;
  uint32_t zone_maps_size;
  uint32_t restart_points_offset;
  uint32_t restart_points_count;
  uint32_t bloom_filter_offset;
  uint32_t bloom_filter_size;

  // Footer 和 Entity Index（16 字节）
  uint32_t footer_offset;
  uint32_t reserved2;
  uint32_t entity_index_offset;   // 持久化倒排索引
  uint32_t entity_index_size;

  // 范围信息（32 字节）
  uint64_t min_timestamp;   // 用于时间范围过滤
  uint64_t max_timestamp;
  uint64_t min_entity_id;   // 用于实体 ID 过滤
  uint64_t max_entity_id;

  // 保留字段（48 字节）- 调整后保持 Header 256 字节对齐
  uint8_t reserved[48];
};

static_assert(sizeof(ZoneColumnarHeader) == 256, "Header must be 256 bytes");
```

---

## Zone 编码详情

### Zone 0: Entity IDs (RLE 编码)

**算法**: 行程长度编码 (Run-Length Encoding)  
**格式**: `[value:8B][run_length:varint]...`

```cpp
class EntityIdZoneEncoder {
  // 编码：[entity_id][count] 对
  // 1111, 1111, 1111, 2222, 2222 -> [1111][3][2222][2]
  
  static constexpr size_t kRestartInterval = 8192;
};
```

**解码器特性**:
- 每 8192 行设置一个重启点，支持快速 seek
- 倒排索引（延迟构建）：entity_id → [positions]
- 查询缓存，加速最近查询过的 entity

---

### Zone 1: Timestamps (Delta-of-Delta)

**算法**: Delta-of-Delta + Varint  
**格式**: `[first_ts:8B][first_delta:varint][dod:varint]...`

```cpp
class TimestampZoneEncoder {
  // 按降序编码时间戳
  // 时间戳：[T0, T1, T2, T3, ...]
  // Delta: d1 = T0 - T1, d2 = T1 - T2, ...
  // Delta-of-Delta: dod2 = d2 - d1, dod3 = d3 - d2, ...
  
  static constexpr size_t kRestartInterval = 4096;
};
```

**解码器特性**:
- 初始化时完全解压（zone 较小，访问快）
- 通过排序索引支持二分查找
- `FindTimeRange(start_ts, end_ts)` 用于范围查询

---

### Zone 2: Target IDs (自适应编码)

**算法**: Raw、Delta 或 RLE（自动选择）  
**选择逻辑**:
- 如果 80%+ 与 entity_id 相同：Delta + Bitmap
- 如果行程长度高：RLE
- 否则：Raw varint

```cpp
class TargetIdZoneEncoder {
  enum class EncodingType : uint8_t {
    kRaw = 0,     // Varint 编码的值
    kDelta = 1,   // Bitmap + delta（相对于 entity_id）
    kRle = 2,     // 行程长度编码
  };
};
```

---

### Zone 3: Key Metadata (8B 元数据区)

Zone 3 存储 CedarKey 的 8B 元数据区（offset 24-31），包含分布式分区信息和操作类型：

| 字段 | 大小 | 符号 | 编码方式 | 说明 |
|------|------|------|----------|------|
| `column_id` | 2B | φ | Column RLE | Predicate ID（边类型/属性 ID）|
| `sequence` | 2B | κ | Sequence RLE | 微秒内版本序列号（99% 为 0）|
| `entity_type` | 1B | τ | Type Bitmap | 3 种类型（Vertex/EdgeOut/EdgeIn）|
| `flags` | 1B | δ | Flags Bitmap | 8 位状态（OpType + 分布式标记）|
| `part_id` | 2B | - | Part RLE | 分布式分区 ID（0-65535）|

**总大小**: 8 字节/行

```cpp
class KeyMetadataZoneEncoder {
  struct EncodedResult {
    std::string column_rle;       // Column ID RLE (通常在一个 SST 内相同)
    std::string sequence_rle;     // Sequence RLE (99% 为 0)
    std::string type_bitmap;      // Entity Type (2 bit/行)
    std::string flags_bitmap;     // Flags (8 bit/行)
    std::string part_rle;         // Part ID RLE (同一 SST 通常相同)
    uint32_t count;
  };
};
```

**各字段编码策略**:
- **Column ID**: 同一 SST 文件通常存储同一列/边类型，RLE 压缩率极高
- **Sequence**: 99% 为 0，RLE 编码 `[0][count]`
- **Entity Type**: 3 种类型用 2 bit/行，或单独的字节数组
- **Flags**: 8 bit/行，包含 OpType (bit 0-1) + 分布式状态 (bit 2-5) + Tombstone (bit 7)
- **Part ID**: 同一 SST 通常属于同一分区，RLE 编码效率高

**分布式场景优化**:
- `part_id` 放在 Zone 3 最后，SST 分裂/迁移时可快速过滤
- `flags` 中的 `is_distributed` (bit 2) 标记是否启用分区
- `flags` 中的 `is_locked` (bit 5) 支持 Percolator 分布式事务

---

### Zone 4: Values (Dictionary 或压缩)

**算法选择**:
```cpp
class ValueZoneEncoder {
  static constexpr size_t kDictionaryThreshold = 1000;
  
  enum class EncodingType : uint8_t {
    kDictionary = 0,  // 如果不重复值 < 1000
    kLz4 = 1,         // LZ4 压缩
    kZstd = 2,        // Zstd 压缩
    kRaw = 3,         // 不压缩
  };
};
```

**字典编码**:
- 构建不重复值的字典
- 存储索引（根据字典大小使用 1-4 字节）

**Blob 存储**:
- 值 > 6 字节 → 存储到 `.blob` 文件
- Descriptor 存储：`offset (4B) + size_kb (2B) + checksum (1B)`

---

## 索引结构

### Zone Map Entry（每个 zone 32 字节）

```cpp
struct ZoneMapEntry {
  uint64_t min_value;       // zone 中的最小值
  uint64_t max_value;       // zone 中的最大值
  uint64_t count;           // 条目数
  uint64_t distinct_count;  // 用于字典编码决策
};
```

**用途**: 查询时快速排除 SST 文件

---

### Restart Point（16 字节）

```cpp
struct ZoneRestartPoint {
  uint64_t entity_id;       // 该块的首个 entity_id
  uint32_t timestamp_hi;    // 时间戳高 32 位
  uint32_t row_index;       // 行索引（相对）
};
```

**位置**: 每 8192 行一个  
**用途**: Seek 操作的二分查找

---

### Bloom Filter

- **每 key 的 bit 数**: 10
- **哈希函数数**: ~7 (计算为 bits_per_key × ln(2))
- **用途**: 快速排除不存在的 key

---

### Entity Index（可选，持久化）

**格式**:
```
[num_entries:4B]
[entity_id:8B][num_positions:4B][pos:4B]...
[entity_id:8B][num_positions:4B][pos:4B]...
...
```

**用途**: O(1) 查找 entity 的所有位置  
**状态**: 当前已禁用（使用内存索引替代）

---

## Footer 结构（128 字节）

```cpp
struct ZoneColumnarFooter {
  // 校验和（16 字节）
  uint64_t data_checksum;     // 数据区的 CRC64
  uint64_t header_checksum;   // Header 的 CRC64

  // 统计信息（32 字节）
  uint64_t entry_count;       // 总条目数
  uint64_t uncompressed_size; // 压缩前大小
  uint64_t compressed_size;   // 压缩后大小
  uint64_t index_size;        // 索引总大小

  // 版本链（16 字节）
  uint64_t file_number;       // 当前文件 ID
  uint64_t prev_file_number;  // 前一个文件（用于 GC）

  // 层级信息（8 字节）
  uint32_t level;             // SST 层级 (L0, L1, ...)
  uint32_t sequence;          // 文件序列号

  // 编码统计（16 字节）
  float compression_ratio;    // 压缩比
  uint32_t encoding_time_us;  // 编码耗时（微秒）
  uint32_t reserved1;
  uint32_t reserved2;

  // 保留字段（40 字节）
  uint8_t reserved[40];
};
```

---

## Blob 文件格式

对于大值（>6 字节），SST 使用配套的 `.blob` 文件：

```
sst_000001.sst  <->  sst_000001.blob
```

### Blob 文件结构

```
┌─────────────────────────────────────┐
│  BlobHeader (4096 字节)             │
│  - 魔数 (0x424C4200 = "BLB")        │
│  - 版本                             │
│  - SST ID                           │
│  - 条目数                           │
│  - 数据大小                         │
├─────────────────────────────────────┤
│  Blob Entries (4KB 对齐)            │
│  ├─ [size:4B][data:可变]            │
│  ├─ [size:4B][data:可变]            │
│  └─ ...                             │
└─────────────────────────────────────┘
```

### Blob Entry Header（12 字节）

```cpp
struct BlobEntryHeader {
  uint32_t size;        // 数据大小
  uint32_t checksum;    // CRC32
  uint8_t  compression; // 压缩类型
  uint8_t  reserved[3];
};
```

**对齐**: 所有条目 4KB 对齐，支持 O_DIRECT

---

## 查询操作

### 点查 (Get)

```
1. 检查 Zone Map (min/max entity_id) - 快速排除
2. 检查 Bloom Filter - 可能包含？
3. 使用 Entity Index 或 Decoder::FindEntityPositions
4. 对每个 position，检查 timestamp (Zone 1)
5. 返回匹配的值 (Zone 4)
```

### 范围查询 (GetRange)

```
1. 检查 Zone Map 时间范围是否重叠 - 快速排除
2. 使用 Timestamp Decoder::FindTimeRange(start, end)
3. 对范围内的每个 position：
   a. 检查 entity_id (Zone 0) - 过滤
   b. 返回值 (Zone 4) - 延迟物化
```

### 扫描 (Iterator)

```
1. 通过二分查找定位到 restart point
2. 顺序读取各 zone
3. 从 zone 数据重建 CedarKey
```

---

## 性能特征

| 操作 | 时间复杂度 | 说明 |
|------|-----------|------|
| Open | O(1) | 内存映射或缓冲区分配 |
| Get | O(log N) | Restart points 的二分查找 |
| GetRange | O(log N + K) | K = 范围内的结果数 |
| Seek | O(log N) | 二分查找 |
| Next | O(1) | 顺序访问 |
| Zone Map Check | O(1) | 常数时间过滤 |
| Bloom Filter | O(1) | 哈希查找 |

---

## 压缩统计

| Zone | 编码 | 典型压缩比 |
|------|------|-----------|
| Zone 0 (Entity IDs) | RLE | 2-10x |
| Zone 1 (Timestamps) | Delta-of-Delta | 3-20x |
| Zone 2 (Target IDs) | Delta/RLE | 2-5x |
| Zone 3 (Metadata) | RLE + Bitmap | 5-50x |
| Zone 4 (Values) | Dictionary/LZ4 | 2-10x |

---

## 设计原理

### 为什么是 5 个 Zone？

1. **Zone 0 (Entity IDs)**: 频繁查询，RLE 适合重复 ID
2. **Zone 1 (Timestamps)**: 时间范围查询，Delta-of-Delta 利用时序局部性
3. **Zone 2 (Target IDs)**: 图遍历，Delta 适合邻居局部性
4. **Zone 3 (Metadata)**: Tombstone 检查，RLE 适合大多数为 0 的 sequence
5. **Zone 4 (Values)**: 延迟物化，与 key 分离

### 为什么是列式？

- **扫描**: 只读取需要的列
- **压缩**: 同类型数据压缩效果更好
- **向量化**: 支持 SIMD 处理
- **延迟物化**: 先过滤，后读值

### 为什么使用独立的 Blob 文件？

- **小 SST 尺寸**: 索引可以放入内存
- **大值流式传输**: 随机访问无惩罚
- **Compaction 效率**: Blob 引用不需要拷贝

---

## 参考

- `include/cedar/sst/zone_columnar_format.h` - SST 格式定义
- `include/cedar/sst/zone_encoder.h` - Zone 编码器
- `docs/cedar_key_design.md` - CedarKey (32B) 格式
