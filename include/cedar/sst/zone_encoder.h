// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// Zone-Columnar Encoder - Zone-Columnar SST (SST v2) 核心编码器
// =============================================================================
// 实现 4-Zone 列式存储编码：
// - Zone 0: Entity IDs (RLE - Run-Length Encoding)
// - Zone 1: Timestamps (Delta-of-Delta + Varint)
// - Zone 2: Target IDs (Delta 或 RLE)
// - Zone 3: Values (Dictionary Encoding 或 LZ4/Zstd)
// =============================================================================

#ifndef FERN_ZONE_ENCODER_H_
#define FERN_ZONE_ENCODER_H_

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <optional>

#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// =============================================================================
// Zone 类型定义
// =============================================================================
enum class ZoneType : uint8_t {
  kEntityIds = 0,     // Zone 0: Entity IDs
  kTimestamps = 1,    // Zone 1: Timestamps
  kTargetIds = 2,     // Zone 2: Target IDs
  kKeyMetadata = 3,   // Zone 3: Key Metadata (sequence + flags)
  kValues = 4,        // Zone 4: Values (Descriptors)
  kCount = 5
};

// Zone 统计信息（Zone Map）
struct ZoneMap {
  uint64_t min_value = 0;
  uint64_t max_value = 0;
  uint64_t count = 0;
  uint64_t distinct_count = 0;  // 用于决定是否使用字典编码
  
  void Reset() {
    min_value = UINT64_MAX;
    max_value = 0;
    count = 0;
    distinct_count = 0;
  }
  
  void Update(uint64_t value) {
    if (value < min_value) min_value = value;
    if (value > max_value) max_value = value;
    count++;
  }
};

// =============================================================================
// Varint 编码/解码辅助函数
// =============================================================================
class VarintCodec {
 public:
  // 编码 uint64_t 为 varint，返回写入的字节数
  static size_t Encode(uint64_t value, char* buf);
  static void Encode(uint64_t value, std::string& dst);
  
  // 编码 int64_t (有符号，使用 ZigZag 编码)
  static size_t EncodeSigned(int64_t value, char* buf);
  static void EncodeSigned(int64_t value, std::string& dst);
  
  // 解码 varint，返回解码后的值和解码的字节数
  static std::pair<uint64_t, size_t> Decode(const char* buf, size_t max_len);
  static std::pair<int64_t, size_t> DecodeSigned(const char* buf, size_t max_len);
  
  // 计算编码后的大小（不实际编码）
  static size_t Size(uint64_t value);
};

// =============================================================================
// Zone 0: Entity IDs 编码器 (RLE)
// =============================================================================
// 使用 Run-Length Encoding，适合连续相同的 Entity ID
// 格式: [value:8B][run:varint]...
class EntityIdZoneEncoder {
 public:
  // 编码一组 Entity IDs
  static std::string Encode(const std::vector<uint64_t>& ids);
  
  // 带统计信息的编码
  static std::string Encode(const std::vector<uint64_t>& ids, ZoneMap* zone_map);
  
  // 解码器
  class Decoder {
   public:
    explicit Decoder(const std::string& data);
    
    // 获取指定索引的 Entity ID
    uint64_t Get(size_t idx) const;
    
    // 批量获取
    void GetRange(size_t start_idx, size_t count, std::vector<uint64_t>* out) const;
    
    // 二分查找指定 Entity ID 的所有位置
    // 返回: 该 Entity ID 出现的所有索引（已解压后的逻辑索引）
    std::vector<size_t> FindEntityPositions(uint64_t entity_id) const;
    
    // OPTIMIZATION: 使用 Bitmap 索引快速检查 entity 是否存在
    // 返回: true = 可能存在, false = 肯定不存在
    bool MayContainEntity(uint64_t entity_id) const;
    
    // 获取总条目数
    size_t Count() const { return total_count_; }
    
   private:
    const std::string& data_;
    mutable std::vector<std::pair<uint64_t, size_t>> restart_points_;  // 重启点
    size_t total_count_ = 0;
    
    // OPTIMIZATION: Entity ID -> Positions 的倒排索引（延迟构建）
    mutable std::unordered_map<uint64_t, std::vector<size_t>> entity_index_;
    mutable bool index_built_ = false;
    
    // OPTIMIZATION: 查询结果缓存（最近查询的 entity positions）
    static constexpr size_t kCacheSize = 64;
    mutable std::vector<std::pair<uint64_t, std::vector<size_t>>> position_cache_;
    mutable size_t cache_hits_ = 0;
    mutable size_t cache_misses_ = 0;
    
    void BuildRestartPoints() const;
    void BuildEntityIndex() const;  // 构建倒排索引
    void AddToCache(uint64_t entity_id, const std::vector<size_t>& positions) const;
    const std::vector<size_t>* GetFromCache(uint64_t entity_id) const;
  };
  
 private:
  static constexpr size_t kRestartInterval = 8192;  // 每 8192 行一个重启点
};

// =============================================================================
// Zone 1: Timestamps 编码器 (Delta-of-Delta)
// =============================================================================
// 利用时间戳的有序性，使用 Delta-of-Delta 编码
// 格式: [first_ts:8B][first_delta:varint][dod:varint]...
class TimestampZoneEncoder {
 public:
  // 编码一组时间戳（假设已按降序排列）
  static std::string Encode(const std::vector<uint64_t>& timestamps);
  
  // 带统计信息的编码
  static std::string Encode(const std::vector<uint64_t>& timestamps, ZoneMap* zone_map);
  
  // 解码器
  class Decoder {
   public:
    explicit Decoder(const std::string& data);
    
    // 获取指定索引的时间戳
    uint64_t Get(size_t idx) const;
    
    // 批量获取
    void GetRange(size_t start_idx, size_t count, std::vector<uint64_t>* out) const;
    
    // 查找时间戳范围内的所有索引
    // 返回: [start_idx, end_idx) 范围内的所有索引
    std::vector<size_t> FindTimeRange(uint64_t start_ts, uint64_t end_ts) const;
    
    // OPTIMIZATION: 使用二分查找快速定位时间戳范围
    // 返回: 大于等于 target_ts 的最小索引（降序排列）
    size_t LowerBound(uint64_t target_ts) const;
    // 返回: 小于等于 target_ts 的最大索引（降序排列）
    size_t UpperBound(uint64_t target_ts) const;
    
    // 获取总条目数
    size_t Count() const { return total_count_; }
    
    // 获取最小/最大时间戳
    uint64_t MinTimestamp() const { return min_ts_; }
    uint64_t MaxTimestamp() const { return max_ts_; }
    
   private:
    const std::string& data_;
    mutable std::vector<std::pair<uint64_t, size_t>> restart_points_;
    size_t total_count_ = 0;
    uint64_t first_ts_ = 0;
    int64_t first_delta_ = 0;
    uint64_t min_ts_ = UINT64_MAX;
    uint64_t max_ts_ = 0;
    mutable bool decompressed_ = false;
    
    void BuildRestartPoints() const;
    void DecodeUpTo(size_t idx) const;
    mutable std::vector<uint64_t> decoded_cache_;
    mutable size_t decoded_up_to_ = 0;
    
    // OPTIMIZATION: 构建索引以支持二分查找
    // 由于 decoded_cache_ 是降序的，我们需要一个升序索引来支持二分查找
    mutable std::vector<size_t> sorted_index_;  // 升序排列的索引
    mutable bool index_built_ = false;
    void BuildSortedIndex() const;
  };
  
 private:
  static constexpr size_t kRestartInterval = 4096;  // 每 4096 行一个重启点
};

// =============================================================================
// Zone 2: Target IDs 编码器 (Delta 或 RLE)
// =============================================================================
// 策略: 如果 Target ID 与 Entity ID 差值较小，使用 Delta 编码
//       如果 Target ID 连续相同，使用 RLE 编码
class TargetIdZoneEncoder {
 public:
  // 编码一组 Target IDs
  // entity_ids: 对应的 Entity IDs（用于 Delta 编码决策）
  static std::string Encode(const std::vector<uint64_t>& target_ids,
                            const std::vector<uint64_t>& entity_ids);
  
  // 带统计信息的编码
  static std::string Encode(const std::vector<uint64_t>& target_ids,
                            const std::vector<uint64_t>& entity_ids,
                            ZoneMap* zone_map);
  
  // 编码类型
  enum class EncodingType : uint8_t {
    kRaw = 0,      // 原始值
    kDelta = 1,    // Delta 编码 (相对于 Entity ID)
    kRle = 2,      // RLE 编码
  };
  
  // 解码器
  class Decoder {
   public:
    Decoder(const std::string& data, EncodingType type);
    
    // 获取指定索引的 Target ID
    uint64_t Get(size_t idx, const std::vector<uint64_t>& entity_ids) const;
    
    // 批量获取
    void GetRange(size_t start_idx, size_t count, 
                  const std::vector<uint64_t>& entity_ids,
                  std::vector<uint64_t>* out) const;
    
    // 获取总条目数
    size_t Count() const { return total_count_; }
    
   private:
    const std::string& data_;
    EncodingType type_;
    size_t total_count_ = 0;
  };
  
 private:
  static constexpr size_t kRestartInterval = 8192;
};

// =============================================================================
// Zone 3: Key Metadata 编码器 (8B 元数据区: φ+κ+τ+δ+part_id)
// =============================================================================
// 存储 CedarKey offset 24-31 的完整元数据区：
// - column_id (2B): φ - Predicate ID
// - sequence (2B): κ - 微秒内版本序列号
// - entity_type (1B): τ - Vertex/EdgeOut/EdgeIn
// - flags (1B): δ - OpType + 分布式状态
// - part_id (2B): 分布式分区 ID
// =============================================================================
class KeyMetadataZoneEncoder {
 public:
  // Key 元数据条目 - 对应 CedarKey 的 8B 元数据区
  struct MetadataEntry {
    uint16_t column_id;     // φ: Predicate ID
    uint16_t sequence;      // κ: intra-microsecond ordering
    uint8_t  entity_type;   // τ: 0=Vertex, 1=EdgeOut, 2=EdgeIn
    uint8_t  flags;         // δ: OpType(bit 0-1) + is_distributed(bit 2) + is_locked(bit 5) + tombstone(bit 7)
    uint16_t part_id;       // Distributed Partition ID (0-65535)
    
    MetadataEntry() : column_id(0), sequence(0), entity_type(0), flags(0), part_id(0) {}
    MetadataEntry(uint16_t col, uint16_t seq, uint8_t type, uint8_t f, uint16_t part)
        : column_id(col), sequence(seq), entity_type(type), flags(f), part_id(part) {}
  };
  
  // 编码一组 Key 元数据
  static std::string Encode(const std::vector<MetadataEntry>& entries);
  
  // 编码辅助：从 CedarKey 提取
  static std::string Encode(const std::vector<CedarKey>& keys);
  
  // 编码结果 - 各字段独立编码，支持延迟物化
  struct EncodedResult {
    std::string column_rle;       // Column ID RLE (通常在一个 SST 内相同)
    std::string sequence_rle;     // Sequence RLE (99% 为 0)
    std::string type_bitmap;      // Entity Type (2 bit/行，或字节数组)
    std::string flags_bitmap;     // Flags (8 bit/行)
    std::string part_rle;         // Part ID RLE (同一 SST 通常相同)
    uint32_t count = 0;
  };
  
  static EncodedResult EncodeWithResult(const std::vector<MetadataEntry>& entries);
  
  // 解码器
  class Decoder {
   public:
    Decoder(const std::string& column_rle,
            const std::string& sequence_rle,
            const std::string& type_bitmap,
            const std::string& flags_bitmap,
            const std::string& part_rle,
            uint32_t count);
    
    // 获取指定索引的完整元数据
    MetadataEntry Get(size_t idx) const;
    
    // 批量获取
    void GetRange(size_t start_idx, size_t count, std::vector<MetadataEntry>* out) const;
    
    // 获取单个字段 (延迟物化优化)
    uint16_t GetColumnId(size_t idx) const;
    uint16_t GetSequence(size_t idx) const;
    uint8_t GetEntityType(size_t idx) const;
    uint8_t GetFlags(size_t idx) const;
    uint16_t GetPartId(size_t idx) const;
    
    // 检查操作类型
    uint8_t GetOpType(size_t idx) const;  // bit 0-1: 0=CREATE, 1=UPDATE, 2=DELETE
    bool IsCreate(size_t idx) const;
    bool IsUpdate(size_t idx) const;
    bool IsDelete(size_t idx) const;
    
    // 检查状态位
    bool IsDistributed(size_t idx) const;  // bit 2
    bool HasVInline(size_t idx) const;     // bit 3
    bool IsCompressed(size_t idx) const;   // bit 4
    bool IsLocked(size_t idx) const;       // bit 5
    bool IsTombstone(size_t idx) const;    // bit 7
    
    // 分区过滤 (分布式场景)
    bool IsInPartition(uint16_t part_id) const;  // 检查是否属于指定分区
    std::vector<size_t> FindPartitionRows(uint16_t part_id) const;  // 找到所有属于该分区的行
    
    // 获取总条目数
    uint32_t Count() const { return count_; }
    
    // 统计 Tombstone 数量
    uint32_t CountTombstones() const;
    
   private:
    std::string column_rle_;
    std::string sequence_rle_;
    std::string type_bitmap_;
    std::string flags_bitmap_;
    std::string part_rle_;
    uint32_t count_ = 0;
    
    // 解压后的缓存 (延迟解压)
    mutable std::vector<uint16_t> column_cache_;
    mutable std::vector<uint16_t> sequence_cache_;
    mutable std::vector<uint8_t>  type_cache_;
    mutable std::vector<uint8_t>  flags_cache_;
    mutable std::vector<uint16_t> part_cache_;
    mutable bool decoded_ = false;
    
    void EnsureDecoded() const;
  };
  
 private:
  // RLE 编码/解码
  static std::string EncodeRLE(const std::vector<uint16_t>& values);
  static std::vector<uint16_t> DecodeRLE(const std::string& data, uint32_t count);
  
  // Bitmap 编码/解码 (用于 2-bit 或 8-bit 数据)
  static std::string EncodeBitmap2(const std::vector<uint8_t>& values);   // 2 bit/value
  static std::string EncodeBitmap8(const std::vector<uint8_t>& values);   // 8 bit/value
  static std::vector<uint8_t> DecodeBitmap2(const std::string& data, uint32_t count);
  static std::vector<uint8_t> DecodeBitmap8(const std::string& data, uint32_t count);
};

// =============================================================================
// Zone 4: Values 编码器 (Dictionary 或 LZ4/Zstd)
// =============================================================================
// 策略: 如果唯一值数量 < 阈值，使用字典编码
//       否则使用 LZ4/Zstd 压缩
class ValueZoneEncoder {
 public:
  // 字典编码阈值
  static constexpr size_t kDictionaryThreshold = 1000;
  
  // 编码一组 Values
  static std::string Encode(const std::vector<Descriptor>& values);
  
  // 带统计信息的编码
  static std::string Encode(const std::vector<Descriptor>& values, ZoneMap* zone_map);
  
  // 编码类型
  enum class EncodingType : uint8_t {
    kDictionary = 0,   // 字典编码
    kLz4 = 1,          // LZ4 压缩
    kZstd = 2,         // Zstd 压缩
    kRaw = 3,          // 原始数据
  };
  
  // 编码结果
  struct EncodedResult {
    EncodingType type;
    std::string data;
    std::vector<std::string> dictionary;  // 仅字典编码时使用
  };
  
  static EncodedResult EncodeWithType(const std::vector<Descriptor>& values);
  
  // 解码器
  class Decoder {
   public:
    Decoder(const std::string& data, EncodingType type,
            const std::vector<std::string>& dictionary = {});
    
    // 获取指定索引的 Value
    Descriptor Get(size_t idx) const;
    
    // 批量获取
    void GetRange(size_t start_idx, size_t count, std::vector<Descriptor>* out) const;
    
    // 选择性读取：只读取指定索引的值（延迟物化）
    void GetSelective(const std::vector<size_t>& indices, 
                      std::vector<Descriptor>* out) const;
    
    // 获取总条目数
    size_t Count() const { return total_count_; }
    
   private:
    const std::string& data_;
    EncodingType type_;
    std::vector<std::string> dictionary_;
    size_t total_count_ = 0;
    
    // 解压后的缓存（延迟解压）
    mutable std::vector<Descriptor> decompressed_cache_;
    mutable bool decompressed_ = false;
    
    void EnsureDecompressed() const;
  };
};

// =============================================================================
// Zone 数据容器 - 用于构建 SST 文件
// =============================================================================
struct ZoneData {
  ZoneType type;
  std::string encoded_data;
  ZoneMap zone_map;
  
  // 编码类型（各 Zone 专用）
  union {
    ValueZoneEncoder::EncodingType value_encoding;  // Zone 4 (Values)
    TargetIdZoneEncoder::EncodingType target_encoding;  // Zone 2 (TargetIds)
    uint8_t raw_encoding = 0;
  } encoding;
  
  // Key Metadata Zone 专用字段 (Zone 3: 8B 元数据区 φ+κ+τ+δ+part_id)
  std::string column_rle;       // Column ID (φ)
  std::string sequence_rle;     // Sequence (κ)
  std::string type_bitmap;      // Entity Type (τ)
  std::string flags_bitmap;     // Flags (δ)
  std::string part_rle;         // Part ID
  
  // Timestamps Zone 专用字段 - 存储原始时间戳用于统一编码
  std::vector<uint64_t> raw_timestamps;  // 仅 Zone 1 使用
  
  ZoneData() = default;
  ZoneData(ZoneType t, std::string data, ZoneMap map)
      : type(t), encoded_data(std::move(data)), zone_map(map) {}
  
  // 用于 Key Metadata Zone 的构造函数
  static ZoneData MakeKeyMetadata(const std::string& column_rle,
                                   const std::string& seq_rle,
                                   const std::string& type_bitmap,
                                   const std::string& flags_bitmap,
                                   const std::string& part_rle,
                                   uint32_t count);
};

// =============================================================================
// ZoneColumnar Builder - 将行数据转换为 Zone-Columnar 格式
// =============================================================================
class ZoneColumnarBuilder {
 public:
  // 添加一行数据
  void Add(const CedarKey& key, const Descriptor& value);
  
  // 完成构建，返回各 Zone 的编码数据
  std::vector<ZoneData> Finish();
  
  // 获取当前条目数
  size_t Count() const { return entity_ids_.size(); }
  
  // 清空数据
  void Reset();
  
  // 预留空间
  void Reserve(size_t count);
  
  // 重建指定行的完整 CedarKey
  CedarKey ReconstructKey(size_t idx, uint16_t column_id, uint8_t entity_type) const;
  
 private:
  std::vector<uint64_t> entity_ids_;
  std::vector<uint64_t> timestamps_;
  std::vector<uint64_t> target_ids_;
  // Zone 3: Key Metadata (8B: φ+κ+τ+δ+part_id) - 5 个独立字段
  std::vector<uint16_t> column_ids_;      // φ: Predicate ID
  std::vector<uint16_t> sequences_;       // κ: intra-microsecond ordering
  std::vector<uint8_t>  entity_types_;    // τ: NODE/EDGE/PROP
  std::vector<uint8_t>  flags_;           // δ: OpType + status bits
  std::vector<uint16_t> part_ids_;        // Distributed Partition ID
  std::vector<Descriptor> values_;
  
  // 压缩阈值
  static constexpr size_t kBlockSize = 64 * 1024;  // 64KB 块大小
};

// =============================================================================
// ZoneColumnar Reader - 读取 Zone-Columnar 格式数据
// =============================================================================
class ZoneColumnarReader {
 public:
  // 从各 Zone 数据初始化
  void Init(const std::vector<ZoneData>& zones);
  
  // 初始化单个 Zone
  void InitZone(ZoneType type, const std::string& data);
  
  // 点查：获取指定行的数据
  bool Get(size_t idx, CedarKey* key, Descriptor* value) const;
  
  // 范围扫描：扫描指定范围的行
  void ScanRange(size_t start_idx, size_t count,
                 std::function<void(size_t, const CedarKey&, const Descriptor&)> callback) const;
  
  // 按 Entity ID 查找（利用 Zone 0 的索引）
  // 返回匹配的所有行索引
  std::vector<size_t> FindByEntityId(uint64_t entity_id) const;
  
  // 按时间戳范围查找（利用 Zone 1 的索引）
  // 返回时间戳在 [start_ts, end_ts] 范围内的所有行索引
  std::vector<size_t> FindByTimeRange(uint64_t start_ts, uint64_t end_ts) const;
  
  // 延迟物化查询：先过滤索引，再读取 Values
  // predicate: 返回 true 表示需要读取该行的 Value
  void ScanWithLateMaterialization(
      std::function<bool(size_t, uint64_t entity_id, uint64_t timestamp)> predicate,
      std::function<void(size_t, const CedarKey&, const Descriptor&)> callback) const;
  
  // 获取总条目数
  size_t Count() const { return count_; }
  
  // Zone Map 过滤：检查 SST 是否可能包含目标数据
  bool ZoneMapCheck(uint64_t entity_id) const;
  bool ZoneMapCheck(uint64_t start_ts, uint64_t end_ts) const;
  
  // 重建完整的 32B CedarKey
  // 需要传入 column_id 和 entity_type（从 SST Header 获取）
  CedarKey ReconstructKey(size_t idx, uint16_t column_id, uint8_t entity_type) const;
  
  // 检查指定行是否是 Tombstone
  bool IsTombstone(size_t idx) const;
  
  // 获取指定行的 Sequence
  uint16_t GetSequence(size_t idx) const;
  
  // 获取指定行的 Flags
  uint8_t GetFlags(size_t idx) const;
  
  // 统计 Tombstone 数量
  size_t CountTombstones() const;
  
 private:
  std::optional<EntityIdZoneEncoder::Decoder> entity_decoder_;
  std::optional<TimestampZoneEncoder::Decoder> timestamp_decoder_;
  std::optional<TargetIdZoneEncoder::Decoder> target_decoder_;
  std::optional<KeyMetadataZoneEncoder::Decoder> metadata_decoder_;
  std::optional<ValueZoneEncoder::Decoder> value_decoder_;
  
  ZoneMap entity_zone_map_;
  ZoneMap timestamp_zone_map_;
  
  size_t count_ = 0;
  
  void ValidateConsistency();
};

}  // namespace cedar

#endif  // FERN_ZONE_ENCODER_H_
