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
// CedarKey - CedarGraph 标准 Key 设计
// =============================================================================
// 32 字节固定长度，针对现代 CPU 优化：
// - 缓存行对齐：64B 缓存行可放 2 个 Key
// - SIMD 友好：8B 对齐，memcmp 可用 AVX2
// - 固定长度：简化比较逻辑，无分支预测失败
// - 降序时间戳：最新版本自然排前
// - 双向边支持：EdgeOut + EdgeIn 实现 O(log N) 入边查询
// =============================================================================

#ifndef CEDAR_FERN_KEY_H_
#define CEDAR_FERN_KEY_H_

#include <cstdint>
#include <cstring>
#include <limits>
#include <chrono>
#include <optional>
#include <string>
#include <string_view>

// 字节序转换（跨平台）
#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define cedar_htobe64(x) OSSwapHostToBigInt64(x)
#define cedar_be64toh(x) OSSwapBigToHostInt64(x)
#define cedar_htobe16(x) OSSwapHostToBigInt16(x)
#define cedar_be16toh(x) OSSwapBigToHostInt16(x)
#elif defined(__linux__)
#include <endian.h>
#define cedar_htobe64(x) htobe64(x)
#define cedar_be64toh(x) be64toh(x)
#define cedar_htobe16(x) htobe16(x)
#define cedar_be16toh(x) be16toh(x)
#else
// 通用实现
inline uint64_t cedar_htobe64(uint64_t x) {
  #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return ((x & 0xFF00000000000000ULL) >> 56) |
         ((x & 0x00FF000000000000ULL) >> 40) |
         ((x & 0x0000FF0000000000ULL) >> 24) |
         ((x & 0x000000FF00000000ULL) >> 8)  |
         ((x & 0x00000000FF000000ULL) << 8)  |
         ((x & 0x0000000000FF0000ULL) << 24) |
         ((x & 0x000000000000FF00ULL) << 40) |
         ((x & 0x00000000000000FFULL) << 56);
  #else
  return x;
  #endif
}
inline uint64_t cedar_be64toh(uint64_t x) { return cedar_htobe64(x); }
inline uint16_t cedar_htobe16(uint16_t x) {
  #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return ((x & 0xFF00) >> 8) | ((x & 0x00FF) << 8);
  #else
  return x;
  #endif
}
inline uint16_t cedar_be16toh(uint16_t x) { return cedar_htobe16(x); }
#endif

namespace cedar {

// =============================================================================
// 时间戳（微秒级）- 降序存储编码
// =============================================================================

class Timestamp {
 public:
  Timestamp(uint64_t micros = 0) : value_(micros) {}
  
  uint64_t value() const { return value_; }
  
  // 转换为用于降序存储的大端序
  // 原理：时间戳越大（越新），存储值越小（排前面）
  uint64_t EncodeForStorage() const {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    return cedar_htobe64(max - value_);
  }
  
  // 从存储格式解码
  static Timestamp DecodeFromStorage(uint64_t stored_be) {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    uint64_t decoded = max - cedar_be64toh(stored_be);
    return Timestamp(decoded);
  }
  
  static Timestamp Max() { return Timestamp(std::numeric_limits<uint64_t>::max()); }
  static Timestamp Min() { return Timestamp(0); }
  /// 静态属性时间戳（值为1，区分于Min()=0）
  static Timestamp Static() { return Timestamp(1); }
  /// 检查是否为静态属性时间戳
  bool IsStatic() const { return value_ == 1; }
  static Timestamp Now() {
    // Get current time in microseconds since epoch
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
    return Timestamp(static_cast<uint64_t>(micros));
  }
  
  // 显式转换到 uint64_t
  explicit operator uint64_t() const { return value_; }
  
  bool operator==(const Timestamp& other) const { return value_ == other.value_; }
  bool operator!=(const Timestamp& other) const { return value_ != other.value_; }
  bool operator<(const Timestamp& other) const { return value_ < other.value_; }
  bool operator>(const Timestamp& other) const { return value_ > other.value_; }
  bool operator<=(const Timestamp& other) const { return value_ <= other.value_; }
  bool operator>=(const Timestamp& other) const { return value_ >= other.value_; }

 private:
  uint64_t value_;
};

// =============================================================================
// 强类型 ID（编译期类型安全）
// =============================================================================

/// 点的列ID（属性ID）- 仅用于 Vertex
struct VertexColumnId {
  uint16_t value;
  explicit constexpr VertexColumnId(uint16_t v) : value(v) {}
  operator uint16_t() const { return value; }
};

/// 边的类型ID - 仅用于 Edge
/// 注意：由于 Descriptor 的 column_id 只有 12bit，edge_type 必须限制在 0-4095 范围内
/// hash 计算时应使用 & 0x0FFF 而非 & 0xFFFF
struct EdgeTypeId {
  uint16_t value;
  explicit constexpr EdgeTypeId(uint16_t v) : value(v) {}
  operator uint16_t() const { return value; }
};

// 便捷字面量
inline constexpr VertexColumnId operator""_vcol(unsigned long long v) {
  return VertexColumnId(static_cast<uint16_t>(v));
}
inline constexpr EdgeTypeId operator""_etype(unsigned long long v) {
  return EdgeTypeId(static_cast<uint16_t>(v));
}

// =============================================================================
// 实体类型
// =============================================================================

enum class EntityType : uint8_t {
  Vertex = 0,    // 点
  EdgeOut = 1,   // 出边 (src->dst)
  EdgeIn = 2,    // 入边 (dst<-src)，用于快速反向查询
};

// =============================================================================
// Flags 位定义 - 分布式架构
// =============================================================================

namespace key_flags {
  // Op Type (Bit 0-1): δ 映射
  constexpr uint8_t kOpTypeMask     = 0x03;  // 00: CREATE, 01: UPDATE, 10: DELETE
  constexpr uint8_t kOpCreate       = 0x00;  // 00: 创建操作
  constexpr uint8_t kOpUpdate       = 0x01;  // 01: 更新操作  
  constexpr uint8_t kOpDelete       = 0x02;  // 10: 删除操作
  
  // Status Bits
  constexpr uint8_t kIsDistributed  = 1 << 2;  // Bit 2: part_id 字段生效标记
  constexpr uint8_t kHasVInline     = 1 << 3;  // Bit 3: 值内联在 target_id 标记
  constexpr uint8_t kIsCompressed   = 1 << 4;  // Bit 4: Value 压缩标记
  constexpr uint8_t kIsLocked       = 1 << 5;  // Bit 5: 分布式事务锁（Percolator）
  constexpr uint8_t kHasExtension   = 1 << 6;  // Bit 6: target_id 包含扩展数据
  constexpr uint8_t kTombstone      = 1 << 7;  // Bit 7: 物理墓碑标记（仅用于 Compaction）
  
  // Column ID 静态标记（使用 column_id 最高位）
  constexpr uint16_t kIsStaticColumn = 0x8000;  // 静态字段标记，扫描时可跳过
}

namespace op_type {
  constexpr uint8_t kCreate = 0x00;
  constexpr uint8_t kUpdate = 0x01;
  constexpr uint8_t kDelete = 0x02;
}

// =============================================================================
// 32 字节固定长度 Key - CedarGraph 标准 Key
// =============================================================================

// 内存布局（严格按此顺序，确保 8 字节对齐）：
// Offset 0-7:   entity_id (uint64_t) - s: source vertex / routing key
// Offset 8-15:  timestamp_be (uint64_t) - t: 降序存储时间戳
// Offset 16-23: target_id (uint64_t) - o: dst_id 或扩展数据
// Offset 24-25: column_id (uint16_t) - φ: predicate ID
// Offset 26-27: sequence (uint16_t) - κ: intra-microsecond ordering
// Offset 28:    entity_type (uint8_t) - τ: NODE/EDGE/PROP
// Offset 29:    flags (uint8_t) - δ: op type (bit 0-1) + status bits
// Offset 30-31: part_id (uint16_t) - distributed partition ID (0-65535)
class alignas(8) CedarKey {
 public:
  // 大小常量
  static constexpr size_t kKeySize = 32;
  static constexpr size_t kUserKeySize = 19;  // entity_id(8) + entity_type(1) + column_id(2) + target_id(8)
  
  // ==================== 默认构造 ====================
  
  /// 默认构造 - 创建空 Key（所字段为0）
  CedarKey() 
      : entity_id_(0),
        timestamp_be_(0),
        target_id_(0),
        column_id_(0),
        sequence_(0),
        entity_type_(0),
        flags_(0),
        part_id_(0) {}
  
  /// 完整构造 - 兼容旧代码用法
  /// \note 仅用于内部实现，外部应使用 Vertex/EdgeOut/EdgeIn 工厂方法
  CedarKey(uint64_t entity_id, 
          EntityType entity_type,
          uint16_t column_id,
          Timestamp timestamp,
          uint16_t sequence = 0,
          uint64_t target_id = 0,
          uint8_t flags = 0,
          uint16_t part_id = 0)
      : entity_id_(cedar_htobe64(entity_id)),  // 统一字节序转换
        timestamp_be_(timestamp.EncodeForStorage()),
        target_id_(cedar_htobe64(target_id)),
        column_id_(cedar_htobe16(column_id)),
        sequence_(cedar_htobe16(sequence)),
        entity_type_(static_cast<uint8_t>(entity_type)),
        flags_(flags),
        part_id_(cedar_htobe16(part_id)) {}
  
  // ==================== 工厂方法 ====================
  
  /// 构造点 Key
  static CedarKey Vertex(uint64_t vertex_id,
                        VertexColumnId col,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint16_t part_id = 0,
                        uint64_t target_id = 0,
                        uint8_t flags = 0) {
    CedarKey key;
    key.entity_id_ = cedar_htobe64(vertex_id);
    key.timestamp_be_ = ts.EncodeForStorage();
    key.target_id_ = cedar_htobe64(target_id);
    key.column_id_ = cedar_htobe16(col.value);
    key.sequence_ = cedar_htobe16(seq);
    key.entity_type_ = static_cast<uint8_t>(EntityType::Vertex);
    key.flags_ = flags;
    key.part_id_ = cedar_htobe16(part_id);
    return key;
  }
  
  /// 构造点 Key（兼容 uint16_t）
  static CedarKey Vertex(uint64_t vertex_id,
                        uint16_t col_id,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint16_t part_id = 0,
                        uint64_t extension = 0,
                        uint8_t flags = 0) {
    return Vertex(vertex_id, VertexColumnId(col_id), ts, seq, part_id, extension, flags);
  }
  
  /// 构造出边 Key (src -> dst)
  static CedarKey EdgeOut(uint64_t src_id,
                         uint64_t dst_id,
                         EdgeTypeId edge_type,
                         Timestamp ts,
                         uint16_t seq = 0,
                         uint16_t part_id = 0,
                         uint8_t flags = 0) {
    CedarKey key;
    key.entity_id_ = cedar_htobe64(src_id);
    key.timestamp_be_ = ts.EncodeForStorage();
    key.target_id_ = cedar_htobe64(dst_id);
    key.column_id_ = cedar_htobe16(edge_type.value);
    key.sequence_ = cedar_htobe16(seq);
    key.entity_type_ = static_cast<uint8_t>(EntityType::EdgeOut);
    key.flags_ = flags;
    key.part_id_ = cedar_htobe16(part_id);
    return key;
  }
  
  /// 构造出边 Key（兼容 uint16_t）
  static CedarKey EdgeOut(uint64_t src_id,
                         uint64_t dst_id,
                         uint16_t edge_type,
                         Timestamp ts,
                         uint16_t seq = 0,
                         uint16_t part_id = 0,
                         uint8_t flags = 0) {
    return EdgeOut(src_id, dst_id, EdgeTypeId(edge_type), ts, seq, part_id, flags);
  }
  
  /// 构造入边 Key (dst <- src)，用于反向索引
  static CedarKey EdgeIn(uint64_t dst_id,
                        uint64_t src_id,
                        EdgeTypeId edge_type,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint16_t part_id = 0,
                        uint8_t flags = 0) {
    CedarKey key;
    key.entity_id_ = cedar_htobe64(dst_id);   // 注意：这里存的是 dst（查询入口）
    key.timestamp_be_ = ts.EncodeForStorage();
    key.target_id_ = cedar_htobe64(src_id);   // 这里存的是 src（邻居）
    key.column_id_ = cedar_htobe16(edge_type.value);
    key.sequence_ = cedar_htobe16(seq);
    key.entity_type_ = static_cast<uint8_t>(EntityType::EdgeIn);
    key.flags_ = flags;
    key.part_id_ = cedar_htobe16(part_id);
    return key;
  }
  
  /// 构造入边 Key（兼容 uint16_t）
  static CedarKey EdgeIn(uint64_t dst_id,
                        uint64_t src_id,
                        uint16_t edge_type,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint16_t part_id = 0,
                        uint8_t flags = 0) {
    return EdgeIn(dst_id, src_id, EdgeTypeId(edge_type), ts, seq, part_id, flags);
  }
  
  /// 创建通用边（自动选择正向/反向）
  static std::pair<CedarKey, CedarKey> MakeEdge(uint64_t src_id,
                                              uint64_t dst_id,
                                              EdgeTypeId edge_type,
                                              Timestamp ts,
                                              uint16_t seq = 0,
                                              uint16_t part_id = 0) {
    return {
      EdgeOut(src_id, dst_id, edge_type, ts, seq, part_id),
      EdgeIn(dst_id, src_id, edge_type, ts, seq, part_id)
    };
  }
  
  // 序列号溢出处理：在高频批量导入时，如果 seq 即将溢出，调整 timestamp
  // 返回调整后的 (timestamp, seq) 对
  static std::pair<Timestamp, uint16_t> HandleSequenceOverflow(Timestamp ts, uint16_t seq) {
    if (seq >= 65530) {  // 接近溢出阈值
      // 将 timestamp 增加 1 微秒（存储值减少 1），并重置 seq
      return {Timestamp(ts.value() + 1), 0};
    }
    return {ts, seq};
  }
  
  // ============================================================================
  // 全局排序契约 (Global Sorting Contract)
  // ============================================================================
  // 用于 SST 文件和 MemTable 的全序排列：
  // 1. entity_id ASC (首要聚簇关键字)
  // 2. entity_type ASC
  // 3. column_id ASC
  // 4. target_id ASC
  // 5. timestamp_be DESC (降序 - 最新版本在前)
  // 6. sequence ASC
  // ============================================================================
  
  // 比较器：用于排序的全序比较
  // 返回: -1 (this < other), 0 (equal), 1 (this > other)
  int CompareForSorting(const CedarKey& other) const {
    // 1. entity_id ASC
    if (entity_id() != other.entity_id()) {
      return entity_id() < other.entity_id() ? -1 : 1;
    }
    // 2. entity_type ASC
    if (entity_type_ != other.entity_type_) {
      return entity_type_ < other.entity_type_ ? -1 : 1;
    }
    // 3. column_id ASC
    if (column_id() != other.column_id()) {
      return column_id() < other.column_id() ? -1 : 1;
    }
    // 4. target_id ASC
    if (target_id() != other.target_id()) {
      return target_id() < other.target_id() ? -1 : 1;
    }
    // 5. timestamp_be DESC (注意是降序！)
    if (timestamp_be_ != other.timestamp_be_) {
      return timestamp_be_ > other.timestamp_be_ ? -1 : 1;  // 降序
    }
    // 6. sequence ASC
    if (sequence() != other.sequence()) {
      return sequence() < other.sequence() ? -1 : 1;
    }
    return 0;
  }
  
  // 排序比较运算符
  bool LessForSorting(const CedarKey& other) const {
    return CompareForSorting(other) < 0;
  }
  
  // 检查两个 Key 是否属于同一个实体 (entity_id, entity_type 相同)
  bool SameEntity(const CedarKey& other) const {
    return entity_id() == other.entity_id() && 
           entity_type_ == other.entity_type_;
  }
  
  // 检查两个 Key 是否属于同一个属性/边类型 (entity_id, entity_type, column_id 相同)
  bool SameProperty(const CedarKey& other) const {
    return entity_id() == other.entity_id() && 
           entity_type_ == other.entity_type_ &&
           column_id() == other.column_id();
  }

  // ==================== 编码/解码 ====================
  
  /// 编码为 32 字节字符串（可直接写入 LSM-Tree）
  std::string Encode() const {
    std::string result;
    result.resize(kKeySize);
    std::memcpy(result.data(), this, kKeySize);
    return result;
  }
  
  /// 编码到已有缓冲区
  void EncodeTo(void* buffer) const {
    std::memcpy(buffer, this, kKeySize);
  }
  
  /// 从字节切片解码（要求长度 >= 32）
  static std::optional<CedarKey> Decode(std::string_view slice) {
    if (slice.size() < kKeySize) return std::nullopt;
    CedarKey key;
    std::memcpy(&key, slice.data(), kKeySize);
    return key;
  }
  
  /// 从指针解码（要求有效内存 >= 32）
  static CedarKey Decode(const void* ptr) {
    CedarKey key;
    std::memcpy(&key, ptr, kKeySize);
    return key;
  }
  


  // ==================== 访问器 ====================
  
  uint64_t entity_id() const { return cedar_be64toh(entity_id_); }
  uint64_t target_id() const { return cedar_be64toh(target_id_); }
  uint16_t column_id() const { return cedar_be16toh(column_id_); }
  uint16_t sequence() const { return cedar_be16toh(sequence_); }
  EntityType entity_type() const { return static_cast<EntityType>(entity_type_); }
  uint8_t flags() const { return flags_; }
  uint16_t part_id() const { return cedar_be16toh(part_id_); }
  
  Timestamp timestamp() const {
    return Timestamp::DecodeFromStorage(timestamp_be_);
  }
  
  // 原始时间戳编码值（用于比较）
  uint64_t timestamp_be() const { return timestamp_be_; }
  
  // 便捷判断
  bool IsVertex() const { return entity_type_ == 0; }
  bool IsEdgeOut() const { return entity_type_ == 1; }
  bool IsEdgeIn() const { return entity_type_ == 2; }
  bool IsEdge() const { return entity_type_ == 1 || entity_type_ == 2; }
  
  // 标记检查
  bool IsTombstone() const { return (flags_ & key_flags::kTombstone) != 0; }
  bool IsCompressed() const { return (flags_ & key_flags::kIsCompressed) != 0; }
  bool IsDistributed() const { return (flags_ & key_flags::kIsDistributed) != 0; }
  bool IsLocked() const { return (flags_ & key_flags::kIsLocked) != 0; }
  bool HasVInline() const { return (flags_ & key_flags::kHasVInline) != 0; }
  
  // 静态字段检查（column_id 最高位）
  bool IsStaticColumn() const { return (column_id_ & key_flags::kIsStaticColumn) != 0; }
  
  // 获取原始 column_id（不含静态标记）
  uint16_t GetBaseColumnId() const { return column_id_ & ~key_flags::kIsStaticColumn; }
  
  // 标记为静态字段
  void MarkStatic() { column_id_ |= key_flags::kIsStaticColumn; }
  
  // 清除静态字段标记
  void ClearStatic() { column_id_ &= ~key_flags::kIsStaticColumn; }
  
  // 获取操作类型 (δ)
  uint8_t GetOpType() const { return flags_ & key_flags::kOpTypeMask; }
  bool IsCreate() const { return GetOpType() == op_type::kCreate; }
  bool IsUpdate() const { return GetOpType() == op_type::kUpdate; }
  bool IsDelete() const { return GetOpType() == op_type::kDelete; }
  
  // 获取边的对端 ID（自动处理出边/入边）
  uint64_t GetEdgePeerId() const {
    if (IsEdge()) return target_id();
    return 0;  // Vertex 返回 0
  }
  
  // 获取入边的源 ID（仅入边有效）
  uint64_t GetInEdgeSrcId() const {
    return IsEdgeIn() ? target_id() : 0;
  }
  
  // 获取出边的目标 ID（仅出边有效）
  uint64_t GetOutEdgeDstId() const {
    return IsEdgeOut() ? target_id() : 0;
  }

  // ==================== 修改器 ====================
  
  void SetFlags(uint8_t flags) { flags_ = flags; }
  void AddFlags(uint8_t flags) { flags_ |= flags; }
  void ClearFlags(uint8_t flags) { flags_ &= ~flags; }
  void SetSequence(uint16_t seq) { sequence_ = cedar_htobe16(seq); }
  void SetPartId(uint16_t part_id) { part_id_ = cedar_htobe16(part_id); }
  void SetTargetId(uint64_t target_id) { target_id_ = cedar_htobe64(target_id); }
  void SetEntityId(uint64_t entity_id) { entity_id_ = cedar_htobe64(entity_id); }
  void SetColumnId(uint16_t col_id) { column_id_ = cedar_htobe16(col_id); }
  void SetTimestamp(Timestamp ts) { timestamp_be_ = ts.EncodeForStorage(); }
  void SetEntityType(uint8_t type) { entity_type_ = type; }
  
  // 设置操作类型
  void SetOpType(uint8_t op_type) { 
    flags_ = (flags_ & ~key_flags::kOpTypeMask) | (op_type & key_flags::kOpTypeMask);
  }

  // ==================== LSM-Tree 比较器支持 ====================
  
  // 三向比较（按字段比较，正确处理小端整数）
  int Compare(const CedarKey& other) const {
    // 按字段顺序比较，避免小端字节序问题
    // entity_id 和 timestamp 是大端序存储，但使用数值比较更安全
    if (entity_id() != other.entity_id()) {
      return entity_id() < other.entity_id() ? -1 : 1;
    }
    if (entity_type_ != other.entity_type_) {
      return entity_type_ < other.entity_type_ ? -1 : 1;
    }
    if (column_id_ != other.column_id_) {
      return column_id_ < other.column_id_ ? -1 : 1;
    }
    if (target_id_ != other.target_id_) {
      return target_id_ < other.target_id_ ? -1 : 1;
    }
    // timestamp 是大端序存储，直接比较字节
    if (timestamp_be_ != other.timestamp_be_) {
      return timestamp_be_ < other.timestamp_be_ ? -1 : 1;
    }
    if (sequence_ != other.sequence_) {
      return sequence_ < other.sequence_ ? -1 : 1;
    }
    if (flags_ != other.flags_) {
      return flags_ < other.flags_ ? -1 : 1;
    }
    return 0;
  }
  
  // 比较器运算符
  bool operator<(const CedarKey& other) const { return Compare(other) < 0; }
  bool operator>(const CedarKey& other) const { return Compare(other) > 0; }
  bool operator==(const CedarKey& other) const { return Compare(other) == 0; }
  bool operator!=(const CedarKey& other) const { return Compare(other) != 0; }
  bool operator<=(const CedarKey& other) const { return Compare(other) <= 0; }
  bool operator>=(const CedarKey& other) const { return Compare(other) >= 0; }
  
  // 仅比较 UserKey 部分（不含 timestamp/sequence，用于 MVCC）
  int CompareUserKey(const CedarKey& other) const {
    // 比较：entity_id(8) + entity_type(1) + column_id(2) + target_id(8)
    if (entity_id() != other.entity_id()) {
      return entity_id() < other.entity_id() ? -1 : 1;
    }
    if (entity_type_ != other.entity_type_) {
      return entity_type_ < other.entity_type_ ? -1 : 1;
    }
    if (column_id_ != other.column_id_) {
      return column_id_ < other.column_id_ ? -1 : 1;
    }
    if (target_id_ != other.target_id_) {
      return target_id_ < other.target_id_ ? -1 : 1;
    }
    return 0;
  }
  
  bool SameUserKey(const CedarKey& other) const {
    return CompareUserKey(other) == 0;
  }

  // ==================== 序列化辅助 ====================
  
  // 获取 UserKey 字节（用于 Bloom Filter）
  std::string GetUserKeyBytes() const {
    std::string result;
    result.resize(kUserKeySize);
    char* p = result.data();
    std::memcpy(p, &entity_id_, 8); p += 8;
    std::memcpy(p, &entity_type_, 1); p += 1;
    std::memcpy(p, &column_id_, 2); p += 2;
    std::memcpy(p, &target_id_, 8);
    return result;
  }
  
  // 调试字符串
  std::string DebugString() const;
  
  // 人类可读的 ToString 格式: "{ID:101, Type:EdgeOut, Time:2026-04-02T10:30:45, ...}"
  std::string ToString() const;
  
  // 十六进制字符串（用于调试二进制格式）
  std::string ToHexString() const;

 private:
  // 严格按此顺序布局，确保 8 字节对齐
  uint64_t entity_id_;      // +0  - 点 ID / 边 Src ID / 反向边 Dst ID
  uint64_t timestamp_be_;   // +8  - 降序存储的大端序时间戳
  uint64_t target_id_;      // +16 - 边 Dst ID / 反向边 Src ID / 点扩展数据
  uint16_t column_id_;      // +24 - 属性 ID（点）/ 边类型 ID（边）
  uint16_t sequence_;       // +26 - 同一微秒内的版本序列号
  uint8_t  entity_type_;    // +28 - EntityType
  uint8_t  flags_;          // +29 - 操作类型 δ + 状态位
  uint16_t part_id_;        // +30 - 分布式分区 ID (0-65535)
};

static_assert(sizeof(CedarKey) == 32, "CedarKey must be exactly 32 bytes");
static_assert(alignof(CedarKey) == 8, "CedarKey must be 8-byte aligned");

// =============================================================================
// LSM-Tree 比较器
// =============================================================================

class CedarKeyComparator {
 public:
  using KeyType = CedarKey;
  
  // 标准 LSM-Tree 比较接口
  int Compare(std::string_view a, std::string_view b) const {
    // 直接比较 32 字节内存（利用 memcmp 的 SIMD 优化）
    if (a.size() < CedarKey::kKeySize || b.size() < CedarKey::kKeySize) {
      return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
    }
    return std::memcmp(a.data(), b.data(), CedarKey::kKeySize);
  }
  
  // 比较两个 CedarKey 对象
  int Compare(const CedarKey& a, const CedarKey& b) const {
    return a.Compare(b);
  }
  
  // 前缀比较：用于 Bloom Filter 等优化
  bool EqualPrefix(std::string_view a, std::string_view b, size_t prefix_len = 24) const {
    size_t len = std::min({a.size(), b.size(), prefix_len});
    return std::memcmp(a.data(), b.data(), len) == 0;
  }
  
  // 获取 Key 的 UserKey 部分（不含 sequence/timestamp，用于 MVCC）
  std::string_view ExtractUserKey(std::string_view key) const {
    if (key.size() < CedarKey::kUserKeySize) return key;
    return key.substr(0, CedarKey::kUserKeySize);
  }
  
  // 比较 UserKey 部分
  int CompareUserKey(std::string_view a, std::string_view b) const {
    if (a.size() < CedarKey::kKeySize || b.size() < CedarKey::kKeySize) {
      return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
    }
    CedarKey ka = CedarKey::Decode(a.data());
    CedarKey kb = CedarKey::Decode(b.data());
    return ka.CompareUserKey(kb);
  }
  
  // 名称（用于 LSM-Tree 配置）
  static const char* Name() { return "cedar.CedarKeyComparator"; }
};

// =============================================================================
// 范围扫描辅助
// =============================================================================

class CedarKeyRange {
 public:
  // 查询某实体的所有版本（任意时间）
  static std::pair<CedarKey, CedarKey> AllVersions(
      uint64_t entity_id, EntityType type, uint16_t column_id);
  
  // 查询某实体的时间范围
  static std::pair<CedarKey, CedarKey> TimeRange(
      uint64_t entity_id, EntityType type, uint16_t column_id,
      Timestamp start, Timestamp end);
};

// =============================================================================
// InternalKey - MemTable 内部使用的键（不含时间戳）
// =============================================================================

struct InternalKey {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  uint64_t target_id;  // 边的 dst_id 或点的扩展数据
  
  InternalKey() = default;
  
  // 从组件构造
  InternalKey(uint64_t eid, EntityType type, uint16_t col, uint64_t target = 0)
      : entity_id(eid), entity_type(type), column_id(col), target_id(target) {}
  
  // 从 CedarKey 构造（提取非时间戳部分）
  explicit InternalKey(const CedarKey& key)
      : entity_id(key.entity_id()),
        entity_type(key.entity_type()),
        column_id(key.column_id()),
        target_id(key.target_id()) {}
  
  bool operator==(const InternalKey& other) const {
    return entity_id == other.entity_id &&
           entity_type == other.entity_type &&
           column_id == other.column_id &&
           target_id == other.target_id;
  }
  
  bool operator<(const InternalKey& other) const {
    if (entity_id != other.entity_id) return entity_id < other.entity_id;
    if (entity_type != other.entity_type)
      return static_cast<uint8_t>(entity_type) < static_cast<uint8_t>(other.entity_type);
    if (column_id != other.column_id) return column_id < other.column_id;
    return target_id < other.target_id;
  }
};

// InternalKey 的哈希函数
struct InternalKeyHash {
  size_t operator()(const InternalKey& k) const noexcept {
    return std::hash<uint64_t>()(k.entity_id) ^
           (std::hash<uint8_t>()(static_cast<uint8_t>(k.entity_type)) << 1) ^
           (std::hash<uint16_t>()(k.column_id) << 2) ^
           (std::hash<uint64_t>()(k.target_id) << 3);
  }
};

}  // namespace cedar

// std::hash 特化 for InternalKey
namespace std {
template <>
struct hash<cedar::InternalKey> {
  size_t operator()(const cedar::InternalKey& k) const noexcept {
    return cedar::InternalKeyHash{}(k);
  }
};
}  // namespace std

#endif  // FERN_FERN_KEY_H_
