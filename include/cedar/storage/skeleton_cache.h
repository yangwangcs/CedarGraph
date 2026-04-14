// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// SkeletonCache - 内存拓扑骨架缓存（Phase 5 极致优化）
// 12B 极致压缩边 + 分片 LRU + 缓存行对齐

#ifndef FERN_SKELETON_CACHE_H_
#define FERN_SKELETON_CACHE_H_

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <list>
#include <optional>

#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/storage/entity_lifecycle.h"

namespace cedar {

// 前向声明
class LsmEngine;

// =============================================================================
// EdgeEntry12B - 12字节极致压缩边
// =============================================================================
// 位填充定义:
// [Timestamp Offset : 22 bit] | [Label ID : 7 bit] | [Flags : 3 bit]
// 总: 32 bit = 4 bytes
//
// 加上 8B dst_id = 12B 总大小
// =============================================================================

#pragma pack(push, 1)
struct EdgeEntry12B {
    uint64_t dst_id;  // 8B: 目标节点 ID
    
    // 4B 位填充区
    union {
        uint32_t packed_data;
        struct {
            uint32_t timestamp_offset : 22;  // 时间戳偏移 (支持约 8 年 @1分钟精度)
            uint32_t label_id : 7;           // 边类型 (支持 128 种)
            uint32_t flags : 3;              // 标志位
        } bits;
    };
    
    // 标志位定义
    static constexpr uint32_t kFlagDeleted = 0x1;      // bit 0: 逻辑删除
    static constexpr uint32_t kFlagDirection = 0x2;    // bit 1: 0=Out, 1=In
    static constexpr uint32_t kFlagHasMoreProps = 0x4; // bit 2: 是否有更多属性
    
    // 默认构造
    EdgeEntry12B() : dst_id(0), packed_data(0) {}
    
    // 从 CedarKey 构造（需要 base_timestamp 计算偏移）
    EdgeEntry12B(const CedarKey& key, uint32_t base_timestamp);
    
    // 解码为 CedarKey（需要 base_timestamp）
    CedarKey ToCedarKey(uint64_t src_id, uint32_t base_timestamp) const;
    
    // 快捷访问
    bool IsDeleted() const { return bits.flags & kFlagDeleted; }
    bool IsInEdge() const { return bits.flags & kFlagDirection; }
    bool HasMoreProps() const { return bits.flags & kFlagHasMoreProps; }
    
    void SetDeleted(bool deleted) {
        if (deleted) bits.flags |= kFlagDeleted;
        else bits.flags &= ~kFlagDeleted;
    }
    
    // 计算时间戳
    uint64_t GetTimestamp(uint32_t base_timestamp) const {
        return base_timestamp + bits.timestamp_offset;
    }
};
static_assert(sizeof(EdgeEntry12B) == 12, "EdgeEntry12B must be exactly 12 bytes");
#pragma pack(pop)

// =============================================================================
// VertexSkeleton - 节点骨架结构（缓存行对齐）
// =============================================================================
// 内存布局优化：按 64B 缓存行对齐，避免 False Sharing
// =============================================================================

struct alignas(64) VertexSkeleton {
    // 第 1 缓存行：元数据（8B + 4B + 4B = 16B）
    std::atomic<uint64_t> version;           // 版本号（用于并发控制）
    uint32_t base_timestamp;                 // 基础时间戳
    uint32_t reserved;                       // 预留（padding）
    
    // 第 2 缓存行：状态 + 计数（8B + 8B + 8B + 8B = 32B）
    std::atomic<uint8_t> status_hint;        // 状态标记（来自 0xFFE 锚点）
    uint8_t padding[7];                      // 对齐到 8B
    
    uint32_t out_count;                      // 出边数量
    uint32_t in_count;                       // 入边数量
    uint32_t capacity;                       // 总容量（出边+入边）
    uint32_t access_count;                   // 访问计数（用于 LRU）
    
    // 第 3-4 缓存行：边数据指针（16B）
    EdgeEntry12B* out_edges;                 // 出边数组指针
    EdgeEntry12B* in_edges;                  // 入边数组指针
    
    // 构造函数
    VertexSkeleton() : version(0), base_timestamp(0), reserved(0),
                       status_hint(0), out_count(0), in_count(0),
                       capacity(0), access_count(0),
                       out_edges(nullptr), in_edges(nullptr) {}
    
    // 禁用拷贝（边数据指针需要特殊处理）
    VertexSkeleton(const VertexSkeleton&) = delete;
    VertexSkeleton& operator=(const VertexSkeleton&) = delete;
    
    // 允许移动
    VertexSkeleton(VertexSkeleton&& other) noexcept;
    VertexSkeleton& operator=(VertexSkeleton&& other) noexcept;
    
    // 析构（释放边数据内存）
    ~VertexSkeleton();
    
    // 状态检查
    bool IsActive() const { return status_hint.load() == 1; }
    bool IsDeleted() const { return status_hint.load() == 2; }
    void SetStatus(uint8_t status) { status_hint.store(status); }
    
    // 内存占用
    size_t MemoryUsage() const {
        return sizeof(*this) + capacity * sizeof(EdgeEntry12B);
    }
};
static_assert(sizeof(VertexSkeleton) == 64, "VertexSkeleton should fit in one cache line ideally, but accept 64B");
static_assert(alignof(VertexSkeleton) == 64, "VertexSkeleton must be 64B aligned");

// =============================================================================
// ShardedSkeletonCache - 分片 LRU 缓存
// =============================================================================
// 1024 分片，每片独立 LRU，减少锁竞争
// =============================================================================

class ShardedSkeletonCache {
 public:
  // 默认配置
  static constexpr size_t kDefaultNumShards = 1024;
  static constexpr size_t kDefaultMaxEntriesPerShard = 1024;  // 每片 1K 条目
  static constexpr size_t kMaxTotalEntries = kDefaultNumShards * kDefaultMaxEntriesPerShard;
  
  explicit ShardedSkeletonCache(
      size_t num_shards = kDefaultNumShards,
      size_t max_entries_per_shard = kDefaultMaxEntriesPerShard);
  
  ~ShardedSkeletonCache();
  
  // 禁止拷贝
  ShardedSkeletonCache(const ShardedSkeletonCache&) = delete;
  ShardedSkeletonCache& operator=(const ShardedSkeletonCache&) = delete;
  
  // ========== 核心接口 ==========
  
  // 获取节点骨架（命中返回 true，未命中返回 false）
  // 使用 shared_lock，高性能但会修改 LRU 顺序
  std::pair<VertexSkeleton*, bool> Get(uint64_t vertex_id);
  
  // 获取并提升 LRU 顺序（使用独占锁）
  // 适合写操作后立即读取的场景
  std::pair<VertexSkeleton*, bool> GetWithPromotion(uint64_t vertex_id);
  
  // 线程安全的出边扫描（拷贝数据，无需保持锁）
  // 返回值: {边列表, 基础时间戳}
  // 如果未命中，返回 {std::nullopt, 0}
  std::pair<std::optional<std::vector<EdgeEntry12B>>, uint32_t> ScanOutEdgesSafe(uint64_t vertex_id);
  
  // 插入或更新节点骨架
  // 如果已存在，更新 LRU 位置
  // 如果不存在，插入新条目（可能触发淘汰）
  void Put(uint64_t vertex_id, VertexSkeleton&& skeleton);
  
  // 获取或创建条目（线程安全）
  // 如果条目不存在，使用 factory 函数创建并插入
  // 返回 {骨架指针, 是否新创建}
  template<typename Factory>
  std::pair<VertexSkeleton*, bool> GetOrCreate(uint64_t vertex_id, Factory&& factory) {
    size_t shard_idx = ShardIndex(vertex_id);
    Shard& shard = *shards_[shard_idx];
    
    // 先尝试读
    {
      std::shared_lock<std::shared_mutex> lock(shard.mutex);
      auto it = shard.entries.find(vertex_id);
      if (it != shard.entries.end() && !it->second.skeleton.IsDeleted()) {
        it->second.skeleton.access_count++;
        shard.hits++;
        return {&it->second.skeleton, false};
      }
    }
    
    // 未命中，使用独占锁创建
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    
    // 双重检查（其他线程可能已创建）
    auto it = shard.entries.find(vertex_id);
    if (it != shard.entries.end() && !it->second.skeleton.IsDeleted()) {
      it->second.skeleton.access_count++;
      shard.hits++;
      return {&it->second.skeleton, false};
    }
    
    // 创建新条目
    VertexSkeleton skeleton = factory();
    
    // 检查是否创建失败（IsDeleted 表示未找到）
    if (skeleton.IsDeleted()) {
      return {nullptr, false};
    }
    
    // 检查容量
    if (shard.entries.size() >= max_entries_per_shard_) {
      shard.EvictOldest(max_entries_per_shard_ - 1);
    }
    
    // 插入 - 使用 emplace 并获取迭代器
    shard.lru_list.push_front(vertex_id);
    Shard::Entry entry;
    entry.lru_iter = shard.lru_list.begin();
    entry.skeleton = std::move(skeleton);
    auto result = shard.entries.emplace(vertex_id, std::move(entry));
    shard.insertions++;
    shard.misses++;  // 这是一次 miss
    
    // 从迭代器获取指针（比 operator[] 更安全）
    return {&result.first->second.skeleton, true};
  }
  
  // 标记节点为删除（快速失效）
  void MarkDeleted(uint64_t vertex_id);
  
  // 使缓存条目失效
  void Invalidate(uint64_t vertex_id);
  
  // ========== 批量操作 ==========
  
  // 批量获取（用于 ScanEdges）
  std::vector<std::pair<uint64_t, VertexSkeleton*>> BatchGet(
      const std::vector<uint64_t>& vertex_ids);
  
  // ========== 统计信息 ==========
  
  struct Stats {
    uint64_t hits = 0;           // 命中次数
    uint64_t misses = 0;         // 未命中次数
    uint64_t evictions = 0;      // 淘汰次数
    uint64_t insertions = 0;     // 插入次数
    uint64_t invalidations = 0;  // 失效次数
    
    double HitRate() const {
      uint64_t total = hits + misses;
      return total > 0 ? static_cast<double>(hits) / total : 0.0;
    }
    
    size_t TotalEntries() const { return insertions - evictions - invalidations; }
  };
  
  Stats GetStats() const;
  void ResetStats();
  
  // 内存占用（字节）
  size_t MemoryUsage() const;
  
  // 清空缓存
  void Clear();
  
  // ========== Phase 5: 内存限制和预热 ==========
  
  // 检查是否超过内存限制，如果超过则淘汰条目直到满足限制
  // 返回淘汰的条目数
  size_t EnforceMemoryLimit(size_t max_memory_bytes);
  
  // 获取当前内存占用（更精确的计算）
  size_t CalculateMemoryUsage() const;
  
  // 批量预热：将多个节点加载到缓存
  // 返回成功预热的节点数
  template<typename Loader>
  size_t Warmup(const std::vector<uint64_t>& vertex_ids, Loader&& loader) {
    size_t warmed = 0;
    for (uint64_t vid : vertex_ids) {
      // 检查是否已在缓存中
      auto [existing, hit] = Get(vid);
      if (hit) {
        continue;  // 已存在，跳过
      }
      
      // 加载骨架
      VertexSkeleton skeleton = loader(vid);
      if (!skeleton.IsDeleted()) {
        Put(vid, std::move(skeleton));
        warmed++;
      }
    }
    return warmed;
  }
  
 private:
  // 分片结构
  struct Shard {
    mutable std::shared_mutex mutex;
    
    // LRU 链表（最新访问的在头部）
    std::list<uint64_t> lru_list;
    
    // Hash 表: vertex_id -> (list_iterator, skeleton)
    struct Entry {
      std::list<uint64_t>::iterator lru_iter;
      VertexSkeleton skeleton;
    };
    std::unordered_map<uint64_t, Entry> entries;
    
    // 统计
    mutable uint64_t hits = 0;
    mutable uint64_t misses = 0;
    mutable uint64_t evictions = 0;
    mutable uint64_t insertions = 0;
    
    // 淘汰最旧条目
    void EvictOldest(size_t target_size);
  };
  
  // 分片数组
  std::vector<std::unique_ptr<Shard>> shards_;
  
  // 配置
  size_t num_shards_;
  size_t max_entries_per_shard_;
  
  // 全局统计
  mutable std::atomic<uint64_t> total_invalidations_{0};
  
  // 计算分片索引
  size_t ShardIndex(uint64_t vertex_id) const {
    return vertex_id % num_shards_;
  }
};

// =============================================================================
// SkeletonHydrator - 骨架物化器
// =============================================================================
// 从 SST (32B CedarKey) 物化为内存 (12B EdgeEntry12B)
// =============================================================================

class SkeletonHydrator {
 public:
  // 物化单个节点的骨架
  // 从 LsmEngine 读取边数据，压缩为 12B 格式
  static Status HydrateVertex(
      uint64_t vertex_id,
      LsmEngine* engine,
      VertexSkeleton* out_skeleton);
  
  // 批量物化（用于预热）
  static Status BatchHydrate(
      const std::vector<uint64_t>& vertex_ids,
      LsmEngine* engine,
      ShardedSkeletonCache* cache);
  
 private:
  // 压缩 CedarKey 为 EdgeEntry12B
  static EdgeEntry12B CompressKey(
      const CedarKey& key,
      uint32_t base_timestamp);
  
  // 计算基础时间戳（取最早边的时间）
  static uint32_t ComputeBaseTimestamp(
      const std::vector<CedarKey>& keys);
};

}  // namespace cedar

#endif  // FERN_SKELETON_CACHE_H_
