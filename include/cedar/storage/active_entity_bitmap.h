// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Active Entity Bitmap - 内存级活跃实体索引（Phase 4 内存加速）

#ifndef FERN_ACTIVE_ENTITY_BITMAP_H_
#define FERN_ACTIVE_ENTITY_BITMAP_H_

#include <cstdint>
#include <vector>
#include <unordered_set>
#include <mutex>
#include <memory>

#include "cedar/types/cedar_key.h"
#include "cedar/storage/entity_lifecycle.h"

#include <mutex>
#include <shared_mutex>

namespace cedar {

// =============================================================================
// ActiveEntityBitmap - 内存级活跃实体索引
// =============================================================================
// 使用分桶的 unordered_set 实现 O(1) 存在性检查
// 替代方案：使用 roaring bitmap 的 64 位版本（需要引入 CRoaring 库）
//
// 设计决策：
// - 使用 std::unordered_set<uint64_t> 存储活跃实体 ID
// - 使用读写锁保护并发访问
// - 定期与磁盘状态同步（通过 Compaction 回调）
// =============================================================================

class ActiveEntityBitmap {
 public:
  ActiveEntityBitmap() = default;
  ~ActiveEntityBitmap() = default;
  
  // 禁止拷贝和移动（因为包含 mutex）
  ActiveEntityBitmap(const ActiveEntityBitmap&) = delete;
  ActiveEntityBitmap& operator=(const ActiveEntityBitmap&) = delete;
  ActiveEntityBitmap(ActiveEntityBitmap&&) = delete;
  ActiveEntityBitmap& operator=(ActiveEntityBitmap&&) = delete;
  
  // ========== 实体状态管理 ==========
  
  // 标记实体为活跃（CREATE/RECREATE）- 线程安全
  void MarkActive(uint64_t entity_id);
  
  // 标记实体为删除（DELETE）- 线程安全
  void MarkDeleted(uint64_t entity_id);
  
  // 批量标记活跃
  void MarkActiveBatch(const std::vector<uint64_t>& entity_ids);
  
  // 批量标记删除
  void MarkDeletedBatch(const std::vector<uint64_t>& entity_ids);
  
  // ========== 查询接口 ==========
  
  // O(1) 检查实体是否活跃
  bool IsActive(uint64_t entity_id) const;
  
  // O(1) 检查实体是否存在（活跃或删除）
  bool Contains(uint64_t entity_id) const;
  
  // 批量过滤：返回活跃实体列表
  std::vector<uint64_t> FilterActive(const std::vector<uint64_t>& entity_ids) const;
  
  // ========== 统计信息 ==========
  
  size_t ActiveCount() const;
  size_t DeletedCount() const;
  size_t TotalCount() const { return ActiveCount() + DeletedCount(); }
  
  // 内存占用（字节）
  size_t MemoryUsage() const;
  
  // ========== 维护操作 ==========
  
  // 清空所有数据
  void Clear();
  
  // 从 LsmEngine 全量重建（启动时使用）
  // void RebuildFromEngine(const LsmEngine* engine);
  
  // 压缩删除集合（定期清理，减少内存占用）
  void CompactDeletedSet();
  
 private:
  mutable std::mutex mutex_;  // 使用普通 mutex 替代 shared_mutex 以兼容 MacOS
  std::unordered_set<uint64_t> active_set_;   // 活跃实体集
  std::unordered_set<uint64_t> deleted_set_;  // 删除实体集（用于区分从未存在）
};

// =============================================================================
// VSLNodeHint - VSLMemTable 节点状态位（嵌入 LFNode）
// =============================================================================
// 在 VSLMemTable 的跳表节点中嵌入状态位，避免频繁查询 Anchor
//
// 设计：
// - 在 LFNode 中增加 1 字节状态位
// - 状态在写入时确定，读取时直接使用
// =============================================================================

// VSLNode 状态位（与 CedarKey::flags 分离，专门用于内存优化）
namespace vsl_node_hint {
  constexpr uint8_t kActive    = 0x01;  // 实体当前活跃
  constexpr uint8_t kDeleted   = 0x02;  // 实体已删除
  constexpr uint8_t kDirty     = 0x04;  // 节点已修改（需要刷新到磁盘）
  constexpr uint8_t kHasAnchor = 0x08;  // 节点已同步到锚点
}

// VSLNodeHint 工具类
class VSLNodeHint {
 public:
  static uint8_t FromCedarKey(const CedarKey& key);
  static bool IsActive(uint8_t hint);
  static bool IsDeleted(uint8_t hint);
};

// =============================================================================
// AnchorCache - 锚点 LRU Cache
// =============================================================================
// 内存中缓存最近访问的锚点，减少磁盘 I/O
//
// 设计：
// - 使用 LRU 策略淘汰
// - 缓存 (entity_id, entity_type) -> StateAnchor
// - 容量可配置（默认 10K 条目）
// =============================================================================

struct AnchorCacheKey {
  uint64_t entity_id;
  uint8_t entity_type;
  
  bool operator==(const AnchorCacheKey& other) const {
    return entity_id == other.entity_id && entity_type == other.entity_type;
  }
};

struct AnchorCacheKeyHash {
  size_t operator()(const AnchorCacheKey& key) const {
    return std::hash<uint64_t>()(key.entity_id) ^ 
           (std::hash<uint8_t>()(key.entity_type) << 1);
  }
};

class AnchorCache {
 public:
  explicit AnchorCache(size_t capacity = 10000);
  
  // 获取缓存的锚点，未命中返回 nullopt
  std::optional<StateAnchor> Get(uint64_t entity_id, EntityType entity_type) const;
  
  // 放入缓存
  void Put(uint64_t entity_id, EntityType entity_type, const StateAnchor& anchor);
  
  // 使缓存失效（用于写入新数据后）
  void Invalidate(uint64_t entity_id, EntityType entity_type);
  
  // 清空缓存
  void Clear();
  
  // 统计
  size_t Size() const { return cache_.size(); }
  size_t Capacity() const { return capacity_; }
  
  // 命中率
  double HitRate() const;
  void ResetStats();
  
 private:
  size_t capacity_;
  
  struct CacheEntry {
    StateAnchor anchor;
    uint64_t access_count;
  };
  
  mutable std::mutex mutex_;
  std::unordered_map<AnchorCacheKey, CacheEntry, AnchorCacheKeyHash> cache_;
  
  // 统计
  mutable uint64_t hits_ = 0;
  mutable uint64_t misses_ = 0;
};

}  // namespace cedar

#endif  // FERN_ACTIVE_ENTITY_BITMAP_H_
