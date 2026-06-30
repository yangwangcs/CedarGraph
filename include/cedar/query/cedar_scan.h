//===----------------------------------------------------------------------===//
// CedarGraph Query Engine - CedarScan (Snapshot-based Temporal Scan)
//===----------------------------------------------------------------------===//

#ifndef CEDAR_CEDAR_SCAN_H
#define CEDAR_CEDAR_SCAN_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

#include "cedar/core/cedar_status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/edge_scan_entry.h"
#include "cedar/storage/entity_lifecycle.h"
#include "cedar/storage/active_entity_bitmap.h"

namespace cedar {

// Forward declarations
class LsmEngine;
class Iterator;

//===----------------------------------------------------------------------===//
// View Objects - Zero-copy wrappers around storage data
//===----------------------------------------------------------------------===//

/**
 * @brief View of a graph node at a specific snapshot time
 * 
 * NodeView provides read-only access to vertex properties.
 * Note: For safety, this stores a copy of the Descriptor rather than
 * a pointer, to avoid lifetime issues with temporary objects.
 */
class NodeView {
 public:
  NodeView() = default;
  NodeView(const CedarKey& key, const Descriptor& descriptor)
      : key_(key), descriptor_(descriptor) {}

  // Identity
  uint64_t node_id() const { return key_.entity_id(); }
  Timestamp timestamp() const { return key_.timestamp(); }

  // Properties - inline or from descriptor
  const Descriptor& descriptor() const { return descriptor_; }
  bool has_inline_value() const { return key_.flags() & key_flags::kHasVInline; }

  // Inline value access (when kHasInlineValue flag is set)
  uint64_t inline_value() const { return key_.target_id(); }

  // Metadata
  uint16_t column_id() const { return key_.column_id(); }
  uint16_t sequence() const { return key_.sequence(); }
  uint8_t flags() const { return key_.flags(); }
  bool is_deleted() const { return key_.IsDelete(); }

  // Access the underlying key
  const CedarKey& key() const { return key_; }

 private:
  CedarKey key_;
  Descriptor descriptor_;
};

/**
 * @brief View of a graph edge at a specific snapshot time
 * 
 * EdgeView normalizes both EdgeOut and EdgeIn directions to provide
 * consistent src/dst access regardless of storage direction.
 */
class EdgeView {
 public:
  EdgeView() = default;
  EdgeView(const CedarKey& key, const Descriptor& descriptor, bool is_out_edge)
      : key_(key), descriptor_(descriptor), is_out_edge_(is_out_edge) {}

  // Identity - normalized direction
  uint64_t src_id() const {
    return is_out_edge_ ? key_.entity_id() : key_.target_id();
  }
  uint64_t dst_id() const {
    return is_out_edge_ ? key_.target_id() : key_.entity_id();
  }
  uint64_t edge_id() const { return key_.entity_id(); }

  // Edge type (stored in column_id for edges)
  uint16_t edge_type() const { return key_.column_id(); }
  Timestamp timestamp() const { return key_.timestamp(); }

  // Properties
  const Descriptor& descriptor() const { return descriptor_; }
  bool has_inline_value() const { return key_.flags() & key_flags::kHasVInline; }
  uint64_t inline_value() const { return key_.target_id(); }

  // Direction info
  bool is_out_edge() const { return is_out_edge_; }
  bool is_in_edge() const { return !is_out_edge_; }

  // Metadata
  uint16_t sequence() const { return key_.sequence(); }
  uint8_t flags() const { return key_.flags(); }
  bool is_deleted() const { return key_.IsDelete(); }

  // Access the underlying key
  const CedarKey& key() const { return key_; }

 private:
  CedarKey key_;
  Descriptor descriptor_;
  bool is_out_edge_ = true;
};

//===----------------------------------------------------------------------===//
// Edge Iterator with Version Folding
//===----------------------------------------------------------------------===//

/**
 * @brief Iterator for scanning edges with automatic version folding
 * 
 * EdgeIterator handles:
 * - Direction normalization (out/in edges)
 * - Version folding (returns only latest version of each edge)
 * - Tombstone filtering (skips deleted edges)
 * - Zero-allocation optimization using pre-sorted results
 */
class EdgeIterator {
 public:
  static constexpr uint16_t kAllLabels = 0xFFFF;

  EdgeIterator();
  
  /**
   * @brief Construct from pre-scanned edge entries
   * @param entries Pre-scanned and folded edge entries from storage engine
   * @param is_out_scan True for OutEdges scan, false for InEdges
   */
  EdgeIterator(std::vector<EdgeScanEntry>&& entries, bool is_out_scan);
  ~EdgeIterator();

  // Non-copyable but movable
  EdgeIterator(const EdgeIterator&) = delete;
  EdgeIterator& operator=(const EdgeIterator&) = delete;
  EdgeIterator(EdgeIterator&&) noexcept;
  EdgeIterator& operator=(EdgeIterator&&) noexcept;

  /**
   * @brief Advance to next valid edge
   * @return true if a valid edge is available
   */
  bool Next();

  /**
   * @brief Get current edge view
   * @return EdgeView representing current edge
   * @pre Valid() returns true
   */
  EdgeView Current() const;

  /**
   * @brief Check if iterator is valid
   */
  bool Valid() const;

  /**
   * @brief Get count of edges returned so far
   */
  size_t count() const { return current_idx_; }

  /**
   * @brief Get total number of edges
   */
  size_t total() const { return entries_.size(); }

 private:
  std::vector<EdgeScanEntry> entries_;
  size_t current_idx_ = 0;
  bool is_out_scan_ = true;
};

//===----------------------------------------------------------------------===//
// CedarScan - Main Query Interface
//===----------------------------------------------------------------------===//

/**
 * @brief Snapshot-based temporal scan engine for CedarGraph
 * 
 * CedarScan provides read-only access to the graph at a specific
 * point in time (snapshot isolation). All queries within a scan
 * see a consistent view of the graph.
 */
class CedarScan {
 public:
  /**
   * @brief Create a scan at the specified timestamp
   * @param ts Snapshot timestamp - query sees data as of this time
   * @param engine Storage engine to query
   * @return CedarScan configured for the snapshot
   */
  static CedarScan At(Timestamp ts, LsmEngine* engine);

  /**
   * @brief Create a scan at current time (latest data)
   * @param engine Storage engine to query
   */
  static CedarScan Now(LsmEngine* engine);

  /**
   * @brief Get vertex at snapshot time
   * @param node_id Vertex ID to look up
   * @return NodeView if vertex exists at snapshot time, nullopt otherwise
   * 
   * Returns nullopt if:
   * - Vertex never existed
   * - Vertex was created after snapshot time
   * - Vertex was deleted before or at snapshot time
   */
  std::optional<NodeView> GetNode(uint64_t node_id) const;

  /**
   * @brief Scan outgoing edges from a vertex
   * @param src_id Source vertex ID
   * @param edge_type Filter by edge type (default: all types)
   * @return Iterator over outgoing edges
   * 
   * The iterator automatically handles:
   * - Version folding (latest version only)
   * - Tombstone filtering (deleted edges excluded)
   */
  EdgeIterator OutEdges(uint64_t src_id, uint16_t edge_type = EdgeIterator::kAllLabels) const;

  /**
   * @brief Scan incoming edges to a vertex
   * @param dst_id Destination vertex ID
   * @param edge_type Filter by edge type (default: all types)
   * @return Iterator over incoming edges
   */
  EdgeIterator InEdges(uint64_t dst_id, uint16_t edge_type = EdgeIterator::kAllLabels) const;

  // Accessors
  Timestamp snapshot_time() const { return snapshot_ts_; }
  LsmEngine* engine() const { return engine_; }

  // ========== 锚点统计指标 ==========
  struct AnchorStats {
    std::atomic<uint64_t> anchor_hits{0};        // 锚点命中次数
    std::atomic<uint64_t> anchor_misses{0};      // 锚点未命中次数
    std::atomic<uint64_t> deleted_skipped{0};    // 通过锚点跳过的删除实体数
    std::atomic<uint64_t> fallback_queries{0};   // 回退到传统查询的次数
    
    double HitRate() const {
      uint64_t total = anchor_hits.load() + anchor_misses.load();
      return total > 0 ? static_cast<double>(anchor_hits.load()) / total : 0.0;
    }
  };

  // Anchor stats (public for testing)
  static AnchorStats& GetAnchorStats() { return anchor_stats_; }
  static void ResetAnchorStats() {
    anchor_stats_.anchor_hits.store(0);
    anchor_stats_.anchor_misses.store(0);
    anchor_stats_.deleted_skipped.store(0);
    anchor_stats_.fallback_queries.store(0);
  }

 private:
  CedarScan(Timestamp ts, LsmEngine* engine) 
      : snapshot_ts_(ts), engine_(engine) {}

  Timestamp snapshot_ts_;
  LsmEngine* engine_ = nullptr;
  
  // ========== 锚点机制（Anchor）- O(1) 存在性检查 ==========
  
  /**
   * @brief 通过状态锚点检查实体状态
   * @return StateAnchor 如果锚点存在且有效，否则 nullopt
   */
  std::optional<StateAnchor> CheckEntityStateViaAnchor(
      uint64_t entity_id, EntityType entity_type) const;
  
  /**
   * @brief 快速检查实体是否存在（优先使用锚点）
   */
  bool EntityExistsFast(uint64_t entity_id, EntityType entity_type) const;
  
  /**
   * @brief 传统方法检查实体存在性（回退方案）
   */
  bool EntityExistsTraditional(uint64_t entity_id, EntityType entity_type) const;
  
  /**
   * @brief 批量检查实体状态（用于 ScanEdges 端点过滤）
   * @param entity_ids 实体 ID 列表
   * @param entity_type 实体类型
   * @return 返回每个实体的存在性结果（true=存在/不确定，false=确定不存在）
   */
  std::vector<bool> BatchCheckEntitiesViaAnchor(
      const std::vector<uint64_t>& entity_ids, EntityType entity_type) const;
  
  static AnchorStats anchor_stats_;
  
  // ========== Phase 4: 内存加速（可选）==========
  
  // 活跃实体位图（L1 缓存）
  mutable std::unique_ptr<ActiveEntityBitmap> active_bitmap_;
  
  // 锚点 LRU 缓存（L3 缓存）
  mutable std::unique_ptr<AnchorCache> anchor_cache_;
  
  // 启用内存加速
  void EnableMemoryAcceleration();
  
  // 使用内存级检查（优先于磁盘查询）
  std::optional<bool> CheckEntityStateInMemory(
      uint64_t entity_id, EntityType entity_type) const;
  
  // ========== 区间锚点（Interval Anchor）- 历史查询优化 ==========
  
  /**
   * @brief 使用区间锚点检查实体在指定时间点是否存在
   * @param entity_id 实体 ID
   * @param entity_type 实体类型
   * @param query_time 查询时间点
   * @return true=存在，false=不存在，nullopt=不确定（需回退查询）
   */
  std::optional<bool> CheckIntervalAnchor(
      uint64_t entity_id, EntityType entity_type, Timestamp query_time) const;
  
  /**
   * @brief 使用区间锚点优化的时间范围查询
   * @param entity_id 实体 ID
   * @param entity_type 实体类型
   * @param start_time 开始时间
   * @param end_time 结束时间
   * @return 如果区间锚点覆盖整个查询范围，返回 true；否则返回 false（需回退）
   */
  bool CanUseIntervalAnchorForRange(
      uint64_t entity_id, EntityType entity_type,
      Timestamp start_time, Timestamp end_time) const;
};

}  // namespace cedar

#endif  // CEDAR_CEDAR_SCAN_H
