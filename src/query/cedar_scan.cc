//===----------------------------------------------------------------------===//
// CedarScan Implementation - Snapshot-based Temporal Scan Engine
//===----------------------------------------------------------------------===//

#include "cedar/query/cedar_scan.h"

#include <algorithm>
#include <cstring>

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/entity_lifecycle.h"

namespace cedar {

//===----------------------------------------------------------------------===//
// EdgeIterator Implementation
//===----------------------------------------------------------------------===//

EdgeIterator::EdgeIterator() = default;

EdgeIterator::EdgeIterator(std::vector<EdgeScanEntry>&& entries, 
                           bool is_out_scan)
    : entries_(std::move(entries)),
      current_idx_(0),
      is_out_scan_(is_out_scan) {
}

EdgeIterator::~EdgeIterator() = default;

EdgeIterator::EdgeIterator(EdgeIterator&&) noexcept = default;
EdgeIterator& EdgeIterator::operator=(EdgeIterator&&) noexcept = default;

bool EdgeIterator::Next() {
  if (current_idx_ < entries_.size()) {
    ++current_idx_;
    return current_idx_ <= entries_.size();
  }
  return false;
}

EdgeView EdgeIterator::Current() const {
  if (current_idx_ == 0 || current_idx_ > entries_.size()) {
    return EdgeView();
  }
  const auto& entry = entries_[current_idx_ - 1];
  return EdgeView(entry.key, entry.descriptor, is_out_scan_);
}

bool EdgeIterator::Valid() const {
  return current_idx_ > 0 && current_idx_ <= entries_.size();
}

//===----------------------------------------------------------------------===//
// CedarScan Implementation
//===----------------------------------------------------------------------===//

CedarScan CedarScan::At(Timestamp ts, LsmEngine* engine) {
  CedarScan scan(ts, engine);
  // 默认启用内存加速（Phase 4 优化）
  scan.EnableMemoryAcceleration();
  return scan;
}

CedarScan CedarScan::Now(LsmEngine* engine) {
  // Use current system time as snapshot timestamp
  auto now = std::chrono::system_clock::now();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      now.time_since_epoch()).count();
  CedarScan scan(Timestamp(static_cast<uint64_t>(micros)), engine);
  // 默认启用内存加速（Phase 4 优化）
  scan.EnableMemoryAcceleration();
  return scan;
}

std::optional<NodeView> CedarScan::GetNode(uint64_t node_id) const {
  if (!engine_) {
    return std::nullopt;
  }
  
  // ========== 锚点优化（Anchor Optimization）==========
  // 1. 优先查询状态锚点（0xFFE 列），实现 O(1) 存在性检查
  auto anchor_result = CheckEntityStateViaAnchor(node_id, EntityType::Vertex);
  if (anchor_result.has_value()) {
    // 锚点命中：根据锚点状态快速判定
    if (anchor_result->state == EntityState::Deleted) {
      return std::nullopt;  // Auto-skip: 实体已删除
    }
    // Active 状态继续查询详细数据
  }
  
  // 2. 尝试使用区间锚点（0xFFD 列）优化历史查询
  auto interval_result = CheckIntervalAnchor(node_id, EntityType::Vertex, snapshot_ts_);
  if (interval_result.has_value()) {
    if (!*interval_result) {
      // 区间锚点确定实体在查询时间点不存在
      anchor_stats_.deleted_skipped.fetch_add(1);
      return std::nullopt;
    }
    // 区间锚点确认存在，继续查询详细数据
  }
  // 锚点未命中或状态不确定，回退到传统查询
  
  // Get all column IDs for this vertex
  auto column_ids = engine_->GetEntityColumnIds(node_id, EntityType::Vertex);
  
  // If no tracked columns, try common column IDs (0-10)
  if (column_ids.empty()) {
    for (uint16_t col = 0; col < 10; col++) {
      column_ids.push_back(col);
    }
  }
  
  // Query each column to find the latest version
  std::vector<std::pair<CedarKey, Descriptor>> all_versions;
  for (uint16_t col_id : column_ids) {
    auto result = engine_->GetRecordAtTime(node_id, EntityType::Vertex, col_id, snapshot_ts_);
    if (result.has_value()) {
      all_versions.push_back(result.value());
    }
  }
  
  if (all_versions.empty()) {
    return std::nullopt;
  }
  
  // Find the latest version (highest timestamp)
  auto latest = std::max_element(all_versions.begin(), all_versions.end(),
    [](const auto& a, const auto& b) {
      return a.first.timestamp().value() < b.first.timestamp().value();
    });
  
  const auto& [key, descriptor] = *latest;
  
  // Check if this is a delete operation
  if (key.IsDelete()) {
    return std::nullopt;
  }
  
  return NodeView(key, descriptor);
}

EdgeIterator CedarScan::OutEdges(uint64_t src_id, uint16_t edge_type) const {
  if (!engine_) {
    return EdgeIterator();
  }
  
  // 首先检查源点是否存在（锚点优化）
  auto src_anchor = CheckEntityStateViaAnchor(src_id, EntityType::Vertex);
  if (src_anchor.has_value() && src_anchor->IsDeleted()) {
    // 源点已删除，直接返回空结果
    anchor_stats_.deleted_skipped++;
    return EdgeIterator();
  }
  
  auto entries = engine_->ScanEdgesWithFolding(src_id, EntityType::EdgeOut, 
                                                edge_type, snapshot_ts_);
  
  // 使用锚点对端点进行批量过滤
  if (!entries.empty()) {
    std::vector<uint64_t> dst_ids;
    dst_ids.reserve(entries.size());
    for (const auto& entry : entries) {
      dst_ids.push_back(entry.target_id);
    }
    
    auto dst_exists = BatchCheckEntitiesViaAnchor(dst_ids, EntityType::Vertex);
    
    // 过滤掉对端点不存在的边
    std::vector<EdgeScanEntry> filtered;
    filtered.reserve(entries.size());
    for (size_t i = 0; i < entries.size(); i++) {
      if (dst_exists[i]) {
        filtered.push_back(entries[i]);
      }
    }
    entries = std::move(filtered);
  }
  
  return EdgeIterator(std::move(entries), true);
}

EdgeIterator CedarScan::InEdges(uint64_t dst_id, uint16_t edge_type) const {
  if (!engine_) {
    return EdgeIterator();
  }
  
  // 首先检查终点是否存在（锚点优化）
  auto dst_anchor = CheckEntityStateViaAnchor(dst_id, EntityType::Vertex);
  if (dst_anchor.has_value() && dst_anchor->IsDeleted()) {
    // 终点已删除，直接返回空结果
    anchor_stats_.deleted_skipped++;
    return EdgeIterator();
  }
  
  auto entries = engine_->ScanEdgesWithFolding(dst_id, EntityType::EdgeIn, 
                                                edge_type, snapshot_ts_);
  
  // 使用锚点对端点进行批量过滤
  if (!entries.empty()) {
    std::vector<uint64_t> src_ids;
    src_ids.reserve(entries.size());
    for (const auto& entry : entries) {
      src_ids.push_back(entry.target_id);
    }
    
    auto src_exists = BatchCheckEntitiesViaAnchor(src_ids, EntityType::Vertex);
    
    // 过滤掉对端点不存在的边
    std::vector<EdgeScanEntry> filtered;
    filtered.reserve(entries.size());
    for (size_t i = 0; i < entries.size(); i++) {
      if (src_exists[i]) {
        filtered.push_back(entries[i]);
      }
    }
    entries = std::move(filtered);
  }
  
  return EdgeIterator(std::move(entries), false);
}

//===----------------------------------------------------------------------===//
// 锚点机制（Anchor）实现 - O(1) 存在性检查
//===----------------------------------------------------------------------===//

// 静态统计指标定义
CedarScan::AnchorStats CedarScan::anchor_stats_;

std::optional<StateAnchor> CedarScan::CheckEntityStateViaAnchor(
    uint64_t entity_id, EntityType entity_type) const {
  if (!engine_) {
    anchor_stats_.anchor_misses.fetch_add(1);
    return std::nullopt;
  }
  
  // ========== Phase 4: 内存加速检查 ==========
  auto mem_result = CheckEntityStateInMemory(entity_id, entity_type);
  if (mem_result.has_value()) {
    // 内存命中，转换为 StateAnchor
    StateAnchor anchor;
    anchor.last_update = snapshot_ts_;
    anchor.state = *mem_result ? EntityState::Active : EntityState::Deleted;
    anchor.version = 0;
    return anchor;
  }
  
  // 查询状态锚点列（0xFFE）
  auto result = engine_->GetRecordAtTime(
      entity_id, entity_type, kStateAnchorColumnId, snapshot_ts_);
  
  if (!result.has_value()) {
    // 锚点不存在：实体可能从未被创建，或是旧数据（无锚点）
    anchor_stats_.anchor_misses.fetch_add(1);
    return std::nullopt;
  }
  
  const CedarKey& anchor_key = result->first;
  const Descriptor& anchor_desc = result->second;
  
  // 解析锚点描述符
  auto anchor_opt = LifecycleDescriptor::ParseStateAnchor(anchor_desc);
  if (!anchor_opt.has_value()) {
    anchor_stats_.anchor_misses.fetch_add(1);
    return std::nullopt;
  }
  
  StateAnchor anchor = *anchor_opt;
  
  // 验证锚点时间有效性：锚点的 last_update 必须 <= 查询时间点
  // 锚点的 timestamp 存储在 CedarKey 中
  Timestamp anchor_time = anchor_key.timestamp();
  if (anchor_time > snapshot_ts_) {
    // 锚点是在查询时间点之后创建的，对当前查询无效
    anchor_stats_.anchor_misses.fetch_add(1);
    return std::nullopt;
  }
  
  // 锚点命中
  anchor_stats_.anchor_hits.fetch_add(1);
  
  // 从 target_id 恢复 last_update（如果存储了的话）
  // 注意：target_id 可能存储了 last_update，用于更精确的时间判断
  anchor.last_update = Timestamp(anchor_key.target_id());
  
  return anchor;
}

bool CedarScan::EntityExistsFast(uint64_t entity_id, EntityType entity_type) const {
  auto anchor = CheckEntityStateViaAnchor(entity_id, entity_type);
  if (anchor.has_value()) {
    return anchor->IsActive();
  }
  // 锚点未命中，回退到传统查询
  anchor_stats_.fallback_queries.fetch_add(1);
  return EntityExistsTraditional(entity_id, entity_type);
}

bool CedarScan::EntityExistsTraditional(uint64_t entity_id, EntityType entity_type) const {
  // 传统方法：查询生命周期历史或属性数据
  // 这里简化实现，实际应该调用 CedarGraphStorage 的 EntityExistsAt
  auto column_ids = engine_->GetEntityColumnIds(entity_id, entity_type);
  if (column_ids.empty()) {
    column_ids = {0};  // 默认查询列 0
  }
  
  for (uint16_t col_id : column_ids) {
    auto result = engine_->GetRecordAtTime(entity_id, entity_type, col_id, snapshot_ts_);
    if (result.has_value()) {
      const auto& key = result->first;
      if (!key.IsDelete()) {
        return true;  // 找到非删除记录，实体存在
      }
    }
  }
  return false;
}

std::vector<bool> CedarScan::BatchCheckEntitiesViaAnchor(
    const std::vector<uint64_t>& entity_ids, EntityType entity_type) const {
  std::vector<bool> results;
  results.reserve(entity_ids.size());
  
  if (!engine_) {
    // 引擎不可用，全部返回 true（保守策略，让上层继续检查）
    results.assign(entity_ids.size(), true);
    anchor_stats_.anchor_misses.fetch_add(entity_ids.size());
    return results;
  }
  
  // 批量检查每个实体
  for (uint64_t entity_id : entity_ids) {
    auto anchor = CheckEntityStateViaAnchor(entity_id, entity_type);
    if (anchor.has_value()) {
      // 锚点命中：只有 Active 状态才认为存在
      results.push_back(anchor->IsActive());
      if (anchor->IsDeleted()) {
        anchor_stats_.deleted_skipped.fetch_add(1);
      }
    } else {
      // 锚点未命中：返回 true，让上层使用传统方法验证
      results.push_back(true);
    }
  }
  
  return results;
}

//===----------------------------------------------------------------------===//
// 区间锚点（Interval Anchor）实现 - 历史查询优化
//===----------------------------------------------------------------------===//

std::optional<bool> CedarScan::CheckIntervalAnchor(
    uint64_t entity_id, EntityType entity_type, Timestamp query_time) const {
  if (!engine_) {
    return std::nullopt;
  }
  
  // 查询区间锚点列（0xFFD）
  auto result = engine_->GetRecordAtTime(
      entity_id, entity_type, kIntervalAnchorColumnId, Timestamp::Max());
  
  if (!result.has_value()) {
    // 区间锚点不存在
    return std::nullopt;
  }
  
  const CedarKey& interval_key = result->first;
  
  // 从 target_id 解码区间信息
  uint64_t interval_data = interval_key.target_id();
  Timestamp start_time(interval_data >> 32);
  uint32_t end_low = interval_data & 0xFFFFFFFF;
  
  // 检查查询时间是否在区间内
  if (query_time < start_time) {
    return false;  // 查询时间在创建之前，实体不存在
  }
  
  if (end_low == 0xFFFFFFFF) {
    // 至今仍然存活
    return true;
  }
  
  Timestamp end_time(end_low);
  if (query_time <= end_time) {
    return true;  // 在存活区间内
  } else {
    return false;  // 在删除之后
  }
}

bool CedarScan::CanUseIntervalAnchorForRange(
    uint64_t entity_id, EntityType entity_type,
    Timestamp start_time, Timestamp end_time) const {
  if (!engine_) {
    return false;
  }
  
  // 查询区间锚点
  auto result = engine_->GetRecordAtTime(
      entity_id, entity_type, kIntervalAnchorColumnId, Timestamp::Max());
  
  if (!result.has_value()) {
    return false;  // 没有区间锚点，无法优化
  }
  
  const CedarKey& interval_key = result->first;
  uint64_t interval_data = interval_key.target_id();
  Timestamp anchor_start(interval_data >> 32);
  uint32_t end_low = interval_data & 0xFFFFFFFF;
  
  // 检查查询范围是否完全包含在锚点区间内
  if (start_time < anchor_start) {
    return false;  // 查询范围超出锚点区间左侧
  }
  
  if (end_low == 0xFFFFFFFF) {
    // 实体至今存活，只要查询开始时间 >= 创建时间即可
    return true;
  }
  
  Timestamp anchor_end(end_low);
  if (end_time > anchor_end) {
    return false;  // 查询范围超出锚点区间右侧
  }
  
  return true;  // 查询范围完全在锚点区间内，可以使用锚点优化
}

//===----------------------------------------------------------------------===//
// Phase 4: 内存加速实现
//===----------------------------------------------------------------------===//

void CedarScan::EnableMemoryAcceleration() {
  // 优先使用 LsmEngine 的 ActiveEntityBitmap（全局共享）
  // 如果 LsmEngine 不可用，才创建本地 Bitmap
  if (!engine_) {
    if (!active_bitmap_) {
      active_bitmap_ = std::make_unique<ActiveEntityBitmap>();
    }
  }
  // L3: 锚点 LRU 缓存（每个 CedarScan 实例独立）
  if (!anchor_cache_) {
    anchor_cache_ = std::make_unique<AnchorCache>(10000);  // 默认 10K 容量
  }
}

std::optional<bool> CedarScan::CheckEntityStateInMemory(
    uint64_t entity_id, EntityType entity_type) const {
  // L1: 活跃位图检查（O(1)）
  // 优先使用 LsmEngine 的全局 Bitmap
  if (engine_) {
    if (engine_->IsEntityActive(entity_id)) {
      return true;  // 活跃实体
    }
    // 注意：LsmEngine 的 Bitmap 只追踪活跃实体
    // 删除实体需要通过锚点查询确认
  } else if (active_bitmap_) {
    // 回退到本地 Bitmap
    if (active_bitmap_->IsActive(entity_id)) {
      return true;  // 活跃实体
    }
    // 检查是否在删除集中
    if (!active_bitmap_->Contains(entity_id)) {
      // 不在活跃集也不在删除集，可能是旧数据或未加载
      // 继续检查锚点缓存
    }
  }
  
  // L3: 锚点缓存检查
  if (anchor_cache_) {
    auto cached = anchor_cache_->Get(entity_id, entity_type);
    if (cached.has_value()) {
      return cached->IsActive();
    }
  }
  
  // 内存未命中
  return std::nullopt;
}

}  // namespace cedar
