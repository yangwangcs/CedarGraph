// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/active_entity_bitmap.h"

namespace cedar {

// =============================================================================
// ActiveEntityBitmap Implementation
// =============================================================================

void ActiveEntityBitmap::MarkActive(uint64_t entity_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  active_set_.insert(entity_id);
  deleted_set_.erase(entity_id);  // 从删除集中移除
}

void ActiveEntityBitmap::MarkDeleted(uint64_t entity_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  active_set_.erase(entity_id);
  deleted_set_.insert(entity_id);
}

void ActiveEntityBitmap::MarkActiveBatch(const std::vector<uint64_t>& entity_ids) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (uint64_t id : entity_ids) {
    active_set_.insert(id);
    deleted_set_.erase(id);
  }
}

void ActiveEntityBitmap::MarkDeletedBatch(const std::vector<uint64_t>& entity_ids) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (uint64_t id : entity_ids) {
    active_set_.erase(id);
    deleted_set_.insert(id);
  }
}

bool ActiveEntityBitmap::IsActive(uint64_t entity_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_set_.find(entity_id) != active_set_.end();
}

bool ActiveEntityBitmap::Contains(uint64_t entity_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_set_.find(entity_id) != active_set_.end() ||
         deleted_set_.find(entity_id) != deleted_set_.end();
}

std::vector<uint64_t> ActiveEntityBitmap::FilterActive(
    const std::vector<uint64_t>& entity_ids) const {
  std::vector<uint64_t> result;
  result.reserve(entity_ids.size());
  
  std::lock_guard<std::mutex> lock(mutex_);
  for (uint64_t id : entity_ids) {
    if (active_set_.find(id) != active_set_.end()) {
      result.push_back(id);
    }
  }
  
  return result;
}

size_t ActiveEntityBitmap::ActiveCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_set_.size();
}

size_t ActiveEntityBitmap::DeletedCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return deleted_set_.size();
}

size_t ActiveEntityBitmap::MemoryUsage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  // 粗略估计：每个 entry 约 16 字节（key + bucket overhead）
  return (active_set_.size() + deleted_set_.size()) * 16 + sizeof(*this);
}

void ActiveEntityBitmap::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  active_set_.clear();
  deleted_set_.clear();
}

void ActiveEntityBitmap::CompactDeletedSet() {
  std::lock_guard<std::mutex> lock(mutex_);
  // 如果删除集过大，可以只保留最近的 N 个
  // 简化实现：暂时不做特殊处理
}

// =============================================================================
// VSLNodeHint Implementation
// =============================================================================

uint8_t VSLNodeHint::FromCedarKey(const CedarKey& key) {
  uint8_t hint = 0;
  
  if (key.IsCreate()) {
    hint |= vsl_node_hint::kActive;
  } else if (key.IsDelete()) {
    hint |= vsl_node_hint::kDeleted;
  }
  
  return hint;
}

bool VSLNodeHint::IsActive(uint8_t hint) {
  return (hint & vsl_node_hint::kActive) != 0;
}

bool VSLNodeHint::IsDeleted(uint8_t hint) {
  return (hint & vsl_node_hint::kDeleted) != 0;
}

// =============================================================================
// AnchorCache Implementation
// =============================================================================

AnchorCache::AnchorCache(size_t capacity) : capacity_(capacity) {}

std::optional<StateAnchor> AnchorCache::Get(
    uint64_t entity_id, EntityType entity_type) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  AnchorCacheKey key{entity_id, static_cast<uint8_t>(entity_type)};
  auto it = cache_.find(key);
  
  if (it != cache_.end()) {
    hits_++;
    // 由于 method 是 const，不能直接修改 access_count
    // 简化：暂时不更新 access_count，或使用 mutable
    return it->second.anchor;
  }
  
  misses_++;
  return std::nullopt;
}

void AnchorCache::Put(uint64_t entity_id, EntityType entity_type, 
                      const StateAnchor& anchor) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  AnchorCacheKey key{entity_id, static_cast<uint8_t>(entity_type)};
  
  // If key already exists, update it
  auto existing = cache_.find(key);
  if (existing != cache_.end()) {
    existing->second.anchor = anchor;
    existing->second.access_count++;
    return;
  }
  
  // If full, evict the oldest entry (lowest access_count)
  if (cache_.size() >= capacity_ && !cache_.empty()) {
    auto oldest = cache_.begin();
    for (auto it = cache_.begin(); it != cache_.end(); ++it) {
      if (it->second.access_count < oldest->second.access_count) {
        oldest = it;
      }
    }
    cache_.erase(oldest);
  }
  
  CacheEntry entry{anchor, 1};
  cache_[key] = entry;
}

void AnchorCache::Invalidate(uint64_t entity_id, EntityType entity_type) {
  std::lock_guard<std::mutex> lock(mutex_);
  AnchorCacheKey key{entity_id, static_cast<uint8_t>(entity_type)};
  cache_.erase(key);
}

void AnchorCache::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_.clear();
}

double AnchorCache::HitRate() const {
  std::lock_guard<std::mutex> lock(mutex_);
  uint64_t total = hits_ + misses_;
  return total > 0 ? static_cast<double>(hits_) / total : 0.0;
}

void AnchorCache::ResetStats() {
  std::lock_guard<std::mutex> lock(mutex_);
  hits_ = 0;
  misses_ = 0;
}

}  // namespace cedar
