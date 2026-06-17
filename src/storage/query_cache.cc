// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/query_cache.h"

namespace cedar {

QueryCache::QueryCache(size_t max_size_bytes)
    : max_size_bytes_(max_size_bytes), current_size_bytes_(0) {}

std::optional<Descriptor> QueryCache::Get(uint64_t entity_id, uint16_t column_id, 
                                          uint64_t timestamp) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  QueryCacheKey key{entity_id, column_id, timestamp};
  auto it = cache_.find(key);
  
  if (it != cache_.end()) {
    // Cache hit
    hits_.fetch_add(1, std::memory_order_relaxed);
    
    // Move to front (most recent)
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second.first);
    it->second.second.access_time = std::chrono::steady_clock::now();
    
    return it->second.second.result;
  }
  
  // Cache miss
  misses_.fetch_add(1, std::memory_order_relaxed);
  return std::nullopt;
}

bool QueryCache::Has(uint64_t entity_id, uint16_t column_id, uint64_t timestamp) const {
  std::lock_guard<std::mutex> lock(mutex_);
  QueryCacheKey key{entity_id, column_id, timestamp};
  return cache_.count(key) > 0;
}

std::pair<bool, std::optional<Descriptor>> QueryCache::TryGet(
    uint64_t entity_id, uint16_t column_id, uint64_t timestamp) {
  std::lock_guard<std::mutex> lock(mutex_);
  QueryCacheKey key{entity_id, column_id, timestamp};
  auto it = cache_.find(key);
  if (it != cache_.end()) {
    hits_.fetch_add(1, std::memory_order_relaxed);
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second.first);
    it->second.second.access_time = std::chrono::steady_clock::now();
    return {true, it->second.second.result};
  }
  misses_.fetch_add(1, std::memory_order_relaxed);
  return {false, std::nullopt};
}

void QueryCache::Put(uint64_t entity_id, uint16_t column_id, uint64_t timestamp,
                     const std::optional<Descriptor>& result) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  QueryCacheKey key{entity_id, column_id, timestamp};
  size_t entry_size = CalculateEntrySize(result);
  
  // Check if already exists
  auto it = cache_.find(key);
  if (it != cache_.end()) {
    // Update existing entry
    current_size_bytes_ -= it->second.second.size_bytes;
    current_size_bytes_ += entry_size;
    it->second.second.result = result;
    it->second.second.size_bytes = entry_size;
    it->second.second.access_time = std::chrono::steady_clock::now();
    MoveToFront(key);
    return;
  }
  
  // Evict if necessary
  while (current_size_bytes_ + entry_size > max_size_bytes_ && !lru_list_.empty()) {
    EvictLRU();
  }
  
  // Insert new entry
  lru_list_.push_front(key);
  QueryCacheEntry entry;
  entry.result = result;
  entry.size_bytes = entry_size;
  entry.access_time = std::chrono::steady_clock::now();
  
  cache_[key] = std::make_pair(lru_list_.begin(), entry);
  entity_index_[entity_id].push_back(key);
  current_size_bytes_ += entry_size;
}

void QueryCache::Invalidate(uint64_t entity_id, uint16_t column_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto idx_it = entity_index_.find(entity_id);
  if (idx_it == entity_index_.end()) return;
  
  auto& keys = idx_it->second;
  auto write_it = keys.begin();
  for (auto read_it = keys.begin(); read_it != keys.end(); ++read_it) {
    const auto& k = *read_it;
    if (column_id != UINT16_MAX && k.column_id != column_id) {
      *write_it++ = k;
      continue;
    }
    auto it = cache_.find(k);
    if (it != cache_.end()) {
      current_size_bytes_ -= it->second.second.size_bytes;
      lru_list_.erase(it->second.first);
      cache_.erase(it);
    }
  }
  keys.erase(write_it, keys.end());
  if (keys.empty()) {
    entity_index_.erase(idx_it);
  }
}

void QueryCache::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  cache_.clear();
  lru_list_.clear();
  entity_index_.clear();
  current_size_bytes_ = 0;
}

QueryCache::Stats QueryCache::GetStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  Stats stats;
  stats.size_bytes = current_size_bytes_;
  stats.max_size_bytes = max_size_bytes_;
  stats.entries = cache_.size();
  stats.hits = hits_.load(std::memory_order_relaxed);
  stats.misses = misses_.load(std::memory_order_relaxed);
  
  uint64_t total = stats.hits + stats.misses;
  if (total > 0) {
    stats.hit_rate = static_cast<double>(stats.hits) / total;
  }
  
  return stats;
}

void QueryCache::MoveToFront(const QueryCacheKey& key) {
  auto it = cache_.find(key);
  if (it != cache_.end()) {
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second.first);
  }
}

void QueryCache::EvictLRU() {
  if (lru_list_.empty()) return;
  
  const QueryCacheKey& lru_key = lru_list_.back();
  auto it = cache_.find(lru_key);
  if (it != cache_.end()) {
    current_size_bytes_ -= it->second.second.size_bytes;
    cache_.erase(it);
  }
  // Remove from entity index
  auto idx_it = entity_index_.find(lru_key.entity_id);
  if (idx_it != entity_index_.end()) {
    auto& keys = idx_it->second;
    keys.erase(std::remove_if(keys.begin(), keys.end(),
        [&lru_key](const QueryCacheKey& k) {
          return k.entity_id == lru_key.entity_id &&
                 k.column_id == lru_key.column_id &&
                 k.timestamp == lru_key.timestamp;
        }), keys.end());
    if (keys.empty()) entity_index_.erase(idx_it);
  }
  lru_list_.pop_back();
}

size_t QueryCache::CalculateEntrySize(const std::optional<Descriptor>& result) {
  // Base size for key and metadata
  size_t size = sizeof(QueryCacheKey) + sizeof(QueryCacheEntry);
  
  // Add size of descriptor if present
  if (result.has_value()) {
    size += sizeof(Descriptor);
  }
  
  return size;
}

}  // namespace cedar
