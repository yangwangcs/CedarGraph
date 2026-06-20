// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// LRU Cache implementation for plan caching

#pragma once

#include <list>
#include <unordered_map>
#include <shared_mutex>
#include <cstddef>

namespace cedar {

template <typename Key, typename Value>
class LRUCache {
 public:
  explicit LRUCache(size_t capacity) : capacity_(capacity) {}

  // Get value by key. Returns nullptr if not found.
  // Moves the entry to the front of the LRU list (most recently used).
  std::shared_ptr<Value> Get(const Key& key) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = cache_.find(key);
    if (it == cache_.end()) {
      return nullptr;
    }
    // Move to front (most recently used)
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second.iterator);
    it->second.hits++;
    return it->second.value;
  }

  // Put key-value pair into cache.
  // If key already exists, updates the value and moves to front.
  // If cache is full, evicts the least recently used entry.
  void Put(const Key& key, std::shared_ptr<Value> value) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      // Update existing entry
      it->second.value = value;
      lru_list_.splice(lru_list_.begin(), lru_list_, it->second.iterator);
      return;
    }
    
    // Evict if full
    if (cache_.size() >= capacity_) {
      auto last = lru_list_.back();
      cache_.erase(last);
      lru_list_.pop_back();
    }
    
    // Insert new entry
    lru_list_.push_front(key);
    cache_[key] = {value, lru_list_.begin(), 0};
  }

  // Remove entry by key
  void Remove(const Key& key) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      lru_list_.erase(it->second.iterator);
      cache_.erase(it);
    }
  }

  // Clear all entries
  void Clear() {
    std::unique_lock<std::mutex> lock(mutex_);
    cache_.clear();
    lru_list_.clear();
  }

  // Get current size
  size_t Size() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return cache_.size();
  }

  // Get capacity
  size_t Capacity() const {
    return capacity_;
  }

  // Get cache statistics
  struct Stats {
    size_t size;
    size_t capacity;
    uint64_t total_hits;
    uint64_t total_misses;
    double hit_rate;
  };

  Stats GetStats() const {
    std::unique_lock<std::mutex> lock(mutex_);
    Stats stats;
    stats.size = cache_.size();
    stats.capacity = capacity_;
    stats.total_hits = total_hits_;
    stats.total_misses = total_misses_;
    stats.hit_rate = (total_hits_ + total_misses_ > 0) ? 
        static_cast<double>(total_hits_) / (total_hits_ + total_misses_) : 0.0;
    return stats;
  }

 private:
  struct CacheEntry {
    std::shared_ptr<Value> value;
    typename std::list<Key>::iterator iterator;
    uint64_t hits;
  };

  size_t capacity_;
  mutable std::mutex mutex_;
  std::list<Key> lru_list_;  // Front = most recently used
  std::unordered_map<Key, CacheEntry> cache_;
  uint64_t total_hits_ = 0;
  uint64_t total_misses_ = 0;
};

}  // namespace cedar
