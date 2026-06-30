#include "query/query_cache.h"

#include <algorithm>

namespace cedar {
namespace query {

QueryCache::QueryCache(const QueryCacheConfig& config) : config_(config) {
  // Reserve space to avoid rehashing
  cache_map_.reserve(config.max_entries);
}

QueryCache::~QueryCache() {
  // Clear all entries
  InvalidateAll();
}

StatusOr<ResultSet> QueryCache::Get(const CacheKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_map_.find(key);
  if (it == cache_map_.end()) {
    stats_.misses++;
    return Status::NotFound("Cache miss for query key");
  }
  
  // Check if entry is expired
  CacheEntry& entry = it->second->second;
  if (IsExpired(entry)) {
    // Remove expired entry
    current_memory_ -= entry.memory_size;
    lru_list_.erase(it->second);
    cache_map_.erase(it);
    stats_.misses++;
    stats_.evictions++;
    return Status::NotFound("Cache entry expired");
  }
  
  // Update LRU order and access stats
  UpdateLRU(key, it->second);
  entry.last_accessed = std::chrono::steady_clock::now();
  entry.access_count++;
  
  stats_.hits++;
  return entry.result_set;
}

Status QueryCache::Put(const CacheKey& key, const ResultSet& result) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Check if key already exists
  auto existing_it = cache_map_.find(key);
  if (existing_it != cache_map_.end()) {
    // Update existing entry
    CacheEntry& existing_entry = existing_it->second->second;
    current_memory_ -= existing_entry.memory_size;
    
    // Remove from current position in LRU list
    lru_list_.erase(existing_it->second);
    cache_map_.erase(existing_it);
  }
  
  // Calculate memory size for the new entry
  size_t result_size = CalculateResultSize(result);
  
  // Check if single entry exceeds max memory
  if (result_size > config_.max_memory_bytes) {
    return Status::InvalidArgument("Result too large to cache");
  }
  
  // Evict entries if needed
  while ((lru_list_.size() >= config_.max_entries || 
          current_memory_ + result_size > config_.max_memory_bytes) && 
         !lru_list_.empty()) {
    EvictIfNeeded();
  }
  
  // Create new cache entry
  CacheEntry entry;
  entry.result_set = result;
  entry.created_at = std::chrono::steady_clock::now();
  entry.last_accessed = entry.created_at;
  entry.access_count = 0;
  entry.memory_size = result_size;
  
  // Add to front of LRU list (most recent)
  lru_list_.emplace_front(key, std::move(entry));
  auto list_it = lru_list_.begin();
  
  // Add to hash map
  cache_map_[key] = list_it;
  current_memory_ += result_size;
  
  return Status::OK();
}

Status QueryCache::Invalidate(const CacheKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_map_.find(key);
  if (it == cache_map_.end()) {
    return Status::NotFound("Key not found in cache");
  }
  
  // Free memory and remove from both structures
  CacheEntry& entry = it->second->second;
  current_memory_ -= entry.memory_size;
  lru_list_.erase(it->second);
  cache_map_.erase(it);
  
  return Status::OK();
}

Status QueryCache::InvalidateAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  cache_map_.clear();
  lru_list_.clear();
  current_memory_ = 0;
  
  return Status::OK();
}

void QueryCache::InvalidateByPrefix(const std::string& fingerprint_prefix) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = cache_map_.begin(); it != cache_map_.end(); ) {
    if (it->first.query_fingerprint.find(fingerprint_prefix) == 0) {
      current_memory_ -= it->second->second.memory_size;
      lru_list_.erase(it->second);
      it = cache_map_.erase(it);
    } else {
      ++it;
    }
  }
}

QueryCache::Stats QueryCache::GetStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  Stats stats = stats_;
  stats.total_entries = cache_map_.size();
  stats.current_memory = current_memory_;
  
  // Calculate hit rate
  uint64_t total_requests = stats_.hits + stats_.misses;
  if (total_requests > 0) {
    stats.hit_rate = static_cast<double>(stats_.hits) / total_requests;
  } else {
    stats.hit_rate = 0.0;
  }
  
  return stats;
}

void QueryCache::EvictIfNeeded() {
  if (lru_list_.empty()) {
    return;
  }
  
  // Remove from back of LRU list (least recent)
  auto last_it = std::prev(lru_list_.end());
  CacheEntry& entry = last_it->second;
  
  current_memory_ -= entry.memory_size;
  cache_map_.erase(last_it->first);
  lru_list_.pop_back();
  
  stats_.evictions++;
}

void QueryCache::UpdateLRU(const CacheKey& key, 
                           std::list<std::pair<CacheKey, CacheEntry>>::iterator it) {
  // Move the entry to the front of the list (most recent)
  if (it != lru_list_.begin()) {
    lru_list_.splice(lru_list_.begin(), lru_list_, it);
  }
}

size_t QueryCache::CalculateResultSize(const ResultSet& result) {
  size_t size = 0;
  
  // Estimate size based on rows
  for (const auto& row : result.rows()) {
    // Add overhead for each row
    size += sizeof(Row);
    
    // Add size of values in the row
    for (const auto& value : row.values()) {
      size += sizeof(Value);
      switch (value.value_type_case()) {
        case Value::kStringVal:
          size += value.string_val().size();
          break;
        case Value::kBytesVal:
          size += value.bytes_val().size();
          break;
        default:
          break;
      }
    }
  }
  
  // Add size for column names
  for (const auto& col : result.columns()) {
    size += col.size();
  }
  
  // Add metadata overhead
  size += sizeof(ResultSet);
  size += sizeof(result.total_rows());
  
  return size;
}

bool QueryCache::IsExpired(const CacheEntry& entry) const {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
      now - entry.created_at).count();
  return elapsed > config_.default_ttl_seconds;
}

}  // namespace query
}  // namespace cedar
