#ifndef CEDAR_QUERY_QUERY_CACHE_H_
#define CEDAR_QUERY_QUERY_CACHE_H_

#include <cstdint>
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <list>
#include <chrono>
#include "cedar/core/status.h"
#include "query_service.pb.h"

namespace cedar {
namespace query {

struct QueryCacheConfig {
  size_t max_entries = 10000;
  size_t max_memory_bytes = 100 * 1024 * 1024;
  uint32_t default_ttl_seconds = 60;
  bool enable_temporal_cache = true;
};

struct CacheKey {
  std::string query_fingerprint;
  uint64_t partition_hash;
  uint64_t as_of_timestamp;
  
  bool operator==(const CacheKey& other) const {
    return query_fingerprint == other.query_fingerprint &&
           partition_hash == other.partition_hash &&
           as_of_timestamp == other.as_of_timestamp;
  }
};

struct CacheKeyHash {
  size_t operator()(const CacheKey& key) const {
    return std::hash<std::string>()(key.query_fingerprint) ^
           std::hash<uint64_t>()(key.partition_hash) ^
           std::hash<uint64_t>()(key.as_of_timestamp);
  }
};

struct CacheEntry {
  ResultSet result_set;
  std::chrono::steady_clock::time_point created_at;
  std::chrono::steady_clock::time_point last_accessed;
  uint32_t access_count = 0;
  size_t memory_size = 0;
};

class QueryCache {
 public:
  explicit QueryCache(const QueryCacheConfig& config);
  ~QueryCache();
  QueryCache(const QueryCache&) = delete;
  QueryCache& operator=(const QueryCache&) = delete;

  StatusOr<ResultSet> Get(const CacheKey& key);
  Status Put(const CacheKey& key, const ResultSet& result);
  Status Invalidate(const CacheKey& key);
  Status InvalidateAll();
  
  // Invalidate all entries whose fingerprint contains the given prefix.
  void InvalidateByPrefix(const std::string& fingerprint_prefix);

  struct Stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t evictions = 0;
    uint64_t total_entries = 0;
    size_t current_memory = 0;
    double hit_rate = 0.0;
  };
  Stats GetStats() const;

 private:
  QueryCacheConfig config_;
  mutable std::mutex mutex_;
  std::unordered_map<CacheKey, std::list<std::pair<CacheKey, CacheEntry>>::iterator, CacheKeyHash> cache_map_;
  std::list<std::pair<CacheKey, CacheEntry>> lru_list_;
  size_t current_memory_ = 0;
  Stats stats_;
  
  void EvictIfNeeded();
  void UpdateLRU(const CacheKey& key, std::list<std::pair<CacheKey, CacheEntry>>::iterator it);
  size_t CalculateResultSize(const ResultSet& result);
  bool IsExpired(const CacheEntry& entry) const;
};

}  // namespace query
}  // namespace cedar

#endif
