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

#include "cedar/graph/graph_semantic_layer.h"

#include <thread>
#include <vector>
#include <algorithm>
#include <chrono>
#include <future>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/core/env.h"
#include "cedar/graph/pushdown_predicate.h"
#include "cedar/graph/cedar_graph.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// ============================================================================
// SharedIOContext Implementation
// ============================================================================

SharedIOContext::SharedIOContext() = default;

SharedIOContext::~SharedIOContext() = default;

std::shared_ptr<CachedBlock> SharedIOContext::GetBlock(
    const std::string& file_path, size_t block_idx) const {
  CacheKey key = {file_path, block_idx};
  
  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto it = block_cache_.find(key);
    if (it != block_cache_.end()) {
      if (auto block = it->second.lock()) {
        block->last_access = std::chrono::steady_clock::now();
        block->ref_count.fetch_add(1);
        
        {
          std::lock_guard<std::mutex> stats_lock(stats_mutex_);
          cache_hits_++;
        }
        return block;
      }
    }
  }
  
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    cache_misses_++;
  }
  return nullptr;
}

void SharedIOContext::CacheBlock(const std::string& file_path, size_t block_idx,
                                 std::shared_ptr<CachedBlock> block) {
  CacheKey key = {file_path, block_idx};
  
  std::lock_guard<std::mutex> lock(cache_mutex_);
  block_cache_[key] = block;
  
  constexpr size_t kBlockCacheLimit = 1000;
  if (block_cache_.size() > kBlockCacheLimit) {
    for (auto it = block_cache_.begin(); it != block_cache_.end();) {
      if (it->second.expired()) {
        it = block_cache_.erase(it);
      } else {
        if (auto block = it->second.lock()) {
          if (block->ref_count.load() > 0) {
            block->ref_count.fetch_sub(1);
          }
        }
        it = block_cache_.erase(it);
      }
    }
  }
}

bool SharedIOContext::HasBlock(const std::string& file_path, size_t block_idx) const {
  CacheKey key = {file_path, block_idx};
  
  std::lock_guard<std::mutex> lock(cache_mutex_);
  auto it = block_cache_.find(key);
  return it != block_cache_.end() && !it->second.expired();
}

bool SharedIOContext::BlockMayContainTimeRange(const std::string& file_path, 
                                               size_t block_idx,
                                               Timestamp start, 
                                               Timestamp end) const {
  if (auto block = GetBlock(file_path, block_idx)) {
    if (block->max_timestamp < start || block->min_timestamp > end) {
      return false;
    }
  }
  return true;
}

bool SharedIOContext::BlockMayContainEntity(const std::string& file_path, 
                                            size_t block_idx,
                                            uint64_t entity_id) const {
  if (auto block = GetBlock(file_path, block_idx)) {
    if (entity_id < block->min_entity_id || entity_id > block->max_entity_id) {
      return false;
    }
  }
  return true;
}

void SharedIOContext::MarkBlockPrefetched(const std::string& file_path, size_t block_idx) {
  CacheKey key = {file_path, block_idx};
  
  std::lock_guard<std::mutex> lock(prefetch_mutex_);
  prefetched_blocks_.insert(key);
}

bool SharedIOContext::IsBlockPrefetched(const std::string& file_path, 
                                        size_t block_idx) const {
  CacheKey key = {file_path, block_idx};
  
  std::lock_guard<std::mutex> lock(prefetch_mutex_);
  return prefetched_blocks_.find(key) != prefetched_blocks_.end();
}

std::vector<std::pair<std::string, size_t>> SharedIOContext::GetPrefetchCandidates(
    size_t max_candidates) const {
  (void)max_candidates;
  std::vector<std::pair<std::string, size_t>> candidates;
  
  std::lock_guard<std::mutex> lock(history_mutex_);
  
  if (access_history_.size() < 2) {
    return candidates;
  }
  
  const auto& last = access_history_.back();
  const auto& second_last = access_history_[access_history_.size() - 2];
  
  if (last.first == second_last.first && last.second == second_last.second + 1) {
    std::string file_path = last.first;
    size_t next_block = last.second + 1;
    
    CacheKey next_key = {file_path, next_block};
    
    std::lock_guard<std::mutex> prefetch_lock(prefetch_mutex_);
    if (prefetched_blocks_.find(next_key) == prefetched_blocks_.end()) {
      candidates.push_back({file_path, next_block});
    }
  }
  
  return candidates;
}

void SharedIOContext::ClearCache() {
  std::lock_guard<std::mutex> lock(cache_mutex_);
  block_cache_.clear();
}

double SharedIOContext::GetHitRate() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  size_t total = cache_hits_ + cache_misses_;
  if (total == 0) return 0.0;
  return static_cast<double>(cache_hits_) / total;
}

void SharedIOContext::ResetStats() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  cache_hits_ = 0;
  cache_misses_ = 0;
}

// ============================================================================
// GraphSemanticLayer Implementation
// ============================================================================

GraphSemanticLayer::GraphSemanticLayer(CedarGraphStorage* storage)
    : storage_(storage),
      thread_pool_(std::make_unique<ThreadPool>(std::thread::hardware_concurrency())) {}

GraphSemanticLayer::~GraphSemanticLayer() = default;

void GraphSemanticLayer::ClearCache() {
  std::lock_guard<std::mutex> lock(cache_mutex_);
  neighbor_cache_.clear();
}

std::vector<BatchNeighborResult> GraphSemanticLayer::BatchGetNeighbors(
    const std::vector<uint64_t>& vertex_ids,
    uint16_t edge_type,
    const PushdownPredicate& predicate,
    size_t num_threads) {
  
  std::vector<BatchNeighborResult> results;
  results.reserve(vertex_ids.size());
  
  if (vertex_ids.empty() || !storage_) {
    return results;
  }
  
  for (uint64_t id : vertex_ids) {
    results.emplace_back(id);
  }
  
  SharedIOContext shared_io;
  
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;
  }
  num_threads = std::min(num_threads, vertex_ids.size());
  num_threads = std::max(size_t(1), num_threads);
  
  if (num_threads > 1 && vertex_ids.size() >= num_threads * 2) {
    size_t vertices_per_thread = vertex_ids.size() / num_threads;
    std::atomic<size_t> completed{0};
    
    for (size_t t = 0; t < num_threads; ++t) {
      size_t start = t * vertices_per_thread;
      size_t end = (t == num_threads - 1) ? vertex_ids.size() : (t + 1) * vertices_per_thread;
      
      thread_pool_->Schedule([this, &results, &vertex_ids, edge_type, &predicate, &shared_io, &completed, start, end]() {
        for (size_t i = start; i < end; ++i) {
          results[i].neighbors = GetOutNeighborsWithPushdown(
              vertex_ids[i], edge_type, predicate, &shared_io);
        }
        completed.fetch_add(1, std::memory_order_release);
      });
    }
    
    while (completed.load(std::memory_order_acquire) < num_threads) {
      std::this_thread::yield();
    }
  } else {
    for (size_t i = 0; i < vertex_ids.size(); ++i) {
      results[i].neighbors = GetOutNeighborsWithPushdown(
          vertex_ids[i], edge_type, predicate, &shared_io);
    }
  }
  
  return results;
}

std::vector<Neighbor> GraphSemanticLayer::GetOutNeighborsWithPushdown(
    uint64_t vertex_id,
    uint16_t edge_type,
    const PushdownPredicate& predicate,
    SharedIOContext* shared_io) {
  
  std::vector<Neighbor> results;
  (void)shared_io;
  
  if (!storage_) {
    return results;
  }
  
  // Check cache
  if (predicate.IsEmpty() || predicate.IsTimeOnly()) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto it = neighbor_cache_.find(vertex_id);
    if (it != neighbor_cache_.end()) {
      auto now = std::chrono::system_clock::now();
      auto cache_time = std::chrono::time_point<std::chrono::system_clock>(
          std::chrono::microseconds(it->second.timestamp.value()));
      auto elapsed = std::chrono::duration_cast<std::chrono::minutes>(now - cache_time).count();
      
      if (elapsed < 5) {
        for (const auto& n : it->second.neighbors) {
          if (predicate.CheckTimeRange(n.timestamp)) {
            results.push_back(n);
            if (predicate.CheckLimit(results.size())) {
              break;
            }
          }
        }
        return results;
      }
    }
  }
  
  // Query edge storage directly instead of vertex property history
  auto edges = storage_->ScanEdgesWithFolding(
      vertex_id, EntityType::EdgeOut, edge_type,
      predicate.time_end.value_or(Timestamp::Max()));

  for (const auto& edge : edges) {
    if (edge.timestamp < predicate.time_start.value_or(Timestamp(0))) {
      continue;
    }
    results.emplace_back(edge.target_id, edge.edge_type, edge.timestamp, std::nullopt);
    if (predicate.CheckLimit(results.size())) {
      break;
    }
  }
  
  // Update cache
  if (predicate.IsEmpty() || predicate.IsTimeOnly()) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    if (neighbor_cache_.size() >= kMaxCacheSize) {
      neighbor_cache_.clear();
    }
    CacheEntry entry;
    entry.neighbors = results;
    entry.timestamp = Timestamp(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    neighbor_cache_[vertex_id] = std::move(entry);
  }
  
  return results;
}

std::vector<std::vector<uint64_t>> GraphSemanticLayer::BfsWithPushdown(
    uint64_t start, uint16_t edge_type, size_t max_depth,
    const PushdownPredicate& predicate,
    size_t num_threads) {
  
  std::vector<std::vector<uint64_t>> result;
  
  if (!storage_ || max_depth == 0) {
    return result;
  }
  
  std::vector<uint64_t> current_level = {start};
  std::unordered_set<uint64_t> visited;
  visited.insert(start);
  
  for (size_t depth = 0; depth < max_depth; ++depth) {
    if (current_level.empty()) {
      break;
    }
    
    result.push_back(current_level);
    
    auto batch_results = BatchGetNeighbors(current_level, edge_type, predicate, num_threads);
    
    std::vector<uint64_t> next_level;
    for (const auto& batch_result : batch_results) {
      for (const auto& neighbor : batch_result.neighbors) {
        if (visited.insert(neighbor.id).second) {
          next_level.push_back(neighbor.id);
        }
      }
    }
    
    current_level = std::move(next_level);
  }
  
  return result;
}

std::unordered_map<uint64_t, size_t> GraphSemanticLayer::DegreeCentrality(
    const std::vector<uint64_t>& vertex_ids,
    uint16_t edge_type,
    const PushdownPredicate& predicate) {
  
  std::unordered_map<uint64_t, size_t> degrees;
  
  auto results = BatchGetNeighbors(vertex_ids, edge_type, predicate);
  
  for (const auto& result : results) {
    degrees[result.vertex_id] = result.neighbors.size();
  }
  
  return degrees;
}

std::vector<std::vector<uint64_t>> GraphSemanticLayer::ConnectedComponents(
    const std::vector<uint64_t>& seed_vertices,
    uint16_t edge_type) {
  
  std::vector<std::vector<uint64_t>> components;
  std::unordered_set<uint64_t> visited;
  
  PushdownPredicate empty_predicate;
  
  for (uint64_t seed : seed_vertices) {
    if (visited.find(seed) != visited.end()) {
      continue;
    }
    
    std::vector<uint64_t> component;
    std::vector<uint64_t> current_level = {seed};
    visited.insert(seed);
    
    while (!current_level.empty()) {
      component.insert(component.end(), current_level.begin(), current_level.end());
      
      auto results = BatchGetNeighbors(current_level, edge_type, empty_predicate);
      
      std::vector<uint64_t> next_level;
      for (const auto& result : results) {
        for (const auto& neighbor : result.neighbors) {
          if (visited.find(neighbor.id) == visited.end()) {
            visited.insert(neighbor.id);
            next_level.push_back(neighbor.id);
          }
        }
      }
      
      current_level = std::move(next_level);
    }
    
    components.push_back(std::move(component));
  }
  
  return components;
}

void GraphSemanticLayer::GetCacheStats(size_t* cache_size, double* hit_rate) const {
  *cache_size = neighbor_cache_.size();
  *hit_rate = 0.0;
}

}  // namespace cedar
