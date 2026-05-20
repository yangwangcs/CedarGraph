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

#include "cedar/storage/plan_cache.h"

#include "cedar/cypher/execution_plan.h"

namespace cedar {
namespace storage {

PlanCache::PlanCache(size_t max_entries, std::chrono::seconds ttl)
    : max_entries_(max_entries), ttl_(ttl) {}

std::shared_ptr<cypher::ExecutionPlan> PlanCache::Lookup(
    const std::string& fingerprint) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = cache_.find(fingerprint);
  if (it == cache_.end()) {
    return nullptr;
  }
  if (std::chrono::steady_clock::now() - it->second.inserted_at > ttl_) {
    // Expired: upgrade to unique lock and erase.
    lock.unlock();
    {
      std::unique_lock<std::shared_mutex> ulock(mutex_);
      cache_.erase(fingerprint);
    }
    return nullptr;
  }
  it->second.hit_count++;
  return it->second.plan;
}

void PlanCache::Store(const std::string& fingerprint,
                      std::shared_ptr<cypher::ExecutionPlan> plan) {
  if (!plan || fingerprint.empty()) return;
  std::unique_lock<std::shared_mutex> lock(mutex_);
  EvictExpired();
  EvictIfNeeded();
  Entry entry;
  entry.plan = std::move(plan);
  entry.inserted_at = std::chrono::steady_clock::now();
  cache_[fingerprint] = std::move(entry);
}

void PlanCache::InvalidateAll() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  cache_.clear();
}

size_t PlanCache::Size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return cache_.size();
}

void PlanCache::EvictIfNeeded() {
  if (cache_.size() < max_entries_) return;
  // Simple LRU eviction: remove the oldest entry by insertion time.
  auto oldest = cache_.begin();
  for (auto it = cache_.begin(); it != cache_.end(); ++it) {
    if (it->second.inserted_at < oldest->second.inserted_at) {
      oldest = it;
    }
  }
  if (oldest != cache_.end()) {
    cache_.erase(oldest);
  }
}

void PlanCache::EvictExpired() {
  auto now = std::chrono::steady_clock::now();
  for (auto it = cache_.begin(); it != cache_.end();) {
    if (now - it->second.inserted_at > ttl_) {
      it = cache_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace storage
}  // namespace cedar
