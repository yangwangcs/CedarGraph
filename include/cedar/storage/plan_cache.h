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

// =============================================================================
// StorageD Plan Cache
// =============================================================================
// Caches parsed + planned execution plans on the storage node so that
// parameterized queries (same template, different literals) avoid repeated
// parse/plan overhead.
// =============================================================================

#ifndef CEDAR_STORAGE_PLAN_CACHE_H_
#define CEDAR_STORAGE_PLAN_CACHE_H_

#include <chrono>
#include <cstddef>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace cedar {
namespace cypher {
class ExecutionPlan;
}
namespace storage {

class PlanCache {
 public:
  struct Entry {
    std::shared_ptr<cypher::ExecutionPlan> plan;
    std::chrono::steady_clock::time_point inserted_at;
    uint64_t hit_count{0};
  };

  explicit PlanCache(size_t max_entries = 1024,
                     std::chrono::seconds ttl = std::chrono::seconds(300));

  // Lookup a cached plan by AST fingerprint.
  std::shared_ptr<cypher::ExecutionPlan> Lookup(const std::string& fingerprint);

  // Store a newly compiled plan.
  void Store(const std::string& fingerprint,
             std::shared_ptr<cypher::ExecutionPlan> plan);

  // Invalidate all entries (e.g., after schema change or partition migration).
  void InvalidateAll();

  // Current number of cached entries.
  size_t Size() const;

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, Entry> cache_;
  size_t max_entries_;
  std::chrono::seconds ttl_;

  void EvictIfNeeded();
  void EvictExpired();
};

}  // namespace storage
}  // namespace cedar

#endif  // CEDAR_STORAGE_PLAN_CACHE_H_
