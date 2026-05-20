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

#ifndef CEDAR_COORDINATOR_LOCATION_TABLE_H_
#define CEDAR_COORDINATOR_LOCATION_TABLE_H_

#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace cedar {
namespace coordinator {

struct CacheWindow {
  uint64_t entity_id;
  uint64_t cached_from;
  uint64_t cached_to;
  uint32_t gcn_node_id;
  uint64_t version;
  uint64_t expire_at;
};

class VertexLocationTable {
 public:
  VertexLocationTable() = default;
  ~VertexLocationTable() = default;

  // Non-copyable, non-movable
  VertexLocationTable(const VertexLocationTable&) = delete;
  VertexLocationTable& operator=(const VertexLocationTable&) = delete;

  // Query the cache window covering entity_id at query_time.
  // Returns std::nullopt if no valid cache is registered.
  std::optional<CacheWindow> Locate(uint64_t entity_id, uint64_t query_time) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = table_.find(entity_id);
    if (it == table_.end()) return std::nullopt;
    if (query_time < it->second.cached_from || query_time > it->second.cached_to) {
      return std::nullopt;
    }
    return it->second;
  }

  // Report a locally-held cache window. Overwrites any existing window
  // for the same entity_id if the new version is >= current.
  void ReportCache(const CacheWindow& window) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = table_.find(window.entity_id);
    if (it == table_.end() || window.version >= it->second.version) {
      table_[window.entity_id] = window;
    }
  }

  // Process a heartbeat containing all currently cached windows.
  // Refreshes expire_at for known windows and adds new ones.
  void Heartbeat(const std::vector<CacheWindow>& windows) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& w : windows) {
      auto it = table_.find(w.entity_id);
      if (it == table_.end() || w.version >= it->second.version) {
        table_[w.entity_id] = w;
      }
    }
  }

  // Remove expired entries.
  void GCExpired(uint64_t now) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = table_.begin(); it != table_.end();) {
      if (it->second.expire_at < now) {
        it = table_.erase(it);
      } else {
        ++it;
      }
    }
  }

  size_t Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return table_.size();
  }

 private:
  mutable std::mutex mutex_;
  std::unordered_map<uint64_t, CacheWindow> table_;
};

}  // namespace coordinator
}  // namespace cedar

#endif  // CEDAR_COORDINATOR_LOCATION_TABLE_H_
