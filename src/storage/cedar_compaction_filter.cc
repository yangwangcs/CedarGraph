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

#include "cedar/storage/cedar_compaction_filter.h"

#include <algorithm>

namespace cedar {

// ============================================================================
// CedarCompactionFilter Implementation
// ============================================================================

CedarCompactionFilter::CedarCompactionFilter(const Options& options)
    : options_(options) {
}

CompactionDecision CedarCompactionFilter::Filter(
    const CedarKey& key, const Descriptor& descriptor) {
  
  stats_.total_filtered++;
  
  // 检查是否是墓碑
  if (key.IsTombstone()) {
    // 墓碑：检查是否超出保留期
    if (ShouldRemoveTombstone(key)) {
      stats_.removed++;
      return CompactionDecision::kRemove;
    }
    // 保留墓碑以维护一致性
    stats_.tombstones_retained++;
    return CompactionDecision::kKeepAsTombstone;
  }
  
  // 检查是否应该移动到冷存储
  if (options_.enable_cold_storage && ShouldMoveToCold(key)) {
    if (cold_storage_) {
      // 写入冷存储
      if (cold_storage_->Write(key, descriptor)) {
        stats_.moved_to_cold++;
        return CompactionDecision::kMoveToCold;
      }
    }
  }
  
  // 默认：保留在热存储
  stats_.kept_in_hot++;
  return CompactionDecision::kKeep;
}

void CedarCompactionFilter::FilterBatch(
    const std::vector<std::pair<CedarKey, Descriptor>>& entries,
    std::vector<CompactionDecision>* decisions) {
  
  decisions->clear();
  decisions->reserve(entries.size());
  
  for (const auto& [key, descriptor] : entries) {
    decisions->push_back(Filter(key, descriptor));
  }
}

void CedarCompactionFilter::SetColdStorage(
    std::shared_ptr<ColdStorage> cold_storage) {
  cold_storage_ = std::move(cold_storage);
}

bool CedarCompactionFilter::ShouldMoveToCold(const CedarKey& key) const {
  if (options_.cold_storage_threshold_us == std::numeric_limits<uint64_t>::max()) {
    return false;  // 未启用冷热分离
  }
  
  uint64_t key_time = key.timestamp().value();
  uint64_t threshold = options_.current_time_us - options_.cold_storage_threshold_us;
  
  return key_time < threshold;
}

bool CedarCompactionFilter::ShouldRemoveTombstone(const CedarKey& key) const {
  if (options_.min_retention_us == std::numeric_limits<uint64_t>::max()) {
    return false;  // 永不删除
  }
  
  uint64_t key_time = key.timestamp().value();
  uint64_t threshold = options_.current_time_us - options_.min_retention_us;
  
  // 只有当墓碑的创建时间早于保留期阈值时，才能删除
  return key_time < threshold;
}

// ============================================================================
// GlobalRetentionManager Implementation
// ============================================================================

GlobalRetentionManager& GlobalRetentionManager::Instance() {
  static GlobalRetentionManager instance;
  return instance;
}

uint64_t GlobalRetentionManager::GetMinRetentionUs() const {
  return min_retention_us_.load();
}

void GlobalRetentionManager::SetMinRetentionUs(uint64_t retention_us) {
  min_retention_us_.store(retention_us);
}

uint64_t GlobalRetentionManager::GetColdStorageThresholdUs() const {
  return cold_storage_threshold_us_.load();
}

void GlobalRetentionManager::SetColdStorageThresholdUs(uint64_t threshold_us) {
  cold_storage_threshold_us_.store(threshold_us);
}

}  // namespace cedar
