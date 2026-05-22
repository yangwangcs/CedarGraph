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

#include "cedar/dtx/version_chain.h"

#include <algorithm>

namespace cedar {
namespace dtx {

// =============================================================================
// VersionChainHead 实现
// =============================================================================

VersionChainHead::~VersionChainHead() {
  std::unique_lock<std::shared_mutex> lock(chain_mutex_);
  VersionChainNode* node = latest.load(std::memory_order_relaxed);
  while (node) {
    VersionChainNode* next = node->next.load(std::memory_order_relaxed);
    delete node;
    node = next;
  }
}

VersionChainNode* VersionChainHead::GetLatestVisible() const {
  VersionChainNode* node = latest.load(std::memory_order_acquire);
  
  while (node) {
    if (node->visible.load(std::memory_order_acquire)) {
      return node;
    }
    node = node->next.load(std::memory_order_acquire);
  }
  
  return nullptr;
}

VersionChainNode* VersionChainHead::GetVersion(uint64_t target_version) const {
  VersionChainNode* node = latest.load(std::memory_order_acquire);
  
  while (node) {
    if (node->version == target_version && 
        node->visible.load(std::memory_order_acquire)) {
      return node;
    }
    node = node->next.load(std::memory_order_acquire);
  }
  
  return nullptr;
}

bool VersionChainHead::InsertVersion(VersionChainNode* new_node) {
  if (!new_node) return false;
  
  std::unique_lock<std::shared_mutex> lock(chain_mutex_);
  
  VersionChainNode* expected = latest.load(std::memory_order_relaxed);
  new_node->next.store(expected, std::memory_order_relaxed);
  
  // CAS操作：将新节点设置为最新
  while (!latest.compare_exchange_weak(expected, new_node,
                                        std::memory_order_release,
                                        std::memory_order_relaxed)) {
    // 失败，更新expected并重试
    new_node->next.store(expected, std::memory_order_relaxed);
  }
  
  // 增加链长度
  chain_length.fetch_add(1, std::memory_order_relaxed);
  
  return true;
}

// =============================================================================
// VersionChainIndex 实现
// =============================================================================

VersionChainIndex::VersionChainIndex() = default;

VersionChainIndex::~VersionChainIndex() = default;

VersionChainHead* VersionChainIndex::GetOrCreateHead(const CedarKey& key) {
  {
    std::shared_lock<std::shared_mutex> lock(index_mutex_);
    auto it = index_.find(key);
    if (it != index_.end()) {
      return it->second.get();
    }
  }
  
  // 需要创建新的头
  std::unique_lock<std::shared_mutex> lock(index_mutex_);
  auto it = index_.find(key);
  if (it != index_.end()) {
    return it->second.get();
  }
  
  auto head = std::make_unique<VersionChainHead>();
  auto* head_ptr = head.get();
  index_[key] = std::move(head);
  return head_ptr;
}

VersionChainHead* VersionChainIndex::GetHead(const CedarKey& key) const {
  std::shared_lock<std::shared_mutex> lock(index_mutex_);
  auto it = index_.find(key);
  return (it != index_.end()) ? it->second.get() : nullptr;
}

Status VersionChainIndex::ReadVersion(const CedarKey& key,
                                       uint64_t version,
                                       VersionChainNode** node) {
  auto* head = GetHead(key);
  if (!head) {
    return Status::NotFound("VersionChainIndex", "Key not found");
  }
  
  std::shared_lock<std::shared_mutex> chain_lock(head->chain_mutex_);
  head->EnterRead();
  *node = head->GetVersion(version);
  head->ExitRead();
  
  if (!*node) {
    return Status::NotFound("VersionChainIndex", "Version not found");
  }
  
  return Status::OK();
}

Status VersionChainIndex::ReadLatestVisible(const CedarKey& key,
                                             VersionChainNode** node) {
  auto* head = GetHead(key);
  if (!head) {
    return Status::NotFound("VersionChainIndex", "Key not found");
  }
  
  std::shared_lock<std::shared_mutex> chain_lock(head->chain_mutex_);
  head->EnterRead();
  *node = head->GetLatestVisible();
  head->ExitRead();
  
  if (!*node) {
    return Status::NotFound("VersionChainIndex", "No visible version");
  }
  
  return Status::OK();
}

Status VersionChainIndex::CommitVersion(const CedarKey& key,
                                         uint64_t txn_id,
                                         Timestamp commit_ts,
                                         uint64_t version) {
  auto* head = GetOrCreateHead(key);
  
  // 创建新版本节点
  auto* new_node = new VersionChainNode(txn_id, commit_ts, version);
  
  // 插入版本链
  if (!head->InsertVersion(new_node)) {
    delete new_node;
    return Status::IOError("VersionChainIndex", "Failed to insert version");
  }
  
  // 标记为可见
  new_node->visible.store(true, std::memory_order_release);
  
  return Status::OK();
}

ValidationResult VersionChainIndex::FastValidate(const CedarKey& key,
                                                  uint64_t read_version,
                                                  Timestamp commit_ts) {
  auto* head = GetHead(key);
  
  // 如果没有版本链，验证通过
  if (!head) {
    return ValidationResult::kValid;
  }
  
  std::shared_lock<std::shared_mutex> chain_lock(head->chain_mutex_);
  head->EnterRead();
  auto result = ValidateWithHead(head, read_version, commit_ts);
  head->ExitRead();
  return result;
}

ValidationResult VersionChainIndex::FullValidate(const CedarKey& key,
                                                  uint64_t read_version,
                                                  Timestamp commit_ts) {
  auto* head = GetHead(key);
  
  if (!head) {
    return ValidationResult::kValid;
  }
  
  // 先尝试 O(1) 快速验证
  auto result = ValidateWithHead(head, read_version, commit_ts);
  if (result != ValidationResult::kNeedFullCheck) {
    return result;
  }
  
  // 需要遍历版本链
  std::shared_lock<std::shared_mutex> chain_lock(head->chain_mutex_);
  head->EnterRead();
  VersionChainNode* node = head->latest.load(std::memory_order_acquire);
  
  while (node) {
    // 如果找到读取的版本
    if (node->version == read_version) {
      head->ExitRead();
      return ValidationResult::kValid;  // 版本还在，有效
    }
    
    // 如果遇到提交时间戳在读取之后的新版本
    if (node->commit_ts > commit_ts && node->visible.load(std::memory_order_acquire)) {
      head->ExitRead();
      return ValidationResult::kInvalid;  // 冲突
    }
    
    node = node->next.load(std::memory_order_acquire);
  }
  
  head->ExitRead();
  
  // 读取的版本已不存在（被GC或从未存在）
  return ValidationResult::kInvalid;
}

std::vector<std::pair<CedarKey, ValidationResult>> VersionChainIndex::BatchValidate(
    const std::vector<std::pair<CedarKey, uint64_t>>& read_set,
    Timestamp commit_ts) {
  
  std::vector<std::pair<CedarKey, ValidationResult>> results;
  results.reserve(read_set.size());
  
  // 并行验证（简单实现，实际可用线程池）
  for (const auto& [key, version] : read_set) {
    auto result = FastValidate(key, version, commit_ts);
    if (result == ValidationResult::kNeedFullCheck) {
      result = FullValidate(key, version, commit_ts);
    }
    results.emplace_back(key, result);
  }
  
  return results;
}

ValidationResult VersionChainIndex::ValidateWithHead(VersionChainHead* head,
                                                      uint64_t read_version,
                                                      Timestamp commit_ts) {
  VersionChainNode* latest = head->latest.load(std::memory_order_acquire);
  
  // 情况1: 无版本
  if (!latest) {
    return ValidationResult::kValid;
  }
  
  // 情况2: 最新版本就是读取的版本
  if (latest->version == read_version && latest->visible.load(std::memory_order_acquire)) {
    return ValidationResult::kValid;
  }
  
  // 情况3: 最新版本在事务开始后提交，不影响本事务
  if (latest->commit_ts > commit_ts) {
    // 需要进一步检查是否只是追加新版本
    // 如果 read_version 仍然可见，则验证通过
    return ValidationResult::kNeedFullCheck;
  }
  
  // 情况4: 最新版本提交时间在本事务之前，但版本号不同
  // 说明有并发事务提交了新版本
  if (latest->visible.load(std::memory_order_acquire) &&
      latest->version != read_version) {
    return ValidationResult::kInvalid;
  }
  
  return ValidationResult::kNeedFullCheck;
}

size_t VersionChainIndex::GetKeyCount() const {
  std::shared_lock<std::shared_mutex> lock(index_mutex_);
  return index_.size();
}

size_t VersionChainIndex::GetTotalVersionCount() const {
  std::shared_lock<std::shared_mutex> lock(index_mutex_);
  size_t total = 0;
  for (const auto& [_, head] : index_) {
    total += head->chain_length.load(std::memory_order_relaxed);
  }
  return total;
}

void VersionChainIndex::RunGC(Timestamp global_safe_ts) {
  std::unique_lock<std::shared_mutex> lock(index_mutex_);
  
  for (auto& [key, head] : index_) {
    // 如果有活跃读取者，跳过（保守策略）
    if (head->reader_count.load(std::memory_order_relaxed) > 0) {
      continue;
    }
    
    // 获取链表结构锁，阻止新的读者进入链表遍历
    std::unique_lock<std::shared_mutex> chain_lock(head->chain_mutex_);
    
    // 双重检查：获取锁后再次确认没有读者
    if (head->reader_count.load(std::memory_order_relaxed) > 0) {
      continue;
    }
    
    VersionChainNode* current = head->latest.load(std::memory_order_acquire);
    VersionChainNode* prev = nullptr;
    
    while (current) {
      VersionChainNode* next = current->next.load(std::memory_order_acquire);
      
      // 判断是否可以删除
      bool can_delete = false;
      
      // 条件1: 版本已可见
      if (current->visible.load(std::memory_order_acquire)) {
        // 条件2: 提交时间早于安全时间戳
        if (current->commit_ts < global_safe_ts) {
          // 条件3: 不是最新版本
          if (prev != nullptr) {
            can_delete = true;
          }
        }
      }
      
      if (can_delete) {
        // 从链中移除
        if (prev) {
          prev->next.store(next, std::memory_order_release);
        }
        
        // 更新统计
        gc_versions_removed_.fetch_add(1, std::memory_order_relaxed);
        gc_bytes_freed_.fetch_add(sizeof(VersionChainNode) + current->data_size,
                                   std::memory_order_relaxed);
        
        // 减少链长度
        head->chain_length.fetch_sub(1, std::memory_order_relaxed);
        
        // 删除节点（现在安全，因为 chain_mutex_ 阻止了读者遍历）
        delete current;
        
        if (prev) {
          current = next;
        } else {
          // 删除了头节点，更新latest
          head->latest.store(next, std::memory_order_release);
          current = next;
        }
      } else {
        prev = current;
        current = next;
      }
    }
  }
}

VersionChainIndex::GCStats VersionChainIndex::GetGCStats() const {
  GCStats stats;
  stats.versions_removed = gc_versions_removed_.load();
  stats.bytes_freed = gc_bytes_freed_.load();
  return stats;
}

// =============================================================================
// CrossShardVersionView 实现
// =============================================================================

void CrossShardVersionView::AddShardView(PartitionID pid,
                                          const std::vector<VersionInfo>& versions) {
  std::lock_guard<std::mutex> lock(mutex_);
  shard_views_[pid] = versions;
}

bool CrossShardVersionView::GlobalValidate(
    const std::vector<std::pair<CedarKey, uint64_t>>& read_set,
    Timestamp commit_ts) {
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (const auto& [key, read_version] : read_set) {
    bool found = false;
    
    // 在所有分片视图中查找
    for (const auto& [_, versions] : shard_views_) {
      for (const auto& info : versions) {
        if (info.key == key && info.version == read_version && info.visible) {
          found = true;
          break;
        }
      }
      if (found) break;
    }
    
    if (!found) {
      return false;  // 有读取的版本不存在或不可见
    }
  }
  
  return true;
}

Timestamp CrossShardVersionView::ComputeGlobalSafeTimestamp() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  Timestamp min_ts = Timestamp::Max();
  
  for (const auto& [_, versions] : shard_views_) {
    for (const auto& info : versions) {
      if (info.visible && info.commit_ts < min_ts) {
        min_ts = info.commit_ts;
      }
    }
  }
  
  return min_ts;
}

void CrossShardVersionView::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  shard_views_.clear();
}

// =============================================================================
// DistributedValidationCoordinator 实现
// =============================================================================

DistributedValidationCoordinator::DistributedValidationCoordinator(DTxRpcClient* rpc_client)
    : rpc_client_(rpc_client) {}

ValidationResult DistributedValidationCoordinator::CoordinateValidation(
    const DistributedTxnContext& ctx) {
  
  // Cross-shard validation requires:
  // 1. Collect all participating partitions from ctx.GetParticipants()
  // 2. Map PartitionIDs to NodeIDs via PartitionManager
  // 3. Send validation requests to each node via DTxRpcClient
  // 4. Aggregate results (all must agree for kValid)
  //
  // For now, assume valid until full coordination is wired.
  (void)ctx;
  return ValidationResult::kValid;
}

ValidationResult DistributedValidationCoordinator::HandleValidationRequest(
    VersionChainIndex* index,
    const std::vector<std::pair<CedarKey, uint64_t>>& read_set,
    Timestamp commit_ts) {
  
  if (!index) {
    return ValidationResult::kValid;
  }
  
  // 批量验证
  auto results = index->BatchValidate(read_set, commit_ts);
  
  // 检查是否全部有效
  for (const auto& [_, result] : results) {
    if (result != ValidationResult::kValid) {
      return result;
    }
  }
  
  return ValidationResult::kValid;
}

}  // namespace dtx
}  // namespace cedar
