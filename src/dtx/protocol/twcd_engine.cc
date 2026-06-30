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

#include "cedar/dtx/twcd_engine.h"

#include <algorithm>
#include <chrono>
#include <mutex>

namespace cedar {
namespace dtx {

// =============================================================================
// TemporalWindowIntervalTree 实现
// =============================================================================

TemporalWindowIntervalTree::TemporalWindowIntervalTree() = default;

void TemporalWindowIntervalTree::Insert(const TemporalWindow& window, TxnID txn_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  root_.reset(InsertRecursive(root_.release(), window.start, window.end, txn_id));
  ++size_;
}

void TemporalWindowIntervalTree::Remove(const TemporalWindow& window, TxnID txn_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  root_.reset(RemoveRecursive(root_.release(), window.start, window.end, txn_id));
  if (size_ > 0) --size_;
}

std::unordered_set<TxnID> TemporalWindowIntervalTree::QueryOverlapping(
    const TemporalWindow& window) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::unordered_set<TxnID> result;
  QueryRecursive(root_.get(), window, result);
  return result;
}

void TemporalWindowIntervalTree::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  root_.reset();
  size_ = 0;
}

IntervalTreeNode* TemporalWindowIntervalTree::InsertRecursive(
    IntervalTreeNode* node,
    Timestamp start,
    Timestamp end,
    TxnID txn_id) {
  
  if (!node) {
    return new IntervalTreeNode(start, end, txn_id);
  }
  
  if (start < node->start) {
    node->left.reset(InsertRecursive(node->left.release(), start, end, txn_id));
  } else {
    node->right.reset(InsertRecursive(node->right.release(), start, end, txn_id));
  }
  
  // 更新max_end
  UpdateMaxEnd(node);
  
  return node;
}

IntervalTreeNode* TemporalWindowIntervalTree::RemoveRecursive(
    IntervalTreeNode* node,
    Timestamp start,
    Timestamp end,
    TxnID txn_id) {
  
  if (!node) return nullptr;
  
  if (start < node->start) {
    node->left.reset(RemoveRecursive(node->left.release(), start, end, txn_id));
  } else if (start > node->start) {
    node->right.reset(RemoveRecursive(node->right.release(), start, end, txn_id));
  } else {
    // 找到匹配的start
    node->txns.erase(txn_id);
    
    if (node->txns.empty()) {
      // 需要删除此节点
      if (!node->left && !node->right) {
        delete node;
        return nullptr;
      }
      
      if (!node->left) {
        auto* right = node->right.release();
        delete node;
        return right;
      }
      
      if (!node->right) {
        auto* left = node->left.release();
        delete node;
        return left;
      }
      
      // 有两个子节点，找到后继节点
      auto* min_node = FindMin(node->right.get());
      node->start = min_node->start;
      node->end = min_node->end;
      node->txns = min_node->txns;
      node->right.reset(RemoveRecursive(node->right.release(), min_node->start, min_node->end, *min_node->txns.begin()));
    }
  }
  
  UpdateMaxEnd(node);
  return node;
}

void TemporalWindowIntervalTree::QueryRecursive(
    IntervalTreeNode* node,
    const TemporalWindow& window,
    std::unordered_set<TxnID>& result) const {
  
  if (!node) return;
  
  // 检查当前节点是否与查询窗口重叠
  if (TemporalWindow(node->start, node->end).Overlaps(window)) {
    result.insert(node->txns.begin(), node->txns.end());
  }
  
  // 如果左子树可能有重叠，递归查询
  if (node->left && node->left->max_end >= window.start) {
    QueryRecursive(node->left.get(), window, result);
  }
  
  // 如果当前节点的start小于查询窗口的end，检查右子树
  if (node->start <= window.end || window.end.value() == 0) {
    QueryRecursive(node->right.get(), window, result);
  }
}

void TemporalWindowIntervalTree::UpdateMaxEnd(IntervalTreeNode* node) {
  if (!node) return;
  
  node->max_end = node->end;
  if (node->left && node->left->max_end > node->max_end) {
    node->max_end = node->left->max_end;
  }
  if (node->right && node->right->max_end > node->max_end) {
    node->max_end = node->right->max_end;
  }
}

IntervalTreeNode* TemporalWindowIntervalTree::FindMin(IntervalTreeNode* node) {
  while (node && node->left) {
    node = node->left.get();
  }
  return node;
}

// =============================================================================
// TwcdEngine 实现
// =============================================================================

TwcdEngine::TwcdEngine(const DTxConfig& config) 
    : config_(config) {}

TwcdEngine::~TwcdEngine() = default;

Status TwcdEngine::RegisterWindow(TxnID txn_id, const TemporalWindow& window) {
  if (txn_id == kInvalidTxnID) {
    return Status::InvalidArgument("TwcdEngine", "Invalid txn_id");
  }
  
  std::unique_lock<std::shared_mutex> lock(active_txns_mutex_);
  
  // 检查是否已存在
  if (active_txns_.count(txn_id)) {
    return Status::InvalidArgument("TwcdEngine", "Txn already registered");
  }
  
  // 添加到活跃事务表
  auto entry = std::make_unique<ActiveTxnEntry>();
  entry->window = window;
  entry->register_time = Timestamp::Now();
  active_txns_[txn_id] = std::move(entry);
  
  lock.unlock();
  
  // 添加到区间树
  interval_tree_.Insert(window, txn_id);
  
  return Status::OK();
}

void TwcdEngine::UnregisterWindow(TxnID txn_id) {
  UnregisterWriteSet(txn_id);

  std::unique_lock<std::shared_mutex> lock(active_txns_mutex_);
  
  auto it = active_txns_.find(txn_id);
  if (it == active_txns_.end()) {
    return;
  }
  
  auto window = it->second->window;
  active_txns_.erase(it);
  
  lock.unlock();
  
  // 从区间树移除
  interval_tree_.Remove(window, txn_id);
}

Status TwcdEngine::UpdateWindow(TxnID txn_id, const TemporalWindow& new_window) {
  std::unique_lock<std::shared_mutex> lock(active_txns_mutex_);
  
  auto it = active_txns_.find(txn_id);
  if (it == active_txns_.end()) {
    return Status::NotFound("TwcdEngine", "Txn not found");
  }
  
  auto old_window = it->second->window;
  it->second->window = new_window;
  
  lock.unlock();
  
  // 更新区间树
  interval_tree_.Remove(old_window, txn_id);
  interval_tree_.Insert(new_window, txn_id);
  
  return Status::OK();
}

ConflictCheckResult TwcdEngine::CheckConflict(
    TxnID txn_id,
    const TemporalWindow& window,
    const std::vector<CedarKey>& read_set,
    const std::vector<CedarKey>& write_set) {
  
  auto start_time = std::chrono::steady_clock::now();
  
  ++total_checks_;
  
  // 步骤1: 查询时间重叠的事务
  auto overlapping_txns = interval_tree_.QueryOverlapping(window);
  
  // 排除自己
  overlapping_txns.erase(txn_id);
  
  if (overlapping_txns.empty()) {
    // 无时间重叠，肯定无冲突
    auto end_time = std::chrono::steady_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    total_check_latency_us_ += latency.count();
    
    return ConflictCheckResult::NoConflict();
  }
  
  // 步骤2: 检查读-写冲突
  auto rw_conflicts = DetectReadWriteConflicts(txn_id, read_set, overlapping_txns);
  if (!rw_conflicts.empty()) {
    ++conflict_detected_;
    
    auto end_time = std::chrono::steady_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    total_check_latency_us_ += latency.count();
    
    return ConflictCheckResult::ReadWriteConflict(rw_conflicts, overlapping_txns);
  }
  
  // 步骤3: 检查写-写冲突
  auto ww_conflicts = DetectWriteWriteConflicts(txn_id, write_set, overlapping_txns);
  if (!ww_conflicts.empty()) {
    ++conflict_detected_;
    
    auto end_time = std::chrono::steady_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    total_check_latency_us_ += latency.count();
    
    return ConflictCheckResult::WriteWriteConflict(ww_conflicts, overlapping_txns);
  }
  
  // 无冲突
  auto end_time = std::chrono::steady_clock::now();
  auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
  total_check_latency_us_ += latency.count();
  
  return ConflictCheckResult::NoConflict();
}

bool TwcdEngine::HasOverlappingTransactions(TxnID txn_id, const TemporalWindow& window) {
  auto overlapping = interval_tree_.QueryOverlapping(window);
  overlapping.erase(txn_id);
  return !overlapping.empty();
}

std::unordered_set<TxnID> TwcdEngine::GetOverlappingTxns(const TemporalWindow& window) {
  return interval_tree_.QueryOverlapping(window);
}

Status TwcdEngine::RegisterWriteSet(TxnID txn_id, const std::vector<CedarKey>& write_set) {
  std::unique_lock<std::shared_mutex> lock(active_txns_mutex_);
  
  auto it = active_txns_.find(txn_id);
  if (it == active_txns_.end()) {
    return Status::NotFound("TwcdEngine", "Txn not found");
  }
  
  for (const auto& key : write_set) {
    it->second->write_set.insert(key);
  }
  
  lock.unlock();
  
  // 更新Key索引
  std::unique_lock<std::shared_mutex> key_lock(key_index_mutex_);
  for (const auto& key : write_set) {
    key_to_txns_[key].insert(txn_id);
  }
  
  return Status::OK();
}

void TwcdEngine::UnregisterWriteSet(TxnID txn_id) {
  std::unique_lock<std::shared_mutex> lock(active_txns_mutex_);
  
  auto it = active_txns_.find(txn_id);
  if (it == active_txns_.end()) {
    return;
  }
  
  auto write_set = it->second->write_set;
  
  lock.unlock();
  
  // 从Key索引移除
  std::unique_lock<std::shared_mutex> key_lock(key_index_mutex_);
  for (const auto& key : write_set) {
    auto kt_it = key_to_txns_.find(key);
    if (kt_it != key_to_txns_.end()) {
      kt_it->second.erase(txn_id);
      if (kt_it->second.empty()) {
        key_to_txns_.erase(kt_it);
      }
    }
  }
}

std::vector<CedarKey> TwcdEngine::DetectReadWriteConflicts(
    TxnID txn_id,
    const std::vector<CedarKey>& read_set,
    const std::unordered_set<TxnID>& overlapping_txns) {
  
  std::vector<CedarKey> conflicts;
  
  std::shared_lock<std::shared_mutex> lock(key_index_mutex_);
  
  for (const auto& key : read_set) {
    auto it = key_to_txns_.find(key);
    if (it == key_to_txns_.end()) continue;
    
    // 检查是否有重叠事务写了这个Key
    for (TxnID other_txn : it->second) {
      if (other_txn != txn_id && overlapping_txns.count(other_txn)) {
        conflicts.push_back(key);
        break;  // 找到一个冲突即可
      }
    }
  }
  
  return conflicts;
}

std::vector<CedarKey> TwcdEngine::DetectWriteWriteConflicts(
    TxnID txn_id,
    const std::vector<CedarKey>& write_set,
    const std::unordered_set<TxnID>& overlapping_txns) {
  
  std::vector<CedarKey> conflicts;
  
  std::shared_lock<std::shared_mutex> lock(key_index_mutex_);
  
  for (const auto& key : write_set) {
    auto it = key_to_txns_.find(key);
    if (it == key_to_txns_.end()) continue;
    
    // 检查是否有重叠事务也写了这个Key
    for (TxnID other_txn : it->second) {
      if (other_txn != txn_id && overlapping_txns.count(other_txn)) {
        conflicts.push_back(key);
        break;
      }
    }
  }
  
  return conflicts;
}

size_t TwcdEngine::GetActiveTxnCount() const {
  std::shared_lock<std::shared_mutex> lock(active_txns_mutex_);
  return active_txns_.size();
}

size_t TwcdEngine::GetActiveWindowCount() const {
  return interval_tree_.Size();
}

TwcdEngine::Stats TwcdEngine::GetStats() const {
  Stats stats;
  stats.active_txns = GetActiveTxnCount();
  stats.active_windows = GetActiveWindowCount();
  stats.total_checks = total_checks_.load();
  stats.conflict_detected = conflict_detected_.load();
  
  auto total = total_checks_.load();
  if (total > 0) {
    stats.avg_check_latency_us = static_cast<double>(total_check_latency_us_.load()) / total;
  }
  
  return stats;
}

void TwcdEngine::ResetStats() {
  total_checks_.store(0);
  conflict_detected_.store(0);
  total_check_latency_us_.store(0);
}

}  // namespace dtx
}  // namespace cedar
