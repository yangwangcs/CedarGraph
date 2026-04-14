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
// TW-CD Engine - 时序窗口冲突检测引擎
// =============================================================================

#ifndef CEDAR_DTX_TWCD_ENGINE_H_
#define CEDAR_DTX_TWCD_ENGINE_H_

#include <map>
#include <set>
#include <unordered_map>
#include <shared_mutex>
#include <atomic>
#include <memory>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/temporal_window.h"

namespace cedar {
namespace dtx {

/**
 * @brief 活跃事务记录
 * 
 * 存储在窗口索引中，用于冲突检测
 */
struct ActiveTxnRecord {
  TxnID txn_id;
  TemporalWindow window;
  std::unordered_set<CedarKey, CedarKeyHash> write_set;
  Timestamp register_time;
  std::atomic<bool> active{true};
};

/**
 * @brief 窗口区间树节点
 * 
 * 用于高效的范围查询
 */
struct IntervalTreeNode {
  Timestamp start;
  Timestamp end;
  std::set<TxnID> txns;  // 覆盖此区间的所有事务
  std::unique_ptr<IntervalTreeNode> left;
  std::unique_ptr<IntervalTreeNode> right;
  Timestamp max_end;  // 子树中的最大end值
  
  IntervalTreeNode(Timestamp s, Timestamp e, TxnID txn)
      : start(s), end(e), max_end(e) {
    txns.insert(txn);
  }
};

/**
 * @brief 区间树索引
 * 
 * 支持高效的范围查询：找出所有与给定窗口重叠的事务
 */
class TemporalWindowIntervalTree {
 public:
  TemporalWindowIntervalTree();
  ~TemporalWindowIntervalTree() = default;
  
  // 插入窗口
  void Insert(const TemporalWindow& window, TxnID txn_id);
  
  // 删除窗口
  void Remove(const TemporalWindow& window, TxnID txn_id);
  
  // 查询与窗口重叠的所有事务
  std::set<TxnID> QueryOverlapping(const TemporalWindow& window) const;
  
  // 获取树中事务数量
  size_t Size() const { return size_; }
  
  // 清空
  void Clear();
  
 private:
  std::unique_ptr<IntervalTreeNode> root_;
  size_t size_{0};
  mutable std::shared_mutex mutex_;
  
  // 递归插入
  IntervalTreeNode* InsertRecursive(
      IntervalTreeNode* node,
      Timestamp start,
      Timestamp end,
      TxnID txn_id);
  
  // 递归删除
  IntervalTreeNode* RemoveRecursive(
      IntervalTreeNode* node,
      Timestamp start,
      Timestamp end,
      TxnID txn_id);
  
  // 递归查询
  void QueryRecursive(
      IntervalTreeNode* node,
      const TemporalWindow& window,
      std::set<TxnID>& result) const;
  
  // 更新max_end
  void UpdateMaxEnd(IntervalTreeNode* node);
  
  // 查找最小节点
  IntervalTreeNode* FindMin(IntervalTreeNode* node);
};

/**
 * @brief TW-CD 冲突检测结果
 */
struct ConflictCheckResult {
  bool has_conflict{false};
  enum class Type : uint8_t {
    kNoConflict = 0,
    kReadWrite = 1,
    kWriteWrite = 2,
    kTemporalOverlap = 3,
  } type{Type::kNoConflict};
  
  std::vector<CedarKey> conflict_keys;
  std::set<TxnID> conflict_txns;
  
  bool Ok() const { return !has_conflict; }
  
  static ConflictCheckResult NoConflict() {
    return ConflictCheckResult{false, Type::kNoConflict, {}, {}};
  }
  
  static ConflictCheckResult ReadWriteConflict(
      const std::vector<CedarKey>& keys,
      const std::set<TxnID>& txns) {
    return ConflictCheckResult{true, Type::kReadWrite, keys, txns};
  }
  
  static ConflictCheckResult WriteWriteConflict(
      const std::vector<CedarKey>& keys,
      const std::set<TxnID>& txns) {
    return ConflictCheckResult{true, Type::kWriteWrite, keys, txns};
  }
};

/**
 * @brief TW-CD 冲突检测引擎
 * 
 * 核心组件，管理所有活跃事务的时序窗口，提供高效的冲突检测
 */
class TwcdEngine {
 public:
  explicit TwcdEngine(const DTxConfig& config);
  ~TwcdEngine();
  
  // 禁止拷贝
  TwcdEngine(const TwcdEngine&) = delete;
  TwcdEngine& operator=(const TwcdEngine&) = delete;
  
  // ==================== 事务窗口管理 ====================
  
  // 注册事务的时序窗口（事务开始时调用）
  Status RegisterWindow(TxnID txn_id, const TemporalWindow& window);
  
  // 注销事务窗口（事务结束时调用）
  void UnregisterWindow(TxnID txn_id);
  
  // 更新事务窗口（窗口扩展时调用）
  Status UpdateWindow(TxnID txn_id, const TemporalWindow& new_window);
  
  // ==================== 冲突检测 ====================
  
  /**
   * @brief 检测冲突（核心方法）
   * 
   * @param txn_id 当前事务ID
   * @param window 当前事务的时序窗口
   * @param read_set 读集
   * @param write_set 写集
   * @return ConflictCheckResult 冲突检测结果
   */
  ConflictCheckResult CheckConflict(
      TxnID txn_id,
      const TemporalWindow& window,
      const std::vector<CedarKey>& read_set,
      const std::vector<CedarKey>& write_set);
  
  /**
   * @brief 快速冲突检查（仅检查窗口重叠）
   * 
   * 用于快速过滤，不检查Key级别冲突
   */
  bool HasOverlappingTransactions(TxnID txn_id, const TemporalWindow& window);
  
  /**
   * @brief 获取与指定窗口重叠的所有事务
   */
  std::set<TxnID> GetOverlappingTxns(const TemporalWindow& window);
  
  // ==================== Key级冲突检测 ====================
  
  // 注册事务的写集（用于Key级别冲突检测）
  Status RegisterWriteSet(TxnID txn_id, const std::vector<CedarKey>& write_set);
  
  // 注销事务的写集
  void UnregisterWriteSet(TxnID txn_id);
  
  // 检查读-写冲突
  std::vector<CedarKey> DetectReadWriteConflicts(
      TxnID txn_id,
      const std::vector<CedarKey>& read_set,
      const std::set<TxnID>& overlapping_txns);
  
  // 检查写-写冲突
  std::vector<CedarKey> DetectWriteWriteConflicts(
      TxnID txn_id,
      const std::vector<CedarKey>& write_set,
      const std::set<TxnID>& overlapping_txns);
  
  // ==================== 统计和监控 ====================
  
  // 获取当前活跃事务数量
  size_t GetActiveTxnCount() const;
  
  // 获取当前活跃窗口数量
  size_t GetActiveWindowCount() const;
  
  // 获取统计信息
  struct Stats {
    size_t active_txns{0};
    size_t active_windows{0};
    uint64_t total_checks{0};
    uint64_t conflict_detected{0};
    double avg_check_latency_us{0.0};
  };
  Stats GetStats() const;
  
  // 重置统计
  void ResetStats();
  
 private:
  DTxConfig config_;
  
  // 活跃事务表
  struct ActiveTxnEntry {
    TemporalWindow window;
    std::unordered_set<CedarKey, CedarKeyHash> write_set;
    Timestamp register_time;
    
    // 默认构造函数
    ActiveTxnEntry() = default;
    
    // 显式移动构造函数（因为 unordered_set 可能有问题）
    ActiveTxnEntry(ActiveTxnEntry&&) = default;
    ActiveTxnEntry& operator=(ActiveTxnEntry&&) = default;
    
    // 禁止拷贝
    ActiveTxnEntry(const ActiveTxnEntry&) = delete;
    ActiveTxnEntry& operator=(const ActiveTxnEntry&) = delete;
  };
  
  mutable std::shared_mutex active_txns_mutex_;
  std::unordered_map<TxnID, std::unique_ptr<ActiveTxnEntry>> active_txns_;
  
  // 区间树索引（用于高效的范围查询）
  TemporalWindowIntervalTree interval_tree_;
  
  // Key -> 事务ID映射（用于Key级别冲突检测）
  std::shared_mutex key_index_mutex_;
  std::unordered_map<CedarKey, std::set<TxnID>, CedarKeyHash> key_to_txns_;
  
  // 统计
  mutable std::mutex stats_mutex_;
  std::atomic<uint64_t> total_checks_{0};
  std::atomic<uint64_t> conflict_detected_{0};
  std::atomic<uint64_t> total_check_latency_us_{0};
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_TWCD_ENGINE_H_
