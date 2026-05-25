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
// Distributed Deadlock Detector - 分布式死锁检测
// =============================================================================
// 使用等待图（Wait-For Graph）检测事务间的循环等待
// 支持分布式场景下的全局死锁检测
// =============================================================================

#ifndef CEDAR_DTX_DEADLOCK_DETECTOR_H_
#define CEDAR_DTX_DEADLOCK_DETECTOR_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// =============================================================================
// 等待关系
// =============================================================================

struct WaitForEdge {
  TxnID waiter;           // 等待者
  TxnID holder;           // 被等待者
  PartitionID partition;  // 发生等待的分区
  std::chrono::steady_clock::time_point created_at;
  std::string resource_id;  // 资源标识（可选）
  
  bool IsExpired(uint64_t timeout_ms = 30000) const {
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - created_at).count();
    return elapsed > static_cast<int64_t>(timeout_ms);
  }
};

// =============================================================================
// 死锁检测结果
// =============================================================================

struct DeadlockDetectionResult {
  bool has_deadlock{false};
  std::vector<TxnID> cycle;  // 死锁环中的事务ID
  TxnID victim{0};            // 建议的牺牲者（ youngest 事务）
  std::string ToString() const;
};

// =============================================================================
// 等待图
// =============================================================================

class WaitForGraph {
 public:
  WaitForGraph() = default;
  ~WaitForGraph() = default;
  
  // 禁止拷贝
  WaitForGraph(const WaitForGraph&) = delete;
  WaitForGraph& operator=(const WaitForGraph&) = delete;
  
  // 添加等待边
  void AddEdge(const WaitForEdge& edge);
  
  // 移除等待边
  void RemoveEdge(TxnID waiter, TxnID holder);
  
  // 移除事务的所有边
  void RemoveTxn(TxnID txn_id);
  
  // 检测死锁
  DeadlockDetectionResult DetectDeadlock(size_t max_cycle_size = 0);
  
  // 检测特定事务是否参与死锁
  DeadlockDetectionResult DetectDeadlockForTxn(TxnID txn_id, size_t max_cycle_size = 0);
  
  // 获取事务的等待信息
  std::vector<WaitForEdge> GetWaitsFor(TxnID txn_id) const;
  std::vector<WaitForEdge> GetWaitsBy(TxnID txn_id) const;
  
  // 获取图的大小
  size_t GetEdgeCount() const;
  size_t GetNodeCount() const;
  
  // 清理过期边
  size_t CleanupExpiredEdges(uint64_t timeout_ms);
  
  // 获取图的字符串表示（用于调试）
  std::string ToString() const;

 private:
  // 使用 DFS 检测环
  bool FindCycle(TxnID start, 
                 std::unordered_set<TxnID>& visited,
                 std::unordered_set<TxnID>& in_stack,
                 std::vector<TxnID>& path,
                 std::vector<TxnID>& cycle,
                 size_t depth,
                 size_t max_depth);
  
  // 选择牺牲者（youngest transaction）
  TxnID SelectVictim(const std::vector<TxnID>& cycle) const;
  
  mutable std::shared_mutex mutex_;
  
  // waiter -> 它等待的 holders
  std::unordered_map<TxnID, std::vector<WaitForEdge>> outgoing_edges_;
  
  // holder -> 等待它的 waiters
  std::unordered_map<TxnID, std::vector<WaitForEdge>> incoming_edges_;
  
  // 所有节点
  std::unordered_set<TxnID> nodes_;
};

// =============================================================================
// 分布式死锁检测器
// =============================================================================

class DistributedDeadlockDetector {
 public:
  struct Config {
    uint64_t detection_interval_ms{1000};  // 检测间隔
    uint64_t edge_timeout_ms{30000};       // 边超时时间
    uint64_t cleanup_interval_ms{5000};    // 清理间隔
    bool auto_abort_victim{false};         // 自动中止牺牲者
    size_t max_cycle_size{10};             // 最大检测环大小
  };
  
  using VictimHandler = std::function<void(TxnID)>;
  
  DistributedDeadlockDetector();
  ~DistributedDeadlockDetector();
  
  // 禁止拷贝
  DistributedDeadlockDetector(const DistributedDeadlockDetector&) = delete;
  DistributedDeadlockDetector& operator=(const DistributedDeadlockDetector&) = delete;
  
  // 初始化
  Status Initialize(const Config& config);
  
  // 关闭
  void Shutdown() noexcept;
  
  // ===== 等待图操作 =====
  
  // 注册等待关系（txn_id 等待 holder 持有的资源）
  void RegisterWait(TxnID txn_id, TxnID holder, PartitionID partition,
                    const std::string& resource_id = "");
  
  // 注销等待关系（事务不再等待）
  void UnregisterWait(TxnID txn_id, TxnID holder);
  
  // 事务完成（提交或回滚），移除所有相关边
  void UnregisterTxn(TxnID txn_id);
  
  // ===== 死锁检测 =====
  
  // 立即执行一次死锁检测
  DeadlockDetectionResult DetectNow();
  
  // 检查特定事务是否参与死锁
  DeadlockDetectionResult CheckTxn(TxnID txn_id);
  
  // 设置牺牲者处理回调
  void SetVictimHandler(VictimHandler handler);
  
  // ===== 统计 =====
  
  struct Stats {
    uint64_t total_deadlocks{0};
    uint64_t victims_aborted{0};
    uint64_t edges_added{0};
    uint64_t edges_removed{0};
    uint64_t detections_run{0};
    uint64_t expired_edges_cleaned{0};
  };
  
  Stats GetStats() const;
  void ResetStats();
  
  // 获取等待图状态
  std::string GetGraphInfo() const;

 private:
  // 后台检测循环
  void DetectionLoop();
  
  // 后台清理循环
  void CleanupLoop();
  
  // 处理检测到的死锁
  void HandleDeadlock(const DeadlockDetectionResult& result);
  
  Config config_;
  std::unique_ptr<WaitForGraph> graph_;
  VictimHandler victim_handler_;
  
  std::atomic<bool> running_{false};
  std::thread detection_thread_;
  std::thread cleanup_thread_;
  
  // 用于精确睡眠和快速关闭的条件变量
  std::mutex cv_mutex_;
  std::condition_variable cv_;
  
  // 统计
  mutable std::mutex stats_mutex_;
  Stats stats_;
  
  // 防止并发处理同一死锁
  mutable std::mutex handle_mutex_;
};

// =============================================================================
// 便捷函数
// =============================================================================

// 获取全局死锁检测器实例
DistributedDeadlockDetector* GetGlobalDeadlockDetector();

// 注册等待关系（便捷函数）
inline void RegisterTxnWait(TxnID txn_id, TxnID holder, PartitionID partition,
                            const std::string& resource_id = "") {
  if (auto* detector = GetGlobalDeadlockDetector()) {
    detector->RegisterWait(txn_id, holder, partition, resource_id);
  }
}

// 注销等待关系（便捷函数）
inline void UnregisterTxnWait(TxnID txn_id, TxnID holder) {
  if (auto* detector = GetGlobalDeadlockDetector()) {
    detector->UnregisterWait(txn_id, holder);
  }
}

// 事务完成（便捷函数）
inline void UnregisterTxnFromDeadlockDetector(TxnID txn_id) {
  if (auto* detector = GetGlobalDeadlockDetector()) {
    detector->UnregisterTxn(txn_id);
  }
}

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_DEADLOCK_DETECTOR_H_
