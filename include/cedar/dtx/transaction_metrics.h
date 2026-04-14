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
// Transaction Metrics - 分布式事务性能监控指标
// =============================================================================

#ifndef CEDAR_DTX_TRANSACTION_METRICS_H_
#define CEDAR_DTX_TRANSACTION_METRICS_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// =============================================================================
// 指标类型定义
// =============================================================================

enum class MetricType {
  kCounter,    // 累计计数器
  kGauge,      // 瞬时值
  kHistogram,  // 直方图分布
  kTimer       // 计时器
};

// =============================================================================
// 事务统计指标
// =============================================================================

struct TransactionMetricsSnapshot {
  // ===== 吞吐量指标 =====
  uint64_t total_txns{0};
  uint64_t committed_txns{0};
  uint64_t aborted_txns{0};
  uint64_t conflict_retries{0};
  
  // ===== 延迟指标（微秒） =====
  uint64_t total_latency_us{0};
  uint64_t min_latency_us{0};
  uint64_t max_latency_us{0};
  uint64_t p50_latency_us{0};
  uint64_t p99_latency_us{0};
  
  // ===== 2PC 特定指标 =====
  uint64_t single_partition_txns{0};
  uint64_t same_range_txns{0};
  uint64_t full_2pc_txns{0};
  
  uint64_t prepare_latency_us{0};
  uint64_t commit_latency_us{0};
  uint64_t abort_latency_us{0};
  
  // ===== 超时与故障 =====
  uint64_t prepare_timeouts{0};
  uint64_t commit_timeouts{0};
  uint64_t prepare_failures{0};
  uint64_t commit_failures{0};
  
  // ===== 冲突统计 =====
  uint64_t waw_conflicts{0};
  uint64_t raw_conflicts{0};
  uint64_t war_conflicts{0};
  uint64_t temporal_conflicts{0};
  
  // ===== 网络指标 =====
  uint64_t network_retries{0};
  uint64_t network_failures{0};
  uint64_t participant_failures{0};
};

struct TransactionMetrics {
  // ===== 吞吐量指标 =====
  std::atomic<uint64_t> total_txns{0};
  std::atomic<uint64_t> committed_txns{0};
  std::atomic<uint64_t> aborted_txns{0};
  std::atomic<uint64_t> conflict_retries{0};
  
  // ===== 延迟指标（微秒） =====
  std::atomic<uint64_t> total_latency_us{0};
  std::atomic<uint64_t> min_latency_us{0};
  std::atomic<uint64_t> max_latency_us{0};
  std::atomic<uint64_t> p50_latency_us{0};
  std::atomic<uint64_t> p99_latency_us{0};
  
  // ===== 2PC 特定指标 =====
  std::atomic<uint64_t> single_partition_txns{0};
  std::atomic<uint64_t> same_range_txns{0};
  std::atomic<uint64_t> full_2pc_txns{0};
  
  std::atomic<uint64_t> prepare_latency_us{0};
  std::atomic<uint64_t> commit_latency_us{0};
  std::atomic<uint64_t> abort_latency_us{0};
  
  // ===== 超时与故障 =====
  std::atomic<uint64_t> prepare_timeouts{0};
  std::atomic<uint64_t> commit_timeouts{0};
  std::atomic<uint64_t> prepare_failures{0};
  std::atomic<uint64_t> commit_failures{0};
  
  // ===== 冲突统计 =====
  std::atomic<uint64_t> waw_conflicts{0};
  std::atomic<uint64_t> raw_conflicts{0};
  std::atomic<uint64_t> war_conflicts{0};
  std::atomic<uint64_t> temporal_conflicts{0};
  
  // ===== 网络指标 =====
  std::atomic<uint64_t> network_retries{0};
  std::atomic<uint64_t> network_failures{0};
  std::atomic<uint64_t> participant_failures{0};
  
  // 重置所有指标
  void Reset() {
    total_txns.store(0);
    committed_txns.store(0);
    aborted_txns.store(0);
    conflict_retries.store(0);
    total_latency_us.store(0);
    min_latency_us.store(0);
    max_latency_us.store(0);
    p50_latency_us.store(0);
    p99_latency_us.store(0);
    single_partition_txns.store(0);
    same_range_txns.store(0);
    full_2pc_txns.store(0);
    prepare_latency_us.store(0);
    commit_latency_us.store(0);
    abort_latency_us.store(0);
    prepare_timeouts.store(0);
    commit_timeouts.store(0);
    prepare_failures.store(0);
    commit_failures.store(0);
    waw_conflicts.store(0);
    raw_conflicts.store(0);
    war_conflicts.store(0);
    temporal_conflicts.store(0);
    network_retries.store(0);
    network_failures.store(0);
    participant_failures.store(0);
  }
  
  // 转换为快照
  TransactionMetricsSnapshot ToSnapshot() const {
    TransactionMetricsSnapshot s;
    s.total_txns = total_txns.load();
    s.committed_txns = committed_txns.load();
    s.aborted_txns = aborted_txns.load();
    s.conflict_retries = conflict_retries.load();
    s.total_latency_us = total_latency_us.load();
    s.min_latency_us = min_latency_us.load();
    s.max_latency_us = max_latency_us.load();
    s.p50_latency_us = p50_latency_us.load();
    s.p99_latency_us = p99_latency_us.load();
    s.single_partition_txns = single_partition_txns.load();
    s.same_range_txns = same_range_txns.load();
    s.full_2pc_txns = full_2pc_txns.load();
    s.prepare_latency_us = prepare_latency_us.load();
    s.commit_latency_us = commit_latency_us.load();
    s.abort_latency_us = abort_latency_us.load();
    s.prepare_timeouts = prepare_timeouts.load();
    s.commit_timeouts = commit_timeouts.load();
    s.prepare_failures = prepare_failures.load();
    s.commit_failures = commit_failures.load();
    s.waw_conflicts = waw_conflicts.load();
    s.raw_conflicts = raw_conflicts.load();
    s.war_conflicts = war_conflicts.load();
    s.temporal_conflicts = temporal_conflicts.load();
    s.network_retries = network_retries.load();
    s.network_failures = network_failures.load();
    s.participant_failures = participant_failures.load();
    return s;
  }
};

// =============================================================================
// 延迟直方图
// =============================================================================

class LatencyHistogram {
 public:
  // 延迟桶边界（微秒）
  static constexpr std::array<uint64_t, 12> kBuckets = {
      100,      // < 100us
      500,      // < 500us
      1000,     // < 1ms
      5000,     // < 5ms
      10000,    // < 10ms
      25000,    // < 25ms
      50000,    // < 50ms
      100000,   // < 100ms
      250000,   // < 250ms
      500000,   // < 500ms
      1000000,  // < 1s
      UINT64_MAX // >= 1s
  };
  
  LatencyHistogram();
  
  // 记录一个延迟样本
  void Record(uint64_t latency_us);
  
  // 获取指定百分位的延迟
  uint64_t GetPercentile(double percentile) const;
  
  // 获取统计信息
  uint64_t GetCount() const { return count_.load(); }
  uint64_t GetSum() const { return sum_.load(); }
  double GetAverage() const;
  
  // 重置
  void Reset();
  
  // 获取桶分布
  std::vector<std::pair<uint64_t, uint64_t>> GetDistribution() const;

 private:
  std::array<std::atomic<uint64_t>, kBuckets.size()> buckets_;
  std::atomic<uint64_t> count_{0};
  std::atomic<uint64_t> sum_{0};
  std::atomic<uint64_t> min_latency_{UINT64_MAX};
  std::atomic<uint64_t> max_latency_{0};
};

// =============================================================================
// 事务指标收集器
// =============================================================================

class TransactionMetricsCollector {
 public:
  TransactionMetricsCollector();
  ~TransactionMetricsCollector();
  
  // 禁止拷贝
  TransactionMetricsCollector(const TransactionMetricsCollector&) = delete;
  TransactionMetricsCollector& operator=(const TransactionMetricsCollector&) = delete;
  
  // ===== 事务生命周期指标 =====
  
  // 记录事务开始
  void RecordTransactionStart(TxnID txn_id, TxnType type);
  
  // 记录事务提交成功
  void RecordTransactionCommit(TxnID txn_id, uint64_t latency_us);
  
  // 记录事务回滚
  void RecordTransactionAbort(TxnID txn_id, uint64_t latency_us, 
                               const std::string& reason);
  
  // 记录冲突重试
  void RecordConflictRetry(TxnID txn_id, const std::string& conflict_type);
  
  // ===== 2PC 阶段指标 =====
  
  // 记录 Prepare 阶段
  void RecordPreparePhase(TxnID txn_id, uint64_t latency_us, bool success);
  void RecordPrepareTimeout(TxnID txn_id);
  
  // 记录 Commit 阶段
  void RecordCommitPhase(TxnID txn_id, uint64_t latency_us, bool success);
  void RecordCommitTimeout(TxnID txn_id);
  
  // 记录 Abort 阶段
  void RecordAbortPhase(TxnID txn_id, uint64_t latency_us);
  
  // ===== 网络指标 =====
  
  // 记录网络重试
  void RecordNetworkRetry(TxnID txn_id, const std::string& operation);
  
  // 记录网络失败
  void RecordNetworkFailure(TxnID txn_id, const std::string& operation, 
                            const std::string& reason);
  
  // 记录参与者故障
  void RecordParticipantFailure(TxnID txn_id, PartitionID partition_id);
  
  // ===== 查询指标 =====
  
  // 获取当前指标快照
  TransactionMetricsSnapshot GetMetricsSnapshot() const;
  
  // 获取延迟直方图快照
  std::vector<std::pair<uint64_t, uint64_t>> GetLatencyDistribution() const;
  
  // 获取冲突率
  double GetConflictRate() const;
  
  // 获取平均延迟
  double GetAverageLatencyMs() const;
  
  // 获取 2PC 协调比例
  double GetCoordinationRatio() const;
  
  // 获取格式化报告
  std::string GetReport() const;
  
  // 重置所有指标
  void Reset();
  
  // ===== 导出 =====
  
  // 导出为 Prometheus 格式
  std::string ExportPrometheusFormat() const;
  
  // 导出为 JSON
  std::string ExportJson() const;

 private:
  mutable std::mutex mutex_;
  TransactionMetrics metrics_;
  LatencyHistogram latency_histogram_;
  LatencyHistogram prepare_histogram_;
  LatencyHistogram commit_histogram_;
  
  // 活跃事务跟踪
  struct ActiveTxn {
    TxnID txn_id;
    TxnType type;
    std::chrono::steady_clock::time_point start_time;
  };
  std::unordered_map<TxnID, ActiveTxn> active_txns_;
};

// =============================================================================
// 全局指标实例
// =============================================================================

TransactionMetricsCollector* GetGlobalTransactionMetrics();

// =============================================================================
// 便捷宏定义
// =============================================================================

#define TXN_METRICS_START(txn_id, type) \
  do { \
    if (auto* metrics = ::cedar::dtx::GetGlobalTransactionMetrics()) { \
      metrics->RecordTransactionStart(txn_id, type); \
    } \
  } while (0)

#define TXN_METRICS_COMMIT(txn_id, latency_us) \
  do { \
    if (auto* metrics = ::cedar::dtx::GetGlobalTransactionMetrics()) { \
      metrics->RecordTransactionCommit(txn_id, latency_us); \
    } \
  } while (0)

#define TXN_METRICS_ABORT(txn_id, latency_us, reason) \
  do { \
    if (auto* metrics = ::cedar::dtx::GetGlobalTransactionMetrics()) { \
      metrics->RecordTransactionAbort(txn_id, latency_us, reason); \
    } \
  } while (0)

#define TXN_METRICS_CONFLICT_RETRY(txn_id, conflict_type) \
  do { \
    if (auto* metrics = ::cedar::dtx::GetGlobalTransactionMetrics()) { \
      metrics->RecordConflictRetry(txn_id, conflict_type); \
    } \
  } while (0)

#define TXN_METRICS_NETWORK_RETRY(txn_id, operation) \
  do { \
    if (auto* metrics = ::cedar::dtx::GetGlobalTransactionMetrics()) { \
      metrics->RecordNetworkRetry(txn_id, operation); \
    } \
  } while (0)

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_TRANSACTION_METRICS_H_
