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

#include "cedar/dtx/transaction_metrics.h"

#include <algorithm>
#include <iomanip>
#include <sstream>

namespace cedar {
namespace dtx {

// =============================================================================
// LatencyHistogram Implementation
// =============================================================================

LatencyHistogram::LatencyHistogram() {
  for (auto& bucket : buckets_) {
    bucket.store(0);
  }
}

void LatencyHistogram::Record(uint64_t latency_us) {
  count_.fetch_add(1, std::memory_order_relaxed);
  sum_.fetch_add(latency_us, std::memory_order_relaxed);
  
  // 更新最小/最大延迟
  uint64_t current_min = min_latency_.load();
  while (latency_us < current_min && 
         !min_latency_.compare_exchange_weak(current_min, latency_us)) {
    // 重试
  }
  
  uint64_t current_max = max_latency_.load();
  while (latency_us > current_max && 
         !max_latency_.compare_exchange_weak(current_max, latency_us)) {
    // 重试
  }
  
  // 更新桶计数
  for (size_t i = 0; i < kBuckets.size(); ++i) {
    if (latency_us <= kBuckets[i]) {
      buckets_[i].fetch_add(1, std::memory_order_relaxed);
      break;
    }
  }
}

uint64_t LatencyHistogram::GetPercentile(double percentile) const {
  uint64_t total = count_.load();
  if (total == 0) return 0;
  
  uint64_t target = static_cast<uint64_t>(total * percentile);
  uint64_t cumulative = 0;
  
  for (size_t i = 0; i < buckets_.size(); ++i) {
    cumulative += buckets_[i].load();
    if (cumulative >= target) {
      return kBuckets[i];
    }
  }
  
  return max_latency_.load();
}

double LatencyHistogram::GetAverage() const {
  uint64_t count = count_.load();
  if (count == 0) return 0.0;
  return static_cast<double>(sum_.load()) / count;
}

void LatencyHistogram::Reset() {
  for (auto& bucket : buckets_) {
    bucket.store(0);
  }
  count_.store(0);
  sum_.store(0);
  min_latency_.store(UINT64_MAX);
  max_latency_.store(0);
}

std::vector<std::pair<uint64_t, uint64_t>> LatencyHistogram::GetDistribution() const {
  std::vector<std::pair<uint64_t, uint64_t>> result;
  result.reserve(kBuckets.size());
  
  for (size_t i = 0; i < kBuckets.size(); ++i) {
    result.emplace_back(kBuckets[i], buckets_[i].load());
  }
  
  return result;
}

// =============================================================================
// TransactionMetricsCollector Implementation
// =============================================================================

TransactionMetricsCollector::TransactionMetricsCollector() = default;

TransactionMetricsCollector::~TransactionMetricsCollector() = default;

void TransactionMetricsCollector::RecordTransactionStart(TxnID txn_id, TxnType type) {
  metrics_.total_txns.fetch_add(1, std::memory_order_relaxed);
  
  switch (type) {
    case TxnType::kSinglePartition:
      metrics_.single_partition_txns.fetch_add(1, std::memory_order_relaxed);
      break;
    case TxnType::kSameTemporalRange:
      metrics_.same_range_txns.fetch_add(1, std::memory_order_relaxed);
      break;
    case TxnType::kCrossTemporalRange:
      metrics_.full_2pc_txns.fetch_add(1, std::memory_order_relaxed);
      break;
    default:
      break;
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  active_txns_[txn_id] = {txn_id, type, std::chrono::steady_clock::now()};
}

void TransactionMetricsCollector::RecordTransactionCommit(TxnID txn_id, 
                                                           uint64_t latency_us) {
  metrics_.committed_txns.fetch_add(1, std::memory_order_relaxed);
  metrics_.total_latency_us.fetch_add(latency_us, std::memory_order_relaxed);
  
  latency_histogram_.Record(latency_us);
  
  std::lock_guard<std::mutex> lock(mutex_);
  active_txns_.erase(txn_id);
}

void TransactionMetricsCollector::RecordTransactionAbort(TxnID txn_id, 
                                                          uint64_t latency_us,
                                                          const std::string& reason) {
  metrics_.aborted_txns.fetch_add(1, std::memory_order_relaxed);
  
  // 根据原因分类统计
  if (reason.find("conflict") != std::string::npos ||
      reason.find("Conflict") != std::string::npos) {
    if (reason.find("write-write") != std::string::npos) {
      metrics_.waw_conflicts.fetch_add(1, std::memory_order_relaxed);
    } else if (reason.find("read-write") != std::string::npos) {
      metrics_.raw_conflicts.fetch_add(1, std::memory_order_relaxed);
    } else if (reason.find("write-read") != std::string::npos) {
      metrics_.war_conflicts.fetch_add(1, std::memory_order_relaxed);
    } else if (reason.find("temporal") != std::string::npos) {
      metrics_.temporal_conflicts.fetch_add(1, std::memory_order_relaxed);
    }
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  active_txns_.erase(txn_id);
}

void TransactionMetricsCollector::RecordConflictRetry(TxnID txn_id, 
                                                       const std::string& conflict_type) {
  metrics_.conflict_retries.fetch_add(1, std::memory_order_relaxed);
}

void TransactionMetricsCollector::RecordPreparePhase(TxnID txn_id, 
                                                      uint64_t latency_us,
                                                      bool success) {
  metrics_.prepare_latency_us.fetch_add(latency_us, std::memory_order_relaxed);
  prepare_histogram_.Record(latency_us);
  
  if (!success) {
    metrics_.prepare_failures.fetch_add(1, std::memory_order_relaxed);
  }
}

void TransactionMetricsCollector::RecordPrepareTimeout(TxnID txn_id) {
  metrics_.prepare_timeouts.fetch_add(1, std::memory_order_relaxed);
}

void TransactionMetricsCollector::RecordCommitPhase(TxnID txn_id, 
                                                     uint64_t latency_us,
                                                     bool success) {
  metrics_.commit_latency_us.fetch_add(latency_us, std::memory_order_relaxed);
  commit_histogram_.Record(latency_us);
  
  if (!success) {
    metrics_.commit_failures.fetch_add(1, std::memory_order_relaxed);
  }
}

void TransactionMetricsCollector::RecordCommitTimeout(TxnID txn_id) {
  metrics_.commit_timeouts.fetch_add(1, std::memory_order_relaxed);
}

void TransactionMetricsCollector::RecordAbortPhase(TxnID txn_id, uint64_t latency_us) {
  metrics_.abort_latency_us.fetch_add(latency_us, std::memory_order_relaxed);
}

void TransactionMetricsCollector::RecordNetworkRetry(TxnID txn_id, 
                                                      const std::string& operation) {
  metrics_.network_retries.fetch_add(1, std::memory_order_relaxed);
}

void TransactionMetricsCollector::RecordNetworkFailure(TxnID txn_id, 
                                                        const std::string& operation,
                                                        const std::string& reason) {
  metrics_.network_failures.fetch_add(1, std::memory_order_relaxed);
}

void TransactionMetricsCollector::RecordParticipantFailure(TxnID txn_id, 
                                                            PartitionID partition_id) {
  metrics_.participant_failures.fetch_add(1, std::memory_order_relaxed);
}

TransactionMetricsSnapshot TransactionMetricsCollector::GetMetricsSnapshot() const {
  return metrics_.ToSnapshot();
}

std::vector<std::pair<uint64_t, uint64_t>> 
TransactionMetricsCollector::GetLatencyDistribution() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return latency_histogram_.GetDistribution();
}

double TransactionMetricsCollector::GetConflictRate() const {
  uint64_t total = metrics_.total_txns.load();
  if (total == 0) return 0.0;
  return static_cast<double>(metrics_.conflict_retries.load()) / total;
}

double TransactionMetricsCollector::GetAverageLatencyMs() const {
  uint64_t committed = metrics_.committed_txns.load();
  if (committed == 0) return 0.0;
  return static_cast<double>(metrics_.total_latency_us.load()) / committed / 1000.0;
}

double TransactionMetricsCollector::GetCoordinationRatio() const {
  uint64_t total = metrics_.total_txns.load();
  if (total == 0) return 0.0;
  uint64_t coord = metrics_.same_range_txns.load() + metrics_.full_2pc_txns.load();
  return static_cast<double>(coord) / total;
}

std::string TransactionMetricsCollector::GetReport() const {
  std::ostringstream oss;
  
  auto snapshot = GetMetricsSnapshot();
  
  oss << "========== Transaction Metrics Report ==========\n";
  oss << "Total Transactions:    " << snapshot.total_txns << "\n";
  oss << "Committed:             " << snapshot.committed_txns << "\n";
  oss << "Aborted:               " << snapshot.aborted_txns << "\n";
  oss << "Commit Rate:           " << std::fixed << std::setprecision(2)
      << (snapshot.total_txns > 0 
          ? 100.0 * snapshot.committed_txns / snapshot.total_txns 
          : 0.0) << "%\n";
  oss << "Conflict Rate:         " << std::fixed << std::setprecision(2)
      << GetConflictRate() * 100 << "%\n";
  oss << "\n";
  
  oss << "--- Transaction Types ---\n";
  oss << "Single Partition:      " << snapshot.single_partition_txns << "\n";
  oss << "Same Temporal Range:   " << snapshot.same_range_txns << "\n";
  oss << "Full 2PC:              " << snapshot.full_2pc_txns << "\n";
  oss << "Coordination Ratio:    " << std::fixed << std::setprecision(2)
      << GetCoordinationRatio() * 100 << "%\n";
  oss << "\n";
  
  oss << "--- Latency (ms) ---\n";
  oss << "Average:               " << std::fixed << std::setprecision(2)
      << GetAverageLatencyMs() << "\n";
  oss << "P50:                   " << std::fixed << std::setprecision(2)
      << latency_histogram_.GetPercentile(0.50) / 1000.0 << "\n";
  oss << "P99:                   " << std::fixed << std::setprecision(2)
      << latency_histogram_.GetPercentile(0.99) / 1000.0 << "\n";
  oss << "\n";
  
  oss << "--- 2PC Phases ---\n";
  oss << "Prepare Timeouts:      " << snapshot.prepare_timeouts << "\n";
  oss << "Commit Timeouts:       " << snapshot.commit_timeouts << "\n";
  oss << "Prepare Failures:      " << snapshot.prepare_failures << "\n";
  oss << "Commit Failures:       " << snapshot.commit_failures << "\n";
  oss << "\n";
  
  oss << "--- Network ---\n";
  oss << "Network Retries:       " << snapshot.network_retries << "\n";
  oss << "Network Failures:      " << snapshot.network_failures << "\n";
  oss << "Participant Failures:  " << snapshot.participant_failures << "\n";
  oss << "================================================\n";
  
  return oss.str();
}

void TransactionMetricsCollector::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  metrics_.Reset();
  latency_histogram_.Reset();
  prepare_histogram_.Reset();
  commit_histogram_.Reset();
  active_txns_.clear();
}

std::string TransactionMetricsCollector::ExportPrometheusFormat() const {
  std::ostringstream oss;
  auto snapshot = GetMetricsSnapshot();
  
  auto output_counter = [&](const std::string& name, uint64_t value) {
    oss << "# TYPE cedar_txn_" << name << " counter\n";
    oss << "cedar_txn_" << name << " " << value << "\n";
  };
  
  auto output_gauge = [&](const std::string& name, double value) {
    oss << "# TYPE cedar_txn_" << name << " gauge\n";
    oss << "cedar_txn_" << name << " " << std::fixed << std::setprecision(2) << value << "\n";
  };
  
  output_counter("total", snapshot.total_txns);
  output_counter("committed", snapshot.committed_txns);
  output_counter("aborted", snapshot.aborted_txns);
  output_counter("conflict_retries", snapshot.conflict_retries);
  output_counter("prepare_timeouts", snapshot.prepare_timeouts);
  output_counter("commit_timeouts", snapshot.commit_timeouts);
  output_counter("network_retries", snapshot.network_retries);
  
  output_gauge("conflict_rate", GetConflictRate());
  output_gauge("avg_latency_ms", GetAverageLatencyMs());
  output_gauge("coordination_ratio", GetCoordinationRatio());
  
  return oss.str();
}

std::string TransactionMetricsCollector::ExportJson() const {
  std::ostringstream oss;
  auto snapshot = GetMetricsSnapshot();
  
  oss << "{\n";
  oss << "  \"total_txns\": " << snapshot.total_txns << ",\n";
  oss << "  \"committed_txns\": " << snapshot.committed_txns << ",\n";
  oss << "  \"aborted_txns\": " << snapshot.aborted_txns << ",\n";
  oss << "  \"conflict_rate\": " << std::fixed << std::setprecision(4) << GetConflictRate() << ",\n";
  oss << "  \"avg_latency_ms\": " << std::fixed << std::setprecision(2) << GetAverageLatencyMs() << ",\n";
  oss << "  \"coordination_ratio\": " << std::fixed << std::setprecision(4) << GetCoordinationRatio() << ",\n";
  oss << "  \"single_partition_txns\": " << snapshot.single_partition_txns << ",\n";
  oss << "  \"same_range_txns\": " << snapshot.same_range_txns << ",\n";
  oss << "  \"full_2pc_txns\": " << snapshot.full_2pc_txns << ",\n";
  oss << "  \"prepare_timeouts\": " << snapshot.prepare_timeouts << ",\n";
  oss << "  \"commit_timeouts\": " << snapshot.commit_timeouts << "\n";
  oss << "}\n";
  
  return oss.str();
}

// =============================================================================
// Global Instance
// =============================================================================

TransactionMetricsCollector* GetGlobalTransactionMetrics() {
  static TransactionMetricsCollector instance;
  return &instance;
}

}  // namespace dtx
}  // namespace cedar
