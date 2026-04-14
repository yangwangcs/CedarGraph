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
// Transaction Context - 分布式事务上下文
// =============================================================================

#ifndef CEDAR_DTX_TXN_CONTEXT_H_
#define CEDAR_DTX_TXN_CONTEXT_H_

#include <cstdint>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <chrono>
#include <memory>
#include <atomic>

#include "cedar/types/cedar_key.h"
#include "cedar/driver/bookmark.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/temporal_window.h"

namespace cedar {
namespace dtx {

// 前向声明
class DistributedBookmark;

/**
 * @brief 事务选项
 */
struct DistributedTxnOptions {
  // 时序窗口（TW-CD）
  TemporalWindow temporal_window;
  
  // 超时配置
  std::chrono::milliseconds timeout{30000};  // 默认30秒
  
  // 一致性级别
  TemporalConsistencyLevel consistency_level{TemporalConsistencyLevel::kStrict};
  
  // 最大重试次数
  uint32_t max_retries{3};
  
  // 重试基础延迟
  uint32_t retry_base_delay_ms{10};
  
  // 是否只读
  bool read_only{false};
  
  // 优先级（用于死锁处理）
  uint32_t priority{0};
  
  // 元数据（用于监控/日志）
  std::unordered_map<std::string, std::string> metadata;
};

/**
 * @brief 2PC投票结果
 */
struct VoteResult {
  PartitionID partition_id{kInvalidPartitionID};
  bool prepared{false};           // true=Prepared, false=Abort
  std::string reason;            // 如果Abort，原因说明
  uint64_t prepared_ts{0};       // 准备时间戳（用于DVC-Val）
  
  // 构造函数
  VoteResult() = default;
  VoteResult(PartitionID pid, bool p, const std::string& r = "")
      : partition_id(pid), prepared(p), reason(r) {}
};

/**
 * @brief 分布式事务上下文
 * 
 * 扩展单机OCCTransaction，添加分布式协调所需信息
 */
class DistributedTxnContext {
 public:
  // 构造函数
  DistributedTxnContext() = default;
  explicit DistributedTxnContext(TxnID tid, NodeID coordinator)
      : txn_id_(tid), coordinator_node_(coordinator) {}
  
  // ==================== 基础信息 ====================
  
  TxnID GetTxnID() const { return txn_id_; }
  void SetTxnID(TxnID tid) { txn_id_ = tid; }
  
  uint64_t GetStartTimestamp() const { return start_ts_; }
  void SetStartTimestamp(uint64_t ts) { start_ts_ = ts; }
  
  uint64_t GetCommitTimestamp() const { return commit_ts_; }
  void SetCommitTimestamp(uint64_t ts) { commit_ts_ = ts; }
  
  TxnType GetType() const { return type_; }
  void SetType(TxnType t) { type_ = t; }
  
  DistributedTxnState GetState() const { return state_.load(); }
  void SetState(DistributedTxnState s) { state_.store(s); }
  
  // ==================== 时序窗口（TW-CD） ====================
  
  const TemporalWindow& GetTemporalWindow() const { return temporal_window_; }
  void SetTemporalWindow(const TemporalWindow& w) { temporal_window_ = w; }
  
  // 扩展时序窗口（包含新的时间点）
  void ExtendTemporalWindow(Timestamp ts) {
    temporal_window_.Merge(TemporalWindow(ts));
  }
  
  // ==================== 参与者信息 ====================
  
  NodeID GetCoordinator() const { return coordinator_node_; }
  void SetCoordinator(NodeID nid) { coordinator_node_ = nid; }
  
  const std::unordered_set<PartitionID>& GetParticipants() const {
    return participant_partitions_;
  }
  
  void AddParticipant(PartitionID pid) {
    participant_partitions_.insert(pid);
  }
  
  void AddParticipants(const std::vector<PartitionID>& pids) {
    for (auto pid : pids) {
      participant_partitions_.insert(pid);
    }
  }
  
  // 设置分区Leader映射
  void SetPartitionLeader(PartitionID pid, NodeID node_id) {
    partition_leaders_[pid] = node_id;
  }
  
  NodeID GetPartitionLeader(PartitionID pid) const {
    auto it = partition_leaders_.find(pid);
    return (it != partition_leaders_.end()) ? it->second : kInvalidNodeID;
  }
  
  // ==================== 读写集 ====================
  
  void AddToReadSet(const CedarKey& key, const TemporalWindow& window = TemporalWindow()) {
    read_set_.emplace_back(key, window);
  }
  
  void AddToWriteSet(const CedarKey& key, const TemporalWindow& window = TemporalWindow()) {
    write_set_.emplace_back(key, window);
  }
  
  const std::vector<TemporalReadSetItem>& GetReadSet() const { return read_set_; }
  const std::vector<TemporalWriteSetItem>& GetWriteSet() const { return write_set_; }
  
  std::vector<TemporalReadSetItem>& GetReadSet() { return read_set_; }
  std::vector<TemporalWriteSetItem>& GetWriteSet() { return write_set_; }
  
  // 清空读写集
  void ClearReadSet() { read_set_.clear(); }
  void ClearWriteSet() { write_set_.clear(); }
  
  // ==================== 图局部性（GLTR） ====================
  
  void AddTouchedSubgraph(SubgraphID sid) {
    touched_subgraphs_.insert(sid);
  }
  
  const std::unordered_set<SubgraphID>& GetTouchedSubgraphs() const {
    return touched_subgraphs_;
  }
  
  void SetLocalSubgraph(bool local) { is_local_subgraph_ = local; }
  bool IsLocalSubgraph() const { return is_local_subgraph_; }
  
  // ==================== 因果一致性（BBCC） ====================
  
  void AddCausalDependency(const driver::Bookmark& bm) {
    causal_dependencies_.push_back(bm);
  }
  
  const std::vector<driver::Bookmark>& GetCausalDependencies() const {
    return causal_dependencies_;
  }
  
  // ==================== 快速判断方法 ====================
  
  bool IsSinglePartition() const {
    return participant_partitions_.size() == 1;
  }
  
  bool NeedsCoordination() const {
    return participant_partitions_.size() > 1;
  }
  
  size_t GetParticipantCount() const {
    return participant_partitions_.size();
  }
  
  // 判断是否与另一个事务冲突（TW-CD）
  bool MayConflictWith(const DistributedTxnContext& other) const {
    // 首先检查时序窗口
    if (!temporal_window_.Overlaps(other.temporal_window_)) {
      return false;  // 时间不重叠，无冲突
    }
    
    // 检查分区重叠
    for (const auto& pid : participant_partitions_) {
      if (other.participant_partitions_.count(pid)) {
        return true;  // 时间和分区都重叠，可能冲突
      }
    }
    return false;
  }
  
  // ==================== 统计信息 ====================
  
  void RecordExecutionStart() {
    execution_start_time_ = std::chrono::steady_clock::now();
  }
  
  void RecordExecutionEnd() {
    auto end = std::chrono::steady_clock::now();
    execution_time_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        end - execution_start_time_).count();
  }
  
  uint64_t GetExecutionTimeMs() const { return execution_time_ms_; }
  
  void RecordCoordinationStart() {
    coord_start_time_ = std::chrono::steady_clock::now();
  }
  
  void RecordCoordinationEnd() {
    auto end = std::chrono::steady_clock::now();
    coord_time_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        end - coord_start_time_).count();
  }
  
  uint64_t GetCoordinationTimeMs() const { return coord_time_ms_; }
  
  void IncrementRetryCount() { ++retry_count_; }
  uint32_t GetRetryCount() const { return retry_count_; }
  
  // ==================== 序列化 ====================
  
  std::string Serialize() const;
  static std::unique_ptr<DistributedTxnContext> Deserialize(const std::string& data);
  
 private:
  // 基础信息
  TxnID txn_id_{kInvalidTxnID};
  uint64_t start_ts_{0};       // 开始时间戳
  uint64_t commit_ts_{0};      // 提交时间戳
  
  TxnType type_{TxnType::kSinglePartition};
  std::atomic<DistributedTxnState> state_{DistributedTxnState::kStarted};
  
  // 时序窗口
  TemporalWindow temporal_window_;
  
  // 参与者
  NodeID coordinator_node_{kInvalidNodeID};
  std::unordered_set<PartitionID> participant_partitions_;
  std::unordered_map<PartitionID, NodeID> partition_leaders_;
  
  // 读写集
  std::vector<TemporalReadSetItem> read_set_;
  std::vector<TemporalWriteSetItem> write_set_;
  
  // 图局部性
  std::unordered_set<SubgraphID> touched_subgraphs_;
  bool is_local_subgraph_{false};
  
  // 因果一致性
  std::vector<driver::Bookmark> causal_dependencies_;
  
  // 统计
  std::chrono::steady_clock::time_point execution_start_time_;
  std::chrono::steady_clock::time_point coord_start_time_;
  uint64_t execution_time_ms_{0};
  uint64_t coord_time_ms_{0};
  uint32_t retry_count_{0};
};

/**
 * @brief 事务路由决策
 * 
 * GLTR根据Key集合和图拓扑决定事务类型和协调策略
 */
struct RoutingDecision {
  TxnType txn_type{TxnType::kSinglePartition};
  std::vector<PartitionID> participants;
  NodeID coordinator{kInvalidNodeID};
  SubgraphID primary_subgraph{kInvalidSubgraphID};
  bool can_use_local_occ{true};
  bool needs_2pc{false};
  std::string reason;  // 决策原因（用于调试）
  
  // 便捷方法
  bool IsLocal() const {
    return txn_type == TxnType::kSinglePartition;
  }
  
  bool NeedsCoordination() const {
    return txn_type != TxnType::kSinglePartition;
  }
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_TXN_CONTEXT_H_
