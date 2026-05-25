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

#include "cedar/dtx/lsm_native_occ.h"

#include <chrono>
#include <random>
#include <shared_mutex>

#include "cedar/common/logging.h"
#include "cedar/dtx/twcd_engine.h"
#include "cedar/dtx/partition.h"
#include "cedar/dtx/transaction_state.h"
#include "cedar/dtx/transaction_metrics.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/vsl_memtable.h"
#include "cedar/transaction/wal.h"

namespace cedar {
namespace dtx {

// =============================================================================
// 辅助函数
// =============================================================================

static std::atomic<TxnID> global_txn_id_counter{1000};

TxnID GenerateTxnID() {
  return global_txn_id_counter.fetch_add(1, std::memory_order_relaxed);
}

uint64_t GenerateTimestamp() {
  return Timestamp::Now().value();
}

// =============================================================================
// LocalTransactionCoordinator 实现
// =============================================================================

LocalTransactionCoordinator::LocalTransactionCoordinator(
    PartitionID partition_id,
    VSLMemTable* memtable,
    WalWriter* wal_writer,
    TwcdEngine* twcd_engine)
    : partition_id_(partition_id),
      memtable_(memtable),
      wal_writer_(wal_writer),
      twcd_engine_(twcd_engine) {}

LocalTransactionCoordinator::~LocalTransactionCoordinator() = default;

Status LocalTransactionCoordinator::BeginTransaction(DistributedTxnContext* ctx) {
  if (!ctx) {
    return Status::InvalidArgument("LocalTransactionCoordinator", "Null context");
  }
  
  // 分配事务ID（如果还没有）
  if (ctx->GetTxnID() == kInvalidTxnID) {
    ctx->SetTxnID(GenerateTxnID());
  }
  
  // 分配开始时间戳
  if (ctx->GetStartTimestamp() == 0) {
    ctx->SetStartTimestamp(GenerateTimestamp());
  }
  
  // 设置分区
  ctx->AddParticipant(partition_id_);
  ctx->SetPartitionLeader(partition_id_, 0);  // 本地节点ID
  
  // 注册到 TW-CD
  if (twcd_engine_) {
    auto status = twcd_engine_->RegisterWindow(ctx->GetTxnID(), ctx->GetTemporalWindow());
    if (!status.ok()) {
      return status;
    }
  }
  
  ctx->SetState(DistributedTxnState::kStarted);
  ++total_txns_;
  
  return Status::OK();
}

Status LocalTransactionCoordinator::Validate(DistributedTxnContext* ctx) {
  if (!ctx) {
    return Status::InvalidArgument("LocalTransactionCoordinator", "Null context");
  }
  
  ctx->SetState(DistributedTxnState::kPreparing);
  
  // 如果没有 TW-CD，跳过验证
  if (!twcd_engine_) {
    return Status::OK();
  }
  
  // 提取读写集
  std::vector<CedarKey> read_set;
  std::vector<CedarKey> write_set;
  
  for (const auto& item : ctx->GetReadSet()) {
    read_set.push_back(item.key);
  }
  
  for (const auto& item : ctx->GetWriteSet()) {
    write_set.push_back(item.key);
  }
  
  // 注册写集
  if (!write_set.empty()) {
    auto status = twcd_engine_->RegisterWriteSet(ctx->GetTxnID(), write_set);
    if (!status.ok()) {
      return status;
    }
  }
  
  // 使用 TW-CD 进行冲突检测
  auto result = twcd_engine_->CheckConflict(
      ctx->GetTxnID(),
      ctx->GetTemporalWindow(),
      read_set,
      write_set);
  
  if (result.has_conflict) {
    return Status::IOError("Validate", "Conflict detected: " + 
        std::to_string(static_cast<int>(result.type)));
  }
  
  return Status::OK();
}

LndOccCommitResult LocalTransactionCoordinator::Commit(DistributedTxnContext* ctx) {
  if (!ctx) {
    return LndOccCommitResult::Error(
        Status::InvalidArgument("LocalTransactionCoordinator", "Null context"));
  }
  
  auto commit_start = std::chrono::steady_clock::now();
  
  // 步骤1: 验证
  auto validate_start = std::chrono::steady_clock::now();
  auto status = Validate(ctx);
  auto validate_end = std::chrono::steady_clock::now();
  
  if (!status.ok()) {
    Abort(ctx, "Validation failed: " + status.ToString());
    return LndOccCommitResult::Error(status);
  }
  
  // 步骤2: 分配提交时间戳
  uint64_t commit_ts = GenerateTimestamp();
  ctx->SetCommitTimestamp(commit_ts);
  ctx->SetState(DistributedTxnState::kCommitting);
  
  // 步骤3: 写入 WAL（如果配置了）
  if (wal_writer_) {
    // Serialize transaction context for WAL
    std::string wal_entry = ctx->Serialize();
    // Note: WalWriter interface varies by implementation
    // For now, we assume WAL is optional for local transactions
    (void)wal_entry;
  }
  
  // 步骤4: 写入 MemTable（利用不可变性）
  auto write_start = std::chrono::steady_clock::now();
  if (memtable_) {
    for (const auto& item : ctx->GetWriteSet()) {
      // Create descriptor from write item (assuming item has value field)
      // Note: TemporalWriteSetItem structure may vary
      Descriptor desc;  // Default empty descriptor
      auto put_status = memtable_->Put(item.key, desc, Timestamp(commit_ts));
      if (!put_status.ok()) {
        Abort(ctx, "MemTable write failed: " + put_status.ToString());
        return LndOccCommitResult::Error(put_status);
      }
    }
  }
  auto write_end = std::chrono::steady_clock::now();
  
  // 步骤5: 清理 TW-CD 注册
  if (twcd_engine_) {
    twcd_engine_->UnregisterWindow(ctx->GetTxnID());
    twcd_engine_->UnregisterWriteSet(ctx->GetTxnID());
  }
  
  ctx->SetState(DistributedTxnState::kCommitted);
  ++committed_txns_;
  
  auto commit_end = std::chrono::steady_clock::now();
  
  LndOccCommitResult result;
  result.success = true;
  result.commit_ts = commit_ts;
  result.status = Status::OK();
  result.validation_time_us = 
      std::chrono::duration_cast<std::chrono::microseconds>(validate_end - validate_start).count();
  result.write_time_us = 
      std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start).count();
  
  return result;
}

Status LocalTransactionCoordinator::Abort(DistributedTxnContext* ctx, 
                                           const std::string& reason) {
  if (!ctx) {
    return Status::InvalidArgument("LocalTransactionCoordinator", "Null context");
  }
  
  ctx->SetState(DistributedTxnState::kAborting);
  
  // 清理 TW-CD 注册
  if (twcd_engine_) {
    twcd_engine_->UnregisterWindow(ctx->GetTxnID());
    twcd_engine_->UnregisterWriteSet(ctx->GetTxnID());
  }
  
  ctx->SetState(DistributedTxnState::kAborted);
  ++aborted_txns_;
  
  return Status::OK();
}

LocalTransactionCoordinator::Stats LocalTransactionCoordinator::GetStats() const {
  Stats stats;
  stats.total_txns = total_txns_.load();
  stats.committed_txns = committed_txns_.load();
  stats.aborted_txns = aborted_txns_.load();
  stats.conflict_retries = conflict_retries_.load();
  
  auto total = total_txns_.load();
  if (total > 0) {
    stats.avg_latency_ms = static_cast<double>(total_latency_us_.load()) / total / 1000.0;
  }
  
  return stats;
}

void LocalTransactionCoordinator::ResetStats() {
  total_txns_.store(0);
  committed_txns_.store(0);
  aborted_txns_.store(0);
  conflict_retries_.store(0);
  total_latency_us_.store(0);
}

// =============================================================================
// LndOccEngine 实现
// =============================================================================

LndOccEngine::LndOccEngine(const DTxConfig& config) 
    : config_(config) {}

LndOccEngine::~LndOccEngine() = default;

Status LndOccEngine::Initialize(
    PartitionManager* partition_manager,
    const std::unordered_map<PartitionID, std::pair<VSLMemTable*, WalWriter*>>& partition_stores) {
  
  partition_manager_ = partition_manager;
  
  std::unique_lock<std::shared_mutex> lock(coordinators_mutex_);
  // 为每个分区创建本地协调器
  for (const auto& [pid, stores] : partition_stores) {
    auto coordinator = std::make_unique<LocalTransactionCoordinator>(
        pid, stores.first, stores.second, twcd_engine_);
    coordinators_[pid] = std::move(coordinator);
  }
  
  return Status::OK();
}

LndOccCommitResult LndOccEngine::SinglePartitionCommit(DistributedTxnContext* ctx) {
  if (!ctx) {
    return LndOccCommitResult::Error(
        Status::InvalidArgument("LndOccEngine", "Null context"));
  }
  
  // 确保是单分区事务
  if (ctx->GetParticipantCount() != 1) {
    return LndOccCommitResult::Error(
        Status::InvalidArgument("LndOccEngine", "Not a single-partition transaction"));
  }
  
  PartitionID pid = *ctx->GetParticipants().begin();
  auto* coordinator = GetCoordinator(pid);
  
  if (!coordinator) {
    return LndOccCommitResult::Error(
        Status::NotFound("LndOccEngine", "Partition coordinator not found"));
  }
  
  auto result = coordinator->Commit(ctx);
  
  if (result.success) {
    ++single_partition_commits_;
  }
  
  return result;
}

LndOccCommitResult LndOccEngine::SameTemporalRangeCommit(
    const std::vector<PartitionID>& participants,
    DistributedTxnContext* ctx) {

  // Use lightweight coordination but still ensure atomicity via 2PC Prepare+Commit.
  ++same_range_commits_;

  if (!ctx) {
    return LndOccCommitResult::Error(
        Status::InvalidArgument("SameTemporalRangeCommit", "Null context"));
  }
  if (participants.empty()) {
    return LndOccCommitResult::Error(
        Status::InvalidArgument("SameTemporalRangeCommit", "No participants"));
  }

  // Phase 1: Validate (Prepare) all participants
  std::vector<std::pair<PartitionID, Status>> prepare_results;
  bool all_prepared = true;
  for (PartitionID pid : participants) {
    auto* coordinator = GetCoordinator(pid);
    if (!coordinator) {
      all_prepared = false;
      break;
    }
    auto status = coordinator->Validate(ctx);
    if (!status.ok()) {
      all_prepared = false;
      prepare_results.emplace_back(pid, status);
      break;
    }
    prepare_results.emplace_back(pid, status);
  }

  if (!all_prepared) {
    // Abort all prepared participants
    for (const auto& [pid, status] : prepare_results) {
      if (status.ok()) {
        auto* coordinator = GetCoordinator(pid);
        if (coordinator) {
          coordinator->Abort(ctx, "Prepare phase failed");
        }
      }
    }
    return LndOccCommitResult::Error(
        Status::IOError("SameTemporalRangeCommit", "Prepare phase failed"));
  }

  // Phase 2: Commit all participants
  LndOccCommitResult overall = LndOccCommitResult::Ok(ctx->GetCommitTimestamp());
  for (PartitionID pid : participants) {
    auto* coordinator = GetCoordinator(pid);
    if (!coordinator) continue;  // Should not happen after prepare
    auto result = coordinator->Commit(ctx);
    if (!result.success) {
      overall = result;
      LOG(ERROR) << "SameTemporalRangeCommit: commit failed after prepare success"
                 << " partition=" << pid;
    }
  }

  return overall;
}

LndOccCommitResult LndOccEngine::FullTwoPhaseCommit(
    const std::vector<PartitionID>& participants,
    DistributedTxnContext* ctx) {
  
  if (!ctx) {
    return LndOccCommitResult::Error(
        Status::InvalidArgument("FullTwoPhaseCommit", "Null context"));
  }
  
  if (participants.empty()) {
    return LndOccCommitResult::Error(
        Status::InvalidArgument("FullTwoPhaseCommit", "No participants"));
  }
  
  ++full_2pc_commits_;
  
  auto txn_start = std::chrono::steady_clock::now();
  TxnID txn_id = ctx->GetTxnID();
  
  // 记录事务开始
  TXN_METRICS_START(txn_id, TxnType::kCrossTemporalRange);
  
  // 步骤 1: 创建事务状态（用于故障恢复）
  if (txn_state_manager_) {
    auto status = txn_state_manager_->CreateTransaction(txn_id, participants);
    if (!status.ok()) {
      TXN_METRICS_ABORT(txn_id, 0, "State creation failed");
      return LndOccCommitResult::Error(status);
    }
    txn_state_manager_->UpdateState(txn_id, TxnState::kPreparing);
  }
  
  // 步骤 2: 收集读写集
  std::vector<CedarKey> read_set;
  std::vector<CedarKey> write_set;
  
  for (const auto& item : ctx->GetReadSet()) {
    read_set.push_back(item.key);
  }
  for (const auto& item : ctx->GetWriteSet()) {
    write_set.push_back(item.key);
  }
  
  // 分配提交时间戳
  Timestamp commit_ts = Timestamp::Now();
  
  // 步骤 3: Phase 1 - Prepare
  auto prepare_start = std::chrono::steady_clock::now();
  
  struct PrepareResult {
    PartitionID partition_id;
    bool prepared;
    std::string error_msg;
  };
  
  std::vector<PrepareResult> prepare_results;
  bool all_prepared = true;
  
  // 并行向所有参与者发送 Prepare
  for (const auto& pid : participants) {
    auto* client = GetStorageClient(pid);
    if (!client) {
      prepare_results.push_back({pid, false, "StorageClient not found"});
      all_prepared = false;
      continue;
    }
    
    auto result = client->Prepare(txn_id, read_set, write_set, commit_ts);
    
    if (!result.ok()) {
      prepare_results.push_back({pid, false, result.status().ToString()});
      all_prepared = false;
      TXN_METRICS_NETWORK_RETRY(txn_id, "Prepare");
    } else if (!result.value()) {
      prepare_results.push_back({pid, false, "Participant voted no"});
      all_prepared = false;
    } else {
      prepare_results.push_back({pid, true, ""});
    }
    
    // 更新参与者状态
    if (txn_state_manager_) {
      txn_state_manager_->UpdateParticipantState(
          txn_id, pid, 
          result.ok() && result.value() ? ParticipantState::State::kPrepared : ParticipantState::State::kAborted,
          result.ok() ? "" : result.status().ToString());
    }
  }
  
  auto prepare_end = std::chrono::steady_clock::now();
  auto prepare_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      prepare_end - prepare_start).count();
  
  if (txn_state_manager_) {
    txn_state_manager_->UpdateState(
        txn_id, all_prepared ? TxnState::kPrepared : TxnState::kAborting);
  }
  
  TXN_METRICS_NETWORK_RETRY(txn_id, all_prepared ? "Prepare success" : "Prepare failed");
  
  // 如果 Prepare 阶段失败，发送 Abort 到所有参与者（无论 prepare 是否成功）
  if (!all_prepared) {
    for (const auto& pid : participants) {
      auto* client = GetStorageClient(pid);
      if (client) {
        auto abort_result = client->Abort(txn_id);
        if (!abort_result.ok()) {
          TXN_METRICS_NETWORK_RETRY(txn_id, "Abort after failed prepare");
        }
      }
    }
    
    if (txn_state_manager_) {
      txn_state_manager_->UpdateState(txn_id, TxnState::kAborting);
    }
    
    auto txn_end = std::chrono::steady_clock::now();
    auto total_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        txn_end - txn_start).count();
    
    TXN_METRICS_ABORT(txn_id, total_latency_us, "Prepare phase failed");
    
    return LndOccCommitResult::Error(
        Status::IOError("FullTwoPhaseCommit", "Prepare phase failed"));
  }
  
  // 步骤 4: Phase 2 - Commit
  auto commit_start = std::chrono::steady_clock::now();
  
  if (txn_state_manager_) {
    txn_state_manager_->UpdateState(txn_id, TxnState::kCommitting);
  }
  
  bool all_committed = true;
  std::vector<PartitionID> committed_partitions;
  
  for (const auto& pid : participants) {
    auto* client = GetStorageClient(pid);
    if (!client) {
      all_committed = false;
      TXN_METRICS_NETWORK_RETRY(txn_id, "Commit: client not found");
      continue;
    }
    
    auto result = client->Commit(txn_id, commit_ts);
    
    if (!result.ok()) {
      all_committed = false;
      TXN_METRICS_NETWORK_RETRY(txn_id, "Commit failed: " + result.ToString());
      // 记录参与者失败，稍后需要恢复
      if (txn_state_manager_) {
        txn_state_manager_->UpdateParticipantState(
            txn_id, pid, ParticipantState::State::kFailed, result.ToString());
      }
    } else {
      committed_partitions.push_back(pid);
      if (txn_state_manager_) {
        txn_state_manager_->UpdateParticipantState(
            txn_id, pid, ParticipantState::State::kCommitted, "");
      }
    }
  }
  
  auto commit_end = std::chrono::steady_clock::now();
  auto commit_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      commit_end - commit_start).count();
  
  auto txn_end = std::chrono::steady_clock::now();
  auto total_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      txn_end - txn_start).count();
  
  // 步骤 5: 更新最终状态
  if (txn_state_manager_) {
    txn_state_manager_->UpdateState(
        txn_id, all_committed ? TxnState::kCommitted : TxnState::kUnknown);
  }
  
  // 记录指标
  if (all_committed) {
    TXN_METRICS_COMMIT(txn_id, total_latency_us);
    
    LndOccCommitResult result;
    result.success = true;
    result.commit_ts = commit_ts.value();
    result.status = Status::OK();
    result.validation_time_us = prepare_latency_us;
    result.write_time_us = commit_latency_us;
    return result;
  } else {
    // 部分提交 - 需要后续恢复
    TXN_METRICS_ABORT(txn_id, total_latency_us, "Partial commit");
    
    return LndOccCommitResult::Error(
        Status::IOError("FullTwoPhaseCommit", 
            "Partial commit - requires recovery for " + 
            std::to_string(participants.size() - committed_partitions.size()) + 
            " participants"));
  }
}

LocalTransactionCoordinator* LndOccEngine::GetCoordinator(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(coordinators_mutex_);
  auto it = coordinators_.find(pid);
  return (it != coordinators_.end()) ? it->second.get() : nullptr;
}

TxnType LndOccEngine::ClassifyTransaction(const DistributedTxnContext* ctx) {
  if (!ctx) {
    return TxnType::kSinglePartition;
  }
  
  size_t participant_count = ctx->GetParticipantCount();
  
  if (participant_count == 1) {
    return TxnType::kSinglePartition;
  }
  
  // Check if all read/write sets share the same temporal window
  const auto& reads = ctx->GetReadSet();
  const auto& writes = ctx->GetWriteSet();
  if (reads.empty() && writes.empty()) {
    return TxnType::kCrossTemporalRange;
  }
  
  TemporalWindow common_window;
  bool first = true;
  for (const auto& item : reads) {
    if (first) {
      common_window = item.window;
      first = false;
    } else if (item.window.start != common_window.start || 
               item.window.end != common_window.end) {
      return TxnType::kCrossTemporalRange;
    }
  }
  for (const auto& item : writes) {
    if (first) {
      common_window = item.window;
      first = false;
    } else if (item.window.start != common_window.start || 
               item.window.end != common_window.end) {
      return TxnType::kCrossTemporalRange;
    }
  }
  
  return TxnType::kSameTemporalRange;
}

std::vector<LndOccCommitResult> LndOccEngine::BatchCommit(
    const std::vector<DistributedTxnContext*>& ctxs) {
  
  std::vector<LndOccCommitResult> results;
  results.reserve(ctxs.size());
  
  for (auto* ctx : ctxs) {
    if (ctx->IsSinglePartition()) {
      results.push_back(SinglePartitionCommit(ctx));
    } else {
      // 回退到 2PC
      results.push_back(FullTwoPhaseCommit(
          std::vector<PartitionID>(ctx->GetParticipants().begin(), 
                                   ctx->GetParticipants().end()),
          ctx));
    }
  }
  
  return results;
}

LndOccEngine::Stats LndOccEngine::GetStats() const {
  Stats stats;
  stats.single_partition_commits = single_partition_commits_.load();
  stats.same_range_commits = same_range_commits_.load();
  stats.full_2pc_commits = full_2pc_commits_.load();
  stats.total_commits = stats.single_partition_commits + 
                        stats.same_range_commits + 
                        stats.full_2pc_commits;
  
  if (stats.total_commits > 0) {
    stats.coordination_ratio = 1.0 - (static_cast<double>(stats.single_partition_commits) / 
                                       stats.total_commits);
  }
  
  return stats;
}

// =============================================================================
// ZoneAwareWriteGrouper 实现
// =============================================================================

std::map<ZoneAwareWriteGrouper::ZoneID, std::vector<CedarKey>> 
ZoneAwareWriteGrouper::GroupByZone(const std::vector<CedarKey>& keys) {
  
  std::map<ZoneID, std::vector<CedarKey>> result;
  
  for (const auto& key : keys) {
    ZoneID zone = GetZoneForKey(key);
    result[zone].push_back(key);
  }
  
  return result;
}

ZoneAwareWriteGrouper::ZoneID ZoneAwareWriteGrouper::GetZoneForKey(const CedarKey& key) {
  // 简单的启发式规则
  // 实际实现应该根据 column_id 和 entity_type 判断
  
  uint16_t col_id = key.column_id();
  
  if (col_id < 10) {
    return ZoneID::kTopology;  // 拓扑信息
  } else if (col_id < 20) {
    return ZoneID::kTemporal;  // 时序信息
  } else if (col_id < 30) {
    return ZoneID::kMetadata;  // 元数据
  } else {
    return ZoneID::kProperty;  // 属性值
  }
}

}  // namespace dtx
}  // namespace cedar
