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
// LND-OCC: LSM-Tree Native Distributed Optimistic Concurrency Control
// =============================================================================
// 核心思想：利用 LSM-Tree 的不可变特性，单分区事务无需分布式协调
// =============================================================================

#ifndef CEDAR_DTX_LSM_NATIVE_OCC_H_
#define CEDAR_DTX_LSM_NATIVE_OCC_H_

#include <cstdint>
#include <vector>
#include <memory>
#include <atomic>
#include <unordered_map>
#include <map>
#include <mutex>
#include <shared_mutex>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/txn_context.h"
#include "cedar/transaction/occ_transaction.h"

// 前向声明
namespace cedar {
class TransactionStateManager;
namespace dtx {
class StorageClient;
class TransactionMetricsCollector;
}  // namespace dtx
}  // namespace cedar



namespace cedar {

class VSLMemTable;
class WalWriter;

namespace dtx {

// 前向声明
class TwcdEngine;
class PartitionManager;

// 辅助函数声明（在源文件中定义）
TxnID GenerateTxnID();
uint64_t GenerateTimestamp();

// 模板函数中使用的内联辅助函数
inline TxnID GenerateTxnIDInline() {
  static std::atomic<TxnID> counter{1000};
  return counter.fetch_add(1, std::memory_order_relaxed);
}

inline uint64_t GenerateTimestampInline() {
  return Timestamp::Now().value();
}

/**
 * @brief LND-OCC 提交结果
 */
struct LndOccCommitResult {
  bool success{false};
  uint64_t commit_ts{0};
  Status status;
  
  // 统计
  uint64_t validation_time_us{0};
  uint64_t write_time_us{0};
  
  static LndOccCommitResult Ok(uint64_t ts) {
    return LndOccCommitResult{true, ts, Status::OK(), 0, 0};
  }
  
  static LndOccCommitResult Error(const Status& s) {
    return LndOccCommitResult{false, 0, s, 0, 0};
  }
};

/**
 * @brief 本地事务协调器
 * 
 * 管理单分区事务的生命周期，无需分布式协调
 */
class LocalTransactionCoordinator {
 public:
  LocalTransactionCoordinator(
      PartitionID partition_id,
      VSLMemTable* memtable,
      WalWriter* wal_writer,
      TwcdEngine* twcd_engine);
  
  ~LocalTransactionCoordinator();
  
  // 禁止拷贝
  LocalTransactionCoordinator(const LocalTransactionCoordinator&) = delete;
  LocalTransactionCoordinator& operator=(const LocalTransactionCoordinator&) = delete;
  
  // ==================== 单分区事务 API ====================
  
  /**
   * @brief 开始本地事务
   * 
   * 分配时间戳，注册到 TW-CD
   */
  Status BeginTransaction(DistributedTxnContext* ctx);
  
  /**
   * @brief 执行本地事务（自动重试）
   * 
   * 模板方法，自动处理 OCC Conflict 重试
   */
  template<typename Func>
  LndOccCommitResult Execute(Func&& func, const DistributedTxnOptions& options = {});
  
  /**
   * @brief 验证阶段（OCC Phase 1）
   * 
   * 使用 TW-CD 进行时序窗口冲突检测
   */
  Status Validate(DistributedTxnContext* ctx);
  
  /**
   * @brief 提交阶段（OCC Phase 2）
   * 
   * 利用 MemTable 切换的原子性
   */
  LndOccCommitResult Commit(DistributedTxnContext* ctx);
  
  /**
   * @brief 回滚事务
   */
  Status Abort(DistributedTxnContext* ctx, const std::string& reason);
  
  // ==================== 统计 ====================
  
  struct Stats {
    uint64_t total_txns{0};
    uint64_t committed_txns{0};
    uint64_t aborted_txns{0};
    uint64_t conflict_retries{0};
    double avg_latency_ms{0.0};
  };
  
  Stats GetStats() const;
  void ResetStats();
  
 private:
  PartitionID partition_id_;
  VSLMemTable* memtable_;
  WalWriter* wal_writer_;
  TwcdEngine* twcd_engine_;
  
  // 统计
  std::atomic<uint64_t> total_txns_{0};
  std::atomic<uint64_t> committed_txns_{0};
  std::atomic<uint64_t> aborted_txns_{0};
  std::atomic<uint64_t> conflict_retries_{0};
  std::atomic<uint64_t> total_latency_us_{0};
};

/**
 * @brief LND-OCC 引擎
 * 
 * 管理所有分区的本地事务协调器
 */
class LndOccEngine {
 public:
  explicit LndOccEngine(const DTxConfig& config);
  ~LndOccEngine();
  
  // 初始化
  Status Initialize(
      PartitionManager* partition_manager,
      const std::unordered_map<PartitionID, std::pair<VSLMemTable*, WalWriter*>>& partition_stores);
  
  // ==================== 分层事务提交 ====================
  
  /**
   * @brief Layer 1: 单分区事务提交（无协调）
   * 
   * 核心优化：利用 LSM-Tree 不可变性，无需 2PC
   */
  LndOccCommitResult SinglePartitionCommit(DistributedTxnContext* ctx);
  
  /**
   * @brief Layer 2: 同时序范围跨分区提交（轻量协调）
   * 
   * 仅验证时序窗口，不锁定数据
   */
  LndOccCommitResult SameTemporalRangeCommit(
      const std::vector<PartitionID>& participants,
      DistributedTxnContext* ctx);
  
  /**
   * @brief Layer 3: 跨时序范围事务（完整 2PC）
   * 
   * 回退到传统 2PC
   */
  LndOccCommitResult FullTwoPhaseCommit(
      const std::vector<PartitionID>& participants,
      DistributedTxnContext* ctx);
  

  
  // ==================== 辅助方法 ====================
  
  // 获取分区的本地协调器
  LocalTransactionCoordinator* GetCoordinator(PartitionID pid);
  
  // 设置状态管理器（用于 2PC）
  void SetTransactionStateManager(::cedar::TransactionStateManager* manager) {
    txn_state_manager_ = manager;
  }
  
  // 设置 StorageClient 获取函数（用于 2PC）
  using StorageClientGetter = std::function<StorageClient*(PartitionID)>;
  void SetStorageClientGetter(StorageClientGetter getter) {
    storage_client_getter_ = getter;
  }
  
  // 获取分区的 StorageClient
  StorageClient* GetStorageClient(PartitionID pid) {
    return storage_client_getter_ ? storage_client_getter_(pid) : nullptr;
  }
  
  // 判断事务类型
  TxnType ClassifyTransaction(const DistributedTxnContext* ctx);
  
  // 批量提交优化（用于确定性事务）
  std::vector<LndOccCommitResult> BatchCommit(
      const std::vector<DistributedTxnContext*>& ctxs);
  
  // ==================== 统计 ====================
  
  struct Stats {
    uint64_t single_partition_commits{0};
    uint64_t same_range_commits{0};
    uint64_t full_2pc_commits{0};
    uint64_t total_commits{0};
    double coordination_ratio{0.0};  // 需要协调的事务比例
  };
  
  Stats GetStats() const;
  
 private:
  DTxConfig config_;
  PartitionManager* partition_manager_{nullptr};
  std::unique_ptr<TwcdEngine> owned_twcd_engine_;
  TwcdEngine* twcd_engine_{nullptr};
  
  // 分区 -> 本地协调器
  mutable std::shared_mutex coordinators_mutex_;
  std::unordered_map<PartitionID, std::unique_ptr<LocalTransactionCoordinator>> coordinators_;
  
  // 2PC 支持
  ::cedar::TransactionStateManager* txn_state_manager_{nullptr};
  StorageClientGetter storage_client_getter_;
  
  // 统计
  std::atomic<uint64_t> single_partition_commits_{0};
  std::atomic<uint64_t> same_range_commits_{0};
  std::atomic<uint64_t> full_2pc_commits_{0};
};

/**
 * @brief Zone-Columnar 感知的事务分组
 * 
 * 按 Zone 分组写入，优化 SST 压缩
 */
class ZoneAwareWriteGrouper {
 public:
  // Zone ID 定义
  enum class ZoneID : uint8_t {
    kTopology = 0,    // Zone 0,2: Entity IDs, Target IDs
    kTemporal = 1,    // Zone 1: Timestamps
    kMetadata = 2,    // Zone 3: Metadata
    kProperty = 3,    // Zone 4: Values
  };
  
  // 按 Zone 分组 Key
  static std::map<ZoneID, std::vector<CedarKey>> GroupByZone(
      const std::vector<CedarKey>& keys);
  
  // 检查 Key 属于哪个 Zone
  static ZoneID GetZoneForKey(const CedarKey& key);
};

// 模板实现
template<typename Func>
LndOccCommitResult LocalTransactionCoordinator::Execute(
    Func&& func, const DistributedTxnOptions& options) {
  
  auto start_time = std::chrono::steady_clock::now();
  
  for (uint32_t attempt = 0; attempt <= options.max_retries; ++attempt) {
    if (attempt > 0) {
      ++conflict_retries_;
    }
    
    // 创建事务上下文
    DistributedTxnContext ctx;
    ctx.SetTxnID(GenerateTxnIDInline());
    ctx.SetStartTimestamp(GenerateTimestampInline());
    ctx.SetTemporalWindow(options.temporal_window);
    ctx.SetType(TxnType::kSinglePartition);
    
    // 开始事务
    auto status = BeginTransaction(&ctx);
    if (!status.ok()) {
      return LndOccCommitResult::Error(status);
    }
    
    try {
      // 执行用户逻辑
      bool success = func(&ctx);
      
      if (!success) {
        Abort(&ctx, "User logic returned false");
        return LndOccCommitResult::Error(
            Status::IOError("Execute", "User logic failed"));
      }
      
      // 提交
      auto result = Commit(&ctx);
      
      if (result.success) {
        auto end_time = std::chrono::steady_clock::now();
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
            end_time - start_time);
        total_latency_us_ += latency.count();
        
        return result;
      }
      
      // 冲突，重试
      if (attempt < options.max_retries) {
        std::this_thread::sleep_for(
            ::cedar::occ_detail::SaturatingExponentialBackoff(
                options.retry_base_delay_ms, attempt));
      }
      
    } catch (const std::exception& e) {
      Abort(&ctx, e.what());
      return LndOccCommitResult::Error(Status::IOError("Execute", e.what()));
    }
  }
  
  return LndOccCommitResult::Error(Status::IOError("Execute", "Max retries exceeded"));
}

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_LSM_NATIVE_OCC_H_
