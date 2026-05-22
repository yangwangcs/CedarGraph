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
// Optimized 2PC Execution Engine
// =============================================================================
// Production-ready 2PC with Parallel, Pipeline, and Batch optimizations
// Integrates with Raft consensus for high availability
// =============================================================================

#ifndef CEDAR_DTX_OPTIMIZED_2PC_ENGINE_H_
#define CEDAR_DTX_OPTIMIZED_2PC_ENGINE_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/core/threading.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/production_config.h"

namespace cedar {
class TransactionStateManager;
class TransactionRecoveryManager;
class TransactionTimeoutManager;
namespace dtx {

// Forward declarations
class StorageClient;
class RaftGroup;
class DTXRpcClient;

// =============================================================================
// Transaction Context for 2PC
// =============================================================================
struct TransactionContext {
  TxnID txn_id;
  std::vector<::cedar::CedarKey> read_set;
  std::vector<::cedar::CedarKey> write_set;
  Timestamp commit_ts;
  
  // 2PC state
  enum class State {
    kInit = 0,
    kPreparing = 1,
    kPrepared = 2,
    kCommitting = 3,
    kCommitted = 4,
    kAborting = 5,
    kAborted = 6,
  };
  std::atomic<State> state{State::kInit};
  
  // Participant tracking
  std::atomic<int> prepare_acks{0};
  std::atomic<int> prepare_nacks{0};
  std::atomic<int> commit_acks{0};
  std::atomic<int> abort_acks{0};
  
  // Callback for async completion
  std::function<void(Status)> callback;
  
  // Promise for synchronous waiters (e.g., SubmitPipelined)
  std::shared_ptr<std::promise<void>> done_promise;
  
  // Timing
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point prepare_complete_time;
  
  TransactionContext(TxnID id, const std::vector<::cedar::CedarKey>& reads,
                     const std::vector<::cedar::CedarKey>& writes, Timestamp ts)
      : txn_id(id), read_set(reads), write_set(writes), commit_ts(ts) {
    start_time = std::chrono::steady_clock::now();
  }
};

// =============================================================================
// Optimized 2PC Engine
// =============================================================================
class Optimized2PCEngine {
 public:
  // Statistics snapshot (non-atomic, for returning to callers)
  struct StatsSnapshot {
    uint64_t total_transactions{0};
    uint64_t committed_transactions{0};
    uint64_t aborted_transactions{0};
    uint64_t timeout_transactions{0};
    uint64_t latency_sum_us{0};
    uint64_t latency_count{0};
    uint64_t ops_last_second{0};
    double current_throughput{0.0};
    
    double GetAverageLatency() const {
      return latency_count > 0 ? (double)latency_sum_us / latency_count : 0.0;
    }
  };
  
  // Internal atomic stats
  struct AtomicStats {
    std::atomic<uint64_t> total_transactions{0};
    std::atomic<uint64_t> committed_transactions{0};
    std::atomic<uint64_t> aborted_transactions{0};
    std::atomic<uint64_t> timeout_transactions{0};
    std::atomic<uint64_t> latency_sum_us{0};
    std::atomic<uint64_t> latency_count{0};
    std::atomic<uint64_t> ops_last_second{0};
    std::atomic<double> current_throughput{0.0};
    
    void RecordLatency(uint64_t latency_us) {
      latency_sum_us += latency_us;
      latency_count++;
      ops_last_second++;
    }
    
    double GetAverageLatency() const {
      auto count = latency_count.load();
      return count > 0 ? (double)latency_sum_us.load() / count : 0.0;
    }
    
    StatsSnapshot GetSnapshot() const {
      StatsSnapshot s;
      s.total_transactions = total_transactions.load();
      s.committed_transactions = committed_transactions.load();
      s.aborted_transactions = aborted_transactions.load();
      s.timeout_transactions = timeout_transactions.load();
      s.latency_sum_us = latency_sum_us.load();
      s.latency_count = latency_count.load();
      s.ops_last_second = ops_last_second.load();
      s.current_throughput = current_throughput.load();
      return s;
    }
  };
  
  explicit Optimized2PCEngine(const TwoPCConfig& config);
  ~Optimized2PCEngine();
  
  // Disable copy
  Optimized2PCEngine(const Optimized2PCEngine&) = delete;
  Optimized2PCEngine& operator=(const Optimized2PCEngine&) = delete;
  
  // Initialize with storage clients for each partition
  Status Initialize(const std::vector<std::shared_ptr<StorageClient>>& clients);
  
  // Dynamic client management (for runtime topology changes)
  void AddClient(std::shared_ptr<StorageClient> client);
  void RemoveClient(const std::string& server_address);
  size_t GetClientCount() const;
  
  // Set state manager for transaction persistence
  void SetStateManager(TransactionStateManager* state_manager) { state_manager_ = state_manager; }
  
  // Set recovery manager for coordinator crash recovery
  void SetRecoveryManager(TransactionRecoveryManager* recovery_manager);
  
  // Set timeout manager for transaction timeout tracking
  void SetTimeoutManager(TransactionTimeoutManager* timeout_manager) { timeout_manager_ = timeout_manager; }
  
  // Shutdown
  void Shutdown() noexcept;
  
  // ==========================================================================
  // Transaction Execution APIs
  // ==========================================================================
  
  // Synchronous 2PC - blocks until complete
  Status Execute2PC(TxnID txn_id, const std::vector<::cedar::CedarKey>& read_set,
                    const std::vector<::cedar::CedarKey>& write_set, Timestamp commit_ts);
  
  // Asynchronous 2PC - returns immediately, callback on completion
  void Execute2PCAsync(TxnID txn_id, const std::vector<::cedar::CedarKey>& read_set,
                       const std::vector<::cedar::CedarKey>& write_set, Timestamp commit_ts,
                       std::function<void(Status)> callback);
  
  // Batch 2PC - execute multiple transactions as a batch
  std::vector<Status> Execute2PCBatch(
      const std::vector<std::tuple<TxnID, std::vector<::cedar::CedarKey>, 
                                   std::vector<::cedar::CedarKey>, Timestamp>>& transactions);
  
  // Pipelined 2PC - submit to pipeline, returns immediately
  Status SubmitPipelined(TxnID txn_id, const std::vector<::cedar::CedarKey>& read_set,
                         const std::vector<::cedar::CedarKey>& write_set, Timestamp commit_ts);
  
  // ==========================================================================
  // Adaptive Tuning
  // ==========================================================================
  void EnableAdaptiveTuning(bool enable);
  void UpdateConfiguration(const TwoPCConfig& config);
  TwoPCConfig GetCurrentConfig() const;
  
  // ==========================================================================
  // Statistics
  // ==========================================================================
  StatsSnapshot GetStats() const {
    return stats_.GetSnapshot();
  }
  void ResetStats();
  
  // Get current throughput (txns/sec)
  double GetCurrentThroughput() const { return stats_.current_throughput.load(); }
  
  // Internal stats for testing
  AtomicStats& GetInternalStats() { return stats_; }
  
 private:
  // Strategy implementations
  Status ExecuteSequential2PC(const std::shared_ptr<TransactionContext>& ctx);
  Status ExecuteParallel2PC(const std::shared_ptr<TransactionContext>& ctx);
  Status ExecutePipelined2PC(const std::shared_ptr<TransactionContext>& ctx);
  Status ExecuteBatched2PC(const std::shared_ptr<TransactionContext>& ctx);
  
  // Pipeline worker
  void PipelineWorkerLoop();
  
  // Adaptive tuning worker
  void AdaptiveTuningLoop();
  void TuneConfiguration();
  
  // Helper methods
  std::vector<std::shared_ptr<StorageClient>> GetParticipants(
      const std::vector<::cedar::CedarKey>& keys);
  std::vector<PartitionID> GetPartitionIDs(
      const std::vector<::cedar::CedarKey>& keys);
  bool WaitForPrepareQuorum(const std::shared_ptr<TransactionContext>& ctx,
                            std::vector<std::future<bool>>& futures);
  bool WaitForCommitQuorum(const std::shared_ptr<TransactionContext>& ctx,
                           std::vector<std::future<bool>>& futures);
  void SyncRecoveryRpcClient();
  
  // Configuration
  TwoPCConfig config_;
  mutable std::mutex config_mutex_;
  
  // Storage clients (one per partition/node)
  std::vector<std::shared_ptr<StorageClient>> clients_;
  mutable std::mutex clients_mutex_;
  
  // Pipeline queue
  std::queue<std::shared_ptr<TransactionContext>> pipeline_queue_;
  std::mutex pipeline_mutex_;
  std::condition_variable pipeline_cv_;
  
  // Batch buffer
  std::vector<std::shared_ptr<TransactionContext>> batch_buffer_;
  std::mutex batch_mutex_;
  std::condition_variable batch_cv_;
  
  // Thread pool for parallel 2PC RPCs
  std::unique_ptr<ThreadPool> thread_pool_;
  
  // Worker threads
  std::vector<std::thread> worker_threads_;
  std::thread pipeline_thread_;
  std::thread tuning_thread_;
  
  // Task queue for async execution
  std::queue<std::function<void()>> task_queue_;
  std::mutex task_mutex_;
  std::condition_variable task_cv_;
  
  // State
  std::atomic<bool> running_{false};
  std::atomic<bool> shutdown_{false};
  
  // Transaction state manager for persistence
  TransactionStateManager* state_manager_ = nullptr;
  
  // Transaction recovery manager for coordinator crash recovery
  TransactionRecoveryManager* recovery_manager_ = nullptr;
  
  // DTX RPC client for recovery manager
  std::shared_ptr<dtx::DTXRpcClient> dtx_rpc_client_;
  
  // Transaction timeout manager for timeout tracking
  TransactionTimeoutManager* timeout_manager_ = nullptr;
  
  // Statistics
  AtomicStats stats_;
  mutable std::mutex stats_mutex_;
  
  // Commit decision log for recovery
  struct CommitDecision {
    TxnID txn_id{0};
    Timestamp commit_ts{0};
    std::vector<PartitionID> participants;
    bool decision_made{false};
  };
  Status PersistCommitDecision(const CommitDecision& decision);
  Status LoadCommitDecision(TxnID txn_id, CommitDecision* out);
  std::string DecisionLogPath(TxnID txn_id) const;

  // Adaptive tuning state
  struct TuningState {
    double last_throughput = 0;
    double last_latency = 0;
    int stable_rounds = 0;
    int current_batch_size = 8;
    int current_pipeline_depth = 4;
  } tuning_state_;
  mutable std::mutex tuning_mutex_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_OPTIMIZED_2PC_ENGINE_H_
