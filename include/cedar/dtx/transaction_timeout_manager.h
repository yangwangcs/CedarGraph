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

#ifndef CEDAR_DTX_TRANSACTION_TIMEOUT_MANAGER_H_
#define CEDAR_DTX_TRANSACTION_TIMEOUT_MANAGER_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "cedar/dtx/types.h"

namespace cedar {

// Transaction timeout configuration
struct TimeoutConfig {
  std::chrono::milliseconds prepare_timeout{5000};      // Prepare phase timeout
  std::chrono::milliseconds commit_timeout{10000};      // Commit phase timeout
  std::chrono::milliseconds abort_timeout{5000};        // Abort phase timeout
  std::chrono::milliseconds max_transaction_duration{60000};  // Max total duration
  size_t max_retries = 3;                               // Max retry attempts
  std::chrono::milliseconds retry_interval{1000};       // Interval between retries
};

// Pending operation for retry
struct PendingOperation {
  enum class Type {
    kPrepare,
    kCommit,
    kAbort,
    kRecovery
  };
  
  Type type;
  dtx::TxnID txn_id;
  dtx::PartitionID partition_id;
  std::chrono::steady_clock::time_point next_attempt;
  int retry_count = 0;
  std::string context;  // Additional context for recovery
};

// Timeout callback interface
class TimeoutCallback {
 public:
  virtual ~TimeoutCallback() = default;
  
  // Called when a transaction times out
  virtual void OnTransactionTimeout(dtx::TxnID txn_id) = 0;
  
  // Called when a participant operation times out
  virtual void OnParticipantTimeout(dtx::TxnID txn_id, dtx::PartitionID pid) = 0;
  
  // Called when an operation should be retried
  virtual void OnOperationRetry(const PendingOperation& op) = 0;
};

// Transaction timeout manager
class TransactionTimeoutManager {
 public:
  TransactionTimeoutManager();
  ~TransactionTimeoutManager();
  
  // Initialize with configuration
  void Initialize(const TimeoutConfig& config, TimeoutCallback* callback);
  void Shutdown();
  
  // Register transactions for timeout tracking
  void RegisterTransaction(dtx::TxnID txn_id, 
                           const std::vector<dtx::PartitionID>& participants);
  void UnregisterTransaction(dtx::TxnID txn_id);
  
  // Update transaction state (resets timeouts)
  void UpdateTransactionState(dtx::TxnID txn_id, bool is_preparing);
  
  // Schedule operation retry
  void ScheduleRetry(const PendingOperation& op);
  void CancelRetries(dtx::TxnID txn_id);
  
  // Get pending operations for retry
  std::vector<PendingOperation> GetPendingRetries(size_t max_count);
  
  // Check if transaction has timed out
  bool IsTimedOut(dtx::TxnID txn_id) const;
  
 private:
  void TimeoutCheckLoop();
  void RetryLoop();
  
  struct TransactionTimeoutInfo {
    dtx::TxnID txn_id;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point last_activity;
    bool is_preparing = false;
    std::map<dtx::PartitionID, std::chrono::steady_clock::time_point> participant_timeouts;
  };
  
  TimeoutConfig config_;
  TimeoutCallback* callback_ = nullptr;
  mutable std::mutex callback_mutex_;
  
  mutable std::mutex mutex_;
  std::map<dtx::TxnID, TransactionTimeoutInfo> transactions_;
  
  // Priority queue for retry operations
  struct RetryCompare {
    bool operator()(const PendingOperation& a, const PendingOperation& b) {
      return a.next_attempt > b.next_attempt;
    }
  };
  std::priority_queue<PendingOperation, std::vector<PendingOperation>, RetryCompare> retry_queue_;
  
  std::atomic<bool> running_{false};
  std::thread timeout_thread_;
  std::thread retry_thread_;
  std::condition_variable cv_;
};

}  // namespace cedar

#endif  // CEDAR_DTX_TRANSACTION_TIMEOUT_MANAGER_H_
