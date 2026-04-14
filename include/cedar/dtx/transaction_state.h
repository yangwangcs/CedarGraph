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

#ifndef CEDAR_DTX_TRANSACTION_STATE_H_
#define CEDAR_DTX_TRANSACTION_STATE_H_

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

#include "cedar/dtx/types.h"
#include "cedar/core/status.h"

namespace cedar {

// Transaction states for 2PC
enum class TxnState {
  kActive = 0,        // Transaction started
  kPreparing = 1,     // Prepare phase in progress
  kPrepared = 2,      // All participants prepared
  kCommitting = 3,    // Commit phase in progress
  kCommitted = 4,     // Transaction committed
  kAborting = 5,      // Abort phase in progress
  kAborted = 6,       // Transaction aborted
  kUnknown = 7,       // State unknown (after recovery)
};

// Participant state in 2PC
struct ParticipantState {
  dtx::PartitionID partition_id;
  std::string address;
  enum class State {
    kUnknown,
    kPreparing,
    kPrepared,
    kFailed,
    kCommitted,
    kAborted
  } state = State::kUnknown;
  std::chrono::steady_clock::time_point last_update;
  std::optional<std::string> error_msg;
};

// Transaction state record
struct TransactionRecord {
  dtx::TxnID txn_id;
  TxnState state;
  Timestamp commit_ts;
  std::vector<dtx::PartitionID> participants;
  std::map<dtx::PartitionID, ParticipantState> participant_states;
  std::chrono::steady_clock::time_point created_at;
  std::chrono::steady_clock::time_point updated_at;
  std::optional<std::chrono::steady_clock::time_point> timeout_at;
  int retry_count = 0;
  
  // Serialization for WAL
  void Serialize(std::string* output) const;
  static Status Deserialize(const std::string& input, TransactionRecord* record);
};

// Transaction state manager
class TransactionStateManager {
 public:
  TransactionStateManager();
  ~TransactionStateManager();
  
  // Initialize with WAL for persistence
  Status Initialize(const std::string& wal_dir);
  void Shutdown();
  
  // Transaction lifecycle
  Status CreateTransaction(dtx::TxnID txn_id, const std::vector<dtx::PartitionID>& participants);
  Status UpdateState(dtx::TxnID txn_id, TxnState new_state);
  Status UpdateParticipantState(dtx::TxnID txn_id, dtx::PartitionID pid, 
                                ParticipantState::State state,
                                const std::optional<std::string>& error = std::nullopt);
  
  // Query state
  std::optional<TransactionRecord> GetTransaction(dtx::TxnID txn_id) const;
  std::vector<TransactionRecord> GetPendingTransactions() const;
  std::vector<TransactionRecord> GetTransactionsByState(TxnState state) const;
  
  // Timeout management
  std::vector<dtx::TxnID> GetTimedOutTransactions(
      std::chrono::milliseconds timeout) const;
  void SetTransactionTimeout(dtx::TxnID txn_id, std::chrono::milliseconds timeout);
  
  // Recovery
  Status RecoverFromWAL(const std::string& wal_dir);
  
 private:
  mutable std::mutex mutex_;
  std::map<dtx::TxnID, TransactionRecord> transactions_;
  std::atomic<bool> initialized_{false};
  std::atomic<bool> shutdown_{false};
  
  // WAL for persistence
  class TransactionWAL;
  std::unique_ptr<TransactionWAL> wal_;
  
  Status PersistState(const TransactionRecord& record);
};

}  // namespace cedar

#endif  // CEDAR_DTX_TRANSACTION_STATE_H_
