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
// Distributed Transaction - 2PC Protocol
// =============================================================================

#ifndef CEDAR_DTX_DISTRIBUTED_TRANSACTION_H_
#define CEDAR_DTX_DISTRIBUTED_TRANSACTION_H_

#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <atomic>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/hybrid_logical_clock.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Transaction State
// =============================================================================

enum class TxnState : uint8_t {
  kActive = 0,
  kPreparing = 1,
  kPrepared = 2,
  kCommitting = 3,
  kCommitted = 4,
  kAborting = 5,
  kAborted = 6,
};

std::string TxnStateToString(TxnState state);

// =============================================================================
// Transaction Coordinator (TC)
// =============================================================================

class DistributedTxn;

struct Participant {
  NodeID node_id;
  std::string address;
  std::vector<PartitionID> partitions;
  bool prepared = false;
  bool vote = false;
};

class TransactionCoordinator {
 public:
  explicit TransactionCoordinator(NodeID coordinator_id);
  ~TransactionCoordinator();
  
  // Begin new distributed transaction
  StatusOr<std::unique_ptr<DistributedTxn>> BeginTransaction();
  
  // Recover pending transactions after crash
  Status RecoverTransactions();

 private:
  NodeID coordinator_id_;
  std::atomic<uint64_t> txn_counter_{0};
  
  mutable std::mutex active_txns_mutex_;
  std::unordered_map<uint64_t, std::unique_ptr<DistributedTxn>> active_txns_;
};

// =============================================================================
// Distributed Transaction
// =============================================================================

class DistributedTxn {
 public:
  DistributedTxn(uint64_t txn_id, 
                 NodeID coordinator_id,
                 HybridLogicalClock* hlc);
  ~DistributedTxn();
  
  uint64_t GetTxnId() const { return txn_id_; }
  TxnState GetState() const { return state_; }
  Timestamp GetTimestamp() const { return timestamp_; }
  
  // Add participant (storage node)
  Status AddParticipant(const Participant& participant);
  
  // Record read set (for conflict detection)
  void RecordRead(const CedarKey& key, Timestamp read_ts);
  
  // Record write set
  void RecordWrite(const CedarKey& key, const std::string& value);
  
  // Two-Phase Commit
  Status Prepare();           // Phase 1
  Status Commit();            // Phase 2 - commit
  Status Abort();             // Phase 2 - abort
  
  // Check if transaction involves multiple partitions
  bool IsDistributed() const { return participants_.size() > 1; }
  
  // Get participants
  std::vector<Participant> GetParticipants() const;

 private:
  // Send prepare to participant
  Status SendPrepare(const Participant& participant);
  
  // Send commit/abort to participant
  Status SendCommit(const Participant& participant);
  Status SendAbort(const Participant& participant);
  
  // Persist transaction state (for recovery)
  Status PersistState();
  
  uint64_t txn_id_;
  NodeID coordinator_id_;
  HybridLogicalClock* hlc_;
  
  std::atomic<TxnState> state_{TxnState::kActive};
  Timestamp timestamp_;
  
  mutable std::mutex mutex_;
  std::vector<Participant> participants_;
  
  // Read/Write sets
  struct ReadRecord {
    CedarKey key;
    Timestamp read_ts;
  };
  std::vector<ReadRecord> read_set_;
  
  struct WriteRecord {
    CedarKey key;
    std::string value;
  };
  std::vector<WriteRecord> write_set_;
};

// =============================================================================
// Transaction Participant (TP) - runs on storage nodes
// =============================================================================

class TransactionParticipant {
 public:
  explicit TransactionParticipant(NodeID node_id);
  
  // Handle prepare request from coordinator
  Status HandlePrepare(uint64_t txn_id, 
                       const std::vector<DistributedTxn::WriteRecord>& writes,
                       Timestamp timestamp);
  
  // Handle commit/abort from coordinator
  Status HandleCommit(uint64_t txn_id);
  Status HandleAbort(uint64_t txn_id);
  
  // Query prepared transactions (for recovery)
  std::vector<uint64_t> GetPreparedTransactions() const;

 private:
  struct PreparedTxn {
    uint64_t txn_id;
    std::vector<DistributedTxn::WriteRecord> writes;
    Timestamp timestamp;
    std::chrono::steady_clock::time_point prepare_time;
  };
  
  NodeID node_id_;
  mutable std::mutex prepared_mutex_;
  std::unordered_map<uint64_t, PreparedTxn> prepared_txns_;
};

// =============================================================================
// Conflict Detection
// =============================================================================

class ConflictDetector {
 public:
  // Check if transaction conflicts with concurrent transactions
  static bool CheckConflict(const DistributedTxn* txn,
                            const std::vector<ReadRecord>& concurrent_reads,
                            const std::vector<WriteRecord>& concurrent_writes);
  
  // OCC validation
  static Status ValidateOCC(const DistributedTxn* txn);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_DISTRIBUTED_TRANSACTION_H_
