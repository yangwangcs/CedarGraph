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

#ifndef CEDAR_DTX_TRANSACTION_RECOVERY_MANAGER_H_
#define CEDAR_DTX_TRANSACTION_RECOVERY_MANAGER_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/transaction_state.h"
#include "cedar/dtx/transaction_timeout_manager.h"
#include "cedar/dtx/dtx_rpc_client.h"

namespace cedar {

// Recovery actions for different scenarios
enum class RecoveryAction {
  kNone,        // No action needed
  kCommit,      // Commit the transaction
  kAbort,       // Abort the transaction
  kInquire,     // Inquire participant for decision
  kWait,        // Wait for coordinator recovery
};

// Recovery result
struct RecoveryResult {
  bool success = false;
  RecoveryAction recommended_action = RecoveryAction::kNone;
  std::string reason;
  std::vector<dtx::PartitionID> pending_participants;
  ::cedar::Timestamp commit_ts;
};

// Transaction recovery manager
class TransactionRecoveryManager : public TimeoutCallback {
 public:
  TransactionRecoveryManager();
  ~TransactionRecoveryManager();
  
  // Initialize with dependencies
  Status Initialize(TransactionStateManager* state_manager);
  void Shutdown();
  
  // Set RPC client and partition routing for recovery
  void SetRpcClient(std::shared_ptr<dtx::DTXRpcClient> rpc_client);
  void SetPartitionNodeMap(const std::unordered_map<dtx::PartitionID, dtx::NodeID>& mapping);
  void SetPartitionResolver(std::function<dtx::NodeID(dtx::PartitionID)> resolver);
  
  using DecisionLogLoader = std::function<Status(dtx::TxnID txn_id,
                                                   std::vector<dtx::PartitionID>* participants,
                                                   ::cedar::Timestamp* commit_ts)>;
  void SetDecisionLogLoader(DecisionLogLoader loader);
  
  // Start recovery for a transaction
  RecoveryResult StartRecovery(dtx::TxnID txn_id);
  
  // Recover all pending transactions (called on startup)
  Status RecoverAllPendingTransactions();
  
  // Heuristic decision for coordinator failure
  RecoveryAction DecideHeuristicAction(const TransactionRecord& record);
  
  // Apply recovery action
  Status ApplyRecoveryAction(dtx::TxnID txn_id, RecoveryAction action);
  
  // TimeoutCallback interface implementation
  void OnTransactionTimeout(dtx::TxnID txn_id) override;
  void OnParticipantTimeout(dtx::TxnID txn_id, dtx::PartitionID pid) override;
  void OnOperationRetry(const PendingOperation& op) override;
  
 private:
  // Recovery procedures
  Status RecoverAsCoordinator(dtx::TxnID txn_id, const TransactionRecord& record);
  Status RecoverAsParticipant(dtx::TxnID txn_id, const TransactionRecord& record);
  
  // Helper methods
  Status SendCommitToParticipants(dtx::TxnID txn_id,
                                   const std::vector<dtx::PartitionID>& participants,
                                   Timestamp commit_ts);
  Status SendAbortToParticipants(dtx::TxnID txn_id,
                                  const std::vector<dtx::PartitionID>& participants);
  Status InquireParticipant(dtx::PartitionID pid, dtx::TxnID txn_id);
  
  // Background recovery thread
  void RecoveryLoop();
  
  TransactionStateManager* state_manager_ = nullptr;
  mutable std::mutex deps_mutex_;
  std::shared_ptr<dtx::DTXRpcClient> rpc_client_;
  std::unordered_map<dtx::PartitionID, dtx::NodeID> partition_node_map_;
  std::function<dtx::NodeID(dtx::PartitionID)> partition_resolver_;
  
  DecisionLogLoader decision_log_loader_;
  mutable std::mutex decision_log_loader_mutex_;
  
  std::atomic<bool> running_{false};
  std::thread recovery_thread_;
  
  mutable std::mutex mutex_;
  std::queue<dtx::TxnID> recovery_queue_;
  std::condition_variable cv_;
};

}  // namespace cedar

#endif  // CEDAR_DTX_TRANSACTION_RECOVERY_MANAGER_H_
