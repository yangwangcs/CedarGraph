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

#include "cedar/dtx/transaction_recovery_manager.h"


namespace cedar {

TransactionRecoveryManager::TransactionRecoveryManager() = default;

TransactionRecoveryManager::~TransactionRecoveryManager() {
  Shutdown();
}

Status TransactionRecoveryManager::Initialize(TransactionStateManager* state_manager) {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Already initialized");
  }
  {
    std::lock_guard<std::mutex> lock(deps_mutex_);
    state_manager_ = state_manager;
  }
  
  recovery_thread_ = std::thread(&TransactionRecoveryManager::RecoveryLoop, this);
  
  // Recover any pending transactions from previous sessions
  return RecoverAllPendingTransactions();
}

void TransactionRecoveryManager::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  cv_.notify_all();
  
  try {
    if (recovery_thread_.joinable()) {
      recovery_thread_.join();
    }
  } catch (...) {
    std::cerr << "[RecoveryManager] Recovery thread join exception" << std::endl;
  }
}

void TransactionRecoveryManager::SetRpcClient(
    std::shared_ptr<dtx::DTXRpcClient> rpc_client) {
  std::lock_guard<std::mutex> lock(deps_mutex_);
  rpc_client_ = std::move(rpc_client);
}

void TransactionRecoveryManager::SetPartitionNodeMap(
    const std::unordered_map<dtx::PartitionID, dtx::NodeID>& mapping) {
  std::lock_guard<std::mutex> lock(deps_mutex_);
  partition_node_map_ = mapping;
}

void TransactionRecoveryManager::SetPartitionResolver(
    std::function<dtx::NodeID(dtx::PartitionID)> resolver) {
  std::lock_guard<std::mutex> lock(deps_mutex_);
  partition_resolver_ = std::move(resolver);
}

void TransactionRecoveryManager::SetDecisionLogLoader(DecisionLogLoader loader) {
  std::lock_guard<std::mutex> lock(decision_log_loader_mutex_);
  decision_log_loader_ = std::move(loader);
}

RecoveryResult TransactionRecoveryManager::StartRecovery(dtx::TxnID txn_id) {
  RecoveryResult result;
  
  if (!state_manager_) {
    result.reason = "State manager not initialized";
    return result;
  }
  
  auto record_opt = state_manager_->GetTransaction(txn_id);
  if (!record_opt) {
    result.reason = "Transaction not found";
    return result;
  }
  
  const auto& record = *record_opt;
  
  {
    std::lock_guard<std::mutex> lock(decision_log_loader_mutex_);
    if (decision_log_loader_) {
      std::vector<dtx::PartitionID> participants;
      ::cedar::Timestamp commit_ts;
      Status s = decision_log_loader_(txn_id, &participants, &commit_ts);
      if (!s.ok()) {
        std::cerr << "[RecoveryManager] Decision log not available for txn=" << txn_id
                  << ": " << s.ToString() << ", falling back to heuristic" << std::endl;
      } else {
        std::cerr << "[RecoveryManager] Decision log found for txn=" << txn_id
                  << ", driving commit to " << participants.size()
                  << " participants" << std::endl;
        result.success = true;
        result.recommended_action = RecoveryAction::kCommit;
        result.pending_participants = std::move(participants);
        result.commit_ts = commit_ts;
        return result;
      }
    }
  }
  
  // Determine recovery action based on transaction state
  switch (record.state) {
    case TxnState::kCommitted:
      result.success = true;
      result.recommended_action = RecoveryAction::kNone;
      result.reason = "Transaction already committed";
      break;
      
    case TxnState::kAborted:
      result.success = true;
      result.recommended_action = RecoveryAction::kNone;
      result.reason = "Transaction already aborted";
      break;
      
    case TxnState::kPrepared:
      // Transaction is prepared but not committed/aborted
      // We are the coordinator - should commit
      result.recommended_action = RecoveryAction::kCommit;
      result.reason = "Transaction prepared but not committed";
      break;
      
    case TxnState::kPreparing:
      // Prepare phase incomplete
      result.recommended_action = RecoveryAction::kAbort;
      result.reason = "Prepare phase incomplete";
      break;
      
    case TxnState::kCommitting:
      // Commit phase incomplete - try to complete
      result.recommended_action = RecoveryAction::kCommit;
      result.reason = "Commit phase incomplete";
      for (const auto& [pid, ps] : record.participant_states) {
        if (ps.state != ParticipantState::State::kCommitted) {
          result.pending_participants.push_back(pid);
        }
      }
      break;
      
    case TxnState::kAborting:
      // Abort phase incomplete - try to complete
      result.recommended_action = RecoveryAction::kAbort;
      result.reason = "Abort phase incomplete";
      break;
      
    case TxnState::kUnknown:
      // State unknown - use heuristic
      result.recommended_action = DecideHeuristicAction(record);
      result.reason = "Transaction state unknown, using heuristic";
      break;
      
    default:
      result.recommended_action = RecoveryAction::kAbort;
      result.reason = "Unknown transaction state";
      break;
  }
  
  return result;
}

Status TransactionRecoveryManager::RecoverAllPendingTransactions() {
  if (!state_manager_) {
    return Status::OK();  // Nothing to recover
  }
  
  auto pending = state_manager_->GetPendingTransactions();
  
  for (const auto& record : pending) {
    std::lock_guard<std::mutex> lock(mutex_);
    recovery_queue_.push(record.txn_id);
  }
  
  cv_.notify_one();
  return Status::OK();
}

RecoveryAction TransactionRecoveryManager::DecideHeuristicAction(
    const TransactionRecord& record) {
  // SAFETY FIX: Remove unsafe majority heuristic.
  // Majority-based commit violates atomicity guarantees - a minority of
  // unprepared participants would be left in inconsistent state.
  // For unknown state transactions, the safe default is to abort.
  // Only transactions explicitly in kPrepared or kCommitting state
  // should be committed during recovery (handled in StartRecovery).
  (void)record;
  return RecoveryAction::kAbort;
}

Status TransactionRecoveryManager::ApplyRecoveryAction(dtx::TxnID txn_id, 
                                                        RecoveryAction action) {
  if (!state_manager_) {
    return Status::IOError("TransactionRecoveryManager", "no state manager");
  }
  
  auto record_opt = state_manager_->GetTransaction(txn_id);
  if (!record_opt.has_value()) {
    return Status::NotFound("TransactionRecoveryManager", "txn record not found");
  }
  
  const auto& record = record_opt.value();
  
  switch (action) {
    case RecoveryAction::kCommit:
      return RecoverAsCoordinator(txn_id, record);
      
    case RecoveryAction::kAbort:
      return RecoverAsParticipant(txn_id, record);
      
    case RecoveryAction::kInquire: {
      std::lock_guard<std::mutex> lock(deps_mutex_);
      if (!record.participants.empty()) {
        auto node_it = partition_node_map_.find(record.participants[0]);
        if (node_it != partition_node_map_.end()) {
          return InquireParticipant(record.participants[0], txn_id);
        }
      }
      return Status::InvalidArgument("TransactionRecoveryManager", "no participants to inquire");
    }
      
    default:
      return Status::OK();
  }
}

void TransactionRecoveryManager::OnTransactionTimeout(dtx::TxnID txn_id) {
  // Add to recovery queue
  {
    std::lock_guard<std::mutex> lock(mutex_);
    recovery_queue_.push(txn_id);
  }
  cv_.notify_one();
}

void TransactionRecoveryManager::OnParticipantTimeout(dtx::TxnID txn_id, 
                                                       dtx::PartitionID pid) {
  // Mark participant as failed
  if (state_manager_) {
    state_manager_->UpdateParticipantState(
        txn_id, pid, ParticipantState::State::kFailed,
        "Timeout");
  }
  
  // Add to recovery queue
  {
    std::lock_guard<std::mutex> lock(mutex_);
    recovery_queue_.push(txn_id);
  }
  cv_.notify_one();
}

void TransactionRecoveryManager::OnOperationRetry(const PendingOperation& op) {
  (void)op;
  // Retry logic is handled by the caller / RPC client layer
}

void TransactionRecoveryManager::RecoveryLoop() {
  while (running_) {
    try {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] {
        return !running_ || !recovery_queue_.empty();
      });
      
      if (!running_) break;
      
      if (recovery_queue_.empty()) continue;
      
      dtx::TxnID txn_id = recovery_queue_.front();
      recovery_queue_.pop();
      lock.unlock();
      
      // Perform recovery
      auto result = StartRecovery(txn_id);
      if (result.recommended_action != RecoveryAction::kNone) {
        Status s = ApplyRecoveryAction(txn_id, result.recommended_action);
        if (!s.ok()) {
          // Track retry count with backoff
          size_t retry_count = 0;
          {
            std::lock_guard<std::mutex> relock(retry_count_mutex_);
            retry_count = ++retry_counts_[txn_id];
          }
          
          constexpr size_t kMaxRetries = 10;
          if (retry_count >= kMaxRetries) {
            std::cerr << "[RecoveryManager] Recovery failed for txn=" << txn_id
                      << " after " << kMaxRetries << " retries, giving up"
                      << std::endl;
            std::lock_guard<std::mutex> relock(retry_count_mutex_);
            retry_counts_.erase(txn_id);
          } else {
            std::cerr << "[RecoveryManager] Recovery failed for txn=" << txn_id
                      << ", action=" << static_cast<int>(result.recommended_action)
                      << ", error=" << s.ToString()
                      << ", retry=" << retry_count << "/" << kMaxRetries
                      << std::endl;
            // Exponential backoff: 100ms, 200ms, 400ms, ...
            auto backoff = std::chrono::milliseconds(100 * (1 << std::min(retry_count, size_t(6))));
            {
              std::lock_guard<std::mutex> relock(mutex_);
              recovery_queue_.push(txn_id);
            }
            {
              std::unique_lock<std::mutex> relock(mutex_);
              cv_.wait_for(relock, backoff, [this] { return !running_.load(); });
            }
            if (!running_) break;
            cv_.notify_one();
          }
        } else {
          // Success - clear retry count
          std::lock_guard<std::mutex> relock(retry_count_mutex_);
          retry_counts_.erase(txn_id);
        }
      }
    } catch (...) {
      std::cerr << "[RecoveryManager] Recovery loop exception" << std::endl;
    }
  }
}

Status TransactionRecoveryManager::RecoverAsCoordinator(
    dtx::TxnID txn_id,
    const TransactionRecord& record) {
  // Recursion guard: prevent infinite recursion
  {
    std::lock_guard<std::mutex> lock(recovering_mutex_);
    if (recovering_txns_.count(txn_id)) {
      // Already recovering this transaction - avoid recursive loop
      // Just send commit directly to pending participants
      std::vector<dtx::PartitionID> pending;
      for (const auto& [pid, ps] : record.participant_states) {
        if (ps.state != ParticipantState::State::kCommitted) {
          pending.push_back(pid);
        }
      }
      if (pending.empty()) {
        if (state_manager_) {
          state_manager_->UpdateState(txn_id, TxnState::kCommitted);
        }
        return Status::OK();
      }
      auto status = SendCommitToParticipants(txn_id, pending, record.commit_ts);
      if (status.ok() && state_manager_) {
        state_manager_->UpdateState(txn_id, TxnState::kCommitted);
      }
      return status;
    }
    recovering_txns_.insert(txn_id);
  }
  // Ensure we remove from recovering set on scope exit
  struct Guard {
    std::set<dtx::TxnID>& set;
    dtx::TxnID id;
    std::mutex& mtx;
    ~Guard() { std::lock_guard<std::mutex> l(mtx); set.erase(id); }
  } guard{recovering_txns_, txn_id, recovering_mutex_};

  // Determine which participants still need commit
  std::vector<dtx::PartitionID> pending;
  for (const auto& [pid, ps] : record.participant_states) {
    if (ps.state != ParticipantState::State::kCommitted) {
      pending.push_back(pid);
    }
  }
  if (pending.empty()) {
    if (state_manager_) {
      state_manager_->UpdateState(txn_id, TxnState::kCommitted);
    }
    return Status::OK();
  }

  auto status = SendCommitToParticipants(txn_id, pending, record.commit_ts);
  if (status.ok() && state_manager_) {
    state_manager_->UpdateState(txn_id, TxnState::kCommitted);
  }
  return status;
}

Status TransactionRecoveryManager::RecoverAsParticipant(
    dtx::TxnID txn_id, 
    const TransactionRecord& record) {
  auto status = SendAbortToParticipants(txn_id, record.participants);
  if (status.ok() && state_manager_) {
    state_manager_->UpdateState(txn_id, TxnState::kAborted);
  }
  return status;
}

Status TransactionRecoveryManager::SendCommitToParticipants(
    dtx::TxnID txn_id,
    const std::vector<dtx::PartitionID>& participants,
    Timestamp commit_ts) {
  std::shared_ptr<dtx::DTXRpcClient> client;
  std::unordered_map<dtx::PartitionID, dtx::NodeID> node_map;
  std::function<dtx::NodeID(dtx::PartitionID)> resolver;
  {
    std::lock_guard<std::mutex> lock(deps_mutex_);
    client = rpc_client_;
    node_map = partition_node_map_;
    resolver = partition_resolver_;
  }
  if (!client) {
    return Status::IOError("TransactionRecoveryManager", "no RPC client");
  }

  bool all_success = true;
  std::string last_error;

  for (dtx::PartitionID pid : participants) {
    dtx::NodeID node_id = dtx::kInvalidNodeID;
    auto node_it = node_map.find(pid);
    if (node_it != node_map.end()) {
      node_id = node_it->second;
    } else if (resolver) {
      node_id = resolver(pid);
    }
    if (node_id == dtx::kInvalidNodeID) {
      all_success = false;
      last_error = "no node mapping for partition " + std::to_string(pid);
      continue;  // Try remaining participants
    }
    cedar::dtx::CommitResponse response;
    auto status = client->Commit(node_id, std::to_string(txn_id), "", commit_ts.value(), &response);
    if (!status.ok()) {
      all_success = false;
      last_error = status.ToString();
      continue;  // Try remaining participants
    }
  }
  
  if (!all_success) {
    return Status::IOError("TransactionRecoveryManager",
        "Partial commit during recovery: " + last_error);
  }
  return Status::OK();
}

Status TransactionRecoveryManager::SendAbortToParticipants(
    dtx::TxnID txn_id, 
    const std::vector<dtx::PartitionID>& participants) {
  std::shared_ptr<dtx::DTXRpcClient> client;
  std::unordered_map<dtx::PartitionID, dtx::NodeID> node_map;
  std::function<dtx::NodeID(dtx::PartitionID)> resolver;
  {
    std::lock_guard<std::mutex> lock(deps_mutex_);
    client = rpc_client_;
    node_map = partition_node_map_;
    resolver = partition_resolver_;
  }
  if (!client) {
    return Status::IOError("TransactionRecoveryManager", "no RPC client");
  }
  
  bool all_success = true;
  std::string last_error;
  
  for (dtx::PartitionID pid : participants) {
    dtx::NodeID node_id = dtx::kInvalidNodeID;
    auto node_it = node_map.find(pid);
    if (node_it != node_map.end()) {
      node_id = node_it->second;
    } else if (resolver) {
      node_id = resolver(pid);
    }
    if (node_id == dtx::kInvalidNodeID) {
      all_success = false;
      last_error = "no node mapping for partition " + std::to_string(pid);
      continue;  // Try remaining participants
    }
    cedar::dtx::AbortResponse response;
    auto status = client->Abort(node_id, std::to_string(txn_id), "", "recovery", &response);
    if (!status.ok()) {
      all_success = false;
      last_error = status.ToString();
      continue;  // Try remaining participants
    }
  }
  
  if (!all_success) {
    return Status::IOError("TransactionRecoveryManager",
        "Partial abort during recovery: " + last_error);
  }
  return Status::OK();
}

Status TransactionRecoveryManager::InquireParticipant(dtx::PartitionID pid, 
                                                       dtx::TxnID txn_id) {
  std::shared_ptr<dtx::DTXRpcClient> client;
  std::unordered_map<dtx::PartitionID, dtx::NodeID> node_map;
  std::function<dtx::NodeID(dtx::PartitionID)> resolver;
  {
    std::lock_guard<std::mutex> lock(deps_mutex_);
    client = rpc_client_;
    node_map = partition_node_map_;
    resolver = partition_resolver_;
  }
  if (!client) {
    return Status::IOError("TransactionRecoveryManager", "no RPC client");
  }
  
  dtx::NodeID node_id = dtx::kInvalidNodeID;
  auto node_it = node_map.find(pid);
  if (node_it != node_map.end()) {
    node_id = node_it->second;
  } else if (resolver) {
    node_id = resolver(pid);
  }
  if (node_id == dtx::kInvalidNodeID) {
    return Status::NotFound("TransactionRecoveryManager", "no node mapping for partition " + std::to_string(pid));
  }
  
  cedar::dtx::InquireResponse response;
  auto status = client->Inquire(node_id, std::to_string(txn_id), &response);
  return status;
}

}  // namespace cedar
