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

#include <chrono>

namespace cedar {

TransactionRecoveryManager::TransactionRecoveryManager() = default;

TransactionRecoveryManager::~TransactionRecoveryManager() {
  Shutdown();
}

Status TransactionRecoveryManager::Initialize(TransactionStateManager* state_manager) {
  state_manager_ = state_manager;
  running_ = true;
  
  recovery_thread_ = std::thread(&TransactionRecoveryManager::RecoveryLoop, this);
  
  // Recover any pending transactions from previous sessions
  return RecoverAllPendingTransactions();
}

void TransactionRecoveryManager::Shutdown() {
  running_ = false;
  cv_.notify_all();
  
  if (recovery_thread_.joinable()) {
    recovery_thread_.join();
  }
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
  // Heuristic: Check if majority of participants are prepared
  size_t prepared_count = 0;
  size_t total_count = record.participant_states.size();
  
  for (const auto& [pid, ps] : record.participant_states) {
    if (ps.state == ParticipantState::State::kPrepared ||
        ps.state == ParticipantState::State::kCommitted) {
      ++prepared_count;
    }
  }
  
  // If majority prepared, commit; otherwise abort
  if (prepared_count > total_count / 2) {
    return RecoveryAction::kCommit;
  } else {
    return RecoveryAction::kAbort;
  }
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
      
    case RecoveryAction::kInquire:
      if (!record.participants.empty()) {
        auto node_it = partition_node_map_.find(record.participants[0]);
        if (node_it != partition_node_map_.end()) {
          return InquireParticipant(record.participants[0], txn_id);
        }
      }
      return Status::InvalidArgument("TransactionRecoveryManager", "no participants to inquire");
      
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
      ApplyRecoveryAction(txn_id, result.recommended_action);
    }
  }
}

Status TransactionRecoveryManager::RecoverAsCoordinator(
    dtx::TxnID txn_id, 
    const TransactionRecord& record) {
  auto status = SendCommitToParticipants(txn_id, record.participants);
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
    const std::vector<dtx::PartitionID>& participants) {
  if (!rpc_client_) {
    return Status::IOError("TransactionRecoveryManager", "no RPC client");
  }
  
  for (dtx::PartitionID pid : participants) {
    auto node_it = partition_node_map_.find(pid);
    if (node_it == partition_node_map_.end()) {
      return Status::NotFound("TransactionRecoveryManager", "no node mapping for partition " + std::to_string(pid));
    }
    cedar::dtx::CommitResponse response;
    auto status = rpc_client_->Commit(node_it->second, std::to_string(txn_id), "", 0, &response);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status TransactionRecoveryManager::SendAbortToParticipants(
    dtx::TxnID txn_id, 
    const std::vector<dtx::PartitionID>& participants) {
  if (!rpc_client_) {
    return Status::IOError("TransactionRecoveryManager", "no RPC client");
  }
  
  for (dtx::PartitionID pid : participants) {
    auto node_it = partition_node_map_.find(pid);
    if (node_it == partition_node_map_.end()) {
      return Status::NotFound("TransactionRecoveryManager", "no node mapping for partition " + std::to_string(pid));
    }
    cedar::dtx::AbortResponse response;
    auto status = rpc_client_->Abort(node_it->second, std::to_string(txn_id), "", "recovery", &response);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status TransactionRecoveryManager::InquireParticipant(dtx::PartitionID pid, 
                                                       dtx::TxnID txn_id) {
  if (!rpc_client_) {
    return Status::IOError("TransactionRecoveryManager", "no RPC client");
  }
  
  auto node_it = partition_node_map_.find(pid);
  if (node_it == partition_node_map_.end()) {
    return Status::NotFound("TransactionRecoveryManager", "no node mapping for partition " + std::to_string(pid));
  }
  
  cedar::dtx::InquireResponse response;
  auto status = rpc_client_->Inquire(node_it->second, std::to_string(txn_id), &response);
  return status;
}

}  // namespace cedar
