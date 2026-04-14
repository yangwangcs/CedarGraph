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

#include <iostream>
#include <cassert>
#include <filesystem>
#include <thread>
#include <chrono>

#include "cedar/dtx/transaction_state.h"
#include "cedar/dtx/transaction_timeout_manager.h"
#include "cedar/dtx/transaction_recovery_manager.h"

using namespace cedar;

class TestTimeoutCallback : public TimeoutCallback {
 public:
  void OnTransactionTimeout(dtx::TxnID txn_id) override {
    std::cout << "[Test] Transaction timeout: " << txn_id << std::endl;
    timeout_count++;
  }
  
  void OnParticipantTimeout(dtx::TxnID txn_id, dtx::PartitionID pid) override {
    std::cout << "[Test] Participant timeout: txn=" << txn_id 
              << ", pid=" << pid << std::endl;
    participant_timeout_count++;
  }
  
  void OnOperationRetry(const PendingOperation& op) override {
    std::cout << "[Test] Operation retry: txn=" << op.txn_id 
              << ", type=" << static_cast<int>(op.type) << std::endl;
    retry_count++;
  }
  
  std::atomic<int> timeout_count{0};
  std::atomic<int> participant_timeout_count{0};
  std::atomic<int> retry_count{0};
};

void TestTransactionStateManager() {
  std::cout << "\n=== Testing TransactionStateManager ===" << std::endl;
  
  std::string wal_dir = "/tmp/test_txn_state";
  std::filesystem::remove_all(wal_dir);
  std::filesystem::create_directories(wal_dir);
  
  TransactionStateManager manager;
  Status s = manager.Initialize(wal_dir);
  assert(s.ok());
  
  // Create transaction
  dtx::TxnID txn_id = 1001;
  std::vector<dtx::PartitionID> participants = {1, 2, 3};
  
  s = manager.CreateTransaction(txn_id, participants);
  assert(s.ok());
  
  // Update state
  s = manager.UpdateState(txn_id, TxnState::kPreparing);
  assert(s.ok());
  
  // Update participant states
  for (const auto& pid : participants) {
    s = manager.UpdateParticipantState(txn_id, pid, 
                                       ParticipantState::State::kPrepared);
    assert(s.ok());
  }
  
  s = manager.UpdateState(txn_id, TxnState::kPrepared);
  assert(s.ok());
  
  // Query state
  auto record_opt = manager.GetTransaction(txn_id);
  assert(record_opt.has_value());
  assert(record_opt->state == TxnState::kPrepared);
  assert(record_opt->participants.size() == 3);
  
  std::cout << "  Transaction created and updated successfully" << std::endl;
  
  // Test pending transactions
  auto pending = manager.GetPendingTransactions();
  assert(pending.size() == 1);
  
  std::cout << "  Pending transactions: " << pending.size() << std::endl;
  
  manager.Shutdown();
  std::cout << "  ✓ TransactionStateManager test passed" << std::endl;
}

void TestTimeoutManager() {
  std::cout << "\n=== Testing TransactionTimeoutManager ===" << std::endl;
  
  TestTimeoutCallback callback;
  TimeoutConfig config;
  config.prepare_timeout = std::chrono::milliseconds(100);
  config.max_transaction_duration = std::chrono::milliseconds(500);
  
  TransactionTimeoutManager timeout_manager;
  timeout_manager.Initialize(config, &callback);
  
  dtx::TxnID txn_id = 2001;
  std::vector<dtx::PartitionID> participants = {1, 2};
  
  // Register transaction
  timeout_manager.RegisterTransaction(txn_id, participants);
  std::cout << "  Transaction registered" << std::endl;
  
  // Wait for transaction to timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  
  // Check if timed out
  bool timed_out = timeout_manager.IsTimedOut(txn_id);
  std::cout << "  Transaction timed out: " << (timed_out ? "yes" : "no") << std::endl;
  
  // Unregister
  timeout_manager.UnregisterTransaction(txn_id);
  
  std::cout << "  Timeout callbacks: " << callback.timeout_count.load() << std::endl;
  
  timeout_manager.Shutdown();
  std::cout << "  ✓ TransactionTimeoutManager test passed" << std::endl;
}

void TestRecoveryManager() {
  std::cout << "\n=== Testing TransactionRecoveryManager ===" << std::endl;
  
  std::string wal_dir = "/tmp/test_recovery";
  std::filesystem::remove_all(wal_dir);
  std::filesystem::create_directories(wal_dir);
  
  // Create state manager with transactions
  TransactionStateManager state_manager;
  Status s = state_manager.Initialize(wal_dir);
  assert(s.ok());
  
  // Create transactions in various states
  dtx::TxnID txn_prepared = 3001;
  state_manager.CreateTransaction(txn_prepared, {1, 2});
  state_manager.UpdateState(txn_prepared, TxnState::kPrepared);
  
  dtx::TxnID txn_committing = 3002;
  state_manager.CreateTransaction(txn_committing, {1, 2, 3});
  state_manager.UpdateState(txn_committing, TxnState::kCommitting);
  state_manager.UpdateParticipantState(txn_committing, 1, 
                                        ParticipantState::State::kCommitted);
  
  // Test recovery decisions
  TransactionRecoveryManager recovery_manager;
  s = recovery_manager.Initialize(&state_manager);
  assert(s.ok());
  
  auto result1 = recovery_manager.StartRecovery(txn_prepared);
  std::cout << "  Prepared transaction recovery action: " 
            << static_cast<int>(result1.recommended_action) << std::endl;
  assert(result1.recommended_action == RecoveryAction::kCommit);
  
  auto result2 = recovery_manager.StartRecovery(txn_committing);
  std::cout << "  Committing transaction recovery action: " 
            << static_cast<int>(result2.recommended_action) << std::endl;
  assert(result2.recommended_action == RecoveryAction::kCommit);
  assert(result2.pending_participants.size() == 2);  // 2 and 3 not committed
  
  std::cout << "  Pending participants: " << result2.pending_participants.size() << std::endl;
  
  state_manager.Shutdown();
  std::cout << "  ✓ TransactionRecoveryManager test passed" << std::endl;
}

void TestHeuristicDecision() {
  std::cout << "\n=== Testing Heuristic Decision ===" << std::endl;
  
  TransactionRecoveryManager recovery_manager;
  
  // Test case 1: Majority prepared -> commit
  {
    TransactionRecord record;
    record.participant_states[1] = ParticipantState{};
    record.participant_states[1].state = ParticipantState::State::kPrepared;
    record.participant_states[2] = ParticipantState{};
    record.participant_states[2].state = ParticipantState::State::kPrepared;
    record.participant_states[3] = ParticipantState{};
    record.participant_states[3].state = ParticipantState::State::kFailed;
    
    auto action = recovery_manager.DecideHeuristicAction(record);
    std::cout << "  Majority prepared -> action: " << static_cast<int>(action) << std::endl;
    assert(action == RecoveryAction::kCommit);
  }
  
  // Test case 2: Majority not prepared -> abort
  {
    TransactionRecord record;
    record.participant_states[1] = ParticipantState{};
    record.participant_states[1].state = ParticipantState::State::kPrepared;
    record.participant_states[2] = ParticipantState{};
    record.participant_states[2].state = ParticipantState::State::kFailed;
    record.participant_states[3] = ParticipantState{};
    record.participant_states[3].state = ParticipantState::State::kFailed;
    
    auto action = recovery_manager.DecideHeuristicAction(record);
    std::cout << "  Majority failed -> action: " << static_cast<int>(action) << std::endl;
    assert(action == RecoveryAction::kAbort);
  }
  
  std::cout << "  ✓ Heuristic decision test passed" << std::endl;
}

int main() {
  std::cout << "=== 2PC Recovery Tests ===" << std::endl;
  
  try {
    TestTransactionStateManager();
    TestTimeoutManager();
    TestRecoveryManager();
    TestHeuristicDecision();
    
    std::cout << "\n=== All Tests Passed! ===" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "Test failed with exception: " << e.what() << std::endl;
    return 1;
  }
}
