// Copyright 2025 The Cedar Authors
//
// Test: TransactionRecoveryManager decision logic for various txn states.

#include <gtest/gtest.h>
#include <filesystem>
#include "cedar/dtx/transaction_recovery_manager.h"
#include "cedar/dtx/transaction_state.h"
#include "cedar/dtx/dtx_rpc_client.h"

using namespace cedar;

TEST(TransactionRecoveryManager, PreparedTxnRecommendsCommit) {
  std::filesystem::remove_all("/tmp/test_2pc_recovery_prepared");
  TransactionStateManager state_mgr;
  state_mgr.Initialize("/tmp/test_2pc_recovery_prepared");

  TransactionRecoveryManager rm;
  rm.Initialize(&state_mgr);

  dtx::TxnID txn_id = 42;
  ASSERT_TRUE(state_mgr.CreateTransaction(txn_id, {1, 2, 3}).ok());
  ASSERT_TRUE(state_mgr.UpdateState(txn_id, TxnState::kPrepared).ok());

  auto result = rm.StartRecovery(txn_id);
  // success=false means recovery is needed; action tells us what to do
  EXPECT_EQ(result.recommended_action, RecoveryAction::kCommit);
}

TEST(TransactionRecoveryManager, PreparingTxnRecommendsAbort) {
  std::filesystem::remove_all("/tmp/test_2pc_recovery_preparing");
  TransactionStateManager state_mgr;
  state_mgr.Initialize("/tmp/test_2pc_recovery_preparing");

  TransactionRecoveryManager rm;
  rm.Initialize(&state_mgr);

  dtx::TxnID txn_id = 43;
  ASSERT_TRUE(state_mgr.CreateTransaction(txn_id, {1, 2}).ok());
  ASSERT_TRUE(state_mgr.UpdateState(txn_id, TxnState::kPreparing).ok());

  auto result = rm.StartRecovery(txn_id);
  EXPECT_EQ(result.recommended_action, RecoveryAction::kAbort);
}

TEST(TransactionRecoveryManager, CommittedTxnRecommendsNone) {
  std::filesystem::remove_all("/tmp/test_2pc_recovery_committed");
  TransactionStateManager state_mgr;
  state_mgr.Initialize("/tmp/test_2pc_recovery_committed");

  TransactionRecoveryManager rm;
  rm.Initialize(&state_mgr);

  dtx::TxnID txn_id = 44;
  ASSERT_TRUE(state_mgr.CreateTransaction(txn_id, {1}).ok());
  ASSERT_TRUE(state_mgr.UpdateState(txn_id, TxnState::kCommitted).ok());

  auto result = rm.StartRecovery(txn_id);
  EXPECT_TRUE(result.success);
  EXPECT_EQ(result.recommended_action, RecoveryAction::kNone);
}

TEST(TransactionRecoveryManager, HeuristicUnknownRecommendsAbort) {
  std::filesystem::remove_all("/tmp/test_2pc_recovery_heuristic");
  TransactionStateManager state_mgr;
  state_mgr.Initialize("/tmp/test_2pc_recovery_heuristic");

  TransactionRecoveryManager rm;
  rm.Initialize(&state_mgr);

  dtx::TxnID txn_id = 45;
  ASSERT_TRUE(state_mgr.CreateTransaction(txn_id, {1, 2, 3}).ok());
  ASSERT_TRUE(state_mgr.UpdateState(txn_id, TxnState::kUnknown).ok());
  ASSERT_TRUE(state_mgr.UpdateParticipantState(txn_id, 1, ParticipantState::State::kPrepared).ok());
  ASSERT_TRUE(state_mgr.UpdateParticipantState(txn_id, 2, ParticipantState::State::kPrepared).ok());
  ASSERT_TRUE(state_mgr.UpdateParticipantState(txn_id, 3, ParticipantState::State::kAborted).ok());

  auto result = rm.StartRecovery(txn_id);
  // SAFETY FIX: heuristic for unknown state always returns kAbort
  EXPECT_EQ(result.recommended_action, RecoveryAction::kAbort);
}

TEST(TransactionRecoveryManager, RecoveryUsesWiredRpcClient) {
  std::filesystem::remove_all("/tmp/test_rpc_wired");
  TransactionStateManager state_mgr;
  state_mgr.Initialize("/tmp/test_rpc_wired");

  TransactionRecoveryManager mgr;
  mgr.Initialize(&state_mgr);

  // Wire a real DTXRpcClient (no actual server needed for this test)
  auto rpc_client = std::make_shared<dtx::DTXRpcClient>(dtx::DTXRpcConfig{});
  mgr.SetRpcClient(rpc_client);

  // Set a partition resolver so the recovery manager can map partitions to nodes
  mgr.SetPartitionResolver([](dtx::PartitionID pid) -> dtx::NodeID {
    return static_cast<dtx::NodeID>(pid);
  });

  dtx::TxnID txn_id = 100;
  ASSERT_TRUE(state_mgr.CreateTransaction(txn_id, {1, 2}).ok());
  ASSERT_TRUE(state_mgr.UpdateState(txn_id, TxnState::kPrepared).ok());

  // ApplyRecoveryAction with kCommit should attempt to use the RPC client.
  // Since no participant was added to the client, it will fail with
  // "Participant not found" rather than "no RPC client".
  auto status = mgr.ApplyRecoveryAction(txn_id, RecoveryAction::kCommit);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("Participant not found"), std::string::npos)
      << "Expected RPC client to be used, but got: " << status.ToString();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
