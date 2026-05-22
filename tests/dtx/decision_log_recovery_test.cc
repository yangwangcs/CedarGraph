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

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "cedar/dtx/optimized_2pc_engine.h"
#include "cedar/dtx/production_config.h"
#include "cedar/dtx/transaction_recovery_manager.h"
#include "cedar/dtx/transaction_state.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(DecisionLogRecoveryTest, LoadCommitDecisionReturnsCorrectData) {
  std::string test_dir = "/tmp/cedar_test_decision_log_recovery";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  // Manually write a decision log file for txn_id=42
  {
    std::string path = test_dir + "/txn_42.decision";
    std::ofstream ofs(path, std::ios::binary);
    ASSERT_TRUE(ofs);
    constexpr uint32_t kMagic = 0x44454301;
    constexpr uint32_t kVersion = 1;
    TxnID txn_id = 42;
    Timestamp commit_ts(12345678);
    uint32_t num_parts = 3;
    PartitionID pids[3] = {1, 7, 42};

    ofs.write(reinterpret_cast<const char*>(&kMagic), sizeof(kMagic));
    ofs.write(reinterpret_cast<const char*>(&kVersion), sizeof(kVersion));
    ofs.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    ofs.write(reinterpret_cast<const char*>(&commit_ts), sizeof(commit_ts));
    ofs.write(reinterpret_cast<const char*>(&num_parts), sizeof(num_parts));
    for (uint32_t i = 0; i < num_parts; ++i) {
      ofs.write(reinterpret_cast<const char*>(&pids[i]), sizeof(pids[i]));
    }
    ofs.flush();
    ASSERT_TRUE(ofs);
    ofs.close();
  }

  // Create engine with decision log directory and wire recovery manager
  TwoPCConfig config;
  config.decision_log_dir = test_dir;
  config.enable_adaptive_tuning = false;
  Optimized2PCEngine engine(config);

  TransactionStateManager state_manager;
  ASSERT_TRUE(state_manager.Initialize(test_dir + "/wal").ok());
  ASSERT_TRUE(state_manager.CreateTransaction(42, {1, 7, 42}).ok());
  ASSERT_TRUE(state_manager.UpdateState(42, TxnState::kUnknown).ok());

  TransactionRecoveryManager recovery_manager;
  ASSERT_TRUE(recovery_manager.Initialize(&state_manager).ok());
  engine.SetRecoveryManager(&recovery_manager);

  Status init_status = engine.Initialize({});
  ASSERT_TRUE(init_status.ok()) << init_status.ToString();

  // StartRecovery should read the decision log and recommend commit.
  auto result = recovery_manager.StartRecovery(42);
  EXPECT_TRUE(result.success);
  EXPECT_EQ(result.recommended_action, RecoveryAction::kCommit);
  EXPECT_EQ(result.commit_ts.value(), 12345678u);
  EXPECT_EQ(result.pending_participants.size(), 3u);
  EXPECT_EQ(result.pending_participants[0], 1u);
  EXPECT_EQ(result.pending_participants[1], 7u);
  EXPECT_EQ(result.pending_participants[2], 42u);

  engine.Shutdown();
  recovery_manager.Shutdown();
  state_manager.Shutdown();

  // Cleanup
  std::filesystem::remove_all(test_dir);
}

TEST(DecisionLogRecoveryTest, MissingDecisionLogFallsBackToHeuristic) {
  std::string test_dir = "/tmp/cedar_test_missing_decision_log";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  TwoPCConfig config;
  config.decision_log_dir = test_dir;
  config.enable_adaptive_tuning = false;
  Optimized2PCEngine engine(config);

  TransactionStateManager state_manager;
  ASSERT_TRUE(state_manager.Initialize(test_dir + "/wal").ok());
  ASSERT_TRUE(state_manager.CreateTransaction(99, {1, 2}).ok());
  ASSERT_TRUE(state_manager.UpdateState(99, TxnState::kUnknown).ok());

  TransactionRecoveryManager recovery_manager;
  ASSERT_TRUE(recovery_manager.Initialize(&state_manager).ok());
  engine.SetRecoveryManager(&recovery_manager);

  Status init_status = engine.Initialize({});
  ASSERT_TRUE(init_status.ok()) << init_status.ToString();

  // No decision log file for txn=99. StartRecovery should fall back to heuristic (abort).
  auto result = recovery_manager.StartRecovery(99);
  EXPECT_EQ(result.recommended_action, RecoveryAction::kAbort);

  engine.Shutdown();
  recovery_manager.Shutdown();
  state_manager.Shutdown();

  std::filesystem::remove_all(test_dir);
}

TEST(DecisionLogRecoveryTest, DecisionLogOverridesPreparedState) {
  std::string test_dir = "/tmp/cedar_test_decision_log_overrides_prepared";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  // Manually write a decision log file for txn_id=77
  {
    std::string path = test_dir + "/txn_77.decision";
    std::ofstream ofs(path, std::ios::binary);
    ASSERT_TRUE(ofs);
    constexpr uint32_t kMagic = 0x44454301;
    constexpr uint32_t kVersion = 1;
    TxnID txn_id = 77;
    Timestamp commit_ts(87654321);
    uint32_t num_parts = 2;
    PartitionID pids[2] = {3, 4};

    ofs.write(reinterpret_cast<const char*>(&kMagic), sizeof(kMagic));
    ofs.write(reinterpret_cast<const char*>(&kVersion), sizeof(kVersion));
    ofs.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    ofs.write(reinterpret_cast<const char*>(&commit_ts), sizeof(commit_ts));
    ofs.write(reinterpret_cast<const char*>(&num_parts), sizeof(num_parts));
    for (uint32_t i = 0; i < num_parts; ++i) {
      ofs.write(reinterpret_cast<const char*>(&pids[i]), sizeof(pids[i]));
    }
    ofs.flush();
    ASSERT_TRUE(ofs);
    ofs.close();
  }

  TwoPCConfig config;
  config.decision_log_dir = test_dir;
  config.enable_adaptive_tuning = false;
  Optimized2PCEngine engine(config);

  TransactionStateManager state_manager;
  ASSERT_TRUE(state_manager.Initialize(test_dir + "/wal").ok());
  ASSERT_TRUE(state_manager.CreateTransaction(77, {3, 4}).ok());
  ASSERT_TRUE(state_manager.UpdateState(77, TxnState::kPrepared).ok());

  TransactionRecoveryManager recovery_manager;
  ASSERT_TRUE(recovery_manager.Initialize(&state_manager).ok());
  engine.SetRecoveryManager(&recovery_manager);

  Status init_status = engine.Initialize({});
  ASSERT_TRUE(init_status.ok()) << init_status.ToString();

  // Decision log exists, so recovery should commit even though state is kPrepared.
  auto result = recovery_manager.StartRecovery(77);
  EXPECT_TRUE(result.success);
  EXPECT_EQ(result.recommended_action, RecoveryAction::kCommit);
  EXPECT_EQ(result.commit_ts.value(), 87654321u);
  EXPECT_EQ(result.pending_participants.size(), 2u);
  EXPECT_EQ(result.pending_participants[0], 3u);
  EXPECT_EQ(result.pending_participants[1], 4u);

  engine.Shutdown();
  recovery_manager.Shutdown();
  state_manager.Shutdown();

  std::filesystem::remove_all(test_dir);
}
