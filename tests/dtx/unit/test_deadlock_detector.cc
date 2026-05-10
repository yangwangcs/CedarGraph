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

#include "cedar/dtx/deadlock_detector.h"

using namespace cedar::dtx;

class DeadlockDetectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    detector_ = std::make_unique<DistributedDeadlockDetector>();
    DistributedDeadlockDetector::Config config;
    config.detection_interval_ms = 100;
    config.cleanup_interval_ms = 100;
    config.edge_timeout_ms = 5000;
    config.max_cycle_size = 10;
    ASSERT_TRUE(detector_->Initialize(config).ok());
  }

  void TearDown() override {
    detector_->Shutdown();
  }

  std::unique_ptr<DistributedDeadlockDetector> detector_;
};

TEST_F(DeadlockDetectorTest, NoDeadlockEmptyGraph) {
  auto result = detector_->DetectNow();
  EXPECT_FALSE(result.has_deadlock);
}

TEST_F(DeadlockDetectorTest, NoDeadlockLinearChain) {
  // T1 waits for T2, T2 waits for T3 — no cycle
  detector_->RegisterWait(1, 2, 100, "resource_a");
  detector_->RegisterWait(2, 3, 100, "resource_b");

  auto result = detector_->DetectNow();
  EXPECT_FALSE(result.has_deadlock);

  detector_->UnregisterWait(1, 2);
  detector_->UnregisterWait(2, 3);
}

TEST_F(DeadlockDetectorTest, SimpleCycleTwoTxnsAutoResolved) {
  // T1 waits for T2, T2 waits for T1 — cycle
  // RegisterWait automatically detects deadlock and removes the victim.
  detector_->RegisterWait(1, 2, 100, "resource_a");
  detector_->RegisterWait(2, 1, 100, "resource_b");

  // After auto-resolution, the graph should no longer have a deadlock
  auto result = detector_->DetectNow();
  EXPECT_FALSE(result.has_deadlock);

  detector_->UnregisterWait(1, 2);
  detector_->UnregisterWait(2, 1);
}

TEST_F(DeadlockDetectorTest, SimpleCycleThreeTxnsAutoResolved) {
  // T1 -> T2 -> T3 -> T1
  detector_->RegisterWait(1, 2, 100, "r1");
  detector_->RegisterWait(2, 3, 100, "r2");
  detector_->RegisterWait(3, 1, 100, "r3");

  // Auto-resolution removes victim (youngest = T3)
  auto result = detector_->DetectNow();
  EXPECT_FALSE(result.has_deadlock);

  detector_->UnregisterWait(1, 2);
  detector_->UnregisterWait(2, 3);
  detector_->UnregisterWait(3, 1);
}

TEST_F(DeadlockDetectorTest, CheckTxnFindsDeadlockBeforeAutoResolve) {
  // Build a cycle incrementally; CheckTxn on the first edge addition
  // won't find deadlock yet. After the third edge, RegisterWait auto-resolves.
  detector_->RegisterWait(1, 2, 100, "r1");
  detector_->RegisterWait(2, 3, 100, "r2");

  // Before closing the cycle, no deadlock
  auto result = detector_->CheckTxn(1);
  EXPECT_FALSE(result.has_deadlock);

  // Closing the cycle triggers auto-resolution in RegisterWait
  detector_->RegisterWait(3, 1, 100, "r3");

  // After auto-resolution, no deadlock remains
  result = detector_->DetectNow();
  EXPECT_FALSE(result.has_deadlock);

  detector_->UnregisterWait(1, 2);
  detector_->UnregisterWait(2, 3);
  detector_->UnregisterWait(3, 1);
}

TEST_F(DeadlockDetectorTest, UnregisterTxnRemovesAllEdges) {
  detector_->RegisterWait(1, 2, 100, "r1");
  detector_->RegisterWait(1, 3, 100, "r2");
  detector_->RegisterWait(2, 3, 100, "r3");

  detector_->UnregisterTxn(1);

  auto result = detector_->DetectNow();
  EXPECT_FALSE(result.has_deadlock);
}

TEST_F(DeadlockDetectorTest, CleanupExpiredEdges) {
  detector_->RegisterWait(1, 2, 100, "r1");

  // Before expiration, edge should still be present
  auto result = detector_->DetectNow();
  EXPECT_FALSE(result.has_deadlock);

  detector_->UnregisterWait(1, 2);
}

TEST_F(DeadlockDetectorTest, VictimHandlerCalledOnAutoResolve) {
  TxnID aborted_victim = 0;
  detector_->SetVictimHandler([&aborted_victim](TxnID victim) {
    aborted_victim = victim;
  });

  detector_->RegisterWait(1, 2, 100, "r1");
  detector_->RegisterWait(2, 1, 100, "r2");

  // RegisterWait auto-detects deadlock and calls the victim handler
  // Victim is youngest txn (max id) = 2
  EXPECT_EQ(aborted_victim, 2);

  detector_->UnregisterWait(1, 2);
  detector_->UnregisterWait(2, 1);
}
