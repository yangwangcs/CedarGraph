// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>

#include "cedar/dtx/transaction_timeout_manager.h"

using cedar::PendingOperation;
using cedar::TransactionTimeoutManager;

namespace {

PendingOperation MakeReadyRetry(cedar::dtx::TxnID txn_id,
                                cedar::dtx::PartitionID partition_id = 1) {
  PendingOperation op;
  op.type = PendingOperation::Type::kCommit;
  op.txn_id = txn_id;
  op.partition_id = partition_id;
  op.next_attempt = std::chrono::steady_clock::now() - std::chrono::milliseconds(1);
  return op;
}

}  // namespace

TEST(TransactionTimeoutManagerTest, CancelRetriesFiltersReadyOperations) {
  TransactionTimeoutManager manager;
  manager.ScheduleRetry(MakeReadyRetry(100));
  manager.CancelRetries(100);

  auto retries = manager.GetPendingRetries(10);

  EXPECT_TRUE(retries.empty());
}

TEST(TransactionTimeoutManagerTest, ScheduleRetryAfterCancelReenablesTxnId) {
  TransactionTimeoutManager manager;
  manager.ScheduleRetry(MakeReadyRetry(100));
  manager.CancelRetries(100);
  EXPECT_TRUE(manager.GetPendingRetries(10).empty());

  manager.ScheduleRetry(MakeReadyRetry(100, 2));
  auto retries = manager.GetPendingRetries(10);

  ASSERT_EQ(retries.size(), 1);
  EXPECT_EQ(retries[0].txn_id, 100);
  EXPECT_EQ(retries[0].partition_id, 2);
}
