// Copyright 2025 The Cedar Authors
//
// Test: WAL recovery pipeline — LoadPreparedTxns + RecoverFromWAL
// Verifies that after a simulated crash, prepared transactions can be
// restored from snapshot and then completed via WAL replay.

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <filesystem>
#include <fstream>
#include "cedar/dtx/storage_service_impl.h"

using namespace cedar::dtx;

class WALRecoveryTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  std::string wal_dir_ = "/tmp/cedar_wal";
  cedar::CedarGraphStorage* storage_ = nullptr;
  // Unique partition id per test instance to avoid WAL file collisions under ctest -j4
  int partition_id_ = 0;

  void SetUp() override {
    partition_id_ = static_cast<int>(
        std::hash<std::string>{}(::testing::UnitTest::GetInstance()->current_test_info()->name()));
    // Ensure positive and not too large
    partition_id_ = std::abs(partition_id_) % 100000 + 1;

    data_dir_ = "/tmp/test_wal_recovery_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);
    // Only remove OUR WAL file, not the whole directory
    std::filesystem::remove(GetWalPath());

    cedar::CedarOptions options;
    options.create_if_missing = true;
    auto status = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
    std::filesystem::remove(GetWalPath());
  }

  std::string GetWalPath() const {
    return wal_dir_ + "/partition_" + std::to_string(partition_id_) + "_wal.log";
  }

  // Re-open storage to simulate process restart
  void ReopenStorage() {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    cedar::CedarOptions options;
    options.create_if_missing = false;
    auto status = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);
  }
};

TEST_F(WALRecoveryTest, CommitAfterSnapshotRecovered) {
  PartitionStorage ps(partition_id_, storage_, nullptr);

  cedar::CedarKey k1;
  k1.SetEntityId(100);
  k1.SetColumnId(1);
  k1.SetEntityType(1);
  cedar::Descriptor d;

  // T1 prepares and is snapshotted
  ASSERT_TRUE(ps.Prepare(1, {}, {k1}, {{static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(k1)), d}},
                         cedar::Timestamp(1)).ok());

  std::string snapshot_path = data_dir_ + "/prepared_txns.snap";
  ASSERT_TRUE(ps.SavePreparedTxns(snapshot_path).ok());

  // T1 commits (writes WAL, but we "crash" before we can rely on in-memory state)
  ASSERT_TRUE(ps.Commit(1, cedar::Timestamp(2)).ok());

  // Simulate crash: reopen storage and create fresh PartitionStorage
  ReopenStorage();
  PartitionStorage ps2(partition_id_, storage_, nullptr);

  // Verify T1 is not in prepared state (fresh PS knows nothing)
  EXPECT_TRUE(ps2.GetPreparedTransactions().empty());

  // Restore prepared state from snapshot
  ASSERT_TRUE(ps2.LoadPreparedTxns(snapshot_path).ok());
  auto prepared = ps2.GetPreparedTransactions();
  EXPECT_EQ(prepared.size(), 1);
  EXPECT_EQ(prepared[0], 1);

  // Now replay WAL — this should complete the commit
  ASSERT_TRUE(ps2.RecoverFromWAL().ok());

  // After WAL replay, T1 should no longer be prepared (commit completed)
  prepared = ps2.GetPreparedTransactions();
  EXPECT_TRUE(prepared.empty());
}

TEST_F(WALRecoveryTest, AbortAfterSnapshotRecovered) {
  PartitionStorage ps(partition_id_, storage_, nullptr);

  cedar::CedarKey k1;
  k1.SetEntityId(200);
  k1.SetColumnId(1);
  k1.SetEntityType(1);
  cedar::Descriptor d;

  ASSERT_TRUE(ps.Prepare(2, {}, {k1}, {{static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(k1)), d}},
                         cedar::Timestamp(1)).ok());

  std::string snapshot_path = data_dir_ + "/prepared_txns.snap";
  ASSERT_TRUE(ps.SavePreparedTxns(snapshot_path).ok());

  // T2 aborts (writes WAL)
  ASSERT_TRUE(ps.Abort(2).ok());

  // Simulate crash
  ReopenStorage();
  PartitionStorage ps2(partition_id_, storage_, nullptr);

  // Restore from snapshot
  ASSERT_TRUE(ps2.LoadPreparedTxns(snapshot_path).ok());
  EXPECT_EQ(ps2.GetPreparedTransactions().size(), 1);

  // Replay WAL — should complete the abort
  ASSERT_TRUE(ps2.RecoverFromWAL().ok());

  // T2 should no longer be prepared
  EXPECT_TRUE(ps2.GetPreparedTransactions().empty());
}

TEST_F(WALRecoveryTest, CorruptWALHandledGracefully) {
  PartitionStorage ps(partition_id_, storage_, nullptr);

  cedar::CedarKey k1;
  k1.SetEntityId(300);
  k1.SetColumnId(1);
  k1.SetEntityType(1);
  cedar::Descriptor d;

  ASSERT_TRUE(ps.Prepare(3, {}, {k1}, {{static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(k1)), d}},
                         cedar::Timestamp(1)).ok());
  ASSERT_TRUE(ps.Commit(3, cedar::Timestamp(2)).ok());

  // Truncate WAL file mid-record to simulate corruption
  std::string wal_path = GetWalPath();
  ASSERT_TRUE(std::filesystem::exists(wal_path));
  {
    auto size = std::filesystem::file_size(wal_path);
    ASSERT_GT(size, 20);  // Should have at least one record
    std::fstream fs(wal_path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs);
    fs.seekp(size - 10);  // Truncate last 10 bytes
    fs.close();
    std::filesystem::resize_file(wal_path, size - 10);
  }

  // Simulate crash
  ReopenStorage();
  PartitionStorage ps2(partition_id_, storage_, nullptr);

  // Recovery should handle corruption gracefully (return OK, stop at corrupt record)
  auto status = ps2.RecoverFromWAL();
  EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST_F(WALRecoveryTest, NoWALReturnsOK) {
  PartitionStorage ps(partition_id_, storage_, nullptr);

  // No WAL file exists yet
  auto status = ps.RecoverFromWAL();
  EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST_F(WALRecoveryTest, MultipleTransactionsRecoveredInOrder) {
  PartitionStorage ps(partition_id_, storage_, nullptr);

  cedar::CedarKey k1, k2;
  k1.SetEntityId(400);
  k1.SetColumnId(1);
  k1.SetEntityType(1);
  k2.SetEntityId(500);
  k2.SetColumnId(1);
  k2.SetEntityType(1);
  cedar::Descriptor d;

  // Prepare two transactions
  ASSERT_TRUE(ps.Prepare(10, {}, {k1}, {{static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(k1)), d}},
                         cedar::Timestamp(1)).ok());
  ASSERT_TRUE(ps.Prepare(11, {}, {k2}, {{static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(k2)), d}},
                         cedar::Timestamp(1)).ok());

  std::string snapshot_path = data_dir_ + "/prepared_txns.snap";
  ASSERT_TRUE(ps.SavePreparedTxns(snapshot_path).ok());

  // Commit T10, abort T11
  ASSERT_TRUE(ps.Commit(10, cedar::Timestamp(2)).ok());
  ASSERT_TRUE(ps.Abort(11).ok());

  // Simulate crash
  ReopenStorage();
  PartitionStorage ps2(partition_id_, storage_, nullptr);

  ASSERT_TRUE(ps2.LoadPreparedTxns(snapshot_path).ok());
  EXPECT_EQ(ps2.GetPreparedTransactions().size(), 2);

  ASSERT_TRUE(ps2.RecoverFromWAL().ok());

  // Both should be gone
  EXPECT_TRUE(ps2.GetPreparedTransactions().empty());
}
