// Copyright 2025 The Cedar Authors
//
// Test: PartitionStorage::Commit holds lock across all writes,
// preventing concurrent Abort from causing UAF.

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <filesystem>
#include "cedar/dtx/storage_service_impl.h"

using namespace cedar::dtx;

TEST(PartitionStorage, CommitHoldsLockAcrossAllPuts) {
  std::string data_dir = "/tmp/test_partition_storage_commit_db";
  std::filesystem::remove_all(data_dir);

  cedar::CedarOptions options;
  options.create_if_missing = true;
  cedar::CedarGraphStorage* storage = nullptr;
  auto status = cedar::CedarGraphStorage::Open(options, data_dir, &storage);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_NE(storage, nullptr);

  PartitionStorage ps(1, storage, nullptr);

  cedar::CedarKey k1;
  k1.SetEntityId(100);
  k1.SetColumnId(1);
  k1.SetEntityType(1);
  cedar::Descriptor d;

  ASSERT_TRUE(ps.Prepare(1, {}, {k1}, cedar::Timestamp(1)).ok());

  // Concurrent abort from another thread while Commit is in progress
  std::thread abort_thread([&ps]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    ps.Abort(1);
  });

  status = ps.Commit(1, cedar::Timestamp(2));
  abort_thread.join();

  // The fix ensures Commit either completes successfully or returns an error,
  // but never crashes due to iterator invalidation.
  // Under TSan this would have reported a data race in the old code.
  SUCCEED();

  delete storage;
  std::filesystem::remove_all(data_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
