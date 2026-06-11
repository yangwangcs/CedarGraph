// Copyright 2025 The Cedar Authors
//
// Test: Commit halts on first failure and rolls back already-committed partitions.
// Previously, if partition N+1 commit failed, partition N was left committed,
// violating atomicity.

#include <gtest/gtest.h>
#include <filesystem>
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/security.h"

using namespace cedar::dtx;

TEST(PartialCommitRollbackTest, HaltsAndRollsBackOnSecondCommitFailure) {
  // Disable auth for this unit test
  {
    auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
    cedar::dtx::security::SecurityManager::Config cfg;
    cfg.enable_auth = false;
    auto s = sm->Initialize(cfg);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  std::string data_dir = "/tmp/test_partial_commit_rollback";
  std::filesystem::remove_all(data_dir);

  // Setup partition manager with two partitions
  StoragePartitionManager pm;
  StoragePartitionManager::PartitionConfig config;
  config.data_root = data_dir;
  ASSERT_TRUE(pm.Initialize(config).ok());
  ASSERT_TRUE(pm.AddPartition(1).ok());
  ASSERT_TRUE(pm.AddPartition(2).ok());

  // No raft manager -> direct commit path
  StorageServiceImpl service(&pm, nullptr);

  // Prepare a transaction touching both partitions
  cedar::storage::PrepareRequest prep_req;
  cedar::storage::PrepareResponse prep_resp;
  prep_req.set_txn_id(42);
  prep_req.set_commit_ts(100);

  // Key for partition 1
  cedar::CedarKey key1 = cedar::CedarKey::Vertex(100, 1, cedar::Timestamp(1), 0, 1);
  *prep_req.add_write_set() = StorageServiceImpl::CedarKeyToProto(key1);

  // Key for partition 2
  cedar::CedarKey key2 = cedar::CedarKey::Vertex(200, 1, cedar::Timestamp(1), 0, 2);
  *prep_req.add_write_set() = StorageServiceImpl::CedarKeyToProto(key2);

  grpc::ServerContext ctx;
  auto status = service.Prepare(&ctx, &prep_req, &prep_resp);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(prep_resp.prepared());

  // Make partition 2 read-only so its Commit will fail
  auto* partition2 = pm.GetPartition(2);
  ASSERT_NE(partition2, nullptr);
  partition2->SetReadOnly(true);

  // Attempt to commit - partition 1 should succeed, partition 2 should fail,
  // and partition 1 must be rolled back to maintain atomicity
  cedar::storage::CommitRequest commit_req;
  cedar::storage::CommitResponse commit_resp;
  commit_req.set_txn_id(42);
  commit_req.set_commit_ts(100);

  status = service.Commit(&ctx, &commit_req, &commit_resp);
  EXPECT_FALSE(status.ok());  // gRPC call returns error on commit failure
  EXPECT_FALSE(commit_resp.success());

  // Verify partition 1 was rolled back (aborted)
  auto* partition1 = pm.GetPartition(1);
  ASSERT_NE(partition1, nullptr);
  DistributedTxnState state;
  auto inquire_status = partition1->Inquire(42, &state);
  EXPECT_TRUE(inquire_status.IsNotFound())
      << "Partition 1 should have been rolled back, but Inquire returned: "
      << inquire_status.ToString();

  // Cleanup
  std::filesystem::remove_all(data_dir);
  cedar::dtx::security::SecurityManager::GetInstance()->Shutdown();
}
