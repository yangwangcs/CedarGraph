// Copyright 2025 The Cedar Authors
//
// Test: Abort (rollback) replicates through Raft when PartitionRaftManager is present.
// On a partitioned leader, a local-only abort would leave followers in prepared state,
// causing inconsistency after failover. This verifies the Abort() RPC handler proposes
// kAbort through Raft (mirroring Commit()), and falls back to direct partition->Abort()
// when no Raft manager exists.

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>

#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/storage/partition_raft_manager.h"
#include "cedar/dtx/security.h"

#include <brpc/server.h>
#include <braft/raft.h>

using namespace cedar::dtx;

TEST(RollbackReplicationTest, AbortWithoutRaftFallsBackToDirect) {
  // Disable auth for this unit test
  {
    auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
    cedar::dtx::security::SecurityManager::Config cfg;
    cfg.enable_auth = false;
    auto s = sm->Initialize(cfg);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  std::string data_dir = "/tmp/test_rollback_replication_direct";
  std::filesystem::remove_all(data_dir);

  StoragePartitionManager pm;
  StoragePartitionManager::PartitionConfig config;
  config.data_root = data_dir;
  ASSERT_TRUE(pm.Initialize(config).ok());
  ASSERT_TRUE(pm.AddPartition(1).ok());

  // No raft manager -> direct abort path
  StorageServiceImpl service(&pm, nullptr);

  // Prepare a transaction
  cedar::storage::PrepareRequest prep_req;
  cedar::storage::PrepareResponse prep_resp;
  prep_req.set_txn_id(42);
  prep_req.set_commit_ts(100);
  cedar::CedarKey key = cedar::CedarKey::Vertex(100, 1, cedar::Timestamp(1), 0, 1);
  *prep_req.add_write_set() = StorageServiceImpl::CedarKeyToProto(key);

  grpc::ServerContext ctx;
  auto status = service.Prepare(&ctx, &prep_req, &prep_resp);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(prep_resp.prepared());

  // Abort the transaction
  cedar::storage::AbortRequest abort_req;
  cedar::storage::AbortResponse abort_resp;
  abort_req.set_txn_id(42);

  status = service.Abort(&ctx, &abort_req, &abort_resp);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(abort_resp.success());

  // Verify transaction is no longer prepared
  auto* partition = pm.GetPartition(1);
  ASSERT_NE(partition, nullptr);
  DistributedTxnState state;
  auto inquire_status = partition->Inquire(42, &state);
  EXPECT_TRUE(inquire_status.IsNotFound())
      << "Transaction should have been aborted, but got: " << inquire_status.ToString();

  std::filesystem::remove_all(data_dir);
  cedar::dtx::security::SecurityManager::GetInstance()->Shutdown();
}

TEST(RollbackReplicationTest, AbortReplicatesThroughSingleNodeRaft) {
  // Disable auth for this unit test
  {
    auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
    cedar::dtx::security::SecurityManager::Config cfg;
    cfg.enable_auth = false;
    auto s = sm->Initialize(cfg);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  std::string data_dir = "/tmp/test_rollback_replication_raft";
  std::filesystem::remove_all(data_dir);

  // Setup partition manager
  StoragePartitionManager pm;
  StoragePartitionManager::PartitionConfig config;
  config.data_root = data_dir;
  ASSERT_TRUE(pm.Initialize(config).ok());
  ASSERT_TRUE(pm.AddPartition(1).ok());

  auto* storage = pm.GetPartition(1);
  ASSERT_NE(storage, nullptr);

  // Setup a brpc server so braft can attach its service.
  // We retry a few ports in case the first choice is transiently in use.
  std::unique_ptr<brpc::Server> server;
  std::unique_ptr<PartitionRaftManager> raft_manager;
  auto* raft_group = static_cast<BraftPartitionNode*>(nullptr);
  bool raft_ready = false;

  for (int attempt = 0; attempt < 10; ++attempt) {
    int port = 29999 + attempt;
    std::string addr = "127.0.0.1:" + std::to_string(port);

    auto srv = std::make_unique<brpc::Server>();
    if (braft::add_service(srv.get(), addr.c_str()) != 0) {
      continue;
    }
    if (srv->Start(addr.c_str(), nullptr) != 0) {
      continue;
    }

    auto rm = std::make_unique<PartitionRaftManager>();
    auto init_status = rm->Initialize(1, data_dir + "/raft", addr);
    ASSERT_TRUE(init_status.ok());

    std::vector<std::string> peers = {addr};
    std::unordered_map<std::string, NodeID> peer_node_ids = {{addr, 1}};
    auto cg_status = rm->CreateRaftGroup(1, peers, storage, 100, peer_node_ids);
    if (!cg_status.ok()) {
      rm->Shutdown();
      srv->Stop(0);
      srv->Join();
      continue;
    }

    auto* rg = rm->GetRaftGroup(1);
    if (!rg) {
      rm->Shutdown();
      srv->Stop(0);
      srv->Join();
      continue;
    }

    // Wait for leader election AND valid lease
    int retries = 100;
    while ((!rg->IsLeader() || !rg->IsLeaseValid()) && retries-- > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (rg->IsLeader() && rg->IsLeaseValid()) {
      server = std::move(srv);
      raft_manager = std::move(rm);
      raft_group = rg;
      raft_ready = true;
      break;
    }
    rm->Shutdown();
    srv->Stop(0);
    srv->Join();
  }
  ASSERT_TRUE(raft_ready) << "Failed to initialize single-node Raft group after 10 attempts";

  // Create service with raft manager
  StorageServiceImpl service(&pm, raft_manager.get());

  // Prepare a transaction (will go through Raft)
  cedar::storage::PrepareRequest prep_req;
  cedar::storage::PrepareResponse prep_resp;
  prep_req.set_txn_id(100);
  prep_req.set_commit_ts(200);
  cedar::CedarKey key = cedar::CedarKey::Vertex(100, 1, cedar::Timestamp(1), 0, 1);
  *prep_req.add_write_set() = StorageServiceImpl::CedarKeyToProto(key);

  grpc::ServerContext ctx;
  auto status = service.Prepare(&ctx, &prep_req, &prep_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_TRUE(prep_resp.prepared());

  // Verify transaction is prepared on storage (applied by Raft state machine)
  DistributedTxnState state;
  auto inquire_status = storage->Inquire(100, &state);
  ASSERT_TRUE(inquire_status.ok());
  ASSERT_EQ(state, DistributedTxnState::kPrepared);

  // Abort the transaction (should propose kAbort through Raft)
  cedar::storage::AbortRequest abort_req;
  cedar::storage::AbortResponse abort_resp;
  abort_req.set_txn_id(100);

  status = service.Abort(&ctx, &abort_req, &abort_resp);
  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_TRUE(abort_resp.success());

  // Verify transaction is aborted (Raft-proposed abort was applied locally)
  inquire_status = storage->Inquire(100, &state);
  EXPECT_TRUE(inquire_status.IsNotFound() || state == DistributedTxnState::kAborted)
      << "Transaction should have been aborted via Raft replication, but got: "
      << inquire_status.ToString();

  // Cleanup
  raft_manager->Shutdown();
  server->Stop(0);
  server->Join();
  std::filesystem::remove_all(data_dir);
  cedar::dtx::security::SecurityManager::GetInstance()->Shutdown();
}
