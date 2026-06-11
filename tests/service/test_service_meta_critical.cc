// Copyright 2025 The Cedar Authors
//
// Service & Meta CRITICAL fixes test suite.

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <filesystem>

#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "cedar/service/partition_migration_service.h"
#include "cedar/service/graph_service_router.h"
#include "cedar/dtx/security.h"

using namespace cedar::dtx;
using namespace cedar::service;

// ============================================================================
// Meta Service Tests
// ============================================================================

class ServiceMetaCriticalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MetaServiceConfig config;
    config.node_id = 1;
    config.listen_address = "127.0.0.1:2379";
    config.advertise_address = "127.0.0.1:2379";
    config.test_mode = true;
    auto status = meta_service_.Initialize(config);
    EXPECT_TRUE(status.ok()) << status.ToString();
  }

  void TearDown() override { meta_service_.Shutdown(); }

  MetadataService meta_service_;
};

// Issue 10: CreateSpace validates space name non-empty and replica_factor <= alive_node_count
TEST_F(ServiceMetaCriticalTest, CreateSpaceRejectsEmptyName) {
  MetaServiceGrpcImpl impl(&meta_service_);
  grpc::ServerContext context;
  cedar::meta::CreateSpaceRequest request;
  cedar::meta::CreateSpaceResponse response;
  request.mutable_space()->set_name("");
  request.mutable_space()->set_partition_num(4);
  request.mutable_space()->set_replica_factor(1);

  auto status = impl.CreateSpace(&context, &request, &response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(ServiceMetaCriticalTest, CreateSpaceRejectsHighReplicaFactor) {
  MetaServiceGrpcImpl impl(&meta_service_);
  grpc::ServerContext context;
  cedar::meta::CreateSpaceRequest request;
  cedar::meta::CreateSpaceResponse response;
  request.mutable_space()->set_name("test_space");
  request.mutable_space()->set_partition_num(4);
  request.mutable_space()->set_replica_factor(100);

  auto status = impl.CreateSpace(&context, &request, &response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

// Issue 5: gRPC error codes mapped properly
TEST_F(ServiceMetaCriticalTest, GetSpaceReturnsNotFoundGrpcCode) {
  MetaServiceGrpcImpl impl(&meta_service_);
  grpc::ServerContext context;
  cedar::meta::GetSpaceRequest request;
  cedar::meta::GetSpaceResponse response;
  request.set_space_name("nonexistent");

  auto status = impl.GetSpace(&context, &request, &response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// Issue 9: UpdatePartitionLeader validates new leader is registered
TEST_F(ServiceMetaCriticalTest, UpdatePartitionLeaderRejectsUnregisteredNode) {
  auto status = meta_service_.UpdatePartitionLeader("default", 0, 9999);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument());
}

// Issue 4: Heartbeat rate limiting (max 10 proposals/sec per node)
TEST_F(ServiceMetaCriticalTest, HeartbeatRateLimiting) {
  NodeStatus status;
  status.node_id = 1;
  status.cpu_usage_percent = 0;
  status.memory_usage_percent = 0;
  status.disk_usage_percent = 0;
  status.qps = 0;
  status.latency_ms = 0;
  status.timestamp = std::chrono::system_clock::now();

  // First 10 should succeed
  for (int i = 0; i < 10; ++i) {
    auto s = meta_service_.Heartbeat(status);
    EXPECT_TRUE(s.ok()) << s.ToString();
  }
  // 11th should be rate limited
  auto s = meta_service_.Heartbeat(status);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsResourceExhausted()) << s.ToString();
}

// Issue 13: Protobuf serialization roundtrip (snapshot version 2)
TEST_F(ServiceMetaCriticalTest, SnapshotRoundtripUsesProtobuf) {
  SpaceDef space;
  space.name = "protobuf_test_space";
  space.partition_num = 8;
  space.replica_factor = 1;
  EXPECT_TRUE(meta_service_.CreateSpace(space).ok());

  auto snapshot_data = meta_service_.SerializeState();
  EXPECT_FALSE(snapshot_data.empty());

  // Verify magic and version 2
  EXPECT_EQ(snapshot_data.substr(0, 4), "CMSN");
  uint32_t version;
  std::memcpy(&version, snapshot_data.data() + 4, sizeof(version));
  EXPECT_EQ(version, 2u);

  MetadataService restored;
  MetaServiceConfig config;
  config.node_id = 2;
  config.listen_address = "127.0.0.1:2380";
  config.advertise_address = "127.0.0.1:2380";
  config.test_mode = true;
  EXPECT_TRUE(restored.Initialize(config).ok());
  EXPECT_TRUE(restored.DeserializeState(snapshot_data));

  auto result = restored.GetSpace("protobuf_test_space");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().name, "protobuf_test_space");
  EXPECT_EQ(result.value().partition_num, 8u);
  restored.Shutdown();
}

// Issue 11: OnBecomeLeader notifies watchers
TEST_F(ServiceMetaCriticalTest, OnBecomeLeaderNotifiesWatchers) {
  bool notified = false;
  PartitionMapChange received_change;
  meta_service_.WatchPartitionMap("",
                                  [&](const PartitionMapChange& change) {
                                    notified = true;
                                    received_change = change;
                                  });

  meta_service_.OnBecomeLeader();

  EXPECT_TRUE(notified);
  EXPECT_EQ(received_change.change_type, PartitionChangeType::kLeaderChanged);
  EXPECT_EQ(received_change.new_leader, 1u);
}

// ============================================================================
// Partition Migration Tests
// ============================================================================

TEST(PartitionMigrationCriticalTest, CleanupOldMigrationsRaceSafe) {
  PartitionMigrationServiceImpl::Options options;
  PartitionMigrationServiceImpl service(options);

  // Start and complete a migration
  cedar::migration::StartMigrationRequest start_req;
  start_req.set_partition_id(1);
  start_req.set_source_node("node1");
  start_req.set_target_node("node2");
  start_req.set_target_address("127.0.0.1:50051");
  start_req.set_estimated_data_size(1000);

  grpc::ServerContext ctx;
  cedar::migration::StartMigrationResponse start_resp;
  EXPECT_TRUE(service.StartMigration(&ctx, &start_req, &start_resp).ok());

  cedar::migration::FinalizeMigrationRequest finalize_req;
  finalize_req.set_migration_id(start_resp.migration_id());
  finalize_req.set_commit(false);
  cedar::migration::FinalizeMigrationResponse finalize_resp;
  EXPECT_TRUE(
      service.FinalizeMigration(&ctx, &finalize_req, &finalize_resp).ok());

  // Cleanup should work without races — allow a brief window since
  // FinalizeMigration sets completed_at to now.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  size_t cleaned = service.CleanupOldMigrations(std::chrono::milliseconds(5));
  EXPECT_EQ(cleaned, 1u);

  auto stats = service.GetStats();
  EXPECT_EQ(stats.total_migrations_started, 1u);
  EXPECT_EQ(stats.total_migrations_rolled_back, 1u);
}

// ============================================================================
// Graph Service Router Tests
// ============================================================================

TEST(GraphServiceRouterCriticalTest, BeginTransactionEnforcesLimit) {
  auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
  cedar::dtx::security::SecurityManager::Config cfg;
  cfg.enable_auth = false;
  auto init_status = sm->Initialize(cfg);
  EXPECT_TRUE(init_status.ok()) << init_status.ToString();

  GraphServiceRouter router;
  grpc::ServerContext context;
  context.AddInitialMetadata("authorization", "Bearer test-token");
  cedargrpc::BeginTransactionRequest request;
  request.set_isolation_level(cedargrpc::IsolationLevel::READ_COMMITTED);

  const size_t kLimit = 10000;
  for (size_t i = 0; i < kLimit; ++i) {
    cedargrpc::Transaction response;
    auto status = router.BeginTransaction(&context, &request, &response);
    EXPECT_TRUE(status.ok()) << status.error_message();
  }

  // One beyond the limit should fail
  cedargrpc::Transaction response;
  auto status = router.BeginTransaction(&context, &request, &response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED);

  sm->Shutdown();
}

TEST(GraphServiceRouterCriticalTest, RefreshPartitionMapReturnsStatus) {
  GraphServiceRouter router;
  // Without meta client, RefreshPartitionMap returns OK
  auto status = router.RefreshPartitionMap();
  EXPECT_TRUE(status.ok());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
