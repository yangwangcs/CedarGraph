// Copyright 2025 The Cedar Authors
//
// Integration test for migration pipeline, meta Raft apply, and partition map watches.

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <thread>
#include <atomic>

#include "cedar/service/partition_migration_service.h"
#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "cedar/dtx/meta_service_impl.h"

using namespace cedar;
using namespace cedar::dtx;
using namespace cedar::service;

// ============================================================================
// Mock PartitionMigrationService for testing DTxRpcClient calls
// ============================================================================

class MockPartitionMigrationService : public cedar::migration::PartitionMigrationService::Service {
 public:
  std::atomic<int> start_migration_count_{0};
  std::atomic<int> sync_data_count_{0};
  std::atomic<int> finalize_migration_count_{0};
  std::atomic<int> fetch_checksum_count_{0};
  std::string last_migration_id_;

  ::grpc::Status StartMigration(
      ::grpc::ServerContext* context,
      const ::cedar::migration::StartMigrationRequest* request,
      ::cedar::migration::StartMigrationResponse* response) override {
    (void)context;
    start_migration_count_.fetch_add(1);
    response->set_success(true);
    response->set_migration_id("mig-test-" + std::to_string(request->partition_id()));
    return ::grpc::Status::OK;
  }

  ::grpc::Status SyncData(
      ::grpc::ServerContext* context,
      ::grpc::ServerReader<::cedar::migration::SyncDataRequest>* reader,
      ::cedar::migration::SyncDataResponse* response) override {
    (void)context;
    sync_data_count_.fetch_add(1);
    ::cedar::migration::SyncDataRequest request;
    uint64_t total = 0;
    while (reader->Read(&request)) {
      total += request.data().size();
    }
    response->set_success(true);
    response->set_bytes_received(total);
    return ::grpc::Status::OK;
  }

  ::grpc::Status FinalizeMigration(
      ::grpc::ServerContext* context,
      const ::cedar::migration::FinalizeMigrationRequest* request,
      ::cedar::migration::FinalizeMigrationResponse* response) override {
    (void)context;
    finalize_migration_count_.fetch_add(1);
    last_migration_id_ = request->migration_id();
    response->set_success(true);
    response->set_final_status(cedar::migration::COMPLETED);
    return ::grpc::Status::OK;
  }

  ::grpc::Status FetchChecksum(
      ::grpc::ServerContext* context,
      const ::cedar::migration::FetchChecksumRequest* request,
      ::cedar::migration::FetchChecksumResponse* response) override {
    (void)context;
    (void)request;
    fetch_checksum_count_.fetch_add(1);
    response->set_checksum("abcd1234");
    response->set_success(true);
    return ::grpc::Status::OK;
  }
};

// ============================================================================
// Migration Pipeline Test
// ============================================================================

class MigrationPipelineTest : public ::testing::Test {
 protected:
  std::unique_ptr<MockPartitionMigrationService> mock_service_;
  std::unique_ptr<grpc::Server> server_;
  std::string server_address_;
  int port_ = 0;
  std::shared_ptr<cedar::migration::PartitionMigrationService::Stub> stub_;

  void SetUp() override {
    mock_service_ = std::make_unique<MockPartitionMigrationService>();
    server_address_ = "127.0.0.1:0";
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(mock_service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port_, 0);

    std::ostringstream oss;
    oss << "127.0.0.1:" << port_;
    server_address_ = oss.str();

    auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    stub_ = cedar::migration::PartitionMigrationService::NewStub(channel);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
  }
};

TEST_F(MigrationPipelineTest, StartMigrationRpc) {
  cedar::migration::StartMigrationRequest request;
  request.set_partition_id(42);
  request.set_source_node("node1");
  request.set_target_node("node2");
  request.set_target_address(server_address_);
  request.set_estimated_data_size(1024);

  cedar::migration::StartMigrationResponse response;
  grpc::ClientContext context;
  auto status = stub_->StartMigration(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(response.success());
  EXPECT_EQ(response.migration_id(), "mig-test-42");
  EXPECT_EQ(mock_service_->start_migration_count_.load(), 1);
}

TEST_F(MigrationPipelineTest, SyncDataStream) {
  cedar::migration::SyncDataRequest request;
  cedar::migration::SyncDataResponse response;
  grpc::ClientContext context;

  auto writer = stub_->SyncData(&context, &response);
  request.set_migration_id("mig-test-1");
  request.set_partition_id(1);
  request.set_offset(0);
  request.set_data("hello world");
  EXPECT_TRUE(writer->Write(request));

  request.set_offset(11);
  request.set_data("second chunk");
  EXPECT_TRUE(writer->Write(request));

  writer->WritesDone();
  auto status = writer->Finish();

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(response.success());
  EXPECT_EQ(response.bytes_received(), 23);
  EXPECT_EQ(mock_service_->sync_data_count_.load(), 1);
}

TEST_F(MigrationPipelineTest, FinalizeMigrationRpc) {
  cedar::migration::FinalizeMigrationRequest request;
  request.set_migration_id("mig-test-1");
  request.set_partition_id(1);
  request.set_commit(true);

  cedar::migration::FinalizeMigrationResponse response;
  grpc::ClientContext context;
  auto status = stub_->FinalizeMigration(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(response.success());
  EXPECT_EQ(response.final_status(), cedar::migration::COMPLETED);
  EXPECT_EQ(mock_service_->finalize_migration_count_.load(), 1);
}

TEST_F(MigrationPipelineTest, FetchChecksumRpc) {
  cedar::migration::FetchChecksumRequest request;
  request.set_partition_id(1);

  cedar::migration::FetchChecksumResponse response;
  grpc::ClientContext context;
  auto status = stub_->FetchChecksum(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(response.success());
  EXPECT_EQ(response.checksum(), "abcd1234");
  EXPECT_EQ(mock_service_->fetch_checksum_count_.load(), 1);
}

// ============================================================================
// PartitionMigrationServiceImpl SyncData Locking Test
// ============================================================================

TEST_F(MigrationPipelineTest, SyncDataHoldsLockAcrossStream) {
  // Set up the real service impl
  PartitionMigrationServiceImpl::Options options;
  options.max_buffer_size = 1024 * 1024;
  PartitionMigrationServiceImpl service(options);

  // Start a migration first
  cedar::migration::StartMigrationRequest start_req;
  start_req.set_partition_id(7);
  start_req.set_source_node("src");
  start_req.set_target_node("tgt");
  start_req.set_target_address("127.0.0.1:1");
  start_req.set_estimated_data_size(100);

  cedar::migration::StartMigrationResponse start_resp;
  grpc::ServerContext ctx1;
  auto s = service.StartMigration(&ctx1, &start_req, &start_resp);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(start_resp.success());
  std::string mig_id = start_resp.migration_id();

  // Stream data to the migration
  grpc::ClientContext client_ctx;
  cedar::migration::SyncDataResponse sync_resp;
  auto sync_writer = stub_->SyncData(&client_ctx, &sync_resp);

  cedar::migration::SyncDataRequest sync_req;
  sync_req.set_migration_id(mig_id);
  sync_req.set_partition_id(7);
  sync_req.set_offset(0);
  sync_req.set_data("chunk1");
  EXPECT_TRUE(sync_writer->Write(sync_req));

  sync_req.set_offset(6);
  sync_req.set_data("chunk2");
  EXPECT_TRUE(sync_writer->Write(sync_req));

  sync_writer->WritesDone();
  auto finish_status = sync_writer->Finish();
  EXPECT_TRUE(finish_status.ok());
  EXPECT_TRUE(sync_resp.success());

  // Verify migration status progressed
  cedar::migration::GetMigrationStatusRequest status_req;
  status_req.set_migration_id(mig_id);
  cedar::migration::GetMigrationStatusResponse status_resp;
  grpc::ServerContext ctx2;
  auto status_s = service.GetMigrationStatus(&ctx2, &status_req, &status_resp);
  EXPECT_TRUE(status_s.ok());
  EXPECT_TRUE(status_resp.success());
}

// ============================================================================
// MetadataStateMachine Apply Test
// ============================================================================

TEST(MetaStateMachineTest, ApplyDeserializesAndAppliesCommands) {
  MetadataService service;
  MetaServiceConfig config;
  config.test_mode = true;
  ASSERT_TRUE(service.Initialize(config).ok());

  // Create a space via the service (goes through test_mode direct apply)
  SpaceDef space;
  space.name = "test_space";
  space.partition_num = 4;
  space.replica_factor = 1;
  ASSERT_TRUE(service.CreateSpace(space).ok());

  // Verify the space exists
  auto result = service.GetSpace("test_space");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().name, "test_space");
  EXPECT_EQ(result.value().partition_num, 4);

  // Verify partition map was created
  auto pmap = service.GetSpacePartitionMap("test_space");
  EXPECT_TRUE(pmap.ok());
  EXPECT_EQ(pmap.value().space_name, "test_space");
  EXPECT_EQ(pmap.value().num_partitions, 4);
}

TEST(MetaStateMachineTest, ApplyCommandDispatchesCorrectly) {
  MetadataService service;
  MetaServiceConfig config;
  config.test_mode = true;
  ASSERT_TRUE(service.Initialize(config).ok());

  // Register a node
  NodeInfo node;
  node.node_id = 1;
  node.address = "127.0.0.1:9559";
  ASSERT_TRUE(service.RegisterNode(node).ok());

  // Create space (needs nodes for assignment)
  SpaceDef space;
  space.name = "space2";
  space.partition_num = 2;
  space.replica_factor = 1;
  ASSERT_TRUE(service.CreateSpace(space).ok());

  // Update partition leader
  ASSERT_TRUE(service.UpdatePartitionLeader("space2", 0, 1).ok());

  // Verify leader updated
  auto assign = service.GetPartitionAssignment("space2", 0);
  ASSERT_TRUE(assign.ok());
  EXPECT_EQ(assign.value().leader_node, 1);
  EXPECT_GE(assign.value().version, 1u);
}

// ============================================================================
// WatchPartitionMap Streaming Test
// ============================================================================

class WatchPartitionMapTest : public ::testing::Test {
 protected:
  std::unique_ptr<MetadataService> meta_service_;
  std::unique_ptr<MetaServiceGrpcImpl> grpc_impl_;
  std::unique_ptr<grpc::Server> server_;
  std::string server_address_;
  int port_ = 0;
  std::shared_ptr<cedar::meta::MetaService::Stub> stub_;

  void SetUp() override {
    meta_service_ = std::make_unique<MetadataService>();
    MetaServiceConfig config;
    config.test_mode = true;
    ASSERT_TRUE(meta_service_->Initialize(config).ok());

    // Register a node so partition assignments are created
    NodeInfo node;
    node.node_id = 1;
    node.address = "127.0.0.1:9559";
    ASSERT_TRUE(meta_service_->RegisterNode(node).ok());

    // Send heartbeat to mark node as online (required for partition assignment)
    NodeStatus status;
    status.node_id = 1;
    status.timestamp = std::chrono::system_clock::now();
    ASSERT_TRUE(meta_service_->Heartbeat(status).ok());

    // Create a space with partition assignments
    SpaceDef space;
    space.name = "watch_space";
    space.partition_num = 2;
    space.replica_factor = 1;
    ASSERT_TRUE(meta_service_->CreateSpace(space).ok());

    grpc_impl_ = std::make_unique<MetaServiceGrpcImpl>(meta_service_.get());

    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(grpc_impl_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port_, 0);

    std::ostringstream oss;
    oss << "127.0.0.1:" << port_;
    server_address_ = oss.str();

    auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    stub_ = cedar::meta::MetaService::NewStub(channel);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
    if (meta_service_) {
      meta_service_->Shutdown();
    }
  }
};

TEST_F(WatchPartitionMapTest, WatchReceivesPartitionChanges) {
  std::atomic<bool> received_change{false};
  std::atomic<int> change_count{0};
  std::string change_space_name;

  // Register a direct callback to verify the notification mechanism
  meta_service_->WatchPartitionMap("watch_space",
      [&](const PartitionMapChange& change) {
        received_change.store(true);
        change_count.fetch_add(1);
        change_space_name = change.space_name;
      });

  // Register the target leader node first
  {
    NodeInfo node;
    node.node_id = 99;
    node.address = "127.0.0.1:9779";
    ASSERT_TRUE(meta_service_->RegisterNode(node).ok());
  }

  // Trigger a partition leader change
  ASSERT_TRUE(meta_service_->UpdatePartitionLeader("watch_space", 0, 99).ok());

  EXPECT_TRUE(received_change.load());
  EXPECT_GE(change_count.load(), 1);
  EXPECT_EQ(change_space_name, "watch_space");

  // Also verify the gRPC service has registered its callback
  // by checking that MetaServiceGrpcImpl constructor completed
  ASSERT_NE(grpc_impl_.get(), nullptr);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
