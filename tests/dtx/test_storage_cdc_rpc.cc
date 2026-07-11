#include <chrono>
#include <filesystem>
#include <memory>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>

#include "cedar/dtx/security.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"

namespace {

class StorageCdcRpcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cedar::dtx::security::SecurityManager::Config security_config;
    security_config.enable_auth = false;
    auto security_status =
        cedar::dtx::security::SecurityManager::GetInstance()->Initialize(
            security_config);
    ASSERT_TRUE(security_status.ok()) << security_status.ToString();

    data_dir_ = (std::filesystem::temp_directory_path() /
                 ("cedar_storage_cdc_rpc_" +
                  std::to_string(std::chrono::steady_clock::now()
                                     .time_since_epoch()
                                     .count())))
                    .string();
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    partition_manager_ = std::make_unique<cedar::dtx::StoragePartitionManager>();
    cedar::dtx::StoragePartitionManager::PartitionConfig config;
    config.data_root = data_dir_;
    config.enable_cdc = true;
    ASSERT_TRUE(partition_manager_->Initialize(config).ok());
    ASSERT_TRUE(partition_manager_->AddPartition(8).ok());
    partition_ = partition_manager_->GetPartition(8);
    ASSERT_NE(partition_, nullptr);
    service_ = std::make_unique<cedar::dtx::StorageServiceImpl>(
        partition_manager_.get());
  }

  void TearDown() override {
    if (grpc_server_) {
      grpc_server_->Shutdown();
      grpc_server_->Wait();
      grpc_server_.reset();
    }
    service_.reset();
    partition_manager_.reset();
    cedar::dtx::security::SecurityManager::GetInstance()->Shutdown();
    std::filesystem::remove_all(data_dir_);
  }

  cedar::CedarKey Key(uint64_t entity_id, uint32_t column_id = 1) const {
    return cedar::CedarKey(entity_id, cedar::EntityType::Vertex, column_id,
                           cedar::Timestamp(100 + entity_id), 0, 0, 0, 8);
  }

  std::unordered_map<uint64_t, cedar::Descriptor> Descriptors(
      const std::vector<cedar::CedarKey>& writes) const {
    std::unordered_map<uint64_t, cedar::Descriptor> descriptors;
    for (size_t i = 0; i < writes.size(); ++i) {
      descriptors[static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(writes[i]))] =
          cedar::Descriptor::InlineInt(static_cast<uint32_t>(i + 1),
                                       static_cast<int64_t>(1000 + i));
    }
    return descriptors;
  }

  void CommitRecords(size_t count,
                     uint64_t txn_id = 100,
                     uint64_t commit_ts = 700,
                     uint64_t first_entity_id = 1000) {
    std::vector<cedar::CedarKey> writes;
    writes.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      writes.push_back(Key(first_entity_id + i, static_cast<uint32_t>(i + 1)));
    }
    ASSERT_TRUE(partition_->Prepare(txn_id, {}, writes, Descriptors(writes),
                                    cedar::Timestamp(commit_ts))
                    .ok());
    ASSERT_TRUE(partition_->Commit(txn_id, cedar::Timestamp(commit_ts)).ok());
  }

  std::unique_ptr<cedar::storage::StorageService::Stub> StartGrpcServer() {
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(service_.get());
    grpc_server_ = builder.BuildAndStart();
    EXPECT_NE(grpc_server_, nullptr);
    EXPECT_GT(port, 0);
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                       grpc::InsecureChannelCredentials());
    return cedar::storage::StorageService::NewStub(channel);
  }

  std::string data_dir_;
  std::unique_ptr<cedar::dtx::StoragePartitionManager> partition_manager_;
  cedar::dtx::PartitionStorage* partition_ = nullptr;
  std::unique_ptr<cedar::dtx::StorageServiceImpl> service_;
  std::unique_ptr<grpc::Server> grpc_server_;
};

TEST_F(StorageCdcRpcTest, FetchChangesReturnsBoundedContinuousBatch) {
  CommitRecords(20);

  grpc::ServerContext context;
  cedar::storage::FetchChangesRequest request;
  request.set_partition_id(8);
  request.set_after_offset(5);
  request.set_limit_records(3);
  request.set_limit_bytes(1 << 20);
  request.set_expected_epoch(1);
  cedar::storage::FetchChangesResponse response;

  auto status = service_->FetchChanges(&context, &request, &response);

  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_TRUE(response.success()) << response.error_msg();
  ASSERT_EQ(response.records_size(), 3);
  EXPECT_EQ(response.records(0).offset(), 6);
  EXPECT_EQ(response.records(2).offset(), 8);
  EXPECT_EQ(response.next_offset(), 8);
  EXPECT_EQ(response.high_watermark(), 20);
}

TEST_F(StorageCdcRpcTest, RejectsStaleEpochAndOversizedLimits) {
  CommitRecords(1);

  grpc::ServerContext stale_context;
  cedar::storage::FetchChangesRequest stale;
  stale.set_partition_id(8);
  stale.set_expected_epoch(999);
  stale.set_limit_records(1);
  stale.set_limit_bytes(1024);
  cedar::storage::FetchChangesResponse stale_response;
  ASSERT_TRUE(service_->FetchChanges(&stale_context, &stale, &stale_response).ok());
  EXPECT_EQ(stale_response.error_code(), cedar::storage::CDC_STALE_EPOCH);

  grpc::ServerContext limit_context;
  cedar::storage::FetchChangesRequest oversized;
  oversized.set_partition_id(8);
  oversized.set_expected_epoch(1);
  oversized.set_limit_records(UINT32_MAX);
  oversized.set_limit_bytes(UINT64_MAX);
  cedar::storage::FetchChangesResponse limit_response;
  ASSERT_TRUE(service_->FetchChanges(&limit_context, &oversized, &limit_response).ok());
  EXPECT_EQ(limit_response.error_code(), cedar::storage::CDC_INVALID_LIMIT);
}

TEST_F(StorageCdcRpcTest, GetChangeLogStateReturnsWatermarks) {
  CommitRecords(2);

  grpc::ServerContext context;
  cedar::storage::GetChangeLogStateRequest request;
  request.set_partition_id(8);
  request.set_expected_epoch(1);
  cedar::storage::GetChangeLogStateResponse response;

  auto status = service_->GetChangeLogState(&context, &request, &response);

  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_TRUE(response.success()) << response.error_msg();
  EXPECT_EQ(response.partition_epoch(), 1);
  EXPECT_EQ(response.earliest_offset(), 1);
  EXPECT_EQ(response.high_watermark(), 2);
  EXPECT_EQ(response.committed_version(), 700);
}

TEST_F(StorageCdcRpcTest, GetComputeSnapshotRejectsInvalidLimits) {
  CommitRecords(1);
  auto stub = StartGrpcServer();

  grpc::ClientContext context;
  cedar::storage::GetComputeSnapshotRequest request;
  request.set_partition_id(8);
  request.set_limit_records(0);
  request.set_limit_bytes(1024);
  request.set_expected_epoch(1);

  auto reader = stub->GetComputeSnapshot(&context, request);
  cedar::storage::ComputeSnapshotBatch batch;
  EXPECT_FALSE(reader->Read(&batch));
  auto status = reader->Finish();

  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(StorageCdcRpcTest, GetComputeSnapshotRejectsExpiredDeadline) {
  CommitRecords(1);
  auto stub = StartGrpcServer();

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() -
                       std::chrono::seconds(1));
  cedar::storage::GetComputeSnapshotRequest request;
  request.set_partition_id(8);
  request.set_limit_records(1);
  request.set_limit_bytes(1024);
  request.set_expected_epoch(1);

  auto reader = stub->GetComputeSnapshot(&context, request);
  cedar::storage::ComputeSnapshotBatch batch;
  EXPECT_FALSE(reader->Read(&batch));
  auto status = reader->Finish();

  EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
}

TEST_F(StorageCdcRpcTest, GetComputeSnapshotStreamsBoundedFinalBatchWithChecksum) {
  CommitRecords(3);
  auto stub = StartGrpcServer();

  grpc::ClientContext context;
  cedar::storage::GetComputeSnapshotRequest request;
  request.set_partition_id(8);
  request.set_resume_offset(1);
  request.set_limit_records(2);
  request.set_limit_bytes(1 << 20);
  request.set_expected_epoch(1);

  auto reader = stub->GetComputeSnapshot(&context, request);
  cedar::storage::ComputeSnapshotBatch batch;
  ASSERT_TRUE(reader->Read(&batch));
  EXPECT_FALSE(reader->Read(&batch));
  auto status = reader->Finish();

  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(batch.partition_id(), 8);
  EXPECT_EQ(batch.partition_epoch(), 1);
  EXPECT_EQ(batch.snapshot_version(), 700);
  EXPECT_EQ(batch.resume_offset(), 1);
  EXPECT_EQ(batch.sequence(), 0);
  ASSERT_EQ(batch.records_size(), 2);
  EXPECT_EQ(batch.records(0).offset(), 2);
  EXPECT_EQ(batch.records(1).offset(), 3);
  EXPECT_TRUE(batch.final());
  EXPECT_NE(batch.checksum(), 0u);
}

TEST_F(StorageCdcRpcTest, GetComputeSnapshotDoesNotMarkTruncatedBatchFinal) {
  CommitRecords(5);
  auto stub = StartGrpcServer();

  grpc::ClientContext context;
  cedar::storage::GetComputeSnapshotRequest request;
  request.set_partition_id(8);
  request.set_limit_records(2);
  request.set_limit_bytes(1 << 20);
  request.set_expected_epoch(1);

  auto reader = stub->GetComputeSnapshot(&context, request);
  cedar::storage::ComputeSnapshotBatch first;
  cedar::storage::ComputeSnapshotBatch second;
  cedar::storage::ComputeSnapshotBatch third;
  ASSERT_TRUE(reader->Read(&first));
  ASSERT_TRUE(reader->Read(&second));
  ASSERT_TRUE(reader->Read(&third));
  EXPECT_FALSE(reader->Read(&third));
  auto status = reader->Finish();

  ASSERT_TRUE(status.ok()) << status.error_message();
  ASSERT_EQ(first.records_size(), 2);
  EXPECT_FALSE(first.final());
  ASSERT_EQ(second.records_size(), 2);
  EXPECT_FALSE(second.final());
  ASSERT_EQ(third.records_size(), 1);
  EXPECT_TRUE(third.final());
  EXPECT_EQ(first.sequence(), 0);
  EXPECT_EQ(second.sequence(), 1);
  EXPECT_EQ(third.sequence(), 2);
  EXPECT_EQ(third.records(0).offset(), 5);
}

TEST_F(StorageCdcRpcTest, GetComputeSnapshotHonorsRequestedSnapshotVersion) {
  CommitRecords(2, 100, 700, 1000);
  CommitRecords(2, 101, 900, 2000);
  auto stub = StartGrpcServer();

  grpc::ClientContext context;
  cedar::storage::GetComputeSnapshotRequest request;
  request.set_partition_id(8);
  request.set_snapshot_version(700);
  request.set_limit_records(4);
  request.set_limit_bytes(1 << 20);
  request.set_expected_epoch(1);

  auto reader = stub->GetComputeSnapshot(&context, request);
  cedar::storage::ComputeSnapshotBatch batch;
  ASSERT_TRUE(reader->Read(&batch));
  EXPECT_FALSE(reader->Read(&batch));
  auto status = reader->Finish();

  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(batch.snapshot_version(), 700);
  ASSERT_EQ(batch.records_size(), 2);
  EXPECT_EQ(batch.records(0).commit_version(), 700);
  EXPECT_EQ(batch.records(1).commit_version(), 700);
  EXPECT_TRUE(batch.final());
}

TEST_F(StorageCdcRpcTest, GetComputeSnapshotRejectsStaleEpoch) {
  CommitRecords(1);
  auto stub = StartGrpcServer();

  grpc::ClientContext context;
  cedar::storage::GetComputeSnapshotRequest request;
  request.set_partition_id(8);
  request.set_limit_records(1);
  request.set_limit_bytes(1024);
  request.set_expected_epoch(999);

  auto reader = stub->GetComputeSnapshot(&context, request);
  cedar::storage::ComputeSnapshotBatch batch;
  EXPECT_FALSE(reader->Read(&batch));
  auto status = reader->Finish();

  EXPECT_EQ(status.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
}

}  // namespace
