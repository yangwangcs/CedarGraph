#include "cedar/gcn/storage_cdc_client.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>

#include "cdc_service.pb.h"
#include "storage_service.grpc.pb.h"

namespace {

cedar::cdc::ChangeRecord MakeRecord(uint32_t partition_id, uint64_t offset,
                                    uint64_t epoch = 7) {
  cedar::cdc::ChangeRecord record;
  record.set_partition_id(partition_id);
  record.set_partition_epoch(epoch);
  record.set_offset(offset);
  record.set_commit_version(1000 + offset);
  record.set_txn_id(2000 + offset);
  record.set_batch_index(0);
  record.set_batch_size(1);
  record.set_entity_id(3000 + offset);
  record.set_operation(cedar::cdc::CHANGE_OPERATION_CREATE);
  record.set_payload(std::string(8, 'x'));
  return record;
}

std::vector<cedar::cdc::ChangeRecord> MakeRecords(uint32_t partition_id,
                                                  uint64_t first_offset,
                                                  int count) {
  std::vector<cedar::cdc::ChangeRecord> records;
  for (int i = 0; i < count; ++i) {
    records.push_back(MakeRecord(partition_id, first_offset + i));
  }
  return records;
}

cedar::storage::ComputeSnapshotBatch MakeBatch(uint32_t partition_id,
                                               uint64_t sequence,
                                               bool final) {
  cedar::storage::ComputeSnapshotBatch batch;
  batch.set_partition_id(partition_id);
  batch.set_partition_epoch(7);
  batch.set_snapshot_version(900);
  batch.set_sequence(sequence);
  batch.set_resume_offset(sequence);
  *batch.add_records() = MakeRecord(partition_id, sequence + 1);
  batch.set_final(final);
  batch.set_checksum(1);
  return batch;
}

class FakeStorageService final : public cedar::storage::StorageService::Service {
 public:
  grpc::Status GetChangeLogState(
      grpc::ServerContext* context,
      const cedar::storage::GetChangeLogStateRequest* request,
      cedar::storage::GetChangeLogStateResponse* response) override {
    std::lock_guard<std::mutex> lock(mu_);
    ++get_state_count_;
    last_state_request_ = *request;
    state_deadline_seen_ = context->deadline() > std::chrono::system_clock::now();
    if (!grpc_state_status_.ok()) {
      return grpc_state_status_;
    }
    *response = state_response_;
    return grpc::Status::OK;
  }

  grpc::Status FetchChanges(
      grpc::ServerContext* context,
      const cedar::storage::FetchChangesRequest* request,
      cedar::storage::FetchChangesResponse* response) override {
    std::unique_lock<std::mutex> lock(mu_);
    ++fetch_count_;
    last_fetch_request_ = *request;
    last_fetch_authorization_ = AuthorizationHeader(context);
    fetch_deadline_seen_ = context->deadline() > std::chrono::system_clock::now();
    if (block_fetch_) {
      fetch_started_ = true;
      cv_.notify_all();
      cv_.wait(lock, [&] { return unblock_fetch_; });
    }
    if (!grpc_fetch_status_.ok()) {
      return grpc_fetch_status_;
    }
    *response = fetch_response_;
    return grpc::Status::OK;
  }

  grpc::Status GetComputeSnapshot(
      grpc::ServerContext* context,
      const cedar::storage::GetComputeSnapshotRequest* request,
      grpc::ServerWriter<cedar::storage::ComputeSnapshotBatch>* writer) override {
    std::lock_guard<std::mutex> lock(mu_);
    ++snapshot_count_;
    last_snapshot_request_ = *request;
    snapshot_deadline_seen_ =
        context->deadline() > std::chrono::system_clock::now();
    if (!grpc_snapshot_status_.ok()) {
      return grpc_snapshot_status_;
    }
    for (const auto& batch : snapshot_batches_) {
      if (!writer->Write(batch)) {
        return grpc::Status(grpc::StatusCode::CANCELLED, "client closed stream");
      }
    }
    return grpc::Status::OK;
  }

  void SetFetchRecords(std::vector<cedar::cdc::ChangeRecord> records) {
    std::lock_guard<std::mutex> lock(mu_);
    fetch_response_.set_success(true);
    fetch_response_.set_error_code(cedar::storage::CDC_OK);
    fetch_response_.set_partition_id(3);
    fetch_response_.set_partition_epoch(7);
    fetch_response_.set_high_watermark(records.empty() ? 0 : records.back().offset());
    fetch_response_.set_next_offset(records.empty() ? 0 : records.back().offset());
    fetch_response_.clear_records();
    for (const auto& record : records) {
      *fetch_response_.add_records() = record;
    }
  }

  void SetStateOk() {
    std::lock_guard<std::mutex> lock(mu_);
    state_response_.set_success(true);
    state_response_.set_error_code(cedar::storage::CDC_OK);
    state_response_.set_partition_id(3);
    state_response_.set_partition_epoch(7);
    state_response_.set_high_watermark(42);
  }

  void SetFetchCdcError(cedar::storage::CdcErrorCode error_code) {
    std::lock_guard<std::mutex> lock(mu_);
    fetch_response_.Clear();
    fetch_response_.set_success(false);
    fetch_response_.set_error_code(error_code);
    fetch_response_.set_error_msg("fake cdc error");
  }

  void SetStateCdcError(cedar::storage::CdcErrorCode error_code) {
    std::lock_guard<std::mutex> lock(mu_);
    state_response_.Clear();
    state_response_.set_success(false);
    state_response_.set_error_code(error_code);
    state_response_.set_error_msg("fake state error");
  }

  void SetSnapshotStatus(grpc::Status status) {
    std::lock_guard<std::mutex> lock(mu_);
    grpc_snapshot_status_ = std::move(status);
  }

  void SetFetchStatus(grpc::Status status) {
    std::lock_guard<std::mutex> lock(mu_);
    grpc_fetch_status_ = std::move(status);
  }

  void SetSnapshotBatches(
      std::vector<cedar::storage::ComputeSnapshotBatch> batches) {
    std::lock_guard<std::mutex> lock(mu_);
    snapshot_batches_ = std::move(batches);
  }

  void BlockFetch() {
    std::lock_guard<std::mutex> lock(mu_);
    block_fetch_ = true;
  }

  void WaitForFetchStarted() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&] { return fetch_started_; });
  }

  void UnblockFetch() {
    std::lock_guard<std::mutex> lock(mu_);
    unblock_fetch_ = true;
    cv_.notify_all();
  }

  cedar::storage::FetchChangesRequest last_fetch_request() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_fetch_request_;
  }

  cedar::storage::GetChangeLogStateRequest last_state_request() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_state_request_;
  }

  cedar::storage::GetComputeSnapshotRequest last_snapshot_request() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_snapshot_request_;
  }

  int fetch_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return fetch_count_;
  }

  int get_state_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return get_state_count_;
  }

  bool fetch_deadline_seen() const {
    std::lock_guard<std::mutex> lock(mu_);
    return fetch_deadline_seen_;
  }

  bool state_deadline_seen() const {
    std::lock_guard<std::mutex> lock(mu_);
    return state_deadline_seen_;
  }

  bool snapshot_deadline_seen() const {
    std::lock_guard<std::mutex> lock(mu_);
    return snapshot_deadline_seen_;
  }

  std::string last_fetch_authorization() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_fetch_authorization_;
  }

 private:
  static std::string AuthorizationHeader(grpc::ServerContext* context) {
    auto it = context->client_metadata().find("authorization");
    if (it == context->client_metadata().end()) {
      return "";
    }
    return std::string(it->second.data(), it->second.length());
  }

  mutable std::mutex mu_;
  std::condition_variable cv_;
  cedar::storage::GetChangeLogStateRequest last_state_request_;
  cedar::storage::FetchChangesRequest last_fetch_request_;
  cedar::storage::GetComputeSnapshotRequest last_snapshot_request_;
  std::string last_fetch_authorization_;
  cedar::storage::GetChangeLogStateResponse state_response_;
  cedar::storage::FetchChangesResponse fetch_response_;
  std::vector<cedar::storage::ComputeSnapshotBatch> snapshot_batches_;
  grpc::Status grpc_state_status_;
  grpc::Status grpc_fetch_status_;
  grpc::Status grpc_snapshot_status_;
  int get_state_count_ = 0;
  int fetch_count_ = 0;
  int snapshot_count_ = 0;
  bool state_deadline_seen_ = false;
  bool fetch_deadline_seen_ = false;
  bool snapshot_deadline_seen_ = false;
  bool block_fetch_ = false;
  bool fetch_started_ = false;
  bool unblock_fetch_ = false;
};

class StorageCdcClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    fake_ = std::make_unique<FakeStorageService>();
    fake_->SetStateOk();
    fake_->SetFetchRecords(MakeRecords(3, 3, 3));
    fake_->SetSnapshotBatches({MakeBatch(3, 0, false), MakeBatch(3, 1, true)});

    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port_);
    builder.RegisterService(fake_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port_, 0);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port_),
                                       grpc::InsecureChannelCredentials());
    cedar::gcn::StorageCdcClient::Options options;
    options.rpc_timeout = std::chrono::milliseconds(250);
    options.max_records = 1024;
    options.max_bytes = 4 * 1024 * 1024;
    options.auth_token_provider = [] { return std::string("cdc-token"); };
    client_ = std::make_unique<cedar::gcn::StorageCdcClient>(channel, options);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
  }

  std::unique_ptr<FakeStorageService> fake_;
  std::unique_ptr<grpc::Server> server_;
  int port_ = 0;
  std::unique_ptr<cedar::gcn::StorageCdcClient> client_;
};

TEST_F(StorageCdcClientTest, FetchSendsConfiguredBoundsAndReturnsRecords) {
  auto result = client_->Fetch(3, 2, 7);

  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto request = fake_->last_fetch_request();
  EXPECT_EQ(request.partition_id(), 3u);
  EXPECT_EQ(request.after_offset(), 2u);
  EXPECT_EQ(request.expected_epoch(), 7u);
  EXPECT_EQ(request.limit_records(), 1024u);
  EXPECT_EQ(request.limit_bytes(), 4u * 1024u * 1024u);
  ASSERT_EQ(result.ValueOrDie().records_size(), 3);
  EXPECT_EQ(result.ValueOrDie().records(0).offset(), 3u);
  EXPECT_TRUE(fake_->fetch_deadline_seen());
}

TEST_F(StorageCdcClientTest, FetchAttachesBearerTokenMetadata) {
  auto result = client_->Fetch(3, 2, 7);

  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(fake_->last_fetch_authorization(), "Bearer cdc-token");
}

TEST_F(StorageCdcClientTest, GetStateSendsExpectedEpochAndUsesFreshContexts) {
  auto first = client_->GetState(3, 7);
  auto second = client_->GetState(3, 7);

  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  auto request = fake_->last_state_request();
  EXPECT_EQ(request.partition_id(), 3u);
  EXPECT_EQ(request.expected_epoch(), 7u);
  EXPECT_EQ(fake_->get_state_count(), 2);
  EXPECT_TRUE(fake_->state_deadline_seen());
}

TEST_F(StorageCdcClientTest, StreamSnapshotInvokesCallbackForBatches) {
  std::vector<uint64_t> sequences;

  auto status = client_->StreamSnapshot(
      3, 900,
      [&](const cedar::storage::ComputeSnapshotBatch& batch) {
        sequences.push_back(batch.sequence());
        return cedar::Status::OK();
      });

  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(sequences.size(), 2u);
  EXPECT_EQ(sequences[0], 0u);
  EXPECT_EQ(sequences[1], 1u);
  auto request = fake_->last_snapshot_request();
  EXPECT_EQ(request.partition_id(), 3u);
  EXPECT_EQ(request.snapshot_version(), 900u);
  EXPECT_EQ(request.limit_records(), 1024u);
  EXPECT_EQ(request.limit_bytes(), 4u * 1024u * 1024u);
  EXPECT_TRUE(fake_->snapshot_deadline_seen());
}

TEST_F(StorageCdcClientTest, StreamSnapshotStopsOnCallbackError) {
  int callbacks = 0;

  auto status = client_->StreamSnapshot(
      3, 900,
      [&](const cedar::storage::ComputeSnapshotBatch&) {
        ++callbacks;
        return cedar::Status::InvalidArgument("stop snapshot");
      });

  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_EQ(callbacks, 1);
}

TEST_F(StorageCdcClientTest, StaleEpochMapsDistinctlyAndDoesNotRetry) {
  fake_->SetFetchCdcError(cedar::storage::CDC_STALE_EPOCH);

  auto result = client_->Fetch(3, 2, 7);

  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsConflict()) << result.status().ToString();
  EXPECT_EQ(fake_->fetch_count(), 1);
}

TEST_F(StorageCdcClientTest, InvalidLimitResponseMapsToInvalidArgument) {
  fake_->SetFetchCdcError(cedar::storage::CDC_INVALID_LIMIT);

  auto result = client_->Fetch(3, 2, 7);

  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalidArgument()) << result.status().ToString();
}

TEST_F(StorageCdcClientTest, MalformedFetchStatusFieldsAreRejected) {
  fake_->SetFetchCdcError(cedar::storage::CDC_OK);

  auto result = client_->Fetch(3, 2, 7);

  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsCorruption()) << result.status().ToString();
}

TEST_F(StorageCdcClientTest, MalformedStateStatusFieldsAreRejected) {
  fake_->SetStateCdcError(cedar::storage::CDC_OK);

  auto result = client_->GetState(3, 7);

  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsCorruption()) << result.status().ToString();
}

TEST_F(StorageCdcClientTest, OversizedServerResponseIsRejected) {
  fake_->SetFetchRecords(MakeRecords(3, 3, 1025));

  auto result = client_->Fetch(3, 2, 7);

  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsResourceExhausted()) << result.status().ToString();
}

TEST_F(StorageCdcClientTest, DeadlineUnavailableMapsToUnavailable) {
  fake_->BlockFetch();
  std::thread unblocker([&] {
    fake_->WaitForFetchStarted();
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    fake_->UnblockFetch();
  });

  auto result = client_->Fetch(3, 2, 7);

  unblocker.join();
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsUnavailable()) << result.status().ToString();
  EXPECT_EQ(fake_->fetch_count(), 1);
}

TEST_F(StorageCdcClientTest, GrpcUnavailableMapsToUnavailable) {
  fake_->SetFetchStatus(
      grpc::Status(grpc::StatusCode::UNAVAILABLE, "temporary outage"));

  auto result = client_->Fetch(3, 2, 7);

  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsUnavailable()) << result.status().ToString();
}

}  // namespace
