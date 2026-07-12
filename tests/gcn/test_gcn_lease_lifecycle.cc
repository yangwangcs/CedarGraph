// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "cedar/gcn/gcn_node.h"
#include "cedar/gcn/storage_cdc_client.h"
#include "cdc_service.pb.h"
#include "meta_service.grpc.pb.h"
#include "storage_service.pb.h"

namespace cedar {
namespace {

cedar::cdc::ChangeRecord MakeRecord(uint32_t partition_id, uint64_t epoch,
                                     uint64_t offset, uint64_t version) {
  cedar::cdc::ChangeRecord record;
  record.set_partition_id(partition_id);
  record.set_partition_epoch(epoch);
  record.set_offset(offset);
  record.set_commit_version(version);
  record.set_txn_id(version);
  record.set_batch_index(0);
  record.set_batch_size(1);
  record.set_operation(cedar::cdc::CHANGE_OPERATION_CREATE);
  record.set_entity_id(42);
  record.set_target_id(100 + offset);
  record.set_edge_type(1);
  return record;
}

class FakeLeaseCdcSource final : public gcn::StorageCdcSource {
 public:
  explicit FakeLeaseCdcSource(uint32_t partition_id)
      : partition_id_(partition_id) {}

  StatusOr<cedar::storage::GetChangeLogStateResponse> GetState(
      uint32_t partition_id, uint64_t expected_epoch) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      observed_get_state_epochs.push_back(expected_epoch);
    }
    cedar::storage::GetChangeLogStateResponse response;
    response.set_partition_id(partition_id);
    response.set_partition_epoch(7);
    response.set_earliest_offset(1);
    response.set_high_watermark(1);
    response.set_committed_version(100 + partition_id_);
    return response;
  }

  StatusOr<cedar::storage::FetchChangesResponse> Fetch(
      uint32_t partition_id, uint64_t after_offset,
      uint64_t expected_epoch) override {
    cedar::storage::FetchChangesResponse response;
    response.set_partition_id(partition_id);
    response.set_partition_epoch(expected_epoch);
    response.set_high_watermark(1);
    if (after_offset < 1) {
      *response.add_records() =
          MakeRecord(partition_id, expected_epoch, 1, 100 + partition_id_);
      response.set_next_offset(1);
    } else {
      response.set_next_offset(after_offset);
    }
    return response;
  }

  Status StreamSnapshot(
      uint32_t partition_id, uint64_t snapshot_version,
      const std::function<Status(
          const cedar::storage::ComputeSnapshotBatch&)>& on_batch) override {
    cedar::storage::ComputeSnapshotBatch batch;
    batch.set_partition_id(partition_id);
    batch.set_partition_epoch(7);
    batch.set_snapshot_version(snapshot_version);
    batch.set_final(true);
    return on_batch(batch);
  }

  void Cancel() override {}

  std::vector<uint64_t> ObservedGetStateEpochs() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return observed_get_state_epochs;
  }

 private:
  uint32_t partition_id_;
  mutable std::mutex mutex_;
  std::vector<uint64_t> observed_get_state_epochs;
};

class FakeLeaseMetaService final : public cedar::meta::MetaService::Service {
 public:
  grpc::Status RegisterGcn(grpc::ServerContext*,
                           const cedar::meta::RegisterGcnRequest* request,
                           cedar::meta::RegisterGcnResponse* response) override {
    std::lock_guard<std::mutex> lock(mutex_);
    registered_gcn_id = request->gcn_id();
    response->set_success(true);
    response->set_status_code(cedar::meta::GCN_LEASE_STATUS_OK);
    return grpc::Status::OK;
  }

  grpc::Status RenewGcnLeases(
      grpc::ServerContext*,
      const cedar::meta::RenewGcnLeasesRequest* request,
      cedar::meta::RenewGcnLeasesResponse* response) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++renew_calls;
    last_progress.clear();
    rejected_progress = 0;
    for (const auto& item : request->progress()) {
      auto lease_it = leases_.find(item.partition_id());
      if (lease_it != leases_.end() &&
          lease_it->second.gcn_id == request->gcn_id() &&
          item.partition_epoch() == lease_it->second.lease_epoch) {
        last_progress.push_back(item);
      } else if (lease_it != leases_.end()) {
        ++rejected_progress;
      }
    }
    if (fail_renewals) {
      response->set_success(false);
      response->set_status_code(cedar::meta::GCN_LEASE_STATUS_INTERNAL_ERROR);
      response->set_error_msg("renewal failed");
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "renewal failed");
    }
    response->set_success(true);
    response->set_status_code(cedar::meta::GCN_LEASE_STATUS_OK);
    for (const auto& [partition_id, lease_info] : leases_) {
      auto* lease = response->add_leases();
      lease->set_partition_id(partition_id);
      lease->set_gcn_id(lease_info.gcn_id);
      lease->set_lease_epoch(lease_info.lease_epoch);
      lease->set_expires_at_ms(lease_info.expires_at_ms == 0
                                   ? NowMs() + 500
                                   : lease_info.expires_at_ms);
      lease->set_lease_token(std::to_string(partition_id) + ":" +
                             std::to_string(lease_info.lease_epoch));
    }
    cv_.notify_all();
    return grpc::Status::OK;
  }

  struct LeaseInfo {
    uint64_t gcn_id = 7;
    uint64_t lease_epoch = 7;
    uint64_t expires_at_ms = 0;
  };

  void SetLeases(std::map<uint32_t, uint64_t> leases) {
    std::map<uint32_t, LeaseInfo> converted;
    for (const auto& [partition_id, epoch] : leases) {
      converted[partition_id] = LeaseInfo{7, epoch, 0};
    }
    SetLeaseInfo(std::move(converted));
  }

  void SetLeaseInfo(std::map<uint32_t, LeaseInfo> leases) {
    std::lock_guard<std::mutex> lock(mutex_);
    leases_ = std::move(leases);
  }

  bool WaitForRenewCalls(int count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(5),
                        [&] { return renew_calls >= count; });
  }

  static uint64_t NowMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
  }

  std::mutex mutex_;
  std::condition_variable cv_;
  std::map<uint32_t, LeaseInfo> leases_;
  std::vector<cedar::meta::GcnPartitionProgress> last_progress;
  uint64_t registered_gcn_id = 0;
  int renew_calls = 0;
  int rejected_progress = 0;
  bool fail_renewals = false;
};

bool WaitForState(GcnNode* node, uint32_t partition_id,
                  gcn::PartitionConsumerState state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    if (node->GetPartitionProgress(partition_id).state == state) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

class GcnLeaseLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    directory_ = std::filesystem::temp_directory_path() /
                 ("cedar-gcn-lease-lifecycle-" +
                  std::to_string(reinterpret_cast<uintptr_t>(this)));
    std::filesystem::remove_all(directory_);
    std::filesystem::create_directories(directory_);

    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port_);
    builder.RegisterService(&meta_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    server_thread_ = std::thread([&] { server_->Wait(); });
  }

  void TearDown() override {
    if (node_) {
      node_->Stop().IgnoreError();
    }
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
    std::filesystem::remove_all(directory_);
  }

  std::string MetaAddress() const {
    std::ostringstream out;
    out << "127.0.0.1:" << port_;
    return out.str();
  }

  void StartNode() {
    GcnNode::Options options;
    options.enable_grpc_server = false;
    options.enable_coordinator = true;
    options.enable_watermark_gc = false;
    options.coordinator_endpoint = MetaAddress();
    options.use_insecure_coordinator = true;
    options.use_metad_leases = true;
    options.checkpoint_directory = directory_.string();
    options.cdc_poll_interval = std::chrono::milliseconds(5);
    options.lease_renew_interval = std::chrono::milliseconds(20);
    options.gcn_id = 7;
    options.gcn_incarnation = 42;
    options.advertised_endpoint = "gcn-7:9780";
    source3_ = std::make_shared<FakeLeaseCdcSource>(3);
    source4_ = std::make_shared<FakeLeaseCdcSource>(4);
    options.storage_cdc_sources.emplace(3, source3_);
    options.storage_cdc_sources.emplace(4, source4_);

    node_ = std::make_unique<GcnNode>(options);
    ASSERT_TRUE(node_->Initialize().ok());
    ASSERT_TRUE(node_->Start().ok());
  }

  FakeLeaseMetaService meta_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  int port_ = 0;
  std::filesystem::path directory_;
  std::unique_ptr<GcnNode> node_;
  std::shared_ptr<FakeLeaseCdcSource> source3_;
  std::shared_ptr<FakeLeaseCdcSource> source4_;
};

TEST_F(GcnLeaseLifecycleTest, StartsAndStopsConsumersWithLeaseSet) {
  meta_.SetLeases({{3, 7}, {4, 7}});
  StartNode();

  ASSERT_TRUE(WaitForState(node_.get(), 3, gcn::PartitionConsumerState::kReady));
  ASSERT_TRUE(WaitForState(node_.get(), 4, gcn::PartitionConsumerState::kReady));

  meta_.SetLeases({{4, 7}});
  ASSERT_TRUE(meta_.WaitForRenewCalls(2));
  ASSERT_TRUE(WaitForState(node_.get(), 3, gcn::PartitionConsumerState::kStopped));
  EXPECT_TRUE(node_->GetPartitionProgress(4).query_ready);
}

TEST_F(GcnLeaseLifecycleTest, StopsServingAfterRenewalDeadline) {
  meta_.SetLeases({{3, 7}});
  StartNode();
  ASSERT_TRUE(WaitForState(node_.get(), 3, gcn::PartitionConsumerState::kReady));

  {
    std::lock_guard<std::mutex> lock(meta_.mutex_);
    meta_.fail_renewals = true;
  }

  ASSERT_TRUE(WaitForState(node_.get(), 3, gcn::PartitionConsumerState::kStopped));
  EXPECT_FALSE(node_->GetPartitionProgress(3).query_ready);
}

TEST_F(GcnLeaseLifecycleTest, IgnoresLeasesOwnedByAnotherGcn) {
  meta_.SetLeaseInfo({{3, FakeLeaseMetaService::LeaseInfo{
                              /*gcn_id=*/99,
                              /*lease_epoch=*/7,
                              /*expires_at_ms=*/0}}});
  StartNode();

  ASSERT_TRUE(meta_.WaitForRenewCalls(1));
  EXPECT_TRUE(WaitForState(node_.get(), 3,
                           gcn::PartitionConsumerState::kStopped));
  EXPECT_FALSE(node_->GetPartitionProgress(3).query_ready);
}

TEST_F(GcnLeaseLifecycleTest, StopsConsumerWhenReturnedLeaseIsExpired) {
  meta_.SetLeases({{3, 7}});
  StartNode();
  ASSERT_TRUE(WaitForState(node_.get(), 3, gcn::PartitionConsumerState::kReady));

  meta_.SetLeaseInfo({{3, FakeLeaseMetaService::LeaseInfo{
                              /*gcn_id=*/7,
                              /*lease_epoch=*/7,
                              /*expires_at_ms=*/FakeLeaseMetaService::NowMs() - 1}}});
  ASSERT_TRUE(meta_.WaitForRenewCalls(2));
  ASSERT_TRUE(WaitForState(node_.get(), 3,
                           gcn::PartitionConsumerState::kStopped));
  EXPECT_FALSE(node_->GetPartitionProgress(3).query_ready);
}

TEST_F(GcnLeaseLifecycleTest, EmptyStaticLeasesDoNotRegisterUnlessMetadLeasesEnabled) {
  GcnNode::Options options;
  options.enable_grpc_server = false;
  options.enable_coordinator = true;
  options.use_insecure_coordinator = true;
  options.use_metad_leases = false;
  options.coordinator_endpoint = MetaAddress();
  options.enable_watermark_gc = false;
  options.checkpoint_directory = directory_.string();
  options.cdc_poll_interval = std::chrono::milliseconds(5);

  node_ = std::make_unique<GcnNode>(options);
  ASSERT_TRUE(node_->Initialize().ok());
  ASSERT_TRUE(node_->Start().ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::lock_guard<std::mutex> lock(meta_.mutex_);
  EXPECT_EQ(meta_.registered_gcn_id, 0u);
}

TEST_F(GcnLeaseLifecycleTest, LeaseEpochCanDifferFromStoragePartitionEpoch) {
  meta_.SetLeases({{3, 2}});
  StartNode();

  ASSERT_TRUE(WaitForState(node_.get(), 3, gcn::PartitionConsumerState::kReady));
  auto progress = node_->GetPartitionProgress(3);
  EXPECT_EQ(progress.partition_epoch, 7u);
  EXPECT_EQ(progress.lease_epoch, 2u);
  EXPECT_TRUE(progress.query_ready);

  const auto observed_epochs = source3_->ObservedGetStateEpochs();
  ASSERT_FALSE(observed_epochs.empty());
  EXPECT_EQ(observed_epochs.front(), 0u);

  ASSERT_TRUE(meta_.WaitForRenewCalls(2));
  std::lock_guard<std::mutex> lock(meta_.mutex_);
  ASSERT_EQ(meta_.last_progress.size(), 1u);
  EXPECT_EQ(meta_.last_progress[0].partition_id(), 3u);
  EXPECT_EQ(meta_.last_progress[0].partition_epoch(), 2u);
  EXPECT_EQ(meta_.rejected_progress, 0);
}

}  // namespace
}  // namespace cedar
