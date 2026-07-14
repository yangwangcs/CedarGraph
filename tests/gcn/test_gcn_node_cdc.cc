#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "cedar/gcn/gcn_node.h"
#include "cedar/gcn/partition_consumer.h"
#include "cedar/gcn/storage_cdc_client.h"
#include "cdc_service.pb.h"
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

class FakeNodeCdcSource final : public gcn::StorageCdcSource {
 public:
  explicit FakeNodeCdcSource(std::vector<cedar::cdc::ChangeRecord> records)
      : records_(std::move(records)) {}

  StatusOr<cedar::storage::GetChangeLogStateResponse> GetState(
      uint32_t partition_id, uint64_t expected_epoch) override {
    cedar::storage::GetChangeLogStateResponse response;
    response.set_partition_id(partition_id);
    response.set_partition_epoch(expected_epoch);
    response.set_earliest_offset(1);
    response.set_high_watermark(records_.empty() ? 0 : records_.back().offset());
    response.set_committed_version(records_.empty() ? 0
                                                   : records_.back().commit_version());
    return response;
  }

  StatusOr<cedar::storage::FetchChangesResponse> Fetch(
      uint32_t partition_id, uint64_t after_offset,
      uint64_t expected_epoch) override {
    cedar::storage::FetchChangesResponse response;
    response.set_partition_id(partition_id);
    response.set_partition_epoch(expected_epoch);
    response.set_high_watermark(records_.empty() ? 0 : records_.back().offset());
    uint64_t next_offset = after_offset;
    for (const auto& record : records_) {
      if (record.offset() > after_offset) {
        *response.add_records() = record;
        next_offset = record.offset();
        break;
      }
    }
    response.set_next_offset(next_offset);
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

 private:
  std::vector<cedar::cdc::ChangeRecord> records_;
};

bool WaitForReady(GcnNode* node, uint32_t partition_id) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    auto progress = node->GetPartitionProgress(partition_id);
    if (progress.query_ready && progress.applied_offset > 0) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

TEST(GcnNodeCdcTest, StandaloneNodeConsumesWithoutSetStorage) {
  const auto directory =
      std::filesystem::temp_directory_path() / "cedar-gcn-node-cdc-test";
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);

  auto source = std::make_shared<FakeNodeCdcSource>(
      std::vector<cedar::cdc::ChangeRecord>{MakeRecord(3, 7, 1, 99)});

  GcnNode::Options options;
  options.enable_grpc_server = false;
  options.enable_coordinator = false;
  options.enable_watermark_gc = false;
  options.checkpoint_directory = directory.string();
  options.partition_leases.push_back(
      gcn::PartitionLease{/*partition_id=*/3, /*partition_epoch=*/7,
                          /*lease_epoch=*/1});
  options.storage_cdc_sources.emplace(3, source);
  options.cdc_poll_interval = std::chrono::milliseconds(5);

  GcnNode node(options);
  ASSERT_TRUE(node.Initialize().ok());
  ASSERT_TRUE(node.Start().ok());

  EXPECT_TRUE(WaitForReady(&node, 3));
  auto progress = node.GetPartitionProgress(3);
  EXPECT_EQ(progress.partition_epoch, 7u);
  EXPECT_EQ(progress.applied_offset, 1u);
  EXPECT_EQ(progress.applied_version, 99u);
  EXPECT_TRUE(std::filesystem::exists(
      directory / "tmv_snapshots" / "partition_3.tmv"));

  EXPECT_TRUE(node.Stop().ok());
  std::filesystem::remove_all(directory);
}

TEST(GcnNodeCdcTest, StartFailureStopsAlreadyStartedConsumers) {
  const auto directory =
      std::filesystem::temp_directory_path() / "cedar-gcn-node-start-fail-test";
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);

  auto source = std::make_shared<FakeNodeCdcSource>(
      std::vector<cedar::cdc::ChangeRecord>{MakeRecord(3, 7, 1, 99)});

  GcnNode::Options options;
  options.enable_grpc_server = false;
  options.enable_coordinator = false;
  options.enable_watermark_gc = false;
  options.checkpoint_directory = directory.string();
  options.partition_leases.push_back(
      gcn::PartitionLease{/*partition_id=*/3, /*partition_epoch=*/7,
                          /*lease_epoch=*/1});
  options.partition_leases.push_back(
      gcn::PartitionLease{/*partition_id=*/4, /*partition_epoch=*/7,
                          /*lease_epoch=*/1});
  options.storage_cdc_sources.emplace(3, source);
  options.cdc_poll_interval = std::chrono::milliseconds(5);

  GcnNode node(options);
  ASSERT_TRUE(node.Initialize().ok());
  EXPECT_FALSE(node.Start().ok());

  auto progress = node.GetPartitionProgress(3);
  EXPECT_FALSE(progress.query_ready);
  EXPECT_EQ(progress.state, gcn::PartitionConsumerState::kStopped);

  EXPECT_TRUE(node.Stop().ok());
  std::filesystem::remove_all(directory);
}

}  // namespace
}  // namespace cedar
