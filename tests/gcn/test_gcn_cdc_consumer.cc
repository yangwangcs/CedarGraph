#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cedar/gcn/checkpoint_store.h"
#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/partition_consumer.h"
#include "cedar/gcn/tmv_engine.h"
#include "cdc_service.pb.h"
#include "storage_service.pb.h"

namespace {

namespace fs = std::filesystem;
using cedar::gcn::CheckpointStore;
using cedar::gcn::Direction;
using cedar::gcn::EventApplier;
using cedar::gcn::PartitionCheckpoint;
using cedar::gcn::PartitionConsumer;
using cedar::gcn::PartitionConsumerState;
using cedar::gcn::PartitionLease;
using cedar::gcn::StorageCdcSource;
using cedar::gcn::TMVEdge;
using cedar::gcn::TMVEngine;

cedar::cdc::ChangeRecord MakeRecord(uint32_t partition_id, uint64_t epoch,
                                    uint64_t offset, uint64_t entity_id,
                                    uint64_t target_id,
                                    uint64_t commit_version) {
  cedar::cdc::ChangeRecord record;
  record.set_partition_id(partition_id);
  record.set_partition_epoch(epoch);
  record.set_offset(offset);
  record.set_commit_version(commit_version);
  record.set_txn_id(1000 + offset);
  record.set_batch_index(0);
  record.set_batch_size(1);
  record.set_entity_id(entity_id);
  record.set_target_id(target_id);
  record.set_edge_type(2);
  record.set_operation(cedar::cdc::CHANGE_OPERATION_CREATE);
  record.set_valid_from(commit_version);
  return record;
}

cedar::storage::GetChangeLogStateResponse MakeState(uint32_t partition_id,
                                                     uint64_t epoch,
                                                     uint64_t earliest_offset,
                                                     uint64_t high_watermark,
                                                     uint64_t version) {
  cedar::storage::GetChangeLogStateResponse response;
  response.set_success(true);
  response.set_error_code(cedar::storage::CDC_OK);
  response.set_partition_id(partition_id);
  response.set_partition_epoch(epoch);
  response.set_earliest_offset(earliest_offset);
  response.set_high_watermark(high_watermark);
  response.set_committed_version(version);
  return response;
}

class FakeCdcSource final : public StorageCdcSource {
 public:
  explicit FakeCdcSource(uint32_t partition_id) : partition_id_(partition_id) {}

  cedar::StatusOr<cedar::storage::GetChangeLogStateResponse> GetState(
      uint32_t partition_id, uint64_t expected_epoch) override {
    std::lock_guard<std::mutex> lock(mu_);
    state_expected_epochs_.push_back(expected_epoch);
    if (!state_status_.ok()) return state_status_;
    auto response = state_;
    response.set_partition_id(partition_id);
    return response;
  }

  cedar::StatusOr<cedar::storage::FetchChangesResponse> Fetch(
      uint32_t partition_id, uint64_t after_offset,
      uint64_t expected_epoch) override {
    std::unique_lock<std::mutex> lock(mu_);
    fetch_after_offsets_.push_back(after_offset);
    fetch_expected_epochs_.push_back(expected_epoch);
    ++fetch_calls_;
    ++active_fetches_;
    fetch_started_ = true;
    cv_.notify_all();
    if (block_fetch_) {
      cv_.wait(lock, [&] { return unblock_fetch_; });
    }
    --active_fetches_;
    if (cancelled_) return cedar::Status::Cancelled("fake source cancelled");
    if (fail_fetch_call_.has_value() && fetch_calls_ == *fail_fetch_call_) {
      fail_fetch_call_.reset();
      return cedar::Status::Unavailable("fake transient fetch failure");
    }
    if (!fetch_status_.ok()) return fetch_status_;

    cedar::storage::FetchChangesResponse response;
    response.set_success(true);
    response.set_error_code(cedar::storage::CDC_OK);
    response.set_partition_id(response_partition_id_.value_or(partition_id));
    response.set_partition_epoch(epoch_);
    response.set_earliest_offset(state_.earliest_offset());
    response.set_high_watermark(state_.high_watermark());
    response.set_committed_version(state_.committed_version());
    response.set_next_offset(after_offset);
    size_t emitted = 0;
    for (const auto& record : records_) {
      if (record.offset() > after_offset) {
        *response.add_records() = record;
        response.set_next_offset(record.offset());
        ++emitted;
        if (max_records_per_fetch_ > 0 && emitted >= max_records_per_fetch_) {
          break;
        }
      }
    }
    response.set_has_more(max_records_per_fetch_ > 0 &&
                          emitted >= max_records_per_fetch_ &&
                          response.next_offset() < state_.high_watermark());
    return response;
  }

  cedar::Status StreamSnapshot(
      uint32_t partition_id, uint64_t snapshot_version,
      const std::function<cedar::Status(
          const cedar::storage::ComputeSnapshotBatch&)>& on_batch) override {
    std::lock_guard<std::mutex> lock(mu_);
    snapshot_versions_.push_back(snapshot_version);
    if (!snapshot_status_.ok()) return snapshot_status_;
    for (const auto& batch : snapshot_batches_) {
      CEDAR_RETURN_IF_ERROR(on_batch(batch));
    }
    return cedar::Status::OK();
  }

  void Cancel() override {
    std::lock_guard<std::mutex> lock(mu_);
    cancelled_ = true;
    unblock_fetch_ = true;
    cv_.notify_all();
  }

  void SetState(uint64_t epoch, uint64_t earliest_offset,
                uint64_t high_watermark, uint64_t version) {
    std::lock_guard<std::mutex> lock(mu_);
    epoch_ = epoch;
    state_ = MakeState(partition_id_, epoch, earliest_offset, high_watermark,
                       version);
  }

  void SetRecords(std::vector<cedar::cdc::ChangeRecord> records) {
    std::lock_guard<std::mutex> lock(mu_);
    records_ = std::move(records);
  }

  void SetMaxRecordsPerFetch(size_t max_records_per_fetch) {
    std::lock_guard<std::mutex> lock(mu_);
    max_records_per_fetch_ = max_records_per_fetch;
  }

  void SetResponsePartitionId(uint32_t partition_id) {
    std::lock_guard<std::mutex> lock(mu_);
    response_partition_id_ = partition_id;
  }

  void FailFetchCallOnce(int fetch_call) {
    std::lock_guard<std::mutex> lock(mu_);
    fail_fetch_call_ = fetch_call;
  }

  void SetSnapshotBatches(
      std::vector<cedar::storage::ComputeSnapshotBatch> batches) {
    std::lock_guard<std::mutex> lock(mu_);
    snapshot_batches_ = std::move(batches);
  }

  void SetFetchStatus(cedar::Status status) {
    std::lock_guard<std::mutex> lock(mu_);
    fetch_status_ = std::move(status);
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

  std::vector<uint64_t> fetch_after_offsets() const {
    std::lock_guard<std::mutex> lock(mu_);
    return fetch_after_offsets_;
  }

  std::vector<uint64_t> snapshot_versions() const {
    std::lock_guard<std::mutex> lock(mu_);
    return snapshot_versions_;
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  uint32_t partition_id_;
  uint64_t epoch_ = 7;
  cedar::storage::GetChangeLogStateResponse state_ =
      MakeState(partition_id_, 7, 1, 0, 0);
  std::vector<cedar::cdc::ChangeRecord> records_;
  size_t max_records_per_fetch_ = 0;
  std::optional<uint32_t> response_partition_id_;
  std::optional<int> fail_fetch_call_;
  std::vector<cedar::storage::ComputeSnapshotBatch> snapshot_batches_;
  std::vector<uint64_t> state_expected_epochs_;
  std::vector<uint64_t> fetch_after_offsets_;
  std::vector<uint64_t> fetch_expected_epochs_;
  std::vector<uint64_t> snapshot_versions_;
  cedar::Status state_status_;
  cedar::Status fetch_status_;
  cedar::Status snapshot_status_;
  bool block_fetch_ = false;
  bool unblock_fetch_ = false;
  bool fetch_started_ = false;
  bool cancelled_ = false;
  int fetch_calls_ = 0;
  int active_fetches_ = 0;
};

class PartitionConsumerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    directory_ = fs::temp_directory_path() /
                 ("cedar_gcn_consumer_test_" +
                  std::to_string(reinterpret_cast<uintptr_t>(this)));
    fs::remove_all(directory_);
    fs::create_directories(directory_);
    store_ = std::make_unique<CheckpointStore>(directory_.string());
    source_ = std::make_unique<FakeCdcSource>(partition_id_);
    engine_ = std::make_unique<TMVEngine>(64);
    applier_ = std::make_unique<EventApplier>(engine_.get());
  }

  void TearDown() override { fs::remove_all(directory_); }

  PartitionConsumer MakeConsumer() {
    PartitionConsumer::Options options;
    options.poll_interval = std::chrono::milliseconds(100);
    options.initial_backoff = std::chrono::milliseconds(1);
    options.max_backoff = std::chrono::milliseconds(2);
    return PartitionConsumer(source_.get(), store_.get(), applier_.get(),
                             engine_.get(), options);
  }

  PartitionLease Lease(uint64_t epoch = 7) const {
    PartitionLease lease;
    lease.partition_id = partition_id_;
    lease.partition_epoch = epoch;
    lease.lease_epoch = 55;
    return lease;
  }

  bool WaitForState(PartitionConsumer* consumer,
                    PartitionConsumerState state) const {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
      if (consumer->GetProgress().state == state) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
  }

  void SaveCheckpoint(uint64_t epoch, uint64_t offset, uint64_t version,
                      std::string snapshot_id = "old") {
    PartitionCheckpoint checkpoint;
    checkpoint.partition_id = partition_id_;
    checkpoint.partition_epoch = epoch;
    checkpoint.applied_offset = offset;
    checkpoint.applied_version = version;
    checkpoint.tmv_snapshot_id = std::move(snapshot_id);
    ASSERT_TRUE(store_->Save(checkpoint).ok());
  }

  cedar::storage::ComputeSnapshotBatch SnapshotBatch(uint64_t sequence,
                                                     bool final) const {
    cedar::storage::ComputeSnapshotBatch batch;
    batch.set_partition_id(partition_id_);
    batch.set_partition_epoch(7);
    batch.set_snapshot_version(500);
    batch.set_resume_offset(sequence);
    batch.set_sequence(sequence);
    *batch.add_records() = MakeRecord(partition_id_, 7, 1 + sequence,
                                      100 + sequence, 200 + sequence,
                                      300 + sequence);
    batch.set_final(final);
    return batch;
  }

  const uint32_t partition_id_ = 3;
  fs::path directory_;
  std::unique_ptr<CheckpointStore> store_;
  std::unique_ptr<FakeCdcSource> source_;
  std::unique_ptr<TMVEngine> engine_;
  std::unique_ptr<EventApplier> applier_;
};

TEST_F(PartitionConsumerTest,
       RestartFromDurableCheckpointContinuesFromLastAppliedOffset) {
  SaveCheckpoint(7, 2, 102);
  source_->SetState(7, 1, 3, 103);
  source_->SetRecords({MakeRecord(partition_id_, 7, 3, 10, 20, 103)});
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kReady));

  auto progress = consumer.GetProgress();
  EXPECT_EQ(progress.state, PartitionConsumerState::kReady);
  EXPECT_TRUE(progress.query_ready);
  EXPECT_EQ(progress.applied_offset, 3u);
  EXPECT_EQ(source_->fetch_after_offsets().front(), 2u);
  auto loaded = store_->Load(partition_id_);
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  ASSERT_TRUE(loaded.ValueOrDie().has_value());
  EXPECT_EQ(loaded.ValueOrDie()->applied_offset, 3u);
  EXPECT_EQ(loaded.ValueOrDie()->applied_version, 103u);
}

TEST_F(PartitionConsumerTest, DuplicateDeliveryDoesNotDoubleApply) {
  SaveCheckpoint(7, 1, 101);
  source_->SetState(7, 1, 2, 102);
  auto duplicate = MakeRecord(partition_id_, 7, 1, 10, 20, 101);
  auto next = MakeRecord(partition_id_, 7, 2, 10, 30, 102);
  source_->SetRecords({duplicate, next});
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kReady));

  EXPECT_EQ(applier_->AppliedOffset(partition_id_), 2u);
  auto edges = engine_->ScanAtTime(10, Direction::kOut, 102);
  ASSERT_EQ(edges.size(), 1u);
  EXPECT_EQ(edges[0].target_id, 30u);
}

TEST_F(PartitionConsumerTest, UsesFetchNextOffsetForPagedTransactionBatch) {
  SaveCheckpoint(7, 0, 0);
  source_->SetState(7, 1, 2, 102);
  auto first = MakeRecord(partition_id_, 7, 1, 10, 20, 102);
  first.set_txn_id(99);
  first.set_batch_index(0);
  first.set_batch_size(2);
  auto second = MakeRecord(partition_id_, 7, 2, 10, 30, 102);
  second.set_txn_id(99);
  second.set_batch_index(1);
  second.set_batch_size(2);
  source_->SetRecords({first, second});
  source_->SetMaxRecordsPerFetch(1);
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kReady));

  EXPECT_EQ((std::vector<uint64_t>{0, 1}), source_->fetch_after_offsets());
  EXPECT_EQ(applier_->AppliedOffset(partition_id_), 2u);
  auto edges = engine_->ScanAtTime(10, Direction::kOut, 102);
  ASSERT_EQ(edges.size(), 2u);
}

TEST_F(PartitionConsumerTest, TransientFailureDuringPagedBatchRecovers) {
  SaveCheckpoint(7, 0, 0);
  source_->SetState(7, 1, 2, 102);
  auto first = MakeRecord(partition_id_, 7, 1, 10, 20, 102);
  first.set_txn_id(99);
  first.set_batch_index(0);
  first.set_batch_size(2);
  auto second = MakeRecord(partition_id_, 7, 2, 10, 30, 102);
  second.set_txn_id(99);
  second.set_batch_index(1);
  second.set_batch_size(2);
  source_->SetRecords({first, second});
  source_->SetMaxRecordsPerFetch(1);
  source_->FailFetchCallOnce(2);
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kReady));

  EXPECT_EQ((std::vector<uint64_t>{0, 1, 0, 1}),
            source_->fetch_after_offsets());
  EXPECT_EQ(applier_->AppliedOffset(partition_id_), 2u);
  auto loaded = store_->Load(partition_id_);
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  ASSERT_TRUE(loaded.ValueOrDie().has_value());
  EXPECT_EQ(loaded.ValueOrDie()->applied_offset, 2u);
}

TEST_F(PartitionConsumerTest, MisroutedFetchPartitionFailsSafely) {
  SaveCheckpoint(7, 0, 0);
  source_->SetState(7, 1, 1, 101);
  source_->SetRecords({MakeRecord(partition_id_, 7, 1, 10, 20, 101)});
  source_->SetResponsePartitionId(partition_id_ + 1);
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kFailed));
  EXPECT_FALSE(consumer.GetProgress().query_ready);
  EXPECT_TRUE(applier_->AppliedOffset(partition_id_) == 0);
}

TEST_F(PartitionConsumerTest, MisroutedFetchRecordFailsSafely) {
  SaveCheckpoint(7, 0, 0);
  source_->SetState(7, 1, 1, 101);
  source_->SetRecords({MakeRecord(partition_id_ + 1, 7, 1, 10, 20, 101)});
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kFailed));
  EXPECT_FALSE(consumer.GetProgress().query_ready);
  EXPECT_TRUE(applier_->AppliedOffset(partition_id_) == 0);
  EXPECT_TRUE(applier_->AppliedOffset(partition_id_ + 1) == 0);
}

TEST_F(PartitionConsumerTest,
       StaleCheckpointOrExpiredOffsetTriggersSnapshotRecovery) {
  SaveCheckpoint(7, 1, 101);
  source_->SetState(7, 5, 8, 500);
  source_->SetSnapshotBatches({SnapshotBatch(0, true)});
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kReady));

  auto progress = consumer.GetProgress();
  EXPECT_EQ(progress.state, PartitionConsumerState::kReady);
  EXPECT_TRUE(progress.query_ready);
  EXPECT_EQ(source_->snapshot_versions(), std::vector<uint64_t>{500});
  EXPECT_TRUE(source_->fetch_after_offsets().empty());
  auto loaded = store_->Load(partition_id_);
  ASSERT_TRUE(loaded.ok()) << loaded.status().ToString();
  ASSERT_TRUE(loaded.ValueOrDie().has_value());
  EXPECT_EQ(loaded.ValueOrDie()->applied_offset, 8u);
  EXPECT_EQ(loaded.ValueOrDie()->applied_version, 500u);
  EXPECT_NE(loaded.ValueOrDie()->tmv_snapshot_id.find("snapshot-"),
            std::string::npos);
}

TEST_F(PartitionConsumerTest, SnapshotRecoveryReplacesOldPartitionEntities) {
  SaveCheckpoint(7, 2, 102);
  source_->SetState(7, 5, 8, 500);
  ASSERT_TRUE(applier_->SeedPartitionProgress(partition_id_, 2, 102).ok());
  ASSERT_TRUE(applier_->ApplyChangeRecord(
      MakeRecord(partition_id_, 7, 3, 999, 888, 103)).ok());
  source_->SetSnapshotBatches({SnapshotBatch(0, true)});
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kReady));

  EXPECT_TRUE(engine_->ScanAtTime(999, Direction::kOut, 500).empty());
  EXPECT_TRUE(engine_->ScanAtTime(888, Direction::kIn, 500).empty());
  EXPECT_FALSE(engine_->ScanAtTime(100, Direction::kOut, 500).empty());
}

TEST_F(PartitionConsumerTest, StaleEpochMarksConsumerFailed) {
  SaveCheckpoint(7, 0, 0);
  source_->SetState(7, 1, 1, 101);
  source_->SetFetchStatus(cedar::Status::Conflict("stale epoch"));
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kFailed));

  auto progress = consumer.GetProgress();
  EXPECT_EQ(progress.state, PartitionConsumerState::kFailed);
  EXPECT_FALSE(progress.query_ready);
  EXPECT_NE(progress.error.find("stale epoch"), std::string::npos);
}

TEST_F(PartitionConsumerTest, BoundedStopCompletesWithinDeadline) {
  SaveCheckpoint(7, 0, 0);
  source_->SetState(7, 1, 1, 101);
  source_->BlockFetch();
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  source_->WaitForFetchStarted();
  const auto started = std::chrono::steady_clock::now();

  auto stop_status = consumer.Stop(std::chrono::milliseconds(20));

  const auto elapsed = std::chrono::steady_clock::now() - started;
  EXPECT_TRUE(stop_status.ok()) << stop_status.ToString();
  EXPECT_LT(elapsed, std::chrono::milliseconds(100));
  EXPECT_EQ(consumer.GetProgress().state, PartitionConsumerState::kStopped);
}

TEST_F(PartitionConsumerTest, NullSourceStartAndStopAreSafe) {
  PartitionConsumer::Options options;
  PartitionConsumer consumer(nullptr, store_.get(), applier_.get(),
                             engine_.get(), options);

  auto start_status = consumer.Start(Lease());
  EXPECT_TRUE(start_status.IsInvalidArgument()) << start_status.ToString();
  EXPECT_TRUE(consumer.Stop(std::chrono::milliseconds(1)).ok());
}

TEST_F(PartitionConsumerTest, NormalCatchUpTransitionsToReady) {
  SaveCheckpoint(7, 0, 0);
  source_->SetState(7, 1, 2, 102);
  source_->SetRecords({MakeRecord(partition_id_, 7, 1, 10, 20, 101),
                       MakeRecord(partition_id_, 7, 2, 11, 21, 102)});
  auto consumer = MakeConsumer();

  ASSERT_TRUE(consumer.Start(Lease()).ok());
  ASSERT_TRUE(WaitForState(&consumer, PartitionConsumerState::kReady));

  auto progress = consumer.GetProgress();
  EXPECT_EQ(progress.state, PartitionConsumerState::kReady);
  EXPECT_TRUE(progress.query_ready);
  EXPECT_EQ(progress.applied_offset, 2u);
  EXPECT_EQ(progress.applied_version, 102u);
}

}  // namespace
