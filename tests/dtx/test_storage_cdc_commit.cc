#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"

#include "cdc_service.pb.h"

namespace {

class StorageCdcCommitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    data_dir_ = (std::filesystem::temp_directory_path() /
                 ("cedar_storage_cdc_commit_" +
                  std::to_string(std::chrono::steady_clock::now()
                                     .time_since_epoch()
                                     .count())))
                    .string();
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    cedar::CedarOptions options;
    options.create_if_missing = true;
    options.enable_accumulated_flush = false;
    auto status = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);

    partition_manager_ = std::make_unique<cedar::dtx::StoragePartitionManager>();
    cedar::dtx::StoragePartitionManager::PartitionConfig config;
    config.data_root = data_dir_;
    config.enable_cdc = true;
    ASSERT_TRUE(partition_manager_->Initialize(config).ok());
    ASSERT_TRUE(partition_manager_->AddPartition(7).ok());
    partition_ = partition_manager_->GetPartition(7);
    ASSERT_NE(partition_, nullptr);

    change_log_ = partition_manager_->GetChangeLog(7);
    ASSERT_NE(change_log_, nullptr);
  }

  void TearDown() override {
    partition_manager_.reset();
    delete storage_;
    storage_ = nullptr;
    std::filesystem::remove_all(data_dir_);
  }

  cedar::CedarKey Key(uint64_t entity_id, uint32_t column_id = 1) const {
    return cedar::CedarKey(entity_id, cedar::EntityType::Vertex, column_id,
                           cedar::Timestamp(100 + entity_id), 0, 0, 0, 7);
  }

  std::vector<cedar::CedarKey> MakeWrites(size_t count) const {
    std::vector<cedar::CedarKey> writes;
    for (size_t i = 0; i < count; ++i) {
      writes.push_back(Key(100 + i, static_cast<uint32_t>(i + 1)));
    }
    return writes;
  }

  std::unordered_map<uint64_t, cedar::Descriptor> Descriptors(
      const std::vector<cedar::CedarKey>& writes) const {
    std::unordered_map<uint64_t, cedar::Descriptor> descriptors;
    for (size_t i = 0; i < writes.size(); ++i) {
      descriptors[static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(writes[i]))] =
          cedar::Descriptor::InlineInt(static_cast<uint32_t>(i + 1),
                                       static_cast<int64_t>(42 + i));
    }
    return descriptors;
  }

  void WritePendingIntent(uint64_t txn_id, uint64_t commit_version,
                          const std::vector<cedar::CedarKey>& writes) {
    auto intent_dir = std::filesystem::path(data_dir_) / "cdc_intents" /
                      "partition_7";
    std::filesystem::create_directories(intent_dir);
    std::ofstream out(intent_dir / ("txn_" + std::to_string(txn_id) + ".intent"),
                      std::ios::binary | std::ios::trunc);
    const std::string magic = "CEDAR_CDC_INTENT_V1";
    uint32_t magic_size = static_cast<uint32_t>(magic.size());
    uint32_t record_count = static_cast<uint32_t>(writes.size());
    out.write(reinterpret_cast<const char*>(&magic_size), sizeof(magic_size));
    out.write(magic.data(), magic.size());
    out.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    out.write(reinterpret_cast<const char*>(&commit_version),
              sizeof(commit_version));
    out.write(reinterpret_cast<const char*>(&record_count), sizeof(record_count));
    for (size_t i = 0; i < writes.size(); ++i) {
      const auto& key = writes[i];
      cedar::cdc::ChangeRecord record;
      record.set_txn_id(txn_id);
      record.set_entity_id(key.entity_id());
      record.set_target_id(key.target_id());
      record.set_entity_type(static_cast<uint32_t>(key.entity_type()));
      record.set_column_id(key.column_id());
      record.set_operation(cedar::cdc::CHANGE_OPERATION_UPDATE);
      record.set_valid_from(commit_version);
      auto desc = cedar::Descriptor::InlineInt(
          static_cast<uint32_t>(i + 1), static_cast<int64_t>(42 + i));
      uint64_t raw_descriptor = desc.AsRaw();
      record.set_payload(
          std::string(reinterpret_cast<const char*>(&raw_descriptor),
                      sizeof(raw_descriptor)));
      std::string serialized;
      ASSERT_TRUE(record.SerializeToString(&serialized));
      uint32_t size = static_cast<uint32_t>(serialized.size());
      uint64_t storage_timestamp = key.timestamp().value();
      out.write(reinterpret_cast<const char*>(&storage_timestamp),
                sizeof(storage_timestamp));
      out.write(reinterpret_cast<const char*>(&size), sizeof(size));
      out.write(serialized.data(), serialized.size());
    }
    ASSERT_TRUE(out.good());
  }

  void RestartPartitionManager() {
    partition_manager_.reset();
    partition_manager_ = std::make_unique<cedar::dtx::StoragePartitionManager>();
    cedar::dtx::StoragePartitionManager::PartitionConfig config;
    config.data_root = data_dir_;
    config.enable_cdc = true;
    ASSERT_TRUE(partition_manager_->Initialize(config).ok());
    ASSERT_TRUE(partition_manager_->AddPartition(7).ok());
    partition_ = partition_manager_->GetPartition(7);
    ASSERT_NE(partition_, nullptr);
    change_log_ = partition_manager_->GetChangeLog(7);
    ASSERT_NE(change_log_, nullptr);
  }

  std::string data_dir_;
  cedar::CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<cedar::dtx::StoragePartitionManager> partition_manager_;
  cedar::dtx::PartitionStorage* partition_ = nullptr;
  cedar::cdc::PartitionChangeLog* change_log_ = nullptr;
};

TEST_F(StorageCdcCommitTest, AbortedPrepareDoesNotAdvanceWatermark) {
  auto writes = MakeWrites(2);
  ASSERT_TRUE(partition_->Prepare(11, {}, writes, Descriptors(writes),
                                  cedar::Timestamp(400))
                  .ok());
  ASSERT_TRUE(partition_->Abort(11).ok());
  EXPECT_EQ(change_log_->GetState().high_watermark, 0);
}

TEST_F(StorageCdcCommitTest, CommitAdvancesDataAndCdcTogether) {
  auto writes = MakeWrites(2);
  ASSERT_TRUE(partition_->Prepare(12, {}, writes, Descriptors(writes),
                                  cedar::Timestamp(500))
                  .ok());
  ASSERT_TRUE(partition_->Commit(12, cedar::Timestamp(500)).ok());
  EXPECT_EQ(change_log_->GetState().high_watermark, 2);
  EXPECT_EQ(change_log_->GetState().committed_version, 500);
  auto records = change_log_->ReadAfter(0, 10, 1024 * 1024);
  ASSERT_TRUE(records.ok()) << records.status().ToString();
  ASSERT_EQ(records.value().size(), 2);
  EXPECT_EQ(records.value()[0].txn_id(), 12);
  EXPECT_EQ(records.value()[0].entity_id(), writes[0].entity_id());
  EXPECT_EQ(records.value()[1].entity_id(), writes[1].entity_id());

  auto found = partition_->Get(writes[0], cedar::Timestamp(500));
  EXPECT_TRUE(found.ok()) << found.status().ToString();
  EXPECT_FALSE(std::filesystem::exists(
      std::filesystem::path(data_dir_) / "cdc_intents" / "partition_7" /
      "txn_12.intent"));
}

TEST_F(StorageCdcCommitTest, ReplayedCommitIsIdempotent) {
  auto writes = MakeWrites(1);
  ASSERT_TRUE(partition_->Prepare(13, {}, writes, Descriptors(writes),
                                  cedar::Timestamp(501))
                  .ok());
  ASSERT_TRUE(partition_->Commit(13, cedar::Timestamp(501)).ok());
  ASSERT_TRUE(partition_->Commit(13, cedar::Timestamp(501)).ok());
  EXPECT_EQ(change_log_->GetState().high_watermark, 1);
}

TEST_F(StorageCdcCommitTest, RestartReplaysPendingCdcIntentOnce) {
  auto writes = MakeWrites(2);
  WritePendingIntent(14, 600, writes);

  RestartPartitionManager();

  EXPECT_EQ(change_log_->GetState().high_watermark, 2);
  EXPECT_EQ(change_log_->GetState().committed_version, 600);
  auto records = change_log_->ReadAfter(0, 10, 1024 * 1024);
  ASSERT_TRUE(records.ok()) << records.status().ToString();
  ASSERT_EQ(records.value().size(), 2);
  EXPECT_EQ(records.value()[0].txn_id(), 14);
  EXPECT_EQ(records.value()[1].entity_id(), writes[1].entity_id());
  auto found = partition_->Get(writes[0], cedar::Timestamp(600));
  EXPECT_TRUE(found.ok()) << found.status().ToString();

  RestartPartitionManager();

  EXPECT_EQ(change_log_->GetState().high_watermark, 2);
  EXPECT_FALSE(std::filesystem::exists(
      std::filesystem::path(data_dir_) / "cdc_intents" / "partition_7" /
      "txn_14.intent"));
}

}  // namespace
