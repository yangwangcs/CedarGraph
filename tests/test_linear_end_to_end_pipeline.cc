// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/types.h"

using namespace cedar;
using namespace cedar::dtx;

class LinearEndToEndPipelineTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<StoragePartitionManager> partition_manager_;

  void SetUp() override {
    data_dir_ = "/tmp/cedar_linear_e2e_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    CedarOptions options;
    options.create_if_missing = true;
    options.enable_accumulated_flush = false;
    auto status = CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);

    partition_manager_ = std::make_unique<StoragePartitionManager>();
    StoragePartitionManager::PartitionConfig config;
    config.data_root = data_dir_;
    ASSERT_TRUE(partition_manager_->Initialize(config).ok());
    ASSERT_TRUE(partition_manager_->AddPartition(1).ok());
  }

  void TearDown() override {
    partition_manager_->Shutdown();
    partition_manager_.reset();
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};

TEST_F(LinearEndToEndPipelineTest, WriteViaTwoPhaseCommit) {
  auto* partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Construct a CedarKey for entity 123, column 1, type Vertex
  CedarKey key;
  key.SetEntityId(123);
  key.SetColumnId(1);
  key.SetEntityType(1);  // Vertex

  // Prepare transaction data
  TxnID txn_id = 42;
  std::vector<CedarKey> read_set;
  std::vector<CedarKey> write_set = {key};

  std::unordered_map<uint64_t, Descriptor> write_descriptors;
  write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, 42);

  Timestamp prepare_ts(1000);
  Timestamp commit_ts(2000);

  // Phase 1: Prepare
  auto status = partition->Prepare(txn_id, read_set, write_set, write_descriptors, prepare_ts);
  EXPECT_TRUE(status.ok()) << "Prepare failed: " << status.ToString();

  // Phase 2: Commit
  status = partition->Commit(txn_id, commit_ts);
  EXPECT_TRUE(status.ok()) << "Commit failed: " << status.ToString();

  // Verify no prepared transactions remain
  EXPECT_TRUE(partition->GetPreparedTransactions().empty());
}

TEST_F(LinearEndToEndPipelineTest, FlushAndRestartPreservesData) {
  auto* partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write data
  CedarKey key;
  key.SetEntityId(456);
  key.SetColumnId(1);
  key.SetEntityType(1);
  key.SetPartId(1);

  TxnID txn_id = 100;
  std::vector<CedarKey> write_set = {key};
  std::unordered_map<uint64_t, Descriptor> write_descriptors;
  write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, 99);

  ASSERT_TRUE(partition->Prepare(txn_id, {}, write_set, write_descriptors, Timestamp(1000)).ok());
  ASSERT_TRUE(partition->Commit(txn_id, Timestamp(2000)).ok());

  // Flush to disk
  ASSERT_TRUE(partition->GetEffectiveStorage()->ForceFlush().ok());

  // Simulate process restart: close everything
  partition_manager_->Shutdown();
  partition_manager_.reset();
  delete storage_;
  storage_ = nullptr;

  // Reopen storage
  CedarOptions options;
  options.create_if_missing = false;
  auto status = CedarGraphStorage::Open(options, data_dir_, &storage_);
  ASSERT_TRUE(status.ok()) << "Reopen failed: " << status.ToString();
  ASSERT_NE(storage_, nullptr);

  // Recreate partition manager and re-add partition
  partition_manager_ = std::make_unique<StoragePartitionManager>();
  StoragePartitionManager::PartitionConfig config;
  config.data_root = data_dir_;
  ASSERT_TRUE(partition_manager_->Initialize(config).ok());
  ASSERT_TRUE(partition_manager_->AddPartition(1).ok());

  // Read back
  auto* partition_after_restart = partition_manager_->GetPartition(1);
  ASSERT_NE(partition_after_restart, nullptr);

  std::vector<std::pair<Timestamp, Descriptor>> versions;
  status = partition_after_restart->GetEffectiveStorage()->ScanNode(456, Timestamp::Max(), &versions);
  ASSERT_TRUE(status.ok()) << "ScanNode failed: " << status.ToString();
  ASSERT_FALSE(versions.empty()) << "No versions found after restart";

  // Verify latest version
  const auto& latest = versions.back();
  EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
  EXPECT_EQ(latest.second.GetPayload(), 99);
}

TEST_F(LinearEndToEndPipelineTest, CompleteLinearPipeline) {
  // ========================================================================
  // Phase 1: Setup (already done in SetUp)
  // ========================================================================
  auto* partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // ========================================================================
  // Phase 2: Write (2PC Prepare + Commit)
  // ========================================================================
  CedarKey key;
  key.SetEntityId(789);
  key.SetColumnId(1);
  key.SetEntityType(1);
  key.SetPartId(1);  // Must match partition ID

  const int32_t expected_value = 12345;
  TxnID txn_id = 999;
  std::vector<CedarKey> write_set = {key};
  std::unordered_map<uint64_t, Descriptor> write_descriptors;
  write_descriptors[CedarKeyHash{}(key)] = Descriptor::InlineInt(0, expected_value);

  Timestamp prepare_ts(1000);
  Timestamp commit_ts(2000);

  ASSERT_TRUE(partition->Prepare(txn_id, {}, write_set, write_descriptors, prepare_ts).ok());
  ASSERT_TRUE(partition->Commit(txn_id, commit_ts).ok());

  // ========================================================================
  // Phase 3: In-Memory Read Verification
  // ========================================================================
  {
    std::vector<std::pair<Timestamp, Descriptor>> versions;
    auto status = partition->GetEffectiveStorage()->ScanNode(789, Timestamp::Max(), &versions);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(versions.empty());

    const auto& latest = versions.back();
    EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
    EXPECT_EQ(latest.second.GetPayload(), expected_value);
  }

  // ========================================================================
  // Phase 4: Flush to Disk
  // ========================================================================
  ASSERT_TRUE(partition->GetEffectiveStorage()->ForceFlush().ok());

  // ========================================================================
  // Phase 5: Restart (simulate crash + recovery)
  // ========================================================================
  partition_manager_->Shutdown();
  partition_manager_.reset();
  delete storage_;
  storage_ = nullptr;

  CedarOptions reopen_options;
  reopen_options.create_if_missing = false;
  auto status = CedarGraphStorage::Open(reopen_options, data_dir_, &storage_);
  ASSERT_TRUE(status.ok()) << "Storage reopen failed: " << status.ToString();

  partition_manager_ = std::make_unique<StoragePartitionManager>();
  StoragePartitionManager::PartitionConfig config;
  config.data_root = data_dir_;
  ASSERT_TRUE(partition_manager_->Initialize(config).ok());
  ASSERT_TRUE(partition_manager_->AddPartition(1).ok());
  
  // Update partition pointer after restart
  partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // ========================================================================
  // Phase 6: Post-Restart Read Verification
  // ========================================================================
  {
    std::vector<std::pair<Timestamp, Descriptor>> versions;
    auto status = partition->GetEffectiveStorage()->ScanNode(789, Timestamp::Max(), &versions);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(versions.empty()) << "Data lost after restart!";

    const auto& latest = versions.back();
    EXPECT_EQ(latest.second.GetKind(), EntryKind::InlineInt);
    EXPECT_EQ(latest.second.GetPayload(), expected_value)
        << "Value corruption detected after restart!";
  }

  // ========================================================================
  // Phase 7: WAL Recovery Verification
  // ========================================================================
  auto* partition_after_restart = partition_manager_->GetPartition(1);
  ASSERT_NE(partition_after_restart, nullptr);
  EXPECT_TRUE(partition_after_restart->GetPreparedTransactions().empty());
}
