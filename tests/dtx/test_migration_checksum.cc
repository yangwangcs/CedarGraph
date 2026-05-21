// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>

#include "cedar/dtx/storage/partition_migrator.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"

namespace cedar {
namespace dtx {
namespace storage {

class MigrationChecksumTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  cedar::CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<StoragePartitionManager> partition_manager_;
  std::unique_ptr<PartitionMigrator> migrator_;

  void SetUp() override {
    data_dir_ = "/tmp/test_migration_checksum_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    cedar::CedarOptions options;
    options.create_if_missing = true;
    options.enable_accumulated_flush = false;
    auto status = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);

    partition_manager_ = std::make_unique<StoragePartitionManager>();
    StoragePartitionManager::PartitionConfig config;
    config.data_root = data_dir_;
    ASSERT_TRUE(partition_manager_->Initialize(config).ok());
    ASSERT_TRUE(partition_manager_->AddPartition(1).ok());

    migrator_ = std::make_unique<PartitionMigrator>();
    MigrationConfig mcfg;
    mcfg.verify_checksum = true;
    ASSERT_TRUE(migrator_->Initialize(mcfg).ok());
    migrator_->SetStoragePartitionManager(partition_manager_.get());
  }

  void TearDown() override {
    if (migrator_) {
      migrator_->Shutdown();
      migrator_.reset();
    }
    partition_manager_.reset();
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};

TEST_F(MigrationChecksumTest, SameDataSameChecksum) {
  auto partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write some data via direct Put (injects partition_id automatically)
  cedar::CedarKey k1(100, cedar::EntityType::Vertex, 1, cedar::Timestamp(100), 0, 0, 0, 0);
  cedar::Descriptor d = cedar::Descriptor::InlineInt(1, 42);
  Status s = partition->Put(k1, d, cedar::Timestamp(2), 0);
  ASSERT_TRUE(s.ok()) << "Put failed: " << s.ToString();

  // Calculate checksum twice — must be deterministic
  std::string chk1, chk2;
  ASSERT_TRUE(migrator_->CalculateChecksum(1, &chk1).ok());
  ASSERT_TRUE(migrator_->CalculateChecksum(1, &chk2).ok());
  EXPECT_EQ(chk1, chk2);
  EXPECT_NE(chk1, "0") << "Checksum should not be zero for non-empty partition";
}

TEST_F(MigrationChecksumTest, DifferentDataDifferentChecksum) {
  auto partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write initial data
  cedar::CedarKey k1(100, cedar::EntityType::Vertex, 1, cedar::Timestamp(100), 0, 0, 0, 0);
  cedar::Descriptor d = cedar::Descriptor::InlineInt(1, 42);
  Status s = partition->Put(k1, d, cedar::Timestamp(2), 0);
  ASSERT_TRUE(s.ok());

  std::string chk_before;
  ASSERT_TRUE(migrator_->CalculateChecksum(1, &chk_before).ok());

  // Write more data
  cedar::CedarKey k2(200, cedar::EntityType::Vertex, 2, cedar::Timestamp(300), 0, 0, 0, 0);
  cedar::Descriptor d2 = cedar::Descriptor::InlineInt(2, 99);
  s = partition->Put(k2, d2, cedar::Timestamp(4), 0);
  ASSERT_TRUE(s.ok());

  std::string chk_after;
  ASSERT_TRUE(migrator_->CalculateChecksum(1, &chk_after).ok());

  EXPECT_NE(chk_before, chk_after);
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
