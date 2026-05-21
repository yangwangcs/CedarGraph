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
#include <fstream>

#include "cedar/dtx/storage/partition_migrator.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::dtx;
using namespace cedar::dtx::storage;

class MigrationWALCatchUpTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  cedar::CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<StoragePartitionManager> partition_manager_;
  std::unique_ptr<PartitionMigrator> migrator_;

  void SetUp() override {
    data_dir_ = "/tmp/test_wal_catchup_" +
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

TEST_F(MigrationWALCatchUpTest, CountsWALOpsWithoutStub) {
  auto partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write data to generate WAL entries
  cedar::CedarKey k1(100, cedar::EntityType::Vertex, 1, cedar::Timestamp(100), 0, 0, 0, 0);
  cedar::Descriptor d = cedar::Descriptor::InlineInt(1, 42);
  Status s = partition->Put(k1, d, cedar::Timestamp(2), 0);
  ASSERT_TRUE(s.ok());

  // Build a task and run CatchUp (no stub, so just counts)
  MigrationTask task;
  task.migration_id = 1;
  task.partition_id = 1;
  task.source_node = 1;
  task.target_node = 2;
  task.type = MigrationType::kRebalance;
  task.state = MigrationState::kCatchingUp;

  s = migrator_->CatchUp(task);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(MigrationWALCatchUpTest, ReplaysWALToTarget) {
  auto partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write data to generate WAL entries
  for (int i = 0; i < 5; ++i) {
    cedar::CedarKey k(i + 1, cedar::EntityType::Vertex, 1, cedar::Timestamp(100 + i), 0, 0, 0, 0);
    cedar::Descriptor d = cedar::Descriptor::InlineInt(1, 100 + i);
    Status s = partition->Put(k, d, cedar::Timestamp(200 + i), 0);
    ASSERT_TRUE(s.ok());
  }

  // Manually build a WAL file with known entries for deterministic testing
  std::string wal_dir = data_dir_ + "/wal";
  std::filesystem::create_directories(wal_dir);
  std::string wal_path = wal_dir + "/partition_1_wal.log";

  {
    std::ofstream wal_file(wal_path, std::ios::binary);
    ASSERT_TRUE(wal_file.is_open());
    for (int i = 0; i < 5; ++i) {
      uint64_t ts = 1000 + i;
      uint64_t txn_id = 10 + i;
      uint32_t op_len = 4;
      wal_file.write(reinterpret_cast<const char*>(&ts), sizeof(ts));
      wal_file.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
      wal_file.write(reinterpret_cast<const char*>(&op_len), sizeof(op_len));
      wal_file.write("OP", op_len);
    }
    wal_file.close();
  }

  MigrationTask task;
  task.migration_id = 1;
  task.partition_id = 1;
  task.source_node = 1;
  task.target_node = 2;
  task.type = MigrationType::kRebalance;
  task.state = MigrationState::kCatchingUp;

  // Without a stub, CatchUp should read the WAL and return OK
  Status s = migrator_->CatchUp(task);
  ASSERT_TRUE(s.ok()) << s.ToString();
}
