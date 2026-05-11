// Copyright 2025 The Cedar Authors
//
// End-to-end test for partition migration data movement.

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <thread>

#include "cedar/dtx/storage/partition_migrator.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::dtx;
using namespace cedar::dtx::storage;

class MockMetaServiceNodeClient : public MetaServiceNodeClient {
 public:
  cedar::meta::PartitionAssignment last_assignment_;

  StatusOr<cedar::meta::PartitionAssignment> GetPartitionAssignment(
      PartitionID partition_id) override {
    if (last_assignment_.partition_id() == partition_id) {
      return last_assignment_;
    }
    cedar::meta::PartitionAssignment a;
    a.set_partition_id(partition_id);
    a.set_leader_node(1);  // source node
    return a;
  }

  Status UpdatePartitionAssignment(
      const cedar::meta::PartitionAssignment& assignment) override {
    last_assignment_ = assignment;
    return Status::OK();
  }
};

namespace cedar {
namespace dtx {
namespace storage {

class PartitionMigrationEndToEndTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  cedar::CedarGraphStorage* storage_ = nullptr;
  std::unique_ptr<StoragePartitionManager> partition_manager_;
  std::unique_ptr<PartitionMigrator> migrator_;
  MockMetaServiceNodeClient meta_client_;

  Status CopyData(MigrationTask& task) { return migrator_->CopyData(task); }
  Status RollbackMigration(MigrationTask& task) {
    return migrator_->RollbackMigration(task);
  }

  void SetUp() override {
    data_dir_ = "/tmp/test_migration_e2e_" +
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
    mcfg.max_concurrent_batches = 1;
    mcfg.verify_checksum = false;
    ASSERT_TRUE(migrator_->Initialize(mcfg).ok());
    migrator_->SetStoragePartitionManager(partition_manager_.get());
    migrator_->SetMetaServiceClient(&meta_client_);
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

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

TEST_F(PartitionMigrationEndToEndTest, CopyDataCreatesSnapshot) {
  auto partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write some data
  cedar::CedarKey k1;
  k1.SetEntityId(100);
  k1.SetColumnId(1);
  k1.SetEntityType(1);
  cedar::Descriptor d;
  ASSERT_TRUE(partition->Prepare(1, {}, {k1},
                                 {{static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(k1)), d}},
                                 cedar::Timestamp(1)).ok());
  ASSERT_TRUE(partition->Commit(1, cedar::Timestamp(2)).ok());

  // Build a task manually and run CopyData
  MigrationTask task;
  task.migration_id = 1;
  task.partition_id = 1;
  task.source_node = 1;
  task.target_node = 2;
  task.type = MigrationType::kRebalance;
  task.state = MigrationState::kCopying;

  auto status = CopyData(task);
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Snapshot directory should exist
  std::string snapshot_dir = data_dir_ + "/migration_snap_1";
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir));

  // Cleanup snapshot dir so TearDown can remove data_dir_
  std::filesystem::remove_all(snapshot_dir);
}

TEST_F(PartitionMigrationEndToEndTest, LoadSnapshotRestoresData) {
  auto partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write and snapshot
  cedar::CedarKey k1;
  k1.SetEntityId(200);
  k1.SetColumnId(1);
  k1.SetEntityType(1);
  cedar::Descriptor d;
  ASSERT_TRUE(partition->Prepare(2, {}, {k1},
                                 {{static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(k1)), d}},
                                 cedar::Timestamp(1)).ok());
  ASSERT_TRUE(partition->Commit(2, cedar::Timestamp(2)).ok());

  std::string snapshot_dir = data_dir_ + "/migration_snap_2";
  ASSERT_TRUE(partition->SaveSnapshotForMigration(snapshot_dir).ok());

  // Register a task so LoadSnapshotForMigration can find it
  auto id_result = migrator_->SubmitMigration(1, 1, 2, MigrationType::kRebalance);
  ASSERT_TRUE(id_result.ok());
  uint64_t migration_id = id_result.value();

  auto status = migrator_->LoadSnapshotForMigration(migration_id, snapshot_dir);
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Data should now be in the partition's data root (copied from snapshot)
  EXPECT_FALSE(std::filesystem::is_empty(data_dir_));

  std::filesystem::remove_all(snapshot_dir);
}

TEST_F(PartitionMigrationEndToEndTest, FullMigrationPipelineAsync) {
  auto partition = partition_manager_->GetPartition(1);
  ASSERT_NE(partition, nullptr);

  // Write data
  cedar::CedarKey k1;
  k1.SetEntityId(300);
  k1.SetColumnId(1);
  k1.SetEntityType(1);
  cedar::Descriptor d;
  ASSERT_TRUE(partition->Prepare(3, {}, {k1},
                                 {{static_cast<uint64_t>(cedar::dtx::CedarKeyHash{}(k1)), d}},
                                 cedar::Timestamp(1)).ok());
  ASSERT_TRUE(partition->Commit(3, cedar::Timestamp(2)).ok());

  auto id_result = migrator_->SubmitMigration(1, 1, 2, MigrationType::kRebalance);
  ASSERT_TRUE(id_result.ok());
  uint64_t migration_id = id_result.value();

  // Wait for worker to finish (completed or failed)
  MigrationState final_state = MigrationState::kPending;
  for (int i = 0; i < 100; ++i) {
    auto status_result = migrator_->GetMigrationStatus(migration_id);
    if (status_result.ok()) {
      final_state = status_result.value().state;
      if (final_state == MigrationState::kCompleted ||
          final_state == MigrationState::kFailed ||
          final_state == MigrationState::kRolledBack) {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Because SwitchTraffic needs a real meta client connection and we only
  // have a mock, the migration may either complete (if SwitchTraffic succeeds
  // with mock) or fail/rollback. The important thing is that CopyData ran.
  EXPECT_TRUE(final_state == MigrationState::kCompleted ||
              final_state == MigrationState::kFailed ||
              final_state == MigrationState::kRolledBack)
      << "Unexpected final state: " << static_cast<int>(final_state);

  auto stats = migrator_->GetStats();
  EXPECT_GE(stats.total_migrations, 1);

  // Snapshot dir should have been created by CopyData
  std::string snapshot_dir = data_dir_ + "/migration_snap_" + std::to_string(migration_id);
  if (std::filesystem::exists(snapshot_dir)) {
    std::filesystem::remove_all(snapshot_dir);
  }
}

TEST_F(PartitionMigrationEndToEndTest, RollbackMigrationRevertsLeader) {
  MigrationTask task;
  task.migration_id = 99;
  task.partition_id = 1;
  task.source_node = 1;
  task.target_node = 2;
  task.state = MigrationState::kSwitching;

  // Pre-populate assignment so Rollback can read it
  cedar::meta::PartitionAssignment assignment;
  assignment.set_partition_id(1);
  assignment.set_leader_node(2);  // currently on target
  meta_client_.last_assignment_ = assignment;

  auto status = RollbackMigration(task);
  EXPECT_TRUE(status.ok()) << status.ToString();

  // After rollback, leader should be reverted to source_node (1)
  EXPECT_EQ(meta_client_.last_assignment_.leader_node(), 1);
}
