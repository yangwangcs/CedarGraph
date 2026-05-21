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
#include <thread>

#include <grpcpp/grpcpp.h>

#include "cedar/dtx/storage/partition_migrator.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/service/partition_migration_service.h"
#include "cedar/storage/cedar_graph_storage.h"

namespace cedar {
namespace dtx {
namespace storage {

class MigrationChecksumVerifyTest : public ::testing::Test {
 protected:
  std::string source_data_dir_;
  std::string target_data_dir_;
  cedar::CedarGraphStorage* source_storage_ = nullptr;
  cedar::CedarGraphStorage* target_storage_ = nullptr;
  std::unique_ptr<StoragePartitionManager> source_partition_manager_;
  std::unique_ptr<StoragePartitionManager> target_partition_manager_;
  std::unique_ptr<PartitionMigrator> source_migrator_;
  std::unique_ptr<PartitionMigrator> target_migrator_;
  std::unique_ptr<cedar::service::PartitionMigrationServiceImpl> service_impl_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::string server_address_;
  int port_ = 0;
  std::shared_ptr<cedar::migration::PartitionMigrationService::Stub> stub_;

  Status VerifyConsistency(MigrationTask& task) {
    return source_migrator_->VerifyConsistency(task);
  }

  void SetUp() override {
    // Setup source storage
    source_data_dir_ = "/tmp/test_migration_checksum_verify_src_" +
                       std::to_string(
                           std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(source_data_dir_);
    std::filesystem::create_directories(source_data_dir_);

    cedar::CedarOptions source_options;
    source_options.create_if_missing = true;
    source_options.enable_accumulated_flush = false;
    auto status =
        cedar::CedarGraphStorage::Open(source_options, source_data_dir_, &source_storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(source_storage_, nullptr);

    source_partition_manager_ = std::make_unique<StoragePartitionManager>();
    StoragePartitionManager::PartitionConfig source_config;
    source_config.data_root = source_data_dir_;
    ASSERT_TRUE(source_partition_manager_->Initialize(source_config).ok());
    ASSERT_TRUE(source_partition_manager_->AddPartition(1).ok());

    source_migrator_ = std::make_unique<PartitionMigrator>();
    MigrationConfig source_mcfg;
    source_mcfg.verify_checksum = true;
    ASSERT_TRUE(source_migrator_->Initialize(source_mcfg).ok());
    source_migrator_->SetStoragePartitionManager(source_partition_manager_.get());

    // Setup target storage
    target_data_dir_ = "/tmp/test_migration_checksum_verify_tgt_" +
                       std::to_string(
                           std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(target_data_dir_);
    std::filesystem::create_directories(target_data_dir_);

    cedar::CedarOptions target_options;
    target_options.create_if_missing = true;
    target_options.enable_accumulated_flush = false;
    status =
        cedar::CedarGraphStorage::Open(target_options, target_data_dir_, &target_storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(target_storage_, nullptr);

    target_partition_manager_ = std::make_unique<StoragePartitionManager>();
    StoragePartitionManager::PartitionConfig target_config;
    target_config.data_root = target_data_dir_;
    ASSERT_TRUE(target_partition_manager_->Initialize(target_config).ok());
    ASSERT_TRUE(target_partition_manager_->AddPartition(1).ok());

    target_migrator_ = std::make_unique<PartitionMigrator>();
    MigrationConfig target_mcfg;
    target_mcfg.verify_checksum = true;
    ASSERT_TRUE(target_migrator_->Initialize(target_mcfg).ok());
    target_migrator_->SetStoragePartitionManager(target_partition_manager_.get());

    // Setup gRPC server with target migrator
    service_impl_ =
        std::make_unique<cedar::service::PartitionMigrationServiceImpl>();
    service_impl_->SetPartitionMigrator(target_migrator_.get());

    server_address_ = "127.0.0.1:0";
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(service_impl_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    std::ostringstream oss;
    oss << "127.0.0.1:" << port_;
    server_address_ = oss.str();

    server_thread_ = std::thread([this]() { server_->Wait(); });

    auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    stub_ = cedar::migration::PartitionMigrationService::NewStub(channel);

    source_migrator_->SetMigrationServiceStub(stub_);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }

    if (source_migrator_) {
      source_migrator_->Shutdown();
      source_migrator_.reset();
    }
    if (target_migrator_) {
      target_migrator_->Shutdown();
      target_migrator_.reset();
    }

    source_partition_manager_.reset();
    target_partition_manager_.reset();

    if (source_storage_) {
      delete source_storage_;
      source_storage_ = nullptr;
    }
    if (target_storage_) {
      delete target_storage_;
      target_storage_ = nullptr;
    }

    std::filesystem::remove_all(source_data_dir_);
    std::filesystem::remove_all(target_data_dir_);
  }
};

TEST_F(MigrationChecksumVerifyTest, MatchingChecksumsPass) {
  auto source_partition = source_partition_manager_->GetPartition(1);
  auto target_partition = target_partition_manager_->GetPartition(1);
  ASSERT_NE(source_partition, nullptr);
  ASSERT_NE(target_partition, nullptr);

  // Write identical data to both source and target
  cedar::CedarKey k1(100, cedar::EntityType::Vertex, 1, cedar::Timestamp(100), 0, 0, 0, 0);
  cedar::Descriptor d = cedar::Descriptor::InlineInt(1, 42);
  Status s = source_partition->Put(k1, d, cedar::Timestamp(2), 0);
  ASSERT_TRUE(s.ok()) << "Source put failed: " << s.ToString();
  s = target_partition->Put(k1, d, cedar::Timestamp(2), 0);
  ASSERT_TRUE(s.ok()) << "Target put failed: " << s.ToString();

  MigrationTask task;
  task.partition_id = 1;
  task.source_node = 1;
  task.target_node = 2;

  auto status = VerifyConsistency(task);
  EXPECT_TRUE(status.ok()) << "Expected OK but got: " << status.ToString();
}

TEST_F(MigrationChecksumVerifyTest, MismatchedChecksumsFail) {
  auto source_partition = source_partition_manager_->GetPartition(1);
  auto target_partition = target_partition_manager_->GetPartition(1);
  ASSERT_NE(source_partition, nullptr);
  ASSERT_NE(target_partition, nullptr);

  // Write different data to source and target
  cedar::CedarKey k1(100, cedar::EntityType::Vertex, 1, cedar::Timestamp(100), 0, 0, 0, 0);
  cedar::Descriptor d1 = cedar::Descriptor::InlineInt(1, 42);
  Status s = source_partition->Put(k1, d1, cedar::Timestamp(2), 0);
  ASSERT_TRUE(s.ok());

  cedar::CedarKey k2(200, cedar::EntityType::Vertex, 2, cedar::Timestamp(300), 0, 0, 0, 0);
  cedar::Descriptor d2 = cedar::Descriptor::InlineInt(2, 99);
  s = target_partition->Put(k2, d2, cedar::Timestamp(4), 0);
  ASSERT_TRUE(s.ok());

  MigrationTask task;
  task.partition_id = 1;
  task.source_node = 1;
  task.target_node = 2;

  auto status = VerifyConsistency(task);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsCorruption()) << "Expected corruption but got: " << status.ToString();
}

TEST_F(MigrationChecksumVerifyTest, VerifyChecksumDisabledSkipsFetch) {
  // Disable checksum verification on source migrator
  source_migrator_->Shutdown();
  source_migrator_.reset();
  source_migrator_ = std::make_unique<PartitionMigrator>();
  MigrationConfig mcfg;
  mcfg.verify_checksum = false;
  ASSERT_TRUE(source_migrator_->Initialize(mcfg).ok());
  source_migrator_->SetStoragePartitionManager(source_partition_manager_.get());
  source_migrator_->SetMigrationServiceStub(stub_);

  MigrationTask task;
  task.partition_id = 1;
  task.source_node = 1;
  task.target_node = 2;

  auto status = VerifyConsistency(task);
  EXPECT_TRUE(status.ok());
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
