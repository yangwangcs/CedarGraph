#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/storage/storaged_raft_state_machine.h"
#include <braft/storage.h>

// Test doubles for braft snapshot IO
class TestSnapshotWriter : public braft::SnapshotWriter {
 public:
  explicit TestSnapshotWriter(const std::string& path) : path_(path) {
    std::filesystem::create_directories(path_);
  }
  std::string get_path() override { return path_; }
  void list_files(std::vector<std::string>* files) override {
    *files = files_;
  }
  int save_meta(const braft::SnapshotMeta& /*meta*/) override { return 0; }
  int add_file(const std::string& filename,
               const ::google::protobuf::Message* /*file_meta*/) override {
    files_.push_back(filename);
    return 0;
  }
  int remove_file(const std::string& /*filename*/) override { return 0; }
 private:
  std::string path_;
  std::vector<std::string> files_;
};

class TestSnapshotReader : public braft::SnapshotReader {
 public:
  explicit TestSnapshotReader(const std::string& path) : path_(path) {}
  std::string get_path() override { return path_; }
  void list_files(std::vector<std::string>* /*files*/) override {}
  int load_meta(braft::SnapshotMeta* /*meta*/) override { return 0; }
  std::string generate_uri_for_copy() override {
    return "local://" + path_;
  }
 private:
  std::string path_;
};

class StorageRaftSnapshotTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  std::string snapshot_dir_;
  cedar::CedarGraphStorage* storage_ = nullptr;

  void SetUp() override {
    data_dir_ = "/tmp/test_raft_snapshot_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    snapshot_dir_ = data_dir_ + "_snapshot";
    std::filesystem::remove_all(data_dir_);
    std::filesystem::remove_all(snapshot_dir_);
    std::filesystem::create_directories(data_dir_);

    cedar::CedarOptions options;
    options.create_if_missing = true;
    options.enable_accumulated_flush = false;
    auto status = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
    std::filesystem::remove_all(snapshot_dir_);
  }
};

TEST_F(StorageRaftSnapshotTest, GetDbPathReturnsCorrectPath) {
  EXPECT_EQ(storage_->GetDbPath(), data_dir_);
}

TEST_F(StorageRaftSnapshotTest, SaveAndLoadPreparedTxnsRoundTrip) {
  std::string txn_path = data_dir_ + "/txn_state";
  auto save_status = storage_->SavePreparedTxns(txn_path);
  EXPECT_TRUE(save_status.ok()) << save_status.ToString();
  EXPECT_TRUE(std::filesystem::exists(txn_path));

  auto load_status = storage_->LoadPreparedTxns(txn_path);
  EXPECT_TRUE(load_status.ok()) << load_status.ToString();
}

TEST_F(StorageRaftSnapshotTest, RestoreFromSnapshotReplacesData) {
  // Write data and flush
  cedar::Descriptor desc = cedar::Descriptor::InlineInt(0, 42);
  auto s = storage_->PutStaticVertex(1001, 0, desc);
  EXPECT_TRUE(s.ok()) << s.ToString();
  s = storage_->ForceFlush();
  EXPECT_TRUE(s.ok()) << s.ToString();

  // Verify data exists
  auto result = storage_->GetStaticVertex(1001, 0);
  EXPECT_TRUE(result.has_value());

  // Simulate snapshot: copy data_dir_ to snapshot_dir_
  std::filesystem::create_directories(snapshot_dir_ + "/data");
  for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir_)) {
    if (entry.is_regular_file()) {
      std::string rel = std::filesystem::relative(entry.path(), data_dir_).string();
      std::string dst = snapshot_dir_ + "/data/" + rel;
      std::filesystem::create_directories(std::filesystem::path(dst).parent_path());
      std::filesystem::copy_file(entry.path(), dst);
    }
  }

  // Clear original data
  for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
    std::filesystem::remove_all(entry.path());
  }

  // Verify data is gone (engine still open, but files removed)
  // We must close and reopen to see the change, which RestoreFromSnapshot does
  s = storage_->RestoreFromSnapshot(snapshot_dir_ + "/data");
  EXPECT_TRUE(s.ok()) << s.ToString();

  // Data should be back
  result = storage_->GetStaticVertex(1001, 0);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result->AsRaw(), desc.AsRaw());
}

TEST_F(StorageRaftSnapshotTest, OnSnapshotSaveCopiesDataFiles) {
  // Write data and flush so SST files exist on disk
  cedar::Descriptor desc = cedar::Descriptor::InlineInt(0, 123);
  auto s = storage_->PutStaticVertex(2001, 1, desc);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = storage_->ForceFlush();
  ASSERT_TRUE(s.ok()) << s.ToString();

  cedar::dtx::storage::StorageRaftStateMachine sm(storage_);
  TestSnapshotWriter writer(snapshot_dir_);
  sm.on_snapshot_save(&writer, nullptr);

  // Verify snapshot data directory was created and contains files
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/data"));
  bool has_files = false;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(snapshot_dir_ + "/data")) {
    if (entry.is_regular_file()) {
      has_files = true;
      break;
    }
  }
  EXPECT_TRUE(has_files) << "Snapshot data dir should contain copied SST files";

  // Verify txn_state file was created
  EXPECT_TRUE(std::filesystem::exists(snapshot_dir_ + "/txn_state"));
}

TEST_F(StorageRaftSnapshotTest, OnSnapshotLoadRestoresDataFiles) {
  // Write data, flush, and save snapshot
  cedar::Descriptor desc = cedar::Descriptor::InlineInt(0, 456);
  auto s = storage_->PutStaticVertex(3001, 0, desc);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = storage_->ForceFlush();
  ASSERT_TRUE(s.ok()) << s.ToString();

  cedar::dtx::storage::StorageRaftStateMachine sm(storage_);
  {
    TestSnapshotWriter writer(snapshot_dir_);
    sm.on_snapshot_save(&writer, nullptr);
  }

  // Verify original data is readable
  auto result = storage_->GetStaticVertex(3001, 0);
  ASSERT_TRUE(result.has_value());

  // Clear original data directory to simulate empty follower
  for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
    std::filesystem::remove_all(entry.path());
  }

  // Data should be unreadable now (engine still has old memtable, but we force a fresh engine)
  // Instead, simulate the load path: close and reopen storage fresh
  delete storage_;
  storage_ = nullptr;
  cedar::CedarOptions options;
  options.create_if_missing = false;
  s = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Before load: data is gone
  result = storage_->GetStaticVertex(3001, 0);
  EXPECT_FALSE(result.has_value());

  // Load snapshot
  cedar::dtx::storage::StorageRaftStateMachine sm2(storage_);
  TestSnapshotReader reader(snapshot_dir_);
  int rc = sm2.on_snapshot_load(&reader);
  EXPECT_EQ(rc, 0);

  // After load: data is back
  result = storage_->GetStaticVertex(3001, 0);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(result->AsRaw(), desc.AsRaw());
}

TEST_F(StorageRaftSnapshotTest, FullSnapshotRoundTrip) {
  // Phase 1: Write multiple data points
  // Note: PutStaticVertex overwrites the descriptor's column_id with property_id,
  // so we create descriptors with matching column_ids for AsRaw() comparison.
  cedar::Descriptor d1 = cedar::Descriptor::InlineInt(1, 111);
  cedar::Descriptor d2 = cedar::Descriptor::InlineInt(1, 222);
  cedar::Descriptor d3 = cedar::Descriptor::InlineInt(2, 333);

  auto s = storage_->PutStaticVertex(4001, 1, d1);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = storage_->PutStaticVertex(4002, 1, d2);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = storage_->PutStaticVertex(4003, 2, d3);
  ASSERT_TRUE(s.ok()) << s.ToString();

  s = storage_->ForceFlush();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Verify all present
  EXPECT_TRUE(storage_->GetStaticVertex(4001, 1).has_value());
  EXPECT_TRUE(storage_->GetStaticVertex(4002, 1).has_value());
  EXPECT_TRUE(storage_->GetStaticVertex(4003, 2).has_value());

  // Phase 2: Leader saves snapshot
  cedar::dtx::storage::StorageRaftStateMachine leader_sm(storage_);
  {
    TestSnapshotWriter writer(snapshot_dir_);
    leader_sm.on_snapshot_save(&writer, nullptr);
  }

  // Phase 3: Simulate follower with empty storage
  delete storage_;
  storage_ = nullptr;
  std::filesystem::remove_all(data_dir_);
  std::filesystem::create_directories(data_dir_);

  cedar::CedarOptions options;
  options.create_if_missing = true;
  s = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Verify empty before load
  EXPECT_FALSE(storage_->GetStaticVertex(4001, 1).has_value());
  EXPECT_FALSE(storage_->GetStaticVertex(4002, 1).has_value());
  EXPECT_FALSE(storage_->GetStaticVertex(4003, 2).has_value());

  // Phase 4: Follower loads snapshot
  cedar::dtx::storage::StorageRaftStateMachine follower_sm(storage_);
  TestSnapshotReader reader(snapshot_dir_);
  int rc = follower_sm.on_snapshot_load(&reader);
  EXPECT_EQ(rc, 0);

  // Phase 5: Verify all data restored
  auto r1 = storage_->GetStaticVertex(4001, 1);
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(r1->AsRaw(), d1.AsRaw());

  auto r2 = storage_->GetStaticVertex(4002, 1);
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(r2->AsRaw(), d2.AsRaw());

  auto r3 = storage_->GetStaticVertex(4003, 2);
  ASSERT_TRUE(r3.has_value());
  EXPECT_EQ(r3->AsRaw(), d3.AsRaw());
}
