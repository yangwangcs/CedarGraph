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
