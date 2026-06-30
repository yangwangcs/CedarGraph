#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include <unistd.h>

#include "cedar/backup/backup_manager.h"

namespace {

namespace fs = std::filesystem;

class BackupManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::temp_directory_path() /
            ("cedar_backup_test_" + std::to_string(::getpid()) + "_" +
             std::to_string(counter_++));
    storage_dir_ = root_ / "storage";
    backup_dir_ = root_ / "backups";
    fs::create_directories(storage_dir_ / "wal");
    WriteFile(storage_dir_ / "000001.sst", "sst-one");
    WriteFile(storage_dir_ / "wal" / "000001.wal", "wal-one");
    WriteFile(storage_dir_ / "property_names.meta", "name\nage\n");
  }

  void TearDown() override {
    std::error_code ec;
    fs::remove_all(root_, ec);
  }

  static void WriteFile(const fs::path& path, const std::string& data) {
    std::ofstream out(path, std::ios::binary);
    ASSERT_TRUE(out.is_open());
    out << data;
  }

  fs::path root_;
  fs::path storage_dir_;
  fs::path backup_dir_;
  inline static int counter_ = 0;
};

TEST_F(BackupManagerTest, FullBackupWritesManifestAndVerifiesChecksum) {
  cedar::BackupConfig config;
  config.backup_dir = backup_dir_.string();
  cedar::BackupManager manager(config);

  cedar::BackupMetadata metadata;
  ASSERT_TRUE(manager.CreateFullBackup(storage_dir_.string(), &metadata).ok());

  EXPECT_FALSE(metadata.backup_id.empty());
  EXPECT_FALSE(metadata.checksum.empty());
  EXPECT_EQ(metadata.sst_count, 1);
  EXPECT_EQ(metadata.wal_count, 1);
  EXPECT_GT(metadata.total_size_bytes, 0);
  EXPECT_TRUE(fs::exists(backup_dir_ / metadata.backup_id / "backup.meta"));
  EXPECT_TRUE(manager.VerifyBackup(metadata.backup_id).ok());
}

TEST_F(BackupManagerTest, ConsecutiveBackupsUseUniqueDirectories) {
  cedar::BackupConfig config;
  config.backup_dir = backup_dir_.string();
  cedar::BackupManager manager(config);

  cedar::BackupMetadata first;
  cedar::BackupMetadata second;
  ASSERT_TRUE(manager.CreateFullBackup(storage_dir_.string(), &first).ok());
  ASSERT_TRUE(manager.CreateFullBackup(storage_dir_.string(), &second).ok());

  EXPECT_NE(first.backup_id, second.backup_id);
  EXPECT_TRUE(fs::exists(backup_dir_ / first.backup_id));
  EXPECT_TRUE(fs::exists(backup_dir_ / second.backup_id));
}

TEST_F(BackupManagerTest, VerifyDetectsTamperedBackupData) {
  cedar::BackupConfig config;
  config.backup_dir = backup_dir_.string();
  cedar::BackupManager manager(config);

  cedar::BackupMetadata metadata;
  ASSERT_TRUE(manager.CreateFullBackup(storage_dir_.string(), &metadata).ok());

  WriteFile(backup_dir_ / metadata.backup_id / "sst" / "000001.sst", "tampered");

  auto status = manager.VerifyBackup(metadata.backup_id);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
}

TEST_F(BackupManagerTest, RejectsUnsafeBackupIdForDeleteAndVerify) {
  cedar::BackupConfig config;
  config.backup_dir = backup_dir_.string();
  cedar::BackupManager manager(config);

  EXPECT_TRUE(manager.DeleteBackup("../outside").IsInvalidArgument());
  EXPECT_TRUE(manager.VerifyBackup("../outside").IsInvalidArgument());
}

}  // namespace
