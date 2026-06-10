#include <gtest/gtest.h>
#include <fstream>
#include <future>
#include <chrono>
#include <filesystem>
#include "cedar/storage/cedar_config.h"

using namespace cedar;

TEST(CedarConfigManagerTest, ReloadConfigDoesNotDeadlock) {
  auto* mgr = cedar::CedarConfigManager::Instance();
  std::string tmp_path = "/tmp/cedar_test_config_" + std::to_string(getpid()) + ".yaml";
  {
    std::ofstream ofs(tmp_path);
    ofs << "engine:\n  memtable_threshold: 65536\n";
  }
  auto s = mgr->LoadConfig(tmp_path);
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::future<cedar::Status> fut = std::async(std::launch::async, [mgr]() {
    return mgr->ReloadConfig();
  });
  auto status = fut.wait_for(std::chrono::seconds(2));
  EXPECT_NE(status, std::future_status::timeout) << "ReloadConfig deadlocked!";
  if (status == std::future_status::ready) {
    auto result = fut.get();
    EXPECT_TRUE(result.ok()) << result.ToString();
  }
  std::remove(tmp_path.c_str());
}

TEST(CedarConfigTest, SaveToFileCreatesValidJson) {
  CedarConfig config;
  config.db.memtable_threshold = 8 * 1024 * 1024;
  config.db.write_buffer_size = 16 * 1024 * 1024;
  config.lsm.max_levels = 5;
  config.wal.num_shards = 8;
  config.mvcc.enable_delta_encoding = true;

  std::string tmp_path = "/tmp/cedar_test_save_config_" + std::to_string(getpid()) + ".json";
  Status s = config.SaveToFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "SaveToFile failed: " << s.ToString();

  // Verify file exists
  EXPECT_TRUE(std::filesystem::exists(tmp_path));

  // Verify it contains expected keys
  std::ifstream ifs(tmp_path);
  ASSERT_TRUE(ifs.is_open());
  std::string content((std::istreambuf_iterator<char>(ifs)),
                       std::istreambuf_iterator<char>());
  ifs.close();

  EXPECT_NE(content.find("\"memtable_threshold\": 8388608"), std::string::npos);
  EXPECT_NE(content.find("\"write_buffer_size\": 16777216"), std::string::npos);
  EXPECT_NE(content.find("\"max_levels\": 5"), std::string::npos);
  EXPECT_NE(content.find("\"num_shards\": 8"), std::string::npos);
  EXPECT_NE(content.find("\"enable_delta_encoding\": true"), std::string::npos);

  // Cleanup
  std::filesystem::remove(tmp_path);
}

TEST(CedarConfigTest, SaveToFileIsAtomic) {
  CedarConfig config;
  std::string tmp_path = "/tmp/cedar_test_atomic_config_" + std::to_string(getpid()) + ".json";

  // Ensure no stale tmp file exists
  std::filesystem::remove(tmp_path + ".tmp");
  std::filesystem::remove(tmp_path);

  Status s = config.SaveToFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "SaveToFile failed: " << s.ToString();

  // The .tmp file must not exist after successful atomic rename
  EXPECT_FALSE(std::filesystem::exists(tmp_path + ".tmp"))
      << "Temp file should be removed after atomic rename";

  // The final file must exist
  EXPECT_TRUE(std::filesystem::exists(tmp_path));

  std::filesystem::remove(tmp_path);
}
