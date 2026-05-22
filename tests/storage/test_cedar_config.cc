#include <gtest/gtest.h>
#include <fstream>
#include <future>
#include <chrono>
#include "cedar/storage/cedar_config.h"

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
