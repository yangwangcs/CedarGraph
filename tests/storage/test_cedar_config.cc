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

TEST(CedarConfigTest, LoadFromFileParsesYamlSecurityAndTls) {
  std::string tmp_path = "/tmp/cedar_test_yaml_security_" + std::to_string(getpid()) + ".yaml";
  {
    std::ofstream ofs(tmp_path);
    ofs << R"(
db:
  memtable_threshold: 2097152
  write_buffer_size: 4194304
security:
  enable_auth: true
  enable_tls: true
  jwt_secret: "super-secret-key"
tls:
  enabled: true
  ca_cert: "/etc/certs/ca.crt"
  server_cert: "/etc/certs/server.crt"
  server_key: "/etc/certs/server.key"
  client_cert: "/etc/certs/client.crt"
  client_key: "/etc/certs/client.key"
)";
  }

  CedarConfig config;
  Status s = config.LoadFromFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "LoadFromFile failed: " << s.ToString();

  EXPECT_EQ(config.db.memtable_threshold, 2097152u);
  EXPECT_EQ(config.db.write_buffer_size, 4194304u);
  EXPECT_TRUE(config.security.enable_auth);
  EXPECT_TRUE(config.security.enable_tls);
  EXPECT_EQ(config.security.jwt_secret, "super-secret-key");
  EXPECT_TRUE(config.tls.enabled);
  EXPECT_EQ(config.tls.ca_cert, "/etc/certs/ca.crt");
  EXPECT_EQ(config.tls.server_cert, "/etc/certs/server.crt");
  EXPECT_EQ(config.tls.server_key, "/etc/certs/server.key");
  EXPECT_EQ(config.tls.client_cert, "/etc/certs/client.crt");
  EXPECT_EQ(config.tls.client_key, "/etc/certs/client.key");

  s = config.Validate();
  EXPECT_TRUE(s.ok()) << "Validation failed: " << s.ToString();

  std::filesystem::remove(tmp_path);
}

TEST(CedarConfigTest, LoadFromFileParsesJsonSecurityAndTls) {
  std::string tmp_path = "/tmp/cedar_test_json_security_" + std::to_string(getpid()) + ".json";
  {
    std::ofstream ofs(tmp_path);
    ofs << R"({
  "db": {
    "memtable_threshold": 1048576,
    "write_buffer_size": 2097152
  },
  "security": {
    "enable_auth": true,
    "enable_tls": false,
    "jwt_secret": "json-secret"
  },
  "tls": {
    "enabled": false,
    "ca_cert": "",
    "server_cert": "",
    "server_key": "",
    "client_cert": "",
    "client_key": ""
  }
})";
  }

  CedarConfig config;
  Status s = config.LoadFromFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "LoadFromFile failed: " << s.ToString();

  EXPECT_EQ(config.db.memtable_threshold, 1048576u);
  EXPECT_EQ(config.db.write_buffer_size, 2097152u);
  EXPECT_TRUE(config.security.enable_auth);
  EXPECT_FALSE(config.security.enable_tls);
  EXPECT_EQ(config.security.jwt_secret, "json-secret");
  EXPECT_FALSE(config.tls.enabled);

  s = config.Validate();
  EXPECT_TRUE(s.ok()) << "Validation failed: " << s.ToString();

  std::filesystem::remove(tmp_path);
}

TEST(CedarConfigTest, ValidateRejectsAuthWithoutJwtSecret) {
  CedarConfig config;
  config.security.enable_auth = true;
  config.security.jwt_secret = "";

  Status s = config.Validate();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(CedarConfigTest, ValidateRejectsTlsWithoutCerts) {
  CedarConfig config;
  config.tls.enabled = true;
  config.tls.server_cert = "";
  config.tls.server_key = "";

  Status s = config.Validate();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(CedarConfigTest, SaveAndLoadRoundTripSecurityTls) {
  CedarConfig original;
  original.security.enable_auth = true;
  original.security.enable_tls = true;
  original.security.jwt_secret = "round-trip-secret";
  original.tls.enabled = true;
  original.tls.ca_cert = "/certs/ca.crt";
  original.tls.server_cert = "/certs/server.crt";
  original.tls.server_key = "/certs/server.key";
  original.tls.client_cert = "/certs/client.crt";
  original.tls.client_key = "/certs/client.key";

  std::string tmp_path = "/tmp/cedar_test_roundtrip_" + std::to_string(getpid()) + ".json";
  Status s = original.SaveToFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "SaveToFile failed: " << s.ToString();

  CedarConfig loaded;
  s = loaded.LoadFromFile(tmp_path);
  ASSERT_TRUE(s.ok()) << "LoadFromFile failed: " << s.ToString();

  EXPECT_EQ(loaded.security.enable_auth, original.security.enable_auth);
  EXPECT_EQ(loaded.security.enable_tls, original.security.enable_tls);
  EXPECT_EQ(loaded.security.jwt_secret, original.security.jwt_secret);
  EXPECT_EQ(loaded.tls.enabled, original.tls.enabled);
  EXPECT_EQ(loaded.tls.ca_cert, original.tls.ca_cert);
  EXPECT_EQ(loaded.tls.server_cert, original.tls.server_cert);
  EXPECT_EQ(loaded.tls.server_key, original.tls.server_key);
  EXPECT_EQ(loaded.tls.client_cert, original.tls.client_cert);
  EXPECT_EQ(loaded.tls.client_key, original.tls.client_key);

  s = loaded.Validate();
  EXPECT_TRUE(s.ok()) << "Validation failed: " << s.ToString();

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
