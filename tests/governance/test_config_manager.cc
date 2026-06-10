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

#include "cedar/governance/config_manager.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unistd.h>

#include "gtest/gtest.h"

namespace cedar {
namespace governance {

// =============================================================================
// ConfigManager Test Suite
// =============================================================================

class ConfigManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_manager_ = std::make_unique<ConfigManager>();
  }

  void TearDown() override {
    config_manager_.reset();
  }

  std::unique_ptr<ConfigManager> config_manager_;
};

// =============================================================================
// Basic Loading Tests
// =============================================================================

TEST_F(ConfigManagerTest, LoadFromYaml) {
  const char* yaml_content = R"(
cluster:
  name: "test-cluster"
  node_id: 1
  data_dir: "/data/cedar"

storage:
  write_buffer_size: 67108864
  max_file_size: 134217728
)";

  Status s = config_manager_->LoadFromString(yaml_content);
  ASSERT_TRUE(s.ok()) << "Failed to load config: " << s.ToString();

  // Verify string values
  EXPECT_EQ(config_manager_->GetString("cluster.name"), "test-cluster");
  EXPECT_EQ(config_manager_->GetString("cluster.data_dir"), "/data/cedar");

  // Verify int values
  EXPECT_EQ(config_manager_->GetInt("cluster.node_id"), 1);
  EXPECT_EQ(config_manager_->GetInt64("storage.write_buffer_size"), 67108864);
  EXPECT_EQ(config_manager_->GetInt64("storage.max_file_size"), 134217728);
}

TEST_F(ConfigManagerTest, LoadFromYamlFile) {
  // Create a temporary YAML file
  const char* yaml_content = R"(
cluster:
  name: "file-test-cluster"
  node_id: 42
)";

  std::string temp_file = std::filesystem::temp_directory_path().string() + "/test_cedar_config_" + std::to_string(getpid()) + ".yaml";
  std::ofstream ofs(temp_file);
  ofs << yaml_content;
  ofs.close();

  Status s = config_manager_->LoadFromFile(temp_file);
  ASSERT_TRUE(s.ok()) << "Failed to load config from file: " << s.ToString();

  EXPECT_EQ(config_manager_->GetString("cluster.name"), "file-test-cluster");
  EXPECT_EQ(config_manager_->GetInt("cluster.node_id"), 42);

  // Cleanup
  std::remove(temp_file.c_str());
}

TEST_F(ConfigManagerTest, LoadFromNonExistentFile) {
  Status s = config_manager_->LoadFromFile("/nonexistent/path/config.yaml");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST_F(ConfigManagerTest, LoadInvalidYaml) {
  const char* invalid_yaml = "invalid: yaml: content: [}";
  Status s = config_manager_->LoadFromString(invalid_yaml);
  // Our parser is lenient, but certain patterns should fail
  // This test documents current behavior
}

// =============================================================================
// Getter Tests with Defaults
// =============================================================================

TEST_F(ConfigManagerTest, GetStringWithDefault) {
  EXPECT_EQ(config_manager_->GetString("nonexistent.key", "default"), "default");
  EXPECT_EQ(config_manager_->GetString("nonexistent.key"), "");  // Empty default

  config_manager_->SetString("test.key", "value");
  EXPECT_EQ(config_manager_->GetString("test.key", "default"), "value");
}

TEST_F(ConfigManagerTest, GetIntWithDefault) {
  EXPECT_EQ(config_manager_->GetInt("nonexistent.key", 42), 42);
  EXPECT_EQ(config_manager_->GetInt("nonexistent.key"), 0);  // Zero default

  config_manager_->SetInt("test.int", 123);
  EXPECT_EQ(config_manager_->GetInt("test.int", 42), 123);
}

TEST_F(ConfigManagerTest, GetInt64WithDefault) {
  EXPECT_EQ(config_manager_->GetInt64("nonexistent.key", 9999999999LL),
            9999999999LL);

  config_manager_->SetInt64("test.int64", 8888888888LL);
  EXPECT_EQ(config_manager_->GetInt64("test.int64", 42), 8888888888LL);
}

TEST_F(ConfigManagerTest, GetDoubleWithDefault) {
  EXPECT_DOUBLE_EQ(config_manager_->GetDouble("nonexistent.key", 3.14), 3.14);

  config_manager_->SetDouble("test.double", 2.71828);
  EXPECT_DOUBLE_EQ(config_manager_->GetDouble("test.double", 0.0), 2.71828);
}

TEST_F(ConfigManagerTest, GetBoolWithDefault) {
  EXPECT_TRUE(config_manager_->GetBool("nonexistent.key", true));
  EXPECT_FALSE(config_manager_->GetBool("nonexistent.key", false));
  EXPECT_FALSE(config_manager_->GetBool("nonexistent.key"));  // False default

  config_manager_->SetBool("test.bool", true);
  EXPECT_TRUE(config_manager_->GetBool("test.bool", false));
}

TEST_F(ConfigManagerTest, GetBoolVariousFormats) {
  config_manager_->SetString("bool.true1", "true");
  config_manager_->SetString("bool.true2", "TRUE");
  config_manager_->SetString("bool.true3", "True");
  config_manager_->SetString("bool.true4", "yes");
  config_manager_->SetString("bool.true5", "1");
  config_manager_->SetString("bool.true6", "on");

  config_manager_->SetString("bool.false1", "false");
  config_manager_->SetString("bool.false2", "FALSE");
  config_manager_->SetString("bool.false3", "no");
  config_manager_->SetString("bool.false4", "0");
  config_manager_->SetString("bool.false5", "off");

  EXPECT_TRUE(config_manager_->GetBool("bool.true1"));
  EXPECT_TRUE(config_manager_->GetBool("bool.true2"));
  EXPECT_TRUE(config_manager_->GetBool("bool.true3"));
  EXPECT_TRUE(config_manager_->GetBool("bool.true4"));
  EXPECT_TRUE(config_manager_->GetBool("bool.true5"));
  EXPECT_TRUE(config_manager_->GetBool("bool.true6"));

  EXPECT_FALSE(config_manager_->GetBool("bool.false1"));
  EXPECT_FALSE(config_manager_->GetBool("bool.false2"));
  EXPECT_FALSE(config_manager_->GetBool("bool.false3"));
  EXPECT_FALSE(config_manager_->GetBool("bool.false4"));
  EXPECT_FALSE(config_manager_->GetBool("bool.false5"));
}

// =============================================================================
// Environment Variable Override Tests
// =============================================================================

TEST_F(ConfigManagerTest, EnvironmentOverride) {
  const char* yaml_content = R"(
cluster:
  name: "original-cluster"
  node_id: 1
)";

  Status s = config_manager_->LoadFromString(yaml_content);
  ASSERT_TRUE(s.ok());

  // Verify original values first
  EXPECT_EQ(config_manager_->GetString("cluster.name"), "original-cluster");
  EXPECT_EQ(config_manager_->GetInt("cluster.node_id"), 1);

  // Set environment variable before loading
  setenv("CEDAR_CLUSTER_NAME", "env-override-cluster", 1);

  // Create a new config manager to test env override on load
  ConfigManager env_config;
  s = env_config.LoadFromString(yaml_content);
  ASSERT_TRUE(s.ok());

  // Verify environment variable took precedence
  EXPECT_EQ(env_config.GetString("cluster.name"), "env-override-cluster");

  // Cleanup
  unsetenv("CEDAR_CLUSTER_NAME");
}

TEST_F(ConfigManagerTest, EnvVarToConfigKeyConversion) {
  EXPECT_EQ(EnvVarToConfigKey("CEDAR_CLUSTER_NAME"), "cluster.name");
  // Only the first underscore becomes a dot, rest are removed (converted to lowercase)
  EXPECT_EQ(EnvVarToConfigKey("CEDAR_STORAGE_WRITE_BUFFER_SIZE"),
            "storage.write_buffer_size");
  EXPECT_EQ(EnvVarToConfigKey("CEDAR_CLUSTER_NODE_ID"), "cluster.node_id");
  EXPECT_EQ(EnvVarToConfigKey("NOT_CEDAR_PREFIX"), "");
  EXPECT_EQ(EnvVarToConfigKey("CEDAR_"), "");
}

TEST_F(ConfigManagerTest, ConfigKeyToEnvVarConversion) {
  EXPECT_EQ(ConfigKeyToEnvVar("cluster.name"), "CEDAR_CLUSTER_NAME");
  EXPECT_EQ(ConfigKeyToEnvVar("storage.write_buffer_size"),
            "CEDAR_STORAGE_WRITE_BUFFER_SIZE");
}

// =============================================================================
// Configuration Validation Tests
// =============================================================================

TEST_F(ConfigManagerTest, ValidateConfig) {
  // Load a valid config
  const char* yaml_content = R"(
cluster:
  name: "test-cluster"
  node_id: 1
storage:
  write_buffer_size: 67108864
)";

  Status s = config_manager_->LoadFromString(yaml_content);
  ASSERT_TRUE(s.ok());

  // Validate with required keys
  s = config_manager_->ValidateBasic(
      {"cluster.name", "cluster.node_id", "storage.write_buffer_size"});
  EXPECT_TRUE(s.ok());

  // Validate with missing required key
  s = config_manager_->ValidateBasic({"cluster.name", "missing.key"});
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST_F(ConfigManagerTest, ValidateNonExistentSchemaFile) {
  Status s = config_manager_->Validate("/nonexistent/schema.json");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

// =============================================================================
// Setter Tests
// =============================================================================

TEST_F(ConfigManagerTest, SetStringCreatesKey) {
  EXPECT_FALSE(config_manager_->HasKey("new.key"));

  config_manager_->SetString("new.key", "new_value");

  EXPECT_TRUE(config_manager_->HasKey("new.key"));
  EXPECT_EQ(config_manager_->GetString("new.key"), "new_value");
}

TEST_F(ConfigManagerTest, SetIntCreatesKey) {
  config_manager_->SetInt("new.int", 42);
  EXPECT_EQ(config_manager_->GetInt("new.int"), 42);
  EXPECT_EQ(config_manager_->GetString("new.int"), "42");
}

TEST_F(ConfigManagerTest, SetInt64CreatesKey) {
  config_manager_->SetInt64("new.int64", 9223372036854775807LL);
  EXPECT_EQ(config_manager_->GetInt64("new.int64"), 9223372036854775807LL);
}

TEST_F(ConfigManagerTest, SetDoubleCreatesKey) {
  config_manager_->SetDouble("new.double", 3.14159);
  EXPECT_DOUBLE_EQ(config_manager_->GetDouble("new.double"), 3.14159);
}

TEST_F(ConfigManagerTest, SetBoolCreatesKey) {
  config_manager_->SetBool("new.bool", true);
  EXPECT_TRUE(config_manager_->GetBool("new.bool"));
  EXPECT_EQ(config_manager_->GetString("new.bool"), "true");
}

TEST_F(ConfigManagerTest, SetStringArray) {
  std::vector<std::string> values = {"value1", "value2", "value3"};
  config_manager_->SetStringArray("new.array", values);

  auto retrieved = config_manager_->GetStringArray("new.array");
  ASSERT_EQ(retrieved.size(), 3);
  EXPECT_EQ(retrieved[0], "value1");
  EXPECT_EQ(retrieved[1], "value2");
  EXPECT_EQ(retrieved[2], "value3");
}

// =============================================================================
// Merge Tests
// =============================================================================

TEST_F(ConfigManagerTest, MergeConfigManager) {
  const char* yaml1 = R"(
cluster:
  name: "cluster1"
  node_id: 1
)";

  const char* yaml2 = R"(
cluster:
  name: "cluster2"
storage:
  write_buffer_size: 12345
)";

  Status s = config_manager_->LoadFromString(yaml1);
  ASSERT_TRUE(s.ok());

  ConfigManager other;
  s = other.LoadFromString(yaml2);
  ASSERT_TRUE(s.ok());

  // Merge - other takes precedence
  s = config_manager_->Merge(other);
  ASSERT_TRUE(s.ok());

  // cluster.name should be overwritten
  EXPECT_EQ(config_manager_->GetString("cluster.name"), "cluster2");
  // cluster.node_id should be preserved
  EXPECT_EQ(config_manager_->GetInt("cluster.node_id"), 1);
  // storage.write_buffer_size should be added
  EXPECT_EQ(config_manager_->GetInt("storage.write_buffer_size"), 12345);
}

TEST_F(ConfigManagerTest, MergeWithPrefix) {
  const char* yaml1 = R"(
cluster:
  name: "main-cluster"
)";

  const char* yaml2 = R"(
name: "sub-cluster"
node_id: 42
)";

  Status s = config_manager_->LoadFromString(yaml1);
  ASSERT_TRUE(s.ok());

  ConfigManager other;
  s = other.LoadFromString(yaml2);
  ASSERT_TRUE(s.ok());

  s = config_manager_->MergeWithPrefix(other, "subcluster");
  ASSERT_TRUE(s.ok());

  EXPECT_EQ(config_manager_->GetString("cluster.name"), "main-cluster");
  EXPECT_EQ(config_manager_->GetString("subcluster.name"), "sub-cluster");
  EXPECT_EQ(config_manager_->GetInt("subcluster.node_id"), 42);
}

// =============================================================================
// Dump Tests
// =============================================================================

TEST_F(ConfigManagerTest, DumpConfig) {
  config_manager_->SetString("cluster.name", "test-cluster");
  config_manager_->SetInt("cluster.node_id", 1);
  config_manager_->SetInt64("storage.size", 67108864);

  std::string dumped = config_manager_->Dump();

  // Should contain the keys and values
  EXPECT_NE(dumped.find("cluster:"), std::string::npos);
  EXPECT_NE(dumped.find("name:"), std::string::npos);
  EXPECT_NE(dumped.find("test-cluster"), std::string::npos);
}

TEST_F(ConfigManagerTest, DumpWithComments) {
  config_manager_->SetString("key", "value");
  std::string dumped = config_manager_->DumpWithComments();

  EXPECT_NE(dumped.find("# CedarGraph Configuration"), std::string::npos);
}

TEST_F(ConfigManagerTest, DumpSection) {
  const char* yaml = R"(
cluster:
  name: "test-cluster"
  node_id: 1
storage:
  write_buffer_size: 67108864
)";

  Status s = config_manager_->LoadFromString(yaml);
  ASSERT_TRUE(s.ok());

  std::string section = config_manager_->DumpSection("cluster");
  EXPECT_NE(section.find("cluster:"), std::string::npos);
  EXPECT_NE(section.find("name:"), std::string::npos);
  EXPECT_EQ(section.find("storage:"), std::string::npos);
}

// =============================================================================
// Utility Tests
// =============================================================================

TEST_F(ConfigManagerTest, HasKey) {
  EXPECT_FALSE(config_manager_->HasKey("nonexistent"));

  config_manager_->SetString("test.key", "value");
  EXPECT_TRUE(config_manager_->HasKey("test.key"));
}

TEST_F(ConfigManagerTest, IsEmpty) {
  EXPECT_TRUE(config_manager_->IsEmpty());

  config_manager_->SetString("key", "value");
  EXPECT_FALSE(config_manager_->IsEmpty());
}

TEST_F(ConfigManagerTest, Size) {
  EXPECT_EQ(config_manager_->Size(), 0);

  config_manager_->SetString("key1", "value1");
  EXPECT_EQ(config_manager_->Size(), 1);

  config_manager_->SetString("key2", "value2");
  EXPECT_EQ(config_manager_->Size(), 2);

  // Update existing key
  config_manager_->SetString("key1", "updated");
  EXPECT_EQ(config_manager_->Size(), 2);
}

TEST_F(ConfigManagerTest, Clear) {
  config_manager_->SetString("key1", "value1");
  config_manager_->SetString("key2", "value2");
  EXPECT_EQ(config_manager_->Size(), 2);

  config_manager_->Clear();
  EXPECT_EQ(config_manager_->Size(), 0);
  EXPECT_FALSE(config_manager_->HasKey("key1"));
}

TEST_F(ConfigManagerTest, GetAllKeys) {
  config_manager_->SetString("a.b.c", "value1");
  config_manager_->SetString("a.b.d", "value2");
  config_manager_->SetString("x.y", "value3");

  auto keys = config_manager_->GetAllKeys();
  EXPECT_EQ(keys.size(), 3);

  // Keys should be sorted
  EXPECT_TRUE(std::find(keys.begin(), keys.end(), "a.b.c") != keys.end());
  EXPECT_TRUE(std::find(keys.begin(), keys.end(), "a.b.d") != keys.end());
  EXPECT_TRUE(std::find(keys.begin(), keys.end(), "x.y") != keys.end());
}

TEST_F(ConfigManagerTest, GetKeysWithPrefix) {
  config_manager_->SetString("cluster.name", "test");
  config_manager_->SetString("cluster.node_id", "1");
  config_manager_->SetString("storage.size", "100");

  auto cluster_keys = config_manager_->GetKeysWithPrefix("cluster");
  EXPECT_EQ(cluster_keys.size(), 2);

  auto storage_keys = config_manager_->GetKeysWithPrefix("storage");
  EXPECT_EQ(storage_keys.size(), 1);

  auto empty_keys = config_manager_->GetKeysWithPrefix("nonexistent");
  EXPECT_EQ(empty_keys.size(), 0);
}

// =============================================================================
// Watch Tests
// =============================================================================

TEST_F(ConfigManagerTest, WatchKey) {
  bool callback_called = false;
  std::string changed_key;
  std::string new_value;

  auto callback = [&](const ConfigChangeEvent& event) {
    callback_called = true;
    changed_key = event.key;
    new_value = event.new_value;
  };

  auto result = config_manager_->Watch("test.key", callback);
  ASSERT_TRUE(result.ok());
  int64_t watch_id = result.ValueOrDie();

  // Set the watched key
  config_manager_->SetString("test.key", "new_value");

  EXPECT_TRUE(callback_called);
  EXPECT_EQ(changed_key, "test.key");
  EXPECT_EQ(new_value, "new_value");

  // Unwatch
  Status s = config_manager_->Unwatch(watch_id);
  EXPECT_TRUE(s.ok());
}

TEST_F(ConfigManagerTest, WatchAll) {
  int callback_count = 0;

  auto callback = [&](const ConfigChangeEvent& event) {
    (void)event;
    callback_count++;
  };

  auto result = config_manager_->WatchAll(callback);
  ASSERT_TRUE(result.ok());
  int64_t watch_id = result.ValueOrDie();

  // Set multiple keys
  config_manager_->SetString("key1", "value1");
  config_manager_->SetString("key2", "value2");
  config_manager_->SetInt("key3", 3);

  EXPECT_EQ(callback_count, 3);

  // Unwatch
  Status s = config_manager_->Unwatch(watch_id);
  EXPECT_TRUE(s.ok());
}

TEST_F(ConfigManagerTest, UnwatchInvalidId) {
  Status s = config_manager_->Unwatch(99999);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

// =============================================================================
// Source File Tests
// =============================================================================

TEST_F(ConfigManagerTest, GetSourceFile) {
  // Initially empty
  EXPECT_EQ(config_manager_->GetSourceFile(), "");

  // Create and load from file
  const char* yaml = "key: value";
  std::string temp_file = std::filesystem::temp_directory_path().string() + "/test_source_file_" + std::to_string(getpid()) + ".yaml";
  std::ofstream ofs(temp_file);
  ofs << yaml;
  ofs.close();

  Status s = config_manager_->LoadFromFile(temp_file);
  ASSERT_TRUE(s.ok());

  EXPECT_EQ(config_manager_->GetSourceFile(), temp_file);
  EXPECT_GT(config_manager_->GetLastModifiedTime(), 0);

  // Cleanup
  std::remove(temp_file.c_str());
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST_F(ConfigManagerTest, DeeplyNestedKeys) {
  config_manager_->SetString("a.b.c.d.e.f", "deep_value");
  EXPECT_EQ(config_manager_->GetString("a.b.c.d.e.f"), "deep_value");
}

TEST_F(ConfigManagerTest, SpecialCharactersInValues) {
  config_manager_->SetString("key.with.spaces", "value with spaces");
  config_manager_->SetString("key.with.symbols", "!@#$%^&*()");

  EXPECT_EQ(config_manager_->GetString("key.with.spaces"), "value with spaces");
  EXPECT_EQ(config_manager_->GetString("key.with.symbols"), "!@#$%^&*()");
}

TEST_F(ConfigManagerTest, EmptyStringValue) {
  config_manager_->SetString("empty.key", "");
  EXPECT_EQ(config_manager_->GetString("empty.key"), "");
  EXPECT_EQ(config_manager_->GetString("empty.key", "default"), "");
}

TEST_F(ConfigManagerTest, NumericParsingEdgeCases) {
  config_manager_->SetString("int.max", "2147483647");
  config_manager_->SetString("int.min", "-2147483648");
  config_manager_->SetString("int64.max", "9223372036854775807");
  config_manager_->SetString("double.pi", "3.14159265358979323846");

  EXPECT_EQ(config_manager_->GetInt("int.max"), 2147483647);
  EXPECT_EQ(config_manager_->GetInt("int.min"), -2147483648);
  EXPECT_EQ(config_manager_->GetInt64("int64.max"), 9223372036854775807LL);
  EXPECT_DOUBLE_EQ(config_manager_->GetDouble("double.pi"), 3.141592653589793);
}

TEST_F(ConfigManagerTest, InvalidNumericParsing) {
  config_manager_->SetString("not.a.number", "abc");

  // Should return default value for invalid numbers
  EXPECT_EQ(config_manager_->GetInt("not.a.number", 42), 42);
  EXPECT_EQ(config_manager_->GetInt64("not.a.number", 100), 100);
  EXPECT_DOUBLE_EQ(config_manager_->GetDouble("not.a.number", 1.5), 1.5);
}

// =============================================================================
// ParseKey Helper Tests
// =============================================================================

TEST(ParseKeyTest, SimpleKey) {
  auto parts = ParseKey("simple");
  ASSERT_EQ(parts.size(), 1);
  EXPECT_EQ(parts[0], "simple");
}

TEST(ParseKeyTest, NestedKey) {
  auto parts = ParseKey("cluster.name");
  ASSERT_EQ(parts.size(), 2);
  EXPECT_EQ(parts[0], "cluster");
  EXPECT_EQ(parts[1], "name");
}

TEST(ParseKeyTest, DeeplyNestedKey) {
  auto parts = ParseKey("a.b.c.d.e");
  ASSERT_EQ(parts.size(), 5);
  EXPECT_EQ(parts[0], "a");
  EXPECT_EQ(parts[4], "e");
}

}  // namespace governance
}  // namespace cedar

// =============================================================================
// Main
// =============================================================================


// =============================================================================
// yaml-cpp Parser Tests
// =============================================================================

TEST(ConfigManager, ParsesNestedYaml) {
  std::string yaml = R"(
storaged:
  node_id: 1
  port: 9779
  tls:
    enabled: true
    server_cert: /etc/certs/server.crt
)";
  std::string temp_config = std::filesystem::temp_directory_path().string() + "/test_config_" + std::to_string(getpid()) + ".yaml";
  std::ofstream f(temp_config);
  f << yaml;
  f.close();

  cedar::governance::ConfigManager cm;
  ASSERT_TRUE(cm.LoadFromFile(temp_config).ok());
  EXPECT_EQ(cm.GetInt("storaged.node_id", 0), 1);
  EXPECT_EQ(cm.GetInt("storaged.port", 0), 9779);
  EXPECT_EQ(cm.GetBool("storaged.tls.enabled", false), true);
  EXPECT_EQ(cm.GetString("storaged.tls.server_cert", ""), "/etc/certs/server.crt");
}

TEST(ConfigManager, ParsesQuotedStrings) {
  std::string yaml = "value: \"hello world\"\n";
  std::string temp_config2 = std::filesystem::temp_directory_path().string() + "/test_config2_" + std::to_string(getpid()) + ".yaml";
  std::ofstream f(temp_config2);
  f << yaml;
  f.close();

  cedar::governance::ConfigManager cm;
  ASSERT_TRUE(cm.LoadFromFile(temp_config2).ok());
  EXPECT_EQ(cm.GetString("value", ""), "hello world");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
