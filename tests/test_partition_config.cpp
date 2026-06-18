// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
//
// Test: Partition Config Loading

#include <iostream>
#include <cassert>
#include <fstream>
#include "cedar/dtx/partition_config.h"

using namespace cedar;
using namespace cedar::dtx;

void TestLoadFromString() {
  std::cout << "=== Test Load From String ===" << std::endl;
  
  const char* yaml_content = R"(
partition:
  strategy_type: "dual_mode"
  num_partitions: 1000

  dual_mode:
    default_mode: "mth_stream"

    auto_switch:
      temporal_query_threshold: 200
      locality_ratio_threshold: 0.8

    mth_config:
      sketch_capacity: 500000
      alpha: 1.5
      beta: 1.5
      gamma: 0.1
      eta: 0.1
      temporal_alpha: 0.02
      sketch_depth: 4
      sketch_width: 128
      fast_path_threshold: 0.7
      load_relaxation: 0.1
      decay_interval: 1000
      decay_factor: 0.9
)";
  
  DualModePartitionStrategy::Config config;
  PartitionID num_partitions = 0;
  
  Status status = PartitionConfigLoader::LoadFromString(
      yaml_content, &config, &num_partitions);
  
  assert(status.ok());
  assert(num_partitions == 1000);
  assert(config.mode == DualModePartitionStrategy::Mode::MTH_STREAM);
  assert(config.temporal_query_threshold == 200);
  assert(config.locality_ratio_threshold == 0.8);
  assert(config.sketch_capacity == 500000);
  assert(config.mth_alpha == 1.5);
  assert(config.sketch_depth == 4);
  assert(config.sketch_width == 128);
  
  std::cout << "✓ Config loaded successfully" << std::endl;
  std::cout << "  num_partitions: " << num_partitions << std::endl;
  std::cout << "  mode: MTH_STREAM" << std::endl;
  std::cout << "  sketch_depth: " << config.sketch_depth << std::endl;
}

void TestLoadFromFile() {
  std::cout << "\n=== Test Load From File ===" << std::endl;
  
  // 创建临时配置文件
  std::string temp_file = (std::filesystem::temp_directory_path() / "test_partition_config.yaml").string();
  {
    std::ofstream file(temp_file);
    file << "partition:\n";
    file << "  num_partitions: 500\n";
    file << "  dual_mode:\n";
    file << "    default_mode: \"auto\"\n";
    file << "    auto_switch:\n";
    file << "      temporal_query_threshold: 50\n";
    file << "    mth_config:\n";
    file << "      sketch_depth: 5\n";
  }
  
  DualModePartitionStrategy::Config config;
  PartitionID num_partitions = 0;
  
  Status status = PartitionConfigLoader::LoadFromFile(
      temp_file, &config, &num_partitions);
  
  assert(status.ok());
  assert(num_partitions == 500);
  assert(config.mode == DualModePartitionStrategy::Mode::AUTO);
  assert(config.temporal_query_threshold == 50);
  assert(config.sketch_depth == 5);
  
  std::cout << "✓ Config loaded from file" << std::endl;
  std::cout << "  num_partitions: " << num_partitions << std::endl;
  std::cout << "  mode: AUTO" << std::endl;
  
  // 清理临时文件
  std::remove(temp_file.c_str());
}

void TestSaveToFile() {
  std::cout << "\n=== Test Save To File ===" << std::endl;
  
  // 创建配置
  DualModePartitionStrategy::Config config;
  config.mode = DualModePartitionStrategy::Mode::MTH_STREAM;
  config.num_partitions = 2000;
  config.sketch_depth = 4;
  config.sketch_width = 128;
  config.fast_path_threshold = 0.75;
  
  std::string temp_file = (std::filesystem::temp_directory_path() / "test_partition_config_save.yaml").string();
  
  // 保存配置
  Status status = PartitionConfigLoader::SaveToFile(temp_file, config, 2000);
  assert(status.ok());
  std::cout << "✓ Config saved to file" << std::endl;
  
  // 重新加载验证
  DualModePartitionStrategy::Config loaded_config;
  PartitionID num_partitions = 0;
  
  status = PartitionConfigLoader::LoadFromFile(temp_file, &loaded_config, &num_partitions);
  assert(status.ok());
  assert(loaded_config.mode == DualModePartitionStrategy::Mode::MTH_STREAM);
  assert(num_partitions == 2000);
  assert(loaded_config.sketch_depth == 4);
  assert(loaded_config.sketch_width == 128);
  
  std::cout << "✓ Config reloaded and verified" << std::endl;
  
  // 显示文件内容
  std::cout << "\n--- Generated Config File ---" << std::endl;
  std::ifstream file(temp_file);
  std::string line;
  while (std::getline(file, line)) {
    std::cout << line << std::endl;
  }
  
  // 清理
  std::remove(temp_file.c_str());
}

void TestDefaultConfig() {
  std::cout << "\n=== Test Default Config ===" << std::endl;
  
  // 空的 YAML 内容
  DualModePartitionStrategy::Config config;
  PartitionID num_partitions = 0;
  
  Status status = PartitionConfigLoader::LoadFromString("", &config, &num_partitions);
  assert(status.ok());
  
  // 验证默认值
  assert(config.mode == DualModePartitionStrategy::Mode::STATIC_HASH);
  assert(config.num_partitions == 32768);
  assert(config.sketch_capacity == 1000000);
  assert(config.sketch_depth == 3);
  assert(config.sketch_width == 64);
  
  std::cout << "✓ Default values applied" << std::endl;
  std::cout << "  Default mode: STATIC_HASH" << std::endl;
  std::cout << "  Default num_partitions: " << config.num_partitions << std::endl;
}

int main() {
  std::cout << "==============================================" << std::endl;
  std::cout << "Partition Config Test Suite" << std::endl;
  std::cout << "==============================================" << std::endl;
  
  TestLoadFromString();
  TestLoadFromFile();
  TestSaveToFile();
  TestDefaultConfig();
  
  std::cout << "\n==============================================" << std::endl;
  std::cout << "All tests passed!" << std::endl;
  std::cout << "==============================================" << std::endl;
  
  return 0;
}
