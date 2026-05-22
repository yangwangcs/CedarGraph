// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/dtx/partition_config.h"

#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <cctype>

namespace cedar {
namespace dtx {

// 简单的 YAML 解析辅助函数
namespace {

// 去除字符串首尾空白
std::string Trim(const std::string& str) {
  size_t start = str.find_first_not_of(" \t\r\n");
  if (start == std::string::npos) return "";
  size_t end = str.find_last_not_of(" \t\r\n");
  return str.substr(start, end - start + 1);
}

// 解析键值对
bool ParseKeyValue(const std::string& line, std::string* key, std::string* value) {
  size_t colon_pos = line.find(':');
  if (colon_pos == std::string::npos) {
    return false;
  }
  
  *key = Trim(line.substr(0, colon_pos));
  *value = Trim(line.substr(colon_pos + 1));
  
  // 去除可能的注释
  size_t comment_pos = value->find('#');
  if (comment_pos != std::string::npos) {
    *value = Trim(value->substr(0, comment_pos));
  }
  
  return true;
}

// 字符串转小写
std::string ToLower(const std::string& str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

}  // anonymous namespace

DualModePartitionStrategy::Mode PartitionConfigLoader::ParseMode(
    const std::string& mode_str) {
  std::string lower = ToLower(mode_str);
  if (lower == "mth_stream" || lower == "mth") {
    return DualModePartitionStrategy::Mode::MTH_STREAM;
  } else if (lower == "auto") {
    return DualModePartitionStrategy::Mode::AUTO;
  } else {
    return DualModePartitionStrategy::Mode::STATIC_HASH;
  }
}

std::string PartitionConfigLoader::ModeToString(DualModePartitionStrategy::Mode mode) {
  switch (mode) {
    case DualModePartitionStrategy::Mode::STATIC_HASH:
      return "static_hash";
    case DualModePartitionStrategy::Mode::MTH_STREAM:
      return "mth_stream";
    case DualModePartitionStrategy::Mode::AUTO:
      return "auto";
    default:
      std::cerr << "[PartitionConfigLoader] Unknown mode" << std::endl;
      return "static_hash";
  }
  return "static_hash";
}

Status PartitionConfigLoader::LoadFromFile(const std::string& filepath,
                                            DualModePartitionStrategy::Config* config,
                                            PartitionID* num_partitions) {
  std::ifstream file(filepath);
  if (!file.is_open()) {
    return Status::IOError("Cannot open config file: " + filepath);
  }
  
  std::stringstream buffer;
  buffer << file.rdbuf();
  
  return LoadFromString(buffer.str(), config, num_partitions);
}

Status PartitionConfigLoader::LoadFromString(const std::string& yaml_content,
                                              DualModePartitionStrategy::Config* config,
                                              PartitionID* num_partitions) {
  // 默认配置
  *config = DualModePartitionStrategy::Config();
  *num_partitions = 32768;
  
  std::istringstream stream(yaml_content);
  std::string line;
  
  // 解析状态
  enum class ParseState {
    ROOT,
    PARTITION,
    DUAL_MODE,
    AUTO_SWITCH,
    MTH_CONFIG
  };
  
  ParseState state = ParseState::ROOT;
  int line_number = 0;
  
  while (std::getline(stream, line)) {
    line_number++;
    
    // 去除空白
    line = Trim(line);
    
    // 跳过空行和注释
    if (line.empty() || line[0] == '#') {
      continue;
    }
    
    // 计算缩进级别
    size_t indent = 0;
    while (indent < line.size() && (line[indent] == ' ' || line[indent] == '\t')) {
      indent++;
    }
    
    std::string content = line.substr(indent);
    
    // 解析键值对
    std::string key, value;
    if (!ParseKeyValue(content, &key, &value)) {
      continue;  // 不是键值对，跳过
    }
    
    // 根据缩进和键名更新状态
    if (indent == 0) {
      if (key == "partition") {
        state = ParseState::PARTITION;
      }
      continue;
    }
    
    // 解析 partition 部分
    if (state == ParseState::PARTITION && indent == 2) {
      if (key == "num_partitions") {
        try {
          *num_partitions = static_cast<PartitionID>(std::stoul(value));
          config->num_partitions = *num_partitions;
        } catch (...) {
          std::cerr << "Warning: Invalid num_partitions value at line " 
                    << line_number << std::endl;
        }
      } else if (key == "dual_mode") {
        state = ParseState::DUAL_MODE;
      }
      continue;
    }
    
    // 解析 dual_mode 部分
    if (state == ParseState::DUAL_MODE && indent == 4) {
      if (key == "default_mode") {
        config->mode = ParseMode(value);
      } else if (key == "auto_switch") {
        state = ParseState::AUTO_SWITCH;
      } else if (key == "mth_config") {
        state = ParseState::MTH_CONFIG;
      }
      continue;
    }
    
    // 解析 auto_switch 部分
    if (state == ParseState::AUTO_SWITCH && indent == 6) {
      if (key == "temporal_query_threshold") {
        try {
          config->temporal_query_threshold = std::stoull(value);
        } catch (...) {
          std::cerr << "Warning: Invalid temporal_query_threshold at line " 
                    << line_number << std::endl;
        }
      } else if (key == "locality_ratio_threshold") {
        try {
          config->locality_ratio_threshold = std::stod(value);
        } catch (...) {
          std::cerr << "Warning: Invalid locality_ratio_threshold at line " 
                    << line_number << std::endl;
        }
      }
      continue;
    }
    
    // 解析 mth_config 部分
    if (state == ParseState::MTH_CONFIG && indent == 6) {
      try {
        if (key == "sketch_capacity") {
          config->sketch_capacity = std::stoull(value);
        } else if (key == "alpha") {
          config->mth_alpha = std::stod(value);
        } else if (key == "beta") {
          config->mth_beta = std::stod(value);
        } else if (key == "gamma") {
          config->mth_gamma = std::stod(value);
        } else if (key == "eta") {
          config->mth_eta = std::stod(value);
        } else if (key == "temporal_alpha") {
          config->temporal_alpha = std::stod(value);
        } else if (key == "sketch_depth") {
          config->sketch_depth = std::stoi(value);
        } else if (key == "sketch_width") {
          config->sketch_width = std::stoi(value);
        } else if (key == "fast_path_threshold") {
          config->fast_path_threshold = std::stod(value);
        } else if (key == "load_relaxation") {
          config->load_relaxation = std::stod(value);
        } else if (key == "decay_interval") {
          config->decay_interval = std::stoi(value);
        } else if (key == "decay_factor") {
          config->decay_factor = std::stod(value);
        }
      } catch (...) {
        std::cerr << "Warning: Invalid value for " << key << " at line " 
                  << line_number << std::endl;
      }
      continue;
    }
    
    // 状态回退
    if (indent <= 4 && state == ParseState::AUTO_SWITCH) {
      state = ParseState::DUAL_MODE;
    }
    if (indent <= 4 && state == ParseState::MTH_CONFIG) {
      state = ParseState::DUAL_MODE;
    }
    if (indent <= 2 && state == ParseState::DUAL_MODE) {
      state = ParseState::PARTITION;
    }
  }
  
  return Status::OK();
}

Status PartitionConfigLoader::SaveToFile(const std::string& filepath,
                                          const DualModePartitionStrategy::Config& config,
                                          PartitionID num_partitions) {
  std::ofstream file(filepath);
  if (!file.is_open()) {
    return Status::IOError("Cannot create config file: " + filepath);
  }
  
  file << "# CedarGraph 分区配置\n";
  file << "# 支持双模式分区策略：StaticHash 和 MTHStream\n\n";
  
  file << "partition:\n";
  file << "  strategy_type: \"dual_mode\"\n";
  file << "  num_partitions: " << num_partitions << "\n\n";
  
  file << "  dual_mode:\n";
  file << "    default_mode: \"" << ModeToString(config.mode) << "\"\n\n";
  
  file << "    auto_switch:\n";
  file << "      temporal_query_threshold: " << config.temporal_query_threshold << "\n";
  file << "      locality_ratio_threshold: " << config.locality_ratio_threshold << "\n\n";
  
  file << "    mth_config:\n";
  file << "      sketch_capacity: " << config.sketch_capacity << "\n";
  file << "      alpha: " << config.mth_alpha << "\n";
  file << "      beta: " << config.mth_beta << "\n";
  file << "      gamma: " << config.mth_gamma << "\n";
  file << "      eta: " << config.mth_eta << "\n";
  file << "      temporal_alpha: " << config.temporal_alpha << "\n";
  file << "      sketch_depth: " << config.sketch_depth << "\n";
  file << "      sketch_width: " << config.sketch_width << "\n";
  file << "      fast_path_threshold: " << config.fast_path_threshold << "\n";
  file << "      load_relaxation: " << config.load_relaxation << "\n";
  file << "      decay_interval: " << config.decay_interval << "\n";
  file << "      decay_factor: " << config.decay_factor << "\n";
  
  return Status::OK();
}

}  // namespace dtx
}  // namespace cedar
