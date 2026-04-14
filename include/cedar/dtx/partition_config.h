// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_DTX_PARTITION_CONFIG_H_
#define CEDAR_DTX_PARTITION_CONFIG_H_

#include <string>
#include "cedar/dtx/partition.h"

namespace cedar {
namespace dtx {

/**
 * @brief 分区配置加载器
 * 
 * 从 YAML 配置文件加载分区策略配置
 */
class PartitionConfigLoader {
 public:
  // 从 YAML 文件加载配置
  static Status LoadFromFile(const std::string& filepath,
                              DualModePartitionStrategy::Config* config,
                              PartitionID* num_partitions);
  
  // 从 YAML 字符串加载配置
  static Status LoadFromString(const std::string& yaml_content,
                               DualModePartitionStrategy::Config* config,
                               PartitionID* num_partitions);
  
  // 保存配置到文件
  static Status SaveToFile(const std::string& filepath,
                           const DualModePartitionStrategy::Config& config,
                           PartitionID num_partitions);

 private:
  // 解析模式字符串
  static DualModePartitionStrategy::Mode ParseMode(const std::string& mode_str);
  
  // 模式转字符串
  static std::string ModeToString(DualModePartitionStrategy::Mode mode);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_PARTITION_CONFIG_H_
