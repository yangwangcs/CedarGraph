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

// =============================================================================
// SST Builder 工厂 - 统一生产级设计
// =============================================================================
// 特性：
// - 256KB Block 大小
// - Block 级稀疏索引（48 bytes/Block）
// - Zone-Columnar 编码（5 个 Zone）
// - 目标 SST 文件大小：8-64MB
// =============================================================================

#ifndef FERN_SST_BUILDER_FACTORY_H_
#define FERN_SST_BUILDER_FACTORY_H_

#include <memory>
#include <string>

#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// 前向声明
class WritableFile;

// Builder 接口
class SstBuilderInterface {
 public:
  virtual ~SstBuilderInterface() = default;
  
  // 添加条目（要求：按 CedarKey 排序）
  virtual void Add(const CedarKey& key, const Descriptor& desc) = 0;
  
  // 完成构建
  virtual Status Finish() = 0;
  
  // 获取统计
  virtual uint64_t FileSize() const = 0;
  virtual uint64_t NumEntries() const = 0;
  
  // 获取序列化后的 TemporalBloomFilter 数据（如果构建了）
  virtual std::string GetTemporalFilterData() const = 0;
};

// Builder 配置
struct SstBuilderOptions {
  // Block 大小目标（默认 256KB）
  size_t target_block_size = 256 * 1024;
  
  // Block 行数上限（默认 16384）
  size_t block_row_limit = 16384;
  
  // 是否启用压缩
  bool enable_compression = true;
};

// Builder 工厂
class SstBuilderFactory {
 public:
  // 创建 Builder（统一使用生产级配置）
  static std::unique_ptr<SstBuilderInterface> Create(
      WritableFile* file,
      const std::string& db_path,
      const SstBuilderOptions& options = SstBuilderOptions());
};

}  // namespace cedar

#endif  // FERN_SST_BUILDER_FACTORY_H_
