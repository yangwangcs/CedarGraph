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
// SST Builder 工厂实现 - 统一生产级设计
// =============================================================================

#include "cedar/sst/sst_builder_factory.h"

#include "cedar/core/env.h"

// Zone-Columnar Builder V2（唯一实现）
#include "zone_columnar_builder_v2.cc"

namespace cedar {

// =============================================================================
// Builder 包装器
// =============================================================================
class SstBuilderWrapper : public SstBuilderInterface {
 public:
  SstBuilderWrapper(WritableFile* file, 
                    const std::string& db_path,
                    const SstBuilderOptions& options) {
    ZoneColumnarSstBuilderV2::Options v2_options;
    v2_options.target_block_size = options.target_block_size;
    v2_options.block_row_limit = options.block_row_limit;
    v2_options.enable_compression = options.enable_compression;
    
    builder_ = std::make_unique<ZoneColumnarSstBuilderV2>(v2_options, file);
  }
  
  void Add(const CedarKey& key, const Descriptor& desc) override {
    builder_->Add(key, desc);
  }
  
  Status Finish() override {
    return builder_->Finish();
  }
  
  uint64_t FileSize() const override {
    return builder_->FileSize();
  }
  
  uint64_t NumEntries() const override {
    return builder_->NumEntries();
  }
  
  std::string GetTemporalFilterData() const override {
    return builder_->GetTemporalFilterData();
  }
  
 private:
  std::unique_ptr<ZoneColumnarSstBuilderV2> builder_;
};

// =============================================================================
// 工厂方法
// =============================================================================
std::unique_ptr<SstBuilderInterface> SstBuilderFactory::Create(
    WritableFile* file,
    const std::string& db_path,
    const SstBuilderOptions& options) {
  return std::make_unique<SstBuilderWrapper>(file, db_path, options);
}

}  // namespace cedar
