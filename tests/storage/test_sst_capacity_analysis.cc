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
// SST 容量分析 & 自动合并触发测试
// =============================================================================

#include <gtest/gtest.h>
#include <chrono>
#include <random>
#include <vector>
#include <filesystem>
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/sst/zone_columnar_reader.h"

using namespace cedar;

class SSTCapacityAnalysis : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/cedar_capacity_test_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    options.enable_accumulated_flush = false;  // 每个 ForceFlush 立即写 SST，避免累积导致循环无法结束
    
    // 配置合并参数（易于触发）
    options.size_tiered_config.l0_max_size = 2 * 1024 * 1024;     // 2MB L0 阈值
    options.size_tiered_config.level_size_trigger_ratio = 1.0;     // 100% 触发
    options.size_tiered_config.l0_max_files = 4;                   // L0 最多 4 个文件
    options.size_tiered_config.enable_background_compaction = true;
    options.size_tiered_config.compaction_threads = 2;
    
    engine_ = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
    EXPECT_TRUE(engine_->Open().ok());
  }

  void TearDown() override {
    engine_->Close();
    std::system(("rm -rf " + test_dir_).c_str());
  }

  // 获取目录中所有 SST 文件
  std::vector<std::pair<std::string, size_t>> GetSSTFiles() {
    std::vector<std::pair<std::string, size_t>> files;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.path().extension() == ".sst") {
        try {
          files.push_back({entry.path().filename().string(), entry.file_size()});
        } catch (const std::filesystem::filesystem_error&) {
          // 文件可能在遍历过程中被后台合并删除，忽略
        }
      }
    }
    return files;
  }

  // 获取 SST 文件数量
  size_t CountSSTFiles() {
    return GetSSTFiles().size();
  }

  // 获取总 SST 大小
  size_t GetTotalSSTSize() {
    size_t total = 0;
    for (const auto& [name, size] : GetSSTFiles()) {
      total += size;
    }
    return total;
  }

  std::string test_dir_;
  std::unique_ptr<LsmEngine> engine_;
};

TEST_F(SSTCapacityAnalysis, SingleSSTCapacity) {
  std::cout << "\n=== Single SST Capacity Analysis ===" << std::endl;
  
  // 写入不同规模的数据，观察 SST 文件大小
  std::vector<int> test_sizes = {1000, 5000, 10000, 50000, 100000};
  
  for (int num_records : test_sizes) {
    // 创建新的目录
    std::string sub_dir = test_dir_ + "_" + std::to_string(num_records);
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    options.enable_accumulated_flush = false;
    
    auto sub_engine = std::make_unique<LsmEngine>(sub_dir, options, cedar::Env::Default());
    sub_engine->Open();
    
    // 写入数据
    for (int i = 0; i < num_records; i++) {
      CedarKey key;
      key.SetEntityId(10000 + i);
      key.SetTimestamp(Timestamp(1000000 + i));
      key.SetColumnId(i % 10);
      key.SetEntityType(0);
      key.SetSequence(0);
      
      Descriptor desc = Descriptor::InlineInt(0, i);
      sub_engine->Put(key, desc, Timestamp(1));
    }
    
    sub_engine->ForceFlush();
    sub_engine->Close();
    
    // 统计 SST 文件
    size_t sst_count = 0;
    size_t total_size = 0;
    for (const auto& entry : std::filesystem::directory_iterator(sub_dir)) {
      if (entry.path().extension() == ".sst") {
        sst_count++;
        total_size += entry.file_size();
      }
    }
    
    double avg_size = sst_count > 0 ? (double)total_size / sst_count / 1024 : 0;
    double bytes_per_record = (double)total_size / num_records;
    
    std::cout << "Records: " << num_records 
              << " -> SST files: " << sst_count 
              << ", Total: " << total_size / 1024 << " KB"
              << ", Avg/file: " << avg_size << " KB"
              << ", Bytes/record: " << bytes_per_record << std::endl;
    
    std::system(("rm -rf " + sub_dir).c_str());
  }
}

TEST_F(SSTCapacityAnalysis, BlockSizeImpact) {
  std::cout << "\n=== Block Size Impact Analysis ===" << std::endl;
  
  // 测试不同 block_size 对 SST 文件大小的影响
  std::vector<size_t> block_sizes = {16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024};
  constexpr int NUM_RECORDS = 10000;
  
  for (size_t block_size : block_sizes) {
    std::string sub_dir = test_dir_ + "_bs" + std::to_string(block_size);
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    options.enable_accumulated_flush = false;
    // block_size is configured in SstBuilder::Options, not CedarOptions
    
    auto sub_engine = std::make_unique<LsmEngine>(sub_dir, options, cedar::Env::Default());
    sub_engine->Open();
    
    // 写入数据
    for (int i = 0; i < NUM_RECORDS; i++) {
      CedarKey key;
      key.SetEntityId(10000 + i);
      key.SetTimestamp(Timestamp(1000000 + i));
      key.SetColumnId(i % 10);
      key.SetEntityType(0);
      key.SetSequence(0);
      
      Descriptor desc = Descriptor::InlineInt(0, i);
      sub_engine->Put(key, desc, Timestamp(1));
    }
    
    sub_engine->ForceFlush();
    sub_engine->Close();
    
    // 统计
    size_t sst_count = 0;
    size_t total_size = 0;
    for (const auto& entry : std::filesystem::directory_iterator(sub_dir)) {
      if (entry.path().extension() == ".sst") {
        sst_count++;
        total_size += entry.file_size();
      }
    }
    
    std::cout << "Block size: " << block_size / 1024 << " KB"
              << " -> SST count: " << sst_count
              << ", Total: " << total_size / 1024 << " KB"
              << ", Overhead: " << (total_size - NUM_RECORDS * 12) * 100.0 / total_size << "%"
              << std::endl;
    
    std::system(("rm -rf " + sub_dir).c_str());
  }
}

TEST_F(SSTCapacityAnalysis, AutoCompactionTrigger) {
  std::cout << "\n=== Auto Compaction Trigger Test ===" << std::endl;
  std::cout << "L0 max size: 2MB, Trigger ratio: 100%" << std::endl;
  
  // 逐步写入数据，观察合并触发
  constexpr int BATCH_SIZE = 1000;
  constexpr int NUM_BATCHES = 20;
  
  for (int batch = 0; batch < NUM_BATCHES; batch++) {
    // 写入一批数据
    for (int i = 0; i < BATCH_SIZE; i++) {
      int record_id = batch * BATCH_SIZE + i;
      CedarKey key;
      key.SetEntityId(10000 + record_id);
      key.SetTimestamp(Timestamp(1000000 + record_id));
      key.SetColumnId(record_id % 10);
      key.SetEntityType(0);
      key.SetSequence(0);
      
      Descriptor desc = Descriptor::InlineInt(0, record_id);
      engine_->Put(key, desc, Timestamp(1));
    }
    
    engine_->ForceFlush();
    
    // 等待可能的合并完成
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    size_t sst_count = CountSSTFiles();
    size_t total_size = GetTotalSSTSize();
    
    std::cout << "Batch " << batch << " (" << (batch + 1) * BATCH_SIZE << " records): "
              << "SST files: " << sst_count 
              << ", Total size: " << total_size / 1024 << " KB" << std::endl;
  }
  
  // 等待后台合并完成
  std::cout << "\nWaiting for background compaction..." << std::endl;
  engine_->WaitForCompactions();
  
  size_t final_count = CountSSTFiles();
  size_t final_size = GetTotalSSTSize();
  
  std::cout << "Final state: SST files: " << final_count 
            << ", Total size: " << final_size / 1024 << " KB" << std::endl;
}

TEST_F(SSTCapacityAnalysis, SSTFileStructure) {
  std::cout << "\n=== SST File Structure Analysis ===" << std::endl;
  
  // 写入一些数据
  for (int i = 0; i < 1000; i++) {
    CedarKey key;
    key.SetEntityId(10000 + i);
    key.SetTimestamp(Timestamp(1000000 + i));
    key.SetColumnId(i % 10);
    key.SetEntityType(0);
    key.SetSequence(0);
    
    Descriptor desc = Descriptor::InlineInt(0, i);
    engine_->Put(key, desc, Timestamp(1));
  }
  
  engine_->ForceFlush();
  
  // 分析 SST 文件
  auto files = GetSSTFiles();
  if (!files.empty()) {
    const auto& [filename, filesize] = files[0];
    std::string filepath = test_dir_ + "/" + filename;
    
    std::cout << "Analyzing: " << filename << " (" << filesize << " bytes)" << std::endl;
    
    // 打开 SST 读取元数据
    ZoneColumnarSstReader reader(filepath);
    if (reader.Open().ok()) {
      std::cout << "  Entity range: [" << reader.MinEntityId() << ", " << reader.MaxEntityId() << "]" << std::endl;
      std::cout << "  Timestamp range: [" << reader.MinTimestamp() << ", " << reader.MaxTimestamp() << "]" << std::endl;
      std::cout << "  Avg bytes/entry: " << (double)filesize / 1000.0 << std::endl;
    }
  }
}

TEST_F(SSTCapacityAnalysis, RecommendedCompactionConfig) {
  std::cout << "\n=== Recommended Compaction Configuration ===" << std::endl;
  
  // 基于分析结果推荐配置
  std::cout << "\n【当前默认配置分析】" << std::endl;
  std::cout << "  Block size: 64 KB" << std::endl;
  std::cout << "  Block row limit: 4096" << std::endl;
  std::cout << "  L0 max size: 64 MB" << std::endl;
  std::cout << "  Trigger ratio: 1.2 (120%)" << std::endl;
  std::cout << "  -> 触发阈值: 76.8 MB" << std::endl;
  
  std::cout << "\n【推荐配置 - 开发/测试环境】" << std::endl;
  std::cout << "  block_size: 64 KB" << std::endl;
  std::cout << "  l0_max_size: 4 MB" << std::endl;
  std::cout << "  level_size_trigger_ratio: 1.0" << std::endl;
  std::cout << "  l0_max_files: 8" << std::endl;
  std::cout << "  -> 触发阈值: 4 MB (约 10-12 个 SST 文件)" << std::endl;
  
  std::cout << "\n【推荐配置 - 生产环境】" << std::endl;
  std::cout << "  block_size: 256 KB" << std::endl;
  std::cout << "  l0_max_size: 256 MB" << std::endl;
  std::cout << "  level_size_trigger_ratio: 1.2" << std::endl;
  std::cout << "  l0_max_files: 16" << std::endl;
  std::cout << "  -> 触发阈值: 307 MB" << std::endl;
  
  std::cout << "\n【极端压缩配置 - 高写入场景】" << std::endl;
  std::cout << "  block_size: 1 MB" << std::endl;
  std::cout << "  l0_max_size: 1 GB" << std::endl;
  std::cout << "  level_size_trigger_ratio: 1.5" << std::endl;
  std::cout << "  l0_max_files: 32" << std::endl;
  std::cout << "  -> 触发阈值: 1.5 GB" << std::endl;
  
  // 验证推荐配置
  std::cout << "\n【验证推荐配置】" << std::endl;
  std::string rec_dir = test_dir_ + "_recommended";
  CedarOptions rec_options;
  rec_options.create_if_missing = true;
  rec_options.enable_skeleton_cache = false;
  rec_options.enable_accumulated_flush = false;
  // rec_options.block_size is in sst builder options, not CedarOptions
  rec_options.size_tiered_config.l0_max_size = 4 * 1024 * 1024;      // 4MB
  rec_options.size_tiered_config.level_size_trigger_ratio = 1.0;      // 100%
  rec_options.size_tiered_config.l0_max_files = 8;
  rec_options.size_tiered_config.enable_background_compaction = false;
  
  auto rec_engine = std::make_unique<LsmEngine>(rec_dir, rec_options, cedar::Env::Default());
  rec_engine->Open();
  
  // 写入直到触发合并
  int batch = 0;
  while (true) {
    for (int i = 0; i < 1000; i++) {
      int record_id = batch * 1000 + i;
      CedarKey key;
      key.SetEntityId(10000 + record_id);
      key.SetTimestamp(Timestamp(1000000 + record_id));
      key.SetColumnId(record_id % 10);
      key.SetEntityType(0);
      key.SetSequence(0);
      
      Descriptor desc = Descriptor::InlineInt(0, record_id);
      rec_engine->Put(key, desc, Timestamp(1));
    }
    rec_engine->ForceFlush();
    batch++;
    
    // 统计
    size_t total_size = 0;
    try {
      for (const auto& entry : std::filesystem::directory_iterator(rec_dir)) {
        if (entry.path().extension() == ".sst") {
          try {
            total_size += entry.file_size();
          } catch (const std::filesystem::filesystem_error&) {
            // 文件可能在遍历过程中被后台合并删除，忽略
          }
        }
      }
    } catch (const std::filesystem::filesystem_error&) {
      // directory_iterator 本身可能在并发修改时抛出，忽略
    }
    
    std::cout << "Batch " << batch << ": " << total_size / 1024 << " KB" << std::endl;
    
    if (total_size > 5 * 1024 * 1024) break;  // 超过 5MB 停止
  }
  
  // 等待合并
  rec_engine->WaitForCompactions();
  
  size_t final_size = 0;
  size_t final_count = 0;
  try {
    for (const auto& entry : std::filesystem::directory_iterator(rec_dir)) {
      if (entry.path().extension() == ".sst") {
        try {
          final_size += entry.file_size();
          final_count++;
        } catch (const std::filesystem::filesystem_error&) {
          // 文件可能在遍历过程中被后台合并删除，忽略
        }
      }
    }
  } catch (const std::filesystem::filesystem_error&) {
    // directory_iterator 本身可能在并发修改时抛出，忽略
  }
  
  std::cout << "Final: " << final_count << " files, " << final_size / 1024 << " KB" << std::endl;
  std::cout << "Compaction triggered: " << (final_count < batch ? "YES" : "NO") << std::endl;
  
  rec_engine->Close();
  std::system(("rm -rf " + rec_dir).c_str());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
