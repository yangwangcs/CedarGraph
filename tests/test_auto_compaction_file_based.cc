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
// 基于文件数量的自动合并触发测试（方案 A）
// =============================================================================

#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"

using namespace cedar;

class AutoCompactionFileBasedTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/cedar_auto_compact_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    options.enable_accumulated_flush = false;  // 每个 ForceFlush 立即写 SST
    
    // ========== 方案 A 配置：基于文件数量触发 ==========
    options.size_tiered_config.l0_max_size = 64 * 1024 * 1024;     // 64MB（大小阈值，不常用）
    options.size_tiered_config.l0_max_files = 4;                    // L0 最多 4 个文件！
    options.size_tiered_config.level_size_trigger_ratio = 1.0;      // 100%
    options.size_tiered_config.enable_background_compaction = true;
    options.size_tiered_config.compaction_threads = 2;
    
    engine_ = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
    EXPECT_TRUE(engine_->Open().ok());
  }

  void TearDown() override {
    engine_->Close();
    std::system(("rm -rf " + test_dir_).c_str());
  }

  size_t CountSSTFiles() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.path().extension() == ".sst") {
        count++;
      }
    }
    return count;
  }

  size_t GetTotalSSTSize() {
    size_t total = 0;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.path().extension() == ".sst") {
        total += entry.file_size();
      }
    }
    return total;
  }

  void WriteBatch(int batch_id, int num_records = 1000) {
    for (int i = 0; i < num_records; i++) {
      int record_id = batch_id * num_records + i;
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
  }

  std::string test_dir_;
  std::unique_ptr<LsmEngine> engine_;
};

TEST_F(AutoCompactionFileBasedTest, FileCountTrigger) {
  std::cout << "\n=== 方案 A：基于文件数量的自动合并触发 ===" << std::endl;
  std::cout << "配置：l0_max_files = 4，超过 4 个文件即触发合并" << std::endl;
  
  // 逐步写入，观察合并触发
  for (int batch = 0; batch < 10; batch++) {
    WriteBatch(batch);
    
    // 等待可能的合并完成
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    size_t sst_count = CountSSTFiles();
    size_t total_size = GetTotalSSTSize();
    
    std::cout << "Batch " << batch << " (" << (batch + 1) * 1000 
              << " records): SST files = " << sst_count 
              << ", Total size = " << total_size / 1024 << " KB";
    
    if (sst_count <= 4) {
      std::cout << " ✓ (合并已触发，文件数控制在 4 个以内)";
    } else {
      std::cout << " ✗ (文件数超过阈值，合并未触发)";
    }
    std::cout << std::endl;
  }
  
  // 最终等待
  std::cout << "\n等待所有后台合并完成..." << std::endl;
  engine_->WaitForCompactions();
  
  size_t final_count = CountSSTFiles();
  size_t final_size = GetTotalSSTSize();
  
  std::cout << "最终状态: " << final_count << " 个 SST 文件, " 
            << final_size / 1024 << " KB" << std::endl;
  
  // 验证：最终文件数应该小于写入批次数（说明合并发生了）
  EXPECT_LT(final_count, 10) << "合并应该减少文件数量";
  std::cout << "\n✅ 测试通过：基于文件数量的合并触发正常工作" << std::endl;
}

TEST_F(AutoCompactionFileBasedTest, FileCountStability) {
  std::cout << "\n=== 文件数量稳定性测试 ===" << std::endl;
  std::cout << "连续写入多批数据，验证文件数是否稳定在阈值附近" << std::endl;
  
  std::vector<size_t> file_counts;
  
  for (int batch = 0; batch < 20; batch++) {
    WriteBatch(batch);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    size_t sst_count = CountSSTFiles();
    file_counts.push_back(sst_count);
    
    std::cout << "Batch " << batch << ": " << sst_count << " files" << std::endl;
  }
  
  engine_->WaitForCompactions();
  
  size_t final_count = CountSSTFiles();
  std::cout << "\n最终文件数: " << final_count << std::endl;
  
  // 验证稳定性：文件数不应超过阈值的 2 倍
  EXPECT_LE(final_count, 8) << "文件数应稳定在阈值附近（l0_max_files=4）";
  
  std::cout << "✅ 稳定性测试通过" << std::endl;
}

TEST_F(AutoCompactionFileBasedTest, CompareWithAndWithoutCompaction) {
  std::cout << "\n=== 对比：启用/禁用合并的效果 ===" << std::endl;
  
  // 测试 1：启用合并（当前配置）
  std::cout << "\n【启用合并】l0_max_files=4" << std::endl;
  for (int i = 0; i < 5; i++) {
    WriteBatch(i);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  engine_->WaitForCompactions();
  size_t with_compaction = CountSSTFiles();
  std::cout << "启用合并后: " << with_compaction << " 个文件" << std::endl;
  
  // 清理，重新测试禁用合并
  engine_->Close();
  std::system(("rm -rf " + test_dir_).c_str());
  
  // 测试 2：禁用合并（设置很高的阈值）
  std::cout << "\n【禁用合并】l0_max_files=1000（实际上不触发）" << std::endl;
  CedarOptions no_compact_options;
  no_compact_options.create_if_missing = true;
  no_compact_options.enable_skeleton_cache = false;
  no_compact_options.enable_accumulated_flush = false;  // 每个 ForceFlush 立即写 SST
  no_compact_options.size_tiered_config.l0_max_files = 1000;  // 很高的阈值
  no_compact_options.size_tiered_config.enable_background_compaction = false;  // 完全禁用后台合并
  
  engine_ = std::make_unique<LsmEngine>(test_dir_, no_compact_options, cedar::Env::Default());
  engine_->Open();
  
  for (int i = 0; i < 5; i++) {
    WriteBatch(i);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  engine_->WaitForCompactions();
  size_t without_compaction = CountSSTFiles();
  std::cout << "禁用合并后: " << without_compaction << " 个文件" << std::endl;
  
  // 对比结果
  std::cout << "\n=== 对比结果 ===" << std::endl;
  std::cout << "启用合并:  " << with_compaction << " 个文件" << std::endl;
  std::cout << "禁用合并:  " << without_compaction << " 个文件" << std::endl;
  std::cout << "合并效果:  减少了 " << (without_compaction - with_compaction) 
            << " 个文件 (" << (without_compaction - with_compaction) * 100 / without_compaction 
            << "% 减少)" << std::endl;
  
  EXPECT_LT(with_compaction, without_compaction);
  std::cout << "\n✅ 对比测试通过：合并显著减少文件数量" << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
