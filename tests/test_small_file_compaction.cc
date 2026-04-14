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
// 小文件自动合并测试
// =============================================================================

#include <gtest/gtest.h>
#include <filesystem>
#include <random>
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"

using namespace cedar;

class SmallFileCompactionTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  
  void SetUp() override {
    test_dir_ = "/tmp/cedar_small_file_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);
  }
  
  void TearDown() override {
    std::system(("rm -rf " + test_dir_).c_str());
  }
  
  void WriteSmallBatch(LsmEngine* engine, int batch_id, int records = 100) {
    std::mt19937_64 gen(batch_id);
    std::uniform_int_distribution<uint64_t> entity_dist(1000000 + batch_id * 1000, 
                                                        1000000 + (batch_id + 1) * 1000 - 1);
    std::uniform_int_distribution<uint64_t> ts_dist(1609459200000, 1704067200000);
    
    for (int i = 0; i < records; i++) {
      CedarKey key;
      key.SetEntityId(entity_dist(gen));
      key.SetTimestamp(Timestamp(ts_dist(gen)));
      key.SetColumnId(0);  // 使用单一 Column ID 生成小文件
      key.SetEntityType(0);
      key.SetSequence(i);
      
      Descriptor desc = Descriptor::InlineInt(0, batch_id * 1000 + i);
      engine->Put(key, desc, Timestamp(1));
    }
  }
  
  struct SSTStats {
    size_t file_count = 0;
    size_t total_size = 0;
    size_t small_files = 0;  // < 1MB
    size_t large_files = 0;  // >= 1MB
  };
  
  SSTStats GetSSTStats() {
    SSTStats stats;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.path().extension() == ".sst") {
        stats.file_count++;
        size_t size = entry.file_size();
        stats.total_size += size;
        if (size < 1024 * 1024) {
          stats.small_files++;
        } else {
          stats.large_files++;
        }
      }
    }
    return stats;
  }
};

TEST_F(SmallFileCompactionTest, SmallFileAutoMerge) {
  std::cout << "\n=== 小文件自动合并测试 ===" << std::endl;
  
  CedarOptions options;
  options.create_if_missing = true;
  // 启用后台合并，小文件阈值 8MB
  options.size_tiered_config.enable_background_compaction = true;
  options.size_tiered_config.compaction_threads = 2;
  options.size_tiered_config.l0_max_files = 4;
  
  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  EXPECT_TRUE(engine->Open().ok());
  
  std::cout << "写入 10 批小数据（每批 100 条，生成小文件）..." << std::endl;
  
  // 写入多批小数据（每批生成一个小文件）
  for (int i = 0; i < 10; i++) {
    WriteSmallBatch(engine.get(), i, 100);
    engine->ForceFlush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    auto stats = GetSSTStats();
    std::cout << "Batch " << i << ": files=" << stats.file_count 
              << ", small(<1MB)=" << stats.small_files 
              << ", large(>=1MB)=" << stats.large_files << std::endl;
  }
  
  std::cout << "\n等待后台合并完成..." << std::endl;
  engine->WaitForCompactions();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  auto final_stats = GetSSTStats();
  std::cout << "\n最终状态:" << std::endl;
  std::cout << "  总文件数: " << final_stats.file_count << std::endl;
  std::cout << "  小文件数(<1MB): " << final_stats.small_files << std::endl;
  std::cout << "  大文件数(>=1MB): " << final_stats.large_files << std::endl;
  std::cout << "  总大小: " << final_stats.total_size / 1024 << " KB" << std::endl;
  
  // 验证：小文件应该被合并
  EXPECT_LT(final_stats.small_files, 5);  // 小文件应该少于 5 个
  
  engine->Close();
  std::cout << "✅ 小文件自动合并测试通过" << std::endl;
}

TEST_F(SmallFileCompactionTest, ManualCompactSmallFiles) {
  std::cout << "\n=== 手动合并小文件测试 ===" << std::endl;
  
  CedarOptions options;
  options.create_if_missing = true;
  options.size_tiered_config.enable_background_compaction = false;  // 禁用后台，手动合并
  
  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  EXPECT_TRUE(engine->Open().ok());
  
  // 写入多批小数据
  std::cout << "写入 10 批小数据..." << std::endl;
  for (int i = 0; i < 10; i++) {
    WriteSmallBatch(engine.get(), i, 100);
    engine->ForceFlush();
  }
  
  auto before_stats = GetSSTStats();
  std::cout << "\n合并前:" << std::endl;
  std::cout << "  文件数: " << before_stats.file_count << std::endl;
  
  // 手动触发合并
  std::cout << "手动触发 CompactAll..." << std::endl;
  engine->CompactAll();
  engine->WaitForCompactions();
  
  auto after_stats = GetSSTStats();
  std::cout << "\n合并后:" << std::endl;
  std::cout << "  文件数: " << after_stats.file_count << std::endl;
  
  // 文件数应该减少
  EXPECT_LT(after_stats.file_count, before_stats.file_count);
  
  engine->Close();
  std::cout << "✅ 手动合并小文件测试通过" << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
