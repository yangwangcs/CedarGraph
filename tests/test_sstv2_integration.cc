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
// Zone-Columnar SST 集成验证测试
// =============================================================================

#include <gtest/gtest.h>
#include <filesystem>
#include <random>
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/sst/sst_builder_factory.h"

using namespace cedar;

class SSTIntegrationTest : public ::testing::Test {
 protected:
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_sst_test_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }

  void WriteRealisticData(LsmEngine* engine, int count, int id_offset = 0) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> vertex_dist(1, 10000);
    std::uniform_int_distribution<> column_dist(0, 9);
    std::uniform_int_distribution<> ts_dist(1, 1000000);

    for (int i = 0; i < count; i++) {
      uint64_t vertex_id = vertex_dist(gen);
      uint16_t column_id = column_dist(gen);
      uint64_t timestamp = ts_dist(gen);
      
      // 使用简单的 Put 接口
      std::string data = "v" + std::to_string(id_offset + i);
      Status s = engine->Put(vertex_id, timestamp, Slice(data), Timestamp(timestamp));
      EXPECT_TRUE(s.ok());
    }
  }

  struct SSTStats {
    size_t file_count = 0;
    size_t total_size = 0;
    size_t avg_size = 0;
    size_t max_size = 0;
  };

  SSTStats GetSSTStats() {
    SSTStats stats;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.path().extension() == ".sst") {
        stats.file_count++;
        auto size = entry.file_size();
        stats.total_size += size;
        stats.max_size = std::max(stats.max_size, size);
      }
    }
    if (stats.file_count > 0) {
      stats.avg_size = stats.total_size / stats.file_count;
    }
    return stats;
  }
};

TEST_F(SSTIntegrationTest, DefaultFormat) {
  std::cout << "\n=== Zone-Columnar SST 默认格式验证 ===" << std::endl;
  
  CedarOptions options;
  options.create_if_missing = true;
  
  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  EXPECT_TRUE(engine->Open().ok());
  
  // 写入数据
  WriteRealisticData(engine.get(), 10000);
  engine->ForceFlush();
  
  auto stats = GetSSTStats();
  std::cout << "SST 文件统计:" << std::endl;
  std::cout << "  文件数: " << stats.file_count << std::endl;
  std::cout << "  总大小: " << stats.total_size / 1024 << " KB" << std::endl;
  std::cout << "  平均大小: " << stats.avg_size / 1024 << " KB" << std::endl;
  
  // 验证生成了合理大小的文件
  EXPECT_GT(stats.avg_size, 5 * 1024);   // 平均 > 5KB
  
  engine->Close();
  std::cout << "✅ 默认格式验证通过" << std::endl;
}

TEST_F(SSTIntegrationTest, LargeSSTFiles) {
  std::cout << "\n=== 大文件 SST 验证 ===" << std::endl;
  
  CedarOptions options;
  options.create_if_missing = true;
  // 大 SST 文件配置
  options.size_tiered_config.l0_file_size = 64 * 1024 * 1024;  // 64MB
  options.size_tiered_config.l0_max_files = 8;
  
  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  EXPECT_TRUE(engine->Open().ok());
  
  // 写入大量数据
  std::cout << "写入 50,000 条记录..." << std::endl;
  WriteRealisticData(engine.get(), 50000);
  engine->ForceFlush();
  
  auto stats = GetSSTStats();
  std::cout << "\nSST 文件统计:" << std::endl;
  std::cout << "  文件数: " << stats.file_count << std::endl;
  std::cout << "  总大小: " << stats.total_size / 1024 << " KB" << std::endl;
  std::cout << "  平均大小: " << stats.avg_size / 1024 << " KB" << std::endl;
  std::cout << "  最大大小: " << stats.max_size / 1024 << " KB" << std::endl;
  
  // 应该生成更少、更大的文件
  EXPECT_LT(stats.file_count, 50);
  EXPECT_GT(stats.avg_size, 20 * 1024);
  
  engine->Close();
  std::cout << "✅ 大文件验证通过" << std::endl;
}

TEST_F(SSTIntegrationTest, CompactionWithLargeSST) {
  std::cout << "\n=== Compaction 大文件合并验证 ===" << std::endl;
  
  CedarOptions options;
  options.create_if_missing = true;
  options.size_tiered_config.l0_max_size = 64 * 1024 * 1024;
  options.size_tiered_config.l0_max_files = 4;
  options.size_tiered_config.level_size_trigger_ratio = 1.0;
  options.size_tiered_config.enable_background_compaction = true;
  
  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  EXPECT_TRUE(engine->Open().ok());
  
  std::cout << "分 5 批写入数据..." << std::endl;
  for (int i = 0; i < 5; i++) {
    WriteRealisticData(engine.get(), 10000, i * 10000);
    engine->ForceFlush();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    auto stats = GetSSTStats();
    std::cout << "  Batch " << i << ": " << stats.file_count << " files, " 
              << stats.total_size / 1024 << " KB" << std::endl;
  }
  
  std::cout << "\n等待后台合并完成..." << std::endl;
  engine->WaitForCompactions();
  
  auto final_stats = GetSSTStats();
  std::cout << "合并后统计:" << std::endl;
  std::cout << "  文件数: " << final_stats.file_count << std::endl;
  std::cout << "  总大小: " << final_stats.total_size / 1024 << " KB" << std::endl;
  std::cout << "  平均大小: " << final_stats.avg_size / 1024 << " KB" << std::endl;
  
  // 合并后文件数应该减少
  EXPECT_LT(final_stats.file_count, 100);
  
  engine->Close();
  std::cout << "✅ Compaction 验证通过" << std::endl;
}
