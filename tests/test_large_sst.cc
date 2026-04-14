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
// 超大 SST 文件测试（64MB+）
// =============================================================================

#include <gtest/gtest.h>
#include <filesystem>
#include <random>
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"

using namespace cedar;

class LargeSSTTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  
  void SetUp() override {
    test_dir_ = "/tmp/cedar_large_sst_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);
  }
  
  void TearDown() override {
    std::system(("rm -rf " + test_dir_).c_str());
  }
  
  void WriteData(LsmEngine* engine, int num_records, uint64_t seed = 42) {
    std::mt19937_64 gen(seed);
    std::uniform_int_distribution<uint64_t> entity_dist(1000000, 9999999);
    std::uniform_int_distribution<uint64_t> ts_dist(1609459200000, 1704067200000);
    std::uniform_int_distribution<uint16_t> col_dist(0, 9);
    
    for (int i = 0; i < num_records; i++) {
      CedarKey key;
      key.SetEntityId(entity_dist(gen));
      key.SetTimestamp(Timestamp(ts_dist(gen)));
      key.SetColumnId(col_dist(gen));
      key.SetEntityType(0);
      key.SetSequence(i);
      
      Descriptor desc = Descriptor::InlineInt(0, i);
      engine->Put(key, desc, Timestamp(1));
    }
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
  
  size_t CountSSTFiles() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.path().extension() == ".sst") {
        count++;
      }
    }
    return count;
  }
};

TEST_F(LargeSSTTest, AccumulatedFlush_10MB) {
  std::cout << "\n=== 累积 Flush 测试（目标 10MB）===" << std::endl;
  
  CedarOptions options;
  options.create_if_missing = true;
  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  EXPECT_TRUE(engine->Open().ok());
  
  // 启用累积 Flush，目标 10MB
  engine->EnableAccumulatedFlush(10 * 1024 * 1024);
  EXPECT_TRUE(engine->IsAccumulatedFlushEnabled());
  
  // 分多批写入数据（每批 1万条，约 300KB）
  for (int batch = 0; batch < 50; batch++) {
    WriteData(engine.get(), 10000, batch);
    engine->ForceFlush();
    
    size_t accumulated = engine->GetAccumulatedSize();
    size_t files = CountSSTFiles();
    
    std::cout << "Batch " << batch << ": accumulated=" << accumulated / 1024 
              << " KB, files=" << files << std::endl;
    
    // 当达到 10MB 目标时，会自动 Flush 生成 SST
    // 所以 files 可能为 0 或 1（取决于是否已触发 Flush）
  }
  
  // 手动 Flush 剩余数据
  engine->FlushAccumulated();
  
  size_t final_files = CountSSTFiles();
  size_t final_size = GetTotalSSTSize();
  
  std::cout << "\n最终结果:" << std::endl;
  std::cout << "  SST 文件数: " << final_files << std::endl;
  std::cout << "  总大小: " << final_size / 1024 << " KB" << std::endl;
  
  EXPECT_GE(final_size, 10 * 1024 * 1024);  // >= 10MB
  EXPECT_LE(final_files, 5);  // 不超过 5 个文件
  
  engine->Close();
  std::cout << "✅ 累积 Flush 测试通过" << std::endl;
}

TEST_F(LargeSSTTest, LargeSST_64MB) {
  std::cout << "\n=== 超大 SST 测试（目标 64MB）===" << std::endl;
  
  CedarOptions options;
  options.create_if_missing = true;
  auto engine = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
  EXPECT_TRUE(engine->Open().ok());
  
  // 启用累积 Flush，目标 64MB
  engine->EnableAccumulatedFlush(64 * 1024 * 1024);
  
  // 写入约 200 万条记录（估算 40 bytes/条 = 80MB）
  const int total_records = 2000000;
  const int batch_size = 50000;
  
  std::cout << "写入 " << total_records << " 条记录..." << std::endl;
  
  for (int i = 0; i < total_records; i += batch_size) {
    WriteData(engine.get(), batch_size, i / batch_size);
    engine->ForceFlush();
    
    if ((i / batch_size) % 5 == 0) {
      size_t accumulated = engine->GetAccumulatedSize();
      std::cout << "  Progress: " << i << " records, accumulated=" 
                << accumulated / 1024 / 1024 << " MB" << std::endl;
    }
  }
  
  // 手动 Flush 剩余数据
  std::cout << "Flush 剩余数据..." << std::endl;
  engine->FlushAccumulated();
  
  size_t final_files = CountSSTFiles();
  size_t final_size = GetTotalSSTSize();
  
  std::cout << "\n最终结果:" << std::endl;
  std::cout << "  SST 文件数: " << final_files << std::endl;
  std::cout << "  总大小: " << final_size / 1024 / 1024 << " MB" << std::endl;
  std::cout << "  平均文件大小: " << final_size / final_files / 1024 / 1024 << " MB" << std::endl;
  
  // 验证：应该生成 1-2 个 64MB 级别的 SST
  EXPECT_GE(final_size, 60 * 1024 * 1024);  // >= 60MB
  EXPECT_LE(final_files, 3);  // 不超过 3 个文件
  
  engine->Close();
  std::cout << "✅ 超大 SST 测试通过" << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
