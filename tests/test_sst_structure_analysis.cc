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
// SST 文件结构分析 - Block 数量和大小
// =============================================================================

#include <gtest/gtest.h>
#include <filesystem>
#include <iomanip>
#include "cedar/storage/lsm_engine.h"
#include "cedar/sst/zone_columnar_reader.h"

using namespace cedar;

class SSTStructureAnalysis : public ::testing::Test {
 protected:
  std::string test_dir_;
  std::unique_ptr<LsmEngine> engine_;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_struct_test_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    
    engine_ = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
    EXPECT_TRUE(engine_->Open().ok());
  }

  void TearDown() override {
    engine_->Close();
    std::system(("rm -rf " + test_dir_).c_str());
  }

  void WriteRecords(int num_records, int start_id = 0) {
    for (int i = 0; i < num_records; i++) {
      int record_id = start_id + i;
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

  struct SSTInfo {
    std::string filename;
    size_t file_size;
    int num_blocks;
    int total_rows;
    uint64_t min_entity_id;
    uint64_t max_entity_id;
  };

  std::vector<SSTInfo> AnalyzeSSTFiles() {
    std::vector<SSTInfo> infos;
    
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
      if (entry.path().extension() == ".sst") {
        SSTInfo info;
        info.filename = entry.path().filename().string();
        info.file_size = entry.file_size();
        
        // 打开 SST 读取信息
        ZoneColumnarSstReader reader(entry.path().string());
        if (reader.Open().ok()) {
          info.num_blocks = reader.NumBlocks();
          info.total_rows = static_cast<int>(reader.NumEntries());
          info.min_entity_id = reader.MinEntityId();
          info.max_entity_id = reader.MaxEntityId();
        }
        
        infos.push_back(info);
      }
    }
    
    return infos;
  }
};

TEST_F(SSTStructureAnalysis, SingleSSTBlockCount) {
  std::cout << "\n=== SST 文件 Block 数量分析 ===" << std::endl;
  
  // 测试不同数据量下的 Block 数量
  std::vector<int> record_counts = {100, 500, 1000, 2000, 5000};
  
  for (int count : record_counts) {
    std::string sub_dir = test_dir_ + "_" + std::to_string(count);
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    
    auto sub_engine = std::make_unique<LsmEngine>(sub_dir, options, cedar::Env::Default());
    sub_engine->Open();
    
    // 写入数据
    for (int i = 0; i < count; i++) {
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
    
    // 分析 SST
    for (const auto& entry : std::filesystem::directory_iterator(sub_dir)) {
      if (entry.path().extension() == ".sst") {
        ZoneColumnarSstReader reader(entry.path().string());
        if (reader.Open().ok()) {
          std::cout << "Records: " << std::setw(5) << count 
                    << " -> File: " << std::setw(8) << entry.file_size() << " bytes"
                    << " (" << std::setw(5) << entry.file_size() / 1024 << " KB)"
                    << ", Blocks: " << std::setw(2) << reader.NumBlocks()
                    << ", Rows/Block: " << std::setw(4) << reader.NumEntries() / reader.NumBlocks()
                    << std::endl;
        }
      }
    }
    
    std::system(("rm -rf " + sub_dir).c_str());
  }
}

TEST_F(SSTStructureAnalysis, BlockSizeDistribution) {
  std::cout << "\n=== Block 大小分布分析 ===" << std::endl;
  
  // 写入大量数据，分析 Block 分布
  WriteRecords(10000);
  
  auto infos = AnalyzeSSTFiles();
  
  std::cout << "生成了 " << infos.size() << " 个 SST 文件" << std::endl;
  
  size_t total_size = 0;
  int total_blocks = 0;
  
  for (const auto& info : infos) {
    total_size += info.file_size;
    total_blocks += info.num_blocks;
    
    double avg_block_size = info.num_blocks > 0 
        ? static_cast<double>(info.file_size) / info.num_blocks 
        : 0;
    
    std::cout << "File: " << std::setw(12) << info.filename
              << ", Size: " << std::setw(6) << info.file_size / 1024 << " KB"
              << ", Blocks: " << std::setw(2) << info.num_blocks
              << ", Rows: " << std::setw(5) << info.total_rows
              << ", Avg Block: " << std::fixed << std::setprecision(1) 
              << avg_block_size / 1024 << " KB"
              << std::endl;
  }
  
  std::cout << "\n总计:"
            << "\n  文件数: " << infos.size()
            << "\n  总大小: " << total_size / 1024 << " KB"
            << "\n  总 Blocks: " << total_blocks
            << "\n  平均 Block 大小: " << (total_blocks > 0 ? total_size / total_blocks / 1024 : 0) << " KB"
            << std::endl;
}

TEST_F(SSTStructureAnalysis, SSTFileStructureBreakdown) {
  std::cout << "\n=== SST 文件结构分解 ===" << std::endl;
  
  WriteRecords(5000);
  
  // 找到第一个 SST 文件
  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    if (entry.path().extension() == ".sst" && entry.file_size() > 0) {
      std::cout << "\n分析文件: " << entry.path().filename() << std::endl;
      std::cout << "文件大小: " << entry.file_size() << " bytes (" 
                << entry.file_size() / 1024 << " KB)" << std::endl;
      
      ZoneColumnarSstReader reader(entry.path().string());
      if (reader.Open().ok()) {
        std::cout << "Block 数量: " << reader.NumBlocks() << std::endl;
        std::cout << "总记录数: " << reader.NumEntries() << std::endl;
        std::cout << "Entity 范围: [" << reader.MinEntityId() 
                  << ", " << reader.MaxEntityId() << "]" << std::endl;
        std::cout << "Timestamp 范围: [" << reader.MinTimestamp() 
                  << ", " << reader.MaxTimestamp() << "]" << std::endl;
        
        // 估算各区域大小
        std::cout << "\n估算结构:"
                  << "\n  - Header: ~256 bytes"
                  << "\n  - Blocks (数据): ~" << (entry.file_size() * 0.85 / 1024) << " KB"
                  << "\n  - Block Info 表: ~" << (reader.NumBlocks() * 40 / 1024.0) << " KB"
                  << "\n  - Zone Maps: ~64 bytes"
                  << "\n  - Restart Points: ~" << (reader.NumBlocks() * 16 / 1024.0) << " KB"
                  << "\n  - Entity Index: ~" << (reader.NumEntries() * 12 / 1024.0) << " KB"
                  << "\n  - Footer: ~128 bytes"
                  << std::endl;
      }
      break;
    }
  }
}

TEST_F(SSTStructureAnalysis, CompareDifferentDataSizes) {
  std::cout << "\n=== 不同数据规模的 SST 对比 ===" << std::endl;
  
  struct TestCase {
    int records;
    const char* desc;
  };
  
  std::vector<TestCase> cases = {
    {1000, "小数据量"},
    {10000, "中等数据量"},
    {50000, "大数据量"},
  };
  
  for (const auto& tc : cases) {
    std::string sub_dir = test_dir_ + "_" + std::to_string(tc.records);
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;
    
    auto sub_engine = std::make_unique<LsmEngine>(sub_dir, options, cedar::Env::Default());
    sub_engine->Open();
    
    // 写入数据
    for (int i = 0; i < tc.records; i++) {
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
    size_t total_size = 0;
    int total_files = 0;
    int total_blocks = 0;
    
    for (const auto& entry : std::filesystem::directory_iterator(sub_dir)) {
      if (entry.path().extension() == ".sst" && entry.file_size() > 0) {
        total_files++;
        total_size += entry.file_size();
        
        ZoneColumnarSstReader reader(entry.path().string());
        if (reader.Open().ok()) {
          total_blocks += reader.NumBlocks();
        }
      }
    }
    
    std::cout << "\n" << tc.desc << " (" << tc.records << " records):"
              << "\n  SST 文件数: " << total_files
              << "\n  总大小: " << total_size / 1024 << " KB"
              << "\n  总 Blocks: " << total_blocks
              << "\n  平均记录大小: " << (double)total_size / tc.records << " bytes"
              << "\n  平均 Block 大小: " << (total_blocks > 0 ? total_size / total_blocks / 1024 : 0) << " KB"
              << std::endl;
    
    std::system(("rm -rf " + sub_dir).c_str());
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
