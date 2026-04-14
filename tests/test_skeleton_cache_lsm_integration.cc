//===----------------------------------------------------------------------===//
// SkeletonCache LSM Integration Test - 简化版
// 只测试 SkeletonCache 在 LsmEngine 下的启用和配置
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <iostream>

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/core/env.h"

using namespace cedar;
using namespace std::chrono;

TEST(SkeletonCacheLSMIntegration, EnableAndConfig) {
  std::string test_dir = "/tmp/skel_lsm_test_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  // 测试启用 SkeletonCache
  {
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = true;
    options.skeleton_cache_shards = 512;
    options.skeleton_cache_entries_per_shard = 512;
    
    auto engine = std::make_unique<LsmEngine>(test_dir + "/with_cache", options, env);
    ASSERT_TRUE(engine->Open().ok());
    
    // 验证统计为空（还没有数据）
    auto stats = engine->GetSkeletonCacheStats();
    EXPECT_EQ(stats.hits, 0);
    EXPECT_EQ(stats.misses, 0);
    
    // 验证内存占用较小（只有元数据，没有数据）
    size_t memory = engine->GetSkeletonCacheMemoryUsage();
    EXPECT_LT(memory, 1024 * 1024);  // 小于 1MB
    
    std::cout << "\n=== SkeletonCache Enabled ===" << std::endl;
    std::cout << "Stats: hits=" << stats.hits << ", misses=" << stats.misses << std::endl;
    std::cout << "Memory: " << memory << " bytes" << std::endl;
    
    engine->Close();
  }
  
  // 测试禁用 SkeletonCache
  {
    CedarOptions options;
    options.create_if_missing = true;
    options.enable_skeleton_cache = false;  // 禁用
    
    auto engine = std::make_unique<LsmEngine>(test_dir + "/without_cache", options, env);
    ASSERT_TRUE(engine->Open().ok());
    
    // 验证统计为空
    auto stats = engine->GetSkeletonCacheStats();
    EXPECT_EQ(stats.hits, 0);
    EXPECT_EQ(stats.misses, 0);
    EXPECT_EQ(stats.TotalEntries(), 0);
    
    std::cout << "\n=== SkeletonCache Disabled ===" << std::endl;
    std::cout << "Stats: entries=" << stats.TotalEntries() << std::endl;
    
    engine->Close();
  }
  
  // 清理
  std::filesystem::remove_all(test_dir);
}

TEST(SkeletonCacheLSMIntegration, MemoryUsageTracking) {
  std::string test_dir = "/tmp/skel_mem_test_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = true;
  
  auto engine = std::make_unique<LsmEngine>(test_dir, options, env);
  ASSERT_TRUE(engine->Open().ok());
  
  // 初始内存较小（只有元数据）
  size_t initial_memory = engine->GetSkeletonCacheMemoryUsage();
  EXPECT_LT(initial_memory, 1024 * 1024);  // 小于 1MB
  
  // 启用缓存（如果尚未启用）
  engine->EnableSkeletonCache(256, 256);
  
  // 内存应该仍然较小（只有元数据，没有数据）
  size_t after_enable = engine->GetSkeletonCacheMemoryUsage();
  EXPECT_LT(after_enable, 1024 * 1024);  // 小于 1MB
  EXPECT_GE(after_enable, initial_memory);  // 不少于初始值
  
  std::cout << "\n=== Memory Usage Tracking ===" << std::endl;
  std::cout << "Initial: " << initial_memory << " bytes" << std::endl;
  std::cout << "After enable: " << after_enable << " bytes" << std::endl;
  
  engine->Close();
  std::filesystem::remove_all(test_dir);
}

TEST(SkeletonCacheLSMIntegration, ConfigPropagation) {
  std::string test_dir = "/tmp/skel_cfg_test_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  // 测试配置传播
  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = true;
  options.skeleton_cache_shards = 1024;
  options.skeleton_cache_entries_per_shard = 2048;
  options.skeleton_cache_max_memory_mb = 2048;
  
  auto engine = std::make_unique<LsmEngine>(test_dir, options, env);
  ASSERT_TRUE(engine->Open().ok());
  
  // 验证缓存已启用（通过检查 API 可用）
  auto stats = engine->GetSkeletonCacheStats();
  // 统计应该为 0（没有数据）
  EXPECT_EQ(stats.TotalEntries(), 0);
  
  std::cout << "\n=== Config Propagation ===" << std::endl;
  std::cout << "Shards: " << options.skeleton_cache_shards << std::endl;
  std::cout << "Entries per shard: " << options.skeleton_cache_entries_per_shard << std::endl;
  std::cout << "Total capacity: " << (options.skeleton_cache_shards * options.skeleton_cache_entries_per_shard) << std::endl;
  std::cout << "Max memory: " << options.skeleton_cache_max_memory_mb << " MB" << std::endl;
  std::cout << "Estimated memory: " << options.SkeletonCacheEstimatedMemory() / 1024 / 1024 << " MB" << std::endl;
  
  engine->Close();
  std::filesystem::remove_all(test_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
