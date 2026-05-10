//===----------------------------------------------------------------------===//
// CedarScan Crash Reproduction Test
//===----------------------------------------------------------------------===//

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <iostream>

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/query/cedar_scan.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/env.h"

using namespace cedar;
using namespace std::chrono;

TEST(CedarScanCrash, MultipleCalls) {
  std::string test_dir = "/tmp/scan_crash_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = true;  // 启用，测试完整路径
  
  auto engine = std::make_unique<LsmEngine>(test_dir, options, env);
  ASSERT_TRUE(engine->Open().ok());
  
  // 写入多个节点
  for (int i = 0; i < 10; ++i) {
    CedarKey key = CedarKey::Vertex(10000 + i, 1, Timestamp(100));
    Status s = engine->Put(key, Descriptor::InlineInt(1, i), Timestamp(1));
    ASSERT_TRUE(s.ok());
  }
  
  engine->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  std::cout << "Data written, about to call GetNode multiple times..." << std::endl;
  
  // 使用同一个 scan 对象多次调用 GetNode
  auto scan = CedarScan::At(Timestamp(150), engine.get());
  std::cout << "CedarScan created" << std::endl;
  
  for (int i = 0; i < 10; ++i) {
    std::cout << "  Calling GetNode(" << (10000 + i) << ")..." << std::endl;
    auto node = scan.GetNode(10000 + i);
    std::cout << "  Result: " << (node.has_value() ? "found" : "not found") << std::endl;
  }
  
  std::cout << "All GetNode calls completed" << std::endl;
  
  engine->Close();
  std::filesystem::remove_all(test_dir);
}

TEST(CedarScanCrash, MultipleScans) {
  std::string test_dir = "/tmp/scan_crash2_" + 
      std::to_string(system_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(test_dir);
  
  auto env = cedar::Env::Default();
  
  CedarOptions options;
  options.create_if_missing = true;
  options.enable_skeleton_cache = true;
  
  auto engine = std::make_unique<LsmEngine>(test_dir, options, env);
  ASSERT_TRUE(engine->Open().ok());
  
  // 写入节点
  for (int i = 0; i < 5; ++i) {
    CedarKey key = CedarKey::Vertex(10000 + i, 1, Timestamp(100));
    engine->Put(key, Descriptor::InlineInt(1, i), Timestamp(1));
  }
  
  engine->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  std::cout << "Testing multiple CedarScan instances..." << std::endl;
  
  // 创建多个 CedarScan 实例
  for (int iter = 0; iter < 5; ++iter) {
    std::cout << "  Iteration " << iter << ": creating CedarScan..." << std::endl;
    auto scan = CedarScan::At(Timestamp(150), engine.get());
    
    for (int i = 0; i < 5; ++i) {
      auto node = scan.GetNode(10000 + i);
      (void)node;
    }
    std::cout << "  Iteration " << iter << " completed" << std::endl;
  }
  
  std::cout << "All iterations completed" << std::endl;
  
  engine->Close();
  std::filesystem::remove_all(test_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
