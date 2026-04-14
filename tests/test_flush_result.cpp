// 测试 Flush 返回值
#include <iostream>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

int main() {
  std::string data_dir = "/tmp/flush_test";
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarGraphStorage* storage = nullptr;
  CedarOptions options;
  options.create_if_missing = true;
  
  auto status = CedarGraphStorage::Open(options, data_dir, &storage);
  if (!status.ok()) {
    std::cerr << "Open failed: " << status.ToString() << std::endl;
    return 1;
  }

  // 写入数据
  uint64_t entity_id = 12345;
  Timestamp now = Timestamp::Now();
  Descriptor desc(EntryKind::InlineInt, 1, 42, sizeof(42));
  
  status = storage->Put(entity_id, now.value(), desc, now);
  std::cout << "Put status: " << status.ToString() << std::endl;
  
  // 获取写入前的文件列表
  std::cout << "\nFiles before flush:" << std::endl;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    if (entry.is_regular_file()) {
      std::cout << "  " << entry.path().filename().string() << std::endl;
    }
  }
  
  // 强制刷盘并检查返回值
  std::cout << "\nCalling ForceFlush()..." << std::endl;
  status = storage->ForceFlush();
  std::cout << "ForceFlush status: " << status.ToString() << std::endl;
  
  // 获取写入后的文件列表
  std::cout << "\nFiles after flush:" << std::endl;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    if (entry.is_regular_file()) {
      std::cout << "  " << entry.path().filename().string() 
                << " (" << entry.file_size() << " bytes)" << std::endl;
    }
  }
  
  // 尝试多次 flush
  for (int i = 0; i < 3; i++) {
    status = storage->ForceFlush();
    std::cout << "ForceFlush attempt " << (i+1) << ": " << status.ToString() << std::endl;
  }

  delete storage;
  return 0;
}
