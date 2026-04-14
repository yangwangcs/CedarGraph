// 详细测试 Flush
#include <iostream>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

int main() {
  std::string data_dir = "/tmp/flush_debug";
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
  std::cout << "Storage opened" << std::endl;

  // 写入多条数据
  std::cout << "\nWriting 100 records..." << std::endl;
  for (int i = 0; i < 100; i++) {
    uint64_t entity_id = 1000 + i;
    Timestamp now = Timestamp::Now();
    Descriptor desc(EntryKind::InlineInt, 1, i, sizeof(i));
    
    status = storage->Put(entity_id, now.value(), desc, now);
    if (!status.ok()) {
      std::cerr << "Put " << i << " failed: " << status.ToString() << std::endl;
      break;
    }
  }
  std::cout << "Write complete" << std::endl;
  
  // 检查统计
  auto stats = storage->GetStats();
  std::cout << "\nStats before flush:" << std::endl;
  std::cout << "  memtable_size: " << stats.memtable_size << std::endl;
  std::cout << "  imm_memtable_size: " << stats.imm_memtable_size << std::endl;
  std::cout << "  sst_count: " << stats.sst_count << std::endl;
  
  // 列出文件
  std::cout << "\nFiles before flush:" << std::endl;
  int file_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    if (entry.is_regular_file()) {
      std::cout << "  " << entry.path().filename().string() 
                << " (" << entry.file_size() << " bytes)" << std::endl;
      file_count++;
    }
  }
  if (file_count == 0) std::cout << "  (no files)" << std::endl;
  
  // 第一次 Flush
  std::cout << "\n=== First ForceFlush() ===" << std::endl;
  status = storage->ForceFlush();
  std::cout << "Status: " << status.ToString() << std::endl;
  
  stats = storage->GetStats();
  std::cout << "\nStats after first flush:" << std::endl;
  std::cout << "  memtable_size: " << stats.memtable_size << std::endl;
  std::cout << "  imm_memtable_size: " << stats.imm_memtable_size << std::endl;
  std::cout << "  sst_count: " << stats.sst_count << std::endl;
  
  // 列出文件
  std::cout << "\nFiles after first flush:" << std::endl;
  file_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    if (entry.is_regular_file()) {
      std::cout << "  " << entry.path().filename().string() 
                << " (" << entry.file_size() << " bytes)" << std::endl;
      file_count++;
    }
  }
  if (file_count == 0) std::cout << "  (no files)" << std::endl;
  
  // 再写入一些数据
  std::cout << "\nWriting 50 more records..." << std::endl;
  for (int i = 0; i < 50; i++) {
    uint64_t entity_id = 2000 + i;
    Timestamp now = Timestamp::Now();
    Descriptor desc(EntryKind::InlineInt, 1, i, sizeof(i));
    storage->Put(entity_id, now.value(), desc, now);
  }
  
  // 第二次 Flush
  std::cout << "\n=== Second ForceFlush() ===" << std::endl;
  status = storage->ForceFlush();
  std::cout << "Status: " << status.ToString() << std::endl;
  
  // 最终文件列表
  std::cout << "\nFiles after second flush:" << std::endl;
  file_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    if (entry.is_regular_file()) {
      std::cout << "  " << entry.path().filename().string() 
                << " (" << entry.file_size() << " bytes)" << std::endl;
      file_count++;
    }
  }
  if (file_count == 0) std::cout << "  (no files)" << std::endl;

  delete storage;
  return 0;
}
