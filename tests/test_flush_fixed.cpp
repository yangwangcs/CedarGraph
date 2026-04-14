// 修复后的 Flush 测试
#include <iostream>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

int main() {
  std::string data_dir = "/tmp/flush_fixed";
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  options.enable_accumulated_flush = false;  // 禁用累积 Flush
  
  CedarGraphStorage* storage = nullptr;
  auto status = CedarGraphStorage::Open(options, data_dir, &storage);
  if (!status.ok()) {
    std::cerr << "Open failed: " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "Storage opened (accumulated_flush disabled)" << std::endl;

  // 写入数据
  std::cout << "\nWriting 10 records..." << std::endl;
  for (int i = 0; i < 10; i++) {
    uint64_t entity_id = 1000 + i;
    Timestamp now = Timestamp::Now();
    Descriptor desc(EntryKind::InlineInt, 1, i, sizeof(i));
    
    status = storage->Put(entity_id, now.value(), desc, now);
    if (!status.ok()) {
      std::cerr << "Put " << i << " failed: " << status.ToString() << std::endl;
    }
  }
  std::cout << "Write complete" << std::endl;
  
  // 检查 MemTable
  auto stats = storage->GetStats();
  std::cout << "\nStats before flush:" << std::endl;
  std::cout << "  memtable_size: " << stats.memtable_size << std::endl;
  
  // 强制刷盘
  std::cout << "\nCalling ForceFlush()..." << std::endl;
  status = storage->ForceFlush();
  std::cout << "Status: " << status.ToString() << std::endl;
  
  // 检查 SST
  stats = storage->GetStats();
  std::cout << "\nStats after flush:" << std::endl;
  std::cout << "  sst_count: " << stats.sst_count << std::endl;
  std::cout << "  sst_size: " << stats.sst_size << " bytes" << std::endl;
  
  // 列出文件
  std::cout << "\nFiles after flush:" << std::endl;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    if (entry.is_regular_file()) {
      std::cout << "  " << entry.path().filename().string() 
                << " (" << entry.file_size() << " bytes)" << std::endl;
    }
  }
  
  // 读取测试
  std::cout << "\n=== Read Test ===" << std::endl;
  int found = 0;
  for (int i = 0; i < 10; i++) {
    uint64_t entity_id = 1000 + i;
    auto result = storage->Get(entity_id, EntityType::Vertex, 1, Timestamp::Max());
    if (result.has_value()) {
      found++;
    }
  }
  std::cout << "Found " << found << "/10 records" << std::endl;

  delete storage;
  return (found == 10) ? 0 : 1;
}
