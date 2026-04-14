// 调试存储测试
#include <iostream>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

int main() {
  std::string data_dir = "/tmp/debug_test";
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

  std::cout << "=== 测试 column_id 匹配 ===" << std::endl;

  // 写入时使用 column_id = 1
  uint64_t entity_id = 12345;
  Timestamp now = Timestamp::Now();
  uint16_t column_id = 1;
  
  std::cout << "Writing: entity_id=" << entity_id 
            << " column_id=" << column_id 
            << " tx_time=" << now.value() << std::endl;
  
  Descriptor desc(EntryKind::InlineInt, column_id, 42, sizeof(42));
  
  std::cout << "Descriptor column_id: " << desc.GetColumnId() << std::endl;
  
  status = storage->Put(entity_id, now.value(), desc, now);
  if (!status.ok()) {
    std::cerr << "Put failed: " << status.ToString() << std::endl;
    delete storage;
    return 1;
  }
  
  std::cout << "Put successful" << std::endl;

  // 立即读取 - 使用相同的 column_id
  std::cout << "\nReading with correct column_id=" << column_id << "..." << std::endl;
  auto result = storage->Get(entity_id, EntityType::Vertex, column_id, now);
  if (result.has_value()) {
    std::cout << "Get SUCCESS! column_id=" << column_id << " works" << std::endl;
  } else {
    std::cout << "Get FAILED with column_id=" << column_id << std::endl;
  }
  
  // 尝试不同 column_id
  std::cout << "\nReading with wrong column_id=999..." << std::endl;
  result = storage->Get(entity_id, EntityType::Vertex, 999, now);
  if (result.has_value()) {
    std::cout << "Get SUCCESS with column_id=999 (unexpected)" << std::endl;
  } else {
    std::cout << "Get FAILED with column_id=999 (expected)" << std::endl;
  }
  
  // 刷盘
  std::cout << "\n=== Force Flush ===" << std::endl;
  storage->ForceFlush();
  
  // 检查生成的文件
  std::cout << "\nGenerated files:" << std::endl;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    if (entry.is_regular_file()) {
      std::cout << "  " << entry.path().filename().string() 
                << " (" << entry.file_size() << " bytes)" << std::endl;
    }
  }
  
  // 刷盘后读取
  std::cout << "\nReading after flush with column_id=" << column_id << "..." << std::endl;
  result = storage->Get(entity_id, EntityType::Vertex, column_id, Timestamp::Max());
  if (result.has_value()) {
    std::cout << "Get after flush SUCCESS!" << std::endl;
  } else {
    std::cout << "Get after flush FAILED" << std::endl;
  }
  
  // 尝试所有 column_id 0-10
  std::cout << "\nTrying all column_ids 0-10:" << std::endl;
  for (uint16_t col = 0; col <= 10; col++) {
    result = storage->Get(entity_id, EntityType::Vertex, col, Timestamp::Max());
    if (result.has_value()) {
      std::cout << "  column_id=" << col << ": FOUND" << std::endl;
    }
  }

  delete storage;
  return 0;
}
