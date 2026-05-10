// 简单存储测试
#include <iostream>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

int main() {
  std::string data_dir = "/tmp/simple_test";
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

  std::cout << "Storage opened successfully" << std::endl;

  // 写入一个简单的值
  uint64_t entity_id = 12345;
  Timestamp now = Timestamp::Now();
  Descriptor desc(EntryKind::InlineInt, 1, 42, sizeof(42));
  
  std::cout << "Writing entity_id=" << entity_id << " tx_time=" << now.value() << std::endl;
  
  status = storage->Put(entity_id, now.value(), desc, now);
  if (!status.ok()) {
    std::cerr << "Put failed: " << status.ToString() << std::endl;
    delete storage;
    return 1;
  }
  
  std::cout << "Put successful" << std::endl;

  // 立即读取
  std::cout << "Reading with tx_time=" << now.value() << std::endl;
  auto result = storage->Get(entity_id, now.value());
  if (result.has_value()) {
    std::cout << "Get successful! Value found" << std::endl;
  } else {
    std::cerr << "Get failed: not found" << std::endl;
  }
  
  // 尝试用 max 时间戳读取
  std::cout << "Reading with max tx_time" << std::endl;
  result = storage->Get(entity_id, std::numeric_limits<uint64_t>::max());
  if (result.has_value()) {
    std::cout << "Get with max successful!" << std::endl;
  } else {
    std::cerr << "Get with max failed: not found" << std::endl;
  }

  // 刷盘
  std::cout << "Force flush..." << std::endl;
  storage->ForceFlush();
  
  // 再次读取
  std::cout << "Reading after flush..." << std::endl;
  result = storage->Get(entity_id, std::numeric_limits<uint64_t>::max());
  if (result.has_value()) {
    std::cout << "Get after flush successful!" << std::endl;
  } else {
    std::cerr << "Get after flush failed: not found" << std::endl;
  }

  delete storage;
  return 0;
}
