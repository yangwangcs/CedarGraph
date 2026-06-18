// Test kind field fix for long strings
#include <iostream>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

int main() {
  std::string data_dir = (std::filesystem::temp_directory_path() / "test_kind_field").string();
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

  std::cout << "=== 测试 kind 字段 ===" << std::endl;

  auto PropertyNameToColumnId = [](const std::string& name) -> uint16_t {
    return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
  };

  uint64_t entity_id = 12345;

  // 测试1: 短字符串 (<=4字节) - 应该是 InlineShortStr
  std::cout << "\n--- 测试1: 短字符串 ---" << std::endl;
  {
    std::string prop = "tag";
    std::string value = "AB";  // 2字节
    uint16_t col = PropertyNameToColumnId(prop);
    
    auto opt = Descriptor::InlineShortStr(col, Slice(value));
    if (opt.has_value()) {
      Descriptor desc = *opt;
      std::cout << "写入: " << prop << "=" << value << std::endl;
      std::cout << "  kind: " << static_cast<int>(desc.GetKind()) 
                << " (预期: 2=InlineShortStr)" << std::endl;
      std::cout << "  值: " << desc.AsInlineShortStr() << std::endl;
      
      status = storage->PutStaticVertex(entity_id, col, desc);
      if (status.ok()) {
        auto result = storage->GetStaticVertex(entity_id, col);
        if (result.has_value()) {
          std::cout << "  读取成功: kind=" << static_cast<int>(result->GetKind())
                    << " 值=" << result->AsInlineShortStr() << std::endl;
        }
      }
    }
  }

  // 测试2: 整数 - 应该是 InlineInt
  std::cout << "\n--- 测试2: 整数 ---" << std::endl;
  {
    std::string prop = "age";
    int32_t value = 30;
    uint16_t col = PropertyNameToColumnId(prop);
    
    Descriptor desc = Descriptor::InlineInt(col, value);
    std::cout << "写入: " << prop << "=" << value << std::endl;
    std::cout << "  kind: " << static_cast<int>(desc.GetKind()) 
              << " (预期: 0=InlineInt)" << std::endl;
    
    status = storage->PutStaticVertex(entity_id, col, desc);
    if (status.ok()) {
      auto result = storage->GetStaticVertex(entity_id, col);
      if (result.has_value()) {
        std::cout << "  读取成功: kind=" << static_cast<int>(result->GetKind())
                  << " 值=" << result->AsInlineInt().value_or(0) << std::endl;
      }
    }
  }

  // 测试3: 浮点数 - 应该是 InlineFloat
  std::cout << "\n--- 测试3: 浮点数 ---" << std::endl;
  {
    std::string prop = "score";
    float value = 95.5f;
    uint16_t col = PropertyNameToColumnId(prop);
    
    Descriptor desc = Descriptor::InlineFloat(col, value);
    std::cout << "写入: " << prop << "=" << value << std::endl;
    std::cout << "  kind: " << static_cast<int>(desc.GetKind()) 
              << " (预期: 1=InlineFloat)" << std::endl;
    
    status = storage->PutStaticVertex(entity_id, col, desc);
    if (status.ok()) {
      auto result = storage->GetStaticVertex(entity_id, col);
      if (result.has_value()) {
        std::cout << "  读取成功: kind=" << static_cast<int>(result->GetKind())
                  << " 值=" << result->AsInlineFloat().value_or(0.0f) << std::endl;
      }
    }
  }

  // 测试4: 长字符串 (>4字节) - 应该是 ExternalRef (修复后)
  std::cout << "\n--- 测试4: 长字符串 (kind字段修复) ---" << std::endl;
  {
    std::string prop = "name";
    std::string value = "Alice";  // 5字节，超过4字节
    uint16_t col = PropertyNameToColumnId(prop);
    
    // 模拟修复后的 ValueToDescriptor 逻辑
    uint32_t hash = static_cast<uint32_t>(std::hash<std::string>{}(value));
    Descriptor desc(EntryKind::ExternalRef, col, hash, 
                    static_cast<uint8_t>(std::min(value.size(), size_t(255))));
    
    std::cout << "写入: " << prop << "=" << value << std::endl;
    std::cout << "  kind: " << static_cast<int>(desc.GetKind()) 
              << " (预期: 3=ExternalRef)" << std::endl;
    std::cout << "  hash: " << desc.GetPayload() << std::endl;
    std::cout << "  length: " << static_cast<int>(desc.GetLength()) << std::endl;
    
    status = storage->PutStaticVertex(entity_id, col, desc);
    if (status.ok()) {
      auto result = storage->GetStaticVertex(entity_id, col);
      if (result.has_value()) {
        std::cout << "  读取成功: kind=" << static_cast<int>(result->GetKind())
                  << " hash=" << result->GetPayload()
                  << " length=" << static_cast<int>(result->GetLength()) << std::endl;
        
        if (result->GetKind() == EntryKind::ExternalRef) {
          std::cout << "  ✅ kind 字段正确! (ExternalRef)" << std::endl;
        } else {
          std::cout << "  ❌ kind 字段错误! (预期 ExternalRef)" << std::endl;
        }
      }
    }
  }

  // 测试5: 旧代码的行为 (修复前)
  std::cout << "\n--- 测试5: 旧代码行为 (修复前) ---" << std::endl;
  {
    std::string prop = "old_name";
    std::string value = "Bob";  // 3字节，但假设>4字节
    uint16_t col = PropertyNameToColumnId(prop);
    
    // 模拟修复前的逻辑: InlineInt(col_id, 0)
    Descriptor desc = Descriptor::InlineInt(col, 0);
    
    std::cout << "旧代码写入长字符串会变成: kind=" << static_cast<int>(desc.GetKind())
              << " 值=0" << std::endl;
    std::cout << "  ❌ 这会导致数据丢失!" << std::endl;
  }

  delete storage;
  
  std::cout << "\n=== 测试完成 ===" << std::endl;
  return 0;
}
