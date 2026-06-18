// Test property name reverse mapping (column_id -> property_name)
#include <iostream>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

int main() {
  std::string data_dir = (std::filesystem::temp_directory_path() / "test_property_mapping").string();
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

  std::cout << "=== 测试属性名反向映射 ===" << std::endl;

  // 模拟 PropertyNameToColumnId 函数
  auto PropertyNameToColumnId = [](const std::string& name) -> uint16_t {
    return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
  };

  // 定义属性
  std::string prop_name = "age";
  uint16_t col_id = PropertyNameToColumnId(prop_name);
  
  std::cout << "属性名: " << prop_name << std::endl;
  std::cout << "计算的 column_id: " << col_id << std::endl;

  // 注册属性名映射
  storage->RegisterPropertyName(col_id, prop_name);
  std::cout << "已注册属性名映射" << std::endl;

  // 写入数据
  uint64_t entity_id = 12345;
  Descriptor desc = Descriptor::InlineInt(col_id, 30);  // age = 30
  
  status = storage->PutStaticVertex(entity_id, col_id, desc);
  if (!status.ok()) {
    std::cerr << "PutStaticVertex failed: " << status.ToString() << std::endl;
    delete storage;
    return 1;
  }
  std::cout << "写入成功: entity_id=" << entity_id << " " << prop_name << "=30" << std::endl;

  // 读取数据
  auto result = storage->GetStaticVertex(entity_id, col_id);
  if (!result.has_value()) {
    std::cerr << "GetStaticVertex failed" << std::endl;
    delete storage;
    return 1;
  }

  std::cout << "\n读取结果:" << std::endl;
  std::cout << "  column_id: " << result->GetColumnId() << std::endl;
  std::cout << "  kind: " << static_cast<int>(result->GetKind()) << std::endl;
  
  auto int_val = result->AsInlineInt();
  if (int_val.has_value()) {
    std::cout << "  值: " << *int_val << std::endl;
  }

  // 测试反向映射
  std::cout << "\n=== 测试反向映射 ===" << std::endl;
  std::string mapped_name = storage->GetPropertyName(col_id);
  std::cout << "GetPropertyName(" << col_id << ") = " << mapped_name << std::endl;
  
  if (mapped_name == prop_name) {
    std::cout << "✅ 反向映射正确!" << std::endl;
  } else {
    std::cout << "❌ 反向映射错误! 期望: " << prop_name << " 实际: " << mapped_name << std::endl;
  }

  // 测试未注册的 column_id
  uint16_t unknown_col = 9999;
  std::string unknown_name = storage->GetPropertyName(unknown_col);
  std::cout << "\nGetPropertyName(" << unknown_col << ") = " << unknown_name << std::endl;
  std::cout << "  (预期返回 fallback 格式 col_XXXX)" << std::endl;

  // 测试多个属性
  std::cout << "\n=== 测试多个属性 ===" << std::endl;
  
  struct PropTest {
    std::string name;
    int32_t value;
  };
  
  PropTest props[] = {
    {"name", 0},  // 字符串属性，用0占位
    {"score", 95},
    {"level", 5}
  };
  
  for (const auto& prop : props) {
    uint16_t cid = PropertyNameToColumnId(prop.name);
    storage->RegisterPropertyName(cid, prop.name);
    
    Descriptor d = Descriptor::InlineInt(cid, prop.value);
    status = storage->PutStaticVertex(entity_id, cid, d);
    
    if (status.ok()) {
      std::cout << "写入 " << prop.name << "=" << prop.value 
                << " (col_id=" << cid << ")" << std::endl;
    }
  }
  
  // 读取并验证所有属性
  std::cout << "\n读取所有属性:" << std::endl;
  for (const auto& prop : props) {
    uint16_t cid = PropertyNameToColumnId(prop.name);
    auto r = storage->GetStaticVertex(entity_id, cid);
    
    if (r.has_value()) {
      std::string mapped = storage->GetPropertyName(r->GetColumnId());
      auto val = r->AsInlineInt();
      
      std::cout << "  " << mapped << "=" << (val.has_value() ? std::to_string(*val) : "null")
                << " (col_id=" << r->GetColumnId() << ")" << std::endl;
    }
  }

  delete storage;
  
  std::cout << "\n=== 测试完成 ===" << std::endl;
  return 0;
}
