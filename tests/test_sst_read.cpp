#include <iostream>
#include <fstream>
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/sst/zone_columnar_format_v2.h"

using namespace cedar;

int main() {
  std::string file_path = "/tmp/flush_fixed/1.sst";
  
  // 直接读取文件检查 header
  std::ifstream file(file_path, std::ios::binary);
  if (!file) {
    std::cerr << "Cannot open file" << std::endl;
    return 1;
  }
  
  // 读取 header
  ZoneColumnarHeaderV2 header;
  file.read(reinterpret_cast<char*>(&header), sizeof(header));
  
  std::cout << "Header magic: 0x" << std::hex << header.magic << std::dec << std::endl;
  std::cout << "Header version: " << header.version << std::endl;
  std::cout << "Header file_size: " << header.file_size << std::endl;
  std::cout << "Header min_entity_id: " << header.min_entity_id << std::endl;
  std::cout << "Header max_entity_id: " << header.max_entity_id << std::endl;
  std::cout << "Header column_id: " << header.column_id << " (UINT16_MAX=" << UINT16_MAX << ")" << std::endl;
  std::cout << "Header entity_type: " << (int)header.entity_type << std::endl;
  
  file.close();
  
  // 使用 reader 打开
  std::cout << "\n=== Using ZoneColumnarSstReader ===" << std::endl;
  ZoneColumnarSstReader reader(file_path);
  Status s = reader.Open();
  std::cout << "Open status: " << s.ToString() << std::endl;
  
  if (s.ok()) {
    std::cout << "NumEntries: " << reader.NumEntries() << std::endl;
    std::cout << "NumBlocks: " << reader.NumBlocks() << std::endl;
    std::cout << "MinEntityId: " << reader.MinEntityId() << std::endl;
    std::cout << "MaxEntityId: " << reader.MaxEntityId() << std::endl;
    
    // 尝试查询一个 entity
    uint64_t entity_id = 1000;
    std::cout << "\nQuerying entity_id=" << entity_id << std::endl;
    
    bool may_contain = reader.MayContainEntity(entity_id);
    std::cout << "MayContainEntity: " << (may_contain ? "true" : "false") << std::endl;
    
    auto result = reader.GetAtTime(entity_id, EntityType::Vertex, 1, Timestamp::Max());
    std::cout << "GetAtTime result: " << (result.has_value() ? "found" : "not found") << std::endl;
  }
  
  return 0;
}
