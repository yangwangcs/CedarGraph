// Debug SST file loading
#include <iostream>
#include <filesystem>
#include "cedar/sst/zone_columnar_reader.h"

int main() {
  std::string sst_file = "/tmp/test_flush_db/3.sst";
  
  std::cout << "=== Debugging SST File Loading ===" << std::endl;
  std::cout << "File: " << sst_file << std::endl;
  
  // Check file exists
  if (!std::filesystem::exists(sst_file)) {
    std::cerr << "File does not exist!" << std::endl;
    return 1;
  }
  
  std::cout << "File size: " << std::filesystem::file_size(sst_file) << " bytes" << std::endl;
  
  // Try to open with SstReader
  cedar::SstReader reader(sst_file);
  cedar::Status s = reader.Open();
  
  if (!s.ok()) {
    std::cerr << "Failed to open SST: " << s.ToString() << std::endl;
    return 1;
  }
  
  std::cout << "SST opened successfully!" << std::endl;
  std::cout << "  Entries: " << reader.NumEntries() << std::endl;
  std::cout << "  Min Entity ID: " << reader.MinEntityId() << std::endl;
  std::cout << "  Max Entity ID: " << reader.MaxEntityId() << std::endl;
  std::cout << "  Min Timestamp: " << reader.MinTimestamp() << std::endl;
  std::cout << "  Max Timestamp: " << reader.MaxTimestamp() << std::endl;
  std::cout << "  Column ID: " << reader.ColumnId() << std::endl;
  std::cout << "  Entity Type: " << static_cast<int>(reader.GetEntityType()) << std::endl;
  
  // Try to get a specific entry
  cedar::CedarKey key(0, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000));
  cedar::Descriptor desc;
  
  // Note: SstReader doesn't have Get method, it's in ZoneColumnarSstReader
  // Let's just verify metadata is read correctly
  
  std::cout << std::endl << "✅ SST file is valid and readable!" << std::endl;
  return 0;
}
