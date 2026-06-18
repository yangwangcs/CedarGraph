// Test flush and persistence
#include <iostream>
#include <thread>
#include <chrono>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"

int main() {
  std::cout << "=== Testing ForceFlush and Persistence ===" << std::endl;
  
  std::string db_path = (std::filesystem::temp_directory_path() / "test_flush_db").string();
  
  // Clean up
  std::filesystem::remove_all(db_path);
  std::filesystem::create_directories(db_path);
  
  // Phase 1: Open and write
  std::cout << std::endl << "[Phase 1] Opening DB and writing data..." << std::endl;
  
  cedar::CedarOptions options;
  options.create_if_missing = true;
  options.memtable_threshold = 1024;  // 1KB for quick flush
  options.enable_wal = true;
  options.enable_accumulated_flush = false;  // Disable accumulated flush
  
  cedar::CedarGraphStorage* db = nullptr;
  cedar::Status s = cedar::CedarGraphStorage::Open(options, db_path, &db);
  
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
    return 1;
  }
  
  // Write some data
  for (int i = 0; i < 100; ++i) {
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, i * 100);
    s = db->Put(i, 1000000 + i, desc, cedar::Timestamp(1));
    if (!s.ok()) {
      std::cerr << "Put failed: " << s.ToString() << std::endl;
    }
  }
  std::cout << "  Written 100 entries" << std::endl;
  
  // Wait for background compaction to settle
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  // Check SST files before flush
  int sst_count_before = 0;
  for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
    if (entry.path().extension() == ".sst") {
      sst_count_before++;
    }
  }
  std::cout << "  SST files before flush: " << sst_count_before << std::endl;
  
  // Force flush
  std::cout << std::endl << "[Phase 2] ForceFlush..." << std::endl;
  s = db->ForceFlush();
  if (!s.ok()) {
    std::cerr << "  ForceFlush failed: " << s.ToString() << std::endl;
  } else {
    std::cout << "  ForceFlush OK" << std::endl;
  }
  
  // Wait for background compaction to settle
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  // Check SST files after flush
  int sst_count_after = 0;
  for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
    if (entry.path().extension() == ".sst") {
      sst_count_after++;
      std::cout << "  SST file: " << entry.path().filename() << " (" 
                << entry.file_size() << " bytes)" << std::endl;
    }
  }
  std::cout << "  SST files after flush: " << sst_count_after << std::endl;
  
  // Read back
  std::cout << std::endl << "[Phase 3] Reading back..." << std::endl;
  int found = 0;
  for (int i = 0; i < 100; ++i) {
    auto result = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
    if (result.has_value()) {
      found++;
    }
  }
  std::cout << "  Found: " << found << "/100" << std::endl;
  
  // Close DB
  delete db;
  
  // Phase 4: Reopen and verify
  if (sst_count_after > 0) {
    std::cout << std::endl << "[Phase 4] Reopening DB..." << std::endl;
    
    s = cedar::CedarGraphStorage::Open(options, db_path, &db);
    if (!s.ok()) {
      std::cerr << "Failed to reopen DB: " << s.ToString() << std::endl;
      return 1;
    }
    
    int found_after_reopen = 0;
    for (int i = 0; i < 100; ++i) {
      auto result = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
      if (result.has_value()) {
        found_after_reopen++;
      }
    }
    std::cout << "  Found after reopen: " << found_after_reopen << "/100" << std::endl;
    
    if (found_after_reopen == 100) {
      std::cout << std::endl << "✅✅✅ PERSISTENCE TEST PASSED! ✅✅✅" << std::endl;
    } else {
      std::cout << std::endl << "❌❌❌ PERSISTENCE TEST FAILED! ❌❌❌" << std::endl;
    }
    
    delete db;
  } else {
    std::cout << std::endl << "❌ No SST files created!" << std::endl;
  }
  
  // Don't cleanup for debugging
  // std::filesystem::remove_all(db_path);
  std::cout << "  Data kept at: " << db_path << std::endl;
  
  return 0;
}
