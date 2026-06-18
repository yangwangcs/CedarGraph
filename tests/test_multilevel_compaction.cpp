// Test multi-level compaction: L0 → L1 → L2
// Uses small thresholds to trigger level-by-level compaction
#include <iostream>
#include <thread>
#include <chrono>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/sst/zone_columnar_reader.h"

int main() {
  std::cout << "=== Multi-Level Compaction Test (L0→L1→L2) ===" << std::endl;
  
  std::string db_path = (std::filesystem::temp_directory_path() / "test_multilevel_compaction").string();
  std::filesystem::remove_all(db_path);
  std::filesystem::create_directories(db_path);
  
  cedar::CedarOptions options;
  options.create_if_missing = true;
  options.memtable_threshold = 512;        // Very small memtable: 512 bytes
  options.enable_wal = true;
  options.enable_accumulated_flush = false;
  
  cedar::CedarGraphStorage* db = nullptr;
  cedar::Status s = cedar::CedarGraphStorage::Open(options, db_path, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
    return 1;
  }
  
  // Write 500 entries in batches to trigger multiple flushes and compactions
  const int TOTAL_ENTRIES = 500;
  std::cout << std::endl << "[Phase 1] Writing " << TOTAL_ENTRIES << " entries..." << std::endl;
  
  for (int i = 0; i < TOTAL_ENTRIES; ++i) {
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, i * 10);
    s = db->Put(i, 1000000 + i, desc, cedar::Timestamp(1));
    if (!s.ok()) {
      std::cerr << "Put failed at i=" << i << ": " << s.ToString() << std::endl;
    }
  }
  std::cout << "  Written " << TOTAL_ENTRIES << " entries" << std::endl;
  
  // Wait for compaction to settle
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  // Force flush to push all data to SST
  s = db->ForceFlush();
  if (!s.ok()) {
    std::cerr << "  ForceFlush failed: " << s.ToString() << std::endl;
  }
  
  // Wait for compaction again
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  // List SST files
  std::cout << std::endl << "[Phase 2] SST files on disk:" << std::endl;
  int sst_count = 0;
  uint64_t total_sst_size = 0;
  for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
    if (entry.path().extension() == ".sst") {
      sst_count++;
      total_sst_size += entry.file_size();
      cedar::ZoneColumnarSstReader reader(entry.path().string());
      auto open_status = reader.Open();
      if (open_status.ok()) {
        std::cout << "  " << entry.path().filename().string() 
                  << " (" << entry.file_size() << " bytes, " 
                  << reader.NumEntries() << " entries)" << std::endl;
      }
    }
  }
  std::cout << "  Total: " << sst_count << " SST files, " 
            << total_sst_size << " bytes" << std::endl;
  
  // Verify all data is readable
  std::cout << std::endl << "[Phase 3] Verifying all data..." << std::endl;
  int found = 0;
  int missing = 0;
  for (int i = 0; i < TOTAL_ENTRIES; ++i) {
    auto result = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
    if (result.has_value()) {
      found++;
    } else {
      missing++;
      if (missing <= 5) {
        std::cerr << "  Missing entry " << i << std::endl;
      }
    }
  }
  std::cout << "  Found: " << found << "/" << TOTAL_ENTRIES 
            << " (missing: " << missing << ")" << std::endl;
  
  // Close and reopen to test persistence
  std::cout << std::endl << "[Phase 4] Close and reopen..." << std::endl;
  delete db;
  
  // List SST files after close
  sst_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
    if (entry.path().extension() == ".sst") {
      sst_count++;
    }
  }
  std::cout << "  SST files after close: " << sst_count << std::endl;
  
  s = cedar::CedarGraphStorage::Open(options, db_path, &db);
  if (!s.ok()) {
    std::cerr << "Failed to reopen DB: " << s.ToString() << std::endl;
    return 1;
  }
  
  int found_after_reopen = 0;
  int missing_after_reopen = 0;
  for (int i = 0; i < TOTAL_ENTRIES; ++i) {
    auto result = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
    if (result.has_value()) {
      found_after_reopen++;
    } else {
      missing_after_reopen++;
      if (missing_after_reopen <= 5) {
        std::cerr << "  Missing entry " << i << " after reopen" << std::endl;
      }
    }
  }
  std::cout << "  Found after reopen: " << found_after_reopen << "/" << TOTAL_ENTRIES 
            << " (missing: " << missing_after_reopen << ")" << std::endl;
  
  delete db;
  
  // Final verdict
  std::cout << std::endl;
  if (found == TOTAL_ENTRIES && found_after_reopen == TOTAL_ENTRIES) {
    std::cout << "✅✅✅ MULTI-LEVEL COMPACTION TEST PASSED! ✅✅✅" << std::endl;
  } else {
    std::cout << "❌❌❌ MULTI-LEVEL COMPACTION TEST FAILED! ❌❌❌" << std::endl;
  }
  
  std::cout << "  Data kept at: " << db_path << std::endl;
  return 0;
}
