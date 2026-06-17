// Verify leveled compaction with real data import
// Writes enough data to trigger L0→L1, then verifies all data is readable
#include <iostream>
#include <thread>
#include <chrono>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/sst/zone_columnar_reader.h"

int main() {
  std::cout << "=== Leveled Compaction Verification ===" << std::endl;

  std::string db_path = "/tmp/test_leveled_verify";
  std::filesystem::remove_all(db_path);
  std::filesystem::create_directories(db_path);

  cedar::CedarOptions options;
  options.create_if_missing = true;
  options.memtable_threshold = 4096;       // 4KB memtable → many flushes
  options.enable_wal = true;
  options.enable_accumulated_flush = false;
  options.size_tiered_config.l0_max_size = 8192;  // 8KB L0 → quick L0→L1
  options.size_tiered_config.size_ratio = 2.0;
  options.size_tiered_config.max_levels = 5;

  cedar::CedarGraphStorage* db = nullptr;
  cedar::Status s = cedar::CedarGraphStorage::Open(options, db_path, &db);
  if (!s.ok()) { std::cerr << "Open failed: " << s.ToString() << std::endl; return 1; }

  // Phase 1: Write 2000 entries
  const int N = 2000;
  std::cout << "\n[Phase 1] Writing " << N << " entries..." << std::endl;
  for (int i = 0; i < N; ++i) {
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, i);
    s = db->Put(i, 1000000 + i, desc, cedar::Timestamp(1));
    if (!s.ok()) std::cerr << "  Put(" << i << ") failed" << std::endl;
  }
  std::cout << "  Written." << std::endl;

  // Force flush + wait for compaction
  s = db->ForceFlush();
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // List SST files
  std::cout << "\n[Phase 2] SST files on disk:" << std::endl;
  int file_count = 0;
  uint64_t total_size = 0;
  for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
    if (entry.path().extension() == ".sst") {
      file_count++;
      total_size += entry.file_size();
      cedar::ZoneColumnarSstReader reader(entry.path().string());
      if (reader.Open().ok()) {
        std::cout << "  " << entry.path().filename().string()
                  << "  " << entry.file_size() << " bytes"
                  << "  " << reader.NumEntries() << " entries" << std::endl;
      }
    }
  }
  std::cout << "  Total: " << file_count << " files, " << total_size << " bytes" << std::endl;

  // Phase 3: Verify all data readable
  std::cout << "\n[Phase 3] Verifying " << N << " entries..." << std::endl;
  int found = 0, missing = 0;
  for (int i = 0; i < N; ++i) {
    auto r = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
    if (r.has_value()) found++; else { missing++; if (missing <= 5) std::cerr << "  Missing: " << i << std::endl; }
  }
  std::cout << "  Found: " << found << "/" << N << " (missing=" << missing << ")" << std::endl;
  if (missing > 0) { std::cout << "\n❌ PRE-CLOSE VERIFICATION FAILED\n"; delete db; return 1; }

  // Phase 4: Close, reopen, verify persistence
  std::cout << "\n[Phase 4] Close → Reopen → Verify..." << std::endl;
  delete db;
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  s = cedar::CedarGraphStorage::Open(options, db_path, &db);
  if (!s.ok()) { std::cerr << "Reopen failed: " << s.ToString() << std::endl; return 1; }

  // List SST files after reopen
  file_count = 0; total_size = 0;
  for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
    if (entry.path().extension() == ".sst") {
      file_count++;
      total_size += entry.file_size();
    }
  }
  std::cout << "  SST files after reopen: " << file_count << " (" << total_size << " bytes)" << std::endl;

  found = 0; missing = 0;
  for (int i = 0; i < N; ++i) {
    auto r = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
    if (r.has_value()) found++; else { missing++; if (missing <= 5) std::cerr << "  Missing after reopen: " << i << std::endl; }
  }
  std::cout << "  Found after reopen: " << found << "/" << N << " (missing=" << missing << ")" << std::endl;
  if (missing > 0) { std::cout << "\n❌ POST-REOPEN VERIFICATION FAILED\n"; delete db; return 1; }

  // Phase 5: Write more data on top
  std::cout << "\n[Phase 5] Append " << N << " more entries..." << std::endl;
  for (int i = N; i < N * 2; ++i) {
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, i);
    s = db->Put(i, 1000000 + i, desc, cedar::Timestamp(1));
  }
  s = db->ForceFlush();
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Final verification
  found = 0; missing = 0;
  for (int i = 0; i < N * 2; ++i) {
    auto r = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
    if (r.has_value()) found++; else { missing++; if (missing <= 5) std::cerr << "  Missing final: " << i << std::endl; }
  }
  std::cout << "  Final: " << found << "/" << (N * 2) << " (missing=" << missing << ")" << std::endl;

  delete db;

  if (missing == 0) {
    std::cout << "\n✅✅✅ LEVELED COMPACTION VERIFICATION PASSED ✅✅✅" << std::endl;
  } else {
    std::cout << "\n❌❌❌ LEVELED COMPACTION VERIFICATION FAILED ❌❌❌" << std::endl;
  }
  std::cout << "  Data: " << db_path << std::endl;
  return missing > 0 ? 1 : 0;
}
