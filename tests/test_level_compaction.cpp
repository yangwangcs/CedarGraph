// Test L0→L1→L2 level-by-level compaction
// Uses tiny thresholds to force level promotion
#include <iostream>
#include <thread>
#include <chrono>
#include <filesystem>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/sst/zone_columnar_reader.h"

static void PrintSstFiles(const std::string& db_path, const std::string& label) {
  std::cout << "  [" << label << "] SST files:" << std::endl;
  int count = 0;
  uint64_t total_size = 0;
  for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
    if (entry.path().extension() == ".sst") {
      count++;
      total_size += entry.file_size();
      cedar::ZoneColumnarSstReader reader(entry.path().string());
      if (reader.Open().ok()) {
        auto stats = reader.GetStats();
        std::cout << "    " << entry.path().filename().string()
                  << "  " << entry.file_size() << " bytes"
                  << "  " << reader.NumEntries() << " entries"
                  << "  " << stats.total_blocks << " blocks" << std::endl;
      }
    }
  }
  std::cout << "  Total: " << count << " files, " << total_size << " bytes" << std::endl;
}

int main() {
  std::cout << "=== L0→L1→L2 Level Compaction Test ===" << std::endl;

  std::string db_path = (std::filesystem::temp_directory_path() / "test_level_compaction").string();
  std::filesystem::remove_all(db_path);
  std::filesystem::create_directories(db_path);

  cedar::CedarOptions options;
  options.create_if_missing = true;
  options.memtable_threshold = 512;        // 512B → many small flushes
  options.enable_wal = true;
  options.enable_accumulated_flush = false;
  
  // 设置极小的 L0 阈值，强制触发 L0→L1→L2 推进
  options.size_tiered_config.l0_max_size = 4096;     // 4KB → 极小 L0
  options.size_tiered_config.l0_max_files = 2;        // 2 个文件就触发
  options.size_tiered_config.size_ratio = 2.0;        // 每层 2 倍
  options.size_tiered_config.max_levels = 5;          // L0-L4

  cedar::CedarGraphStorage* db = nullptr;
  cedar::Status s = cedar::CedarGraphStorage::Open(options, db_path, &db);
  if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << std::endl;
    return 1;
  }

  // Phase 1: Write enough data to trigger L0 overflow
  const int TOTAL = 1000;
  std::cout << std::endl << "[Phase 1] Writing " << TOTAL << " entries..." << std::endl;
  for (int i = 0; i < TOTAL; ++i) {
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, i);
    s = db->Put(i, 1000000 + i, desc, cedar::Timestamp(1));
    if (!s.ok()) {
      std::cerr << "  Put(" << i << ") failed: " << s.ToString() << std::endl;
    }
  }
  std::cout << "  Written." << std::endl;

  // Force flush all memtable data to SST
  s = db->ForceFlush();
  if (!s.ok()) std::cerr << "  ForceFlush: " << s.ToString() << std::endl;

  // Wait for compaction to fully settle
  std::this_thread::sleep_for(std::chrono::seconds(3));

  PrintSstFiles(db_path, "After compaction settle");

  // Phase 2: Verify all data readable before close
  std::cout << std::endl << "[Phase 2] Read-back before close..." << std::endl;
  {
    int found = 0, missing = 0;
    for (int i = 0; i < TOTAL; ++i) {
      auto r = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
      if (r.has_value()) found++; else { missing++; if (missing <= 3) std::cerr << "  Missing: " << i << std::endl; }
    }
    std::cout << "  Found: " << found << "/" << TOTAL << " (missing=" << missing << ")" << std::endl;
    if (missing > 0) { std::cout << "\n❌ PRE-CLOSE READ FAILED\n"; delete db; return 1; }
  }

  // Phase 3: Close and reopen (persistence test)
  std::cout << std::endl << "[Phase 3] Close → Reopen → Verify..." << std::endl;
  delete db;

  // Reopen
  s = cedar::CedarGraphStorage::Open(options, db_path, &db);
  if (!s.ok()) {
    std::cerr << "Reopen failed: " << s.ToString() << std::endl;
    return 1;
  }

  PrintSstFiles(db_path, "After reopen");

  {
    int found = 0, missing = 0;
    for (int i = 0; i < TOTAL; ++i) {
      auto r = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
      if (r.has_value()) found++; else { missing++; if (missing <= 3) std::cerr << "  Missing after reopen: " << i << std::endl; }
    }
    std::cout << "  Found after reopen: " << found << "/" << TOTAL << " (missing=" << missing << ")" << std::endl;
    if (missing > 0) { std::cout << "\n❌ POST-REOPEN READ FAILED\n"; delete db; return 1; }
  }

  // Phase 4: Write MORE data on top of existing, close, reopen
  std::cout << std::endl << "[Phase 4] Append 500 more entries on existing data..." << std::endl;
  for (int i = TOTAL; i < TOTAL + 500; ++i) {
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, i);
    s = db->Put(i, 1000000 + i, desc, cedar::Timestamp(1));
    if (!s.ok()) std::cerr << "  Put(" << i << ") failed: " << s.ToString() << std::endl;
  }

  s = db->ForceFlush();
  std::this_thread::sleep_for(std::chrono::seconds(3));

  PrintSstFiles(db_path, "After append + compaction");

  // Verify ALL data (old + new)
  {
    int found = 0, missing = 0;
    for (int i = 0; i < TOTAL + 500; ++i) {
      auto r = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
      if (r.has_value()) found++; else { missing++; if (missing <= 3) std::cerr << "  Missing append: " << i << std::endl; }
    }
    std::cout << "  Found: " << found << "/" << (TOTAL + 500) << " (missing=" << missing << ")" << std::endl;
    if (missing > 0) { std::cout << "\n❌ APPEND READ FAILED\n"; delete db; return 1; }
  }

  // Final close + reopen
  delete db;
  s = cedar::CedarGraphStorage::Open(options, db_path, &db);
  if (!s.ok()) { std::cerr << "Final reopen failed: " << s.ToString() << std::endl; return 1; }

  PrintSstFiles(db_path, "Final reopen");

  {
    int found = 0, missing = 0;
    for (int i = 0; i < TOTAL + 500; ++i) {
      auto r = db->Get(i, cedar::EntityType::Vertex, 1, cedar::Timestamp(1000000 + i));
      if (r.has_value()) found++; else { missing++; if (missing <= 3) std::cerr << "  Missing final: " << i << std::endl; }
    }
    std::cout << "  Found after final reopen: " << found << "/" << (TOTAL + 500) << " (missing=" << missing << ")" << std::endl;
    if (missing > 0) { std::cout << "\n❌ FINAL READ FAILED\n"; delete db; return 1; }
  }

  delete db;

  std::cout << std::endl << "✅✅✅ LEVEL COMPACTION TEST PASSED ✅✅✅" << std::endl;
  std::cout << "  Data: " << db_path << std::endl;
  return 0;
}
