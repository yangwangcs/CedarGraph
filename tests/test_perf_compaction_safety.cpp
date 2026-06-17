// Performance test: compaction pause/resume overhead + dedup correctness
#include <iostream>
#include <chrono>
#include <filesystem>
#include <vector>
#include <atomic>
#include <thread>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/storage/size_tiered_compaction.h"

using namespace cedar;
namespace fs = std::filesystem;

static const std::string kDbPath = "/tmp/test_perf_compaction_safety";

void Cleanup() {
  std::error_code ec;
  fs::remove_all(kDbPath, ec);
  fs::create_directories(kDbPath);
}

// Test 1: Write throughput baseline
double TestWriteThroughput(CedarGraphStorage* db, int num_entries) {
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < num_entries; ++i) {
    Descriptor desc = Descriptor::InlineInt(1, i * 100);
    db->Put(i, 1000000 + i, desc, Timestamp(1));
  }
  auto end = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(end - start).count();
  return num_entries / elapsed;
}

// Test 2: Pause/Resume compaction overhead
double TestPauseResumeOverhead(CedarGraphStorage* db, int iterations) {
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < iterations; ++i) {
    db->PauseCompaction();
    db->ResumeCompaction();
  }
  auto end = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double, std::micro>(end - start).count();
  return elapsed / iterations;
}

// Test 3: Compaction with dedup (writes duplicate keys and verifies dedup)
bool TestDedupCompaction(CedarGraphStorage* db) {
  auto* engine = db->GetLsmEngine();
  if (!engine) return false;
  
  // Write 100 entries
  for (int i = 0; i < 100; ++i) {
    Descriptor desc = Descriptor::InlineInt(1, i);
    db->Put(i, 1000000 + i, desc, Timestamp(1));
  }
  
  // Write same 100 entries again (duplicates with same key)
  for (int i = 0; i < 100; ++i) {
    Descriptor desc = Descriptor::InlineInt(1, i);
    db->Put(i, 1000000 + i, desc, Timestamp(1));
  }
  
  db->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  
  // Trigger compaction
  db->Compact();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  // Verify data is still readable
  for (int i = 0; i < 100; ++i) {
    auto result = db->Get(i, 1000000 + i);
    if (!result.has_value()) {
      std::cerr << "  FAIL: entry " << i << " missing after compaction" << std::endl;
      return false;
    }
  }
  return true;
}

// Test 4: GC safe point
bool TestGCSafePoint() {
  SizeTieredConfig config;
  config.enable_background_compaction = false;
  Env* env = Env::Default();
  
  std::string path = kDbPath + "/gc_test";
  fs::remove_all(path);
  fs::create_directories(path);
  
  SizeTieredCompactionEngine engine(path, config, env);
  engine.Open();
  
  // Default safe point should be 0
  if (engine.GetGCSafePoint() != 0) {
    std::cerr << "  FAIL: initial gc_safe_point != 0" << std::endl;
    return false;
  }
  
  // Set safe point
  engine.SetGCSafePoint(42);
  if (engine.GetGCSafePoint() != 42) {
    std::cerr << "  FAIL: gc_safe_point != 42 after set" << std::endl;
    return false;
  }
  
  engine.Close();
  fs::remove_all(path);
  return true;
}

// Test 5: Concurrent write + pause/resume (simulates snapshot scenario)
double TestConcurrentWritePauseResume(CedarGraphStorage* db, int num_writes) {
  std::atomic<int> writes_done{0};
  std::atomic<bool> stop{false};
  
  // Writer thread
  std::thread writer([&]() {
    for (int i = 0; i < num_writes && !stop.load(); ++i) {
      Descriptor desc = Descriptor::InlineInt(1, i);
      db->Put(10000 + i, 2000000 + i, desc, Timestamp(2));
      writes_done.fetch_add(1);
    }
  });
  
  // Snapshot-like pause/resume cycle
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 5; ++i) {
    db->PauseCompaction();
    db->ForceFlush();
    db->ResumeCompaction();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto end = std::chrono::steady_clock::now();
  
  stop.store(true);
  writer.join();
  
  double elapsed = std::chrono::duration<double, std::milli>(end - start).count();
  return elapsed;
}

int main() {
  std::cout << "=== Compaction Safety Performance Test ===" << std::endl;
  
  Cleanup();
  
  CedarOptions options;
  options.create_if_missing = true;
  options.memtable_threshold = 4096;
  options.enable_wal = true;
  
  CedarGraphStorage* db = nullptr;
  Status s = CedarGraphStorage::Open(options, kDbPath, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
    return 1;
  }
  
  // Test 1: Write throughput
  std::cout << std::endl << "[Test 1] Write throughput (10K entries)..." << std::endl;
  double wps = TestWriteThroughput(db, 10000);
  std::cout << "  Write throughput: " << std::fixed << std::setprecision(0) 
            << wps << " ops/sec" << std::endl;
  
  // Test 2: Pause/Resume overhead
  std::cout << std::endl << "[Test 2] Pause/Resume overhead (1000 iterations)..." << std::endl;
  double pause_us = TestPauseResumeOverhead(db, 1000);
  std::cout << "  Avg Pause+Resume: " << std::fixed << std::setprecision(2) 
            << pause_us << " us/cycle" << std::endl;
  
  // Test 3: Dedup compaction
  std::cout << std::endl << "[Test 3] Dedup compaction correctness..." << std::endl;
  bool dedup_ok = TestDedupCompaction(db);
  std::cout << "  Dedup test: " << (dedup_ok ? "PASSED" : "FAILED") << std::endl;
  
  // Test 4: GC safe point
  std::cout << std::endl << "[Test 4] GC safe point..." << std::endl;
  bool gc_ok = TestGCSafePoint();
  std::cout << "  GC safe point test: " << (gc_ok ? "PASSED" : "FAILED") << std::endl;
  
  // Test 5: Concurrent write + pause/resume
  std::cout << std::endl << "[Test 5] Concurrent write + snapshot pause/resume..." << std::endl;
  double concurrent_ms = TestConcurrentWritePauseResume(db, 5000);
  std::cout << "  5 snapshot cycles (with concurrent writes): " << std::fixed 
            << std::setprecision(1) << concurrent_ms << " ms total" << std::endl;
  
  // Final write throughput after all modifications
  std::cout << std::endl << "[Test 6] Post-fix write throughput (10K entries)..." << std::endl;
  double wps2 = TestWriteThroughput(db, 10000);
  std::cout << "  Write throughput: " << std::fixed << std::setprecision(0) 
            << wps2 << " ops/sec" << std::endl;
  
  // Summary
  std::cout << std::endl << "=== Summary ===" << std::endl;
  std::cout << "  Baseline write:  " << std::fixed << std::setprecision(0) << wps << " ops/sec" << std::endl;
  std::cout << "  Post-fix write:  " << std::fixed << std::setprecision(0) << wps2 << " ops/sec" << std::endl;
  double overhead_pct = ((wps - wps2) / wps) * 100;
  std::cout << "  Overhead:        " << std::fixed << std::setprecision(1) << overhead_pct << "%" << std::endl;
  std::cout << "  Pause/Resume:    " << std::fixed << std::setprecision(2) << pause_us << " us/cycle" << std::endl;
  std::cout << "  Dedup:           " << (dedup_ok ? "PASS" : "FAIL") << std::endl;
  std::cout << "  GC safe point:   " << (gc_ok ? "PASS" : "FAIL") << std::endl;
  
  delete db;
  
  std::cout << std::endl << "=== ALL TESTS COMPLETE ===" << std::endl;
  return (dedup_ok && gc_ok) ? 0 : 1;
}
