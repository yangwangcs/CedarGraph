// Performance test: write/read throughput with batch operations (multi-replica scenario)
#include <iostream>
#include <chrono>
#include <filesystem>
#include <vector>
#include <atomic>
#include <thread>
#include <random>
#include <iomanip>
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

double TestBatchWrite(CedarGraphStorage* db, int total, int batch_size) {
  auto start = std::chrono::steady_clock::now();
  int written = 0;
  while (written < total) {
    int count = std::min(batch_size, total - written);
    std::vector<CedarGraphStorage::WriteBatchEntry> batch;
    batch.reserve(count);
    for (int i = 0; i < count; ++i) {
      int idx = written + i;
      batch.push_back({(uint64_t)idx, (uint64_t)(1000000 + idx),
                       Descriptor::InlineInt(1, idx % 1000), Timestamp(1)});
    }
    db->WriteBatch(batch);
    written += count;
  }
  auto end = std::chrono::steady_clock::now();
  return written / std::chrono::duration<double>(end - start).count();
}

double TestReadThroughput(CedarGraphStorage* db, int num_reads, int max_entity) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> dist(0, max_entity - 1);
  volatile int found = 0;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < num_reads; ++i) {
    int key = dist(rng);
    auto result = db->Get(key, 1000000 + key);
    if (result.has_value()) found++;
  }
  auto end = std::chrono::steady_clock::now();
  (void)found;
  return num_reads / std::chrono::duration<double>(end - start).count();
}

double TestPauseResume(CedarGraphStorage* db, int n) {
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < n; ++i) {
    db->PauseCompaction();
    db->ResumeCompaction();
  }
  return std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count() / n;
}

bool TestDedup(CedarGraphStorage* db) {
  std::vector<CedarGraphStorage::WriteBatchEntry> batch;
  for (int i = 0; i < 100; ++i)
    batch.push_back({(uint64_t)i, (uint64_t)(5000000 + i), Descriptor::InlineInt(1, i), Timestamp(1)});
  db->WriteBatch(batch);
  db->WriteBatch(batch);
  db->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  db->Compact();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  for (int i = 0; i < 100; ++i)
    if (!db->Get(i, 5000000 + i).has_value()) return false;
  return true;
}

bool TestGCSafePoint() {
  SizeTieredConfig config; config.enable_background_compaction = false;
  std::string path = kDbPath + "/gc";
  fs::remove_all(path); fs::create_directories(path);
  SizeTieredCompactionEngine engine(path, config, Env::Default());
  engine.Open();
  bool ok = engine.GetGCSafePoint() == 0;
  engine.SetGCSafePoint(42);
  ok = ok && engine.GetGCSafePoint() == 42;
  engine.Close(); fs::remove_all(path);
  return ok;
}

double TestConcurrentSnapshot(CedarGraphStorage* db, int batch_size) {
  std::atomic<bool> stop{false};
  std::atomic<int> total{0};
  std::thread writer([&]() {
    while (!stop.load()) {
      std::vector<CedarGraphStorage::WriteBatchEntry> b;
      b.reserve(batch_size);
      for (int i = 0; i < batch_size; ++i) {
        int idx = total.load() + i;
        b.push_back({(uint64_t)(200000 + idx), (uint64_t)(8000000 + idx),
                     Descriptor::InlineInt(1, idx), Timestamp(2)});
      }
      db->WriteBatch(b);
      total.fetch_add(batch_size);
    }
  });
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 5; ++i) {
    db->PauseCompaction();
    db->ForceFlush();
    db->ResumeCompaction();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
  stop.store(true); writer.join();
  return elapsed;
}

double TestMixedRW(CedarGraphStorage* db, int num_ops, int max_entity) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> op_dist(0, 99), key_dist(0, max_entity - 1);
  volatile int found = 0;
  auto start = std::chrono::steady_clock::now();
  std::vector<CedarGraphStorage::WriteBatchEntry> pending;
  for (int i = 0; i < num_ops; ++i) {
    int key = key_dist(rng);
    if (op_dist(rng) < 80) {
      auto r = db->Get(key, 1000000 + key);
      if (r.has_value()) found++;
    } else {
      pending.push_back({(uint64_t)key, (uint64_t)(9000000 + i),
                         Descriptor::InlineInt(1, i), Timestamp(2)});
      if (pending.size() >= 100) { db->WriteBatch(pending); pending.clear(); }
    }
  }
  if (!pending.empty()) db->WriteBatch(pending);
  (void)found;
  return num_ops / std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
}

int main() {
  std::cout << "=== Multi-Replica Compaction Safety: Write/Read Performance ===" << std::endl;
  Cleanup();
  CedarOptions opts; opts.create_if_missing = true; opts.memtable_threshold = 64*1024*1024; opts.enable_wal = true;
  CedarGraphStorage* db = nullptr;
  CedarGraphStorage::Open(opts, kDbPath, &db);
  
  std::cout << std::endl << "[1] Batch write (100K, batch=1000)..." << std::endl;
  double wps = TestBatchWrite(db, 100000, 1000);
  std::cout << "  " << std::fixed << std::setprecision(0) << wps << " ops/sec" << std::endl;
  
  db->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  
  std::cout << std::endl << "[2] Point read (100K)..." << std::endl;
  double rps = TestReadThroughput(db, 100000, 100000);
  std::cout << "  " << std::fixed << std::setprecision(0) << rps << " ops/sec" << std::endl;
  
  // Test 2b: BatchGet throughput (100K, batch=1000)
  std::cout << std::endl << "[2b] BatchGet throughput (100K, batch=1000)..." << std::endl;
  {
    std::mt19937 rng2(42);
    std::uniform_int_distribution<int> dist2(0, 99999);
    volatile int found2 = 0;
    auto t2b_start = std::chrono::steady_clock::now();
    int queried = 0;
    while (queried < 100000) {
      int count = std::min(1000, 100000 - queried);
      std::vector<CedarGraphStorage::BatchQueryItem> items;
      items.reserve(count);
      for (int i = 0; i < count; ++i) {
        items.emplace_back(dist2(rng2), EntityType::Vertex, 0, Timestamp(1000000 + queried + i));
      }
      db->BatchGet(items);
      for (const auto& item : items)
        if (item.result.has_value()) found2++;
      queried += count;
    }
    auto t2b_end = std::chrono::steady_clock::now();
    double t2b_elapsed = std::chrono::duration<double>(t2b_end - t2b_start).count();
    double batch_rps = 100000.0 / t2b_elapsed;
    std::cout << "  " << std::fixed << std::setprecision(0) << batch_rps << " ops/sec (found=" << found2 << ")" << std::endl;
  }
  
  std::cout << std::endl << "[3] Pause/Resume overhead..." << std::endl;
  double pr = TestPauseResume(db, 1000);
  std::cout << "  " << std::fixed << std::setprecision(2) << pr << " us/cycle" << std::endl;
  
  std::cout << std::endl << "[4] Dedup correctness..." << std::endl;
  bool dedup = TestDedup(db);
  std::cout << "  " << (dedup ? "PASS" : "FAIL") << std::endl;
  
  std::cout << std::endl << "[5] GC safe point..." << std::endl;
  bool gc = TestGCSafePoint();
  std::cout << "  " << (gc ? "PASS" : "FAIL") << std::endl;
  
  std::cout << std::endl << "[6] Concurrent batch write + 5 snapshots..." << std::endl;
  double snap = TestConcurrentSnapshot(db, 500);
  std::cout << "  " << std::fixed << std::setprecision(1) << snap << " ms" << std::endl;
  
  std::cout << std::endl << "[7] Mixed R/W (80/20, 50K ops)..." << std::endl;
  double mixed = TestMixedRW(db, 50000, 100000);
  std::cout << "  " << std::fixed << std::setprecision(0) << mixed << " ops/sec" << std::endl;
  
  std::cout << std::endl << "==========================================" << std::endl;
  std::cout << "  Batch write (100K): " << std::fixed << std::setprecision(0) << wps << " ops/sec" << std::endl;
  std::cout << "  Point read (100K):  " << std::fixed << std::setprecision(0) << rps << " ops/sec" << std::endl;
  std::cout << "  Mixed R/W (80/20):  " << std::fixed << std::setprecision(0) << mixed << " ops/sec" << std::endl;
  std::cout << "  Pause/Resume:       " << std::fixed << std::setprecision(2) << pr << " us/cycle" << std::endl;
  std::cout << "  Dedup:              " << (dedup ? "PASS" : "FAIL") << std::endl;
  std::cout << "  GC safe point:      " << (gc ? "PASS" : "FAIL") << std::endl;
  std::cout << "==========================================" << std::endl;
  
  delete db;
  return (dedup && gc) ? 0 : 1;
}
