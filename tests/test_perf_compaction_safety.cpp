// Performance test: multi-threaded write/read throughput (multi-replica scenario)
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

double TestMTBatchWrite(CedarGraphStorage* db, int total, int batch_size, int num_threads) {
  std::atomic<uint64_t> ops{0};
  std::vector<std::thread> threads;
  int per_thread = total / num_threads;
  auto start = std::chrono::steady_clock::now();
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      int base = t * per_thread, written = 0;
      while (written < per_thread) {
        int count = std::min(batch_size, per_thread - written);
        std::vector<CedarGraphStorage::WriteBatchEntry> batch;
        batch.reserve(count);
        for (int i = 0; i < count; ++i) {
          int idx = base + written + i;
          batch.push_back({(uint64_t)idx, (uint64_t)(1000000 + idx),
                           Descriptor::InlineInt(1, idx % 1000), Timestamp(1)});
        }
        db->WriteBatch(batch);
        written += count;
        ops.fetch_add(count);
      }
    });
  }
  for (auto& t : threads) t.join();
  return ops.load() / std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
}

double TestMTRead(CedarGraphStorage* db, int total, int num_threads, int max_entity) {
  std::atomic<uint64_t> ops{0};
  std::vector<std::thread> threads;
  auto start = std::chrono::steady_clock::now();
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 rng(t * 1000 + 42);
      std::uniform_int_distribution<int> dist(0, max_entity - 1);
      int per_thread = total / num_threads;
      [[maybe_unused]] int found = 0;
      for (int i = 0; i < per_thread; ++i) {
        int key = dist(rng);
        auto r = db->Get(key, (uint64_t)(1000000 + key));
        if (r.has_value()) found++;
        ops.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  return ops.load() / std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
}

double TestMTMixedRW(CedarGraphStorage* db, int total, int num_threads, int max_entity) {
  std::atomic<uint64_t> ops{0};
  std::vector<std::thread> threads;
  auto start = std::chrono::steady_clock::now();
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 rng(t * 1000 + 42);
      std::uniform_int_distribution<int> op_dist(0, 99), key_dist(0, max_entity - 1);
      int per_thread = total / num_threads;
      [[maybe_unused]] int found = 0;
      std::vector<CedarGraphStorage::WriteBatchEntry> pending;
      for (int i = 0; i < per_thread; ++i) {
        int key = key_dist(rng);
        if (op_dist(rng) < 80) {
          auto r = db->Get(key, (uint64_t)(1000000 + key));
          if (r.has_value()) found++;
        } else {
          pending.push_back({(uint64_t)key, (uint64_t)(9000000 + i),
                             Descriptor::InlineInt(1, i), Timestamp(2)});
          if (pending.size() >= 50) { db->WriteBatch(pending); pending.clear(); }
        }
        ops.fetch_add(1);
      }
      if (!pending.empty()) db->WriteBatch(pending);
    });
  }
  for (auto& t : threads) t.join();
  return ops.load() / std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
}

double TestPauseResume(CedarGraphStorage* db, int n) {
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < n; ++i) { db->PauseCompaction(); db->ResumeCompaction(); }
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
    if (!db->Get(i, (uint64_t)(5000000 + i)).has_value()) return false;
  return true;
}

bool TestGCSafePoint() {
  SizeTieredConfig c; c.enable_background_compaction = false;
  std::string p = kDbPath + "/gc"; fs::remove_all(p); fs::create_directories(p);
  SizeTieredCompactionEngine e(p, c, Env::Default()); e.Open();
  bool ok = e.GetGCSafePoint() == 0; e.SetGCSafePoint(42); ok = ok && e.GetGCSafePoint() == 42;
  e.Close(); fs::remove_all(p); return ok;
}

int main() {
  std::cout << "=== Multi-Replica Compaction Safety: Performance ===" << std::endl;
  Cleanup();
  CedarOptions opts; opts.create_if_missing = true; opts.memtable_threshold = 64*1024*1024; opts.enable_wal = true;
  CedarGraphStorage* db = nullptr;
  CedarGraphStorage::Open(opts, kDbPath, &db);
  
  int nthreads = std::max(2u, std::thread::hardware_concurrency());
  std::cout << "  Threads: " << nthreads << std::endl;
  
  // Write (small dataset that stays in memtable)
  std::cout << std::endl << "[1] MT batch write (100K, batch=1000, " << nthreads << "T)..." << std::endl;
  double wps = TestMTBatchWrite(db, 100000, 1000, nthreads);
  std::cout << "  " << std::fixed << std::setprecision(0) << wps << " ops/sec" << std::endl;
  
  // Read hot data (all in memtable, 10K entries)
  std::cout << std::endl << "[2a] MT read HOT data (10K entries in memtable, " << nthreads << "T)..." << std::endl;
  double rps_hot = TestMTRead(db, 100000, nthreads, 10000);
  std::cout << "  " << std::fixed << std::setprecision(0) << rps_hot << " ops/sec" << std::endl;
  
  // Flush + compact to create multiple SST files
  db->ForceFlush();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  
  // Write more data and flush again to create multiple SST files
  for (int batch = 0; batch < 5; ++batch) {
    int base = 100000 + batch * 20000;
    for (int i = 0; i < 20000; i += 1000) {
      std::vector<CedarGraphStorage::WriteBatchEntry> b;
      b.reserve(1000);
      for (int j = 0; j < 1000 && i + j < 20000; ++j) {
        int idx = base + i + j;
        b.push_back({(uint64_t)idx, (uint64_t)(1000000 + idx),
                     Descriptor::InlineInt(1, idx % 1000), Timestamp(1)});
      }
      db->WriteBatch(b);
    }
    db->ForceFlush();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  std::cout << "  Created multiple SST files" << std::endl;
  
  // Read cold data (spread across multiple SST files)
  std::cout << std::endl << "[2b] MT read COLD data (multi-SST, " << nthreads << "T)..." << std::endl;
  double rps_cold = TestMTRead(db, 100000, nthreads, 200000);
  std::cout << "  " << std::fixed << std::setprecision(0) << rps_cold << " ops/sec" << std::endl;
  
  // Read cold data with NON-EXISTENT keys (bloom filter should help)
  std::cout << std::endl << "[2c] MT read COLD (non-existent keys, bloom filter helps, " << nthreads << "T)..." << std::endl;
  {
    std::atomic<uint64_t> ops{0};
    std::vector<std::thread> threads2;
    auto t2c_start = std::chrono::steady_clock::now();
    for (int t = 0; t < nthreads; ++t) {
      threads2.emplace_back([&, t]() {
        int per_thread = 100000 / nthreads;
        [[maybe_unused]] int found = 0;
        for (int i = 0; i < per_thread; ++i) {
          // Use entity_ids in range 500000-599999 (NOT in the database)
          int key = 500000 + (t * per_thread + i) % 100000;
          auto r = db->Get(key, (uint64_t)(1000000 + key));
          if (r.has_value()) found++;
          ops.fetch_add(1);
        }
      });
    }
    for (auto& t : threads2) t.join();
    auto t2c_end = std::chrono::steady_clock::now();
    double rps_miss = ops.load() / std::chrono::duration<double>(t2c_end - t2c_start).count();
    std::cout << "  " << std::fixed << std::setprecision(0) << rps_miss << " ops/sec" << std::endl;
  }
  
  // Mixed
  std::cout << std::endl << "[3] MT mixed R/W (80/20, 50K, " << nthreads << "T)..." << std::endl;
  double mixed = TestMTMixedRW(db, 50000, nthreads, 100000);
  std::cout << "  " << std::fixed << std::setprecision(0) << mixed << " ops/sec" << std::endl;
  
  // Pause/Resume
  std::cout << std::endl << "[4] Pause/Resume overhead..." << std::endl;
  double pr = TestPauseResume(db, 1000);
  std::cout << "  " << std::fixed << std::setprecision(2) << pr << " us/cycle" << std::endl;
  
  // Dedup
  std::cout << std::endl << "[5] Dedup correctness..." << std::endl;
  bool dedup = TestDedup(db);
  std::cout << "  " << (dedup ? "PASS" : "FAIL") << std::endl;
  
  // GC
  std::cout << std::endl << "[6] GC safe point..." << std::endl;
  bool gc = TestGCSafePoint();
  std::cout << "  " << (gc ? "PASS" : "FAIL") << std::endl;
  
  std::cout << std::endl << "==========================================" << std::endl;
  std::cout << "  Batch write (" << nthreads << "T): " << std::fixed << std::setprecision(0) << wps << " ops/sec" << std::endl;
  std::cout << "  Read HOT    (" << nthreads << "T): " << std::fixed << std::setprecision(0) << rps_hot << " ops/sec" << std::endl;
  std::cout << "  Read COLD   (" << nthreads << "T): " << std::fixed << std::setprecision(0) << rps_cold << " ops/sec" << std::endl;
  std::cout << "  Mixed R/W   (" << nthreads << "T): " << std::fixed << std::setprecision(0) << mixed << " ops/sec" << std::endl;
  std::cout << "  Pause/Resume:      " << std::fixed << std::setprecision(2) << pr << " us/cycle" << std::endl;
  std::cout << "  Dedup:             " << (dedup ? "PASS" : "FAIL") << std::endl;
  std::cout << "  GC safe point:     " << (gc ? "PASS" : "FAIL") << std::endl;
  std::cout << "==========================================" << std::endl;
  
  delete db;
  return (dedup && gc) ? 0 : 1;
}
