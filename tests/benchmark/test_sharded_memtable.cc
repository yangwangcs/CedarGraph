#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include "cedar/storage/vsl_memtable.h"

using namespace cedar;

void benchmark_sharded(int num_threads, int ops_per_thread) {
    VSLMemTable sharded;
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&sharded, t, ops_per_thread]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                uint64_t entity_id = static_cast<uint64_t>(t * ops_per_thread + i) % 10000;
                CedarKey key = CedarKey::Vertex(entity_id, 0, Timestamp(static_cast<uint64_t>(i)));
                auto desc = Descriptor::InlineInt(0, static_cast<int32_t>(i));
                sharded.Put(key, desc, Timestamp(1));
            }
        });
    }
    for (auto& t : threads) t.join();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    double ops_sec = static_cast<double>(num_threads * ops_per_thread) / (us / 1000000.0);
    std::cout << "VSLMemTable (16 shards): " << static_cast<int>(ops_sec) << " ops/sec"
              << " (" << us / 1000 << " ms)" << std::endl;
}

void benchmark_single(int num_threads, int ops_per_thread) {
    VSLMemTable single;
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&single, t, ops_per_thread]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                uint64_t entity_id = static_cast<uint64_t>(t * ops_per_thread + i) % 10000;
                CedarKey key = CedarKey::Vertex(entity_id, 0, Timestamp(static_cast<uint64_t>(i)));
                auto desc = Descriptor::InlineInt(0, static_cast<int32_t>(i));
                single.Put(key, desc, Timestamp(1));
            }
        });
    }
    for (auto& t : threads) t.join();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    double ops_sec = static_cast<double>(num_threads * ops_per_thread) / (us / 1000000.0);
    std::cout << "VSLMemTable (single lock):      " << static_cast<int>(ops_sec) << " ops/sec"
              << " (" << us / 1000 << " ms)" << std::endl;
}

int main() {
    std::cout << "=== Concurrent MemTable Write Benchmark ===" << std::endl;
    std::cout << std::endl;
    
    for (int num_threads : {1, 2, 4, 8, 16}) {
        int ops_per_thread = 100000 / num_threads;
        std::cout << "--- " << num_threads << " thread(s), " << (num_threads * ops_per_thread)
                  << " total ops ---" << std::endl;
        benchmark_sharded(num_threads, ops_per_thread);
        benchmark_single(num_threads, ops_per_thread);
        std::cout << std::endl;
    }
    
    return 0;
}
