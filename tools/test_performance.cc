// tools/test_performance.cc
#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "query_service.grpc.pb.h"

int main(int argc, char* argv[]) {
  std::string graphd_addr = (argc > 1) ? argv[1] : "127.0.0.1:9669";
  int num_threads = (argc > 2) ? std::stoi(argv[2]) : 4;
  int queries_per_thread = (argc > 3) ? std::stoi(argv[3]) : 1000;
  
  auto channel = grpc::CreateChannel(graphd_addr, grpc::InsecureChannelCredentials());
  
  // 等待连接
  if (!channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(5))) {
    std::cerr << "Failed to connect to " << graphd_addr << std::endl;
    return 1;
  }
  
  std::atomic<uint64_t> total_queries{0};
  std::atomic<uint64_t> cache_hits{0};
  std::atomic<uint64_t> total_latency_us{0};
  
  auto start = std::chrono::steady_clock::now();
  
  std::vector<std::thread> threads;
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      auto stub = cedar::query::QueryService::NewStub(channel);
      
      for (int i = 0; i < queries_per_thread; ++i) {
        cedar::query::ExecuteQueryRequest request;
        // 50% 重复查询以测试缓存
        int entity_id = (i % 2 == 0) ? 12345 : (t * 10000 + i);
        request.set_query("MATCH (n) WHERE id(n) = " + std::to_string(entity_id) + " RETURN n");
        
        cedar::query::ExecuteQueryResponse response;
        grpc::ClientContext context;
        
        auto qstart = std::chrono::steady_clock::now();
        auto status = stub->ExecuteQuery(&context, request, &response);
        auto qend = std::chrono::steady_clock::now();
        
        if (status.ok() && response.success()) {
          total_queries++;
          if (response.stats().plan_from_cache()) {
            cache_hits++;
          }
          total_latency_us += std::chrono::duration_cast<std::chrono::microseconds>(qend - qstart).count();
        }
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  auto end = std::chrono::steady_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  
  uint64_t total = total_queries.load();
  uint64_t hits = cache_hits.load();
  
  std::cout << "\n========== Performance Test Results ==========" << std::endl;
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Threads: " << num_threads << std::endl;
  std::cout << "  Queries per thread: " << queries_per_thread << std::endl;
  std::cout << "  Total queries: " << total << std::endl;
  std::cout << std::endl;
  std::cout << "Results:" << std::endl;
  std::cout << "  Duration: " << duration_ms << " ms" << std::endl;
  std::cout << "  Throughput: " << (total * 1000 / duration_ms) << " queries/sec" << std::endl;
  std::cout << "  Avg latency: " << (total > 0 ? total_latency_us.load() / total : 0) << " us" << std::endl;
  std::cout << "  Cache hits: " << hits << " (" << (total > 0 ? hits * 100 / total : 0) << "%)" << std::endl;
  std::cout << "==============================================" << std::endl;
  
  return 0;
}
