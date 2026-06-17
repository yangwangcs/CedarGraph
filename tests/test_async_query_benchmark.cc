// Async Query Pool Performance Benchmark (Simplified)
// Tests the performance of the optimized thread pool

#include <gtest/gtest.h>
#include <iostream>
#include <chrono>
#include <vector>
#include <atomic>
#include <thread>

#include "cedar/client/async_query.h"
#include "cedar/client/types.h"

using namespace cedar::client;

class AsyncQueryBenchmark : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a simple executor that simulates query execution
    executor_ = [](const std::string& query, const std::string& space) -> QueryResult {
      // Simulate some work
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      
      QueryResult result;
      result.success = true;
      result.execution_time_ms = 10;
      return result;
    };
  }

  std::function<QueryResult(const std::string&, const std::string&)> executor_;
};

TEST_F(AsyncQueryBenchmark, ThreadPoolThroughput) {
  const int num_queries = 50;
  
  AsyncQueryPool pool(4);  // 4 worker threads
  
  auto start = std::chrono::high_resolution_clock::now();
  
  std::vector<std::shared_ptr<AsyncQuery>> queries;
  for (int i = 0; i < num_queries; ++i) {
    auto query = pool.Submit("MATCH (n) RETURN n", "default", executor_);
    queries.push_back(query);
  }
  
  // Wait for all queries to complete
  for (auto& query : queries) {
    query->Wait();
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  
  std::cout << "=== Thread Pool Performance ===" << std::endl;
  std::cout << "Queries: " << num_queries << std::endl;
  std::cout << "Duration: " << duration << " ms" << std::endl;
  std::cout << "Average: " << (double)duration / num_queries << " ms/query" << std::endl;
  std::cout << "Throughput: " << (num_queries * 1000.0 / duration) << " queries/sec" << std::endl;
  
  EXPECT_GT(num_queries * 1000.0 / duration, 100);  // At least 100 queries/sec
}

TEST_F(AsyncQueryBenchmark, QueryLatency) {
  const int num_queries = 20;
  
  AsyncQueryPool pool(4);
  
  std::vector<int64_t> latencies;
  
  for (int i = 0; i < num_queries; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto query = pool.Submit("MATCH (n) RETURN n", "default", executor_);
    auto result = query->Wait();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    latencies.push_back(latency);
  }
  
  // Calculate statistics
  int64_t total = 0;
  int64_t min_latency = INT64_MAX;
  int64_t max_latency = 0;
  
  for (auto latency : latencies) {
    total += latency;
    min_latency = std::min(min_latency, latency);
    max_latency = std::max(max_latency, latency);
  }
  
  double avg_latency = (double)total / num_queries;
  
  std::cout << "=== Query Latency ===" << std::endl;
  std::cout << "Queries: " << num_queries << std::endl;
  std::cout << "Min: " << min_latency << " ms" << std::endl;
  std::cout << "Max: " << max_latency << " ms" << std::endl;
  std::cout << "Avg: " << avg_latency << " ms" << std::endl;
  
  EXPECT_LT(avg_latency, 100);  // Average latency should be less than 100ms
}

TEST_F(AsyncQueryBenchmark, CancellationPerformance) {
  const int num_queries = 20;
  
  AsyncQueryPool pool(4);
  
  auto start = std::chrono::high_resolution_clock::now();
  
  // Submit queries
  std::vector<std::shared_ptr<AsyncQuery>> queries;
  for (int i = 0; i < num_queries; ++i) {
    auto query = pool.Submit("MATCH (n) RETURN n", "default", executor_);
    queries.push_back(query);
  }
  
  // Cancel half of the queries
  for (int i = 0; i < num_queries / 2; ++i) {
    queries[i]->Cancel();
  }
  
  // Wait for remaining queries
  for (auto& query : queries) {
    if (!query->IsCancelled()) {
      query->Wait();
    }
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  
  int cancelled = 0;
  int completed = 0;
  for (auto& query : queries) {
    if (query->IsCancelled()) cancelled++;
    if (query->IsCompleted()) completed++;
  }
  
  std::cout << "=== Cancellation Performance ===" << std::endl;
  std::cout << "Queries: " << num_queries << std::endl;
  std::cout << "Cancelled: " << cancelled << std::endl;
  std::cout << "Completed: " << completed << std::endl;
  std::cout << "Duration: " << duration << " ms" << std::endl;
  
  EXPECT_EQ(cancelled, num_queries / 2);
  EXPECT_EQ(completed, num_queries / 2);
}
