// tools/benchmark_workloads.cc
// Temporal graph benchmark workload implementations

#include "benchmark_workloads.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <queue>
#include <unordered_set>

namespace cedar {
namespace benchmark {

// =============================================================================
// Basic Read/Write Workload
// =============================================================================

BasicReadWriteWorkload::BasicReadWriteWorkload(Config config, 
                                                CedarGraphStorage* storage)
    : config_(config), storage_(storage) {}

void BasicReadWriteWorkload::Run(BenchmarkReport& report) {
  std::cout << "\n📋 Running Basic Read/Write Workload\n";
  std::cout << "   Operations: " << config_.num_operations << "\n";
  std::cout << "   Threads:    " << config_.num_threads << "\n";
  std::cout << "   Write Ratio: " << config_.write_ratio << "%\n\n";
  
  std::atomic<uint64_t> progress{0};
  uint64_t ops_per_thread = config_.num_operations / config_.num_threads;
  
  std::vector<std::thread> threads;
  auto start = std::chrono::steady_clock::now();
  
  for (uint32_t i = 0; i < config_.num_threads; i++) {
    threads.emplace_back(&BasicReadWriteWorkload::WorkerThread, this, 
                         i, ops_per_thread, std::ref(report), std::ref(progress));
  }
  
  // Progress reporter
  while (progress.load() < config_.num_operations) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto pct = 100.0 * progress.load() / config_.num_operations;
    std::cout << "   Progress: " << std::fixed << std::setprecision(1) 
              << pct << "%\r" << std::flush;
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
  
  std::cout << "\n   Completed in " << duration << " seconds\n";
}

void BasicReadWriteWorkload::WorkerThread(uint32_t thread_id, 
                                          uint64_t ops_per_thread,
                                          BenchmarkReport& report,
                                          std::atomic<uint64_t>& progress) {
  std::random_device rd;
  std::mt19937 gen(rd() + thread_id);
  std::uniform_int_distribution<uint64_t> entity_dist(1, config_.num_vertices);
  std::uniform_int_distribution<uint32_t> op_dist(1, 100);
  std::uniform_int_distribution<uint64_t> time_dist(1, 1000000);
  
  for (uint64_t i = 0; i < ops_per_thread; i++) {
    uint64_t entity_id = entity_dist(gen);
    uint64_t timestamp = time_dist(gen);
    bool is_write = op_dist(gen) <= config_.write_ratio;
    
    auto op_start = std::chrono::steady_clock::now();
    
    if (is_write) {
      // Write operation
      Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(i));
      auto status = storage_->Put(entity_id, timestamp, desc, Timestamp(1));
      
      auto op_end = std::chrono::steady_clock::now();
      auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
          op_end - op_start).count();
      
      report.write_metrics.Record(latency_us, config_.value_size, status.ok());
    } else {
      // Read operation
      auto result = storage_->Get(entity_id, timestamp);
      
      auto op_end = std::chrono::steady_clock::now();
      auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
          op_end - op_start).count();
      
      report.read_metrics.Record(latency_us, 0, result.has_value());
    }
    
    progress.fetch_add(1);
  }
}

// =============================================================================
// Temporal Point Query Workload
// =============================================================================

TemporalPointQueryWorkload::TemporalPointQueryWorkload(Config config, 
                                                        CedarGraphStorage* storage)
    : config_(config), storage_(storage) {}

void TemporalPointQueryWorkload::Run(BenchmarkReport& report) {
  std::cout << "\n⏱️  Running Temporal Point Query Workload\n";
  std::cout << "   Queries:     " << config_.num_queries << "\n";
  std::cout << "   Threads:     " << config_.num_threads << "\n";
  std::cout << "   Timestamps:  " << config_.num_timestamps << "\n\n";
  
  // First, populate some test data
  std::cout << "   Populating test data...\n";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, 10000);
  std::uniform_int_distribution<uint64_t> time_dist(1, config_.time_range_seconds);
  
  // Write some data first
  for (uint64_t i = 0; i < 10000; i++) {
    uint64_t entity_id = entity_dist(gen);
    uint64_t timestamp = time_dist(gen);
    Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(i));
    storage_->Put(entity_id, timestamp, desc, Timestamp(1));
  }
  
  std::cout << "   Running queries by exact time...\n";
  QueryByExactTime(report);
  
  std::cout << "   Running queries by version...\n";
  QueryByVersion(report);
  
  std::cout << "   Completed\n";
}

void TemporalPointQueryWorkload::QueryByExactTime(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, 10000);
  std::uniform_int_distribution<uint64_t> time_dist(1, config_.time_range_seconds);
  
  uint64_t queries_per_thread = config_.num_queries / config_.num_threads;
  std::vector<std::thread> threads;
  
  for (uint32_t t = 0; t < config_.num_threads; t++) {
    threads.emplace_back([&, t]() {
      std::mt19937 local_gen(rd() + t);
      
      for (uint64_t i = 0; i < queries_per_thread; i++) {
        uint64_t entity_id = entity_dist(local_gen);
        uint64_t timestamp = time_dist(local_gen);
        
        auto op_start = std::chrono::steady_clock::now();
        auto result = storage_->Get(entity_id, timestamp);
        auto op_end = std::chrono::steady_clock::now();
        
        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
            op_end - op_start).count();
        
        report.temporal_metrics.point_query_by_time.Record(latency_us, 0, result.has_value());
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
}

void TemporalPointQueryWorkload::QueryByVersion(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, 10000);
  std::uniform_int_distribution<uint64_t> version_dist(1, 100);
  
  uint64_t queries_per_thread = config_.num_queries / config_.num_threads;
  std::vector<std::thread> threads;
  
  for (uint32_t t = 0; t < config_.num_threads; t++) {
    threads.emplace_back([&, t]() {
      std::mt19937 local_gen(rd() + t);
      
      for (uint64_t i = 0; i < queries_per_thread; i++) {
        uint64_t entity_id = entity_dist(local_gen);
        Timestamp version(version_dist(local_gen));
        
        auto op_start = std::chrono::steady_clock::now();
        // Query with specific entity type and column
        auto result = storage_->Get(entity_id, EntityType::Vertex, 0, version);
        auto op_end = std::chrono::steady_clock::now();
        
        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
            op_end - op_start).count();
        
        report.temporal_metrics.point_query_by_version.Record(latency_us, 0, result.has_value());
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
}

// =============================================================================
// Temporal Range Query Workload
// =============================================================================

TemporalRangeQueryWorkload::TemporalRangeQueryWorkload(Config config, 
                                                        CedarGraphStorage* storage)
    : config_(config), storage_(storage) {}

void TemporalRangeQueryWorkload::Run(BenchmarkReport& report) {
  std::cout << "\n📊 Running Temporal Range Query Workload\n";
  std::cout << "   Queries: " << config_.num_queries << "\n";
  std::cout << "   Threads: " << config_.num_threads << "\n\n";
  
  // Populate test data with time series
  std::cout << "   Populating time series data...\n";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, 5000);
  
  for (uint64_t t = 0; t < config_.time_range_seconds; t += 60) {
    for (int e = 0; e < 10; e++) {
      uint64_t entity_id = entity_dist(gen);
      Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(t));
      storage_->Put(entity_id, t, desc, Timestamp(1));
    }
  }
  
  uint64_t queries_per_range = config_.num_queries / 3;
  
  std::cout << "   Testing small ranges (1 hour)...\n";
  RunRangeQuery(report, config_.small_range_seconds, 
                report.temporal_metrics.range_query_small, "small");
  
  std::cout << "   Testing medium ranges (1 day)...\n";
  RunRangeQuery(report, config_.medium_range_seconds,
                report.temporal_metrics.range_query_medium, "medium");
  
  std::cout << "   Testing large ranges (1 week)...\n";
  RunRangeQuery(report, config_.large_range_seconds,
                report.temporal_metrics.range_query_large, "large");
  
  std::cout << "   Completed\n";
}

void TemporalRangeQueryWorkload::RunRangeQuery(BenchmarkReport& report, 
                                               uint64_t range_seconds,
                                               OperationMetrics& metrics,
                                               const std::string& name) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, 5000);
  std::uniform_int_distribution<uint64_t> start_time_dist(1, 
      config_.time_range_seconds > range_seconds ? 
      config_.time_range_seconds - range_seconds : 1);
  
  uint64_t queries_per_thread = (config_.num_queries / 3) / config_.num_threads;
  std::vector<std::thread> threads;
  
  for (uint32_t t = 0; t < config_.num_threads; t++) {
    threads.emplace_back([&, t]() {
      std::mt19937 local_gen(rd() + t);
      
      for (uint64_t i = 0; i < queries_per_thread; i++) {
        uint64_t entity_id = entity_dist(local_gen);
        uint64_t start_time = start_time_dist(local_gen);
        uint64_t end_time = start_time + range_seconds;
        
        auto op_start = std::chrono::steady_clock::now();
        auto results = storage_->Scan(entity_id, Timestamp(start_time), Timestamp(end_time));
        auto op_end = std::chrono::steady_clock::now();
        
        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
            op_end - op_start).count();
        
        size_t bytes = results.size() * sizeof(std::pair<Timestamp, Descriptor>);
        metrics.Record(latency_us, bytes, true);
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
}

// =============================================================================
// Temporal Analytics Workload
// =============================================================================

TemporalAnalyticsWorkload::TemporalAnalyticsWorkload(Config config, 
                                                      CedarGraphStorage* storage)
    : config_(config), storage_(storage) {}

void TemporalAnalyticsWorkload::Run(BenchmarkReport& report) {
  std::cout << "\n📈 Running Temporal Analytics Workload\n";
  std::cout << "   Snapshot Queries:   " << config_.num_snapshots << "\n";
  std::cout << "   Diff Queries:       " << config_.num_diff_queries << "\n";
  std::cout << "   Lifecycle Queries:  " << config_.num_lifecycle_queries << "\n\n";
  
  // Populate test data
  std::cout << "   Populating test data...\n";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, config_.entity_range);
  
  for (uint64_t i = 0; i < 5000; i++) {
    uint64_t entity_id = entity_dist(gen);
    for (uint64_t t = 0; t < 100; t++) {
      Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(t));
      storage_->Put(entity_id, t * 1000, desc, Timestamp(t));
    }
  }
  
  std::cout << "   Running snapshot queries...\n";
  SnapshotQuery(report);
  
  std::cout << "   Running diff queries...\n";
  DiffQuery(report);
  
  std::cout << "   Running lifecycle queries...\n";
  LifecycleQuery(report);
  
  std::cout << "   Completed\n";
}

void TemporalAnalyticsWorkload::SnapshotQuery(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, config_.entity_range);
  std::uniform_int_distribution<uint64_t> time_dist(0, 100000);
  
  for (uint64_t i = 0; i < config_.num_snapshots; i++) {
    uint64_t entity_id = entity_dist(gen);
    Timestamp snapshot_time(time_dist(gen));
    
    auto op_start = std::chrono::steady_clock::now();
    
    // Query all versions up to snapshot_time
    auto results = storage_->ScanLimit(entity_id, Timestamp(0), snapshot_time, 100);
    
    auto op_end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        op_end - op_start).count();
    
    size_t bytes = results.size() * sizeof(std::pair<Timestamp, Descriptor>);
    report.temporal_metrics.temporal_snapshot.Record(latency_us, bytes, true);
  }
}

void TemporalAnalyticsWorkload::DiffQuery(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, config_.entity_range);
  std::uniform_int_distribution<uint64_t> time_dist(0, 50000);
  
  for (uint64_t i = 0; i < config_.num_diff_queries; i++) {
    uint64_t entity_id = entity_dist(gen);
    Timestamp start_time(time_dist(gen));
    Timestamp end_time(start_time.value() + 50000);
    
    auto op_start = std::chrono::steady_clock::now();
    
    // Get versions at both time points
    auto start_versions = storage_->ScanLimit(entity_id, Timestamp(0), start_time, 100);
    auto end_versions = storage_->ScanLimit(entity_id, start_time, end_time, 100);
    
    auto op_end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        op_end - op_start).count();
    
    size_t bytes = (start_versions.size() + end_versions.size()) * 
                   sizeof(std::pair<Timestamp, Descriptor>);
    report.temporal_metrics.temporal_diff.Record(latency_us, bytes, true);
  }
}

void TemporalAnalyticsWorkload::LifecycleQuery(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, config_.entity_range);
  
  for (uint64_t i = 0; i < config_.num_lifecycle_queries; i++) {
    uint64_t entity_id = entity_dist(gen);
    
    auto op_start = std::chrono::steady_clock::now();
    
    // Get entity lifecycle history
    auto history = storage_->GetEntityLifecycleHistory(
        entity_id, EntityType::Vertex, Timestamp(0), Timestamp(100000));
    
    auto op_end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        op_end - op_start).count();
    
    report.temporal_metrics.entity_lifecycle.Record(latency_us, history.size() * 16, true);
  }
}

// =============================================================================
// Graph Analytics Workload
// =============================================================================

GraphAnalyticsWorkload::GraphAnalyticsWorkload(Config config, 
                                                CedarGraphStorage* storage)
    : config_(config), storage_(storage) {}

void GraphAnalyticsWorkload::Run(BenchmarkReport& report) {
  std::cout << "\n🌐 Running Graph Analytics Workload\n";
  std::cout << "   Neighbor Queries:      " << config_.num_neighbor_queries << "\n";
  std::cout << "   BFS Queries:           " << config_.num_bfs_queries << "\n";
  std::cout << "   Shortest Path Queries: " << config_.num_shortest_path_queries << "\n\n";
  
  // Create a simple graph structure
  std::cout << "   Creating test graph structure...\n";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> node_dist(1, 1000);
  std::uniform_int_distribution<uint64_t> edge_dist(1, 10);
  
  // Create edges (src -> dst)
  for (uint64_t src = 1; src <= 1000; src++) {
    uint64_t num_edges = edge_dist(gen);
    for (uint64_t e = 0; e < num_edges; e++) {
      uint64_t dst = node_dist(gen);
      if (dst != src) {
        Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(dst));
        storage_->PutEdge(src, dst, 1, Timestamp(1), desc, Timestamp(1));
      }
    }
  }
  
  std::cout << "   Running neighbor queries...\n";
  NeighborQuery(report);
  
  std::cout << "   Running BFS traversal...\n";
  BFSTraversal(report);
  
  std::cout << "   Running shortest path...\n";
  ShortestPath(report);
  
  std::cout << "   Completed\n";
}

void GraphAnalyticsWorkload::NeighborQuery(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> node_dist(1, 1000);
  
  uint64_t queries_per_thread = config_.num_neighbor_queries / config_.num_threads;
  std::vector<std::thread> threads;
  
  for (uint32_t t = 0; t < config_.num_threads; t++) {
    threads.emplace_back([&, t]() {
      std::mt19937 local_gen(rd() + t);
      
      for (uint64_t i = 0; i < queries_per_thread; i++) {
        uint64_t src_id = node_dist(local_gen);
        
        auto op_start = std::chrono::steady_clock::now();
        auto edges = storage_->ScanEdges(src_id, 1, Timestamp(0), Timestamp::Max());
        auto op_end = std::chrono::steady_clock::now();
        
        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
            op_end - op_start).count();
        
        size_t bytes = edges.size() * sizeof(std::tuple<uint64_t, Timestamp, Descriptor>);
        report.analytics_metrics.neighbor_query.Record(latency_us, bytes, true);
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
}

void GraphAnalyticsWorkload::BFSTraversal(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> node_dist(1, 1000);
  
  for (uint64_t i = 0; i < config_.num_bfs_queries; i++) {
    uint64_t start_node = node_dist(gen);
    
    auto op_start = std::chrono::steady_clock::now();
    
    // BFS traversal
    std::queue<std::pair<uint64_t, uint32_t>> queue;
    std::unordered_set<uint64_t> visited;
    queue.push({start_node, 0});
    visited.insert(start_node);
    
    size_t visited_count = 0;
    while (!queue.empty() && visited_count < 1000) {
      auto [current, depth] = queue.front();
      queue.pop();
      visited_count++;
      
      if (depth < config_.bfs_depth) {
        auto edges = storage_->ScanEdges(current, 1, Timestamp(0), Timestamp::Max());
        for (const auto& [dst, ts, desc] : edges) {
          if (visited.find(dst) == visited.end()) {
            visited.insert(dst);
            queue.push({dst, depth + 1});
          }
        }
      }
    }
    
    auto op_end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        op_end - op_start).count();
    
    report.analytics_metrics.bfs_traversal.Record(latency_us, visited_count * 8, true);
  }
}

void GraphAnalyticsWorkload::ShortestPath(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> node_dist(1, 1000);
  
  for (uint64_t i = 0; i < config_.num_shortest_path_queries; i++) {
    uint64_t src = node_dist(gen);
    uint64_t dst = node_dist(gen);
    
    auto op_start = std::chrono::steady_clock::now();
    
    // Simple BFS for shortest path
    std::queue<std::pair<uint64_t, uint32_t>> queue;
    std::unordered_set<uint64_t> visited;
    queue.push({src, 0});
    visited.insert(src);
    
    bool found = false;
    uint32_t path_length = 0;
    
    while (!queue.empty() && !found) {
      auto [current, dist] = queue.front();
      queue.pop();
      
      if (current == dst) {
        found = true;
        path_length = dist;
        break;
      }
      
      if (dist < 10) {  // Limit search depth
        auto edges = storage_->ScanEdges(current, 1, Timestamp(0), Timestamp::Max());
        for (const auto& [next, ts, desc] : edges) {
          if (visited.find(next) == visited.end()) {
            visited.insert(next);
            queue.push({next, dist + 1});
          }
        }
      }
    }
    
    auto op_end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        op_end - op_start).count();
    
    report.analytics_metrics.shortest_path.Record(latency_us, visited.size() * 8, found);
  }
}

// =============================================================================
// Real-time Latency Workload
// =============================================================================

RealtimeLatencyWorkload::RealtimeLatencyWorkload(Config config, 
                                                  CedarGraphStorage* storage)
    : config_(config), storage_(storage) {}

void RealtimeLatencyWorkload::Run(BenchmarkReport& report) {
  std::cout << "\n⚡ Running Real-time Latency Workload\n";
  std::cout << "   Duration:     " << config_.duration_seconds << " seconds\n";
  std::cout << "   Target QPS:   " << config_.target_qps << "\n";
  std::cout << "   Warmup:       " << config_.warmup_seconds << " seconds\n\n";
  
  // Clear any previous samples
  {
    std::lock_guard<std::mutex> lock(samples_mutex_);
    latency_samples_.clear();
    latency_samples_.reserve(config_.target_qps * config_.duration_seconds);
  }
  
  // Warmup phase
  std::cout << "   Warming up...\n";
  MeasureLatencyDistribution(report);
  
  // Clear warmup samples
  {
    std::lock_guard<std::mutex> lock(samples_mutex_);
    latency_samples_.clear();
  }
  
  // Actual measurement
  std::cout << "   Measuring latency distribution...\n";
  auto start = std::chrono::steady_clock::now();
  MeasureLatencyDistribution(report);
  auto end = std::chrono::steady_clock::now();
  
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
  std::cout << "   Measured for " << duration << " seconds\n";
  
  // Calculate percentiles
  CalculatePercentiles(report);
  
  std::cout << "   P50:  " << report.p50_latency_ms << " ms\n";
  std::cout << "   P99:  " << report.p99_latency_ms << " ms\n";
  std::cout << "   P999: " << report.p999_latency_ms << " ms\n";
  std::cout << "   Completed\n";
}

void RealtimeLatencyWorkload::MeasureLatencyDistribution(BenchmarkReport& report) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> entity_dist(1, 10000);
  std::uniform_int_distribution<uint64_t> time_dist(1, 1000000);
  
  auto interval_us = 1000000 / config_.target_qps;  // Microseconds between requests
  auto start_time = std::chrono::steady_clock::now();
  auto end_time = start_time + std::chrono::seconds(config_.duration_seconds);
  
  while (std::chrono::steady_clock::now() < end_time) {
    auto op_start = std::chrono::steady_clock::now();
    
    uint64_t entity_id = entity_dist(gen);
    uint64_t timestamp = time_dist(gen);
    
    // Mixed read/write
    if (entity_dist(gen) % 2 == 0) {
      auto result = storage_->Get(entity_id, timestamp);
    } else {
      Descriptor desc = Descriptor::InlineInt(0, static_cast<int32_t>(timestamp));
      storage_->Put(entity_id, timestamp, desc, Timestamp(1));
    }
    
    auto op_end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        op_end - op_start).count();
    
    {
      std::lock_guard<std::mutex> lock(samples_mutex_);
      latency_samples_.push_back(latency_us);
    }
    
    // Rate limiting
    auto next_op_time = op_start + std::chrono::microseconds(interval_us);
    while (std::chrono::steady_clock::now() < next_op_time) {
      std::this_thread::yield();
    }
  }
}

void RealtimeLatencyWorkload::CalculatePercentiles(BenchmarkReport& report) {
  std::vector<uint64_t> sorted_samples;
  {
    std::lock_guard<std::mutex> lock(samples_mutex_);
    sorted_samples = latency_samples_;
  }
  
  if (sorted_samples.empty()) {
    report.p50_latency_ms = 0;
    report.p99_latency_ms = 0;
    report.p999_latency_ms = 0;
    return;
  }
  
  std::sort(sorted_samples.begin(), sorted_samples.end());
  
  auto percentile = [&sorted_samples](double p) -> double {
    size_t index = static_cast<size_t>(p * sorted_samples.size());
    if (index >= sorted_samples.size()) index = sorted_samples.size() - 1;
    return sorted_samples[index] / 1000.0;  // Convert to ms
  };
  
  report.p50_latency_ms = percentile(0.50);
  report.p99_latency_ms = percentile(0.99);
  report.p999_latency_ms = percentile(0.999);
}

}  // namespace benchmark
}  // namespace cedar
