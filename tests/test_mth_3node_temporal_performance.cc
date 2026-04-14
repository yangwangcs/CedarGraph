// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0
//
// MTHStream 3-Node Cluster Temporal Performance Benchmark
//
// Tests:
// 1. Temporal graph write throughput (vertices + edges with timestamps)
// 2. Temporal point query latency (get value at exact timestamp)
// 3. Temporal range query throughput (scan time range)

#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <thread>
#include <atomic>
#include <iomanip>
#include <algorithm>
#include <memory>
#include <filesystem>
#include <numeric>
#include <sstream>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/partition.h"
#include "cedar/sst/zone_columnar_reader.h"

using namespace cedar;
using namespace cedar::dtx;
namespace fs = std::filesystem;

// =============================================================================
// Benchmark Configuration
// =============================================================================
struct BenchmarkConfig {
  int node_count = 3;
  PartitionID num_partitions = 256;
  int num_vertices = 10000;
  int edges_per_vertex = 10;
  int num_threads = 8;
  int point_queries = 50000;
  int range_queries = 10000;
  uint64_t time_range_micros = 3600ULL * 1000000ULL;  // 1 hour
  bool cleanup_after = true;
};

struct PerfResult {
  double throughput = 0.0;    // ops/sec
  double avg_latency_us = 0.0;
  double p50_latency_us = 0.0;
  double p99_latency_us = 0.0;
  int64_t total_ops = 0;
  int64_t success_ops = 0;
  int64_t duration_ms = 0;
};

// =============================================================================
// Simulated Storage Node with CedarGraphStorage
// =============================================================================
class StorageNode {
 public:
  StorageNode(int node_id, const std::string& data_dir)
      : node_id_(node_id), data_dir_(data_dir) {
    CedarOptions options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.memtable_threshold = 128 * 1024 * 1024;  // 128MB to avoid excessive flush threads
    options.write_buffer_size = 128 * 1024 * 1024;
    options.enable_wal = true;

    CedarGraphStorage* storage_ptr = nullptr;
    auto status = CedarGraphStorage::Open(options, data_dir, &storage_ptr);
    if (!status.ok()) {
      throw std::runtime_error("Failed to open storage: " + status.ToString());
    }
    storage_.reset(storage_ptr);
  }

  ~StorageNode() = default;

  int GetId() const { return node_id_; }

  // Write a CedarKey + value to this node's storage
  Status Put(const CedarKey& key, const std::string& value) {
    // Encode value as inline integer (hash of string for simplicity)
    uint32_t value_hash = 0;
    for (char c : value) value_hash = value_hash * 31 + c;
    Descriptor desc(EntryKind::InlineInt, key.column_id(), value_hash, 4);
    // Bypass CedarGraphStorage::Put (requires partition_router) and write directly to LSM engine
    return storage_->GetLsmEngine()->Put(key, desc, key.timestamp());
  }

  // Temporal point query: read value at specific timestamp for a key
  std::optional<std::string> ReadAtTime(uint64_t entity_id, Timestamp ts) {
    auto results = storage_->Scan(entity_id, Timestamp(0), ts);
    if (!results.empty()) {
      // results are in descending timestamp order, first is latest
      auto opt_int = results.front().second.AsInlineInt();
      if (opt_int) {
        return std::to_string(opt_int.value());
      }
      return std::to_string(results.front().second.AsRaw());
    }
    return std::nullopt;
  }

  // Temporal range query: read all versions in [start_ts, end_ts]
  std::vector<std::pair<Timestamp, std::string>> ReadRange(
      uint64_t entity_id, Timestamp start_ts, Timestamp end_ts) {
    auto results = storage_->Scan(entity_id, start_ts, end_ts);
    std::vector<std::pair<Timestamp, std::string>> out;
    out.reserve(results.size());
    for (auto& r : results) {
      auto opt_int = r.second.AsInlineInt();
      if (opt_int) {
        out.push_back({r.first, std::to_string(opt_int.value())});
      } else {
        out.push_back({r.first, std::to_string(r.second.AsRaw())});
      }
    }
    return out;
  }

  // Extract all keys belonging to a specific partition from this node's storage.
  std::vector<std::pair<CedarKey, Descriptor>> ExtractPartitionData(PartitionID target_pid) {
    std::vector<std::pair<CedarKey, Descriptor>> results;
    auto* engine = storage_->GetLsmEngine();
    if (!engine) return results;

    auto collect = [&](const CedarKey& key, const Descriptor& desc) {
      if (key.part_id() == target_pid) {
        results.emplace_back(key, desc);
      }
      return true;
    };

    auto* mem = engine->GetMemTable();
    if (mem) {
      mem->Traverse(collect);
    }

    std::string db_path = engine->GetDbPath();
    const auto& levels = engine->GetSstFiles();
    for (const auto& level : levels) {
      for (const auto& file_meta : level) {
        std::string file_path = db_path + "/" + file_meta.file_name();
        cedar::ZoneColumnarSstReader reader(file_path);
        if (!reader.Open().ok()) continue;

        cedar::ReadPredicate pred;
        pred.part_id = target_pid;
        pred.skip_tombstones = true;
        reader.Scan(pred, [&](const CedarKey& key, const Descriptor& desc) {
          results.emplace_back(key, desc);
        });
      }
    }

    return results;
  }

  // Bulk write a vector of (key, descriptor) pairs.
  Status BulkPut(const std::vector<std::pair<CedarKey, Descriptor>>& items) {
    auto* engine = storage_->GetLsmEngine();
    if (!engine) {
      return Status::InvalidArgument("StorageNode", "engine not available");
    }
    for (const auto& [key, desc] : items) {
      auto s = engine->Put(key, desc, key.timestamp());
      if (!s.ok()) return s;
    }
    return Status::OK();
  }

  size_t GetKeyCount() const {
    // Approximate via stats
    auto stats = storage_->GetStats();
    // Rough estimate: memtable entries + SST files indicator
    return stats.memtable_size / 64 + stats.sst_count * 1000;
  }

  std::string GetStats() {
    auto stats = storage_->GetStats();
    std::ostringstream oss;
    oss << "MemTable: " << stats.memtable_size
        << ", SST: " << stats.sst_count;
    return oss.str();
  }

 private:
  int node_id_;
  std::string data_dir_;
  std::unique_ptr<CedarGraphStorage> storage_;
};

// =============================================================================
// 3-Node Cluster with MTHStream Partitioning
// =============================================================================
class MTH3NodeCluster {
 public:
  MTH3NodeCluster(const BenchmarkConfig& config,
                  DualModePartitionStrategy::Mode mode)
      : config_(config) {
    // Initialize partition manager
    DualModePartitionStrategy::Config partition_config;
    partition_config.mode = mode;
    partition_config.num_partitions = config.num_partitions;
    partition_config.sketch_depth = 4;
    partition_config.sketch_width = 128;
    partition_config.fast_path_threshold = 0.5;
    partition_config.temporal_alpha = 0.01;

    DTxConfig dtx_config;
    partition_manager_ = std::make_unique<PartitionManager>(dtx_config);
    auto status = partition_manager_->InitializeDualMode(partition_config);
    if (!status.ok()) {
      throw std::runtime_error("Failed to init partition manager: " + status.ToString());
    }

    // Initialize 3 storage nodes
    for (int i = 0; i < config.node_count; ++i) {
      std::string data_dir = "/tmp/cedar_mth_perf/node" + std::to_string(i);
      fs::remove_all(data_dir);
      fs::create_directories(data_dir);
      nodes_.push_back(std::make_unique<StorageNode>(i, data_dir));
    }

    // Assign 256 partitions to 3 nodes via round-robin
    std::vector<NodeID> node_ids;
    for (int i = 0; i < config.node_count; ++i) {
      node_ids.push_back(static_cast<NodeID>(i));
    }
    status = partition_manager_->AssignPartitionsToNodes(node_ids);
    if (!status.ok()) {
      throw std::runtime_error("Failed to assign partitions: " + status.ToString());
    }
  }

  ~MTH3NodeCluster() {
    nodes_.clear();
    if (config_.cleanup_after) {
      for (int i = 0; i < 8; ++i) {
        fs::remove_all("/tmp/cedar_mth_perf/node" + std::to_string(i));
      }
    }
  }

  // Route key to node based on partition leader assignment
  StorageNode* RouteToNode(const CedarKey& key) {
    PartitionID pid = partition_manager_->GetPartition(key);
    NodeID node_id = partition_manager_->GetPartitionLeader(pid);
    if (node_id == kInvalidNodeID || node_id >= static_cast<NodeID>(nodes_.size())) {
      // Fallback for safety
      node_id = static_cast<NodeID>(pid % nodes_.size());
    }
    return nodes_[static_cast<size_t>(node_id)].get();
  }

  // Write vertex with timestamp
  Status WriteVertex(uint64_t vid, Timestamp ts, const std::string& value) {
    CedarKey key = CedarKey::Vertex(vid, 0_vcol, ts, 0, kInvalidPartitionID);
    StorageNode* node = RouteToNode(key);
    key = CedarKeyPartitionHelper::SetPartitionID(key, partition_manager_->GetPartition(key));
    return node->Put(key, value);
  }

  // Write edge with timestamp
  Status WriteEdge(uint64_t src, uint64_t dst, uint16_t type, Timestamp ts,
                   const std::string& value) {
    CedarKey edge_out = CedarKey::EdgeOut(src, dst, EdgeTypeId(type), ts, 0, kInvalidPartitionID);
    StorageNode* src_node = RouteToNode(edge_out);
    edge_out = CedarKeyPartitionHelper::SetPartitionID(
        edge_out, partition_manager_->GetPartition(edge_out));
    auto status = src_node->Put(edge_out, value);
    if (!status.ok()) return status;

    CedarKey edge_in = CedarKey::EdgeIn(dst, src, EdgeTypeId(type), ts, 0, kInvalidPartitionID);
    StorageNode* dst_node = RouteToNode(edge_in);
    edge_in = CedarKeyPartitionHelper::SetPartitionID(
        edge_in, partition_manager_->GetPartition(edge_in));
    return dst_node->Put(edge_in, value);
  }

  // Temporal point query
  std::optional<std::string> PointQuery(uint64_t vid, Timestamp ts) {
    CedarKey key = CedarKey::Vertex(vid, 0_vcol, ts, 0, kInvalidPartitionID);
    StorageNode* node = RouteToNode(key);
    return node->ReadAtTime(vid, ts);
  }

  // Temporal range query
  std::vector<std::pair<Timestamp, std::string>> RangeQuery(
      uint64_t vid, Timestamp start_ts, Timestamp end_ts) {
    CedarKey key = CedarKey::Vertex(vid, 0_vcol, end_ts, 0, kInvalidPartitionID);
    StorageNode* node = RouteToNode(key);
    return node->ReadRange(vid, start_ts, end_ts);
  }

  // Perform count-based rebalancing across existing nodes.
  Status Rebalance() {
    auto plan = partition_manager_->ComputeRebalancePlan();
    if (plan.empty()) {
      std::cout << "  Rebalance: no migration needed" << std::endl;
      return Status::OK();
    }

    std::cout << "  Rebalance: migrating " << plan.size() << " partitions..." << std::endl;
    for (const auto& mig : plan) {
      StorageNode* from_node = nodes_[static_cast<size_t>(mig.from_node)].get();
      StorageNode* to_node   = nodes_[static_cast<size_t>(mig.to_node)].get();

      auto data = from_node->ExtractPartitionData(mig.partition_id);
      if (!data.empty()) {
        auto s = to_node->BulkPut(data);
        if (!s.ok()) return s;
      }

      auto s = partition_manager_->MigratePartition(mig.partition_id, mig.to_node);
      if (!s.ok()) return s;
    }

    std::cout << "  Rebalance: completed" << std::endl;
    return Status::OK();
  }

  // Scale out from current node_count to new_node_count.
  Status ScaleOut(int new_node_count) {
    if (new_node_count <= config_.node_count) {
      return Status::InvalidArgument("ScaleOut", "new_node_count must be larger");
    }

    std::cout << "  ScaleOut: expanding from " << config_.node_count
              << " to " << new_node_count << " nodes..." << std::endl;

    for (int i = config_.node_count; i < new_node_count; ++i) {
      std::string data_dir = "/tmp/cedar_mth_perf/node" + std::to_string(i);
      fs::remove_all(data_dir);
      fs::create_directories(data_dir);
      nodes_.push_back(std::make_unique<StorageNode>(i, data_dir));
    }

    std::vector<NodeID> all_nodes;
    for (int i = 0; i < new_node_count; ++i) {
      all_nodes.push_back(static_cast<NodeID>(i));
    }
    auto s = partition_manager_->AssignPartitionsToNodes(all_nodes);
    if (!s.ok()) return s;

    for (PartitionID pid = 0; pid < config_.num_partitions; ++pid) {
      NodeID new_leader = partition_manager_->GetPartitionLeader(pid);
      if (new_leader == kInvalidNodeID) continue;

      for (int old_node = 0; old_node < config_.node_count; ++old_node) {
        if (static_cast<NodeID>(old_node) == new_leader) continue;

        auto data = nodes_[old_node]->ExtractPartitionData(pid);
        if (!data.empty()) {
          auto s2 = nodes_[static_cast<size_t>(new_leader)]->BulkPut(data);
          if (!s2.ok()) return s2;
        }
      }
    }

    config_.node_count = new_node_count;
    std::cout << "  ScaleOut: completed" << std::endl;
    return Status::OK();
  }

  void PrintDistribution() {
    std::cout << "\n=== Node Distribution ===" << std::endl;
    for (auto& node : nodes_) {
      std::cout << "  Node " << node->GetId() << ": " << node->GetStats() << std::endl;
    }
  }

  void PrintPartitionStats() {
    auto* dual_mode = partition_manager_->GetDualModeStrategy();
    if (dual_mode) {
      std::cout << "\n=== Partition Strategy Stats ===" << std::endl;
      std::cout << dual_mode->GetStats() << std::endl;
    }
  }

  PartitionManager* GetPartitionManager() { return partition_manager_.get(); }

 private:
  BenchmarkConfig config_;
  std::unique_ptr<PartitionManager> partition_manager_;
  std::vector<std::unique_ptr<StorageNode>> nodes_;
};

// =============================================================================
// Performance Measurement Helpers
// =============================================================================
PerfResult MeasureThroughput(int64_t total_ops, int64_t success_ops,
                             int64_t duration_ms,
                             std::vector<double>& latencies) {
  PerfResult result;
  result.total_ops = total_ops;
  result.success_ops = success_ops;
  result.duration_ms = duration_ms;
  result.throughput = duration_ms > 0
                          ? (success_ops * 1000.0) / duration_ms
                          : 0.0;

  if (!latencies.empty()) {
    std::sort(latencies.begin(), latencies.end());
    result.avg_latency_us = std::accumulate(latencies.begin(), latencies.end(), 0.0)
                            / latencies.size();
    result.p50_latency_us = latencies[static_cast<size_t>(latencies.size() * 0.50)];
    result.p99_latency_us = latencies[static_cast<size_t>(latencies.size() * 0.99)];
  }
  return result;
}

void PrintResult(const std::string& name, const PerfResult& r) {
  std::cout << std::left << std::setw(30) << name
            << std::right << std::setw(12) << std::fixed << std::setprecision(0)
            << r.throughput << " ops/s"
            << std::setw(12) << std::setprecision(1) << r.avg_latency_us << " us"
            << std::setw(12) << std::setprecision(1) << r.p50_latency_us << " us"
            << std::setw(12) << std::setprecision(1) << r.p99_latency_us << " us"
            << std::setw(15) << r.success_ops << "/" << r.total_ops
            << std::endl;
}

// =============================================================================
// Benchmark 1: Temporal Graph Write Throughput
// =============================================================================
PerfResult BenchmarkTemporalWrite(MTH3NodeCluster& cluster,
                                   const BenchmarkConfig& config) {
  std::cout << "\n[BENCHMARK 1] Temporal Graph Write Throughput" << std::endl;
  std::cout << "  Vertices: " << config.num_vertices
            << ", Edges/vertex: " << config.edges_per_vertex
            << ", Threads: " << config.num_threads << std::endl;

  std::atomic<int64_t> success_count{0};
  std::vector<double> latencies;
  std::mutex latency_mutex;
  std::mt19937_64 rng_base(2026);

  int total_writes = config.num_vertices + config.num_vertices * config.edges_per_vertex;
  int writes_per_thread = total_writes / config.num_threads;

  auto start = std::chrono::steady_clock::now();

  std::vector<std::thread> threads;
  for (int t = 0; t < config.num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937_64 rng(rng_base() + t);
      std::uniform_int_distribution<uint64_t> ts_dist(0, config.time_range_micros);
      std::uniform_int_distribution<uint64_t> vid_dist(1, config.num_vertices * 10);

      for (int i = 0; i < writes_per_thread; ++i) {
        int global_idx = t * writes_per_thread + i;

        if (global_idx < config.num_vertices) {
          // Write vertex
          uint64_t vid = global_idx + 1;
          Timestamp ts(ts_dist(rng));
          auto op_start = std::chrono::steady_clock::now();
          auto status = cluster.WriteVertex(vid, ts, "v_" + std::to_string(vid));
          auto op_end = std::chrono::steady_clock::now();

          if (status.ok()) {
            success_count++;
            double lat = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             op_end - op_start)
                             .count() /
                         1000.0;
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.push_back(lat);
          }
        } else {
          // Write edge
          int edge_idx = global_idx - config.num_vertices;
          uint64_t src = (edge_idx % config.num_vertices) + 1;
          uint64_t dst = vid_dist(rng);
          if (src == dst) dst = src + 1;
          Timestamp ts(ts_dist(rng));
          auto op_start = std::chrono::steady_clock::now();
          auto status = cluster.WriteEdge(src, dst, 1, ts,
                                          "e_" + std::to_string(edge_idx));
          auto op_end = std::chrono::steady_clock::now();

          if (status.ok()) {
            success_count++;
            double lat = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             op_end - op_start)
                             .count() /
                         1000.0;
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.push_back(lat);
          }
        }
      }
    });
  }

  for (auto& t : threads) t.join();

  auto end = std::chrono::steady_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  return MeasureThroughput(total_writes, success_count.load(), duration_ms, latencies);
}

// =============================================================================
// Benchmark 2: Temporal Point Query Latency
// =============================================================================
PerfResult BenchmarkTemporalPointQuery(MTH3NodeCluster& cluster,
                                        const BenchmarkConfig& config) {
  std::cout << "\n[BENCHMARK 2] Temporal Point Query" << std::endl;
  std::cout << "  Queries: " << config.point_queries
            << ", Threads: " << config.num_threads << std::endl;

  std::atomic<int64_t> success_count{0};
  std::vector<double> latencies;
  std::mutex latency_mutex;
  std::mt19937_64 rng_base(42);

  int queries_per_thread = config.point_queries / config.num_threads;

  auto start = std::chrono::steady_clock::now();

  std::vector<std::thread> threads;
  for (int t = 0; t < config.num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937_64 rng(rng_base() + t);
      std::uniform_int_distribution<uint64_t> vid_dist(1, config.num_vertices);
      std::uniform_int_distribution<uint64_t> ts_dist(0, config.time_range_micros);

      for (int i = 0; i < queries_per_thread; ++i) {
        uint64_t vid = vid_dist(rng);
        Timestamp ts(ts_dist(rng));

        auto op_start = std::chrono::steady_clock::now();
        auto result = cluster.PointQuery(vid, ts);
        auto op_end = std::chrono::steady_clock::now();

        success_count++;
        double lat = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         op_end - op_start)
                         .count() /
                     1000.0;
        std::lock_guard<std::mutex> lock(latency_mutex);
        latencies.push_back(lat);
      }
    });
  }

  for (auto& t : threads) t.join();

  auto end = std::chrono::steady_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  return MeasureThroughput(config.point_queries, success_count.load(), duration_ms, latencies);
}

// =============================================================================
// Benchmark 3: Temporal Range Query Throughput
// =============================================================================
PerfResult BenchmarkTemporalRangeQuery(MTH3NodeCluster& cluster,
                                        const BenchmarkConfig& config) {
  std::cout << "\n[BENCHMARK 3] Temporal Range Query" << std::endl;
  std::cout << "  Queries: " << config.range_queries
            << ", Threads: " << config.num_threads << std::endl;

  std::atomic<int64_t> success_count{0};
  std::atomic<int64_t> total_rows_scanned{0};
  std::vector<double> latencies;
  std::mutex latency_mutex;
  std::mt19937_64 rng_base(99);

  int queries_per_thread = config.range_queries / config.num_threads;

  auto start = std::chrono::steady_clock::now();

  std::vector<std::thread> threads;
  for (int t = 0; t < config.num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937_64 rng(rng_base() + t);
      std::uniform_int_distribution<uint64_t> vid_dist(1, config.num_vertices);
      std::uniform_int_distribution<uint64_t> ts_start_dist(0,
                                                            config.time_range_micros / 2);
      std::uniform_int_distribution<uint64_t> range_dist(100000, 10000000);  // 100ms - 10s

      for (int i = 0; i < queries_per_thread; ++i) {
        uint64_t vid = vid_dist(rng);
        Timestamp start_ts(ts_start_dist(rng));
        Timestamp end_ts(start_ts.value() + range_dist(rng));

        auto op_start = std::chrono::steady_clock::now();
        auto results = cluster.RangeQuery(vid, start_ts, end_ts);
        auto op_end = std::chrono::steady_clock::now();

        success_count++;
        total_rows_scanned += results.size();

        double lat = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         op_end - op_start)
                         .count() /
                     1000.0;
        std::lock_guard<std::mutex> lock(latency_mutex);
        latencies.push_back(lat);
      }
    });
  }

  for (auto& t : threads) t.join();

  auto end = std::chrono::steady_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  auto result = MeasureThroughput(config.range_queries, success_count.load(), duration_ms, latencies);
  std::cout << "  Average rows/query: "
            << (success_count > 0 ? total_rows_scanned / success_count : 0) << std::endl;
  return result;
}

// =============================================================================
// Comparison Benchmark: StaticHash vs MTHStream
// =============================================================================
void RunComparison(const BenchmarkConfig& config) {
  std::cout << std::string(80, '=') << std::endl;
  std::cout << "MTHStream vs StaticHash Comparison (3-Node Cluster)" << std::endl;
  std::cout << std::string(80, '=') << std::endl;

  PerfResult mth_write, mth_point, mth_range;
  PerfResult hash_write, hash_point, hash_range;

  // MTHStream run
  {
    std::cout << "\n>>> Running with MTHStream strategy <<<" << std::endl;
    MTH3NodeCluster cluster(config, DualModePartitionStrategy::Mode::MTH_STREAM);
    mth_write = BenchmarkTemporalWrite(cluster, config);
    mth_point = BenchmarkTemporalPointQuery(cluster, config);
    mth_range = BenchmarkTemporalRangeQuery(cluster, config);
    cluster.PrintDistribution();

    // Phase: Rebalance validation
    std::cout << "\n[VALIDATION] Rebalancing MTHStream cluster..." << std::endl;
    auto rebalance_status = cluster.Rebalance();
    if (rebalance_status.ok()) {
      auto post_rebalance = BenchmarkTemporalPointQuery(cluster, config);
      PrintResult("MTH Point Query (post-rebalance)", post_rebalance);
    } else {
      std::cout << "  Rebalance skipped: " << rebalance_status.ToString() << std::endl;
    }

    // Phase: Scale-out validation (3 -> 5 nodes)
    std::cout << "\n[VALIDATION] Scaling out MTHStream cluster to 5 nodes..." << std::endl;
    auto scale_status = cluster.ScaleOut(5);
    if (scale_status.ok()) {
      auto post_scale = BenchmarkTemporalPointQuery(cluster, config);
      PrintResult("MTH Point Query (post-scale-out)", post_scale);
    } else {
      std::cout << "  Scale-out failed: " << scale_status.ToString() << std::endl;
    }

    cluster.PrintPartitionStats();
  }

  // StaticHash run
  {
    std::cout << "\n>>> Running with StaticHash strategy <<<" << std::endl;
    MTH3NodeCluster cluster(config, DualModePartitionStrategy::Mode::STATIC_HASH);
    hash_write = BenchmarkTemporalWrite(cluster, config);
    hash_point = BenchmarkTemporalPointQuery(cluster, config);
    hash_range = BenchmarkTemporalRangeQuery(cluster, config);
    cluster.PrintDistribution();
    cluster.PrintPartitionStats();
  }

  // Print comparison table
  std::cout << "\n" << std::string(80, '=') << std::endl;
  std::cout << "PERFORMANCE COMPARISON SUMMARY" << std::endl;
  std::cout << std::string(80, '=') << std::endl;

  std::cout << std::left << std::setw(30) << "Benchmark"
            << std::setw(18) << "MTHStream"
            << std::setw(18) << "StaticHash"
            << std::setw(15) << "Speedup"
            << std::endl;
  std::cout << std::string(80, '-') << std::endl;

  auto print_row = [](const std::string& name, const PerfResult& mth,
                      const PerfResult& hash) {
    double speedup = hash.throughput > 0 ? mth.throughput / hash.throughput : 0.0;
    std::cout << std::left << std::setw(30) << name
              << std::right << std::setw(15) << std::fixed << std::setprecision(0)
              << mth.throughput << " ops/s"
              << std::setw(15) << std::fixed << std::setprecision(0)
              << hash.throughput << " ops/s"
              << std::setw(12) << std::fixed << std::setprecision(2)
              << speedup << "x"
              << std::endl;
  };

  print_row("Temporal Write", mth_write, hash_write);
  print_row("Temporal Point Query", mth_point, hash_point);
  print_row("Temporal Range Query", mth_range, hash_range);

  std::cout << std::string(80, '-') << std::endl;

  // Latency comparison
  std::cout << "\nLATENCY COMPARISON (P99)" << std::endl;
  std::cout << std::left << std::setw(30) << "Benchmark"
            << std::setw(18) << "MTHStream P99"
            << std::setw(18) << "StaticHash P99"
            << std::endl;
  std::cout << std::string(66, '-') << std::endl;

  auto print_lat = [](const std::string& name, const PerfResult& mth,
                      const PerfResult& hash) {
    std::cout << std::left << std::setw(30) << name
              << std::right << std::setw(15) << std::fixed << std::setprecision(1)
              << mth.p99_latency_us << " us"
              << std::setw(15) << std::fixed << std::setprecision(1)
              << hash.p99_latency_us << " us"
              << std::endl;
  };

  print_lat("Temporal Write", mth_write, hash_write);
  print_lat("Temporal Point Query", mth_point, hash_point);
  print_lat("Temporal Range Query", mth_range, hash_range);
}

// =============================================================================
// Main
// =============================================================================
int main(int argc, char* argv[]) {
  std::cout << "╔══════════════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║  CedarGraph MTHStream 3-Node Temporal Performance Benchmark         ║" << std::endl;
  std::cout << "╚══════════════════════════════════════════════════════════════════════╝" << std::endl;

  BenchmarkConfig config;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--vertices" && i + 1 < argc) {
      config.num_vertices = std::stoi(argv[++i]);
    } else if (arg == "--edges" && i + 1 < argc) {
      config.edges_per_vertex = std::stoi(argv[++i]);
    } else if (arg == "--threads" && i + 1 < argc) {
      config.num_threads = std::stoi(argv[++i]);
    } else if (arg == "--point-queries" && i + 1 < argc) {
      config.point_queries = std::stoi(argv[++i]);
    } else if (arg == "--range-queries" && i + 1 < argc) {
      config.range_queries = std::stoi(argv[++i]);
    } else if (arg == "--keep-data") {
      config.cleanup_after = false;
    } else if (arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  --vertices N         Number of vertices to write (default: 10000)" << std::endl;
      std::cout << "  --edges N            Edges per vertex (default: 10)" << std::endl;
      std::cout << "  --threads N          Number of threads (default: 8)" << std::endl;
      std::cout << "  --point-queries N    Number of point queries (default: 50000)" << std::endl;
      std::cout << "  --range-queries N    Number of range queries (default: 10000)" << std::endl;
      std::cout << "  --keep-data          Don't cleanup data directories" << std::endl;
      return 0;
    }
  }

  std::cout << "\nConfiguration:" << std::endl;
  std::cout << "  Nodes: " << config.node_count << std::endl;
  std::cout << "  Partitions: " << config.num_partitions << std::endl;
  std::cout << "  Vertices: " << config.num_vertices << std::endl;
  std::cout << "  Edges/vertex: " << config.edges_per_vertex << std::endl;
  std::cout << "  Threads: " << config.num_threads << std::endl;
  std::cout << "  Point queries: " << config.point_queries << std::endl;
  std::cout << "  Range queries: " << config.range_queries << std::endl;

  try {
    RunComparison(config);
    std::cout << "\n✅ Benchmark completed successfully!" << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "\n❌ Benchmark failed: " << e.what() << std::endl;
    return 1;
  }
}
