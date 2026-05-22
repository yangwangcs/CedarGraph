// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// Production Configuration - CedarGraph Distributed Storage
// =============================================================================
// Optimized configurations for production deployment
// Includes: Raft consensus, network optimization, 2PC tuning
// =============================================================================

#ifndef CEDAR_DTX_PRODUCTION_CONFIG_H_
#define CEDAR_DTX_PRODUCTION_CONFIG_H_

#include <cstdint>
#include <string>
#include <chrono>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Network Transport Configuration
// =============================================================================
struct NetworkConfig {
  // Transport type
  enum class TransportType {
    kTCP = 0,       // Standard TCP (default)
    kRDMA = 1,      // RDMA (InfiniBand/RoCE)
    kDPDK = 2,      // DPDK kernel bypass
    kQUIC = 3,      // QUIC protocol
  };
  
  TransportType transport_type = TransportType::kTCP;
  
  // TCP settings
  int tcp_send_buffer_size = 256 * 1024;   // 256KB
  int tcp_recv_buffer_size = 256 * 1024;   // 256KB
  bool tcp_nodelay = true;                  // Disable Nagle's algorithm
  int tcp_keepalive_secs = 60;
  
  // RDMA settings (when transport_type = kRDMA)
  struct RDMAConfig {
    int ib_port = 1;                        // InfiniBand port
    int gid_idx = -1;                       // GID index for RoCE
    int cq_size = 1024;                     // Completion queue size
    int max_wr = 256;                       // Max work requests
    int max_sge = 4;                        // Max scatter-gather elements
    bool use_inline = true;                 // Use inline data for small messages
    size_t inline_threshold = 200;          // Inline threshold in bytes
  } rdma;
  
  // DPDK settings (when transport_type = kDPDK)
  struct DPDKConfig {
    int num_cores = 4;                      // DPDK cores
    int mbuf_pool_size = 8192;              // Memory buffer pool
    int rx_ring_size = 1024;                // RX ring size
    int tx_ring_size = 1024;                // TX ring size
    std::string eal_params;                 // Extra EAL parameters
  } dpdk;
  
  // Connection pool settings
  int min_connections = 4;
  int max_connections = 64;
  int connection_timeout_ms = 5000;
  int idle_timeout_ms = 60000;
};

// =============================================================================
// 2PC Transaction Optimization Configuration
// =============================================================================
struct TwoPCConfig {
  // Execution strategy
  enum class Strategy {
    kSequential = 0,        // Traditional sequential 2PC
    kParallel = 1,          // Parallel prepare/commit across nodes
    kPipelined = 2,         // Transaction pipeline
    kBatched = 3,           // Batch multiple transactions
    kHybrid = 4,            // Auto-select based on workload
  };
  
  Strategy strategy = Strategy::kParallel;
  
  // Pipeline settings
  int pipeline_depth = 4;              // Number of concurrent transactions in pipeline
  int pipeline_timeout_ms = 100;       // Pipeline stage timeout
  
  // Batch settings
  int batch_size = 8;                  // Transactions per batch
  int batch_timeout_us = 50;           // Max wait to form a batch
  int max_batch_delay_ms = 10;         // Max delay for batch formation
  
  // Parallel settings
  int parallel_threads = 4;            // Thread pool size for parallel 2PC
  bool async_prepare = true;           // Async prepare phase
  bool async_commit = true;            // Async commit phase
  
  // Timeout settings
  int prepare_timeout_ms = 5000;       // Prepare phase timeout
  int commit_timeout_ms = 5000;        // Commit phase timeout
  int participant_timeout_ms = 3000;   // Per-participant timeout
  
  // Retry settings
  int max_retries = 3;
  int retry_base_delay_ms = 10;
  bool exponential_backoff = true;
  
  // Adaptive tuning (auto-adjust based on metrics)
  bool enable_adaptive_tuning = true;
  int tuning_interval_sec = 60;        // Re-tune every 60 seconds
  double latency_target_ms = 10.0;     // Target latency for auto-tuning

  // Decision log directory for coordinator crash recovery
  std::string decision_log_dir;  // Directory for coordinator decision logs
};

// =============================================================================
// Raft Consensus Configuration
// =============================================================================
struct RaftConfig {
  // Node configuration
  NodeID node_id = 0;
  std::vector<std::string> peer_addresses;  // Other nodes in cluster
  std::string listen_address = "0.0.0.0:9200";
  std::string data_directory = "/var/lib/cedar/raft";
  
  // Election timing (tune based on network latency)
  int election_timeout_min_ms = 150;   // Min election timeout
  int election_timeout_max_ms = 300;   // Max election timeout
  int heartbeat_interval_ms = 50;      // Leader heartbeat interval
  
  // Log replication
  int max_append_entries = 100;        // Max entries per AppendEntries
  int snapshot_threshold = 10000;      // Log entries before snapshot
  int snapshot_chunk_size = 64 * 1024; // 64KB snapshot chunks
  
  // Performance tuning
  int apply_batch_size = 100;          // Batch apply committed entries
  int commit_pipeline = 10;            // Pipeline commit index advance
  bool async_log_write = true;         // Async log persistence
  int log_sync_interval_ms = 10;       // Log sync interval
  
  // Multi-Raft optimization (for partition sharding)
  bool enable_multi_raft = true;
  int max_raft_groups = 100;           // Max concurrent Raft groups
  int raft_group_cache_size = 1000;    // Raft group metadata cache
  
  // Leader transfer
  bool enable_leader_transfer = true;
  int leader_transfer_timeout_ms = 5000;
};

// =============================================================================
// Storage Partition Configuration
// =============================================================================
struct PartitionConfig {
  // Partition strategy
  enum class Strategy {
    kHash = 0,         // Hash-based partitioning
    kRange = 1,        // Range-based partitioning
    kHybrid = 2,       // Hash + Range hybrid
  };
  
  Strategy strategy = Strategy::kHash;
  int num_partitions = 1024;           // Total partitions
  int replication_factor = 3;          // Replication factor
  
  // Load balancing
  bool enable_rebalancing = true;
  double rebalance_threshold = 0.2;    // Trigger at 20% imbalance
  int rebalance_interval_min = 60;     // Min interval between rebalances
  
  // Placement strategy
  bool enable_aware_placement = true;  // Rack/zone-aware placement
  int min_racks = 2;                   // Min racks for replication
  int min_zones = 2;                   // Min zones for replication
};

// =============================================================================
// Production Environment Presets
// =============================================================================
struct ProductionConfig {
  // Environment type
  enum class Environment {
    kDevelopment = 0,    // Single node, no replication
    kTesting = 1,        // Local cluster, minimal replication
    kStaging = 2,        // Production-like, smaller scale
    kProduction = 3,     // Full production settings
  };
  
  Environment env = Environment::kProduction;
  
  // Sub-configurations
  NetworkConfig network;
  TwoPCConfig twopc;
  RaftConfig raft;
  PartitionConfig partition;
  
  // Global settings
  std::string cluster_name = "cedar-cluster";
  std::string data_center = "dc1";
  std::string rack = "rack1";
  int metrics_port = 9090;
  bool enable_tracing = true;
};

// =============================================================================
// Configuration Factory - Predefined Presets
// =============================================================================
class ProductionConfigFactory {
 public:
  // Low-latency LAN environment (same data center)
  static ProductionConfig ForLowLatencyCluster();
  
  // High-latency WAN environment (cross data center)
  static ProductionConfig ForHighLatencyCluster();
  
  // High-throughput batch processing
  static ProductionConfig ForHighThroughput();
  
  // Strong consistency priority
  static ProductionConfig ForStrongConsistency();
  
  // Balanced general purpose
  static ProductionConfig ForBalanced();
  
  // Load from configuration file
  static ProductionConfig FromFile(const std::string& path);
  
  // Validate configuration
  static Status Validate(const ProductionConfig& config);
};

// =============================================================================
// Implementation
// =============================================================================

inline ProductionConfig ProductionConfigFactory::ForLowLatencyCluster() {
  ProductionConfig config;
  config.env = ProductionConfig::Environment::kProduction;
  
  // Network: TCP with aggressive tuning
  config.network.transport_type = NetworkConfig::TransportType::kTCP;
  config.network.tcp_nodelay = true;
  config.network.tcp_send_buffer_size = 512 * 1024;
  config.network.tcp_recv_buffer_size = 512 * 1024;
  
  // 2PC: Parallel strategy for low latency
  config.twopc.strategy = TwoPCConfig::Strategy::kParallel;
  config.twopc.parallel_threads = 8;
  config.twopc.async_prepare = true;
  config.twopc.async_commit = true;
  config.twopc.prepare_timeout_ms = 1000;
  config.twopc.commit_timeout_ms = 1000;
  
  // Raft: Aggressive heartbeat for fast failover
  config.raft.heartbeat_interval_ms = 20;
  config.raft.election_timeout_min_ms = 100;
  config.raft.election_timeout_max_ms = 200;
  config.raft.max_append_entries = 200;
  
  return config;
}

inline ProductionConfig ProductionConfigFactory::ForHighLatencyCluster() {
  ProductionConfig config;
  config.env = ProductionConfig::Environment::kProduction;
  
  // Network: Batching to amortize latency
  config.network.transport_type = NetworkConfig::TransportType::kTCP;
  
  // 2PC: Batched strategy to reduce round trips
  config.twopc.strategy = TwoPCConfig::Strategy::kBatched;
  config.twopc.batch_size = 32;
  config.twopc.batch_timeout_us = 200;
  config.twopc.max_batch_delay_ms = 50;
  config.twopc.prepare_timeout_ms = 10000;
  config.twopc.commit_timeout_ms = 10000;
  
  // Raft: Conservative timing for high latency
  config.raft.heartbeat_interval_ms = 100;
  config.raft.election_timeout_min_ms = 500;
  config.raft.election_timeout_max_ms = 1000;
  config.raft.max_append_entries = 500;  // Larger batches
  
  return config;
}

inline ProductionConfig ProductionConfigFactory::ForHighThroughput() {
  ProductionConfig config;
  config.env = ProductionConfig::Environment::kProduction;
  
  // Network: DPDK/RDMA for high throughput
  #ifdef CEDAR_HAS_RDMA
  config.network.transport_type = NetworkConfig::TransportType::kRDMA;
  #else
  config.network.transport_type = NetworkConfig::TransportType::kTCP;
  #endif
  
  // 2PC: Pipelined for throughput
  config.twopc.strategy = TwoPCConfig::Strategy::kPipelined;
  config.twopc.pipeline_depth = 16;
  config.twopc.batch_size = 16;
  config.twopc.parallel_threads = 16;
  
  // Raft: High throughput tuning
  config.raft.max_append_entries = 500;
  config.raft.apply_batch_size = 200;
  config.raft.async_log_write = true;
  
  return config;
}

inline ProductionConfig ProductionConfigFactory::ForStrongConsistency() {
  ProductionConfig config;
  config.env = ProductionConfig::Environment::kProduction;
  
  // Network: Reliable TCP
  config.network.transport_type = NetworkConfig::TransportType::kTCP;
  
  // 2PC: Sequential for strict ordering
  config.twopc.strategy = TwoPCConfig::Strategy::kSequential;
  config.twopc.max_retries = 5;
  config.twopc.prepare_timeout_ms = 10000;
  config.twopc.commit_timeout_ms = 10000;
  
  // Raft: Strong consistency
  config.raft.async_log_write = false;  // Sync write for durability
  config.raft.log_sync_interval_ms = 0;
  config.raft.snapshot_threshold = 5000;  // Frequent snapshots
  
  return config;
}

inline ProductionConfig ProductionConfigFactory::ForBalanced() {
  ProductionConfig config;
  config.env = ProductionConfig::Environment::kProduction;
  
  // Network: Standard TCP
  config.network.transport_type = NetworkConfig::TransportType::kTCP;
  
  // 2PC: Hybrid with adaptive tuning
  config.twopc.strategy = TwoPCConfig::Strategy::kHybrid;
  config.twopc.enable_adaptive_tuning = true;
  config.twopc.pipeline_depth = 4;
  config.twopc.batch_size = 8;
  
  // Raft: Balanced settings
  config.raft.heartbeat_interval_ms = 50;
  config.raft.election_timeout_min_ms = 150;
  config.raft.election_timeout_max_ms = 300;
  
  return config;
}

inline Status ProductionConfigFactory::Validate(const ProductionConfig& config) {
  // Validate 2PC config
  if (config.twopc.batch_size < 1 || config.twopc.batch_size > 1000) {
    return Status::InvalidArgument("Invalid batch_size: " + 
                                   std::to_string(config.twopc.batch_size));
  }
  
  if (config.twopc.pipeline_depth < 1 || config.twopc.pipeline_depth > 100) {
    return Status::InvalidArgument("Invalid pipeline_depth: " + 
                                   std::to_string(config.twopc.pipeline_depth));
  }
  
  // Validate Raft config
  if (config.raft.heartbeat_interval_ms < 10) {
    return Status::InvalidArgument("heartbeat_interval too small");
  }
  
  if (config.raft.election_timeout_min_ms >= config.raft.election_timeout_max_ms) {
    return Status::InvalidArgument("election_timeout_min must be < max");
  }
  
  // Validate network config
  if (config.network.min_connections > config.network.max_connections) {
    return Status::InvalidArgument("min_connections > max_connections");
  }
  
  return Status::OK();
}

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_PRODUCTION_CONFIG_H_
