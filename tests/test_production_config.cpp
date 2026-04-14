// Copyright 2025 The Cedar Authors
// Production Configuration Demo - CedarGraph
// Demonstrates production-ready configurations and optimizations

#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>

#include "cedar/dtx/production_config.h"
#include "cedar/dtx/optimized_2pc_engine.h"
#include "cedar/dtx/storage_service_impl.h"

using namespace cedar;
using namespace cedar::dtx;

// =============================================================================
// Print Configuration Helper
// =============================================================================
void PrintNetworkConfig(const NetworkConfig& net) {
  std::cout << "  Network Configuration:" << std::endl;
  std::cout << "    Transport: ";
  switch (net.transport_type) {
    case NetworkConfig::TransportType::kTCP: std::cout << "TCP"; break;
    case NetworkConfig::TransportType::kRDMA: std::cout << "RDMA"; break;
    case NetworkConfig::TransportType::kDPDK: std::cout << "DPDK"; break;
    case NetworkConfig::TransportType::kQUIC: std::cout << "QUIC"; break;
  }
  std::cout << std::endl;
  
  std::cout << "    TCP NoDelay: " << (net.tcp_nodelay ? "Yes" : "No") << std::endl;
  std::cout << "    Send/Recv Buffer: " << net.tcp_send_buffer_size / 1024 << "KB" << std::endl;
  std::cout << "    Connections: " << net.min_connections << "-" << net.max_connections << std::endl;
}

void Print2PCConfig(const TwoPCConfig& twopc) {
  std::cout << "  2PC Configuration:" << std::endl;
  std::cout << "    Strategy: ";
  switch (twopc.strategy) {
    case TwoPCConfig::Strategy::kSequential: std::cout << "Sequential"; break;
    case TwoPCConfig::Strategy::kParallel: std::cout << "Parallel"; break;
    case TwoPCConfig::Strategy::kPipelined: std::cout << "Pipelined"; break;
    case TwoPCConfig::Strategy::kBatched: std::cout << "Batched"; break;
    case TwoPCConfig::Strategy::kHybrid: std::cout << "Hybrid (Auto)"; break;
  }
  std::cout << std::endl;
  
  std::cout << "    Pipeline Depth: " << twopc.pipeline_depth << std::endl;
  std::cout << "    Batch Size: " << twopc.batch_size << std::endl;
  std::cout << "    Parallel Threads: " << twopc.parallel_threads << std::endl;
  std::cout << "    Async Prepare/Commit: " 
            << (twopc.async_prepare ? "Yes" : "No") << "/"
            << (twopc.async_commit ? "Yes" : "No") << std::endl;
  std::cout << "    Adaptive Tuning: " << (twopc.enable_adaptive_tuning ? "Yes" : "No") << std::endl;
  std::cout << "    Timeout (Prepare/Commit): " 
            << twopc.prepare_timeout_ms << "ms/" << twopc.commit_timeout_ms << "ms" << std::endl;
}

void PrintRaftConfig(const RaftConfig& raft) {
  std::cout << "  Raft Configuration:" << std::endl;
  std::cout << "    Node ID: " << raft.node_id << std::endl;
  std::cout << "    Listen: " << raft.listen_address << std::endl;
  std::cout << "    Peers: " << raft.peer_addresses.size() << std::endl;
  std::cout << "    Heartbeat: " << raft.heartbeat_interval_ms << "ms" << std::endl;
  std::cout << "    Election Timeout: " << raft.election_timeout_min_ms << "-" 
            << raft.election_timeout_max_ms << "ms" << std::endl;
  std::cout << "    Max Append Entries: " << raft.max_append_entries << std::endl;
  std::cout << "    Async Log Write: " << (raft.async_log_write ? "Yes" : "No") << std::endl;
  std::cout << "    Multi-Raft: " << (raft.enable_multi_raft ? "Yes" : "No") << std::endl;
}

void PrintProductionConfig(const ProductionConfig& config, const std::string& name) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║  " << std::left << std::setw(54) << name << "║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  
  PrintNetworkConfig(config.network);
  Print2PCConfig(config.twopc);
  PrintRaftConfig(config.raft);
  std::cout << std::endl;
}

// =============================================================================
// Demo: Configuration Presets
// =============================================================================
void DemoConfigurationPresets() {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Production Configuration Presets            ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Low Latency Cluster (LAN)
  auto low_latency = ProductionConfigFactory::ForLowLatencyCluster();
  PrintProductionConfig(low_latency, "Low Latency Cluster (LAN)");
  
  // High Latency Cluster (WAN)
  auto high_latency = ProductionConfigFactory::ForHighLatencyCluster();
  PrintProductionConfig(high_latency, "High Latency Cluster (WAN)");
  
  // High Throughput
  auto high_throughput = ProductionConfigFactory::ForHighThroughput();
  PrintProductionConfig(high_throughput, "High Throughput");
  
  // Strong Consistency
  auto strong_consistency = ProductionConfigFactory::ForStrongConsistency();
  PrintProductionConfig(strong_consistency, "Strong Consistency");
  
  // Balanced
  auto balanced = ProductionConfigFactory::ForBalanced();
  PrintProductionConfig(balanced, "Balanced (General Purpose)");
}

// =============================================================================
// Demo: Optimized 2PC Engine
// =============================================================================
void DemoOptimized2PCEngine() {
  std::cout << std::endl;
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Optimized 2PC Engine Demo                              ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Create production config
  auto config = ProductionConfigFactory::ForHighThroughput();
  
  std::cout << "Production Configuration for High Throughput:" << std::endl;
  std::cout << "  Strategy: ";
  switch (config.twopc.strategy) {
    case TwoPCConfig::Strategy::kParallel: std::cout << "Parallel"; break;
    case TwoPCConfig::Strategy::kPipelined: std::cout << "Pipelined"; break;
    case TwoPCConfig::Strategy::kBatched: std::cout << "Batched"; break;
    default: std::cout << "Other"; break;
  }
  std::cout << std::endl;
  std::cout << "  Threads: " << config.twopc.parallel_threads << std::endl;
  std::cout << "  Pipeline Depth: " << config.twopc.pipeline_depth << std::endl;
  std::cout << "  Batch Size: " << config.twopc.batch_size << std::endl;
  std::cout << "  Async Prepare: " << (config.twopc.async_prepare ? "Yes" : "No") << std::endl;
  std::cout << "  Async Commit: " << (config.twopc.async_commit ? "Yes" : "No") << std::endl;
  std::cout << "  Adaptive Tuning: " << (config.twopc.enable_adaptive_tuning ? "Yes" : "No") << std::endl;
  std::cout << std::endl;
  
  std::cout << "Optimized 2PC Engine Features:" << std::endl;
  std::cout << "  [✓] Parallel 2PC - Async operations across nodes" << std::endl;
  std::cout << "  [✓] Transaction Pipeline - Overlap prepare/commit" << std::endl;
  std::cout << "  [✓] Batch Processing - Amortize network overhead" << std::endl;
  std::cout << "  [✓] Adaptive Tuning - Auto-adjust based on metrics" << std::endl;
  std::cout << "  [✓] Raft Integration - Consensus-based coordination" << std::endl;
  std::cout << std::endl;
  
  std::cout << "API Usage Example:" << std::endl;
  std::cout << "  // Create engine with config" << std::endl;
  std::cout << "  Optimized2PCEngine engine(config.twopc);" << std::endl;
  std::cout << "  engine.Initialize(clients);" << std::endl;
  std::cout << std::endl;
  std::cout << "  // Execute transaction" << std::endl;
  std::cout << "  Status s = engine.Execute2PC(txn_id, reads, writes, commit_ts);" << std::endl;
  std::cout << std::endl;
  std::cout << "  // Async execution" << std::endl;
  std::cout << "  engine.Execute2PCAsync(txn_id, reads, writes, commit_ts, callback);" << std::endl;
  std::cout << std::endl;
  std::cout << "  // Batch execution" << std::endl;
  std::cout << "  auto results = engine.Execute2PCBatch(transactions);" << std::endl;
}

// =============================================================================
// Demo: Configuration Validation
// =============================================================================
void DemoConfigurationValidation() {
  std::cout << std::endl;
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Configuration Validation                               ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Valid config
  auto valid_config = ProductionConfigFactory::ForBalanced();
  auto status = ProductionConfigFactory::Validate(valid_config);
  std::cout << "Valid config: " << (status.ok() ? "PASS" : "FAIL") << std::endl;
  
  // Invalid config (batch size too large)
  auto invalid_config = valid_config;
  invalid_config.twopc.batch_size = 10000;
  status = ProductionConfigFactory::Validate(invalid_config);
  std::cout << "Invalid config (batch_size=10000): " << (status.ok() ? "PASS" : "FAIL") << std::endl;
  if (!status.ok()) {
    std::cout << "  Error: " << status.ToString() << std::endl;
  }
  
  // Invalid config (election timeout)
  invalid_config = valid_config;
  invalid_config.raft.election_timeout_min_ms = 500;
  invalid_config.raft.election_timeout_max_ms = 400;  // min > max
  status = ProductionConfigFactory::Validate(invalid_config);
  std::cout << "Invalid config (election_timeout): " << (status.ok() ? "PASS" : "FAIL") << std::endl;
  if (!status.ok()) {
    std::cout << "  Error: " << status.ToString() << std::endl;
  }
}

// =============================================================================
// Main
// =============================================================================
int main(int argc, char* argv[]) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Production Configuration System             ║" << std::endl;
  std::cout << "║     (Raft + Optimized 2PC + Network Tuning)                ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Run demos
  DemoConfigurationPresets();
  DemoOptimized2PCEngine();
  DemoConfigurationValidation();
  
  std::cout << std::endl;
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Production Deployment Checklist                        ║" << std::endl;
  std::cout << "╠════════════════════════════════════════════════════════════╣" << std::endl;
  std::cout << "║  [ ] Select configuration preset based on workload         ║" << std::endl;
  std::cout << "║  [ ] Configure network transport (TCP/RDMA/DPDK)           ║" << std::endl;
  std::cout << "║  [ ] Set up Raft cluster with proper node discovery        ║" << std::endl;
  std::cout << "║  [ ] Tune 2PC strategy (Parallel/Pipeline/Batch)           ║" << std::endl;
  std::cout << "║  [ ] Enable adaptive tuning for dynamic workloads          ║" << std::endl;
  std::cout << "║  [ ] Configure monitoring and alerting                     ║" << std::endl;
  std::cout << "║  [ ] Test failover and recovery procedures                 ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  
  return 0;
}
