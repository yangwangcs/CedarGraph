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
// Chaos Testing - Fault Injection for Long-term Stability Testing
// =============================================================================
// Provides various fault injection mechanisms to test system resilience:
// - Network partition
// - Node failure
// - Disk failure
// - Latency injection
// - Packet loss
// =============================================================================

#ifndef CEDAR_DTX_CHAOS_TESTING_H_
#define CEDAR_DTX_CHAOS_TESTING_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// Forward declarations
class StorageClient;

// =============================================================================
// Fault Types
// =============================================================================
enum class FaultType {
  kNetworkPartition = 0,  // Network isolation between nodes
  kNodeCrash = 1,         // Complete node failure
  kDiskFailure = 2,       // Disk I/O failure
  kNetworkLatency = 3,    // Add latency to network calls
  kPacketLoss = 4,        // Random packet dropping
  kMemoryExhaustion = 5,  // High memory pressure
  kCPUOverload = 6,       // High CPU load
  kClockSkew = 7,         // Time synchronization issues
};

struct FaultSpec {
  FaultType type;
  double probability;     // 0.0 - 1.0
  std::chrono::milliseconds duration;
  std::chrono::milliseconds start_after;  // Start after delay
  std::vector<NodeID> target_nodes;  // Empty = all nodes
  
  // Type-specific parameters
  struct NetworkPartitionParams {
    std::vector<NodeID> isolated_nodes;
  } partition_params;
  
  struct LatencyParams {
    std::chrono::milliseconds min_latency;
    std::chrono::milliseconds max_latency;
  } latency_params;
  
  struct PacketLossParams {
    double drop_rate;  // 0.0 - 1.0
  } packet_loss_params;
};

// =============================================================================
// Fault Injection Manager
// =============================================================================
class FaultInjector {
 public:
  FaultInjector();
  ~FaultInjector();
  
  // Start fault injection
  Status Initialize(const std::vector<FaultSpec>& specs);
  void Shutdown();
  
  // Inject specific fault
  Status InjectFault(const FaultSpec& spec);
  
  // Remove fault
  Status RemoveFault(FaultType type);
  
  // Recover all faults
  void ClearAllFaults();
  
  // Check if fault is active for node
  bool IsFaultActive(FaultType type, NodeID node_id) const;
  
  // Get active faults
  std::vector<FaultType> GetActiveFaults(NodeID node_id) const;
  
  // Callback registration for fault events
  using FaultCallback = std::function<void(FaultType, NodeID, bool /*injected*/)>;
  void RegisterFaultCallback(FaultCallback callback);
  
 private:
  void FaultWorkerLoop();
  void ApplyFault(const FaultSpec& spec);
  void RevertFault(const FaultSpec& spec);
  
  std::vector<FaultSpec> specs_;
  mutable std::mutex specs_mutex_;
  
  std::unordered_map<NodeID, std::unordered_set<FaultType>> active_faults_;
  mutable std::mutex faults_mutex_;
  
  std::vector<FaultCallback> callbacks_;
  mutable std::mutex callback_mutex_;
  
  std::thread worker_thread_;
  std::atomic<bool> running_{false};
  std::atomic<bool> shutdown_{false};
  
  std::mt19937 rng_;
};

// =============================================================================
// Long-term Stability Test Framework
// =============================================================================
class LongTermStabilityTest {
 public:
  struct Config {
    // Test duration
    std::chrono::hours test_duration{24};  // Default 24 hours
    
    // Workload
    int64_t target_throughput = 1000;  // ops/sec
    double read_write_ratio = 0.8;     // 80% reads
    int value_size = 100;              // bytes
    
    // Fault injection
    bool enable_fault_injection = true;
    std::chrono::minutes fault_interval{30};  // Inject fault every 30 min
    std::chrono::minutes fault_duration{5};   // Each fault lasts 5 min
    
    // Validation
    std::chrono::minutes consistency_check_interval{10};
    bool enable_data_integrity_check = true;
    
    // Reporting
    std::chrono::seconds metrics_interval{60};  // Report every minute
    std::string output_directory = "/tmp/cedar_stability_test";
  };
  
  // Non-atomic stats for returning to callers
  struct TestStats {
    int64_t total_operations{0};
    int64_t successful_operations{0};
    int64_t failed_operations{0};
    int64_t timeout_operations{0};
    int64_t faults_injected{0};
    int64_t faults_recovered{0};
    int64_t latency_sum_us{0};
    int64_t latency_count{0};
    int64_t max_latency_us{0};
    int64_t p99_latency_us{0};
    int64_t consistency_checks{0};
    int64_t consistency_violations{0};
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;
    
    double GetAverageLatency() const {
      return latency_count > 0 ? (double)latency_sum_us / latency_count : 0.0;
    }
    
    double GetSuccessRate() const {
      return total_operations > 0 ? (double)successful_operations / total_operations : 0.0;
    }
  };
  
  // Internal atomic stats
  struct AtomicTestStats {
    std::atomic<int64_t> total_operations{0};
    std::atomic<int64_t> successful_operations{0};
    std::atomic<int64_t> failed_operations{0};
    std::atomic<int64_t> timeout_operations{0};
    std::atomic<int64_t> faults_injected{0};
    std::atomic<int64_t> faults_recovered{0};
    std::atomic<int64_t> latency_sum_us{0};
    std::atomic<int64_t> latency_count{0};
    std::atomic<int64_t> max_latency_us{0};
    std::atomic<int64_t> p99_latency_us{0};
    std::atomic<int64_t> consistency_checks{0};
    std::atomic<int64_t> consistency_violations{0};
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;
    
    TestStats GetSnapshot() const {
      TestStats s;
      s.total_operations = total_operations.load();
      s.successful_operations = successful_operations.load();
      s.failed_operations = failed_operations.load();
      s.timeout_operations = timeout_operations.load();
      s.faults_injected = faults_injected.load();
      s.faults_recovered = faults_recovered.load();
      s.latency_sum_us = latency_sum_us.load();
      s.latency_count = latency_count.load();
      s.max_latency_us = max_latency_us.load();
      s.p99_latency_us = p99_latency_us.load();
      s.consistency_checks = consistency_checks.load();
      s.consistency_violations = consistency_violations.load();
      s.start_time = start_time;
      s.end_time = end_time;
      return s;
    }
  };
  
  explicit LongTermStabilityTest(const Config& config);
  ~LongTermStabilityTest();
  
  // Initialize with storage clients
  Status Initialize(const std::vector<std::shared_ptr<cedar::dtx::StorageClient>>& clients);
  
  // Run the test
  Status Run();
  
  // Stop the test early
  void Stop();
  
  // Get current stats
  TestStats GetStats() const {
    return stats_.GetSnapshot();
  }
  
  // Generate report
  Status GenerateReport(const std::string& output_path) const;
  
 private:
  // Worker threads
  void WorkloadGeneratorLoop();
  void FaultInjectionLoop();
  void ConsistencyCheckLoop();
  void MetricsReporterLoop();
  
  // Operations
  Status PerformRead();
  Status PerformWrite();
  Status PerformTransaction();
  
  // Validation
  Status CheckDataConsistency();
  Status CheckNodeHealth();
  
  // Fault injection
  void InjectRandomFault();
  void ClearAllFaults();
  
  // Helper
  void UpdateLatencyStats(int64_t latency_us);
  void LogMessage(const std::string& level, const std::string& message);
  
  Config config_;
  AtomicTestStats stats_;
  mutable std::mutex stats_mutex_;
  
  std::vector<std::shared_ptr<StorageClient>> clients_;
  mutable std::mutex clients_mutex_;
  
  std::unique_ptr<FaultInjector> fault_injector_;
  
  // Worker threads
  std::vector<std::thread> workload_threads_;
  std::thread fault_thread_;
  std::thread consistency_thread_;
  std::thread metrics_thread_;
  
  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};
  
  // Latency histogram for P99 calculation
  std::vector<int64_t> latency_histogram_;
  mutable std::mutex histogram_mutex_;
  
  std::mt19937 rng_;
};

// =============================================================================
// Automated Recovery Manager
// =============================================================================
class AutomatedRecoveryManager {
 public:
  enum class FailureType {
    kNodeUnreachable = 0,
    kDiskFull = 1,
    kMemoryExhaustion = 2,
    kNetworkPartition = 3,
    kRaftLeaderElectionFailure = 4,
    kTransactionTimeout = 5,
    kDataCorruption = 6,
  };
  
  struct FailureEvent {
    FailureType type;
    NodeID node_id;
    std::chrono::system_clock::time_point timestamp;
    std::string details;
    int severity;  // 1-5, 5 being most severe
  };
  
  struct RecoveryAction {
    FailureType failure_type;
    std::string name;
    std::function<Status(const FailureEvent&)> action;
    int max_attempts;
    std::chrono::seconds backoff_duration;
  };
  
  AutomatedRecoveryManager();
  ~AutomatedRecoveryManager();
  
  // Initialize with node information
  Status Initialize(const std::vector<std::string>& node_addresses);
  
  // Start monitoring and recovery
  void Start();
  void Stop();
  
  // Register custom recovery action
  void RegisterRecoveryAction(const RecoveryAction& action);
  
  // Report failure (manual or from monitor)
  void ReportFailure(const FailureEvent& event);
  
  // Get recovery history
  std::vector<std::pair<FailureEvent, Status>> GetRecoveryHistory() const;
  
  // Configuration
  void SetAutoRecoveryEnabled(bool enabled);
  bool IsAutoRecoveryEnabled() const;
  
 private:
  void MonitoringLoop();
  void RecoveryWorkerLoop();
  
  // Built-in recovery actions
  Status RestartNode(const FailureEvent& event);
  Status ReassignLeader(const FailureEvent& event);
  Status ClearDiskSpace(const FailureEvent& event);
  Status RepairNetwork(const FailureEvent& event);
  Status RollbackTransaction(const FailureEvent& event);
  Status RestoreFromBackup(const FailureEvent& event);
  
  // Helper
  FailureType DetectFailureType(const std::string& error_message);
  Status ExecuteRecovery(const FailureEvent& event);
  
  std::vector<std::string> node_addresses_;
  mutable std::mutex nodes_mutex_;
  
  std::unordered_map<FailureType, RecoveryAction> recovery_actions_;
  mutable std::mutex actions_mutex_;
  
  std::queue<FailureEvent> failure_queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  
  std::vector<std::pair<FailureEvent, Status>> recovery_history_;
  mutable std::mutex history_mutex_;
  
  std::thread monitor_thread_;
  std::thread recovery_thread_;
  
  std::atomic<bool> running_{false};
  std::atomic<bool> auto_recovery_enabled_{true};
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_CHAOS_TESTING_H_
